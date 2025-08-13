<!-- go/markdown -->

# Creating Koda Operators

This guide provides instructions for adding Koda operators in Python and in C++.
Operators are an Arolla concept that offer lower-level building blocks that are
available in both eager and tracing mode. When bound to arguments, whether
literals or tracing inputs, they create Expressions, which form the
computational part of a Functor. See the
[Technical Deep Dive: Deferred Evaluation, Tracing, and Functors](/koladata/g3doc/deep_dive/functors.md)
for a deep dive into this topic.

* TOC
{:toc}

## Defining Operators

There are three main types of operators:

1.  *Lambda operator*: A high-level operator composed of a collection of other
    operators.
1.  *Py-function operator*: Wraps a Python function that is invoked upon
    evaluation.
1.  *Backend operator*: An operator stub backed by a linked C++ implementation
    of the same name.

You can add an operator by name in a global registry by calling
`kd.optools.add_to_registry`.

By default, there is an assumption that the result of an operator evaluation is
deterministic and therefore cacheable. If this is not the case, all operator
decorators described below support a `deterministic=False` argument forcing
evaluation to always happen.

### Lambda Operators

A lambda operator is a high-level operator composed of a collection of other
operators. These operators allow you to compose logic into higher level
abstractions entirely from python. `kd.optools.as_lambda_operator` is the main
interface for defining them. Many core operators available through `kd.[...]`
are lambdas. Example:

```py
from arolla import arolla
from koladata import kd

P = arolla.P

@kd.optools.add_to_registry()
@kd.optools.as_lambda_operator(
  'my_project.cond',
  qtype_constraints=[
    kd.optools.constraints.expect_data_slice(P.condition),
    kd.optools.constraints.expect_data_slice(P.yes),
    kd.optools.constraints.expect_data_slice(P.no),
  ]
)
def cond(condition, yes, no)
  return kd.lazy.coalesce((yes & condition), (no & ~condition))
```

In the example above, we define and register the operator `my_project.cond`
using a combination of `&` (syntactic sugar for `kd.apply_mask`) and
`kd.coalesce`. Things to note:

*   The output QType, in this case `DATA_SLICE`, is automatically computed from
    the composed operators. This differs from the other operator types described
    below.
*   The allowed input QTypes are limited by a combination of what is allowed by
    the composed operators and the manually provided `qtype_constraints`. The
    optional `qtype_constraints` parameter allows the types to be restricted
    further than what the constraints inherited from the implementation. The
    additional constraints can help make error messages more relevant.
*   The function body works entirely in tracing mode, requiring
    `kd.lazy.coalesce` to be used instead of `kd.coalesce`.
*   A lambda operator is servable in C++ as long as all operators used in its
    body are servable.
*   All operators used in the body must be imported before constructing the
    lambda opeator.

### Py-function Operators

Py-function operators wrap python logic as part of an operator. This allows for
quick experimentation and for functionality that cannot be expressed in C++ or
through a combination of other operators to be expressed as an operator. These
operators cannot be evaluated through C++.

```py
from arolla import arolla
from koladata import kd

P = arolla.P

@kd.optools.add_to_registry()
@kd.optools.as_py_function_operator(
  'my_project.cond',
  qtype_constraints=[
    kd.optools.constraints.expect_data_slice(P.condition),
    kd.optools.constraints.expect_data_slice(P.yes),
    kd.optools.constraints.expect_data_slice(P.no),
  ],
  qtype_inference_expr=kd.qtypes.DATA_SLICE,  # optional in this case.
)
def cond(condition, yes, no)
  return kd.eager.coalesce((yes & condition), (no & ~condition))
```

Things to note:

*   These operators cannot be evaluated through C++.
*   The allowed input QTypes are limited by the manually provided
    `qtype_constraints`. The `qtype_constraints` allows invalid input QTypes
    (but not Schemas) to be detected at Expression creation time.
*   `qtype_inference_expr` defaults to `kd.qtypes.DATA_SLICE` and can be omitted
    for this case.
*   The function body works entirely in eager mode, and lazy operators should
    not be used, hence the use of `kd.eager.coalesce` instead of
    `kd.lazy.coalesce` (`kd.coalesce` can be used as well since it defaults to
    the eager implementation).

### Backend Operators

Backend operators provide C++ evaluation logic and are comprised of two parts:

1.  A Python operator definition that provides an operator stub defining the
    interface, signature, QType constraints, and similar for the operator.
1.  A corresponding C++ backend operator (also known as a QExpr operator) that
    defines the logic.

Below follows an example defining the `my_project.apply_mask` operator, which is
one of the building blocks of the higher-level `my_project.cond` operator.

#### Python Definition

```py
from arolla import arolla
from koladata import kd

P = arolla.P

@kd.optools.add_to_registry()
@kd.optools.as_backend_operator(
  'my_project.apply_mask',
  qtype_constraints=[
    kd.optools.constraints.expect_data_slice(P.obj),
    kd.optools.constraints.expect_data_slice(P.mask),
  ],
  qtype_inference_expr=kd.qtypes.DATA_SLICE,  # optional in this case.
)
def apply_mask(obj, mask):
  raise NotImplementedError('implemented by the backend')
```

Things to note:

*   The allowed input QTypes are limited by the manually provided
    `qtype_constraints`. The `qtype_constraints` allows invalid input QTypes
    (but not Schemas) to be detected at Expression creation time.
*   `qtype_inference_expr` defaults to `kd.qtypes.DATA_SLICE` and can be omitted
    for this case.
*   The operator body is completely ignored and the implementation must be
    provided by a C++ implementation.
*   The name *must* match the operator name passed to the `KODA_QEXPR_OPERATOR`
    macro in C++ (see below).

#### C++ Definition

The C++ definition is comprised of two main parts:

1.  Operator declaration and definition.
1.  Registering the operator as a backend operator.

**Declaring and Defining the operator**

```cc
// In apply_mask.h

namespace koladata::ops {

absl::StatusOr<DataSlice> ApplyMask(
    const DataSlice& obj, const DataSlice& mask);

}  // namespace koladata::ops
```

Below follows an example definition of the operator. In practice, the
[implementation](https://github.com/google/koladata/blob/main//koladata/operators/masking.h)
is optimized and looks very different. See the
[Working with `DataSlice` in C++](data_slice.md) doc for tips on how to
efficiently work with DataSlices.

```cc
// In apply_mask.cc

#include "apply_mask.h"

absl::StatusOr<DataSlice> ApplyMask(
    const DataSlice& obj, const DataSlice& mask) {
  // Check that the data has the expected type. `obj` is allowed to be anything
  // but we require `mask` to be a MASK.
  RETURN_IF_ERROR(ExpectMask("mask", mask));

  // The slices are aligned to have the same shape, allowing them to be iterated
  // over together.
  ASSIGN_OR_RETURN(std::vector<DataSlice> aligned_slices,
                   shape::Align(std::vector<DataSlice>{obj, mask}));

  const DataSlice& broadcasted_obj = aligned_slices[0];
  const DataSlice& broadcasted_mask = aligned_slices[1];
  // Both values are scalars.
  if (broadcasted_obj.is_item()) {
    if (mask.IsEmpty()) {
      return DataSlice::Create(
          internal::DataItem(), obj.GetSchemaImpl(), obj.GetBag());
    } else {
      return obj;
    }
  }

  // They are not scalars. We apply a pointwise operation by iterating through
  // the values. Note that this is _inefficient_ compared to more involved
  // alternatives seen in the "Working with `DataSlice` in C++" doc above.
  const internal::DataSliceImpl& obj_slice = broadcasted_obj.slice();
  const internal::DataSliceImpl& mask_slice = broadcasted_mask.slice();
  internal::SliceBuilder bldr(/*size=*/broadcasted_obj.size());
  for (int i = 0; i < broadcasted_obj.size(); ++i) {
    if (mask_slice[i].IsEmpty()) {
      bldr.InsertIfNotSet(i, DataItem());
    } else {
      bldr.InsertIfNotSet(i, obj_slice[i]);
    }
  }
  internal::DataSliceImpl result_slice = std::move(bldr).Build();
  return DataSlice::Create(
          std::move(result_slice), obj.GetSchemaImpl(), obj.GetBag());
}
```

**Registering the Operator**

To allow the Python operator definition and the C++ function defined above to be
associated, we register the operator:

```cc
// In operators.cc
#include "apply_mask.h"


namespace koladata::ops {
namespace {

// The name _must_ match the name of the backend operator defined in Python.
KODA_QEXPR_OPERATOR("my_project.apply_mask", ApplyMask);

}  // namespace
}  // namespace koladata::ops
```

The associated `cc_library` build target must have `alwayslink = 1` defined. The
`cc_library` build target must thereafter be included as a dependency in the
Python build target for the Python operator.

## Supporting Eager and Lazy Operator Dispatching

Defining an operator does *not* make it automatically supported in both eager
and lazy workflows. These operators are available only as lazy versions. In
order to support eager and lazy dispatching, similar to `kd.[...]`, a file
should first be defined that exports all relevant registered lazy operators:

```py
# In lazy_ops.py.
from koladata import kd

# Contains the python definition of `my_project.cond`.
from my.namespace import cond_module as _
# Contains the python definition of `my_project.apply_mask`.
from my.namespace import apply_mask_module as _
# Contains the python definition of e.g. `my_project.foo.bar`.
from my.namespace import some_other_module as _

my_project_ops = kd.optools.make_operators_container(
    # Lists all namespaces that should be available through lazy_ops.
    'my_project', 'my_project.foo'
).my_project
```

To implement dispatching, the following file should be created (we hope to make
this smoother in the future):

```py
# In my_project.py.
import sys as _sys
import types as _py_types
import typing as _typing

from koladata import kd as _kd
from koladata.expr import tracing_mode as _tracing_mode
from koladata.operators import eager_op_utils as _eager_op_utils

from my.namespace import lazy_ops as _lazy_ops


_HAS_DYNAMIC_ATTRIBUTES = True

_tracing_config = {}
_dispatch = lambda eager, tracing: _tracing_mode.configure_tracing(
    _tracing_config, eager=eager, tracing=tracing
)
_eager_only = lambda obj: _tracing_mode.eager_only(_tracing_config, obj)
_same_when_tracing = lambda obj: _tracing_mode.same_when_tracing(
    _tracing_config, obj
)


def _init_ops_and_containers():
  eager_ops = _eager_op_utils.operators_container(
      top_level_arolla_container=_lazy_ops.my_project_ops
  )
  for op_or_container_name in dir(eager_ops):
    globals()[op_or_container_name] = _dispatch(
        eager=getattr(eager_ops, op_or_container_name),
        tracing=getattr(_lazy_ops.my_project_ops, op_or_container_name),
    )


_init_ops_and_containers()


lazy = _eager_only(_lazy_ops.my_project_ops)
eager = _same_when_tracing(_py_types.ModuleType('eager'))


__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__


# `eager` has eager versions of everything, available even in tracing mode.
def _set_up_eager():
  for name in __all__:
    if name != 'eager':
      setattr(eager, name, globals()[name])
  eager.__all__ = [x for x in __all__ if x != 'eager']
  eager.__dir__ = lambda: eager.__all__


_set_up_eager()

# Set up the tracing mode machinery. This must be the last thing in this file.
if not _typing.TYPE_CHECKING:
  _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
      _sys.modules[__name__], _tracing_config
  )
```

Importing `my_project` allows the operators to be accessed through
`my_project.cond` for a dispatching version, `my_project.eager.cond` for an
eager version and `my_project.lazy.cond` for a lazy version.

## Making Operators Available in C++

In order to evaluate Expressions from C++, the operator definitions created in
Python must be made available in C++, which is done through creating operator
snapshots and adding relevant build dependencies. Additionally, the
`arolla_cc_operator_package` must depend on the `cc_library` build targets for
all relevant backend operators defined in C++ - in our case the one implementing
`apply_mask`. See
[implementation](https://github.com/google/koladata/blob/main//py/koladata/operators/BUILD)
for real examples of this. The following is a simplified example:

```
arolla_cc_operator_package(
    name = "cc_operators",
    arolla_initializer = {
        "name": "arolla_operators/my_project",
        "deps": ["arolla_operators/koda_base"],
    },
    snapshot = ":operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [
      ":operators",  # Dependency on the C++ operator definition of apply_mask.
      "//py/koladata:cc_operators",
    ],
)


arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = [
        "my.namespace.lazy_ops",
    ],
    preimports = ["koladata.kd"],
    visibility = ["//visibility:private"],
    deps = [
        ":lazy_ops",
        "//py/koladata:kd",
    ],
)
```

The `cc_operators` build target should be added as a dependency to the C++
library that evaluates the Expressions.
