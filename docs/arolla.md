# Overview of Arolla usage in Koda

This page describes how Koda is built on top of
[Arolla](https://github.com/google/arolla/tree/main/arolla/README.md). The goal is to map out which
pieces come from Arolla and which are Koda's own, so that developers and
advanced users know where to look — and what to change — when working on each
part of the system.

* TOC
{:toc}

## What is Arolla?

[Arolla](https://github.com/google/arolla/tree/main/arolla/README.md) is a framework for creating and
manipulating custom expressions — computational graphs (DAGs) composed of
operators. These expressions can be composed, explored, and modified
interactively in Python, and then loaded in C++ for efficient evaluation.

The key components used by Koda are:

*   **QType** and **QValue**: Arolla has an extensible type system described by
    `QType` objects (an analogue to `std::type_info` in C++). Values in Arolla
    are stored in `QValue` (`TypedValue` and `TypedRef` in C++) containers (an
    analogue to `std::any` in C++). Each value used in Arolla has a QType. See
    [QType library](https://github.com/google/arolla/tree/main/arolla/qtype/README.md) for details.
*   **Expressions**: Immutable directed acyclic graphs built from operators and
    leaf/literal/placeholder nodes. See
    [Arolla Expressions](https://github.com/google/arolla/tree/main/arolla/expr/README.md).
*   **Operators**: Defined at two levels — high-level

## How Koda uses Arolla

Koda is built entirely on top of Arolla. This section describes the key ways
Koda extends and customizes the framework.

### Koda Expressions

Koda Expressions are
[Arolla Expressions](https://github.com/google/arolla/tree/main/arolla/expr/expr_node.h) built of Koda
operators and operating on Koda-specific QTypes.

An important API difference from Arolla is that Koda operators are **eager** by
default: `kd.math.add(x, y)` immediately evaluates and returns a `DataSlice`,
not an expression (unlike `arolla.M.math.add(x, y)`). Expressions are
constructed either during automated tracing (see
[Functors and Tracing](functors.md)) or by using the `kd.lazy` namespace
explicitly (e.g. `kd.lazy.math.add`). The `KodaView` (see below) makes
expression nodes look and feel like DataSlices so that the same syntax works in
both eager and tracing modes.

Arolla expressions use `L.x` (leaf) and `P.x` (placeholder) as entry points for
data or subexpression substitution. Because Koda expressions are usually packed
into functors (see below), it uses a different notion:

*   `I.x` — functor/expression **input**, analogous to `arolla.L.x`. Represents
    data provided at evaluation time.
*   `V.x` — functor **variable**, a local named sub-expression within a functor.
    Has no Arolla equivalent — this is a Koda-level concept for structuring
    functor computation.
*   `S.x` — shortcut for `I.self.x`, used for the positional `self` argument in
    `kd.eval`.

In Koda code, always use `I`/`V`/`S`. Arolla's `P` should only appear in
low-level operator QType constraint definitions (e.g.
`expect_data_slice(arolla.P.x)`), and `L` is only used by Koda internally, e.g.
for defining non-deterministic operators (see below).

#### KodaView

Arolla provides an `ExprView` mechanism that attaches methods and properties to
expression nodes based on their operator or output QType. Koda uses this to
register a `KodaView` for all the nodes with a Koda operator (and also for the
nodes with deduced `DATA_SLICE`/`DATA_BAG`/`JAGGED_SHAPE` QTypes). `KodaView`
mimics combined APIs of `DataSlice` and `DataBag` (e.g. `expr.x`, `expr +
other`). This ensures a smooth transition between eager and tracing modes of the
same Python code.

#### Functors and Tracing

Koda introduces the concept of **Functors** — callable DataItems that store a
computation graph of one or several Koda Exprs — and a **tracing** mechanism
that converts Python functions into Functors. These are purely Koda-level
concepts; Arolla itself has no notion of functors. Arolla utilizes a mechanism
similar to tracing e.g. for lambda operators definition, but there is no
mechanism to provide smooth transition between eager and lazy modes.

See
[Technical Deep Dive: Deferred Evaluation, Tracing, and Functors](functors.md)
for more details.

### Custom QTypes

Koda defines its own QTypes in `kd.qtypes`:

*   `DATA_SLICE` — the QType for all `DataSlice` (and `DataItem`) values.
*   `DATA_BAG` — the QType for `DataBag` values.
*   `JAGGED_SHAPE` — the QType for `JaggedShape` values. Note that it is derived
    from Arolla's `JAGGED_SHAPE` QType in order to wire it with `KodaView` (see
    below).

Koda also reuses Arolla's `TUPLE` and `NAMEDTUPLE` QTypes, but only as
containers for the Koda QTypes above (e.g. a tuple of DataSlices). Arolla's
`UNSPECIFIED` QType is sometimes used for default arguments of operators, in
cases when static default argument resolution is needed.

QTypes in Arolla are deduced statically, before expression evaluation.
Critically, all `DataSlice` instances share the same `DATA_SLICE` QType
regardless of their Koda Schema (e.g. whether they hold `INT32`, `STRING`, or
entities). The Koda Schema is a runtime property, not encoded in the QType. See
[QTypes and Koda Schemas](functors.md#qtypes-and-koda-schemas) for a detailed
discussion.

### Custom Operator Library

Koda provides a fully custom set of operators under `kd.*` (and `kd.lazy.*` for
lazy versions). Standard Arolla operators (e.g. `arolla.M.math.add`,
`arolla.M.core.*`) are **not** used in Koda code. Instead, Koda operators work
with `DATA_SLICE`, `DATA_BAG`, and `JAGGED_SHAPE` QTypes and handle
Koda-specific concepts like schemas, sparsity, and DataBag management.

See
[Creating Koda Operators](koladata/g3doc/creating_operators.md)
for a detailed guide.

### Koda optools

Koda operators are defined using `kd.optools` — a wrapper around
`arolla.optools` that adds Koda-specific functionality to every operator
decorator (`as_lambda_operator`, `as_backend_operator`,
`as_py_function_operator`). On top of what Arolla provides, `kd.optools` wires
in the unified binding policy (see below), determinism control, custom
per-parameter boxing, per-operator repr functions, `ExprView` registration, C++
operator package support (`via_cc_operator_package`), operator containers for
eager/lazy dispatching (`make_operators_container`), and operator aliases.

#### Operator packages

In Arolla, Expr operators are usually defined in Python, then built into
`arolla_cc_operator_package` which is then used from both C++ and Python
targets. The *.py files defining the operators are only used for building
`arolla_cc_operator_package` and **never imported in runtime**. But Koda Expr
operators more frequently require additional Python-specific components (e.g.
custom `repr_fn`, custom binding policy). This makes standard Arolla operator
package structure insufficient.

Instead each Koda operator package consists of two parts:
`koladata_cc_operator_package` library that provides `Operator` objects for both
C++ (serving) and Python environments and an additional `py_library` that
imports all the *.py files defining the operators to guarantee registration of
the Python-specific operator details.

Consequently, the *.py files with operators are used in two ways: from blaze
during `koladata_cc_operator_package` building, and from Python during module
(`kd`, `kd_ext`, ...) import. The operators marked with
`via_cc_operator_package=True` are registered only in the "build" mode, and are
retrieved from the compiled-in operator registry during normal runtime. In order
to distinguish between the two modes `kd.optools` introduces
`building_cc_operator_package` context manager.

Additionally Koda requires more machinery for eager / tracing dispatching, which
is currently implemented in each operator package separately (kd.py, kd_ext.py,
etc.).

See
[Creating Koda Operators](koladata/g3doc/creating_operators.md)
for a detailed guide.

### Determinism and caching

Arolla's evaluation engine caches operator results by default, assuming
deterministic output for the same inputs. Standard Arolla operators are always
deterministic and have no mechanism to opt out. Koda introduces the
`deterministic=False` flag for operators that are non-deterministic (e.g. random
number generation) to prevent incorrect caching:

```py
@kd.optools.as_py_function_operator('my_project.random_op', deterministic=False)
def random_op(x):
  ...
```

### Unified Binding Policy

Arolla's standard binding rules only support positional-or-keyword parameters.
Koda introduces a **unified binding policy** that extends this with:

*   **Positional-only parameters** — parameters that cannot be passed by name.
*   **Keyword-only parameters** — parameters that must be passed by name.
*   **Variadic positional (`*args`)** — automatically packed into an Arolla
    `tuple`.
*   **Variadic keyword (`**kwargs`)** — automatically packed into an Arolla
    `namedtuple`.
*   **Custom per-parameter boxing** — automatic conversion of Python values to
    Koda DataItems, with configurable boxing functions per parameter.
*   **Non-deterministic marking** — a hidden parameter that signals to the
    evaluation engine that operator results should not be cached (see above).

### The `return_type_as` pattern

Because Arolla requires static type deduction for all expression nodes, and
Koda's Functor output types are not known at tracing time (since a Functor is
just a `DataItem` with an opaque `DATA_SLICE` QType), some Koda operations need
a `return_type_as` argument to tell the type system what the output QType will
be.

This is most relevant for `kd.call` and `kd.fn`:

```py
fn = kd.fn(lambda x: x + 1)

# Without return_type_as, the output is assumed to be DATA_SLICE.
result = kd.call(fn, kd.item(1))

# With return_type_as, the output QType is inferred from the provided value:
result = kd.call(fn, kd.item(1), return_type_as=kd.types.DataBag)
```

This pattern is also used in `kd.trace_as_fn(return_type_as=...)` and
`kd.functor.bind`. See
[Unavailable Information During Tracing](functors.md#unavailable-information-during-tracing)
for more context on why this is necessary.
