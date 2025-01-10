# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""py.* operators."""

import concurrent.futures
import functools
import itertools
from typing import Any, Callable, Iterable

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as _
from koladata.operators import slices as _
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


P = arolla.P
M = arolla.M
constraints = arolla.optools.constraints
eval_op = py_expr_eval_py_ext.eval_op


def _expect_py_callable(
    param: constraints.Placeholder,
) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a python callable.

  Important: The constraint `_expect_py_callable(P.fn)` is intended to be used
  in pair with `_unwrap_py_callable(fn, param_name='fn')`, which performs
  the necessary runtime checks and returns a python object.

  Args:
      param: A placeholder with the parameter name.

  Returns:
      A qtype constraint.
  """
  return (
      param == arolla.abc.PY_OBJECT,
      f'expected a python callable, got {constraints.name_type_msg(param)}',
  )


def _expect_optional_py_callable(
    param: constraints.Placeholder,
) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is an python callable or None.

  Important: The constraint `_expect_optional_py_callable(P.fn)` is intended to
  be used in pair with `_unwrap_optional_py_callable(fn, param_name='fn')`,
  which performs the necessary runtime checks and returns a python object or
  `None`.

  Args:
      param: A placeholder with the parameter name.

  Returns:
      A qtype constraint.
  """
  return (
      (param == arolla.abc.PY_OBJECT) | (param == qtypes.DATA_SLICE),
      f'expected a python callable, got {constraints.name_type_msg(param)}',
  )


def _expect_optional_schema(
    param: constraints.Placeholder,
) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a schema or None.

  Important: The constraint `_expect_optional_py_callable(P.schema)` is intended
  to be used with `_unwrap_optional_schema(schema, param_name='schema')`,
  which performs the necessary runtime checks and returns a schema or `None`.

  Args:
      param: A placeholder with the parameter name.

  Returns:
      A qtype constraint.
  """
  return (
      param == qtypes.DATA_SLICE,
      f'expected a schema, got {constraints.name_type_msg(param)}',
  )


def _expect_scalar_integer(
    param: constraints.Placeholder,
) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is an integer scalar.

  Important: The constraint `_expect_scalar_integer(P.schema)` is intended
  to be used in pair with `_unwrap_scalar_integer(schema, param_name='schema')`,
  which performs the necessary runtime checks and returns a python `int`.

  Args:
      param: A placeholder with the parameter name.

  Returns:
      A qtype constraint.
  """
  return (
      param == qtypes.DATA_SLICE,
      f'expected a scalar integer, got {constraints.name_type_msg(param)}',
  )


def _expect_optional_boolean(
    param: constraints.Placeholder,
) -> constraints.QTypeConstraint:
  """Returns a constraint that the argument is a scalar boolean or None.

  Important: The constraint `_expect_optional_boolean(P.flag)` is
  intended to be used with `_unwrap_optional_boolean(flag, param_name='flag')`,
  which performs the necessary runtime checks and returns a python `bool` or
  `None`.

  Args:
      param: A placeholder with the parameter name.

  Returns:
      A qtype constraint.
  """
  return (
      (param == qtypes.DATA_SLICE) | (param == arolla.UNSPECIFIED),
      f'expected a scalar boolean, got {constraints.name_type_msg(param)}',
  )


# Note: Intended to be used in pair with _expect_py_callable().
def _unwrap_py_callable(
    value: arolla.abc.PyObject, *, param_name: str
) -> Callable[..., Any]:
  """Returns a python callable stored in the parameter."""
  if isinstance(value, arolla.abc.PyObject):
    result = value.py_value()
    if callable(result):
      return result
  raise ValueError(f'expected a python callable, got {param_name}={value!r}')


# Note: Intended to be used in pair with _expect_optional_py_callable().
def _unwrap_optional_py_callable(
    value: arolla.abc.PyObject | data_slice.DataSlice, *, param_name: str
) -> Callable[..., Any] | None:
  """Returns a python callable or None stored in the parameter."""
  if isinstance(value, arolla.abc.PyObject):
    result = value.py_value()
    if callable(result):
      return result
  elif (
      isinstance(value, data_item.DataItem)
      and value.get_schema() == schema_constants.NONE
  ):
    return None
  raise ValueError(f'expected a python callable, got {param_name}={value!r}')


# Note: Intended to be used in pair with _expect_optional_schema().
def _unwrap_optional_schema(
    value: data_slice.DataSlice, *, param_name: str
) -> data_item.DataItem | None:
  """Returns a schema or none stored in the parameter."""
  if isinstance(value, data_item.DataItem):
    value_schema = value.get_schema()
    if value_schema == schema_constants.SCHEMA:
      return value
    if value_schema == schema_constants.NONE:
      return None
  raise ValueError(f'expected a schema, got {param_name}={value}')


# Note: Intended to be used in pair with _expect_scalar_integer().
def _unwrap_scalar_integer(
    value: data_slice.DataSlice, *, param_name: str
) -> int:
  """Returns a python scalar integer stored in the parameter."""
  if isinstance(value, data_item.DataItem):
    try:
      return int(
          eval_op('kde.schema.cast_to_narrow', value, schema_constants.INT64)
      )
    except ValueError:
      pass
  raise ValueError(f'expected a scalar integer, got {param_name}={value}')


# Note: Intended to be used in pair with _expect_optional_boolean().
def _unwrap_optional_boolean(
    value: data_slice.DataSlice, *, param_name: str
) -> bool:
  """Returns a boolean none stored in the parameter."""
  if isinstance(value, data_item.DataItem):
    try:
      return eval_op(
          'kde.schema.cast_to_narrow', value, schema_constants.BOOLEAN
      ).to_py()
    except ValueError:
      pass
  raise ValueError(f'expected a scalar boolean, got {param_name}={value}')


#
# kde.py.apply_py* operators
#


@optools.add_to_registry(aliases=['kde.apply_py'])
@optools.as_py_function_operator(
    'kde.py.apply_py',
    qtype_constraints=[_expect_py_callable(P.fn)],
    qtype_inference_expr=arolla.M.qtype.conditional_qtype(
        P.return_type_as == arolla.UNSPECIFIED,
        qtypes.DATA_SLICE,
        P.return_type_as,
    ),
)
def apply_py(fn, *args, return_type_as=arolla.unspecified(), **kwargs):
  """Applies Python function `fn` on args.

  It is equivalent to fn(*args, **kwargs).

  Args:
    fn: function to apply to `*args` and `**kwargs`. It is required that this
      function returns a DataSlice/DataItem or a primitive that will be
      automatically wrapped into a DataItem.
    *args: positional arguments to pass to `fn`.
    return_type_as: The return type of the function is expected to be the same
      as the return type of this expression. In most cases, this will be a
      literal of the corresponding type. This needs to be specified if the
      function does not return a DataSlice/DataItem or a primitive that would be
      auto-boxed into a DataItem. kd.types.DataSlice and kd.types.DataBag can
      also be passed here.
    **kwargs: keyword arguments to pass to `fn`.

  Returns:
    Result of fn applied on the arguments.
  """
  fn = _unwrap_py_callable(fn, param_name='fn')
  result = py_boxing.as_qvalue(fn(*args, **kwargs))
  if return_type_as.qtype == arolla.UNSPECIFIED and not isinstance(
      result, data_slice.DataSlice
  ):
    raise ValueError(
        f'expected the result to have qtype DATA_SLICE, got {result.qtype};'
        ' consider specifying the `return_type_as=` parameter'
    )
  return result


@optools.add_to_registry(aliases=['kde.apply_py_on_cond'])
@optools.as_py_function_operator(
    'kde.py.apply_py_on_cond',
    qtype_constraints=[
        _expect_py_callable(P.yes_fn),
        _expect_optional_py_callable(P.no_fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def apply_py_on_cond(yes_fn, no_fn, cond, *args, **kwargs):
  """Applies Python functions on args filtered with `cond` and `~cond`.

  It is equivalent to

    yes_fn(
        *( x & cond for x in args ),
        **{ k: (v & cond) for k, v in kwargs.items() },
    ) | no_fn(
        *( x & ~cond for x in args ),
        **{ k: (v & ~cond) for k, v in kwargs.items() },
    )

  Args:
    yes_fn: function to apply on filtered args.
    no_fn: function to apply on inverse filtered args (this parameter can be
      None).
    cond: filter dataslice.
    *args: arguments to filter and then pass to yes_fn and no_fn.
    **kwargs: keyword arguments to filter and then pass to yes_fn and no_fn.

  Returns:
    The union of results of yes_fn and no_fn applied on filtered args.
  """
  yes_fn = _unwrap_py_callable(yes_fn, param_name='yes_fn')
  no_fn = _unwrap_optional_py_callable(no_fn, param_name='no_fn')
  result = py_boxing.as_qvalue(
      yes_fn(
          *(x & cond for x in args),
          **{k: v & cond for k, v in kwargs.items()},
      )
  )
  if no_fn is not None:
    inv_cond = ~cond
    result = result | py_boxing.as_qvalue(
        no_fn(
            *(x & inv_cond for x in args),
            **{k: v & inv_cond for k, v in kwargs.items()},
        )
    )
  return result


@optools.add_to_registry(aliases=['kde.apply_py_on_selected'])
@optools.as_lambda_operator(
    'kde.py.apply_py_on_selected',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def apply_py_on_selected(fn, cond, *args, **kwargs):
  """Applies Python function `fn` on args filtered with cond.

  It is equivalent to

    fn(
        *( x & cond for x in args ),
        **{ k: (v & cond) for k, v in kwargs.items() },
    )

  Args:
    fn: function to apply on filtered args.
    cond: filter dataslice.
    *args: arguments to filter and then pass to fn.
    **kwargs: keyword arguments to filter and then pass to fn.

  Returns:
    Result of fn applied on filtered args.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return arolla.abc.bind_op(
      apply_py_on_cond, fn, py_boxing.as_qvalue(None), cond, args, kwargs
  )


#
# kde.py.map_py* operators
#


def _from_py(py_obj, *, schema, from_dim):
  return data_slice.DataSlice._from_py_impl(  # pylint: disable=protected-access
      py_obj,
      False,  # dict_as_obj=
      None,  # itemid=
      schema,
      from_dim,
  )


def _parallel_map(
    fn: Callable[..., Any],
    *iterables: Iterable[Any],
    max_threads: int,
    item_completed_callback: Callable[[Any], None] | None,
) -> list[Any]:
  """A generalized `map(...)` function with multithreading support.

  Args:
    fn: The function to apply.
    *iterables: Input iterables to be processed.
    max_threads: The maximum number of threads to use.
    item_completed_callback: A callback invoked after each item is processed. It
      will be called in the original thread that invoked `map_py`.

  Returns:
    A list containing the results of the function calls.
  """
  if not item_completed_callback:
    item_completed_callback = type  # use a cheap callable as a stub
  if max_threads <= 1:  # Single-thread mode
    result = []
    for item in map(fn, *iterables):
      result.append(item)
      item_completed_callback(item)
    return result

  # Multi-thread mode
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
  try:
    future_to_idx = {
        executor.submit(fn, *args): idx
        for idx, args in enumerate(zip(*iterables))
    }
    result = [None] * len(future_to_idx)
    for future in concurrent.futures.as_completed(future_to_idx):
      idx = future_to_idx[future]
      item = future.result()
      result[idx] = item
      item_completed_callback(item)
    return result
  finally:
    # we do not use `with` syntax to pass non default arguments
    executor.shutdown(wait=False, cancel_futures=True)


def _basic_map_py(
    fn: Callable[..., Any],
    arg0: data_slice.DataSlice,
    *args: data_slice.DataSlice,
    schema: data_slice.DataSlice | None,
    ndim: int,
    max_threads: int,
    item_completed_callback: Callable[[Any], None] | None,
):
  """A basic_map_py() utility.

  This utility function implements only the core functionality; the missing
  features will be built on top of it.

  Args:
    fn: A python callable that implements the computation.
    arg0: The first input DataSlice.
    *args: The remaining input DataSlices; all input DataSlices must be aligned.
    schema: The schema for the resulting DataSlice.
    ndim: Dimensionality of items to pass to `fn`.
    max_threads: Maximum number of threads to use.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called `map_py`
      in case `max_threads` is greater than 1, as we rely on this property for
      cases like progress reporting. As such, it can not be attached to the `fn`
      itself.

  Returns:
    The resulting DataSlice.
  """
  shape = arg0.get_shape()
  shape_rank = shape.rank()
  if ndim < 0 or ndim > shape_rank:
    raise ValueError(f'ndim should be between 0 and {shape_rank}, got {ndim=}')
  result = _parallel_map(
      fn,
      arg0.flatten(0, shape_rank - ndim).internal_as_py(),
      *(arg.flatten(0, shape_rank - ndim).internal_as_py() for arg in args),
      max_threads=max_threads,
      item_completed_callback=item_completed_callback,
  )
  result = _from_py(result, schema=schema, from_dim=1)
  return result.reshape(shape[: shape_rank - ndim])


# TODO: b/370978592 - Consider implementing this operator using kdf.map
#   combined with kd.py_fn, especially if the performance is comparable.
@optools.add_to_registry(aliases=['kde.map_py'])
@optools.as_py_function_operator(
    'kde.py.map_py',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        _expect_optional_py_callable(P.item_completed_callback),
        qtype_utils.expect_data_slice_args(P.args),
        _expect_optional_schema(P.schema),
        _expect_scalar_integer(P.max_threads),
        _expect_scalar_integer(P.ndim),
        _expect_optional_boolean(P.include_missing),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def map_py(
    fn,
    *args,
    schema=None,
    max_threads=1,
    ndim=0,
    include_missing=None,
    item_completed_callback=None,
    **kwargs,
):
  """Apply the python function `fn` on provided `args` and `kwargs`.

  Example:
    def my_fn(x, y):
      if x is None or y is None:
        return None
      return x * y

    kd.map_py(my_fn, slice_1, slice_2)
    # Via keyword
    kd.map_py(my_fn, x=slice_1, y=slice_2)

  All DataSlices in `args` and `kwargs` must have compatible shapes.

  Lambdas also work for object inputs/outputs.
  In this case, objects are wrapped as DataSlices.
  For example:
    def my_fn_object_inputs(x):
      return x.y + x.z

    def my_fn_object_outputs(x):
      return db.obj(x=1, y=2) if x.z > 3 else db.obj(x=2, y=1)

  The `ndim` argument controls how many dimensions should be passed to `fn` in
  each call. If `ndim = 0` then `0`-dimensional values will be passed, if
  `ndim = 1` then python `list`s will be passed, if `ndim = 2` then lists of
  python `list`s will be passed and so on.

  `0`-dimensional (non-`list`) values passed to `fn` are either python
  primitives (`float`, `int`, `str`, etc.) or single-valued `DataSlices`
  containing `ItemId`s in the non-primitive case.

  In this way, `ndim` can be used for aggregation.
  For example:
    def my_agg_count(x):
      return len([i for i in x if i is not None])

    kd.map_py(my_agg_count, data_slice, ndim=1)

  `fn` may return any objects that kd.from_py can handle, in other words
  primitives, lists, dicts and dataslices. They will be converted to
  the corresponding Koda data structures.

  For example:
    def my_expansion(x):
      return [[y, y] for y in x]

    res = kd.map_py(my_expansion, data_slice, ndim=1)
    # Each item of res is a list of lists, so we can get a slice with
    # the inner items like this:
    print(res[:][:])

  It's also possible to set custom serialization for the fn (i.e. if you want to
  serialize the expression and later deserialize it in the different process).

  For example to serialize the function using cloudpickle you can use
  `kd_ext.py_cloudpickle(fn)` instead of fn.

  Args:
    fn: Function.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    ndim: Dimensionality of items to pass to `fn`.
    include_missing: Specifies whether `fn` should be computed to the missing
      items. By default, the function is applied to all items including the
      missing. `include_missing=False` can only be used with `ndim=0`.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called `map_py`
      in case `max_threads` is greater than 1, as we rely on this property for
      cases like progress reporting. As such, it can not be attached to the `fn`
      itself.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """
  fn = _unwrap_py_callable(fn, param_name='fn')
  max_threads = _unwrap_scalar_integer(max_threads, param_name='max_threads')
  schema = _unwrap_optional_schema(schema, param_name='schema')
  ndim = _unwrap_scalar_integer(ndim, param_name='ndim')
  include_missing = _unwrap_optional_boolean(
      include_missing, param_name='include_missing'
  )
  if include_missing is None:
    include_missing = True
  if not include_missing and ndim != 0:
    raise ValueError('`include_missing=False` can only be used with `ndim=0`')
  if not args and not kwargs:
    raise TypeError('expected at least one input DataSlice, got none')
  args = eval_op('kde.slices.align', *args, *kwargs.values())
  presence_mask = None
  if not include_missing:
    presence_mask = functools.reduce(
        functools.partial(eval_op, 'kde.masking.mask_and'),
        map(
            functools.partial(eval_op, 'kde.masking.has'),
            itertools.chain(args, kwargs.values()),
        ),
    )
    if eval_op('kde.masking.all', presence_mask):
      presence_mask = None
  if presence_mask is not None:
    if not eval_op('kde.masking.any', presence_mask):
      # Note: We explicitly handle the case where there are no overlapping
      # present items in the inputs, however the inputs are non-empty.
      #
      # This is necessary because `_basic_map_py(...)` internally uses
      # `from_py(...)`, which assigns different schemas in the following cases:
      #
      #   from_py([None], from_dim=1, schema=None).get_schema() -> NONE
      #
      # and
      #
      #   from_py([], from_dim=1, schema=None).get_schema() -> OBJECT
      #
      # Importantly, the case with empty inputs involves no masking and is
      # handled by the common path.
      return _from_py(None, schema=schema, from_dim=0).expand_to(presence_mask)
    # Apply the presence_mask to the arguments so that masked values don't need
    # unboxing.
    args = map(
        lambda x: eval_op('kde.slices.select', x, presence_mask),
        args,
    )
  kwnames = tuple(kwargs.keys())
  vcall = arolla.abc.vectorcall
  result = _basic_map_py(
      lambda *task_args: vcall(fn, *task_args, kwnames),
      *args,
      schema=schema,
      ndim=ndim,
      max_threads=max_threads,
      item_completed_callback=_unwrap_optional_py_callable(
          item_completed_callback, param_name='item_completed_callback'
      ),
  )
  if presence_mask is not None:
    result = eval_op('kde.slices.inverse_select', result, presence_mask)
  return result


@optools.add_to_registry(aliases=['kde.map_py_on_cond'])
@optools.as_py_function_operator(
    'kde.py.map_py_on_cond',
    qtype_constraints=[
        _expect_py_callable(P.true_fn),
        _expect_optional_py_callable(P.false_fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        _expect_optional_schema(P.schema),
        _expect_scalar_integer(P.max_threads),
        _expect_optional_py_callable(P.item_completed_callback),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def map_py_on_cond(
    true_fn,
    false_fn,
    cond,
    *args,
    schema=None,
    max_threads=1,
    item_completed_callback=None,
    **kwargs,
):
  """Apply python functions on `args` and `kwargs` based on `cond`.

  `cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
  dimensions than `args` or `kwargs`.

  Also see kd.map_py().

  This function supports only pointwise, not aggregational, operations.
  `true_fn` is applied when `cond` is kd.present. Otherwise, `false_fn` is
  applied.

  Args:
    true_fn: Function.
    false_fn: Function.
    cond: Conditional DataSlice.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called
      `map_py_on_cond` in case `max_threads` is greater than 1, as we rely on
      this property for cases like progress reporting. As such, it can not be
      attached to the `true_fn` and `false_fn` themselves.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """
  true_fn = _unwrap_py_callable(true_fn, param_name='true_fn')
  false_fn = _unwrap_optional_py_callable(false_fn, param_name='false_fn')
  max_threads = _unwrap_scalar_integer(max_threads, param_name='max_threads')
  if not args and not kwargs:
    raise TypeError('expected at least one input DataSlice, got none')
  if cond.get_schema() != schema_constants.MASK:
    raise ValueError(f'expected a mask, got cond: {cond.get_schema()}')
  args = (*args, *kwargs.values())
  if cond.get_ndim() > max(arg.get_ndim() for arg in args):
    raise ValueError(
        "'cond' must have the same or smaller dimension than args + kwargs"
    )
  cond, *args = eval_op('kde.slices.align', cond, *args)
  kwnames = tuple(kwargs.keys())
  vcall = arolla.abc.vectorcall
  if false_fn is None:
    # Apply the cond mask to the arguments so that masked values don't need
    # unboxing.
    args = map(lambda x: eval_op('kde.masking.apply_mask', x, cond), args)
    task_fn = (
        lambda task_cond, *task_args: None
        if task_cond is None
        else vcall(true_fn, *task_args, kwnames)
    )
  else:
    task_fn = lambda task_cond, *task_args: vcall(
        false_fn if task_cond is None else true_fn, *task_args, kwnames
    )
  return _basic_map_py(
      task_fn,
      cond,
      *args,
      schema=_unwrap_optional_schema(schema, param_name='schema'),
      ndim=0,
      max_threads=max_threads,
      item_completed_callback=_unwrap_optional_py_callable(
          item_completed_callback, param_name='item_completed_callback'
      ),
  )


@optools.add_to_registry(aliases=['kde.map_py_on_selected'])
@optools.as_lambda_operator(
    'kde.py.map_py_on_selected',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        _expect_optional_schema(P.schema),
        _expect_optional_py_callable(P.item_completed_callback),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def map_py_on_selected(
    fn,
    cond,
    *args,
    schema=None,
    max_threads=1,
    item_completed_callback=None,
    **kwargs,
):
  """Apply python function `fn` on `args` and `kwargs` based on `cond`.

  `cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
  dimensions than `args` or `kwargs`.

  Also see kd.map_py().

  This function supports only pointwise, not aggregational, operations. `fn` is
  applied when `cond` is kd.present.

  Args:
    fn: Function.
    cond: Conditional DataSlice.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called
      `map_py_on_selected` in case `max_threads` is greater than 1, as we rely
      on this property for cases like progress reporting. As such, it can not be
      attached to the `fn` itself.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return arolla.abc.bind_op(
      map_py_on_cond,
      true_fn=fn,
      false_fn=data_slice.DataSlice.from_vals(None),
      cond=cond,
      args=args,
      schema=schema,
      max_threads=max_threads,
      item_completed_callback=item_completed_callback,
      kwargs=kwargs,
  )


@optools.add_to_registry(aliases=['kde.map_py_on_present'])
@optools.as_py_function_operator(
    'kde.py.map_py_on_present',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        qtype_utils.expect_data_slice_args(P.args),
        _expect_optional_schema(P.schema),
        _expect_optional_py_callable(P.item_completed_callback),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def map_py_on_present(
    fn,
    *args,
    schema=None,
    max_threads=1,
    item_completed_callback=None,
    **kwargs,
):
  """Apply python function `fn` to items present in all `args` and `kwargs`.

  Also see kd.map_py().

  Args:
    fn: function.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called
      `map_py_on_present` in case `max_threads` is greater than 1, as we rely on
      this property for cases like progress reporting. As such, it can not be
      attached to the `fn` itself.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """
  if not args and not kwargs:
    raise TypeError('expected at least one input DataSlice, got none')
  cond = functools.reduce(
      functools.partial(eval_op, 'kde.masking.mask_and'),
      map(
          functools.partial(eval_op, 'kde.masking.has'),
          itertools.chain(args, kwargs.values()),
      ),
  )
  return eval_op(
      map_py_on_selected,
      fn,
      cond,
      *args,
      schema=schema,
      max_threads=max_threads,
      item_completed_callback=item_completed_callback,
      **kwargs,
  )


@optools.add_to_registry(aliases=['kde.map'])
@optools.as_lambda_operator(
    'kde.functor.map',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fn),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def _map(fn, *args, **kwargs):
  """Aligns fn and args/kwargs and calls corresponding fn on corresponding arg.

  Current implentaion is a wrapper around kde.py.map_py_on_cond (Python
  based) so it might be slow and intended for experiments only.

  If certain items of fn are missing, the corresponding items of the result will
  be also missing.
  If certain items of args/kwars are missing we are still calling the functor
  on those missing args/kwargs.

  Example:
    fn1 = kdf.fn(lambda x, y: x + y)
    fn2 = kdf.fn(lambda x, y: x - y)
    fn = kd.slice([fn1, fn2])
    x = kd.slice([[1, None, 3], [4, 5, 6]])
    y = kd.slice(1)

    kd.map(kd.slice([fn1, fn2]), x=x, y=y)
    # returns kd.slice([[2, None, 4], [3, 4, 5]])

    kd.map(kd.slice([fn1, None]), x=x, y=y)
    # returns kd.slice([[2, None, 4], [None, None, None]])

  Args:
    fn: DataSlice containing the functor(s) to evaluate. All functors must
      return a DataItem.
    *args: The positional argument(s) to pass to the functions.
    **kwargs: The keyword argument(s) to pass to the functions.

  Returns:
    The evaluation result.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return arolla.abc.bind_op(
      map_py_on_selected,
      arolla.abc.PyObject(lambda _fn, *args, **kwargs: _fn(*args, **kwargs)),
      masking.has(fn),
      schema=data_slice.DataSlice.from_vals(None),
      item_completed_callback=py_boxing.as_qvalue(None),
      max_threads=py_boxing.as_qvalue(1),
      args=M.core.concat_tuples(M.core.make_tuple(fn), args),
      kwargs=kwargs,
  )
