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
from typing import Any, Callable, Iterable

from arolla import arolla
from arolla.jagged_shape import jagged_shape as arolla_jagged_shape
from koladata.operators import core
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


def _expect_py_callable(param):
  """Returns a constraint that the argument is a python callable."""
  return (
      (param == arolla.abc.PY_OBJECT),
      (
          'expected a python callable, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
  )


def _expect_optional_py_callable(param):
  """Returns a constraint that the argument is a python callable or None.

  Important: The constraint `_expect_optional_py_callable(P.fn)` should be
  complemented with runtime checks:

    isinstance(fn, arolla.abc.PyObject)  # `fn` is a PY_OBJECT, presumably
                                         # holding a Python callable.

    (isinstance(fn, data_item.DataItem) and
     fn.get_schema() == schema_constants.NONE)  # `fn` is None.

  Args:
    param: A placeholder expr-node with the parameter name.

  Returns;
    A qtype constraint.
  """

  return (
      (param == arolla.abc.PY_OBJECT) | (param == qtypes.DATA_SLICE),
      (
          'expected a python callable, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
  )


@optools.add_to_registry(aliases=['kde.apply_py'])
@optools.as_lambda_operator(
    'kde.py.apply_py',
    qtype_constraints=[_expect_py_callable(P.fn)],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py(
    fn,
    args=py_boxing.var_positional(),
    return_type_as=py_boxing.keyword_only(data_slice.DataSlice),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
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

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py._impl',
      qtype_inference_expr=P.return_type_as,
  )
  def impl(fn, args, return_type_as, kwargs):
    del return_type_as  # Only used for type inference.
    fn = fn.py_value()
    return py_boxing.as_qvalue(fn(*args, **kwargs.as_dict()))

  return impl(fn, args, return_type_as, kwargs)


@optools.add_to_registry(aliases=['kde.apply_py_on_cond'])
@optools.as_lambda_operator(
    'kde.py.apply_py_on_cond',
    qtype_constraints=[
        _expect_py_callable(P.yes_fn),
        _expect_optional_py_callable(P.no_fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py_on_cond(
    yes_fn,
    no_fn,
    cond,
    args=py_boxing.var_positional(),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
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

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py_on_cond._impl', qtype_inference_expr=qtypes.DATA_SLICE
  )
  def impl(yes_fn, no_fn, cond, args, kwargs):
    yes_fn = yes_fn.py_value()

    if isinstance(no_fn, arolla.abc.PyObject):
      no_fn = no_fn.py_value()
    elif (
        isinstance(no_fn, data_item.DataItem)
        and no_fn.get_schema() == schema_constants.NONE
    ):
      no_fn = None
    else:
      raise TypeError(f'expected a python callable, got no_fn: {no_fn.qtype}')

    args = tuple(args)
    kwargs = kwargs.as_dict()

    result = yes_fn(
        *(x & cond for x in args), **{k: v & cond for k, v in kwargs.items()}
    )
    if no_fn is not None:
      inv_cond = ~cond
      result = result | no_fn(
          *(x & inv_cond for x in args),
          **{k: v & inv_cond for k, v in kwargs.items()},
      )
    return result

  return impl(yes_fn, no_fn, cond, args, kwargs)


@optools.add_to_registry(aliases=['kde.apply_py_on_selected'])
@optools.as_lambda_operator(
    'kde.py.apply_py_on_selected',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def apply_py_on_selected(
    fn,
    cond,
    args=py_boxing.var_positional(),
    kwargs=py_boxing.var_keyword(),
):
  # pylint: disable=g-doc-args  # *args, **kwargs
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

  @arolla.optools.as_py_function_operator(
      'kde.py.apply_py_on_selected._impl',
      qtype_inference_expr=qtypes.DATA_SLICE,
  )
  def impl(fn, cond, args, kwargs):
    fn = fn.py_value()
    return fn(
        *(x & cond for x in args),
        **{k: v & cond for k, v in kwargs.as_dict().items()},
    )

  return impl(fn, cond, args, kwargs)


## py.map_py* operators ##


def _basic_map_py(
    *,
    map_fn: Callable[..., Iterable[Any]],
    fn: Callable[..., Any],
    args: tuple[data_slice.DataSlice, ...],
    schema: data_slice.DataSlice | None,
    ndim: int,
):
  """A basic_map_py() utility.

  This utility function implements only the core functionality; the missing
  features will be built on top of it.

  Args:
    map_fn: A generalisation of the map(...) function.
    fn: A python callable that implements the computation.
    args: Input data-slices.
    schema: The schema to use for resulting data-slice.
    ndim: Dimensionality of items to pass to `fn`.

  Returns:
    Result DataSlice.
  """
  args = core.align._eval(*args)  # pylint: disable=protected-access
  dims = args[0].get_shape().edges()
  max_ndim = len(dims)

  if ndim < 0 or ndim > max_ndim:
    raise ValueError(f'ndim should be between 0 and {max_ndim}, got {ndim=}')

  arg_lists = []
  for arg in args:
    arg_lists.append(arg.flatten(0, max_ndim - ndim).internal_as_py())

  result = list(map_fn(fn, *arg_lists))
  bag = data_bag.DataBag.empty()
  from_py_schema = (
      schema_constants.OBJECT if schema is None else bag.list_schema(schema)
  )
  # TODO: b/323305977 - Use .from_py(..., from_dim=1) instead of the manual list
  # explosion when available.
  result = bag._from_py_impl(  # pylint: disable=protected-access
      result,
      False,  # dict_as_obj=
      None,  # itemid=
      from_py_schema,  # schema=
      0,  # from_dim=,
  )[:]
  shape = arolla_jagged_shape.JaggedDenseArrayShape.from_edges(
      *dims[: max_ndim - ndim]
  )
  return result.reshape(shape)


def _map_py(
    fn: Callable[..., Any],
    *args: data_slice.DataSlice,
    schema: data_slice.DataSlice | None,
    max_threads: int,
    ndim: int,
    item_completed_callback: Callable[[Any], None],
    **kwargs: data_slice.DataSlice,
):
  """A feature complete map_py() utility.

  This utility implements support for kwargs, multi-threading,
  item_completed_callback.

  Args:
    fn: A python callable that implements the computation.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: Maximum number of threads to use.
    ndim: Dimensionality of items to pass to `fn`.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called `map_py`
      in case `max_threads` is greater than 1, as we rely on this property for
      cases like progress reporting. As such, it can not be attached to the `fn`
      itself.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """
  if not args and not kwargs:
    raise TypeError('expected at least one input DataSlice, got none')

  def map_fn(task_fn, *iterables):
    if max_threads <= 1:
      for item in map(task_fn, *iterables):
        item_completed_callback(item)
        yield item
    else:
      executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)
      try:
        future_to_idx = {
            executor.submit(task_fn, *task_args): idx
            for idx, task_args in enumerate(zip(*iterables))
        }
        result = [None] * len(future_to_idx)
        for future in concurrent.futures.as_completed(future_to_idx):
          idx = future_to_idx[future]
          item = future.result()
          result[idx] = item
          item_completed_callback(item)
      finally:
        # we do not use `with` syntax to pass non default arguments
        executor.shutdown(wait=False, cancel_futures=True)
      yield from result

  task_kwnames = tuple(kwargs.keys())
  vcall = arolla.abc.vectorcall
  return _basic_map_py(
      map_fn=map_fn,
      fn=lambda *task_args: vcall(fn, *task_args, task_kwnames),
      args=(*args, *kwargs.values()),
      schema=schema,
      ndim=ndim,
  )


# TODO: b/365026427 - Add a reference to kd.py_cloudpickle in the docstring.
# TODO: b/370978592 - Consider implementing this operator using kdf.map
#   combined with kd.py_fn, especially if the performance is comparable.
@optools.add_to_registry(aliases=['kde.map_py'])
@optools.as_lambda_operator(
    'kde.py.map_py',
    qtype_constraints=[
        _expect_py_callable(P.fn),
        _expect_optional_py_callable(P.item_completed_callback),
        qtype_utils.expect_data_slice(P.schema),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
)
def map_py(
    fn,
    args=py_boxing.var_positional(),
    schema=py_boxing.keyword_only(None),
    max_threads=py_boxing.keyword_only(1),
    ndim=py_boxing.keyword_only(0),
    item_completed_callback=py_boxing.keyword_only(None),
    kwargs=py_boxing.var_keyword(),
):  # pylint: disable=g-doc-args
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


  Args:
    fn: function.
    *args: Input DataSlices.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    ndim: Dimensionality of items to pass to `fn`.
    item_completed_callback: A callback that will be called after each item is
      processed. It will be called in the original thread that called `map_py`
      in case `max_threads` is greater than 1, as we rely on this property for
      cases like progress reporting. As such, it can not be attached to the `fn`
      itself.
    **kwargs: Input DataSlices.

  Returns:
    Result DataSlice.
  """

  @arolla.optools.as_py_function_operator(
      'kde.py.map_py._impl', qtype_inference_expr=qtypes.DATA_SLICE
  )
  def impl(
      fn, args, schema, ndim, max_threads, item_completed_callback, kwargs
  ):
    if schema.get_ndim() != 0:
      raise ValueError('`schema` can be only 0-dim schema slice')
    if schema.get_schema() != schema_constants.SCHEMA:
      if schema.get_schema() != schema_constants.NONE:
        raise ValueError(
            f'expected a schema, got schema: {schema.get_schema()}'
        )
      schema = None
    if isinstance(item_completed_callback, arolla.abc.PyObject):
      item_completed_callback = item_completed_callback.py_value()
    elif (
        isinstance(item_completed_callback, data_item.DataItem)
        and item_completed_callback.get_schema() == schema_constants.NONE
    ):
      item_completed_callback = lambda _: None
    else:
      raise ValueError(
          f'expected a python callable, got {item_completed_callback=!r}'
      )
    return _map_py(
        fn.py_value(),
        *args,
        **kwargs.as_dict(),
        schema=schema,
        ndim=int(ndim),
        max_threads=int(max_threads),
        item_completed_callback=item_completed_callback,
    )

  return impl(
      fn, args, schema, ndim, max_threads, item_completed_callback, kwargs
  )
