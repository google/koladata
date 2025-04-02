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

"""Functor operators."""

from arolla import arolla
from koladata.operators import assertion
from koladata.operators import dicts
from koladata.operators import koda_internal_iterables
from koladata.operators import lists
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema
from koladata.operators import slices
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

P = arolla.P
M = arolla.M


@optools.add_to_registry(aliases=['kd.call'])
@optools.as_backend_operator(
    'kd.functor.call',
    qtype_inference_expr=P.return_type_as,
    qtype_constraints=[(
        P.fn == qtypes.DATA_SLICE,
        (
            'expected a functor DATA_SLICE, got'
            f' {arolla.optools.constraints.name_type_msg(P.fn)}'
        ),
    )],
    deterministic=False,
)
def call(fn, *args, return_type_as=data_slice.DataSlice, **kwargs):  # pylint: disable=unused-argument
  """Calls a functor.

  See the docstring of `kd.fn` on how to create a functor.

  Example:
    kd.call(kd.fn(I.x + I.y), x=2, y=3)
    # returns kd.item(5)

    kd.lazy.call(I.fn, x=2, y=3).eval(fn=kd.fn(I.x + I.y))
    # returns kd.item(5)

  Args:
    fn: The functor to be called, typically created via kd.fn().
    *args: The positional arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.
    return_type_as: The return type of the call is expected to be the same as
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also
      be passed here.
    **kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The result of the call.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.map'])
@optools.as_backend_operator(
    'kd.functor.map',
    qtype_constraints=[
        (
            P.fn == qtypes.DATA_SLICE,
            (
                'expected a functor DATA_SLICE, got'
                f' {arolla.optools.constraints.name_type_msg(P.fn)}'
            ),
        ),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice(P.include_missing),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    deterministic=False,
)
def map_(fn, *args, include_missing=False, **kwargs):  # pylint: disable=unused-argument
  """Aligns fn and args/kwargs and calls corresponding fn on corresponding arg.

  If certain items of `fn` are missing, the corresponding items of the result
  will also be missing.
  If certain items of `args`/`kwargs` are missing then:
  - when include_missing=False (the default), the corresponding item of the
    result will be missing.
  - when include_missing=True, we are still calling the functor on those missing
    args/kwargs.

  `fn`, `args`, `kwargs` will all be broadcast to the common shape. The return
  values of the functors will be converted to a common schema, or an exception
  will be raised if the schemas are not compatible. In that case, you can add
  the appropriate cast inside the functor.

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
    *args: The positional argument(s) to pass to the functors.
    include_missing: Whether to call the functors on missing items of
      args/kwargs.
    **kwargs: The keyword argument(s) to pass to the functors.

  Returns:
    The evaluation result.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.if_'])
@optools.as_lambda_operator(
    'kd.functor.if_',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.cond),
        qtype_utils.expect_data_slice(P.yes_fn),
        qtype_utils.expect_data_slice(P.no_fn),
    ],
)
def if_(
    cond, yes_fn, no_fn, *args, return_type_as=data_slice.DataSlice, **kwargs
):
  """Calls either `yes_fn` or `no_fn` depending on the value of `cond`.

  This a short-circuit sibling of kd.cond which executes only one of the two
  functors, and therefore requires that `cond` is a MASK scalar.

  Example:
    x = kd.item(5)
    kd.if_(x > 3, lambda x: x + 1, lambda x: x - 1, x)
    # returns 6, note that both lambdas were traced into functors.

  Args:
    cond: The condition to branch on. Must be a MASK scalar.
    yes_fn: The functor to be called if `cond` is present.
    no_fn: The functor to be called if `cond` is missing.
    *args: The positional argument(s) to pass to the functor.
    return_type_as: The return type of the call is expected to be the same as
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also
      be passed here.
    **kwargs: The keyword argument(s) to pass to the functor.

  Returns:
    The result of the call of either `yes_fn` or `no_fn`.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  cond = assertion.with_assertion(
      cond,
      (slices.get_ndim(cond) == 0)
      & (schema.get_schema(cond) == schema_constants.MASK),
      'the condition in kd.if_ must be a MASK scalar',
  )
  return arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      call,
      masking.cond(cond, yes_fn, no_fn),
      args=args,
      return_type_as=return_type_as,
      kwargs=kwargs,
      **optools.unified_non_deterministic_kwarg(),
  )


# We could later move this operator to slices.py or a similar lower-level
# operator module if we discover that we have too many unrelated operators in
# this module because they use _maybe_call.
@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.functor._maybe_call',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.maybe_fn),
        qtype_utils.expect_data_slice(P.arg),
    ],
    deterministic=False,
)
def _maybe_call(maybe_fn, arg):  # pylint: disable=unused-argument
  """Returns `maybe_fn(arg)` if `maybe_fn` is a functor or `maybe_fn`."""
  raise NotImplementedError('implemented in the backend')


# This operator is defined here since it depends on _maybe_call.
@optools.add_to_registry(aliases=['kd.select'])
@optools.as_lambda_operator(
    'kd.slices.select',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
        qtype_utils.expect_data_slice(P.expand_filter),
    ],
)
def select(ds, fltr, expand_filter=True):
  """Creates a new DataSlice by filtering out missing items in fltr.

  It is not supported for DataItems because their sizes are always 1.

  The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
  By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
  missing items in `fltr` are removed. The last dimension of the resulting
  DataSlice is changed while the first N-1 dimensions are the same as those in
  `ds`.

  Example:
    val = kd.slice([[1, None, 4], [None], [2, 8]])
    kd.select(val, val > 3) -> [[4], [], [8]]

    fltr = kd.slice(
        [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
    kd.select(val, fltr) -> [[None, 4], [None], [2]]

    fltr = kd.slice([kd.present, kd.present, None])
    kd.select(val, fltr) -> [[1, None, 4], [None], []]
    kd.select(val, fltr, expand_filter=False) -> [[1, None, 4], [None]]

  Args:
    ds: DataSlice with ndim > 0 to be filtered.
    fltr: filter DataSlice with dtype as kd.MASK. It can also be a Koda Functor
      or a Python function which can be evalauted to such DataSlice. A Python
      function will be traced for evaluation, so it cannot have Python control
      flow operations such as `if` or `while`.
    expand_filter: flag indicating if the 'filter' should be expanded to 'ds'

  Returns:
    Filtered DataSlice.
  """
  return slices.internal_select_by_slice(
      ds=ds,
      fltr=_maybe_call(fltr, ds),
      expand_filter=expand_filter,
  )


# This operator is defined here since it depends on _maybe_call.
@optools.add_to_registry(aliases=['kd.select_keys'])
@optools.as_lambda_operator(
    'kd.dicts.select_keys',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
    ],
)
def select_keys(ds, fltr):
  """Selects Dict keys by filtering out missing items in `fltr`.

  Also see kd.select.

  Args:
    ds: Dict DataSlice to be filtered
    fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
      function which can be evalauted to such DataSlice. A Python function will
      be traced for evaluation, so it cannot have Python control flow operations
      such as `if` or `while`.

  Returns:
    Filtered DataSlice.
  """
  return select(ds=dicts.get_keys(ds), fltr=fltr)


# This operator is defined here since it depends on _maybe_call.
@optools.add_to_registry(aliases=['kd.select_values'])
@optools.as_lambda_operator(
    'kd.dicts.select_values',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
    ],
)
def select_values(ds, fltr):
  """Selects Dict values by filtering out missing items in `fltr`.

  Also see kd.select.

  Args:
    ds: Dict DataSlice to be filtered
    fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
      function which can be evalauted to such DataSlice. A Python function will
      be traced for evaluation, so it cannot have Python control flow operations
      such as `if` or `while`.

  Returns:
    Filtered DataSlice.
  """
  return select(ds=dicts.get_values(ds), fltr=fltr)


# This operator is defined here since it depends on _maybe_call.
@optools.add_to_registry(aliases=['kd.select_items'])
@optools.as_lambda_operator(
    'kd.lists.select_items',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
)
def select_items(ds, fltr):
  """Selects List items by filtering out missing items in fltr.

  Also see kd.select.

  Args:
    ds: List DataSlice to be filtered
    fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
      Functor or a Python function which can be evalauted to such DataSlice. A
      Python function will be traced for evaluation, so it cannot have Python
      control flow operations such as `if` or `while`.

  Returns:
    Filtered DataSlice.
  """
  return select(ds=lists.explode(ds), fltr=fltr)


@optools.add_to_registry(aliases=['kd.is_fn'])
@optools.as_backend_operator(
    'kd.functor.is_fn',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def is_fn(x):  # pylint: disable=unused-argument
  """Returns `present` iff `x` is a functor."""
  raise NotImplementedError('implemented in the backend')


@optools.as_lambda_operator(
    'kd.functor._reduce_op',
)
def _reduce_op(tuple_fn_x, y):
  """Helper operator for kd.functor.reduce to apply a functor.

  It is needed to both apply the functor and propagate it further, while
  keeping functor and non-deterministic token in the context.
  input:   ((fn, x, non_deterministic_token), y)
  returns: (fn, fn(x, y), non_deterministic_token).

  Args:
    tuple_fn_x: A tuple of (fn, x, non_deterministic_token), where fn is a
      functor, x is the first argument to be passed to the functor, and
      non_deterministic_token is a non-deterministic token.
    y: The second argument to be passed to the functor.

  Returns:
    A tuple of (fn, fn(x, y), non_deterministic_token).
  """
  fn = M.core.get_first(tuple_fn_x)
  x = M.core.get_second(tuple_fn_x)
  non_deterministic_token = M.core.get_nth(tuple_fn_x, 2)
  return M.core.make_tuple(
      fn,
      arolla.abc.sub_by_fingerprint(
          call(fn, x, y, return_type_as=x),
          {
              py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.fingerprint: (
                  non_deterministic_token
              )
          },
      ),
      non_deterministic_token,
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.functor.reduce',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.items),
        qtype_utils.expect_data_slice(P.fn),
    ],
)
def reduce(fn, items, initial_value):
  """Reduces an iterable using the given functor.

  The result is a DataSlice that has the value: fn(fn(fn(initial_value,
  items[0]), items[1]), ...), where the fn calls are done in the order of the
  items in the iterable.

  Args:
    fn: A binary function or functor to be applied to each item of the iterable;
      its return type must be the same as the first argument.
    items: An iterable to be reduced.
    initial_value: The initial value to be passed to the functor.

  Returns:
    Result of the reduction as a single value.
  """
  items = koda_internal_iterables.to_sequence(items)
  res = M.seq.reduce(
      _reduce_op,
      items,
      M.core.make_tuple(
          fn,
          initial_value,
          py_boxing.new_non_deterministic_token(),
      ),
  )

  return M.core.get_second(res)
