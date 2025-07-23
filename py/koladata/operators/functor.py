# Copyright 2025 Google LLC
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
from koladata.base import py_functors_base_py_ext
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import dicts
from koladata.operators import iterables
from koladata.operators import koda_internal_iterables
from koladata.operators import lists
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema
from koladata.operators import slices
from koladata.operators import tuple as tuple_operators
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants
from koladata.types import signature_utils

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
def call(
    fn,  # pylint: disable=unused-argument
    *args,  # pylint: disable=unused-argument
    return_type_as=data_slice.DataSlice,  # pylint: disable=unused-argument
    **kwargs,  # pylint: disable=unused-argument
):
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
      not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
      kd.types.JaggedShape can also be passed here.
    **kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The result of the call.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.functor.call_fn_returning_stream_when_parallel'
)
def call_fn_returning_stream_when_parallel(
    fn,
    *args,
    return_type_as=iterables.make().eval(),
    **kwargs,
):
  """Special call that will be transformed to expect fn to return a stream.

  It should be used only if functor is provided externally in production
  enviroment. Prefer `functor.call` for functors fully implemented in Python.

  Args:
    fn: function to be called. Should return Iterable in interactive mode and
      Stream in parallel mode.
    *args: positional args to pass to the function.
    return_type_as: The return type of the call is expected to be the same as
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return a Iterable[DataSlice].
    **kwargs: The keyword arguments to pass to the call.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      call,
      fn,
      args=args,
      return_type_as=return_type_as,
      kwargs=kwargs,
      **optools.unified_non_deterministic_kwarg(),
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.functor.call_and_update_namedtuple',
    qtype_inference_expr=P.namedtuple_to_update,
    qtype_constraints=[
        (
            P.fn == qtypes.DATA_SLICE,
            (
                'expected a functor DATA_SLICE, got'
                f' {arolla.optools.constraints.name_type_msg(P.fn)}'
            ),
        ),
        qtype_utils.expect_namedtuple(P.namedtuple_to_update),
    ],
    deterministic=False,
)
def call_and_update_namedtuple(fn, *args, namedtuple_to_update, **kwargs):  # pylint: disable=unused-argument
  """Calls a functor which returns a namedtuple and applies it as an update.

  This operator exists to avoid the need to specify return_type_as for the inner
  call (since the returned namedtuple may have a subset of fields of the
  original namedtuple, potentially in a different order).

  Example:
    kd.functor.call_and_update_namedtuple(
        kd.fn(lambda x: kd.namedtuple(x=x * 2)),
        x=2,
        namedtuple_to_update=kd.namedtuple(x=1, y=2))
    # returns kd.namedtuple(x=4, y=2)

  Args:
    fn: The functor to be called, typically created via kd.fn().
    *args: The positional arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.
    namedtuple_to_update: The namedtuple to be updated with the result of the
      call. The returned namedtuple must have a subset (possibly empty or full)
      of fields of this namedtuple, with the same types.
    **kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The updated namedtuple.
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
      not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
      kd.types.JaggedShape can also be passed here.
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
    'kd.functor.is_fn', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def is_fn(x):  # pylint: disable=unused-argument
  """Returns `present` iff `x` is a scalar functor.

  See `kd.functor.has_fn` for the corresponding pointwise version.

  Args:
    x: DataSlice to check.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.has_fn'])
@optools.as_backend_operator(
    'kd.functor.has_fn', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def has_fn(x):  # pylint: disable=unused-argument
  """Returns `present` for each item in `x` that is a functor.

  Note that this is a pointwise operator. See `kd.functor.is_fn` for the
  corresponding scalar version.

  Args:
    x: DataSlice to check.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.functor._while',
    qtype_inference_expr=arolla.M.qtype.conditional_qtype(
        P.returns != arolla.UNSPECIFIED,
        P.returns,
        arolla.M.qtype.conditional_qtype(
            P.yields != arolla.UNSPECIFIED,
            arolla.M.qtype.make_sequence_qtype(P.yields),
            arolla.M.qtype.make_sequence_qtype(P.yields_interleaved),
        ),
    ),
    deterministic=False,
)
def _while(
    condition_fn,  # pylint: disable=unused-argument
    body_fn,  # pylint: disable=unused-argument
    *,
    returns=arolla.unspecified(),  # pylint: disable=unused-argument
    yields=arolla.unspecified(),  # pylint: disable=unused-argument
    yields_interleaved=arolla.unspecified(),  # pylint: disable=unused-argument
    initial_state,  # pylint: disable=unused-argument
):
  """Backend operator for while_ that doesn't chain/interleave yields."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.while_'])
@optools.as_lambda_operator(
    'kd.functor.while_',
    qtype_constraints=(
        qtype_utils.expect_data_slice(P.condition_fn),
        qtype_utils.expect_data_slice(P.body_fn),
        qtype_utils.expect_iterable_or_unspecified(P.yields),
        qtype_utils.expect_iterable_or_unspecified(P.yields_interleaved),
        (
            arolla.M.array.count(
                arolla.M.array.make_dense_array(
                    P.returns != arolla.UNSPECIFIED,
                    P.yields != arolla.UNSPECIFIED,
                    P.yields_interleaved != arolla.UNSPECIFIED,
                )
            )
            == 1,
            (
                'exactly one of `returns`, `yields`, or `yields_interleaved`'
                ' must be specified'
            ),
        ),
    ),
    deterministic=False,
)
def while_(
    condition_fn,  # pylint: disable=unused-argument
    body_fn,  # pylint: disable=unused-argument
    *,
    returns=arolla.unspecified(),  # pylint: disable=unused-argument
    yields=arolla.unspecified(),  # pylint: disable=unused-argument
    yields_interleaved=arolla.unspecified(),  # pylint: disable=unused-argument
    **initial_state,  # pylint: disable=unused-argument
):
  """While a condition functor returns present, runs a body functor repeatedly.

  The items in `initial_state` (and `returns`, if specified) are used to
  initialize a dict of state variables, which are passed as keyword arguments
  to `condition_fn` and `body_fn` on each loop iteration, and updated from the
  namedtuple (see kd.namedtuple) return value of `body_fn`.

  Exactly one of `returns`, `yields`, or `yields_interleaved` must be specified.
  The return value of this operator depends on which one is present:
  - `returns`: the value of `returns` when the loop ends. The initial value of
    `returns` must have the same qtype (e.g. DataSlice, DataBag) as the final
    return value.
  - `yields`: a single iterable chained (using `kd.iterables.chain`) from the
    value of `yields` returned from each invocation of `body_fn`, The value of
    `yields` must always be an iterable, including initially.
  - `yields_interleaved`: the same as for `yields`, but the iterables are
    interleaved (using `kd.iterables.iterleave`) instead of being chained.

  Args:
    condition_fn: A functor with keyword argument names matching the state
      variable names and returning a MASK DataItem.
    body_fn: A functor with argument names matching the state variable names and
      returning a namedtuple (see kd.namedtuple) with a subset of the keys
      of `initial_state`.
    returns: If present, the initial value of the 'returns' state variable.
    yields: If present, the initial value of the 'yields' state variable.
    yields_interleaved: If present, the initial value of the
      `yields_interleaved` state variable.
    **initial_state: A dict of the initial values for state variables.

  Returns:
    If `returns` is a state variable, the value of `returns` when the loop
    ended. Otherwise, an iterable combining the values of `yields` or
    `yields_interleaved` from each body invocation.
  """
  initial_state = arolla.optools.fix_trace_kwargs(initial_state)
  while_result = _while(
      condition_fn,
      body_fn,
      returns=returns,
      yields=yields,
      yields_interleaved=yields_interleaved,
      initial_state=initial_state,
  )
  return arolla.types.DispatchOperator(
      'while_result, returns, yields, yields_interleaved,'
      ' non_determinism_token',
      returns_case=arolla.types.DispatchCase(
          P.while_result,
          condition=(P.returns != arolla.UNSPECIFIED),
      ),
      yields_case=arolla.types.DispatchCase(
          koda_internal_iterables.from_sequence(
              koda_internal_iterables.sequence_chain(
                  arolla.M.seq.map(
                      koda_internal_iterables.to_sequence, P.while_result
                  )
              )
          ),
          condition=(P.yields != arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          arolla.abc.sub_by_fingerprint(
              koda_internal_iterables.from_sequence(
                  koda_internal_iterables.sequence_interleave(
                      arolla.M.seq.map(
                          koda_internal_iterables.to_sequence, P.while_result
                      ),
                  )
              ),
              {
                  py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.fingerprint: (
                      P.non_determinism_token
                  )
              },
          ),
          condition=(P.yields_interleaved != arolla.UNSPECIFIED),
      ),
  )(
      while_result,
      returns,
      yields,
      yields_interleaved,
      py_boxing.NON_DETERMINISTIC_TOKEN_LEAF,
  )


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


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.functor.expr_fn',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.returns),
        qtype_utils.expect_data_slice(P.signature),
        qtype_utils.expect_data_slice(P.auto_variables),
        qtype_utils.expect_data_slice_kwargs(P.variables),
    ],
    deterministic=False,
)
def expr_fn(
    returns,
    signature=None,
    auto_variables=False,
    **variables,
):
  """Creates a functor.

  Args:
    returns: DataItem with literal or ExprQuote that will be evaluated on the
      functor call. Must evaluate to a DataSlice/DataItem, or the
      return_type_as= argument should be specified at kd.call time.
    signature: The signature of the functor. Will be used to map from args/
      kwargs passed at calling time to I.smth inputs of the expressions. When
      None or dataItem with missing value, the default signature will be created
      based on the inputs from the expressions involved.
    auto_variables: When true, we create additional variables automatically
      based on the provided expressions for 'returns' and user-provided
      variables. All non-scalar-primitive DataSlice literals become their own
      variables, and all named subexpressions become their own variables. This
      helps readability and manipulation of the resulting functor.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataItem representing the functor.
  """
  del returns, signature, auto_variables, variables
  raise NotImplementedError('implemented in the backend')


def _create_for_iteration_condition_or_body_signature():
  """Creates a signature for functors used to express `for` via `while`."""
  kind_enum = signature_utils.ParameterKind
  return signature_utils.signature([
      signature_utils.parameter('_koda_internal_step', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter(
          '_koda_internal_num_steps', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_iterable', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_body_fn', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_finalize_fn', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_condition_fn', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_yields_namedtuple', kind_enum.KEYWORD_ONLY
      ),
      signature_utils.parameter(
          '_koda_internal_variables', kind_enum.VAR_KEYWORD
      ),
  ])


def _create_for_iteration_step_signature():
  """Creates a signature for one step inside `for` via `while`."""
  kind_enum = signature_utils.ParameterKind
  return signature_utils.signature([
      signature_utils.parameter('step', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter('iterable', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter('body_fn', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter('finalize_fn', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter('variables', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter(
          'variables_with_yields', kind_enum.KEYWORD_ONLY
      ),
  ])


def _create_for_iteration_normal_step_fn():
  """Creates a functor used to express 'body' step of the for iteration."""
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  item = arolla.M.seq.at(
      koda_internal_iterables.to_sequence(I.iterable),
      arolla_bridge.to_arolla_int64(I.step),
  )
  returns = arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      call_and_update_namedtuple,
      I.body_fn,
      args=arolla.M.core.make_tuple(item),
      namedtuple_to_update=I.variables_with_yields,
      kwargs=I.variables,
      **optools.unified_non_deterministic_kwarg(),
  )
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(returns),
      _create_for_iteration_step_signature(),
  )


def _create_for_iteration_final_step_fn():
  """Creates a functor used to express 'finalize' step of the for iteration."""
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  returns = arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      call_and_update_namedtuple,
      I.finalize_fn,
      args=arolla.M.core.make_tuple(),
      namedtuple_to_update=I.variables_with_yields,
      kwargs=I.variables,
      **optools.unified_non_deterministic_kwarg(),
  )
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(returns),
      _create_for_iteration_step_signature(),
  )


def _create_for_iteration_body_fn():
  """Creates a functor used to express for iteration."""
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  seq_size = arolla_bridge.to_data_slice(
      arolla.M.seq.size(
          koda_internal_iterables.to_sequence(I['_koda_internal_iterable'])
      )
  )
  variables_with_yields = arolla.M.namedtuple.union(
      I['_koda_internal_yields_namedtuple'],
      I['_koda_internal_variables'],
  )
  new_variables = if_(
      I['_koda_internal_step'] < seq_size,
      V['_koda_internal_normal_step_fn'],
      V['_koda_internal_final_step_fn'],
      return_type_as=variables_with_yields,
      step=I['_koda_internal_step'],
      iterable=I['_koda_internal_iterable'],
      body_fn=I['_koda_internal_body_fn'],
      finalize_fn=I['_koda_internal_finalize_fn'],
      variables=I['_koda_internal_variables'],
      variables_with_yields=variables_with_yields,
  )
  returns = arolla.M.namedtuple.union(
      new_variables,
      arolla.M.namedtuple.make(
          _koda_internal_step=I['_koda_internal_step'] + 1,
      ),
  )
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(returns),
      _create_for_iteration_condition_or_body_signature(),
      _koda_internal_normal_step_fn=_create_for_iteration_normal_step_fn(),
      _koda_internal_final_step_fn=_create_for_iteration_final_step_fn(),
  )


def _create_constant_missing_fn():
  """Creates a functor that returns a constant missing value."""
  return py_functors_base_py_ext.create_functor(
      # We cannot set "missing" directly as the value, as the "returns"
      # attribute of a functor must be present.
      introspection.pack_expr(py_boxing.as_expr(mask_constants.missing)),
      signature_utils.ARGS_KWARGS_SIGNATURE,
  )


def _create_constant_present_fn():
  """Creates a functor that returns a constant present value."""
  return py_functors_base_py_ext.create_functor(
      mask_constants.present,
      signature_utils.ARGS_KWARGS_SIGNATURE,
  )


def _create_for_iteration_condition_fn():
  """Creates a functor used to express for iteration condition."""
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  returns = arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      if_,
      I['_koda_internal_step'] < I['_koda_internal_num_steps'],
      arolla.M.core.default_if_unspecified(
          I['_koda_internal_condition_fn'],
          V['_koda_internal_constant_present_fn'],
      ),
      V['_koda_internal_constant_missing_fn'],
      args=arolla.M.core.make_tuple(),
      kwargs=I['_koda_internal_variables'],
      **optools.unified_non_deterministic_kwarg(),
  )
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(returns),
      _create_for_iteration_condition_or_body_signature(),
      _koda_internal_constant_missing_fn=_create_constant_missing_fn(),
      _koda_internal_constant_present_fn=_create_constant_present_fn(),
  )


_FOR_ITERATION_BODY_FN = _create_for_iteration_body_fn()
_FOR_ITERATION_CONDITION_FN = _create_for_iteration_condition_fn()


@optools.add_to_registry(aliases=['kd.for_'])
@optools.as_lambda_operator(
    'kd.functor.for_',
    qtype_constraints=(
        qtype_utils.expect_iterable(P.iterable),
        qtype_utils.expect_data_slice(P.body_fn),
        qtype_utils.expect_data_slice_or_unspecified(P.finalize_fn),
        qtype_utils.expect_data_slice_or_unspecified(P.condition_fn),
        qtype_utils.expect_iterable_or_unspecified(P.yields),
        qtype_utils.expect_iterable_or_unspecified(P.yields_interleaved),
        (
            arolla.M.array.count(
                arolla.M.array.make_dense_array(
                    P.returns != arolla.UNSPECIFIED,
                    P.yields != arolla.UNSPECIFIED,
                    P.yields_interleaved != arolla.UNSPECIFIED,
                )
            )
            == 1,
            (
                'exactly one of `returns`, `yields`, or `yields_interleaved`'
                ' must be specified'
            ),
        ),
    ),
)
def for_(
    iterable,
    body_fn,
    *,
    finalize_fn=arolla.unspecified(),
    condition_fn=arolla.unspecified(),
    returns=arolla.unspecified(),
    yields=arolla.unspecified(),
    yields_interleaved=arolla.unspecified(),
    **initial_state,
):
  """Executes a loop over the given iterable.

  Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
  and that dictates what this operator returns.

  When `returns` is specified, it is one more variable added to `initial_state`,
  and the value of that variable at the end of the loop is returned.

  When `yields` is specified, it must be an iterable, and the value
  passed there, as well as the values set to this variable in each
  iteration of the loop, are chained to get the resulting iterable.

  When `yields_interleaved` is specified, the behavior is the same as `yields`,
  but the values are interleaved instead of chained.

  The behavior of the loop is equivalent to the following pseudocode:

    state = initial_state  # Also add `returns` to it if specified.
    while condition_fn(state):
      item = next(iterable)
      if item == <end-of-iterable>:
        upd = finalize_fn(**state)
      else:
        upd = body_fn(item, **state)
      if yields/yields_interleaved is specified:
        yield the corresponding data from upd, and remove it from upd.
      state.update(upd)
      if item == <end-of-iterable>:
        break
    if returns is specified:
      return state['returns']

  Args:
    iterable: The iterable to iterate over.
    body_fn: The function to be executed for each item in the iterable. It will
      receive the iterable item as the positional argument, and the loop
      variables as keyword arguments (excluding `yields`/`yields_interleaved` if
      those are specified), and must return a namedtuple with the new values for
      some or all loop variables (including `yields`/`yields_interleaved` if
      those are specified).
    finalize_fn: The function to be executed when the iterable is exhausted. It
      will receive the same arguments as `body_fn` except the positional
      argument, and must return the same namedtuple. If not specified, the state
      at the end will be the same as the state after processing the last item.
      Note that finalize_fn is not called if condition_fn ever returns a missing
      mask.
    condition_fn: The function to be executed to determine whether to continue
      the loop. It will receive the loop variables as keyword arguments, and
      must return a MASK scalar. Can be used to terminate the loop early without
      processing all items in the iterable. If not specified, the loop will
      continue until the iterable is exhausted.
    returns: The loop variable that holds the return value of the loop.
    yields: The loop variables that holds the values to yield at each iteration,
      to be chained together.
    yields_interleaved: The loop variables that holds the values to yield at
      each iteration, to be interleaved.
    **initial_state: The initial state of the loop variables.

  Returns:
    Either the return value or the iterable of yielded values.
  """
  initial_state = arolla.optools.fix_trace_kwargs(initial_state)
  num_steps = arolla_bridge.to_data_slice(
      arolla.M.seq.size(koda_internal_iterables.to_sequence(P.iterable))
  )
  num_steps += arolla.types.DispatchOperator(
      'finalize_fn',
      no_finalize_case=arolla.types.DispatchCase(
          data_slice.DataSlice.from_vals(0),
          condition=(P.finalize_fn == arolla.UNSPECIFIED),
      ),
      default=data_slice.DataSlice.from_vals(1),
  )(finalize_fn)
  yields_namedtuple = arolla.types.DispatchOperator(
      'yields, yields_interleaved',
      yields_case=arolla.types.DispatchCase(
          arolla.M.namedtuple.make(
              yields=koda_internal_iterables.empty_as(P.yields)
          ),
          condition=(P.yields != arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          arolla.M.namedtuple.make(
              yields_interleaved=koda_internal_iterables.empty_as(
                  P.yields_interleaved
              )
          ),
          condition=(P.yields_interleaved != arolla.UNSPECIFIED),
      ),
      default=arolla.M.namedtuple.make(),
  )(yields, yields_interleaved)
  return arolla.abc.bind_op(
      while_,
      condition_fn=_FOR_ITERATION_CONDITION_FN,
      body_fn=_FOR_ITERATION_BODY_FN,
      returns=returns,
      yields=yields,
      yields_interleaved=yields_interleaved,
      initial_state=arolla.M.namedtuple.union(
          initial_state,
          arolla.M.namedtuple.make(
              _koda_internal_step=data_slice.DataSlice.from_vals(0),
              _koda_internal_num_steps=num_steps,
              _koda_internal_iterable=iterable,
              _koda_internal_body_fn=body_fn,
              _koda_internal_finalize_fn=finalize_fn,
              _koda_internal_condition_fn=condition_fn,
              _koda_internal_yields_namedtuple=yields_namedtuple,
          ),
      ),
      **optools.unified_non_deterministic_kwarg(),
  )


@optools.add_to_registry(aliases=['kd.bind'])
@optools.as_backend_operator(
    'kd.functor.bind',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fn_def),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    deterministic=False,
)
def bind(fn_def, return_type_as=data_slice.DataSlice, **kwargs):
  """Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  Example:
    f = kd.bind(kd.fn(I.x + I.y), x=0)
    kd.call(f, y=1)  # 1

  Args:
    fn_def: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
      kd.types.JaggedShape can also be passed here.
    **kwargs: Partial parameter binding. The values in this map may be
      DataItems. This function creates auxiliary variables with names starting
      with '_aux_fn', so it is not recommended to pass variables with such
      names.

  Returns:
    A new Koda functor with some parameters bound.
  """
  del fn_def, return_type_as, kwargs
  raise NotImplementedError('implemented in the backend')


def _create_for_flat_map_step_signature():
  """Creates a signature for one step inside flat_map_[chain|interleaved] via `for_`."""
  kind_enum = signature_utils.ParameterKind
  return signature_utils.signature([
      signature_utils.parameter('item', kind_enum.POSITIONAL_ONLY),
      signature_utils.parameter('_koda_flat_map_fn', kind_enum.KEYWORD_ONLY),
      signature_utils.parameter('_return_type_as', kind_enum.KEYWORD_ONLY),
  ])


def _create_for_flat_map_step_fn(interleaved=False):
  """Creates a functor used to express 'body' step of the for flat_map_[chain|interleaved]."""
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name

  internal_call = arolla.abc.bind_op(
      call,
      fn=I['_koda_flat_map_fn'],
      args=arolla.M.core.make_tuple(I.item),
      return_type_as=I['_return_type_as'],
      **optools.unified_non_deterministic_kwarg(),
  )

  if interleaved:
    returns = tuple_operators.namedtuple_(yields_interleaved=internal_call)
  else:
    returns = tuple_operators.namedtuple_(yields=internal_call)
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(returns),
      _create_for_flat_map_step_signature(),
  )


_FOR_FLAT_MAP_CHAIN_STEP_FN = _create_for_flat_map_step_fn(interleaved=False)
_FOR_FLAT_MAP_INTERLEAVED_STEP_FN = _create_for_flat_map_step_fn(
    interleaved=True
)


@optools.add_to_registry(aliases=['kd.flat_map_chain'])
@optools.as_lambda_operator(
    'kd.functor.flat_map_chain',
    qtype_constraints=(
        qtype_utils.expect_iterable(P.iterable),
        qtype_utils.expect_data_slice(P.fn),
    ),
)
def flat_map_chain(
    iterable,
    fn,
    value_type_as=data_slice.DataSlice,
):
  """Executes flat maps over the given iterable.

  `fn` is called for each item in the iterable, and must return an iterable.
  The resulting iterable is then chained to get the final result.

  If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
  `iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
  `kd.iterables.make(f(x1), g(x1), f(x2), g(x2))`.

  Example:
    ```
    kd.functor.flat_map_chain(
        kd.iterables.make(1, 10),
        lambda x: kd.iterables.make(x, x * 2, x * 3),
    )
    ```
    result: `kd.iterables.make(1, 2, 3, 10, 20, 30)`.

  Args:
    iterable: The iterable to iterate over.
    fn: The function to be executed for each item in the iterable. It will
      receive the iterable item as the positional argument and must return an
      iterable.
    value_type_as: The type to use as element type of the resulting iterable.

  Returns:
    The resulting iterable as chained output of `fn`.
  """
  empty_iterable = iterables.make(value_type_as=value_type_as)
  return arolla.abc.bind_op(
      for_,
      iterable=iterable,
      body_fn=_FOR_FLAT_MAP_CHAIN_STEP_FN,
      yields=empty_iterable,
      initial_state=arolla.M.namedtuple.make(
          _koda_internal_iterable=iterable,
          _koda_flat_map_fn=fn,
          _return_type_as=empty_iterable,
      ),
      **optools.unified_non_deterministic_kwarg(),
  )


@optools.add_to_registry(aliases=['kd.flat_map_interleaved'])
@optools.as_lambda_operator(
    'kd.functor.flat_map_interleaved',
    qtype_constraints=(
        qtype_utils.expect_iterable(P.iterable),
        qtype_utils.expect_data_slice(P.fn),
    ),
)
def flat_map_interleaved(
    iterable,
    fn,
    value_type_as=data_slice.DataSlice,
):
  """Executes flat maps over the given iterable.

  `fn` is called for each item in the iterable, and must return an
  iterable. The resulting iterables are then interleaved to get the final
  result. Please note that the order of the items in each functor output
  iterable is preserved, while the order of these iterables is not preserved.

  If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
  `iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
  `kd.iterables.make(f(x1), g(x1), f(x2), g(x2))` or `kd.iterables.make(f(x1),
  f(x2), g(x1), g(x2))` or `kd.iterables.make(g(x1), f(x1), f(x2), g(x2))` or
  `kd.iterables.make(g(x1), g(x2), f(x1), f(x2))`.

  Example:
    ```
    kd.functor.flat_map_interleaved(
        kd.iterables.make(1, 10),
        lambda x: kd.iterables.make(x, x * 2, x * 3),
    )
    ```
    result: `kd.iterables.make(1, 10, 2, 3, 20, 30)`.

  Args:
    iterable: The iterable to iterate over.
    fn: The function to be executed for each item in the iterable. It will
      receive the iterable item as the positional argument and must return an
      iterable.
    value_type_as: The type to use as element type of the resulting iterable.

  Returns:
    The resulting iterable as interleaved output of `fn`.
  """
  empty_iterable = iterables.make(value_type_as=value_type_as)
  return arolla.abc.bind_op(
      for_,
      iterable=iterable,
      body_fn=_FOR_FLAT_MAP_INTERLEAVED_STEP_FN,
      yields_interleaved=empty_iterable,
      initial_state=arolla.M.namedtuple.make(
          _koda_internal_iterable=iterable,
          _koda_flat_map_fn=fn,
          _return_type_as=empty_iterable,
      ),
      **optools.unified_non_deterministic_kwarg(),
  )
