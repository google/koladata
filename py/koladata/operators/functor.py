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
from koladata.operators import jagged_shape
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

P = arolla.P


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
      # We cannot use slices.get_ndim due to a cyclic dependency between
      # slices and functor.
      (jagged_shape.rank(jagged_shape.get_shape(cond)) == 0)
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


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.functor._maybe_call',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.maybe_fn),
        qtype_utils.expect_data_slice(P.arg),
    ],
)
def _maybe_call(maybe_fn, arg):  # pylint: disable=unused-argument
  """Returns `maybe_fn(arg)` if `maybe_fn` is a functor or `maybe_fn`."""
  raise NotImplementedError('implemented in the backend')
