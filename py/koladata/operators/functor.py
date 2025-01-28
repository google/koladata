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
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice

P = arolla.P


@optools.add_to_registry(aliases=['kd.call'])
@optools.as_backend_operator(
    'kd.functor.call',
    qtype_inference_expr=P.return_type_as,
    deterministic=False,
)
def call(fn, *args, return_type_as=data_slice.DataSlice, **kwargs):
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
        qtype_utils.expect_data_slice(P.fn),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice(P.include_missing),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    deterministic=False,
)
def map_(fn, *args, include_missing=False, **kwargs):
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
