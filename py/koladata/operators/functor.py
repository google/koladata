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
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(
    aliases=['kde.call'], repr_fn=op_repr.full_signature_repr
)
@optools.as_backend_operator(
    'kde.functor.call',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fn),
        (
            M.qtype.is_tuple_qtype(P.args),
            f'expected tuple, got {constraints.name_type_msg(P.args)}',
        ),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
    ],
    qtype_inference_expr=P.return_type_as,
)
def call(
    fn,
    args=py_boxing.var_positional(),
    return_type_as=py_boxing.keyword_only(data_slice.DataSlice),
    kwargs=py_boxing.var_keyword(),
):
  """Calls a functor.

  See the docstring of `kdf.fn` on how to create a functor.

  Example:
    kd.call(kdf.fn(I.x + I.y), x=2, y=3)
    # returns kd.item(5)

    kde.call(I.fn, x=2, y=3).eval(fn=kdf.fn(I.x + I.y))
    # returns kd.item(5)

  Args:
    fn: The functor to be called, typically created via kdf.fn().
    args: The positional arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.
    return_type_as: The return type of the call is expected to be the same as
      the return type of this expression. In most cases, this will be a literal
      of the corresponding type. This needs to be specified if the functor does
      not return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also
      be passed here.
    kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The result of the call.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.functor._maybe_call',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.maybe_fn),
        qtype_utils.expect_data_slice(P.arg),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _maybe_call(maybe_fn, arg):  # pylint: disable=unused-argument
  """Returns `maybe_fn(arg)` if `maybe_fn` is a functor or `maybe_fn`."""
  raise NotImplementedError('implemented in the backend')
