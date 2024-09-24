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
from koladata.types import py_boxing
from koladata.types import qtypes

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.call'])
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
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def call(fn, args=py_boxing.var_positional(), kwargs=py_boxing.var_keyword()):  # pylint: disable=unused-argument
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
    kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    The result of the call.
  """
  raise NotImplementedError('implemented in the backend')
