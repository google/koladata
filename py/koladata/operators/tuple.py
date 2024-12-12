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

"""Tuple operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import schema_constants

M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


optools.add_to_registry(
    name='kde.tuple.make_tuple', aliases=['kde.make_tuple']
)(arolla.M.core.make_tuple)


@optools.add_to_registry_as_overload(
    'koda_internal.view.get_item._tuple',
    overload_condition_expr=arolla.M.qtype.is_tuple_qtype(arolla.P.x)
    | arolla.M.qtype.is_slice_qtype(arolla.P.x),
)
@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.tuple.get_nth',
    qtype_constraints=[qtype_utils.expect_data_slice(P.n)],
)
def get_nth(x, n):
  """Returns the nth element of the tuple `x`.

  Note that `n` _must_ be a literal integer in [0, len(x)).

  Args:
    x: a tuple.
    n: the index of the element to return. _Must_ be a literal integer in the
      range [0, len(x)).
  """
  n = arolla_bridge.to_arolla_int64(n)
  return M.core.get_nth(x, n)
