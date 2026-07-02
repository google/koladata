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

"""Matrix operations for Koda.

The kd.matrix library provides fully vectorized support of batches of
independent matrices. Leading dimensions are interpreted as batch dimensions.
In operators that take 2 or more matrix arguments, the batch dimensions are
subject to standard Koda broadcasting rules.
"""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils

P = arolla.P

optools.set_namespace_docstring('kd.matrix', __doc__)


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.transpose',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def transpose(x):  # pylint: disable=unused-argument
  """Transpose a matrix (swap last two dimensions).

  Supports leading batch dimensions: (..., m, n) -> (..., n, m).
  Leading batch dimensions (all except the last two) can be jagged.
  The last two dimensions must be uniform within each matrix entry (i.e.,
  every row of a given matrix must have the same number of columns), but
  different matrix entries can have different shapes.
  Preserves sparsity: None values remain None.
  Works with any schema, including numeric, TEXT, BYTES, and entities.

  Args:
    x: A DataSlice with at least 2 dimensions. The last two dimensions must be
      uniform within each matrix entry, but leading batch dimensions can be
      jagged.

  Returns:
    The transposed DataSlice.
  """
  raise NotImplementedError('implemented in the backend')
