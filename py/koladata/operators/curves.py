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

"""Curve Koda operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import qtypes


P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.curves.log_pwl_curve',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.p),
        qtype_utils.expect_data_slice(P.adjustments),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def log_pwl_curve(p, adjustments):  # pylint: disable=unused-argument
  """Specialization of PWLCurve with log(x) transformation.

  Args:
   p: (DataSlice) input points to the curve
   adjustments: (DataSlice) 2D data slice with points used for interpolation.
     The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
     3.6], [7, 5.7]]

  Returns:
    FLOAT64 DataSlice with the same dimensions as p with interpolation results.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.pwl_curve'])
@optools.as_backend_operator(
    'kd.curves.pwl_curve',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.p),
        qtype_utils.expect_data_slice(P.adjustments),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def pwl_curve(p, adjustments):  # pylint: disable=unused-argument
  """Piecewise Linear (PWL) curve interpolation operator.

  Args:
   p: (DataSlice) input points to the curve
   adjustments: (DataSlice) 2D data slice with points used for interpolation.
     The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
     3.6], [7, 5.7]]

  Returns:
    FLOAT64 DataSlice with the same dimensions as p with interpolation results.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.curves.log_p1_pwl_curve',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.p),
        qtype_utils.expect_data_slice(P.adjustments),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def log_p1_pwl_curve(p, adjustments):  # pylint: disable=unused-argument
  """Specialization of PWLCurve with log(x + 1) transformation.

  Args:
   p: (DataSlice) input points to the curve
   adjustments: (DataSlice) 2D data slice with points used for interpolation.
     The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
     3.6], [7, 5.7]]

  Returns:
    FLOAT64 DataSlice with the same dimensions as p with interpolation results.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.curves.symmetric_log_p1_pwl_curve',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.p),
        qtype_utils.expect_data_slice(P.adjustments),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def symmetric_log_p1_pwl_curve(p, adjustments):  # pylint: disable=unused-argument
  """Specialization of PWLCurve with symmetric log(x + 1) transformation.

  Args:
   p: (DataSlice) input points to the curve
   adjustments: (DataSlice) 2D data slice with points used for interpolation.
     The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
     3.6], [7, 5.7]]

  Returns:
    FLOAT64 DataSlice with the same dimensions as p with interpolation results.
  """
  raise NotImplementedError('implemented in the backend')
