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

"""Comparison DataSlice operators."""

from arolla import arolla as _arolla
from koladata.operators import masking as _masking
from koladata.operators import op_repr as _op_repr
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils

_P = _arolla.P


@_optools.add_to_registry(
    aliases=['kd.equal'],
    repr_fn=_op_repr.equal_repr,
    via_cc_operator_package=True,
)
@_optools.as_backend_operator(
    'kd.comparison.equal',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def equal(x, y):  # pylint: disable=unused-argument
  """Returns present iff `x` and `y` are equal.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` and `y` are equal. Returns `kd.present` for equal items and
  `kd.missing` in other cases.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.full_equal'], via_cc_operator_package=True
)
@_optools.as_lambda_operator(
    'kd.comparison.full_equal',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def full_equal(x, y):  # pylint: disable=unused-argument
  """Returns present iff all present items in `x` and `y` are equal.

  The result is a zero-dimensional DataItem. Note that it is different from
  `kd.all(x == y)`.

  For example,
    kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, 3])) -> kd.present
    kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, None])) -> kd.missing
    kd.full_equal(kd.slice([1, 2, None]), kd.slice([1, 2, None])) -> kd.present

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  return _masking.all_((x == y) | (_masking.has_not(x) & _masking.has_not(y)))


@_optools.add_to_registry(
    aliases=['kd.not_equal'],
    repr_fn=_op_repr.not_equal_repr,
    via_cc_operator_package=True,
)
@_optools.as_lambda_operator(
    'kd.comparison.not_equal',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def not_equal(x, y):
  """Returns present iff `x` and `y` are not equal.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` and `y` are not equal. Returns `kd.present` for not equal items and
  `kd.missing` in other cases.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  return ~(x == y) & (_masking.has(x) & _masking.has(y))


@_optools.add_to_registry(
    aliases=['kd.greater'],
    repr_fn=_op_repr.greater_repr,
    via_cc_operator_package=True,
)
@_optools.as_backend_operator(
    'kd.comparison.greater',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def greater(x, y):  # pylint: disable=unused-argument
  """Returns present iff `x` is greater than `y`.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` is greater than `y`. Returns `kd.present` when `x` is greater and
  `kd.missing` when `x` is less than or equal to `y`.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.less'],
    repr_fn=_op_repr.less_repr,
    via_cc_operator_package=True,
)
@_optools.as_backend_operator(
    'kd.comparison.less',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def less(x, y):  # pylint: disable=unused-argument
  """Returns present iff `x` is less than `y`.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` is less than `y`. Returns `kd.present` when `x` is less and
  `kd.missing` when `x` is greater than or equal to `y`.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.greater_equal'],
    repr_fn=_op_repr.greater_equal_repr,
    via_cc_operator_package=True,
)
@_optools.as_backend_operator(
    'kd.comparison.greater_equal',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def greater_equal(x, y):  # pylint: disable=unused-argument
  """Returns present iff `x` is greater than or equal to `y`.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` is greater than or equal to `y`. Returns `kd.present` when `x` is
  greater than or equal to `y` and `kd.missing` when `x` is less than `y`.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.less_equal'],
    repr_fn=_op_repr.less_equal_repr,
    via_cc_operator_package=True,
)
@_optools.as_backend_operator(
    'kd.comparison.less_equal',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def less_equal(x, y):  # pylint: disable=unused-argument
  """Returns present iff `x` is less than or equal to `y`.

  Pointwise operator which takes a DataSlice and returns a MASK indicating
  iff `x` is less than or equal to `y`. Returns `kd.present` when `x` is
  less than or equal to `y` and `kd.missing` when `x` is greater than `y`.

  Args:
    x: DataSlice.
    y: DataSlice.
  """
  raise NotImplementedError('implemented in the backend')
