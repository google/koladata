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

from arolla import arolla
from koladata.operators import masking
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils

P = arolla.P


@optools.add_to_registry(aliases=['kd.equal'], repr_fn=op_repr.equal_repr)
@optools.as_backend_operator(
    'kd.comparison.equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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


@optools.add_to_registry(aliases=['kd.full_equal'])
@optools.as_lambda_operator(
    'kd.comparison.full_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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
  return masking.all_((x == y) | (masking.has_not(x) & masking.has_not(y)))


@optools.add_to_registry(
    aliases=['kd.not_equal'],
    repr_fn=op_repr.not_equal_repr,
)
@optools.as_lambda_operator(
    'kd.comparison.not_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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
  return ~(x == y) & (masking.has(x) & masking.has(y))


@optools.add_to_registry(
    aliases=['kd.greater'],
    repr_fn=op_repr.greater_repr,
)
@optools.as_backend_operator(
    'kd.comparison.greater',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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


@optools.add_to_registry(aliases=['kd.less'], repr_fn=op_repr.less_repr)
@optools.as_backend_operator(
    'kd.comparison.less',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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


@optools.add_to_registry(
    aliases=['kd.greater_equal'],
    repr_fn=op_repr.greater_equal_repr,
)
@optools.as_backend_operator(
    'kd.comparison.greater_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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


@optools.add_to_registry(
    aliases=['kd.less_equal'],
    repr_fn=op_repr.less_equal_repr,
)
@optools.as_backend_operator(
    'kd.comparison.less_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
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
