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

"""Predicate operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import qtypes

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.has_primitive'])
@optools.as_backend_operator(
    'kde.core.has_primitive',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def has_primitive(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is primitive.

  Note that this is a pointwise operation.

  Also see `kd.is_primitive` for checking if `x` is a primitive DataSlice. But
  note that `kd.all(kd.has_primitive(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataSlice with the same shape as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.is_primitive'])
@optools.as_backend_operator(
    'kde.core.is_primitive',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def is_primitive(x):  # pylint: disable=unused-argument
  """Returns whether x is a primitive DataSlice.

  `x` is a primitive DataSlice if it meets one of the following conditions:
    1) it has a primitive schema
    2) it has OBJECT/ANY/SCHEMA schema and only has primitives

  Also see `kd.has_primitive` for a pointwise version. But note that
  `kd.all(kd.has_primitive(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')
