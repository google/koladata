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


@optools.add_to_registry(aliases=['kde.are_primitives'])
@optools.as_backend_operator(
    'kde.core.are_primitives',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def are_primitives(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is primitive.

  Note that this is a pointwise operation.

  Also see `kd.is_primitive` for checking if `x` is a primitive DataSlice. But
  note that `kd.all(kd.are_primitives(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.are_primitives(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.are_primitives(kd.int32([None]))) -> kd.missing

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

  Also see `kd.are_primitives` for a pointwise version. But note that
  `kd.all(kd.are_primitives(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.are_primitives(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.are_primitives(kd.int32([None]))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.is_list'])
@optools.as_backend_operator(
    'kde.core.is_list',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def is_list(x):  # pylint: disable=unused-argument
  """Returns whether x is a List DataSlice.

  `x` is a List DataSlice if it meets one of the following conditions:
    1) it has a List schema
    2) it has OBJECT/ANY schema and only has List items

  Also see `kd.are_lists` for a pointwise version. But note that
  `kd.all(kd.are_lists(x))` is not always equivalent to
  `kd.is_list(x)`. For example,

    kd.is_list(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.are_lists(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_list(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.are_lists(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.is_dict'])
@optools.as_backend_operator(
    'kde.core.is_dict',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def is_dict(x):  # pylint: disable=unused-argument
  """Returns whether x is a Dict DataSlice.

  `x` is a Dict DataSlice if it meets one of the following conditions:
    1) it has a Dict schema
    2) it has OBJECT/ANY schema and only has Dict items

  Also see `kd.are_dicts` for a pointwise version. But note that
  `kd.all(kd.are_dicts(x))` is not always equivalent to
  `kd.is_dict(x)`. For example,

    kd.is_dict(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.are_dicts(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_dict(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.are_dicts(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')
