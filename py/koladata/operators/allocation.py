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

"""Koda operators that allocate new ItemIds."""

from arolla import arolla
from koladata.operators import jagged_shape
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import qtypes


P = arolla.P


@optools.add_to_registry(aliases=['kde.new_itemid_shaped'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_itemid_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_itemid_shaped(shape):
  """Allocates new ItemIds of the given shape without any DataBag attached."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_itemid'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_itemid', deterministic=False
)
def new_itemid():
  """Allocates new ItemId."""
  return new_itemid_shaped(jagged_shape.create_shape())


@optools.add_to_registry(aliases=['kde.new_itemid_like'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_itemid_like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_itemid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_itemid_shaped_as'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_itemid_shaped_as', deterministic=False
)
def new_itemid_shaped_as(shape_from):
  """Allocates new ItemIds with the shape of shape_from."""
  return new_itemid_shaped(jagged_shape.get_shape(shape_from))


@optools.add_to_registry(aliases=['kde.new_listid_shaped'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_listid_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_listid_shaped(shape):  # pylint: disable=unused-argument
  """Allocates new List ItemIds of the given shape."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_listid'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_listid', deterministic=False
)
def new_listid():
  """Allocates new List ItemId."""
  return new_listid_shaped(jagged_shape.create_shape())


@optools.add_to_registry(aliases=['kde.new_listid_like'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_listid_like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_listid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new List ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_listid_shaped_as'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_listid_shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
    ],
    deterministic=False,
)
def new_listid_shaped_as(shape_from):  # pylint: disable=unused-argument
  """Allocates new List ItemIds with the shape of shape_from."""
  return new_listid_shaped(jagged_shape.get_shape(shape_from))


@optools.add_to_registry(aliases=['kde.new_dictid_shaped'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_dictid_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_dictid_shaped(shape):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds of the given shape."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_dictid'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_dictid', deterministic=False
)
def new_dictid():
  """Allocates new Dict ItemId."""
  return new_dictid_shaped(jagged_shape.create_shape())


@optools.add_to_registry(aliases=['kde.new_dictid_like'])
@optools.as_unified_backend_operator(
    'kde.allocation.new_dictid_like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    deterministic=False,
)
def new_dictid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.new_dictid_shaped_as'])
@optools.as_unified_lambda_operator(
    'kde.allocation.new_dictid_shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
    ],
    deterministic=False,
)
def new_dictid_shaped_as(shape_from):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds with the shape of shape_from."""
  return new_dictid_shaped(jagged_shape.get_shape(shape_from))
