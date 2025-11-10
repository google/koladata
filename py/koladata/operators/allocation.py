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

"""Koda operators that allocate new ItemIds."""

from arolla import arolla as _arolla
from koladata.operators import jagged_shape as _jagged_shape
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils


_P = _arolla.P


@_optools.add_to_registry(
    aliases=['kd.new_itemid_shaped'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_itemid_shaped',
    qtype_constraints=[
        _qtype_utils.expect_jagged_shape(_P.shape),
    ],
    deterministic=False,
)
def new_itemid_shaped(shape):
  """Allocates new ItemIds of the given shape without any DataBag attached."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_itemid'], via_cc_operator_package=True
)
@_optools.as_lambda_operator('kd.allocation.new_itemid')
def new_itemid():
  """Allocates new ItemId."""
  return new_itemid_shaped(_jagged_shape.new())


@_optools.add_to_registry(
    aliases=['kd.new_itemid_like'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_itemid_like',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.shape_and_mask_from),
    ],
    deterministic=False,
)
def new_itemid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_itemid_shaped_as'], via_cc_operator_package=True
)
@_optools.as_lambda_operator('kd.allocation.new_itemid_shaped_as')
def new_itemid_shaped_as(shape_from):
  """Allocates new ItemIds with the shape of shape_from."""
  return new_itemid_shaped(_jagged_shape.get_shape(shape_from))


@_optools.add_to_registry(
    aliases=['kd.new_listid_shaped'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_listid_shaped',
    qtype_constraints=[
        _qtype_utils.expect_jagged_shape(_P.shape),
    ],
    deterministic=False,
)
def new_listid_shaped(shape):  # pylint: disable=unused-argument
  """Allocates new List ItemIds of the given shape."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_listid'], via_cc_operator_package=True
)
@_optools.as_lambda_operator('kd.allocation.new_listid')
def new_listid():
  """Allocates new List ItemId."""
  return new_listid_shaped(_jagged_shape.new())


@_optools.add_to_registry(
    aliases=['kd.new_listid_like'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_listid_like',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.shape_and_mask_from),
    ],
    deterministic=False,
)
def new_listid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new List ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_listid_shaped_as'], via_cc_operator_package=True
)
@_optools.as_lambda_operator(
    'kd.allocation.new_listid_shaped_as',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.shape_from),
    ],
)
def new_listid_shaped_as(shape_from):  # pylint: disable=unused-argument
  """Allocates new List ItemIds with the shape of shape_from."""
  return new_listid_shaped(_jagged_shape.get_shape(shape_from))


@_optools.add_to_registry(
    aliases=['kd.new_dictid_shaped'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_dictid_shaped',
    qtype_constraints=[
        _qtype_utils.expect_jagged_shape(_P.shape),
    ],
    deterministic=False,
)
def new_dictid_shaped(shape):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds of the given shape."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_dictid'], via_cc_operator_package=True
)
@_optools.as_lambda_operator('kd.allocation.new_dictid')
def new_dictid():
  """Allocates new Dict ItemId."""
  return new_dictid_shaped(_jagged_shape.new())


@_optools.add_to_registry(
    aliases=['kd.new_dictid_like'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.allocation.new_dictid_like',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.shape_and_mask_from),
    ],
    deterministic=False,
)
def new_dictid_like(shape_and_mask_from):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds with the shape and sparsity of shape_and_mask_from."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.new_dictid_shaped_as'], via_cc_operator_package=True
)
@_optools.as_lambda_operator(
    'kd.allocation.new_dictid_shaped_as',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.shape_from),
    ],
)
def new_dictid_shaped_as(shape_from):  # pylint: disable=unused-argument
  """Allocates new Dict ItemIds with the shape of shape_from."""
  return new_dictid_shaped(_jagged_shape.get_shape(shape_from))
