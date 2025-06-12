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

"""Koda predicate functions."""

from typing import Any

from arolla import arolla
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice
from koladata.types import mask_constants


def is_item(obj: Any) -> data_slice.DataSlice:
  """Returns kd.present if the given object is a scalar DataItem and kd.missing otherwise."""
  if not isinstance(obj, data_slice.DataSlice) or obj.get_ndim() != 0:
    return mask_constants.missing
  return mask_constants.present


def is_expr(obj: Any) -> data_slice.DataSlice:
  """Returns kd.present if the given object is an Expr and kd.missing otherwise."""
  if isinstance(obj, arolla.abc.Expr):
    return mask_constants.present
  return mask_constants.missing


def is_slice(obj: Any) -> data_slice.DataSlice:
  """Returns kd.present if the given object is a DataSlice and kd.missing otherwise."""
  if isinstance(obj, data_slice.DataSlice):
    return mask_constants.present
  return mask_constants.missing
