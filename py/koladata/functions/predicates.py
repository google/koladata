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

"""Koda predicate functions."""

from typing import Any

from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice


def is_item(obj: Any) -> bool:
  """Returns True if the given object is a scalar DataItem."""
  if not isinstance(obj, data_slice.DataSlice):
    return False
  return obj.get_ndim() == 0
