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

"""DictItem."""

from typing import Any

from arolla import arolla
from koladata.types import data_item
from koladata.types import data_slice


@data_slice.register_reserved_class_method_names
class DictItem(data_item.DataItem):
  """DictItem is a DataItem representing a Koda Dict."""

  def pop(self, key: Any) -> data_item.DataItem:
    """Removes an item from the DictItem."""
    if key not in self:
      raise KeyError(key)
    self[key] = None

  def __len__(self) -> int:
    return self.dict_size().internal_as_py()

  def __iter__(self):
    return (data_slice.DataSlice.from_vals(k)
            for k in self.get_keys().internal_as_py())

  def __contains__(self, key: Any):
    return not self[key].is_empty()

  # NOTE: DictItem.clear is inherited from DataSlice.clear.


arolla.abc.register_qvalue_specialization(
    '::koladata::python::DictItem', DictItem
)
