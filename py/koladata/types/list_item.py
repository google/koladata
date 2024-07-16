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

"""ListItem."""

from arolla import arolla
from koladata.types import data_item
from koladata.types import data_slice


@data_slice.register_reserved_class_method_names
class ListItem(data_item.DataItem):
  """ListItem is a DataItem representing a Koda List."""

  def pop(self, i: int = -1) -> data_item.DataItem:
    """Removes an item from the ListItem."""
    l = len(self)
    if not isinstance(i, int):
      try:
        i = int(i)
      except ValueError:
        raise ValueError(f'{i!r} cannot be interpreted as an integer') from None
    if (i < 0 and abs(i) > l) or (i > 0 and i >= l):
      raise IndexError(f'List index out of range: list size {l} vs index {i}')
    return self._internal_pop(i)

  def __len__(self) -> int:
    return self.list_size().internal_as_py()

  def __iter__(self):
    return (self[i] for i in range(len(self)))


arolla.abc.register_qvalue_specialization(
    '::koladata::python::ListItem', ListItem
)
