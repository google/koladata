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

"""ListItem."""

from typing import Any

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.types import data_item
from koladata.types import data_slice


_eval_op = py_expr_eval_py_ext.eval_op


@data_slice.register_reserved_class_method_names
class ListItem(data_item.DataItem):
  """ListItem is a DataItem representing a Koda List."""

  def pop(self, index: int = -1) -> data_item.DataItem:
    """Removes an item from the ListItem."""
    l = len(self)
    if not isinstance(index, int):
      try:
        index = int(index)
      except ValueError:
        raise ValueError(
            f'{index!r} cannot be interpreted as an integer'
        ) from None
    if (index < 0 and abs(index) > l) or (index > 0 and index >= l):
      raise IndexError(
          f'List index out of range: list size {l} vs index {index}'
      )
    return super(ListItem, self).pop(index)

  def __len__(self) -> int:
    return self.list_size().internal_as_py()

  def __iter__(self):
    return (self[i] for i in range(len(self)))

  def __contains__(self, key: Any) -> bool:
    return bool(_eval_op('kd.isin', key, self[:]))

  # NOTE: ListItem.clear is inherited from DataSlice.clear.


arolla.abc.register_qvalue_specialization(
    '::koladata::python::ListItem', ListItem
)
