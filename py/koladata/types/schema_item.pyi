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

from typing import Any, cast, overload, Union
from arolla import arolla
from koladata.types import data_item
from koladata.types import data_slice


class SchemaItem(data_item.DataItem):
  def get_nofollowed_schema(self) -> data_item.DataItem: ...

  @overload
  def new(self, **attrs: data_slice.DataSlice | int | float | str) -> data_slice.DataSlice: ...
  @overload
  def new(self, **attrs: Any) -> data_slice.DataSlice | arolla.Expr: ...
  @overload
  def strict_new(self, **attrs: data_slice.DataSlice | int | float | str) -> data_slice.DataSlice: ...
  @overload
  def strict_new(self, **attrs: Any) -> data_slice.DataSlice | arolla.Expr: ...
  def get_attr(
      self, attr_name: Union[str, data_slice.DataSlice], /, default: Any = None
  ) -> Union[data_slice.DataSlice, 'SchemaItem']: ...
  @overload
  def get_attr(
      self, attr_name: data_slice.DataSlice, /, default: Any = None
  ) -> data_slice.DataSlice: ...
  @overload
  def get_attr(self, attr_name: str, /, default: Any = None) -> SchemaItem: ...

  def get_item_schema(self) -> SchemaItem: ...
  def get_key_schema(self) -> SchemaItem: ...
  def get_value_schema(self) -> SchemaItem: ...
