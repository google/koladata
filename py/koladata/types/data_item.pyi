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

from typing import Any, Callable
from arolla import arolla
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import type_defs


class DataItem(data_slice.DataSlice):
    def __call__(self, *args: Any, return_type_as: Any = ..., **kwargs: Any) -> data_slice.DataSlice | arolla.Expr: ...
    def bind(self, *args: Any, return_type_as: Any = ..., **kwargs: Any) -> DataItem: ...

    def __float__(self) -> float: ...
    def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    def __hash__(self) -> int: ...

    # Methods inherited from DataSlice that return DataItem for DataItem, but
    # don't return Self for all DataSlice sub-classes (e.g. ListItem, DictItem).
    # We aim to make this list exhaustive, but it's a best effort approach.
    def list_size(self) -> DataItem: ...
    def dict_size(self) -> DataItem: ...
    def get_size(self) -> DataItem: ...

    def get_itemid(self) -> DataItem: ...
    def get_present_count(self) -> DataItem: ...

def register_bind_method_implementation(
    fn: Callable[..., DataItem],
): ...
