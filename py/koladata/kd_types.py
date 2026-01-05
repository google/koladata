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

"""Types used as type annotations in users's code."""

from arolla import arolla as _arolla
from koladata.functor.parallel import clib as _functor_parallel_clib
from koladata.types import data_bag as _data_bag
from koladata.types import data_item as _data_item
from koladata.types import data_slice as _data_slice
from koladata.types import dict_item as _dict_item
from koladata.types import iterable_qvalue as _iterable_qvalue
from koladata.types import jagged_shape as _jagged_shape
from koladata.types import list_item as _list_item
from koladata.types import schema_item as _schema_item

DataBag = _data_bag.DataBag
DataItem = _data_item.DataItem
DataSlice = _data_slice.DataSlice
DictItem = _dict_item.DictItem
Executor = _functor_parallel_clib.Executor
Expr = _arolla.Expr
Iterable = _iterable_qvalue.Iterable
JaggedShape = _jagged_shape.JaggedShape
ListItem = _list_item.ListItem
SchemaItem = _schema_item.SchemaItem
Stream = _functor_parallel_clib.Stream
StreamReader = _functor_parallel_clib.StreamReader
StreamWriter = _functor_parallel_clib.StreamWriter
