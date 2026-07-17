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

from typing import Any

from koladata.functor.parallel import clib as _functor_parallel_clib
from koladata.types import iterable_qvalue as _iterable_qvalue
from koladata.types import jagged_shape as _jagged_shape

# TODO: Fix the client-side type errors and then turn these Any
# into proper types.

DataBag: Any
DataItem: Any
DataSlice: Any
DictItem: Any
Executor = _functor_parallel_clib.Executor
Expr: Any
Iterable = _iterable_qvalue.Iterable
JaggedShape = _jagged_shape.JaggedShape
ListItem: Any
SchemaItem: Any
Stream = _functor_parallel_clib.Stream
StreamReader = _functor_parallel_clib.StreamReader
StreamWriter = _functor_parallel_clib.StreamWriter
