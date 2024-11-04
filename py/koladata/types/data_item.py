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

"""DataItem abstraction."""

from typing import Any

from arolla import arolla
from koladata.types import data_item_py_ext as _data_item_py_ext
# Used to initialize DataSlice, so it is available when defining subclasses of
# DataItem.
from koladata.types import data_slice
from koladata.types import operator_lookup


_op_impl_lookup = operator_lookup.OperatorLookup()

DataItem = _data_item_py_ext.DataItem


### Implementation of the DataItem's additional functionality.
@DataItem._add_method('__hash__')  # pylint: disable=protected-access
def _hash(self) -> int:
  return hash(self.fingerprint)


# Ideally we'd do this only for functors, but we don't have a fast way
# to check if a DataItem is a functor now. Note that SchemaItem overrides
# this behavior.
@DataItem._add_method('__call__')  # pylint: disable=protected-access
def _call(
    self, *args: Any, return_type_as: Any = data_slice.DataSlice, **kwargs: Any
) -> data_slice.DataSlice:
  if any(isinstance(arg, arolla.Expr) for arg in args) or any(
      isinstance(arg, arolla.Expr) for arg in kwargs.values()
  ):
    return _op_impl_lookup.call(
        self, *args, return_type_as=return_type_as, **kwargs
    )
  else:
    return arolla.abc.aux_eval_op(
        _op_impl_lookup.call,
        self,
        *args,
        return_type_as=return_type_as,
        **kwargs
    )
