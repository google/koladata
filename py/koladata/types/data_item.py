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

from koladata.types import data_item_py_ext as _data_item_py_ext
# Used to initialize DataSlice, so it is available when defining subclasses of
# DataItem.
from koladata.types import data_slice as _  # pylint: disable=unused-import


DataItem = _data_item_py_ext.DataItem


### Implementation of the DataItem's additional functionality.
DataItem.__hash__ = lambda self: hash(self.fingerprint)
