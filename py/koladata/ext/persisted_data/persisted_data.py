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

"""Tools for persisted incremental data."""

from koladata.ext.persisted_data import data_slice_manager_interface as _data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_manager_view as _data_slice_manager_view
from koladata.ext.persisted_data import data_slice_path as _data_slice_path
from koladata.ext.persisted_data import fs_implementation as _fs_implementation
from koladata.ext.persisted_data import fs_interface as _fs_interface
from koladata.ext.persisted_data import fs_util as _fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as _pidbm
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager as _pidsm

fs_interface = _fs_interface
fs_implementation = _fs_implementation
fs_util = _fs_util

BagToAdd = _pidbm.BagToAdd
PersistedIncrementalDataBagManager = _pidbm.PersistedIncrementalDataBagManager

data_slice_path = _data_slice_path
DataSlicePath = _data_slice_path.DataSlicePath
DataSliceManagerInterface = (
    _data_slice_manager_interface.DataSliceManagerInterface
)
DataSliceManagerView = _data_slice_manager_view.DataSliceManagerView
persisted_incremental_data_slice_manager = _pidsm
PersistedIncrementalDataSliceManager = (
    _pidsm.PersistedIncrementalDataSliceManager
)
