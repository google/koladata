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

"""Facade module exposing selected functionality of the persisted_data package."""

from koladata.ext.persisted_data import fs_implementation as _fs_implementation
from koladata.ext.persisted_data import fs_interface as _fs_interface
from koladata.ext.persisted_data import fs_util as _fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as _pidbm

fs_interface = _fs_interface
fs_implementation = _fs_implementation
fs_util = _fs_util
PersistedIncrementalDataBagManager = _pidbm.PersistedIncrementalDataBagManager
