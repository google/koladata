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

"""Flags to configure the global cache for persisted incremental data."""

from absl import flags

DEFAULT_MAX_SIZE_MB = 1024 * 10  # 10 GiB

KD_EXT_STORAGE_GLOBAL_CACHE_MAX_SIZE_MB = flags.DEFINE_integer(
    'kd_ext_storage_global_cache_max_size_mb',
    DEFAULT_MAX_SIZE_MB,
    'The maximum size of the kd_ext.storage global cache in megabytes.'
    ' The cache resides in RAM.',
)
