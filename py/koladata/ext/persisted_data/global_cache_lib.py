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

"""Global cache for persisted incremental data."""

from absl import flags
from koladata import kd
from koladata.ext.persisted_data import lru_size_tracking_cache as _lru_size_tracking_cache

_DEFAULT_MAX_SIZE_MB = 1024 * 10  # 10 GiB
_mb_to_bytes = lambda mb: mb * 1024 * 1024

KD_EXT_PERSISTED_DATA_GLOBAL_CACHE_MAX_SIZE_MB = flags.DEFINE_integer(
    'kd_ext_persisted_data_global_cache_max_size_mb',
    _DEFAULT_MAX_SIZE_MB,
    'The maximum size of the kd_ext.persisted_data global cache in megabytes.',
)


def _kd_ext_persisted_data_global_cache_max_size_mb_validator(
    value: int,
) -> bool:
  if value < 0:
    return False
  get_global_cache().set_max_total_bytes_of_entries_in_cache(
      _mb_to_bytes(value)
  )
  return True


flags.register_validator(
    'kd_ext_persisted_data_global_cache_max_size_mb',
    _kd_ext_persisted_data_global_cache_max_size_mb_validator,
    message='Must be a non-negative number.',
    flag_values=flags.FLAGS,
)


CACHE_VALUE_TYPE = kd.types.DataBag | kd.types.DataSlice

Cache = _lru_size_tracking_cache.LruSizeTrackingCache
CacheEntryMetadata = _lru_size_tracking_cache.CacheEntryMetadata

_CACHE: Cache[str, CACHE_VALUE_TYPE] = Cache(
    max_total_bytes_of_entries_in_cache=_mb_to_bytes(_DEFAULT_MAX_SIZE_MB)
)


def get_global_cache() -> Cache[str, CACHE_VALUE_TYPE]:
  """Returns the global cache."""
  return _CACHE
