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

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from koladata.ext.persisted_data import global_cache_lib
from koladata.ext.persisted_data import lru_size_tracking_cache


class GlobalCacheLibTest(absltest.TestCase):

  def test_get_global_cache_return_type(self):
    cache = global_cache_lib.get_global_cache()
    self.assertIsInstance(cache, lru_size_tracking_cache.LruSizeTrackingCache)

  def test_get_global_cache_repeated_calls_return_same_object(self):
    result1 = global_cache_lib.get_global_cache()
    result2 = global_cache_lib.get_global_cache()

    self.assertIs(result1, result2)

  def test_global_cache_default_max_size(self):
    self.assertEqual(
        global_cache_lib.get_global_cache().get_max_total_bytes_of_entries_in_cache(),
        global_cache_lib._DEFAULT_MAX_SIZE_MB * 1024 * 1024,
    )

  @flagsaver.flagsaver()
  def test_kd_ext_persisted_data_global_cache_max_size_mb_flag_validation(self):
    with self.assertRaises(flags._exceptions.IllegalFlagValueError):
      flags.FLAGS.kd_ext_persisted_data_global_cache_max_size_mb = -1

  @flagsaver.flagsaver()
  def test_kd_ext_persisted_data_global_cache_max_size_mb_flag_updates_cache(
      self,
  ):
    self.assertNotEqual(
        global_cache_lib.get_global_cache().get_max_total_bytes_of_entries_in_cache(),
        42 * 1024 * 1024,
    )
    flags.FLAGS.kd_ext_persisted_data_global_cache_max_size_mb = 42
    self.assertEqual(
        global_cache_lib.get_global_cache().get_max_total_bytes_of_entries_in_cache(),
        42 * 1024 * 1024,
    )


if __name__ == '__main__':
  absltest.main()
