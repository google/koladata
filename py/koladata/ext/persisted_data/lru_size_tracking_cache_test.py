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

from absl.testing import absltest
from koladata.ext.persisted_data import lru_size_tracking_cache


def bytes_estimate(x: int) -> lru_size_tracking_cache.CacheEntryMetadata:
  return lru_size_tracking_cache.CacheEntryMetadata(num_bytes_estimate=x)


class LruSizeTrackingCacheTest(absltest.TestCase):

  def test_basic_functionality(self):
    cache: lru_size_tracking_cache.LruSizeTrackingCache[str, int] = (
        lru_size_tracking_cache.LruSizeTrackingCache(
            max_total_bytes_of_entries_in_cache=10,
        )
    )
    # Real applications would provide estimates of the whole entries, i.e. that
    # includes the key and the value. Here, for simplicity, we use "size
    # estimates" that are simply the values themselves.
    cache.set('a', 1, bytes_estimate(1))
    cache.set('b', 2, bytes_estimate(2))
    cache.set('c', 3, bytes_estimate(3))
    self.assertEqual(cache.get('a'), 1)
    self.assertEqual(cache.get('b'), 2)
    self.assertEqual(cache.get('c'), 3)
    self.assertIsNone(cache.get('d'))
    self.assertEqual(cache.get_total_bytes_of_entries_in_cache(), 6)

    # Add a new entry to reach the maximum size allowed.
    cache.set('d', 4, bytes_estimate(4))
    self.assertEqual(cache.get('a'), 1)
    self.assertEqual(cache.get('b'), 2)
    self.assertEqual(cache.get('c'), 3)
    self.assertEqual(cache.get('d'), 4)
    self.assertEqual(cache.get_total_bytes_of_entries_in_cache(), 10)
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 10)

    # Adding a new entry now evicts the least recently used entries until the
    # new value fits.
    cache.set('e', 5, bytes_estimate(5))
    self.assertIsNone(cache.get('a'))
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertEqual(cache.get('d'), 4)
    self.assertEqual(cache.get('e'), 5)
    self.assertEqual(cache.get_total_bytes_of_entries_in_cache(), 9)

    # There's still space to add an entry with size 1.
    cache.set('a', 1, bytes_estimate(1))
    self.assertEqual(cache.get('a'), 1)
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertEqual(cache.get('d'), 4)
    self.assertEqual(cache.get('e'), 5)

    # We can reduce the maximum size of the cache. LRU entries are evicted to
    # respect the new maximum size.
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 10)
    cache.set_max_total_bytes_of_entries_in_cache(8)
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 8)
    # Although 'a' is the most recently added entry, it is evicted because it is
    # the least recently used - the calls to get() above used 'b' to 'e' more
    # recently than 'a'.
    self.assertIsNone(cache.get('a'))
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertIsNone(cache.get('d'))
    self.assertEqual(cache.get('e'), 5)

    # Bumping the maximum size does not evict any entries.
    cache.set_max_total_bytes_of_entries_in_cache(10)
    self.assertIsNone(cache.get('a'))
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertIsNone(cache.get('d'))
    self.assertEqual(cache.get('e'), 5)

    # We can associate a key with a new value.
    cache.set('a', 2, bytes_estimate(2))
    self.assertEqual(cache.get('a'), 2)
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertIsNone(cache.get('d'))
    self.assertEqual(cache.get('e'), 5)

    # But if the new entry is too large, it is not added to the cache. Any
    # previous value associated with the key will still be evicted.
    cache.set('a', 100, bytes_estimate(100))
    self.assertIsNone(cache.get('a'))
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertIsNone(cache.get('d'))
    self.assertEqual(cache.get('e'), 5)

    # We can clear the cache.
    cache.clear()
    self.assertIsNone(cache.get('a'))
    self.assertIsNone(cache.get('b'))
    self.assertIsNone(cache.get('c'))
    self.assertIsNone(cache.get('d'))
    self.assertIsNone(cache.get('e'))

    # Setting the max size to a negative value raises an error.
    with self.assertRaises(ValueError):
      cache.set_max_total_bytes_of_entries_in_cache(-1)
    # The max size is not changed.
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 10)

    # Using zero is fine and evicts all entries, thereby disabling caching.
    cache.set_max_total_bytes_of_entries_in_cache(0)
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 0)

    # Creating a new cache with a negative max size raises an error.
    with self.assertRaises(ValueError):
      lru_size_tracking_cache.LruSizeTrackingCache(
          max_total_bytes_of_entries_in_cache=-1,
      )

    # We can create a cache with zero max size. Such a cache is not very useful
    # unless we plan to increase the max size later.
    cache = lru_size_tracking_cache.LruSizeTrackingCache(
        max_total_bytes_of_entries_in_cache=0,
    )
    self.assertEqual(cache.get_max_total_bytes_of_entries_in_cache(), 0)


if __name__ == '__main__':
  absltest.main()
