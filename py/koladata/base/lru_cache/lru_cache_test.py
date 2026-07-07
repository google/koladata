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

"""Tests for Koda LruCache wrapper."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.base.lru_cache import lru_cache


class LruCacheTest(parameterized.TestCase):

  def test_init_default_and_custom_capacity(self):
    cache_default = lru_cache.LruCache()
    self.assertIsNotNone(cache_default)
    cache_custom = lru_cache.LruCache(capacity=10)
    self.assertIsNotNone(cache_custom)

  def test_item_get_and_set(self):
    cache = lru_cache.LruCache()
    k1 = kd.uuid(x=1)
    v1 = kd.item(42)

    kd.testing.assert_equivalent(cache[k1], kd.item(None))

    cache[k1] = v1
    kd.testing.assert_equivalent(cache[k1], kd.item(42))

    cache[k1] = kd.item(100)
    kd.testing.assert_equivalent(cache[k1], kd.item(100))

  def test_item_invalid_key_type(self):
    cache = lru_cache.LruCache()
    with self.assertRaisesRegex(Exception, 'ObjectId expected'):
      _ = cache[kd.item(123)]
    with self.assertRaises(TypeError):
      _ = cache[123]
    with self.assertRaisesRegex(Exception, 'ObjectId expected'):
      _ = cache[kd.item('not_an_object_id')]
    with self.assertRaisesRegex(Exception, 'ObjectId expected'):
      cache[kd.item(123)] = kd.item(1)

  def test_missing_item_key(self):
    cache = lru_cache.LruCache()
    kd.testing.assert_equivalent(cache[kd.item(None)], kd.item(None))

    cache[kd.item(None)] = kd.item(42)
    kd.testing.assert_equivalent(cache[kd.item(None)], kd.item(None))

  def test_slice_get_and_set(self):
    cache = lru_cache.LruCache()
    k1 = kd.uuid(x=1)
    k2 = kd.uuid(x=2)
    k3 = kd.uuid(x=3)

    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2, k3])], kd.slice([None, None, None])
    )

    cache[kd.slice([k1, k2])] = kd.slice([10, 20])
    kd.testing.assert_equivalent(cache[k1], kd.item(10))
    kd.testing.assert_equivalent(cache[k2], kd.item(20))
    kd.testing.assert_equivalent(cache[k3], kd.item(None))

    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2, k3])], kd.slice([10, 20, None])
    )

  def test_slice_multidimensional(self):
    cache = lru_cache.LruCache()
    k1, k2, k3, k4 = kd.uuid(k=1), kd.uuid(k=2), kd.uuid(k=3), kd.uuid(k=4)
    keys_slice = kd.slice([[k1, k2], [k3, k4]])
    values_slice = kd.slice([[1, 2], [3, 4]])

    cache[keys_slice] = values_slice
    kd.testing.assert_equivalent(cache[keys_slice], values_slice)

  def test_slice_empty_and_unknown(self):
    cache = lru_cache.LruCache()
    kd.testing.assert_equivalent(cache[kd.slice([])], kd.slice([]))
    kd.testing.assert_equivalent(
        cache[kd.slice([None, None])], kd.slice([None, None])
    )

    cache[kd.slice([], kd.OBJECT)] = kd.slice([])

  def test_slice_invalid_key_type(self):
    cache = lru_cache.LruCache()
    with self.assertRaisesRegex(Exception, 'ObjectId expected'):
      _ = cache[kd.slice([1, 2])]
    with self.assertRaisesRegex(Exception, 'ObjectId expected'):
      cache[kd.slice([1, 2])] = kd.slice([3, 4])

  def test_slice_broadcasting(self):
    cache = lru_cache.LruCache()
    k1, k2, k3 = kd.uuid(b=1), kd.uuid(b=2), kd.uuid(b=3)
    cache[kd.slice([k1, k2, k3])] = kd.item(99)
    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2, k3])], kd.slice([99, 99, 99])
    )

  def test_slice_set_ignores_missing_values(self):
    cache = lru_cache.LruCache()
    k1, k2 = kd.uuid(m=1), kd.uuid(m=2)
    cache[k1] = kd.item(10)
    cache[kd.slice([k1, k2])] = kd.slice([None, 20])

    kd.testing.assert_equivalent(cache[k1], kd.item(10))
    kd.testing.assert_equivalent(cache[k2], kd.item(20))

  def test_schema_aggregation(self):
    cache = lru_cache.LruCache()
    k1, k2 = kd.uuid(s=1), kd.uuid(s=2)
    cache[k1] = kd.item(10, kd.INT32)
    cache[k2] = kd.item('hello', kd.STRING)

    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2])], kd.slice([10, 'hello'], kd.OBJECT)
    )

  def test_caching_objects_with_databags(self):
    cache = lru_cache.LruCache()
    k1, k2 = kd.uuid(o=1), kd.uuid(o=2)
    obj1 = kd.obj(alpha=100, beta='hello')
    obj2 = kd.obj(alpha=200, beta='world')

    cache[k1] = obj1
    cache[k2] = obj2

    res1 = cache[k1]
    kd.testing.assert_equivalent(res1.alpha, kd.item(100))
    kd.testing.assert_equivalent(res1.beta, kd.item('hello'))

    res_slice = cache[kd.slice([k1, k2])]
    kd.testing.assert_equivalent(res_slice.alpha, kd.slice([100, 200]))
    kd.testing.assert_equivalent(
        res_slice.beta, kd.slice(['hello', 'world'])
    )

  def test_clear(self):
    cache = lru_cache.LruCache()
    k1, k2 = kd.uuid(c=1), kd.uuid(c=2)
    cache[kd.slice([k1, k2])] = kd.slice([1, 2])
    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2])], kd.slice([1, 2])
    )

    cache.clear()
    kd.testing.assert_equivalent(
        cache[kd.slice([k1, k2])], kd.slice([None, None])
    )

  def test_lru_eviction(self):
    cache = lru_cache.LruCache(capacity=2)
    k1, k2, k3 = kd.uuid(e=1), kd.uuid(e=2), kd.uuid(e=3)

    cache[k1] = kd.item('first')
    cache[k2] = kd.item('second')

    # Access k1 so k2 becomes least recently used.
    _ = cache[k1]

    cache[k3] = kd.item('third')

    kd.testing.assert_equivalent(cache[k1], kd.item('first'))
    kd.testing.assert_equivalent(cache[k2], kd.item(None))
    kd.testing.assert_equivalent(cache[k3], kd.item('third'))


if __name__ == '__main__':
  absltest.main()
