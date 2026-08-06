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
from absl.testing import parameterized
from koladata import kd
from koladata.caching import lru_cache


class LruCacheTest(parameterized.TestCase):

  def test_delegation(self):
    cache = lru_cache.LruCache(10)
    k1 = kd.uuid(x=1)
    v1 = kd.item(42)

    kd.testing.assert_equivalent(cache[k1], kd.item(None))
    cache[k1] = v1
    kd.testing.assert_equivalent(cache[k1], v1)

    cache.clear()
    kd.testing.assert_equivalent(cache[k1], kd.item(None))

  def test_cache_fn_basic(self):
    cache = lru_cache.LruCache(10)
    calls = []

    @cache.cache_fn('test_basic')
    def fn(x):
      calls.append(x)
      return x * 10

    with self.assertRaisesRegex(
        AssertionError, "LruCache.cache_fn can't be used with tracing"
    ):
      kd.trace_py_fn(fn)

    # First call with a slice of items: all misses.
    res1 = fn(kd.slice([1, 2, 3]))
    kd.testing.assert_equivalent(res1, kd.slice([10, 20, 30]))
    self.assertLen(calls, 1)
    kd.testing.assert_equivalent(calls[0], kd.slice([1, 2, 3]))

    # Second call with the same slice: all hits, fn not invoked.
    res2 = fn(kd.slice([1, 2, 3]))
    kd.testing.assert_equivalent(res2, kd.slice([10, 20, 30]))
    self.assertLen(calls, 1)

    # Third call with partial overlap: 2 and 3 are cached, 4 and 5 are misses.
    res3 = fn(kd.slice([2, 3, 4, 5]))
    kd.testing.assert_equivalent(res3, kd.slice([20, 30, 40, 50]))
    self.assertLen(calls, 2)
    # fn should only be invoked on the uncached elements (4 and 5).
    kd.testing.assert_equivalent(calls[1], kd.slice([None, None, 4, 5]))

    # Fourth call with all elements now in cache.
    res4 = fn(kd.slice([1, 2, 3, 4, 5]))
    kd.testing.assert_equivalent(res4, kd.slice([10, 20, 30, 40, 50]))
    self.assertLen(calls, 2)

  def test_cache_fn_empty_or_all_missing_args(self):
    cache = lru_cache.LruCache(10)
    calls = []

    @cache.cache_fn('test_empty')
    def fn(x):
      calls.append(x)
      return x * 2

    res1 = fn(kd.slice([]))
    kd.testing.assert_equivalent(res1, kd.slice([]))
    self.assertEmpty(calls)

    res2 = fn(kd.slice([None, None]))
    kd.testing.assert_equivalent(res2, kd.slice([None, None]))
    self.assertEmpty(calls)

  def test_cache_fn_func_id_separation(self):
    cache = lru_cache.LruCache(10)

    @cache.cache_fn('func_1')
    def fn1(x):
      return x + 10

    @cache.cache_fn('func_2')
    def fn2(x):
      return x * 10

    res1 = fn1(kd.slice([1, 2]))
    res2 = fn2(kd.slice([1, 2]))

    kd.testing.assert_equivalent(res1, kd.slice([11, 12]))
    kd.testing.assert_equivalent(res2, kd.slice([10, 20]))

  def test_cache_fn_func_id_reuse_override(self):
    cache = lru_cache.LruCache(10)
    calls1 = []
    calls2 = []

    @cache.cache_fn('shared_id')
    def fn1(x):
      calls1.append(x)
      return x + 100

    @cache.cache_fn('shared_id')
    def fn2(x):
      calls2.append(x)
      return x + 200

    # Populate cache via fn1.
    res1 = fn1(kd.slice([1, 2]))
    kd.testing.assert_equivalent(res1, kd.slice([101, 102]))
    self.assertLen(calls1, 1)
    self.assertEmpty(calls2)

    # Calling fn2 with the same inputs should hit the cache populated by fn1.
    res2 = fn2(kd.slice([1, 2]))
    kd.testing.assert_equivalent(res2, kd.slice([101, 102]))
    self.assertLen(calls1, 1)
    self.assertEmpty(calls2)

    # Calling fn2 with new inputs will populate the cache via fn2.
    res3 = fn2(kd.slice([3]))
    kd.testing.assert_equivalent(res3, kd.slice([203]))
    self.assertLen(calls1, 1)
    self.assertLen(calls2, 1)

    # Now calling fn1 with 3 will get fn2's cached result.
    res4 = fn1(kd.slice([3]))
    kd.testing.assert_equivalent(res4, kd.slice([203]))
    self.assertLen(calls1, 1)
    self.assertLen(calls2, 1)

    @cache.cache_fn('shared_id')
    def fn3(x):
      return kd.new(x='incompatible') & kd.has(x)

    with self.assertRaisesRegex(
        ValueError, 'arguments do not have a common schema'
    ):
      _ = fn3(kd.slice([1, 4]))

  def test_cache_fn_is_hit_fn(self):
    cache = lru_cache.LruCache(10)
    calls = []

    # Only cache positive results.
    is_hit_fn = lambda x: kd.has(x) & (x > 0)

    @cache.cache_fn('test_hit_fn', is_hit_fn=is_hit_fn)
    def fn(x):
      calls.append(x)
      return x

    # First call: `fn` returns -5 without caching since it is < 0.
    res1 = fn(kd.slice([1, -5, 2]))
    kd.testing.assert_equivalent(res1, kd.slice([1, -5, 2]))
    self.assertLen(calls, 1)
    kd.testing.assert_equivalent(calls[0], kd.slice([1, -5, 2]))

    # Second call: re-invocation of `fn` for -5.
    res2 = fn(kd.slice([1, -5, 2]))
    kd.testing.assert_equivalent(res2, kd.slice([1, -5, 2]))
    self.assertLen(calls, 2)
    kd.testing.assert_equivalent(calls[1], kd.slice([None, -5, None]))

  def test_cache_fn_is_hit_fn_on_lookup(self):
    cache = lru_cache.LruCache(10)
    calls = []
    min_val = 0

    is_hit_fn = lambda x: kd.has(x) & (x > min_val)

    @cache.cache_fn('test_hit_fn_lookup', is_hit_fn=is_hit_fn)
    def fn(x):
      calls.append(x)
      return x

    res1 = fn(kd.slice([1, 2, 3]))
    kd.testing.assert_equivalent(res1, kd.slice([1, 2, 3]))
    self.assertLen(calls, 1)

    # Now increase min_val to 2. Values 1 and 2 in cache should no longer be
    # considered hits by is_hit_fn on lookup, triggering re-computation.
    min_val = 2
    res2 = fn(kd.slice([1, 2, 3]))
    kd.testing.assert_equivalent(res2, kd.slice([1, 2, 3]))
    self.assertLen(calls, 2)
    kd.testing.assert_equivalent(calls[1], kd.slice([1, 2, None]))

  def test_cache_fn_with_objects_and_entities(self):
    cache = lru_cache.LruCache(10)
    calls = []

    @cache.cache_fn('test_objects')
    def fn(obj):
      calls.append(obj)
      return obj.x + 100

    obj1 = kd.obj(x=10, y='a')
    obj2 = kd.obj(x=10, y='a')  # Different ItemId, but identical content.

    res1 = fn(obj1)
    kd.testing.assert_equivalent(res1, kd.item(110))
    self.assertLen(calls, 1)

    # Calling with obj2 should hit the cache due to matching deep_uuid.
    res2 = fn(obj2)
    kd.testing.assert_equivalent(res2, kd.item(110))
    self.assertLen(calls, 1)

    obj3 = kd.obj(x=20, y='b')
    res3 = fn(kd.slice([obj1, obj3]))
    kd.testing.assert_equivalent(res3, kd.slice([110, 120]))
    self.assertLen(calls, 2)
    kd.testing.assert_equivalent(calls[1].x, kd.slice([None, 20]))

  def test_cache_fn_multidimensional_slice(self):
    cache = lru_cache.LruCache(10)
    calls = []

    @cache.cache_fn('test_multidim')
    def fn(x):
      calls.append(x)
      return x * 10

    res1 = fn(kd.slice([[1, 2], [3, 4]]))
    kd.testing.assert_equivalent(res1, kd.slice([[10, 20], [30, 40]]))
    self.assertLen(calls, 1)

    # Partial overlap across dimensions.
    res2 = fn(kd.slice([[2, 3], [4, 5]]))
    kd.testing.assert_equivalent(res2, kd.slice([[20, 30], [40, 50]]))
    self.assertLen(calls, 2)
    kd.testing.assert_equivalent(
        calls[1], kd.slice([[None, None], [None, 5]])
    )

  def test_cache_fn_eviction(self):
    cache = lru_cache.LruCache(capacity=2)
    calls = []

    @cache.cache_fn('test_eviction')
    def fn(x):
      calls.append(x)
      return x * 10

    fn(kd.slice([1, 2]))
    self.assertLen(calls, 1)

    # Accessing 3 causes eviction of 1 (the least recently used).
    fn(kd.slice([3]))
    self.assertLen(calls, 2)

    # Accessing 1 again should trigger a cache miss and invocation of fn.
    res = fn(kd.slice([1]))
    kd.testing.assert_equivalent(res, kd.slice([10]))
    self.assertLen(calls, 3)
    kd.testing.assert_equivalent(calls[2], kd.slice([1]))

  def test_cache_fn_update_wrapper(self):
    cache = lru_cache.LruCache(10)

    @cache.cache_fn('test_wrapper')
    def my_custom_fn(x):
      """My custom docstring."""
      return x

    self.assertEqual(my_custom_fn.__name__, 'my_custom_fn')
    self.assertEqual(my_custom_fn.__doc__, 'My custom docstring.')


if __name__ == '__main__':
  absltest.main()
