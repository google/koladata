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

"""LRU cache Object/Entity -> DataSlice.
"""

from collections.abc import Callable
import enum
import functools
from koladata.caching import clib
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import tracing_mode
from koladata.types import data_slice


DataSlice = data_slice.DataSlice
DsFn = Callable[[DataSlice], DataSlice]
_eval_op = py_expr_eval_py_ext.eval_op


class LruCache:
  """LRU cache Object/Entity -> DataSlice."""

  __slots__ = ('_cache',)

  class Mode(enum.Enum):
    """Mode for LruCache."""

    # Low performance overhead, unpredictable memory usage.
    # (no extraction on __setitem__)
    CAPACITY_IS_ELEMENT_COUNT = 0

    # High performance overhead, predictable memory usage.
    # (extraction + size estimation on __setitem__)
    CAPACITY_IS_BYTE_SIZE = 1

  def __init__(self, capacity: int, mode: Mode):
    self._cache = clib.LruCache(
        capacity,
        extract_and_track_size=(mode == self.Mode.CAPACITY_IS_BYTE_SIZE),
    )

  def __getitem__(self, keys: DataSlice) -> DataSlice:
    return self._cache[keys]

  def __setitem__(self, keys: DataSlice, vals: DataSlice) -> None:
    self._cache[keys] = vals

  def clear(self) -> None:
    self._cache.clear()

  def cache_fn(
      self, func_id: str, is_hit_fn: DsFn | None = None
  ) -> Callable[[DsFn], DsFn]:
    """Caching decorator for DataSlice->DataSlice functions.

    In case of a partial cache hit, the newly calculated results and the cached
    results will be interleaved directly using '|', so the schema it required
    to be stable between runs.

    Example:
      cache = kd.caching.LruCache(capacity=10)

      @cache.cache_fn('my_func')
      def fn(x):
        print(f'arg: {x}')
        return x * x

      print('[2, 3] ->', fn(kd.slice([2, 3])))  # no cache hit
      # arg: DataSlice([2, 3])
      # [2, 3] -> DataSlice([4, 9])
      print('[3, 4] ->', fn(kd.slice([3, 4])))  # partial cache hit
      # arg: DataSlice([None, 4])
      # [3, 4] -> DataSlice([9, 16])
      print('[3, 4] ->', fn(kd.slice([3, 4])))  # full cache hit
      # [3, 4] -> DataSlice([9, 16])

    Args:
      func_id: Function id is added to each key. It is needed to avoid
        collisions if one LruCache is used for caching results of different
        functions. If func_id is reused by two function, then results of one
        function will override results of another function in the cache.
      is_hit_fn: Optional function returning MASK DataSlice. If specified and
        returns kd.missing for some value, then this value will not be
        considered a cache hit.

    Returns:
      Decorator function.
    """

    func_uuid = _eval_op('kd.deep_uuid', func_id)

    def decorator_fn(fn: DsFn) -> DsFn:

      @functools.wraps(fn)
      def wrapped_fn(args: DataSlice) -> DataSlice:
        if tracing_mode.is_tracing_enabled():
          raise AssertionError('LruCache.cache_fn can\'t be used with tracing')
        keys = _eval_op(
            'kd.uuid', func=func_uuid, args=_eval_op('kd.deep_uuid', args)
        )
        cached_res = self._cache[keys]
        if is_hit_fn is not None and not cached_res.is_empty():
          cached_res = cached_res & is_hit_fn(cached_res)
        fltr = _eval_op('kd.has_not', cached_res)
        uncached_args = args & fltr

        if uncached_args.is_empty():
          return cached_res

        uncached_keys = keys & fltr
        uncached_res = fn(uncached_args)
        res = cached_res | uncached_res
        if is_hit_fn is not None and not uncached_res.is_empty():
          uncached_res = uncached_res & is_hit_fn(uncached_res)
        self._cache[uncached_keys] = uncached_res
        return res

      return wrapped_fn

    return decorator_fn
