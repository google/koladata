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

"""An LRU cache that tracks the size of the entries in the cache."""

import collections
import dataclasses
import threading
from typing import Generic, TypeVar

K = TypeVar('K', bound=collections.abc.Hashable)
V = TypeVar('V')


@dataclasses.dataclass(frozen=True, kw_only=True)
class CacheEntryMetadata:
  # The estimated size of the entry in bytes.
  num_bytes_estimate: int


@dataclasses.dataclass(frozen=True, kw_only=True)
class _CacheValueAndMetadata(Generic[V]):
  value: V
  metadata: CacheEntryMetadata


class LruSizeTrackingCache(Generic[K, V]):
  """An LRU cache that tracks the size of the entries in the cache.

  The cache is thread-safe.

  Implementation note: this class has a caching policy that is baked in. In
  the future, we might want to make the policy configurable. For example, we
  could have a dataclass InternalCacheEntryMetadata that stores a
  CacheEntryMetadata object an a timestamp of the most recent access, etc. By
  having a CachingPolicy interface, with a clear protocol between this class and
  CachingPolicy, we could allow configurable caching policies that implement
  LRU with size tracking, etc. If we augment CacheEntryMetadata with e.g. an
  estimated cost of creation/loading, then we could support configurable
  policies that balance the computation/memory trade-off. For example, a simple
  policy might simply decide never to add an entry to the cache if its
  creation/loading cost is smaller than say 1 second. We could also add a
  field to CacheEntryMetadata to indicate the relative priority of the entry,
  and then have policies that always caches and never evicts entries with a
  "cache forever" priority. These are just examples. Time will tell what kind of
  policies we shall need.
  """

  _rlock: threading.RLock

  # All attributes below are protected by _rlock.
  _max_total_bytes_of_entries_in_cache: int
  # The code relies on the fact that dicts preserve insertion order, which is
  # true of all Python versions supported by Koda.
  _cache: dict[K, _CacheValueAndMetadata[V]]
  _total_bytes_of_entries_in_cache: int

  def __init__(
      self,
      *,
      max_total_bytes_of_entries_in_cache: int,
  ):
    """Initializes the cache.

    Args:
      max_total_bytes_of_entries_in_cache: The maximum total size of the entries
        that the cache should hold. The cache will evict the least recently used
        entries to ensure that it never exceeds this limit.
    """
    if max_total_bytes_of_entries_in_cache < 0:
      raise ValueError(
          'max_total_bytes_of_entries_in_cache must be non-negative, but is'
          f' {max_total_bytes_of_entries_in_cache}'
      )

    self._rlock = threading.RLock()
    self._max_total_bytes_of_entries_in_cache = (
        max_total_bytes_of_entries_in_cache
    )
    self._cache = {}
    self._total_bytes_of_entries_in_cache = 0

  def get(self, key: K) -> V | None:
    """Returns the value associated with the key, or None if the key is not in the cache."""
    with self._rlock:
      try:
        value_and_metadata = self._cache.pop(key)
      except KeyError:
        return None
      # Insert again to mark as most recently used in the order.
      self._cache[key] = value_and_metadata
      return value_and_metadata.value

  def set(self, key: K, value: V, metadata: CacheEntryMetadata) -> V:
    """Adds the given key-value pair to the cache if applicable.

    This method assumes that the same key is always associated with the same
    value and metadata. It might happen that two threads simultaneously look up
    a key in the cache, see that it is not there, proceed to create a large
    value, and store the key-value pair in the cache. In such a situation the
    two values are conceptually equivalent, but they will typically be two
    different objects in memory and each could consume significant amounts of
    RAM.

    This cache does not try to coordinate between the different threads, i.e. it
    does not have a mechanism to register the intent to provide a value for a
    key. Such a scheme is possible, but rather complex to implement, because if
    the thread that is supposed to create the value fails for some reason, then
    we need to detect that and notify a waiting thread that it should try to
    create the value instead.

    So instead of a complex coordination mechanism, we use a simpler protocol:
    the two threads might both create values, which can temporarily increase the
    RAM usage. Both threads will call this set method, which uses a lock to let
    one thread proceed at a time. The first thread to set will actually set the
    value in the cache. The second thread's value is not set, and the value that
    is already in the cache is returned. The protocol on the caller side is that
    it should always use the returned value from this method. In that way, the
    long term RAM usage will be kept low, because the duplicate value will be
    available for garbage collection. For example, a typical caller can do:

    value = cache.set(key, value, metadata)

    if the `value` is available in a local variable, and the caller might have
    to propagate `value` back to its caller in turn, etc.

    To summarize:
    * If the key is already in the cache, the value is not updated and the
      existing value is returned.
    * Else if the new value is too large to fit in the cache, it is not added
      but simply returned.
    * Otherwise, the new key-value pair is added to the cache and the value is
      returned.
    Callers should always use the returned value from this method, and not hold
    on to the value that was passed to this method.

    Args:
      key: The key to add to the cache.
      value: The value to add to the cache.
      metadata: The metadata associated with the entry.

    Returns:
      The value that the client should use. This is either the existing value in
      the cache for `key`, or the new value.
    """
    with self._rlock:
      existing_value = self.get(key)
      if existing_value is not None:
        return existing_value
      if (
          metadata.num_bytes_estimate
          > self._max_total_bytes_of_entries_in_cache
      ):
        return value  # The entry is too large to fit in the cache.
      self._shrink_cache_to_at_most(
          self._max_total_bytes_of_entries_in_cache
          - metadata.num_bytes_estimate
      )
      self._cache[key] = _CacheValueAndMetadata(value=value, metadata=metadata)
      self._total_bytes_of_entries_in_cache += metadata.num_bytes_estimate
      return value

  def remove(self, key: K) -> None:
    """Removes the given key from the cache if it is present."""
    with self._rlock:
      try:
        value_and_metadata = self._cache.pop(key)
      except KeyError:
        return  # The key is not in the cache. Do nothing.
      self._total_bytes_of_entries_in_cache -= (
          value_and_metadata.metadata.num_bytes_estimate
      )

  def get_total_bytes_of_entries_in_cache(self) -> int:
    """Returns an estimate of the total number of bytes occupied by the entries."""
    with self._rlock:
      return self._total_bytes_of_entries_in_cache

  def get_max_total_bytes_of_entries_in_cache(self) -> int:
    """Returns the limit on the total bytes of all the cache entries."""
    with self._rlock:
      return self._max_total_bytes_of_entries_in_cache

  def set_max_total_bytes_of_entries_in_cache(
      self, max_total_bytes_of_entries_in_cache: int
  ) -> None:
    """Sets the limit on the total size of all entries in the cache."""
    if max_total_bytes_of_entries_in_cache < 0:
      raise ValueError(
          'max_total_bytes_of_entries_in_cache must be non-negative, but is'
          f' {max_total_bytes_of_entries_in_cache}'
      )
    with self._rlock:
      self._shrink_cache_to_at_most(max_total_bytes_of_entries_in_cache)
      self._max_total_bytes_of_entries_in_cache = (
          max_total_bytes_of_entries_in_cache
      )

  def clear(self) -> None:
    """Clears the cache."""
    with self._rlock:
      self._cache = {}
      self._total_bytes_of_entries_in_cache = 0

  def _shrink_cache_to_at_most(self, max_total_bytes_of_entries: int) -> None:
    """Shrinks the cache so its entries have at most the given size.

    Args:
      max_total_bytes_of_entries: The maximum total size of the entries that the
        cache should hold after shrinking.
    """
    with self._rlock:
      if self._total_bytes_of_entries_in_cache <= max_total_bytes_of_entries:
        return
      cache_items_old_to_new = list(self._cache.items())
      for key, value_and_metadata in cache_items_old_to_new:
        if self._total_bytes_of_entries_in_cache <= max_total_bytes_of_entries:
          return
        self._cache.pop(key)
        self._total_bytes_of_entries_in_cache -= (
            value_and_metadata.metadata.num_bytes_estimate
        )
