<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.caching.LruCache API

<pre class="no-copy"><code class="lang-text no-auto-prettify">LRU cache Object/Entity -> DataSlice.
</code></pre>


Subcategory | Description
----------- | ------------
[Mode](lru_cache/mode.md) | Mode for LruCache.




### `LruCache.Mode(*values)` {#kd.caching.LruCache.Mode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Mode for LruCache.</code></pre>

### `LruCache.__init__(self, capacity: int, mode: Mode)` {#kd.caching.LruCache.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `LruCache.cache_fn(self, func_id: str, is_hit_fn: Callable[[DataSlice], DataSlice] | None = None) -> Callable[[Callable[[DataSlice], DataSlice]], Callable[[DataSlice], DataSlice]]` {#kd.caching.LruCache.cache_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Caching decorator for DataSlice-&gt;DataSlice functions.

In case of a partial cache hit, the newly calculated results and the cached
results will be interleaved directly using &#39;|&#39;, so the schema it required
to be stable between runs.

Example:
  cache = kd.caching.LruCache(capacity=10)

  @cache.cache_fn(&#39;my_func&#39;)
  def fn(x):
    print(f&#39;arg: {x}&#39;)
    return x * x

  print(&#39;[2, 3] -&gt;&#39;, fn(kd.slice([2, 3])))  # no cache hit
  # arg: DataSlice([2, 3])
  # [2, 3] -&gt; DataSlice([4, 9])
  print(&#39;[3, 4] -&gt;&#39;, fn(kd.slice([3, 4])))  # partial cache hit
  # arg: DataSlice([None, 4])
  # [3, 4] -&gt; DataSlice([9, 16])
  print(&#39;[3, 4] -&gt;&#39;, fn(kd.slice([3, 4])))  # full cache hit
  # [3, 4] -&gt; DataSlice([9, 16])

Args:
  func_id: Function id is added to each key. It is needed to avoid
    collisions if one LruCache is used for caching results of different
    functions. If func_id is reused by two function, then results of one
    function will override results of another function in the cache.
  is_hit_fn: Optional function returning MASK DataSlice. If specified and
    returns kd.missing for some value, then this value will not be
    considered a cache hit.

Returns:
  Decorator function.</code></pre>

### `LruCache.clear(self) -> None` {#kd.caching.LruCache.clear}
*No description*
