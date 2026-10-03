<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types.StreamReader API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stream reader.

Note: This class supports parametrization like StreamReader[T].
</code></pre>





### `StreamReader.read_available(limit=None)` {#kd.types.StreamReader.read_available}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the available items from the stream.

Returns an empty list if no more data is currently available and
the stream is still open.

Returns `None` if the stream has been exhausted and closed without
an error; otherwise, raises the error passed during closing.

Args:
  limit: The maximum number of items to return.</code></pre>

### `StreamReader.subscribe_once(callback, /)` {#kd.types.StreamReader.subscribe_once}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Registers a one-time callback for when items arrive or the stream closes.

The `callback` is executed without arguments. It may run synchronously
on the current thread if the stream is already ready, or asynchronously
on the C++ to Python bridge thread. To avoid blocking other notifications
on the bridge thread, the callback must execute quickly; any non-trivial
processing should be offloaded to a separate thread pool.

When the `callback` is invoked, the subsequent `read_available()`
call is guaranteed to return a non-trivial result:
 * a non-empty list if there are more items available,
 * `None` if the stream was closed without an error, or
 * raise an error if the stream was closed with an error.</code></pre>
