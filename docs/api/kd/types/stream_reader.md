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

### `StreamReader.subscribe_once(executor, callback)` {#kd.types.StreamReader.subscribe_once}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Subscribes for a notification when new items are available or when the stream is closed.

Note: The `callback` will be invoked on the executor and is called
without any arguments.

When the `callback` is invoked, the subsequent `read_available()`
call is guaranteed to return a non-trivial result:
 * a non-empty list if there are more items available,
 * `None` if the stream was closed without an error, or
 * raise an error if the stream was closed with an error.</code></pre>

