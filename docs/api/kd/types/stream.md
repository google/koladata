<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types.Stream API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stream of values.

The stream keeps all values in memory and can be read more than once.

Note: This class supports parametrization like Stream[T]; however,
the type parameter is currently used only for documentation purposes.
This might be changed in the future.
</code></pre>





### `Stream.make_reader()` {#kd.types.Stream.make_reader}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new reader for the stream.</code></pre>

### `Stream.new(value_qtype, /)` {#kd.types.Stream.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new `tuple[Stream, StreamWriter]`.</code></pre>

### `Stream.read_all(*, timeout)` {#kd.types.Stream.read_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Waits until the stream is closed and returns all its items.

If `timeout` is not `None` and the stream is not closed within
the given time, the method raises a `TimeoutError`.

Args:
  timeout: A timeout in seconds; None means wait indefinitely.

Returns:
  A list containing the stream items.</code></pre>

### `Stream.yield_all(*, timeout)` {#kd.types.Stream.yield_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an iterator for the stream items.

If `timeout` is not `None` and the stream doesn&#39;t close within
the given time, the method raises a `TimeoutError`.

Args:
  timeout: A timeout in seconds. `None` means wait indefinitely.</code></pre>

