<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.streams API

Operators that work with streams of items. These APIs are in active development and might change often (b/424742492).





### `kd.streams.await_(arg)` {#kd.streams.await_}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Indicates to kd.streams.call that the argument should be awaited.

This operator acts as a marker. When the returned value is passed to
`kd.streams.call`, it signals that `kd.streams.call` should await
the underlying stream to yield a single item. This single item is then
passed to the functor.

Importantly, `stream_await` itself does not perform any awaiting or blocking.

If the input `arg` is not a stream, this operators returns `arg` unchanged.

Note: `kd.streams.call` expects an awaited stream to yield exactly one item.
Producing zero or more than one item from an awaited stream will result in
an error during the `kd.streams.call` evaluation.

Args:
  arg: The input argument (the operator has effect only if `arg` is a stream).

Returns:
  If `arg` was a stream, it gets labeled with &#39;AWAIT&#39;. If `arg` was not
  a stream, `arg` is returned without modification.</code></pre>

### `kd.streams.call(fn, *args, executor=unspecified, return_type_as=None, **kwargs)` {#kd.streams.call}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor on the given executor and yields the result(s) as a stream.

For stream arguments tagged with `kd.streams.await_`, `kd.streams.call` first
awaits the corresponding input streams. Each of these streams is expected to
yield exactly one item, which is then passed as the argument to the functor
`fn`. If a labeled stream is empty or yields more than one item, it is
considered an error.

The `return_type_as` parameter specifies the return type of the functor `fn`.
Unless the return type is already a stream, the result of `kd.streams.call` is
a `STREAM[return_type]` storing a single value returned by the functor.
However, if `return_type_as` is a stream, the result of `kd.streams.call` is
of the same stream type, holding the same items as the stream returned by
the functor.

It&#39;s recommended to specify the same `return_type_as` for `kd.streams.call`
calls as it would be for regular `kd.call`.

Importantly, `kd.streams.call` supports the case when `return_type_as` is
non-stream while the functor actually returns `STREAM[return_type]`. This
enables nested `kd.streams.call` calls.

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. The stream arguments
    tagged with `kd.streams.await_` will be awaited before the call, and
    expected to yield exactly one item.
  executor: The executor to use for computations.
  return_type_as: The return type of the functor `fn` call.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  If the return type of the functor (as specified by `return_type_as`) is
  a non-stream type, the result of `kd.streams.call` is a single-item stream
  with the functor&#39;s return value. Otherwise, the result is a stream of
  the same type as `return_type_as`, containing the same items as the stream
  returned by the functor.</code></pre>

### `kd.streams.chain(*streams, value_type_as=unspecified)` {#kd.streams.chain}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that chains the given streams, in the given order.

The streams must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the streams, if any.

Args:
  *streams: A list of streams to be chained (concatenated).
  value_type_as: A value that has the same type as the items in the streams.
    It is useful to specify this explicitly if the list of streams may be
    empty. If this is not specified and the list of streams is empty, the
    stream will have DATA_SLICE as the value type.

Returns:
  A stream that chains the given streams in the given order.</code></pre>

### `kd.streams.chain_from_stream(stream_of_streams)` {#kd.streams.chain_from_stream}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that chains the given streams.

The resulting stream has all items from the first sub-stream, then all items
from the second sub-stream, and so on.

Example:
    ```
    kd.streams.chain_from_stream(
        kd.streams.make(
            kd.streams.make(1, 2, 3),
            kd.streams.make(4),
            kd.streams.make(5, 6),
        )
    )
    ```
    result: A stream with items [1, 2, 3, 4, 5, 6].

Args:
  stream_of_streams: A stream of input streams.

Returns:
  A stream that chains the input streams.</code></pre>

### `kd.streams.current_executor()` {#kd.streams.current_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the current executor.

If the current computation is running on an executor, this operator
returns it. If no executor is set for the current context, this operator
returns an error.

Note: For the convenience, in Python environments, the default executor
(see `get_default_executor`) is implicitly set as the current executor.
However, this might not be not the case for other environments.</code></pre>

### `kd.streams.flat_map_chained(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.flat_map_chained}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given stream.

`fn` is called for each item in the input stream, and it must return a new
stream. The streams returned by `fn` are then chained to produce the final
result.

Example:
    ```
    kd.streams.flat_map_interleaved(
        kd.streams.make(1, 10),
        lambda x: kd.streams.make(x, x * 2, x * 3),
    )
    ```
    result: A stream with items [1, 2, 3, 10, 20, 30].

Args:
  stream: The stream to iterate over.
  fn: The function to be executed for each item in the stream. It will receive
    the stream item as the positional argument and must return a stream of
    values compatible with value_type_as.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as element type of the resulting stream.

Returns:
  The resulting interleaved results of `fn` calls.</code></pre>

### `kd.streams.flat_map_interleaved(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.flat_map_interleaved}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given stream.

`fn` is called for each item in the input stream, and it must return a new
stream. The streams returned by `fn` are then interleaved to produce the final
result. Note that while the internal order of items within each stream
returned by `fn` is preserved, the overall order of items from different
streams is not guaranteed.

Example:
    ```
    kd.streams.flat_map_interleaved(
        kd.streams.make(1, 10),
        lambda x: kd.streams.make(x, x * 2, x * 3),
    )
    ```
    result: A stream with items {1, 2, 3, 10, 20, 30}. While the relative
      order within {1, 2, 3} and {10, 20, 30} is guaranteed, the overall order
      of items is unspecified. For instance, the following orderings are both
      possible:
       * [1, 10, 2, 20, 3, 30]
       * [10, 20, 30, 1, 2, 3]

Args:
  stream: The stream to iterate over.
  fn: The function to be executed for each item in the stream. It will receive
    the stream item as the positional argument and must return a stream of
    values compatible with value_type_as.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as element type of the resulting stream.

Returns:
  The resulting interleaved results of `fn` calls.</code></pre>

### `kd.streams.foreach(stream, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, executor=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.streams.foreach}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes a loop over the given stream.

Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
and that dictates what this operator returns.

When `returns` is specified, it is one more variable added to `initial_state`,
and the value of that variable at the end of the loop is returned in a single-
item stream.

When `yields` is specified, it must be an stream, and the value
passed there, as well as the values set to this variable in each
stream of the loop, are chained to get the resulting stream.

When `yields_interleaved` is specified, the behavior is the same as `yields`,
but the values are interleaved instead of chained.

The behavior of the loop is equivalent to the following pseudocode (with
a simplification that `stream` is an `iterable`):

  state = initial_state  # Also add `returns` to it if specified.
  while condition_fn(state):
    item = next(iterable)
    if item == &lt;end-of-iterable&gt;:
      upd = finalize_fn(**state)
    else:
      upd = body_fn(item, **state)
    if yields/yields_interleaved is specified:
      yield the corresponding data from upd, and remove it from upd.
    state.update(upd)
    if item == &lt;end-of-iterable&gt;:
      break
  if returns is specified:
    yield state[&#39;returns&#39;]

Args:
  stream: The stream to iterate over.
  body_fn: The function to be executed for each item in the stream. It will
    receive the stream item as the positional argument, and the loop variables
    as keyword arguments (excluding `yields`/`yields_interleaved` if those are
    specified), and must return a namedtuple with the new values for some or
    all loop variables (including `yields`/`yields_interleaved` if those are
    specified).
  finalize_fn: The function to be executed when the stream is exhausted. It
    will receive the same arguments as `body_fn` except the positional
    argument, and must return the same namedtuple. If not specified, the state
    at the end will be the same as the state after processing the last item.
    Note that finalize_fn is not called if condition_fn ever returns false.
  condition_fn: The function to be executed to determine whether to continue
    the loop. It will receive the loop variables as keyword arguments, and
    must return a MASK scalar. Can be used to terminate the loop early without
    processing all items in the stream. If not specified, the loop will
    continue until the stream is exhausted.
  executor: The executor to use for computations.
  returns: The loop variable that holds the return value of the loop.
  yields: The loop variables that holds the values to yield at each iteration,
    to be chained together.
  yields_interleaved: The loop variables that holds the values to yield at
    each iteration, to be interleaved.
  **initial_state: The initial state of the loop variables.

Returns:
  Either a stream with a single returns value or a stream of yielded values.</code></pre>

### `kd.streams.from_1d_slice(slice_)` {#kd.streams.from_1d_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a 1D DataSlice to a stream of DataItems.

Args:
  slice_: A 1D DataSlice to be converted to a stream.

Returns:
  A stream of DataItems, in the order of the slice. All returned
  DataItems point to the same DataBag as the input DataSlice.</code></pre>

### `kd.streams.get_default_executor()` {#kd.streams.get_default_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the default executor.</code></pre>

### `kd.streams.get_eager_executor()` {#kd.streams.get_eager_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an executor that runs tasks right away on the same thread.</code></pre>

### `kd.streams.get_stream_qtype(value_qtype)` {#kd.streams.get_stream_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the stream qtype for the given value qtype.</code></pre>

### `kd.streams.interleave(*streams, value_type_as=unspecified)` {#kd.streams.interleave}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that interleaves the given streams.

The resulting stream has all items from all input streams, and the order of
items from each stream is preserved. But the order of interleaving of
different streams can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

The input streams must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the streams, if any.

Args:
  *streams: Input streams.
  value_type_as: A value that has the same type as the items in the streams.
    It is useful to specify this explicitly if the list of streams may be
    empty. If this is not specified and the list of streams is empty, the
    resulting stream will have DATA_SLICE as the value type.

Returns:
  A stream that interleaves the input streams in an unspecified order.</code></pre>

### `kd.streams.interleave_from_stream(stream_of_streams)` {#kd.streams.interleave_from_stream}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that interleaves the given streams.

The resulting stream has all items from all input streams, and the order of
items from each stream is preserved. But the order of interleaving of
different streams can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

Args:
  stream_of_streams: A stream of input streams.

Returns:
  A stream that interleaves the input streams in an unspecified order.</code></pre>

### `kd.streams.make(*items, value_type_as=unspecified)` {#kd.streams.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream from the given items, in the given order.

The items must all have the same type (for example data slice, or data bag).
However, in case of data slices, the items can have different shapes or
schemas.

Args:
  *items: Items to be put into the stream.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  A stream with the given items.</code></pre>

### `kd.streams.make_executor(*, thread_limit=0)` {#kd.streams.make_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new executor.

Note: The `thread_limit` limits the concurrency; however, the executor may
have no dedicated threads, and the actual concurrency limit might be lower.

Args:
  thread_limit: The number of threads to use. Must be non-negative; 0 means
    that the number of threads is selected automatically.</code></pre>

### `kd.streams.map(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new stream by applying `fn` to each item in the input stream.

For each item of the input `stream`, the `fn` is called. The single
resulting item from each call is then written into the new output stream.

Args:
  stream: The input stream.
  fn: The function to be executed for each item of the input stream. It will
    receive an item as the positional argument and its result must be of the
    same type as `value_type_as`.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as value type of the resulting stream.

Returns:
  The resulting stream.</code></pre>

### `kd.streams.map_unordered(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.map_unordered}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new stream by applying `fn` to each item in the input `stream`.

For each item of the input `stream`, the `fn` is called. The single
resulting item from each call is then written into the new output stream.

IMPORTANT: The order of the items in the resulting stream is not guaranteed.

Args:
  stream: The input stream.
  fn: The function to be executed for each item of the input stream. It will
    receive an item as the positional argument and its result must be of the
    same type as `value_type_as`.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as value type of the resulting stream.

Returns:
  The resulting stream.</code></pre>

### `kd.streams.reduce(fn, stream, initial_value, *, executor=unspecified)` {#kd.streams.reduce}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces a stream by iteratively applying a functor `fn`.

This operator applies `fn` sequentially to an accumulating value and each
item of the `stream`. The process begins with `initial_value`, then follows
this pattern:

         value_0 = initial_value
         value_1 = fn(value_0, stream[0])
         value_2 = fn(value_1, stream[1])
                ...

The result of the reduction is the final computed value.

Args:
  fn: A binary function that takes two positional arguments -- the current
    accumulating value and the next item from the stream -- and returns a new
    value. It&#39;s expected to return a value of the same type as
    `initial_value`.
  stream: The input stream.
  initial_value: The initial value.
  executor: The executor to use for computations.

Returns:
  A stream with a single item containing the final result of the reduction.</code></pre>

### `kd.streams.reduce_concat(stream, initial_value, *, ndim=1, executor=unspecified)` {#kd.streams.reduce_concat}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates data slices from the stream.

A specialized version of kd.streams.reduce() designed to speed up
the concatenation of data slices.

Using a standard kd.streams.reduce() with kd.concat() would result in
an O(N**2) computational complexity. This implementation, however, achieves
an O(N) complexity.

See the docstring for `kd.concat` for more details about the concatenation
semantics.

Args:
  stream: A stream of data slices to be concatenated.
  initial_value: The initial value to be concatenated before items.
  ndim: The number of last dimensions to concatenate.
  executor: The executor to use for computations.

Returns:
  A single-item stream with the concatenated data slice.</code></pre>

### `kd.streams.reduce_stack(stream, initial_value, *, ndim=0, executor=unspecified)` {#kd.streams.reduce_stack}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stacks data slices from the stream.

A specialized version of kd.streams.reduce() designed to speed up
the concatenation of data slices.

Using a standard kd.streams.reduce() with kd.stack() would result in
an O(N**2) computational complexity. This implementation, however,
achieves an O(N) complexity.

See the docstring for `kd.stack` for more details about the stacking
semantics.

Args:
  stream: A stream of data slices to be stacked.
  initial_value: The initial value to be stacked before items.
  ndim: The number of last dimensions to stack (default 0).
  executor: The executor to use for computations.

Returns:
  A single-item stream with the stacked data slice.</code></pre>

### `kd.streams.sync_wait(stream)` {#kd.streams.sync_wait}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Blocks until the given stream yields a single item.

NOTE: This operator cannot be used from an asynchronous task running on
an executor (even if it&#39;s an eager executor).

Args:
  stream: A single-item input stream.

Returns:
  The single item from the stream.</code></pre>

### `kd.streams.unsafe_blocking_wait(stream)` {#kd.streams.unsafe_blocking_wait}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Blocks until the given stream yields a single item.

Unlike `kd.streams.sync_wait`, this operator can be used from an asynchronous
task running on an executor, but that makes it inherently unsafe.

IMPORTANT: This operator is inherently unsafe and should be used with extreme
caution. It&#39;s primarily intended for transitional periods when migrating
a complex, synchronous computation to a concurrent model, enabling incremental
changes instead of a complete migration in one step.

The main danger stems from its blocking nature: it blocks the calling thread
until the stream is ready. However, if the task responsible for filling
the stream is also scheduled on the same executor, and all executor workers
become blocked, that task may never execute, leading to a deadlock.

While seemingly acceptable initially, prolonged or widespread use of this
operator will eventually cause deadlocks, requiring a non-trivial refactoring
of your computation.

Args:
  stream: A single-item input stream.

Returns:
  The single item from the stream.</code></pre>

### `kd.streams.while_(condition_fn, body_fn, *, executor=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.streams.while_}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Repeatedly applies a body functor while a condition is met.

Each iteration, the operator passes current state variables (including
`returns`, if specified) as keyword arguments to `condition_fn` and `body_fn`.
The loop continues if `condition_fn` returns `present`. State variables are
then updated from `body_fn`&#39;s namedtuple return value.

This operator always returns a stream, with the concrete behaviour
depending on whether `returns`, `yields`, or `yields_interleaved` was
specified (exactly one of them must be specified):

- `returns`: a single-item stream with the final value of the `returns` state
  variable;

- `yields`: a stream created by chaining the initial `yields` stream with any
  subsequent streams produced by the `body_fn`;

- `yields_interleaved`: the same as for `yields`, but instead of being chained
  the streams are interleaved.

Args:
  condition_fn: A functor that accepts state variables (including `returns`,
    if specified) as keyword arguments and returns a MASK data-item, either
    directly or as a single-item stream. A `present` value indicates the loop
    should continue; `missing` indicates it should stop.
  body_fn: A functor that accepts state variables *including `returns`, if
    specified) as keyword arguments and returns a namedtuple (see
    `kd.make_namedtuple`) containing updated values for a subset of the state
    variables. These updated values must retain their original types.
  executor: The executor to use for computations.
  returns: If present, the initial value of the &#39;returns&#39; state variable.
  yields: If present, the initial value of the &#39;yields&#39; state variable.
  yields_interleaved: If present, the initial value of the
    `yields_interleaved` state variable.
  **initial_state: Initial values for state variables.

Returns:
  If `returns` is a state variable, the value of `returns` when the loop
  ended. Otherwise, a stream combining the values of `yields` or
  `yields_interleaved` from each body invocation.</code></pre>

