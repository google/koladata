<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.parallel API

Operators for parallel computation.





### `kd.parallel.call_multithreaded(fn: DataItem, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, max_threads: int | None = None, timeout: float | None = None, **kwargs: Any) -> Any` {#kd.parallel.call_multithreaded}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor with the given arguments.

Variables of the functor or of its sub-functors will be computed in parallel
when they don&#39;t depend on each other. If the internal computation involves
iterables, the corresponding computations will be done in a streaming fashion.

Note that you should not use this function inside another functor (via py_fn),
as it will block the thread executing it, which can lead to deadlock when we
don&#39;t have enough threads in the thread pool. Instead, please compose all
functors first into one functor and then use one call to call_multithreaded to
execute them all in parallel.

Args:
  fn: The functor to call.
  *args: The positional arguments to pass to the functor.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  max_threads: The maximum number of threads to use. None means to use the
    default executor.
  timeout: The maximum time to wait for the call to finish. None means to wait
    indefinitely.
  **kwargs: The keyword arguments to pass to the functor.

Returns:
  The result of the call. Iterables and tuples/namedtuples of iterables are
  not yet supported for the result, since that would mean that the result
  is/has a stream, and this method needs to return multiple values at
  different times instead of one value at the end.</code></pre>

### `kd.parallel.get_default_transform_config(*, allow_runtime_transforms: bool = False)` {#kd.parallel.get_default_transform_config}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the default parallel transform config for parallel computation.</code></pre>

### `kd.parallel.transform(fn: DataItem | function | partial[Any], *, allow_runtime_transforms: bool = False) -> DataItem` {#kd.parallel.transform}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Transforms a functor to run in parallel.

The resulting functor will take and return parallel versions of the arguments
and return values of `fn`. Currently there is no public API to create a
parallel version (DataSlice -&gt; future[DataSlice]), this is work in progress.

Args:
  fn: The functor to transform.
  allow_runtime_transforms: Whether to allow sub-functors to be not fully
    defined at transform time (i.e. to depend on functor inputs), which will
    therefore have to be transformed at runtime. This can be slow.

Returns:
  The transformed functor.</code></pre>

### `kd.parallel.yield_multithreaded(fn: DataItem, /, *args: Any, value_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, max_threads: int | None = None, timeout: float | None = None, **kwargs: Any) -> Iterator[Any]` {#kd.parallel.yield_multithreaded}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor returning an iterable, and yields the results as they go.

Variables of the functor or of its sub-functors will be computed in parallel
when they don&#39;t depend on each other. If the internal computation involves
iterables, the corresponding computations will be done in a streaming fashion.
The functor must return an iterable.

Note that you should not use this function inside another functor (via py_fn),
as it will block the thread executing it, which can lead to deadlock when we
don&#39;t have enough threads in the thread pool. Instead, please compose all
functors first into one functor and then use
one call to call_multithreaded/yield_multithreaded to execute them all in
parallel.

Args:
  fn: The functor to call.
  *args: The positional arguments to pass to the functor.
  value_type_as: The return type of the call is expected to be an iterable of
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return an iterable of DataSlice. kd.types.DataSlice, kd.types.DataBag
    and kd.types.JaggedShape can also be passed here.
  max_threads: The maximum number of threads to use. None means to use the
    default executor.
  timeout: The maximum time to wait for the computation of all items of the
    output iterable to finish. None means to wait indefinitely.
  **kwargs: The keyword arguments to pass to the functor.

Returns:
  Yields the items of the output iterable as soon as they are available.</code></pre>

