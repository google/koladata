<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.iterables API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Operators that work with iterables.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
</code></pre>





### `kd.iterables.chain(*iterables, value_type_as=unspecified)` {#kd.iterables.chain}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable that chains the given iterables, in the given order.

The iterables must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the iterables, if any.

Args:
  *iterables: A list of iterables to be chained (concatenated).
  value_type_as: A value that has the same type as the iterables. It is useful
    to specify this explicitly if the list of iterables may be empty. If this
    is not specified and the list of iterables is empty, the iterable will
    have DataSlice as the value type.

Returns:
  An iterable that chains the given iterables, in the given order.</code></pre>

### `kd.iterables.flat_map_chain(iterable, fn, value_type_as=None)` {#kd.iterables.flat_map_chain}
Aliases:

- [kd.functor.flat_map_chain](functor.md#kd.functor.flat_map_chain)

- [kd.flat_map_chain](../kd.md#kd.flat_map_chain)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an iterable.
The resulting iterable is then chained to get the final result.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))`.

Example:
  ```
  kd.functor.flat_map_chain(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 2, 3, 10, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as chained output of `fn`.</code></pre>

### `kd.iterables.flat_map_interleaved(iterable, fn, value_type_as=None)` {#kd.iterables.flat_map_interleaved}
Aliases:

- [kd.functor.flat_map_interleaved](functor.md#kd.functor.flat_map_interleaved)

- [kd.flat_map_interleaved](../kd.md#kd.flat_map_interleaved)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an
iterable. The resulting iterables are then interleaved to get the final
result. Please note that the order of the items in each functor output
iterable is preserved, while the order of these iterables is not preserved.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))` or `kd.iterables.make(f(x1),
f(x2), g(x1), g(x2))` or `kd.iterables.make(g(x1), f(x1), f(x2), g(x2))` or
`kd.iterables.make(g(x1), g(x2), f(x1), f(x2))`.

Example:
  ```
  kd.functor.flat_map_interleaved(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 10, 2, 3, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as interleaved output of `fn`.</code></pre>

### `kd.iterables.for_(iterable, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.iterables.for_}
Aliases:

- [kd.functor.for_](functor.md#kd.functor.for_)

- [kd.for_](../kd.md#kd.for_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes a loop over the given iterable.

Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
and that dictates what this operator returns.

When `returns` is specified, it is one more variable added to `initial_state`,
and the value of that variable at the end of the loop is returned.

When `yields` is specified, it must be an iterable, and the value
passed there, as well as the values set to this variable in each
iteration of the loop, are chained to get the resulting iterable.

When `yields_interleaved` is specified, the behavior is the same as `yields`,
but the values are interleaved instead of chained.

The behavior of the loop is equivalent to the following pseudocode:

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
    return state[&#39;returns&#39;]

Args:
  iterable: The iterable to iterate over.
  body_fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument, and the loop
    variables as keyword arguments (excluding `yields`/`yields_interleaved` if
    those are specified), and must return a namedtuple with the new values for
    some or all loop variables (including `yields`/`yields_interleaved` if
    those are specified).
  finalize_fn: The function to be executed when the iterable is exhausted. It
    will receive the same arguments as `body_fn` except the positional
    argument, and must return the same namedtuple. If not specified, the state
    at the end will be the same as the state after processing the last item.
    Note that finalize_fn is not called if condition_fn ever returns a missing
    mask.
  condition_fn: The function to be executed to determine whether to continue
    the loop. It will receive the loop variables as keyword arguments, and
    must return a MASK scalar. Can be used to terminate the loop early without
    processing all items in the iterable. If not specified, the loop will
    continue until the iterable is exhausted.
  returns: The loop variable that holds the return value of the loop.
  yields: The loop variables that holds the values to yield at each iteration,
    to be chained together.
  yields_interleaved: The loop variables that holds the values to yield at
    each iteration, to be interleaved.
  **initial_state: The initial state of the loop variables.

Returns:
  Either the return value or the iterable of yielded values.</code></pre>

### `kd.iterables.from_1d_slice(slice_)` {#kd.iterables.from_1d_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a 1D DataSlice to a Koda iterable of DataItems.

Args:
  slice_: A 1D DataSlice to be converted to an iterable.

Returns:
  A Koda iterable of DataItems, in the order of the slice. All returned
  DataItems point to the same DataBag as the input DataSlice.</code></pre>

### `kd.iterables.interleave(*iterables, value_type_as=unspecified)` {#kd.iterables.interleave}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable that interleaves the given iterables.

The resulting iterable has all items from all input iterables, and the order
within each iterable is preserved. But the order of interleaving of different
iterables can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

The iterables must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the iterables, if any.

Args:
  *iterables: A list of iterables to be interleaved.
  value_type_as: A value that has the same type as the iterables. It is useful
    to specify this explicitly if the list of iterables may be empty. If this
    is not specified and the list of iterables is empty, the iterable will
    have DataSlice as the value type.

Returns:
  An iterable that interleaves the given iterables, in arbitrary order.</code></pre>

### `kd.iterables.make(*items, value_type_as=unspecified)` {#kd.iterables.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable from the provided items, in the given order.

The items must all have the same type (for example data slice, or data bag).
However, in case of data slices, the items can have different shapes or
schemas.

Args:
  *items: Items to be put into the iterable.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  An iterable with the given items.</code></pre>

### `kd.iterables.make_unordered(*items, value_type_as=unspecified)` {#kd.iterables.make_unordered}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable from the provided items, in an arbitrary order.

Having unspecified order allows the parallel execution to put the items into
the iterable in the order they are computed, potentially increasing the amount
of parallel processing done.

When used with the non-parallel evaluation, we intentionally randomize the
order to prevent user code from depending on the order, and avoid
discrepancies when switching to parallel evaluation.

Args:
  *items: Items to be put into the iterable.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  An iterable with the given items, in an arbitrary order.</code></pre>

### `kd.iterables.reduce(fn, items, initial_value)` {#kd.iterables.reduce}
Aliases:

- [kd.functor.reduce](functor.md#kd.functor.reduce)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces an iterable using the given functor.

The result is a DataSlice that has the value: fn(fn(fn(initial_value,
items[0]), items[1]), ...), where the fn calls are done in the order of the
items in the iterable.

Args:
  fn: A binary function or functor to be applied to each item of the iterable;
    its return type must be the same as the first argument.
  items: An iterable to be reduced.
  initial_value: The initial value to be passed to the functor.

Returns:
  Result of the reduction as a single value.</code></pre>

### `kd.iterables.reduce_concat(items, initial_value, ndim=1)` {#kd.iterables.reduce_concat}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates the values of the given iterable.

This operator is a concrete case of the more general kd.functor.reduce, which
exists to speed up such concatenation from O(N^2) that the general reduce
would provide to O(N). See the docstring of kd.concat for more details about
the concatenation semantics.

Args:
  items: An iterable of data slices to be concatenated.
  initial_value: The initial value to be concatenated before items.
  ndim: The number of last dimensions to concatenate.

Returns:
  The concatenated data slice.</code></pre>

### `kd.iterables.reduce_updated_bag(items, initial_value)` {#kd.iterables.reduce_updated_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Merges the bags from the given iterable into one.

This operator is a concrete case of the more general kd.functor.reduce, which
exists to speed up such merging from O(N^2) that the general reduce
would provide to O(N). See the docstring of kd.updated_bag for more details
about the merging semantics.

Args:
  items: An iterable of data bags to be merged.
  initial_value: The data bag to be merged with the items. Note that the items
    will be merged as updates to this bag, meaning that they will take
    precedence over the initial_value on conflicts.

Returns:
  The merged data bag.</code></pre>

### `kd.iterables.while_(condition_fn, body_fn, *, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.iterables.while_}
Aliases:

- [kd.functor.while_](functor.md#kd.functor.while_)

- [kd.while_](../kd.md#kd.while_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">While a condition functor returns present, runs a body functor repeatedly.

The items in `initial_state` (and `returns`, if specified) are used to
initialize a dict of state variables, which are passed as keyword arguments
to `condition_fn` and `body_fn` on each loop iteration, and updated from the
namedtuple (see kd.namedtuple) return value of `body_fn`.

Exactly one of `returns`, `yields`, or `yields_interleaved` must be specified.
The return value of this operator depends on which one is present:
- `returns`: the value of `returns` when the loop ends. The initial value of
  `returns` must have the same qtype (e.g. DataSlice, DataBag) as the final
  return value.
- `yields`: a single iterable chained (using `kd.iterables.chain`) from the
  value of `yields` returned from each invocation of `body_fn`, The value of
  `yields` must always be an iterable, including initially.
- `yields_interleaved`: the same as for `yields`, but the iterables are
  interleaved (using `kd.iterables.iterleave`) instead of being chained.

Args:
  condition_fn: A functor with keyword argument names matching the state
    variable names and returning a MASK DataItem.
  body_fn: A functor with argument names matching the state variable names and
    returning a namedtuple (see kd.namedtuple) with a subset of the keys of
    `initial_state`.
  returns: If present, the initial value of the &#39;returns&#39; state variable.
  yields: If present, the initial value of the &#39;yields&#39; state variable.
  yields_interleaved: If present, the initial value of the
    `yields_interleaved` state variable.
  **initial_state: A dict of the initial values for state variables.

Returns:
  If `returns` is a state variable, the value of `returns` when the loop
  ended. Otherwise, an iterable combining the values of `yields` or
  `yields_interleaved` from each body invocation.</code></pre>
