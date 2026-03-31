<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.iterables API

Operators that work with iterables. These APIs are in active development and might change often.





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

