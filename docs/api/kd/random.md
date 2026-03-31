<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.random API

Random and sampling operators.





### `kd.random.cityhash(x, seed)` {#kd.random.cityhash}
Aliases:

- [kd.cityhash](../kd.md#kd.cityhash)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a hash value of &#39;x&#39; for given seed.

The hash value is generated using CityHash library. The result will have the
same shape and sparsity as `x`. The output values are INT64.

Args:
  x: DataSlice for hash.
  seed: seed for hash, must be a scalar.

Returns:
  The hash values as INT64 DataSlice.</code></pre>

### `kd.random.mask(x, ratio, seed, key=unspecified)` {#kd.random.mask}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a mask with near size(x) * ratio present values at random indices.

The sampling of indices is performed on flatten `x` rather than on the last
dimension.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) &lt; ratio * 2^63
Therefore, exact sampled count is not guaranteed. E.g. result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.random.mask(ds, 0.5, 123)
    -&gt; kd.slice([
           [None, None, kd.present, None],
           [kd.present, None, None, kd.present]
       ])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.random.mask(ds_1, 0.5, 123, key_1)
    -&gt; kd.slice([
           [None, None, None, kd.present],
           [None, None, None, kd.present],
       ])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;d&#39;, &#39;b&#39;, &#39;a&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.random.mask(ds_2, 0.5, 123, key_2)
    -&gt; kd.slice([
           [None, kd.present, None, None],
           [None, None, None, kd.present],
       ])

Args:
  x: DataSlice whose shape is used for sampling.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.</code></pre>

### `kd.random.randint_like(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_like}
Aliases:

- [kd.randint_like](../kd.md#kd.randint_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the same sparsity as `x`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  x: used to determine the shape and sparsity of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.</code></pre>

### `kd.random.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped}
Aliases:

- [kd.randint_shaped](../kd.md#kd.randint_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the given shape.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  shape: used for the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.</code></pre>

### `kd.random.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped_as}
Aliases:

- [kd.randint_shaped_as](../kd.md#kd.randint_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the same shape as `x`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  x: used to determine the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.</code></pre>

### `kd.random.sample(x, ratio, seed, key=unspecified)` {#kd.random.sample}
Aliases:

- [kd.sample](../kd.md#kd.sample)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly sample items in `x` based on ratio.

The sampling is performed on flatten `x` rather than on the last dimension.

All items including missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) &lt; ratio * 2^63
Therefore, exact sampled count is not guaranteed. E.g. result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample(ds, 0.5, 123) -&gt; kd.slice([[None, 4], [None, 8]])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample(ds_1, 0.5, 123, key_1) -&gt; kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;a&#39;, &#39;b&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample(ds_2, 0.5, 123, key_2) -&gt; kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.</code></pre>

### `kd.random.sample_n(x, n, seed, key=unspecified)` {#kd.random.sample_n}
Aliases:

- [kd.sample_n](../kd.md#kd.sample_n)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly sample n items in `x` from the last dimension.

The sampling is performed over the last dimension rather than on flatten `x`.

`n` can either can be a scalar integer or DataSlice. If it is a DataSlice, it
must have compatible shape with `x.get_shape()[:-1]`. All items including
missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set.
Otherwise, the indices of `x` are used.

Examples:
  # Select 2 items from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, 2, 123) -&gt; kd.slice([[2, 4], [None, 8]])

  # Select 1 item from the first and 2 items from the second.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, [1, 2], 123) -&gt; kd.slice([[4], [None, 5]])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample_n(ds_1, 2, 123, key_1) -&gt; kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;a&#39;, &#39;b&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample_n(ds_2, 2, 123, key_2) -&gt; kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  n: number of items to sample. Either an integer or a DataSlice.
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.</code></pre>

### `kd.random.shuffle(x, /, ndim=unspecified, seed=unspecified)` {#kd.random.shuffle}
Aliases:

- [kd.shuffle](../kd.md#kd.shuffle)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly shuffles a DataSlice along a single dimension (last by default).

If `ndim` is not specified, items are shuffled in the last dimension.
If `ndim` is specified, then the dimension `ndim` from the last is shuffled,
equivalent to `kd.explode(kd.shuffle(kd.implode(x, ndim)), ndim)`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

For example:

  kd.shuffle(kd.slice([[1, 2, 3], [4, 5], [6]]))
  -&gt; kd.slice([[3, 1, 2], [5, 4], [6]]) (possible output)

  kd.shuffle(kd.slice([[1, 2, 3], [4, 5]]), ndim=1)
  -&gt; kd.slice([[4, 5], [6], [1, 2, 3]]) (possible output)

Args:
  x: DataSlice to shuffle.
  ndim: The index of the dimension to shuffle, from the end (0 = last dim).
    The last dimension is shuffled if this is unspecified.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  Shuffled DataSlice.</code></pre>

