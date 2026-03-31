<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.masking API

Masking operators.





### `kd.masking.agg_all(x, ndim=unspecified)` {#kd.masking.agg_all}
Aliases:

- [kd.agg_all](../kd.md#kd.agg_all)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present if all elements are present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.agg_any(x, ndim=unspecified)` {#kd.masking.agg_any}
Aliases:

- [kd.agg_any](../kd.md#kd.agg_any)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present if any element is present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.agg_has(x, ndim=unspecified)` {#kd.masking.agg_has}
Aliases:

- [kd.agg_has](../kd.md#kd.agg_has)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff any element is present along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

It is equivalent to `kd.agg_any(kd.has(x))`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.all(x)` {#kd.masking.all}
Aliases:

- [kd.all](../kd.md#kd.all)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff all elements are present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.</code></pre>

### `kd.masking.any(x)` {#kd.masking.any}
Aliases:

- [kd.any](../kd.md#kd.any)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff any element is present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.</code></pre>

### `kd.masking.apply_mask(x, y)` {#kd.masking.apply_mask}
Aliases:

- [kd.apply_mask](../kd.md#kd.apply_mask)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Filters `x` to items where `y` is present.

Pointwise masking operator that replaces items in DataSlice `x` by None
if corresponding items in DataSlice `y` of MASK dtype is `kd.missing`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  Masked DataSlice.</code></pre>

### `kd.masking.coalesce(x, y)` {#kd.masking.coalesce}
Aliases:

- [kd.coalesce](../kd.md#kd.coalesce)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Fills in missing values of `x` with values of `y`.

Pointwise masking operator that replaces missing items (i.e. None) in
DataSlice `x` by corresponding items in DataSlice y`.
`x` and `y` do not need to have the same type.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.</code></pre>

### `kd.masking.cond(condition, yes, no=None)` {#kd.masking.cond}
Aliases:

- [kd.cond](../kd.md#kd.cond)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `yes` where `condition` is present, otherwise `no`.

Pointwise operator selects items in `yes` if corresponding items are
`kd.present` or items in `no` otherwise. `condition` must have MASK dtype.

If `no` is unspecified corresponding items in result are missing.

Note that there is _no_ short-circuiting based on the `condition` - both `yes`
and `no` branches will be evaluated irrespective of its value. See `kd.if_`
for a short-circuiting version of this operator.

Args:
  condition: DataSlice.
  yes: DataSlice.
  no: DataSlice or unspecified.

Returns:
  DataSlice of items from `yes` and `no` based on `condition`.</code></pre>

### `kd.masking.disjoint_coalesce(x, y)` {#kd.masking.disjoint_coalesce}
Aliases:

- [kd.disjoint_coalesce](../kd.md#kd.disjoint_coalesce)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Fills in missing values of `x` with values of `y`.

Raises if `x` and `y` intersect. It is equivalent to `x | y` with additional
assertion that `x` and `y` are disjoint.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.</code></pre>

### `kd.masking.has(x)` {#kd.masking.has}
Aliases:

- [kd.has](../kd.md#kd.has)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns presence of `x`.

Pointwise operator which take a DataSlice and return a MASK indicating the
presence of each item in `x`. Returns `kd.present` for present items and
`kd.missing` for missing items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the presence of `x`.</code></pre>

### `kd.masking.has_not(x)` {#kd.masking.has_not}
Aliases:

- [kd.has_not](../kd.md#kd.has_not)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is missing element-wise.

Pointwise operator which take a DataSlice and return a MASK indicating
iff `x` is missing element-wise. Returns `kd.present` for missing
items and `kd.missing` for present items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the non-presence of `x`.</code></pre>

### `kd.masking.mask_and(x, y)` {#kd.masking.mask_and}
Aliases:

- [kd.mask_and](../kd.md#kd.mask_and)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_AND operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_AND operation is defined as:
  kd.mask_and(kd.present, kd.present) -&gt; kd.present
  kd.mask_and(kd.present, kd.missing) -&gt; kd.missing
  kd.mask_and(kd.missing, kd.present) -&gt; kd.missing
  kd.mask_and(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x &amp; y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_equal(x, y)` {#kd.masking.mask_equal}
Aliases:

- [kd.mask_equal](../kd.md#kd.mask_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_EQUAL operation is defined as:
  kd.mask_equal(kd.present, kd.present) -&gt; kd.present
  kd.mask_equal(kd.present, kd.missing) -&gt; kd.missing
  kd.mask_equal(kd.missing, kd.present) -&gt; kd.missing
  kd.mask_equal(kd.missing, kd.missing) -&gt; kd.present

Note that this is different from `x == y`. For example,
  kd.missing == kd.missing -&gt; kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_not_equal(x, y)` {#kd.masking.mask_not_equal}
Aliases:

- [kd.mask_not_equal](../kd.md#kd.mask_not_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_NOT_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_NOT_EQUAL operation is defined as:
  kd.mask_not_equal(kd.present, kd.present) -&gt; kd.missing
  kd.mask_not_equal(kd.present, kd.missing) -&gt; kd.present
  kd.mask_not_equal(kd.missing, kd.present) -&gt; kd.present
  kd.mask_not_equal(kd.missing, kd.missing) -&gt; kd.missing

Note that this is different from `x != y`. For example,
  kd.present != kd.missing -&gt; kd.missing
  kd.missing != kd.present -&gt; kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_or(x, y)` {#kd.masking.mask_or}
Aliases:

- [kd.mask_or](../kd.md#kd.mask_or)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_OR operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_OR operation is defined as:
  kd.mask_or(kd.present, kd.present) -&gt; kd.present
  kd.mask_or(kd.present, kd.missing) -&gt; kd.present
  kd.mask_or(kd.missing, kd.present) -&gt; kd.present
  kd.mask_or(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x | y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.present_like(x)` {#kd.masking.present_like}
Aliases:

- [kd.present_like](../kd.md#kd.present_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the shape and sparsity of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.present_like(x) -&gt; kd.slice([[present], [present, None]])

Args:
  x: DataSlice to match the shape and sparsity of.

Returns:
  A DataSlice with the same shape and sparsity as `x`.</code></pre>

### `kd.masking.present_shaped(shape)` {#kd.masking.present_shaped}
Aliases:

- [kd.present_shaped](../kd.md#kd.present_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the given shape.

Example:
  shape = kd.shapes.new([2], [1, 2])
  kd.masking.present_shaped(shape) -&gt; kd.slice([[present], [present,
  present]])

Args:
  shape: shape to expand to.

Returns:
  A DataSlice with the same shape as `shape`.</code></pre>

### `kd.masking.present_shaped_as(x)` {#kd.masking.present_shaped_as}
Aliases:

- [kd.present_shaped_as](../kd.md#kd.present_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.masking.present_shaped_as(x) -&gt; kd.slice([[present], [present, present]])

Args:
  x: DataSlice to match the shape of.

Returns:
  A DataSlice with the same shape as `x`.</code></pre>

### `kd.masking.xor(x, y)` {#kd.masking.xor}
Aliases:

- [kd.xor](../kd.md#kd.xor)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise XOR operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. XOR operation is defined as:
  kd.xor(kd.present, kd.present) -&gt; kd.missing
  kd.xor(kd.present, kd.missing) -&gt; kd.present
  kd.xor(kd.missing, kd.present) -&gt; kd.present
  kd.xor(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x ^ y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

