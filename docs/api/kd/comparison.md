<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.comparison API

Operators that compare DataSlices.





### `kd.comparison.equal(x, y)` {#kd.comparison.equal}
Aliases:

- [kd.equal](../kd.md#kd.equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` and `y` are equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are equal. Returns `kd.present` for equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.full_equal(x, y)` {#kd.comparison.full_equal}
Aliases:

- [kd.full_equal](../kd.md#kd.full_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff all present items in `x` and `y` are equal.

The result is a zero-dimensional DataItem. Note that it is different from
`kd.all(x == y)`.

For example,
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, 3])) -&gt; kd.present
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, None])) -&gt; kd.missing
  kd.full_equal(kd.slice([1, 2, None]), kd.slice([1, 2, None])) -&gt; kd.present

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.greater(x, y)` {#kd.comparison.greater}
Aliases:

- [kd.greater](../kd.md#kd.greater)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is greater than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than `y`. Returns `kd.present` when `x` is greater and
`kd.missing` when `x` is less than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.greater_equal(x, y)` {#kd.comparison.greater_equal}
Aliases:

- [kd.greater_equal](../kd.md#kd.greater_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is greater than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than or equal to `y`. Returns `kd.present` when `x` is
greater than or equal to `y` and `kd.missing` when `x` is less than `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.less(x, y)` {#kd.comparison.less}
Aliases:

- [kd.less](../kd.md#kd.less)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is less than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than `y`. Returns `kd.present` when `x` is less and
`kd.missing` when `x` is greater than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.less_equal(x, y)` {#kd.comparison.less_equal}
Aliases:

- [kd.less_equal](../kd.md#kd.less_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is less than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than or equal to `y`. Returns `kd.present` when `x` is
less than or equal to `y` and `kd.missing` when `x` is greater than `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.not_equal(x, y)` {#kd.comparison.not_equal}
Aliases:

- [kd.not_equal](../kd.md#kd.not_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` and `y` are not equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are not equal. Returns `kd.present` for not equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

