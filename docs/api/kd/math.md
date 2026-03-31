<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.math API

Arithmetic operators.





### `kd.math.abs(x)` {#kd.math.abs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise absolute value of the input.</code></pre>

### `kd.math.add(x, y)` {#kd.math.add}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x + y.</code></pre>

### `kd.math.agg_inverse_cdf(x, cdf_arg, ndim=unspecified)` {#kd.math.agg_inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value with CDF (in [0, 1]) approximately equal to the input.

The value is computed along the last ndim dimensions.

The return value will have an offset of floor((cdf - 1e-6) * size()) in the
(ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.
  ndim: The number of dimensions to compute inverse CDF over. Requires 0 &lt;=
    ndim &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_max(x, ndim=unspecified)` {#kd.math.agg_max}
Aliases:

- [kd.agg_max](../kd.md#kd.agg_max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the maximum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_max(ds)  # -&gt; kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=1)  # -&gt; kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=2)  # -&gt; kd.slice(4)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_mean(x, ndim=unspecified)` {#kd.math.agg_mean}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the means along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, None], [3, 4], [None, None]])
  kd.agg_mean(ds)  # -&gt; kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=1)  # -&gt; kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=2)  # -&gt; kd.slice(2.6666666666666) # (1 + 3 + 4) / 3)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_median(x, ndim=unspecified)` {#kd.math.agg_median}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the medians along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_min(x, ndim=unspecified)` {#kd.math.agg_min}
Aliases:

- [kd.agg_min](../kd.md#kd.agg_min)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the minimum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_min(ds)  # -&gt; kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=1)  # -&gt; kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=2)  # -&gt; kd.slice(1)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_std(x, unbiased=True, ndim=unspecified)` {#kd.math.agg_std}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the standard deviation along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_std(ds)  # -&gt; kd.slice(1.0)
  kd.agg_std(ds, unbiased=False)  # -&gt; kd.slice(0.8164966)

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_sum(x, ndim=unspecified)` {#kd.math.agg_sum}
Aliases:

- [kd.agg_sum](../kd.md#kd.agg_sum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the sums along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4], [None, None]])
  kd.agg_sum(ds)  # -&gt; kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=1)  # -&gt; kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=2)  # -&gt; kd.slice(9)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_var(x, unbiased=True, ndim=unspecified)` {#kd.math.agg_var}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the variance along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_var(ds)  # -&gt; kd.slice(1.0)
  kd.agg_var(ds, unbiased=False)  # -&gt; kd.slice([0.6666667])

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.argmax(x, ndim=unspecified)` {#kd.math.argmax}
Aliases:

- [kd.argmax](../kd.md#kd.argmax)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns indices of the maximum of items along the last ndim dimensions.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Returns the index of NaN in case there is a NaN present.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None], [2, NaN, 1]])
  kd.argmax(ds)  # -&gt; kd.slice([0, 1, None, 1])
  kd.argmax(ds, ndim=1)  # -&gt; kd.slice([0, 1, None, 1])
  kd.argmax(ds, ndim=2)  # -&gt; kd.slice(8) # index of NaN

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.argmin(x, ndim=unspecified)` {#kd.math.argmin}
Aliases:

- [kd.argmin](../kd.md#kd.argmin)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns indices of the minimum of items along the last ndim dimensions.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Returns the index of NaN in case there is a NaN present.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None], [2, NaN, 1]])
  kd.argmin(ds)  # -&gt; kd.slice([2, 0, None, 1])
  kd.argmin(ds, ndim=1)  # -&gt; kd.slice([2, 0, None, 1])
  kd.argmin(ds, ndim=2)  # -&gt; kd.slice(8) # index of NaN

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.cdf(x, weights=unspecified, ndim=unspecified)` {#kd.math.cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the CDF of x in the last ndim dimensions of x element-wise.

The CDF is an array of floating-point values of the same shape as x and
weights, where each element represents which percentile the corresponding
element in x is situated at in its sorted group, i.e. the percentage of values
in the group that are smaller than or equal to it.

Args:
  x: a DataSlice of numbers.
  weights: if provided, will compute weighted CDF: each output value will
    correspond to the weight percentage of values smaller than or equal to x.
  ndim: The number of dimensions to compute CDF over.</code></pre>

### `kd.math.ceil(x)` {#kd.math.ceil}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise ceiling of the input, e.g.

rounding up: returns the smallest integer value that is not less than the
input.</code></pre>

### `kd.math.cum_max(x, ndim=unspecified)` {#kd.math.cum_max}
Aliases:

- [kd.cum_max](../kd.md#kd.cum_max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative max of items along the last ndim dimensions.</code></pre>

### `kd.math.cum_min(x, ndim=unspecified)` {#kd.math.cum_min}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative minimum of items along the last ndim dimensions.</code></pre>

### `kd.math.cum_sum(x, ndim=unspecified)` {#kd.math.cum_sum}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative sum of items along the last ndim dimensions.</code></pre>

### `kd.math.divide(x, y)` {#kd.math.divide}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x / y.</code></pre>

### `kd.math.exp(x)` {#kd.math.exp}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise exponential of the input.</code></pre>

### `kd.math.floor(x)` {#kd.math.floor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise floor of the input, e.g.

rounding down: returns the largest integer value that is not greater than the
input.</code></pre>

### `kd.math.floordiv(x, y)` {#kd.math.floordiv}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x // y.</code></pre>

### `kd.math.inverse_cdf(x, cdf_arg)` {#kd.math.inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value with CDF (in [0, 1]) approximately equal to the input.

The return value is computed over all dimensions. It will have an offset of
floor((cdf - 1e-6) * size()) in the (ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.</code></pre>

### `kd.math.is_nan(x)` {#kd.math.is_nan}
Aliases:

- [kd.is_nan](../kd.md#kd.is_nan)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns pointwise `kd.present|missing` if the input is NaN or not.</code></pre>

### `kd.math.log(x)` {#kd.math.log}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise natural logarithm of the input.</code></pre>

### `kd.math.log10(x)` {#kd.math.log10}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise logarithm in base 10 of the input.</code></pre>

### `kd.math.max(x)` {#kd.math.max}
Aliases:

- [kd.max](../kd.md#kd.max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the maximum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.maximum(x, y)` {#kd.math.maximum}
Aliases:

- [kd.maximum](../kd.md#kd.maximum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise max(x, y).</code></pre>

### `kd.math.mean(x)` {#kd.math.mean}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the mean of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.median(x)` {#kd.math.median}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the median of elements over all dimensions.

The result is a zero-dimensional DataItem.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.min(x)` {#kd.math.min}
Aliases:

- [kd.min](../kd.md#kd.min)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the minimum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.minimum(x, y)` {#kd.math.minimum}
Aliases:

- [kd.minimum](../kd.md#kd.minimum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise min(x, y).</code></pre>

### `kd.math.mod(x, y)` {#kd.math.mod}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x % y.</code></pre>

### `kd.math.multiply(x, y)` {#kd.math.multiply}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x * y.</code></pre>

### `kd.math.neg(x)` {#kd.math.neg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise negation of the input, i.e. -x.</code></pre>

### `kd.math.pos(x)` {#kd.math.pos}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise positive of the input, i.e. +x.</code></pre>

### `kd.math.pow(x, y)` {#kd.math.pow}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x ** y.</code></pre>

### `kd.math.round(x)` {#kd.math.round}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise rounding of the input.

Please note that this is NOT bankers rounding, unlike Python built-in or
Tensorflow round(). If the first decimal is exactly  0.5, the result is
rounded to the number with a higher absolute value:
round(1.4) == 1.0
round(1.5) == 2.0
round(1.6) == 2.0
round(2.5) == 3.0 # not 2.0
round(-1.4) == -1.0
round(-1.5) == -2.0
round(-1.6) == -2.0
round(-2.5) == -3.0 # not -2.0</code></pre>

### `kd.math.sigmoid(x, half=0.0, slope=1.0)` {#kd.math.sigmoid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes sigmoid of the input.

sigmoid(x) = 1 / (1 + exp(-slope * (x - half)))

Args:
  x: A DataSlice of numbers.
  half: A DataSlice of numbers.
  slope: A DataSlice of numbers.

Return:
  sigmoid(x) computed with the formula above.</code></pre>

### `kd.math.sign(x)` {#kd.math.sign}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes the sign of the input.

Args:
  x: A DataSlice of numbers.

Returns:
  A dataslice of with {-1, 0, 1} of the same shape and type as the input.</code></pre>

### `kd.math.softmax(x, beta=1.0, ndim=unspecified)` {#kd.math.softmax}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the softmax of x alon the last ndim dimensions.

The softmax represents Exp(x * beta) / Sum(Exp(x * beta)) over last ndim
dimensions of x.

Args:
  x: An array of numbers.
  beta: A floating point scalar number that controls the smooth of the
    softmax.
  ndim: The number of last dimensions to compute softmax over.</code></pre>

### `kd.math.sqrt(x)` {#kd.math.sqrt}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise sqrt of the input.</code></pre>

### `kd.math.subtract(x, y)` {#kd.math.subtract}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x - y.</code></pre>

### `kd.math.sum(x)` {#kd.math.sum}
Aliases:

- [kd.sum](../kd.md#kd.sum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the sum of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.t_distribution_inverse_cdf(x, degrees_of_freedom)` {#kd.math.t_distribution_inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Student&#39;s t-distribution inverse CDF.

Args:
  x: A DataSlice of numbers.
  degrees_of_freedom: A DataSlice of numbers.

Return:
  t_distribution_inverse_cdf(x).</code></pre>

