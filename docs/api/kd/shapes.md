<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.shapes API

Operators that work on shapes





### `kd.shapes.dim_mapping(shape, dim)` {#kd.shapes.dim_mapping}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the parent-to-child mapping of the dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [3, 2], [1, 2, 0, 2, 1])
  kd.shapes.dim_mapping(shape, 0) # -&gt; kd.slice([0, 0])
  kd.shapes.dim_mapping(shape, 1) # -&gt; kd.slice([0, 0, 0, 1, 1])
  kd.shapes.dim_mapping(shape, 2) # -&gt; kd.slice([0, 1, 1, 3, 3, 4])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the parent-to-child mapping for.</code></pre>

### `kd.shapes.dim_sizes(shape, dim)` {#kd.shapes.dim_sizes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the row sizes at the provided dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [2, 1])
  kd.shapes.dim_sizes(shape, 0)  # -&gt; kd.slice([2])
  kd.shapes.dim_sizes(shape, 1)  # -&gt; kd.slice([2, 1])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the sizes for.</code></pre>

### `kd.shapes.expand_to_shape(x, shape, ndim=unspecified)` {#kd.shapes.expand_to_shape}
Aliases:

- [kd.expand_to_shape](../kd.md#kd.expand_to_shape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands `x` based on the provided `shape`.

When `ndim` is not set, expands `x` to `shape`. The dimensions
of `x` must be the same as the first N dimensions of `shape` where N is the
number of dimensions of `x`. For example,

Example 1:
  x: [[1, 2], [3]]
  shape: JaggedShape(3, [2, 1], [1, 2, 3])
  result: [[[1], [2, 2]], [[3, 3, 3]]]

Example 2:
  x: [[1, 2], [3]]
  shape: JaggedShape(3, [1, 1], [1, 3])
  result: incompatible shapes

Example 3:
  x: [[1, 2], [3]]
  shape: JaggedShape(2)
  result: incompatible shapes

When `ndim` is set, the expansion is performed in 3 steps:
  1) the last N dimensions of `x` are first imploded into lists
  2) the expansion operation is performed on the DataSlice of lists
  3) the lists in the expanded DataSlice are exploded

The result will have M + ndim dimensions where M is the number
of dimensions of `shape`.

For example,

Example 4:
  x: [[1, 2], [3]]
  shape: JaggedShape(2, [1, 2])
  ndim: 1
  result: [[[1, 2]], [[3], [3]]]

Example 5:
  x: [[1, 2], [3]]
  shape: JaggedShape(2, [1, 2])
  ndim: 2
  result: [[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]]

Args:
  x: DataSlice to expand.
  shape: JaggedShape.
  ndim: the number of dimensions to implode during expansion.

Returns:
  Expanded DataSlice</code></pre>

### `kd.shapes.flatten(x, from_dim=0, to_dim=unspecified)` {#kd.shapes.flatten}
Aliases:

- [kd.flatten](../kd.md#kd.flatten)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with dimensions `[from_dim:to_dim]` flattened.

Indexing works as in python:
* If `to_dim` is unspecified, `to_dim = rank()` is used.
* If `to_dim &lt; from_dim`, `to_dim = from_dim` is used.
* If `to_dim &lt; 0`, `max(0, to_dim + rank())` is used. The same goes for
  `from_dim`.
* If `to_dim &gt; rank()`, `rank()` is used. The same goes for `from_dim`.

The above-mentioned adjustments places both `from_dim` and `to_dim` in the
range `[0, rank()]`. After adjustments, the new DataSlice has `rank() ==
old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
&#34;unit&#34; dimension is inserted at `from_dim`.

Example:
  # Flatten the last two dimensions into a single dimension, producing a
  # DataSlice with `rank = old_rank - 1`.
  kd.get_shape(x)  # -&gt; JaggedShape(..., [2, 1], [7, 5, 3])
  flat_x = kd.flatten(x, -2)
  kd.get_shape(flat_x)  # -&gt; JaggedShape(..., [12, 3])

  # Flatten all dimensions except the last, producing a DataSlice with
  # `rank = 2`.
  kd.get_shape(x)  # -&gt; jaggedShape(..., [7, 5, 3])
  flat_x = kd.flatten(x, 0, -1)
  kd.get_shape(flat_x)  # -&gt; JaggedShape([3], [7, 5, 3])

  # Flatten all dimensions.
  kd.get_shape(x)  # -&gt; JaggedShape([3], [7, 5, 3])
  flat_x = kd.flatten(x)
  kd.get_shape(flat_x)  # -&gt; JaggedShape([15])

Args:
  x: a DataSlice.
  from_dim: start of dimensions to flatten. Defaults to `0` if unspecified.
  to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.</code></pre>

### `kd.shapes.flatten_end(x, n_times=1)` {#kd.shapes.flatten_end}
Aliases:

- [kd.flatten_end](../kd.md#kd.flatten_end)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with a shape flattened `n_times` from the end.

The new shape has x.get_ndim() - n_times dimensions.

Given that flattening happens from the end, only positive integers are
allowed. For more control over flattening, please use `kd.flatten`, instead.

Args:
  x: a DataSlice.
  n_times: number of dimensions to flatten from the end
    (0 &lt;= n_times &lt;= rank).</code></pre>

### `kd.shapes.get_shape(x)` {#kd.shapes.get_shape}
Aliases:

- [kd.get_shape](../kd.md#kd.get_shape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the shape of `x`.</code></pre>

### `kd.shapes.get_sizes(x)` {#kd.shapes.get_sizes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of sizes of a given shape.

Example:
  kd.shapes.get_sizes(kd.shapes.new([2], [2, 1])) -&gt; kd.slice([[2], [2, 1]])
  kd.shapes.get_sizes(kd.slice([[&#39;a&#39;, &#39;b&#39;], [&#39;c&#39;]])) -&gt; kd.slice([[2], [2,
  1]])

Args:
  x: a shape or a DataSlice from which the shape will be taken.

Returns:
  A 2-dimensional DataSlice where the first dimension&#39;s size corresponds to
  the shape&#39;s rank and the n-th subslice corresponds to the sizes of the n-th
  dimension of the original shape.</code></pre>

### `kd.shapes.is_expandable_to_shape(x, target_shape, ndim=unspecified)` {#kd.shapes.is_expandable_to_shape}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true if `x` is expandable to `target_shape`.

See `expand_to_shape` for a detailed description of expansion.

Args:
  x: DataSlice that would be expanded.
  target_shape: JaggedShape that would be expanded to.
  ndim: The number of dimensions to implode before expansion. If unset,
    defaults to 0.</code></pre>

### `kd.shapes.ndim(shape)` {#kd.shapes.ndim}
Aliases:

- [kd.shapes.rank](#kd.shapes.rank)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the rank of the jagged shape.</code></pre>

### `kd.shapes.new(*dimensions)` {#kd.shapes.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a JaggedShape from the provided dimensions.

Example:
  # Creates a scalar shape (i.e. no dimension).
  kd.shapes.new()  # -&gt; JaggedShape()

  # Creates a 3-dimensional shape with all uniform dimensions.
  kd.shapes.new(2, 3, 1)  # -&gt; JaggedShape(2, 3, 1)

  # Creates a 3-dimensional shape with 2 sub-values in the first dimension.
  #
  # The second dimension is jagged with 2 values. The first value in the
  # second dimension has 2 sub-values, and the second value has 1 sub-value.
  #
  # The third dimension is jagged with 3 values. The first value in the third
  # dimension has 1 sub-value, the second has 2 sub-values, and the third has
  # 3 sub-values.
  kd.shapes.new(2, [2, 1], [1, 2, 3])
      # -&gt; JaggedShape(2, [2, 1], [1, 2, 3])

Args:
  *dimensions: A combination of Edges and DataSlices representing the
    dimensions of the JaggedShape. Edges are used as is, while DataSlices are
    treated as sizes. DataItems (of ints) are interpreted as uniform
    dimensions which have the same child size for all parent elements.
    DataSlices (of ints) are interpreted as a list of sizes, where `ds[i]` is
    the child size of parent `i`. Only rank-0 or rank-1 int DataSlices are
    supported.</code></pre>

### `kd.shapes.rank(shape)` {#kd.shapes.rank}

Alias for [kd.shapes.ndim](#kd.shapes.ndim)

### `kd.shapes.reshape(x, shape)` {#kd.shapes.reshape}
Aliases:

- [kd.reshape](../kd.md#kd.reshape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the provided shape.

Examples:
  x = kd.slice([1, 2, 3, 4])

  # Using a shape.
  kd.reshape(x, kd.shapes.new(2, 2))  # -&gt; kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes.
  kd.reshape(x, kd.tuple(2, 2))  # -&gt; kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.tuple(-1, 2))  # -&gt; kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.tuple(-1, 2))  # -&gt; kd.slice([[1, 2], [3, 4]])

  # Using a tuple of slices and a placeholder dimension.
  kd.reshape(x, kd.tuple(-1, kd.slice([3, 1])))
      # -&gt; kd.slice([[1, 2, 3], [4]])

  # Reshaping a scalar.
  kd.reshape(1, kd.tuple(1, 1))  # -&gt; kd.slice([[1]])

  # Reshaping an empty slice.
  kd.reshape(kd.slice([]), kd.tuple(2, 0))  # -&gt; kd.slice([[], []])

Args:
  x: a DataSlice.
  shape: a JaggedShape or a tuple of dimensions that forms a shape through
    `kd.shapes.new`, with additional support for a `-1` placeholder dimension.</code></pre>

### `kd.shapes.reshape_as(x, shape_from)` {#kd.shapes.reshape_as}
Aliases:

- [kd.reshape_as](../kd.md#kd.reshape_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice x reshaped to the shape of DataSlice shape_from.</code></pre>

### `kd.shapes.size(shape)` {#kd.shapes.size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the total number of elements the jagged shape represents.</code></pre>

