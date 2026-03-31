<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.slices API

Operators that perform DataSlice transformations.





### `kd.slices.agg_count(x, ndim=unspecified)` {#kd.slices.agg_count}
Aliases:

- [kd.agg_count](../kd.md#kd.agg_count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns counts of present items over the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_count(ds)  # -&gt; kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=1)  # -&gt; kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=2)  # -&gt; kd.slice(5)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).</code></pre>

### `kd.slices.agg_size(x, ndim=unspecified)` {#kd.slices.agg_size}
Aliases:

- [kd.agg_size](../kd.md#kd.agg_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns number of items in `x` over the last ndim dimensions.

Note that it counts missing items, which is different from `kd.count`.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_size(ds)  # -&gt; kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=1)  # -&gt; kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=2)  # -&gt; kd.slice(8)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  A DataSlice of number of items in `x` over the last `ndim` dimensions.</code></pre>

### `kd.slices.align(*args)` {#kd.slices.align}
Aliases:

- [kd.align](../kd.md#kd.align)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands all of the DataSlices in `args` to the same common shape.

All DataSlices must be expandable to the shape of the DataSlice with the
largest number of dimensions.

Example:
  kd.align(kd.slice([[1, 2, 3], [4, 5]]), kd.slice(&#39;a&#39;), kd.slice([1, 2]))
  # Returns:
  # (
  #   kd.slice([[1, 2, 3], [4, 5]]),
  #   kd.slice([[&#39;a&#39;, &#39;a&#39;, &#39;a&#39;], [&#39;a&#39;, &#39;a&#39;]]),
  #   kd.slice([[1, 1, 1], [2, 2]]),
  # )

Args:
  *args: DataSlices to align.

Returns:
  A tuple of aligned DataSlices, matching `args`.</code></pre>

### `kd.slices.at(x, indices)` {#kd.slices.at}
Aliases:

- [kd.slices.take](#kd.slices.take)

- [kd.at](../kd.md#kd.at)

- [kd.take](../kd.md#kd.take)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataSlice with items at provided indices.

`indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
INT64 items.

Indices in the DataSlice `indices` are based on the last dimension of the
DataSlice `x`. Negative indices are supported and out-of-bound indices result
in missing items.

If ndim(x) - 1 &gt; ndim(indices), indices are broadcasted to shape(x)[:-1].
If ndim(x) &lt;= ndim(indices), indices are unchanged but shape(x)[:-1] must be
broadcastable to shape(indices).

Example:
  x = kd.slice([[1, None, 2], [3, 4]])
  kd.take(x, kd.item(1))  # -&gt; kd.slice([[None, 4]])
  kd.take(x, kd.slice([0, 1]))  # -&gt; kd.slice([1, 4])
  kd.take(x, kd.slice([[0, 1], [1]]))  # -&gt; kd.slice([[1, None], [4]])
  kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
    # -&gt; kd.slice([[[1, None]], []], [[4], [3]]])
  kd.take(x, kd.slice([3, -3]))  # -&gt; kd.slice([None, None])
  kd.take(x, kd.slice([-1, -2]))  # -&gt; kd.slice([2, 3])
  kd.take(x, kd.slice(&#39;1&#39;)) # -&gt; dtype mismatch error
  kd.take(x, kd.slice([1, 2, 3])) -&gt; incompatible shape

Args:
  x: DataSlice to be indexed
  indices: indices used to select items

Returns:
  A new DataSlice with items selected by indices.</code></pre>

### `kd.slices.bool(x: Any) -> DataSlice` {#kd.slices.bool}
Aliases:

- [kd.bool](../kd.md#kd.bool)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.BOOLEAN).</code></pre>

### `kd.slices.bytes(x: Any) -> DataSlice` {#kd.slices.bytes}
Aliases:

- [kd.bytes](../kd.md#kd.bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.BYTES).</code></pre>

### `kd.slices.collapse(x, ndim=unspecified)` {#kd.slices.collapse}
Aliases:

- [kd.collapse](../kd.md#kd.collapse)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Collapses the same items over the last ndim dimensions.

Missing items are ignored. For each collapse aggregation, the result is
present if and only if there is at least one present item and all present
items are the same.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.collapse(ds)  # -&gt; kd.slice([1, None, None])
  kd.collapse(ds, ndim=1)  # -&gt; kd.slice([1, None, None])
  kd.collapse(ds, ndim=2)  # -&gt; kd.slice(None)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to collapse into. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  Collapsed DataSlice.</code></pre>

### `kd.slices.concat(*args, ndim=1)` {#kd.slices.concat}
Aliases:

- [kd.concat](../kd.md#kd.concat)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the concatenation of the given DataSlices on dimension `rank-ndim`.

All given DataSlices must have the same rank, and the shapes of the first
`rank-ndim` dimensions must match. If they have incompatible shapes, consider
using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
to bring them to compatible shapes first.

The shape of the concatenated result is the following:
  1) the shape of the first `rank-ndim` dimensions remains the same
  2) the shape of the concatenation dimension is the element-wise sum of the
    shapes of the arguments&#39; concatenation dimensions
  3) the shapes of the last `ndim-1` dimensions are interleaved within the
    groups implied by the concatenation dimension

Alteratively, if we think of each input DataSlice as a nested Python list,
this operator simultaneously iterates over the inputs at depth `rank-ndim`,
concatenating the root lists of the corresponding nested sub-lists from each
input.

For example,
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[[1], [2]], [[3], [4]]])

kd.concat(a, b, ndim=1) -&gt; [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
kd.concat(a, b, ndim=2) -&gt; [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
kd.concat(a, b, ndim=3) -&gt; [[[1, 2], [3]], [[5], [7, 8]],
                            [[1], [2]], [[3], [4]]]
kd.concat(a, b, ndim=4) -&gt; raise an exception
kd.concat(a, b) -&gt; the same as kd.concat(a, b, ndim=1)

The reason auto-broadcasting is not supported is that such behavior can be
confusing and often not what users want. For example,

a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[1, 2], [3, 4]])
kd.concat(a, b) -&gt; should it be which of the following?
  [[[1, 2, 1, 2], [3, 1, 2]], [[5, 3, 4], [7, 8, 3, 4]]]
  [[[1, 2, 1, 1], [3, 2]], [[5, 3], [7, 8, 4, 4]]]
  [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]

Args:
  *args: The DataSlices to concatenate.
  ndim: The number of last dimensions to concatenate (default 1).

Returns:
  The contatenation of the input DataSlices on dimension `rank-ndim`. In case
  the input DataSlices come from different DataBags, this will refer to a
  new merged immutable DataBag.</code></pre>

### `kd.slices.count(x)` {#kd.slices.count}
Aliases:

- [kd.count](../kd.md#kd.count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.slices.cum_count(x, ndim=unspecified)` {#kd.slices.cum_count}
Aliases:

- [kd.cum_count](../kd.md#kd.cum_count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes a partial count of present items over the last `ndim` dimensions.

If `ndim` isn&#39;t specified, it defaults to 1 (count over the last dimension).

Example:
  x = kd.slice([[1, None, 1, 1], [3, 4, 5]])
  kd.cum_count(x, ndim=1)  # -&gt; kd.slice([[1, None, 2, 3], [1, 2, 3]])
  kd.cum_count(x, ndim=2)  # -&gt; kd.slice([[1, None, 2, 3], [4, 5, 6]])

Args:
  x: A DataSlice.
  ndim: The number of trailing dimensions to count within. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).

Returns:
  A DataSlice of INT64 with the same shape and sparsity as `x`.</code></pre>

### `kd.slices.dense_rank(x, descending=False, ndim=unspecified)` {#kd.slices.dense_rank}
Aliases:

- [kd.dense_rank](../kd.md#kd.dense_rank)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns dense ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By dense ranking (&#34;1 2 2 3&#34; ranking), equal items are assigned to the same
rank and the next items are assigned to that rank plus one (i.e. no gap
between the rank numbers).

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result.

Example:

  ds = kd.slice([[4, 3, None, 3], [3, None, 2, 1]])
  kd.dense_rank(x) -&gt; kd.slice([[1, 0, None, 0], [2, None, 1, 0]])

  kd.dense_rank(x, descending=True) -&gt;
      kd.slice([[0, 1, None, 1], [0, None, 1, 2]])

  kd.dense_rank(x, ndim=0) -&gt; kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.dense_rank(x, ndim=2) -&gt; kd.slice([[3, 2, None, 2], [2, None, 1, 0]])

Args:
  x: DataSlice to rank.
  descending: If true, items are compared in descending order.
  ndim: The number of dimensions to rank over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  A DataSlice of dense ranks.</code></pre>

### `kd.slices.empty_shaped(shape, schema=MASK)` {#kd.slices.empty_shaped}
Aliases:

- [kd.empty_shaped](../kd.md#kd.empty_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of missing items with the given shape.

If `schema` is a Struct schema, an empty Databag is created and attached to
the resulting DataSlice and `schema` is adopted into that DataBag.

Args:
  shape: Shape of the resulting DataSlice.
  schema: optional schema of the resulting DataSlice.</code></pre>

### `kd.slices.empty_shaped_as(shape_from, schema=MASK)` {#kd.slices.empty_shaped_as}
Aliases:

- [kd.empty_shaped_as](../kd.md#kd.empty_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of missing items with the shape of `shape_from`.

If `schema` is a Struct schema, an empty Databag is created and attached to
the resulting DataSlice and `schema` is adopted into that DataBag.

Args:
  shape_from: used for the shape of the resulting DataSlice.
  schema: optional schema of the resulting DataSlice.</code></pre>

### `kd.slices.expand_to(x, target, ndim=unspecified)` {#kd.slices.expand_to}
Aliases:

- [kd.expand_to](../kd.md#kd.expand_to)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands `x` based on the shape of `target`.

When `ndim` is not set, expands `x` to the shape of
`target`. The dimensions of `x` must be the same as the first N
dimensions of `target` where N is the number of dimensions of `x`. For
example,

Example 1:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[[0], [0, 0]], [[0, 0, 0]]])
  result: kd.slice([[[1], [2, 2]], [[3, 3, 3]]])

Example 2:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[[0]], [[0, 0, 0]]])
  result: incompatible shapes

Example 3:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([0, 0])
  result: incompatible shapes

When `ndim` is set, the expansion is performed in 3 steps:
  1) the last N dimensions of `x` are first imploded into lists
  2) the expansion operation is performed on the DataSlice of lists
  3) the lists in the expanded DataSlice are exploded

The result will have M + ndim dimensions where M is the number
of dimensions of `target`.

For example,

Example 4:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[1], [2, 3]])
  ndim: 1
  result: kd.slice([[[1, 2]], [[3], [3]]])

Example 5:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[1], [2, 3]])
  ndim: 2
  result: kd.slice([[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]])

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode during expansion.

Returns:
  Expanded DataSlice</code></pre>

### `kd.slices.expr_quote(x: Any) -> DataSlice` {#kd.slices.expr_quote}
Aliases:

- [kd.expr_quote](../kd.md#kd.expr_quote)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.EXPR).</code></pre>

### `kd.slices.float32(x: Any) -> DataSlice` {#kd.slices.float32}
Aliases:

- [kd.float32](../kd.md#kd.float32)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.FLOAT32).</code></pre>

### `kd.slices.float64(x: Any) -> DataSlice` {#kd.slices.float64}
Aliases:

- [kd.float64](../kd.md#kd.float64)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.FLOAT64).</code></pre>

### `kd.slices.get_ndim(x)` {#kd.slices.get_ndim}
Aliases:

- [kd.get_ndim](../kd.md#kd.get_ndim)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of dimensions of DataSlice `x`.</code></pre>

### `kd.slices.get_repr(x, /, *, depth=25, item_limit=200, item_limit_per_dimension=25, format_html=False, max_str_len=100, max_expr_quote_len=10000, show_attributes=True, show_databag_id=False, show_shape=False, show_schema=False, show_item_id=False, show_present_count=False)` {#kd.slices.get_repr}
Aliases:

- [kd.get_repr](../kd.md#kd.get_repr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a string representation of the DataSlice `x`.

Args:
  x: DataSlice to represent.
  depth: Maximum depth when printing nested DataSlices. -1 means no limit.
  item_limit: When it is a DataSlice, it means the maximum number of items to
    show across all dimensions. When it is a DataItem, it means the maximum
    number of entity/object attributes, list items, or dict key/value pairs to
    show. -1 means no limit.
  item_limit_per_dimension: The maximum number of items to show per dimension
    in a DataSlice. It is only enforced when the size of DataSlice is larger
    than `item_limit`. -1 means no limit.
  format_html: When true, attributes and object ids are wrapped in HTML tags
    to make it possible to style with CSS and interpret interactions with JS.
  max_str_len: Maximum length of repr string to show for text and bytes. -1
    means no limit.
  max_expr_quote_len: Maximum length of repr string to show for expr quotes.
    -1 means no limit.
  show_attributes: When true, show the attributes of the entity/object in non
    DataItem DataSlice.
  show_databag_id: When true, the repr will show the databag id.
  show_shape: When true, the repr will show the shape.
  show_schema: When true, the repr will show the schema.
  show_item_id: When true, the repr will show the itemids for objects.
  show_present_count: When true, the repr will show the size and present_count
    for DataSlices.

Returns:
  A string representation of the DataSlice `x`.</code></pre>

### `kd.slices.group_by(x, *keys, sort=False)` {#kd.slices.group_by}
Aliases:

- [kd.group_by](../kd.md#kd.group_by)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with values in last dimension grouped using a new dimension.

The resulting DataSlice has `get_ndim() + 1`. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions correspond to the groups
and the items within the groups. Elements within the same group are ordered by
the appearance order in `x`.

`keys` are used for the grouping keys. If length of `keys` is greater than 1,
the key is a tuple. If `keys` is empty, the key is `x`.

If sort=True groups are ordered by the grouping key, otherwise groups are
ordered by the appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[1, 1, 1], [3, 3, 3], [2, 2]])

Example 2:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3]), sort=True
  result: kd.slice([[1, 1, 1], [2, 2], [3, 3, 3]])

Example 3:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
  result: kd.slice([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])

Example 4:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  result: kd.slice([[1, 1, 1], [3, 3], [2]])

  Missing values are not listed in the result.

Example 5:
  x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  result: kd.slice([[1, 7], [2, 5], [3, 6, 8], [4]])

  When *keys is present, `x` is not used for the key.

Example 6:
  x: kd.slice([1, 2, 3, 4, None, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4,    0, 7, None]),
  result: kd.slice([[1, 7], [2, None], [3, 6], [4]])

  Items with missing key are not listed in the result.
  Missing `x` values are missing in the result.

Example 7:
  x: kd.slice([ 1,   2,   3,   4,   5,   6,   7,   8]),
  y: kd.slice([ 7,   4,   0,   9,   4,   0,   7,   0]),
  z: kd.slice([&#39;A&#39;, &#39;D&#39;, &#39;B&#39;, &#39;A&#39;, &#39;D&#39;, &#39;C&#39;, &#39;A&#39;, &#39;B&#39;]),
  result: kd.slice([[1, 7], [2, 5], [3, 8], [4], [6]])

  When *keys has two or more values, the  key is a tuple.
  In this example we have the following groups:
  (7, &#39;A&#39;), (4, &#39;D&#39;), (0, &#39;B&#39;), (9, &#39;A&#39;), (0, &#39;C&#39;)

Args:
  x: DataSlice to group.
  *keys: DataSlices keys to group by. All data slices must have the same shape
    as `x`. Scalar DataSlices are not supported. If not present, `x` is used
    as the key.
  sort: Whether groups in the result should be ordered by the grouping key.

Returns:
  DataSlice with the same schema as `x` with items within the last dimension
  reordered into groups and injected grouped by dimension.</code></pre>

### `kd.slices.group_by_indices(*keys, sort=False)` {#kd.slices.group_by_indices}
Aliases:

- [kd.group_by_indices](../kd.md#kd.group_by_indices)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an indices DataSlice that describes the order of `group_by` result.

The resulting DataSlice has `get_ndim() + 1`. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions correspond to the groups
and the items within the groups. Indices within the same group are in
increasing order.

Values of the DataSlice are the indices of the items within the parent
dimension. `kd.take(x, kd.group_by_indices(x))` would group the items in
`x` by their values.

If sort=True groups are ordered by key, otherwise groups are ordered by the
appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[0, 3, 6], [1, 5, 7], [2, 4]])

  We have three groups in order: 1, 3, 2. Each sublist contains the indices of
  the items in the original DataSlice.

Example 2:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3], sort=True)
  result: kd.slice([[0, 3, 6], [2, 4], [1, 5, 7]])

  Groups are now ordered by key.

Example 3:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
  result: kd.slice([[[0, 2, 4], [1], [3, 5]], [[0, 2], [1]]])

  We have three groups in the first sublist in order: 1, 2, 3 and two groups
  in the second sublist in order: 1, 3.
  Each sublist contains the indices of the items in the original sublist.

Example 4:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  result: kd.slice([[0, 3, 6], [1, 5], [2]])

  Missing values are not listed in the result.

Example 5:
  x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  result: kd.slice([[0, 6], [1, 4], [2, 5, 7], [3]])

  With several arguments keys is a tuple.
  In this example we have the following groups: (1, 7), (2, 4), (3, 0), (1, 9)

Args:
  *keys: DataSlices keys to group by. All data slices must have the same
    shape. Scalar DataSlices are not supported.
  sort: Whether groups in the result should be ordered by key.

Returns:
  INT64 DataSlice with indices and injected grouped_by dimension.</code></pre>

### `kd.slices.index(x, dim=-1)` {#kd.slices.index}
Aliases:

- [kd.index](../kd.md#kd.index)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the indices of the elements computed over dimension `dim`.

The resulting slice has the same shape as the input.

Example:
  ds = kd.slice([
      [
          [&#39;a&#39;, None, &#39;c&#39;],
          [&#39;d&#39;, &#39;e&#39;]
      ],
      [
          [None, &#39;g&#39;],
          [&#39;h&#39;, &#39;i&#39;, &#39;j&#39;]
      ]
  ])
  kd.index(ds, dim=0)
    # -&gt; kd.slice([[[0, None, 0], [0, 0]], [[None, 1], [1, 1, 1]]])
  kd.index(ds, dim=1)
    # -&gt; kd.slice([[[0, None, 0], [1, 1]], [[None, 0], [1, 1, 1]]])
  kd.index(ds, dim=2)  # (same as kd.index(ds, -1) or kd.index(ds))
    # -&gt; kd.slice([[[0, None, 2], [0, 1]], [[None, 1], [0, 1, 2]]])

  kd.index(ds) -&gt; kd.index(ds, dim=ds.get_ndim() - 1)

Args:
  x: A DataSlice.
  dim: The dimension to compute indices over.
    Requires -get_ndim(x) &lt;= dim &lt; get_ndim(x).
    If dim &lt; 0 then dim = get_ndim(x) + dim.</code></pre>

### `kd.slices.int32(x: Any) -> DataSlice` {#kd.slices.int32}
Aliases:

- [kd.int32](../kd.md#kd.int32)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.INT32).</code></pre>

### `kd.slices.int64(x: Any) -> DataSlice` {#kd.slices.int64}
Aliases:

- [kd.int64](../kd.md#kd.int64)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.INT64).</code></pre>

### `kd.slices.internal_is_compliant_attr_name(attr_name, /)` {#kd.slices.internal_is_compliant_attr_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `attr_name` can be accessed through `getattr(slice, attr_name)`.</code></pre>

### `kd.slices.internal_select_by_slice(ds, fltr, expand_filter=True)` {#kd.slices.internal_select_by_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A version of kd.select that does not support lambdas/functors.</code></pre>

### `kd.slices.inverse_mapping(x, ndim=unspecified)` {#kd.slices.inverse_mapping}
Aliases:

- [kd.inverse_mapping](../kd.md#kd.inverse_mapping)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns inverse permutations of indices over the last `ndim` dimension.

It interprets `indices` over the last `ndim` dimension as a permutation and
substitute with the corresponding inverse permutation. `ndim` is set to 1 by
default if unspecified. It fails when `indices` is not a valid permutation.

Example:
  indices = kd.slice([[1, 2, 0], [1, None]])
  kd.inverse_mapping(indices)  -&gt;  kd.slice([[2, 0, 1], [None, 0]])

  Explanation:
    indices      = [[1, 2, 0], [1, None]]
    inverse_permutation[1, 2, 0] = [2, 0, 1]
    inverse_permutation[1, None] = [None, 0]

  kd.inverse_mapping(indices, ndim=1) -&gt; raise

  indices = kd.slice([[1, 2, 0], [3, None]])
  kd.inverse_mapping(indices, ndim=2)  -&gt;  kd.slice([[2, 0, 1], [3, None]])

Args:
  x: A DataSlice of indices.
  ndim: The number of dimensions to compute inverse permutations over.
    Requires 0 &lt;= ndim &lt;= get_ndim(x).

Returns:
  An inverse permutation of indices.</code></pre>

### `kd.slices.inverse_select(ds, fltr)` {#kd.slices.inverse_select}
Aliases:

- [kd.slices.reverse_select](#kd.slices.reverse_select)

- [kd.inverse_select](../kd.md#kd.inverse_select)

- [kd.reverse_select](../kd.md#kd.reverse_select)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice by putting items in ds to present positions in fltr.

The shape of `ds` and the shape of `fltr` must have the same rank and the same
first N-1 dimensions. That is, only the last dimension can be different. The
shape of `ds` must be the same as the shape of the DataSlice after applying
`fltr` using kd.select. That is,
ds.get_shape() == kd.select(fltr, fltr).get_shape().

Example:
  ds = kd.slice([[1, None], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -&gt; [[None, 1, None], [2, None]]

  ds = kd.slice([1, None, 2])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -&gt; error due to different ranks

  ds = kd.slice([[1, None, 2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -&gt; error due to different N-1 dimensions

  ds = kd.slice([[1], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -&gt; error due to incompatible shapes

Note, in most cases, kd.inverse_select is not a strict reverse operation of
kd.select as kd.select operation is lossy and does not require `ds` and `fltr`
to have the same rank. That is,
kd.inverse_select(kd.select(ds, fltr), fltr) != ds.

The most common use case of kd.inverse_select is to restore the shape of the
original DataSlice after applying kd.select and performing some operations on
the subset of items in the original DataSlice. E.g.
  filtered_ds = kd.select(ds, fltr)
  # do something on filtered_ds
  ds = kd.inverse_select(filtered_ds, fltr) | ds

Args:
  ds: DataSlice to be reverse filtered
  fltr: filter DataSlice with dtype as kd.MASK.

Returns:
  Reverse filtered DataSlice.</code></pre>

### `kd.slices.is_empty(x)` {#kd.slices.is_empty}
Aliases:

- [kd.is_empty](../kd.md#kd.is_empty)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if all items in the DataSlice are missing.</code></pre>

### `kd.slices.is_expandable_to(x, target, ndim=unspecified)` {#kd.slices.is_expandable_to}
Aliases:

- [kd.is_expandable_to](../kd.md#kd.is_expandable_to)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true if `x` is expandable to `target`.

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode before expansion.

See `expand_to` for a detailed description of expansion.</code></pre>

### `kd.slices.is_shape_compatible(x, y)` {#kd.slices.is_shape_compatible}
Aliases:

- [kd.is_shape_compatible](../kd.md#kd.is_shape_compatible)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present if the shapes of `x` and `y` are compatible.

Two DataSlices have compatible shapes if dimensions of one DataSlice equal or
are prefix of dimensions of another DataSlice.

Args:
  x: DataSlice to check.
  y: DataSlice to check.

Returns:
  A MASK DataItem indicating whether &#39;x&#39; and &#39;y&#39; are compatible.</code></pre>

### `kd.slices.isin(x, y)` {#kd.slices.isin}
Aliases:

- [kd.isin](../kd.md#kd.isin)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataItem indicating whether DataItem x is present in y.</code></pre>

### `kd.slices.item(x, /, schema=None)` {#kd.slices.item}
Aliases:

- [kd.item](../kd.md#kd.item)

- [DataItem.from_vals](../data_item.md#DataItem.from_vals)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataItem created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`. Python value must be convertible to Koda scalar and the result cannot
be multidimensional DataSlice.

Args:
  x: a Python value or a DataItem.
  schema: schema DataItem to set. If `x` is already a DataItem, this will cast
    it to the given schema.</code></pre>

### `kd.slices.mask(x: Any) -> DataSlice` {#kd.slices.mask}
Aliases:

- [kd.mask](../kd.md#kd.mask)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.MASK).</code></pre>

### `kd.slices.ordinal_rank(x, tie_breaker=unspecified, descending=False, ndim=unspecified)` {#kd.slices.ordinal_rank}
Aliases:

- [kd.ordinal_rank](../kd.md#kd.ordinal_rank)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns ordinal ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By ordinal ranking (&#34;1 2 3 4&#34; ranking), equal items receive distinct ranks.
Items are compared by the triple (value, tie_breaker, position) to resolve
ties. When descending=True, values are ranked in descending order but
tie_breaker and position are ranked in ascending order.

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result. If `tie_breaker` is specified, it
cannot be more sparse than `x`.

Example:

  ds = kd.slice([[0, 3, None, 6], [5, None, 2, 1]])
  kd.ordinal_rank(x) -&gt; kd.slice([[0, 1, None, 2], [2, None, 1, 0]])

  kd.ordinal_rank(x, descending=True) -&gt;
      kd.slice([[2, 1, None, 0], [0, None, 1, 2]])

  kd.ordinal_rank(x, ndim=0) -&gt; kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.ordinal_rank(x, ndim=2) -&gt; kd.slice([[0, 3, None, 5], [4, None, 2, 1]])

Args:
  x: DataSlice to rank.
  tie_breaker: If specified, used to break ties. If `tie_breaker` does not
    fully resolve all ties, then the remaining ties are resolved by their
    positions in the DataSlice.
  descending: If true, items are compared in descending order. Does not affect
    the order of tie breaker and position in tie-breaking compairson.
  ndim: The number of dimensions to rank over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  A DataSlice of ordinal ranks.</code></pre>

### `kd.slices.range(start, end=unspecified)` {#kd.slices.range}
Aliases:

- [kd.range](../kd.md#kd.range)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of INT64s with range [start, end).

`start` and `end` must be broadcastable to the same shape. The resulting
DataSlice has one more dimension than the broadcasted shape.

When `end` is unspecified, `start` is used as `end` and 0 is used as `start`.
For example,

  kd.range(5) -&gt; kd.slice([0, 1, 2, 3, 4])
  kd.range(2, 5) -&gt; kd.slice([2, 3, 4])
  kd.range(5, 2) -&gt; kd.slice([])  # empty range
  kd.range(kd.slice([2, 4])) -&gt; kd.slice([[0, 1], [0, 1, 2, 3])
  kd.range(kd.slice([2, 4]), 6) -&gt; kd.slice([[2, 3, 4, 5], [4, 5])

Args:
  start: A DataSlice for start (inclusive) of intervals (unless `end` is
    unspecified, in which case this parameter is used as `end`).
  end: A DataSlice for end (exclusive) of intervals.

Returns:
  A DataSlice of INT64s with range [start, end).</code></pre>

### `kd.slices.repeat(x, sizes)` {#kd.slices.repeat}
Aliases:

- [kd.repeat](../kd.md#kd.repeat)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with values repeated according to `sizes`.

The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
broadcasted to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.repeat(ds, sizes)  # -&gt; kd.slice([[[1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.repeat(ds, sizes)  # -&gt; kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.repeat(ds, size)  # -&gt; kd.slice([[[1, 1], [None, None]], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.</code></pre>

### `kd.slices.repeat_present(x, sizes)` {#kd.slices.repeat_present}
Aliases:

- [kd.repeat_present](../kd.md#kd.repeat_present)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with present values repeated according to `sizes`.

The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
broadcasted to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.repeat_present(ds, sizes)  # -&gt; kd.slice([[[1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.repeat_present(ds, sizes)  # -&gt; kd.slice([[[1, 1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.repeat_present(ds, size)  # -&gt; kd.slice([[[1, 1], []], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.</code></pre>

### `kd.slices.reverse(ds)` {#kd.slices.reverse}
Aliases:

- [kd.reverse](../kd.md#kd.reverse)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with items reversed on the last dimension.

Example:
  ds = kd.slice([[1, None], [2, 3, 4]])
  kd.reverse(ds) -&gt; [[None, 1], [4, 3, 2]]

  ds = kd.slice([1, None, 2])
  kd.reverse(ds) -&gt; [2, None, 1]

Args:
  ds: DataSlice to be reversed.

Returns:
  Reversed on the last dimension DataSlice.</code></pre>

### `kd.slices.reverse_select(ds, fltr)` {#kd.slices.reverse_select}

Alias for [kd.slices.inverse_select](#kd.slices.inverse_select)

### `kd.slices.select(ds, fltr, expand_filter=True)` {#kd.slices.select}
Aliases:

- [kd.select](../kd.md#kd.select)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataSlice by filtering out missing items in fltr.

It is not supported for DataItems because their sizes are always 1.

The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
By default, `fltr` is expanded to &#39;ds&#39; and items in `ds` corresponding
missing items in `fltr` are removed. The last dimension of the resulting
DataSlice is changed while the first N-1 dimensions are the same as those in
`ds`.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select(val, val &gt; 3) -&gt; [[4], [], [8]]

  fltr = kd.slice(
      [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
  kd.select(val, fltr) -&gt; [[None, 4], [None], [2]]

  fltr = kd.slice([kd.present, kd.present, None])
  kd.select(val, fltr) -&gt; [[1, None, 4], [None], []]
  kd.select(val, fltr, expand_filter=False) -&gt; [[1, None, 4], [None]]

Args:
  ds: DataSlice with ndim &gt; 0 to be filtered.
  fltr: filter DataSlice with dtype as kd.MASK. It can also be a Koda Functor
    or a Python function which can be evalauted to such DataSlice. A Python
    function will be traced for evaluation, so it cannot have Python control
    flow operations such as `if` or `while`.
  expand_filter: flag indicating if the &#39;filter&#39; should be expanded to &#39;ds&#39;

Returns:
  Filtered DataSlice.</code></pre>

### `kd.slices.select_present(ds)` {#kd.slices.select_present}
Aliases:

- [kd.select_present](../kd.md#kd.select_present)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataSlice by removing missing items.

It is not supported for DataItems because their sizes are always 1.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select_present(val) -&gt; [[1, 4], [], [2, 8]]

Args:
  ds: DataSlice with ndim &gt; 0 to be filtered.

Returns:
  Filtered DataSlice.</code></pre>

### `kd.slices.size(x)` {#kd.slices.size}
Aliases:

- [kd.size](../kd.md#kd.size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.</code></pre>

### `kd.slices.slice(x, /, schema=None)` {#kd.slices.slice}
Aliases:

- [kd.slice](../kd.md#kd.slice)

- [DataSlice.from_vals](../data_slice.md#DataSlice.from_vals)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`.

Args:
  x: a Python value or a DataSlice. If it is a (nested) Python list or tuple,
    a multidimensional DataSlice is created.
  schema: schema DataItem to set. If `x` is already a DataSlice, this will
    cast it to the given schema.</code></pre>

### `kd.slices.sort(x, sort_by=unspecified, descending=False)` {#kd.slices.sort}
Aliases:

- [kd.sort](../kd.md#kd.sort)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sorts the items in `x` over the last dimension.

When `sort_by` is specified, it is used to sort items in `x`. `sort_by` must
have the same shape as `x` and cannot be more sparse than `x`. Otherwise,
items in `x` are compared by their values. Missing items are put in the end of
the sorted list regardless of the value of `descending`.

Examples:
  ds = kd.slice([[[2, 1, None, 4], [4, 1]], [[5, 4, None]]])

  kd.sort(ds) -&gt; kd.slice([[[1, 2, 4, None], [1, 4]], [[4, 5, None]]])

  kd.sort(ds, descending=True) -&gt;
      kd.slice([[[4, 2, 1, None], [4, 1]], [[5, 4, None]]])

  sort_by = kd.slice([[[9, 2, 1, 3], [2, 3]], [[9, 7, 9]]])
  kd.sort(ds, sort_by) -&gt;
      kd.slice([[[None, 1, 4, 2], [4, 1]], [[4, 5, None]]])

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4])) -&gt;
      raise due to different shapes

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4, None])) -&gt;
      raise as `sort_by` is more sparse than `x`

Args:
  x: DataSlice to sort.
  sort_by: DataSlice used for comparisons.
  descending: whether to do descending sort.

Returns:
  DataSlice with last dimension sorted.</code></pre>

### `kd.slices.stack(*args, ndim=0)` {#kd.slices.stack}
Aliases:

- [kd.stack](../kd.md#kd.stack)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stacks the given DataSlices, creating a new dimension at index `rank-ndim`.

The given DataSlices must have the same rank, and the shapes of the first
`rank-ndim` dimensions must match. If they have incompatible shapes, consider
using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
to bring them to compatible shapes first.

The result has the following shape:
  1) the shape of the first `rank-ndim` dimensions remains the same
  2) a new dimension is inserted at `rank-ndim` with uniform shape `len(args)`
  3) the shapes of the last `ndim` dimensions are interleaved within the
    groups implied by the newly-inserted dimension

Alteratively, if we think of each input DataSlice as a nested Python list,
this operator simultaneously iterates over the inputs at depth `rank-ndim`,
wrapping the corresponding nested sub-lists from each input in new lists.

For example,
a = kd.slice([[1, None, 3], [4]])
b = kd.slice([[7, 7, 7], [7]])

kd.stack(a, b, ndim=0) -&gt; [[[1, 7], [None, 7], [3, 7]], [[4, 7]]]
kd.stack(a, b, ndim=1) -&gt; [[[1, None, 3], [7, 7, 7]], [[4], [7]]]
kd.stack(a, b, ndim=2) -&gt; [[[1, None, 3], [4]], [[7, 7, 7], [7]]]
kd.stack(a, b, ndim=4) -&gt; raise an exception
kd.stack(a, b) -&gt; the same as kd.stack(a, b, ndim=0)

Args:
  *args: The DataSlices to stack.
  ndim: The number of last dimensions to stack (default 0).

Returns:
  The stacked DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.</code></pre>

### `kd.slices.str(x: Any) -> DataSlice` {#kd.slices.str}
Aliases:

- [kd.str](../kd.md#kd.str)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.STRING).</code></pre>

### `kd.slices.subslice(x, *slices)` {#kd.slices.subslice}
Aliases:

- [kd.subslice](../kd.md#kd.subslice)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Slices `x` across all of its dimensions based on the provided `slices`.

`slices` is a variadic argument for slicing arguments where individual
slicing argument can be one of the following:

  1) INT32/INT64 DataItem or Python integer wrapped into INT32 DataItem. It is
     used to select a single item in one dimension. It reduces the number of
     dimensions in the resulting DataSlice by 1.
  2) INT32/INT64 DataSlice. It is used to select multiple items in one
  dimension.
  3) Python slice (e.g. slice(1), slice(1, 3), slice(2, -1)). It is used to
     select a slice of items in one dimension. &#39;step&#39; is not supported and it
     results in no item if &#39;start&#39; is larger than or equal to &#39;stop&#39;. &#39;start&#39;
     and &#39;stop&#39; can be either Python integers, DataItems or DataSlices, in
     the latter case we can select a different range for different items,
     or even select multiple ranges for the same item if the &#39;start&#39;
     or &#39;stop&#39; have more dimensions. If an item is missing either in &#39;start&#39;
     or in &#39;stop&#39;, the corresponding slice is considered empty.
  4) .../Ellipsis. It can appear at most once in `slices` and used to fill
     corresponding dimensions in `x` but missing in `slices`. It means
     selecting all items in these dimensions.

If the Ellipsis is not provided, it is added to the **beginning** of `slices`
by default, which is different from Numpy. Individual slicing argument is used
to slice corresponding dimension in `x`.

The slicing algorithm can be thought as:
  1) implode `x` recursively to a List DataItem
  2) explode the List DataItem recursively with the slicing arguments (i.e.
     imploded_x[slice])

Example 1:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 0)
    =&gt; kd.slice([[1, 3], [4], [7, 8]])

Example 2:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 0, 1, kd.item(0))
    =&gt; kd.item(3)

Example 3:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1))
    =&gt; kd.slice([[[1], []], [[4, 5]], [[], [8]]])

Example 4:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
     =&gt; kd.slice([[[2], []], [[5, 6]]])

Example 5 (also see Example 6/7 for using DataSlices for subslicing):
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), kd.slice(0))
    =&gt; kd.slice([[4, 4], [8, 7]])

Example 6:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), ...)
    =&gt; kd.slice([[[4, 5, 6]], [[7], [8, 9]]])

Example 7:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), ...)
    =&gt; kd.slice([[[4, 5, 6], [4, 5, 6]], [[8, 9], [7]]])

Example 8:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., slice(1, None))
    =&gt; kd.slice([[[2], []], [[5, 6]], [[], [9]]])

Example 9:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 2, ..., slice(1, None))
    =&gt; kd.slice([[], [9]])

Example 10:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., 2, ...)
    =&gt; error as ellipsis can only appear once

Example 11:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 1, 2, 3, 4)
    =&gt; error as at most 3 slicing arguments can be provided

Example 12:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(kd.slice([0, 1, 2]), None))
    =&gt; kd.slice([[[1, 2], [3]], [[5, 6]], [[], []]])

Example 13:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(kd.slice([0, 1, 2]), kd.slice([2, 3, None])), ...)
    =&gt; kd.slice([[[[1, 2], [3]], [[4, 5, 6]]], [[[4, 5, 6]], [[7], [8, 9]]],
    []])

Note that there is a shortcut `ds.S[*slices] for this operator which is more
commonly used and the Python slice can be written as [start:end] format. For
example:
  kd.subslice(x, 0) == x.S[0]
  kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
  kd.subslice(x, slice(0, -1)) == x.S[0:-1]
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
    == x.S[0:-1, 0:1, 1:]
  kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
  kd.subslice(x, slice(1, None)) == x.S[1:]

Args:
  x: DataSlice to slice.
  *slices: variadic slicing argument.

Returns:
  A DataSlice with selected items</code></pre>

### `kd.slices.take(x, indices)` {#kd.slices.take}

Alias for [kd.slices.at](#kd.slices.at)

### `kd.slices.tile(x, shape)` {#kd.slices.tile}
Aliases:

- [kd.tile](../kd.md#kd.tile)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Nests the whole `x` under `shape`.

Example 1:
  x: [1, 2]
  shape: JaggedShape([3])
  result: [[1, 2], [1, 2], [1, 2]]

Example 2:
  x: [1, 2]
  shape: JaggedShape([2], [2, 1])
  result: [[[1, 2], [1, 2]], [[1, 2]]]

Args:
  x: DataSlice to expand.
  shape: JaggedShape.

Returns:
  Expanded DataSlice.</code></pre>

### `kd.slices.translate(keys_to, keys_from, values_from)` {#kd.slices.translate}
Aliases:

- [kd.translate](../kd.md#kd.translate)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Translates `keys_to` based on `keys_from`-&gt;`values_from` mapping.

The translation is done by matching keys from `keys_from` to `keys_to` over
the last dimension of `keys_to`. `keys_from` cannot have duplicate keys within
each group of the last dimension. Also see kd.translate_group.

`values_from` is first broadcasted to `keys_from`. The first N-1 dimensions of
`keys_from` must be broadcastable to `keys_to`. The resulting DataSlice has
the same shape as `keys_to` and the same DataBag as `values_from`.

Missing items or items with no matching keys in `keys_from` result in missing
items in the resulting DataSlice.

For example:

keys_to = kd.slice([[&#39;a&#39;, &#39;d&#39;], [&#39;c&#39;, None]])
keys_from = kd.slice([[&#39;a&#39;, &#39;b&#39;], [&#39;c&#39;, None]])
values_from = kd.slice([[1, 2], [3, 4]])
kd.translate(keys_to, keys_from, values_from) -&gt;
    kd.slice([[1, None], [3, None]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.</code></pre>

### `kd.slices.translate_group(keys_to, keys_from, values_from)` {#kd.slices.translate_group}
Aliases:

- [kd.translate_group](../kd.md#kd.translate_group)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Translates `keys_to` based on `keys_from`-&gt;`values_from` mapping.

The translation is done by creating an additional dimension under `keys_to`
and putting items from `values_from` in this dimension by matching keys from
`keys_from` to `keys_to` over the last dimension of `keys_to`.
`keys_to` can have duplicate keys within each group of the last
dimension.

`values_from` and `keys_from` must have the same shape. The first N-1
dimensions of `keys_from` must be broadcastable to `keys_to`. The shape of the
resulting DataSlice is the combination of the shape of `keys_to` and an
injected group_by dimension.

Missing items or items with no matching keys in `keys_from` result in empty
groups in the resulting DataSlice.

For example:

keys_to = kd.slice([&#39;a&#39;, &#39;c&#39;, None, &#39;d&#39;, &#39;e&#39;])
keys_from = kd.slice([&#39;a&#39;, &#39;c&#39;, &#39;b&#39;, &#39;c&#39;, &#39;a&#39;, &#39;e&#39;])
values_from = kd.slice([1, 2, 3, 4, 5, 6])
kd.translate_group(keys_to, keys_from, values_from) -&gt;
  kd.slice([[1, 5], [2, 4], [], [], [6]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.</code></pre>

### `kd.slices.unique(x, sort=False)` {#kd.slices.unique}
Aliases:

- [kd.unique](../kd.md#kd.unique)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with unique values within each dimension.

The resulting DataSlice has the same rank as `x`, but a different shape.
The first `get_ndim(x) - 1` dimensions are unchanged. The last dimension
contains the unique values.

If `sort` is False elements are ordered by the appearance of the first item.

If `sort` is True:
1. Elements are ordered by the value.
2. Mixed types are not supported.
3. ExprQuote and DType are not supported.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  sort: False
  result: kd.unique([1, 3, 2])

Example 2:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [3, 1, 1]])
  sort: False
  result: kd.slice([[1, 2, 3], [3, 1]])

Example 3:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  sort: False
  result: kd.slice([1, 3, 2])

  Missing values are ignored.

Example 4:
  x: kd.slice([[1, 3, 2, 1, 3, 1, 3], [3, 1, 1]])
  sort: True
  result: kd.slice([[1, 2, 3], [1, 3]])

Args:
  x: DataSlice to find unique values in.
  sort: whether elements must be ordered by the value.

Returns:
  DataSlice with the same rank and schema as `x` with unique values in the
  last dimension.</code></pre>

### `kd.slices.val_like(x, val)` {#kd.slices.val_like}
Aliases:

- [kd.val_like](../kd.md#kd.val_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with `val` masked and expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.slices.val_like(x, 1) -&gt; kd.slice([[1], [1, None]])
  kd.slices.val_like(x, kd.slice([1, 2])) -&gt; kd.slice([[1], [2, None]])
  kd.slices.val_like(x, kd.slice([None, 2])) -&gt; kd.slice([[None], [2, None]])

Args:
  x: DataSlice to match the shape and sparsity of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x` and masked by `x`.</code></pre>

### `kd.slices.val_shaped(shape, val)` {#kd.slices.val_shaped}
Aliases:

- [kd.val_shaped](../kd.md#kd.val_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with `val` expanded to the given shape.

Example:
  shape = kd.shapes.new([2], [1, 2])
  kd.slices.val_shaped(shape, 1) -&gt; kd.slice([[1], [1, 1]])
  kd.slices.val_shaped(shape, kd.slice([None, 2])) -&gt; kd.slice([[None], [2,
  2]])

Args:
  shape: shape to expand to.
  val: value to expand.

Returns:
  A DataSlice with the same shape as `shape`.</code></pre>

### `kd.slices.val_shaped_as(x, val)` {#kd.slices.val_shaped_as}
Aliases:

- [kd.val_shaped_as](../kd.md#kd.val_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with `val` expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.slices.val_shaped_as(x, 1) -&gt; kd.slice([[1], [1, 1]])
  kd.slices.val_shaped_as(x, kd.slice([None, 2])) -&gt; kd.slice([[None], [2,
  2]])

Args:
  x: DataSlice to match the shape of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x`.</code></pre>

### `kd.slices.zip(*args)` {#kd.slices.zip}
Aliases:

- [kd.zip](../kd.md#kd.zip)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Zips the given DataSlices into a new DataSlice with a new last dimension.

Input DataSlices are automatically aligned. The result has the shape of the
aligned inputs, plus a new last dimension with uniform shape `len(args)`
containing the values from each input.

For example,
a = kd.slice([1, 2, 3, 4])
b = kd.slice([5, 6, 7, 8])
c = kd.slice([&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;])
kd.zip(a, b, c) -&gt; [[1, 5, &#39;a&#39;], [2, 6, &#39;b&#39;], [3, 7, &#39;c&#39;], [4, 8, &#39;d&#39;]]

a = kd.slice([[1, None, 3], [4]])
b = kd.slice([7, None])
kd.zip(a, b) -&gt;  [[[1, 7], [None, 7], [3, 7]], [[4, None]]]

Args:
  *args: The DataSlices to zip.

Returns:
  The zipped DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.</code></pre>

