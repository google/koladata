<!-- Note: This file is auto-generated, do not edit manually. -->

# DataSlice API

`DataSlice` represents a jagged array of items (i.e. primitive values, ItemIds).





### `DataSlice.L` {#DataSlice.L}

<pre class="no-copy"><code class="lang-text no-auto-prettify">ListSlicing helper for DataSlice.

x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
of DataSlice x as a a list.</code></pre>

### `DataSlice.S` {#DataSlice.S}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Slicing helper for DataSlice.

It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
is equivalent to ds.S[*slices]. For example,
  kd.subslice(x, 0) == x.S[0]
  kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
  kd.subslice(x, slice(0, -1)) == x.S[0:-1]
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
    == x.S[0:-1, 0:1, 1:]
  kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
  kd.subslice(x, slice(1, None)) == x.S[1:]

Please see kd.subslice for more detailed explanations and examples.</code></pre>

### `DataSlice.append(value, /)` {#DataSlice.append}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Append a value to each list in this DataSlice</code></pre>

### `DataSlice.clear()` {#DataSlice.clear}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Clears all dicts or lists in this DataSlice</code></pre>

### `DataSlice.clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.clone}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with clones of provided entities in a new DataBag.

The entities themselves are cloned (with new ItemIds) and their attributes are
extracted (with the same ItemIds).

Also see kd.shallow_clone and kd.deep_clone.

Note that unlike kd.deep_clone, if there are multiple references to the same
entity, the returned DataSlice will have multiple clones of it rather than
references to the same clone.

Args:
  x: The DataSlice to copy.
  itemid: The ItemId to assign to cloned entities. If not specified, new
    ItemIds will be allocated.
  schema: The schema to resolve attributes, and also to assign the schema to
    the resulting DataSlice. If not specified, will use the schema of `x`.
  **overrides: attribute overrides.

Returns:
  A copy of the entities where entities themselves are cloned (new ItemIds)
  and all of the rest extracted.</code></pre>

### `DataSlice.clone_as_full(self, *, itemid: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.clone_as_full}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Clones the DataSlice, filling missing items with new empty entities.

Equivalent to:
  x = x | kd.new_shaped(x.get_shape(), schema=x.get_schema())
  return kd.clone(x, itemid=itemid, **overrides)

This operator can be used to fill missing items in a DataSlice attribute. For
example, consider the following two snippets:

x.updated(kd.attrs(x.maybe_missing_attr,
    must_be_present_attr=...
))

x.updated(kd.attrs(x,
    maybe_missing_attr=kd.clone_as_full(
        x.maybe_missing_attr,
        must_be_present_attr=...
    ),
))

In the first snippet the values of `must_be_present_attr` will be skipped when
`maybe_missing_attr` is missing. In the second snippet the whole
`maybe_missing_attr` will be overwritten as full (keeping all the pre-existing
attributes) and so all the values of `must_be_present_attr` will be preserved.

Args:
  x: The DataSlice to copy. It must have an entity schema.
  itemid: The ItemId to assign to cloned entities. If not specified, new
    ItemIds will be allocated.
  **overrides: attribute overrides.

Returns:
  A copy of the entities where entities themselves are cloned (new ItemIds)
  and all of the rest extracted. Missing items in `x` are replaced by new
  empty entities.</code></pre>

### `DataSlice.deep_clone(self, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.deep_clone}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a slice with a (deep) copy of the given slice.

The entities themselves and all their attributes including both top-level and
non-top-level attributes are cloned (with new ItemIds).

Also see kd.shallow_clone and kd.clone.

Note that unlike kd.clone, if there are multiple references to the same entity
in `x`, or multiple ways to reach one entity through attributes, there will be
exactly one clone made per entity.

Args:
  x: The slice to copy.
  schema: The schema to use to find attributes to clone, and also to assign
    the schema to the resulting DataSlice. If not specified, will use the
    schema of &#39;x&#39;.
  **overrides: attribute overrides.

Returns:
  A (deep) copy of the given DataSlice.
  All referenced entities will be copied with newly allocated ItemIds. Note
  that UUIDs will be copied as ItemIds.</code></pre>

### `DataSlice.deep_uuid(self, schema: Any = unspecified, *, seed: str | DataSlice = '') -> DataSlice` {#DataSlice.deep_uuid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve &#39;*&#39; and &#39;**&#39; tokens. If not specified,
    will use the schema of the &#39;x&#39; DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.</code></pre>

### `DataSlice.dict_size(self) -> DataSlice` {#DataSlice.dict_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a Dict.</code></pre>

### `DataSlice.display(self: DataSlice, options: Any | None = None) -> None` {#DataSlice.display}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Visualizes a DataSlice as an html widget.

Args:
  self: The DataSlice to visualize.
  options: This should be a `koladata.ext.vis.DataSliceVisOptions`.</code></pre>

### `DataSlice.embed_schema()` {#DataSlice.embed_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as &#39;__schema__&#39; attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.</code></pre>

### `DataSlice.enriched(self, *bag: DataBag) -> DataSlice` {#DataSlice.enriched}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with a additional fallback DataBag(s).

Values in the original DataBag of `ds` take precedence over the ones in
`*bag`. The original DataBag (if present) must be immutable. If the original
DataBag is mutable, either freeze `ds` first, or add updates inplace using
mutable API.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`enriched(x, a, b)` is equivalent to `enriched(enriched(x, a), b)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: additional fallback DataBag(s).

Returns:
  DataSlice with additional fallbacks.</code></pre>

### `DataSlice.expand_to(self, target: Any, ndim: Any = unspecified) -> DataSlice` {#DataSlice.expand_to}

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

### `DataSlice.explode(self, ndim: int | DataSlice = 1) -> DataSlice` {#DataSlice.explode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Explodes a List DataSlice `x` a specified number of times.

A single list &#34;explosion&#34; converts a rank-K DataSlice of LIST[T] to a
rank-(K+1) DataSlice of T, by unpacking the items in the Lists in the original
DataSlice as a new DataSlice dimension in the result. Missing values in the
original DataSlice are treated as empty lists.

A single list explosion can also be done with `x[:]`.

If `ndim` is set to a non-negative integer, explodes recursively `ndim` times.
An `ndim` of zero is a no-op.

If `ndim` is set to a negative integer, explodes as many times as possible,
until at least one of the items of the resulting DataSlice is not a List.

Args:
  x: DataSlice of Lists to explode
  ndim: the number of explosion operations to perform, defaults to 1

Returns:
  DataSlice</code></pre>

### `DataSlice.extract(self, schema: Any = unspecified) -> DataSlice` {#DataSlice.extract}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.</code></pre>

### `DataSlice.extract_update(self, schema: Any = unspecified) -> DataBag` {#DataSlice.extract_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataBag containing only reachable attrs from &#39;ds&#39;.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from &#39;ds&#39;.</code></pre>

### `DataSlice.flatten(self, from_dim: int | DataSlice = 0, to_dim: Any = unspecified) -> DataSlice` {#DataSlice.flatten}

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

### `DataSlice.flatten_end(self, n_times: int | DataSlice = 1) -> DataSlice` {#DataSlice.flatten_end}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with a shape flattened `n_times` from the end.

The new shape has x.get_ndim() - n_times dimensions.

Given that flattening happens from the end, only positive integers are
allowed. For more control over flattening, please use `kd.flatten`, instead.

Args:
  x: a DataSlice.
  n_times: number of dimensions to flatten from the end
    (0 &lt;= n_times &lt;= rank).</code></pre>

### `DataSlice.follow(self) -> DataSlice` {#DataSlice.follow}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it&#39;s attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.</code></pre>

### `DataSlice.fork_bag(self) -> DataSlice` {#DataSlice.fork_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of the DataSlice with a forked mutable DataBag.</code></pre>

### `DataSlice.freeze_bag()` {#DataSlice.freeze_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen DataSlice equivalent to `self`.</code></pre>

### `DataSlice.from_vals(x, /, schema=None)` {#DataSlice.from_vals}

Alias for [kd.slices.slice](kd/slices.md#kd.slices.slice)

### `DataSlice.get_attr(attr_name, /, default=None)` {#DataSlice.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Gets attribute `attr_name` where missing items are filled from `default`.

Args:
  attr_name: name of the attribute to get.
  default: optional default value to fill missing items.
           Note that this value can be fully omitted.</code></pre>

### `DataSlice.get_bag()` {#DataSlice.get_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attached DataBag.</code></pre>

### `DataSlice.get_dtype(self) -> DataSlice` {#DataSlice.get_dtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a primitive schema representing the underlying items&#39; dtype.

If `ds` has a primitive schema, this returns that primitive schema, even if
all items in `ds` are missing. If `ds` has an OBJECT schema but contains
primitive values of a single dtype, it returns the schema for that primitive
dtype.

In case of items in `ds` have non-primitive types or mixed dtypes, returns
a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

Examples:
  kd.get_primitive_schema(kd.slice([1, 2, 3])) -&gt; kd.INT32
  kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -&gt; kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -&gt; kd.INT32
  kd.get_primitive_schema(kd.slice([1, &#39;a&#39;, 3], kd.OBJECT)) -&gt; missing schema
  kd.get_primitive_schema(kd.obj())) -&gt; missing schema

Args:
  ds: DataSlice to get dtype from.

Returns:
  a primitive schema DataSlice.</code></pre>

### `DataSlice.get_itemid(self) -> DataSlice` {#DataSlice.get_itemid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to ITEMID using explicit (permissive) casting rules.</code></pre>

### `DataSlice.get_keys()` {#DataSlice.get_keys}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns keys of all dicts in this DataSlice.</code></pre>

### `DataSlice.get_ndim(self) -> DataSlice` {#DataSlice.get_ndim}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of dimensions of DataSlice `x`.</code></pre>

### `DataSlice.get_obj_schema(self) -> DataSlice` {#DataSlice.get_obj_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of schemas for Objects and primitives in `x`.

DataSlice `x` must have OBJECT schema.

Examples:
  db = kd.bag()
  s = db.new_schema(a=kd.INT32)
  obj = s(a=1).embed_schema()
  kd.get_obj_schema(kd.slice([1, None, 2.0, obj]))
    -&gt; kd.slice([kd.INT32, NONE, kd.FLOAT32, s])

Args:
  x: OBJECT DataSlice

Returns:
  A DataSlice of schemas.</code></pre>

### `DataSlice.get_present_count(self) -> DataSlice` {#DataSlice.get_present_count}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `DataSlice.get_schema()` {#DataSlice.get_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a schema DataItem with type information about this DataSlice.</code></pre>

### `DataSlice.get_shape()` {#DataSlice.get_shape}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the shape of the DataSlice.</code></pre>

### `DataSlice.get_size(self) -> DataSlice` {#DataSlice.get_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.</code></pre>

### `DataSlice.get_sizes(self) -> DataSlice` {#DataSlice.get_sizes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of sizes of the DataSlice&#39;s shape.</code></pre>

### `DataSlice.get_values(self, key_ds: Any = unspecified) -> DataSlice` {#DataSlice.get_values}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns values corresponding to `key_ds` for dicts in `dict_ds`.

When `key_ds` is specified, it is equivalent to dict_ds[key_ds].

When `key_ds` is unspecified, it returns all values in `dict_ds`. The result
DataSlice has one more dimension used to represent values in each dict than
`dict_ds`. While the order of values within a dict is arbitrary, it is the
same as get_keys().

Args:
  dict_ds: DataSlice of Dicts.
  key_ds: DataSlice of keys or unspecified.

Returns:
  A DataSlice of values.</code></pre>

### `DataSlice.has_attr(self, attr_name: str) -> DataSlice` {#DataSlice.has_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Indicates whether the items in `x` DataSlice have the given attribute.

This function checks for attributes based on data rather than &#34;schema&#34; and may
be slow in some cases.

Args:
  x: DataSlice
  attr_name: Name of the attribute to check.

Returns:
  A MASK DataSlice with the same shape as `x` that contains present if the
  attribute exists for the corresponding item.</code></pre>

### `DataSlice.has_bag()` {#DataSlice.has_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataSlice `ds` has a DataBag attached.</code></pre>

### `DataSlice.implode(self, ndim: int | DataSlice = 1, itemid: Any = unspecified) -> DataSlice` {#DataSlice.implode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Implodes a Dataslice `x` a specified number of times.

A single list &#34;implosion&#34; converts a rank-(K+1) DataSlice of T to a rank-K
DataSlice of LIST[T], by folding the items in the last dimension of the
original DataSlice into newly-created Lists.

If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

If `ndim` is set to a negative integer, implodes as many times as possible,
until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
nested List.

Args:
  x: the DataSlice to implode
  ndim: the number of implosion operations to perform
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  DataSlice of nested Lists</code></pre>

### `DataSlice.internal_as_arolla_value()` {#DataSlice.internal_as_arolla_value}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts primitive DataSlice / DataItem into an equivalent Arolla value.</code></pre>

### `DataSlice.internal_as_dense_array()` {#DataSlice.internal_as_dense_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts primitive DataSlice to an Arolla DenseArray with appropriate qtype.</code></pre>

### `DataSlice.internal_as_py()` {#DataSlice.internal_as_py}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Python object equivalent to this DataSlice.

If the values in this DataSlice represent objects, then the returned python
structure will contain DataItems.</code></pre>

### `DataSlice.internal_is_itemid_schema()` {#DataSlice.internal_is_itemid_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is ITEMID Schema.</code></pre>

### `DataSlice.is_dict()` {#DataSlice.is_dict}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has Dict schema or contains only dicts.</code></pre>

### `DataSlice.is_dict_schema()` {#DataSlice.is_dict_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a Dict Schema.</code></pre>

### `DataSlice.is_empty()` {#DataSlice.is_empty}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is empty.</code></pre>

### `DataSlice.is_entity()` {#DataSlice.is_entity}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has Entity schema or contains only entities.</code></pre>

### `DataSlice.is_entity_schema()` {#DataSlice.is_entity_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice represents an Entity Schema.</code></pre>

### `DataSlice.is_list()` {#DataSlice.is_list}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has List schema or contains only lists.</code></pre>

### `DataSlice.is_list_schema()` {#DataSlice.is_list_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a List Schema.</code></pre>

### `DataSlice.is_mutable()` {#DataSlice.is_mutable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff the attached DataBag is mutable.</code></pre>

### `DataSlice.is_primitive(self) -> DataSlice` {#DataSlice.is_primitive}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is a primitive DataSlice.

`x` is a primitive DataSlice if it meets one of the following conditions:
  1) it has a primitive schema
  2) it has OBJECT/SCHEMA/NONE schema and only has primitives

Also see `kd.has_primitive` for a pointwise version. But note that
`kd.all(kd.has_primitive(x))` is not always equivalent to
`kd.is_primitive(x)`. For example,

  kd.is_primitive(kd.int32(None)) -&gt; kd.present
  kd.all(kd.has_primitive(kd.int32(None))) -&gt; invalid for kd.all
  kd.is_primitive(kd.int32([None])) -&gt; kd.present
  kd.all(kd.has_primitive(kd.int32([None]))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `DataSlice.is_primitive_schema()` {#DataSlice.is_primitive_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a primitive (scalar) Schema.</code></pre>

### `DataSlice.is_struct_schema()` {#DataSlice.is_struct_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice represents a Struct Schema.</code></pre>

### `DataSlice.list_size(self) -> DataSlice` {#DataSlice.list_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a List.</code></pre>

### `DataSlice.maybe(self, attr_name: str) -> DataSlice` {#DataSlice.maybe}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A shortcut for kd.get_attr(x, attr_name, default=None).</code></pre>

### `DataSlice.new(self, **attrs)` {#DataSlice.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new Entity with this Schema.</code></pre>

### `DataSlice.no_bag()` {#DataSlice.no_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice without DataBag.</code></pre>

### `DataSlice.pop(index, /)` {#DataSlice.pop}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Pop a value from each list in this DataSlice</code></pre>

### `DataSlice.ref(self) -> DataSlice` {#DataSlice.ref}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.</code></pre>

### `DataSlice.repeat(self, sizes: Any) -> DataSlice` {#DataSlice.repeat}

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

### `DataSlice.reshape(self, shape: JaggedShape) -> DataSlice` {#DataSlice.reshape}

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

### `DataSlice.reshape_as(self, shape_from: DataSlice) -> DataSlice` {#DataSlice.reshape_as}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice x reshaped to the shape of DataSlice shape_from.</code></pre>

### `DataSlice.select(self, fltr: Any, expand_filter: bool | DataSlice = True) -> DataSlice` {#DataSlice.select}

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

### `DataSlice.select_items(self, fltr: Any) -> DataSlice` {#DataSlice.select_items}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Selects List items by filtering out missing items in fltr.

Also see kd.select.

Args:
  ds: List DataSlice to be filtered
  fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
    Functor or a Python function which can be evalauted to such DataSlice. A
    Python function will be traced for evaluation, so it cannot have Python
    control flow operations such as `if` or `while`.

Returns:
  Filtered DataSlice.</code></pre>

### `DataSlice.select_keys(self, fltr: Any) -> DataSlice` {#DataSlice.select_keys}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Selects Dict keys by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice. A Python function will
    be traced for evaluation, so it cannot have Python control flow operations
    such as `if` or `while`.

Returns:
  Filtered DataSlice.</code></pre>

### `DataSlice.select_present(self) -> DataSlice` {#DataSlice.select_present}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataSlice by removing missing items.

It is not supported for DataItems because their sizes are always 1.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select_present(val) -&gt; [[1, 4], [], [2, 8]]

Args:
  ds: DataSlice with ndim &gt; 0 to be filtered.

Returns:
  Filtered DataSlice.</code></pre>

### `DataSlice.select_values(self, fltr: Any) -> DataSlice` {#DataSlice.select_values}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Selects Dict values by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice. A Python function will
    be traced for evaluation, so it cannot have Python control flow operations
    such as `if` or `while`.

Returns:
  Filtered DataSlice.</code></pre>

### `DataSlice.set_attr(attr_name, value, /, overwrite_schema=False)` {#DataSlice.set_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets an attribute `attr_name` to `value`.

Requires DataSlice to have a mutable DataBag attached. Compared to
`__setattr__`, it allows overwriting the schema for attribute `attr_name` when
`overwrite_schema` is True. Additionally, it allows `attr_name` to be a
non-Python-identifier (e.g. &#34;123-f&#34;, &#34;5&#34;, &#34;%#$&#34;, etc.). `attr_name` still has to
be a valid UTF-8 unicode.

Args:
  attr_name: UTF-8 unicode representing the attribute name.
  value: new value for attribute `attr_name`.
  overwrite_schema: if True, schema for attribute is always updated.</code></pre>

### `DataSlice.set_attrs(*, overwrite_schema=False, **attrs)` {#DataSlice.set_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets multiple attributes on an object / entity.

Args:
  overwrite_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.</code></pre>

### `DataSlice.set_schema(schema, /)` {#DataSlice.set_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.</code></pre>

### `DataSlice.shallow_clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.shallow_clone}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with shallow clones of immediate attributes.

The entities themselves get new ItemIds and their top-level attributes are
copied by reference.

Also see kd.clone and kd.deep_clone.

Note that unlike kd.deep_clone, if there are multiple references to the same
entity, the returned DataSlice will have multiple clones of it rather than
references to the same clone.

Args:
  x: The DataSlice to copy.{SELF}
  itemid: The ItemId to assign to cloned entities. If not specified, will
    allocate new ItemIds.
  schema: The schema to resolve attributes, and also to assign the schema to
    the resulting DataSlice. If not specified, will use the schema of &#39;x&#39;.
  **overrides: attribute overrides.

Returns:
  A copy of the entities with new ItemIds where all top-level attributes are
  copied by reference.</code></pre>

### `DataSlice.strict_with_attrs(self, **attrs) -> DataSlice` {#DataSlice.strict_with_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `DataSlice.stub(self, attrs: DataSlice = DataSlice([], schema: NONE, present: 0/0)) -> DataSlice` {#DataSlice.stub}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Copies a DataSlice&#39;s schema stub to a new DataBag.

The &#34;schema stub&#34; of a DataSlice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that DataSlice.

Optionally copies `attrs` schema attributes to the new DataBag as well.

This method works for items, objects, and for lists and dicts stored as items
or objects. The intended usage is to add new attributes to the object in the
new bag, or new items to the dict in the new bag, and then to be able
to merge the bags to obtain a union of attributes/values. For lists, we
extract the list with stubs for list items, which also works recursively so
nested lists are deep-extracted. Note that if you modify the list afterwards
by appending or removing items, you will no longer be able to merge the result
with the original bag.

Args:
  x: DataSlice to extract the schema stub from.
  attrs: Optional list of additional schema attribute names to copy. The
    schemas for those attributes will be copied recursively (so including
    attributes of those attributes etc).

Returns:
  DataSlice with the same schema stub in the new DataBag.</code></pre>

### `DataSlice.take(self, indices: Any) -> DataSlice` {#DataSlice.take}

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

### `DataSlice.to_py(ds: DataSlice, max_depth: int = 2, obj_as_dict: bool = False, include_missing_attrs: bool = True, output_class: Any | None = None) -> Any` {#DataSlice.to_py}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a readable python object from a DataSlice.

Attributes, lists, and dicts are recursively converted to Python objects.

Args:
  ds: A DataSlice
  max_depth: Maximum depth for recursive printing. Each attribute, list, and
    dict increments the depth by 1. Use -1 for unlimited depth.
  obj_as_dict: Whether to convert objects to python dicts. By default objects
    are converted to automatically constructed &#39;Obj&#39; dataclass instances.
  include_missing_attrs: whether to include attributes with None value in
    objects.
  output_class: If not None, will be used recursively as the output type.</code></pre>

### `DataSlice.to_pytree(ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True) -> Any` {#DataSlice.to_pytree}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a readable python object from a DataSlice.

Attributes, lists, and dicts are recursively converted to Python objects.
Objects are converted to Python dicts.

Same as kd.to_py(..., obj_as_dict=True)

Args:
  ds: A DataSlice
  max_depth: Maximum depth for recursive printing. Each attribute, list, and
    dict increments the depth by 1. Use -1 for unlimited depth.
  include_missing_attrs: whether to include attributes with None value in
    objects.</code></pre>

### `DataSlice.updated(self, *bag: DataBag) -> DataSlice` {#DataSlice.updated}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with DataBag(s) of updates applied.

Values in `*bag` take precedence over the ones in the original DataBag of
`ds`. The original DataBag (if present) must be immutable. If the original
DataBag is mutable, either freeze `ds` first, or add updates inplace using
mutable API.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`updated(x, a, b)` is equivalent to `updated(updated(x, b), a)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: DataBag(s) of updates.

Returns:
  DataSlice with additional fallbacks.</code></pre>

### `DataSlice.with_attr(self, attr_name: str | DataSlice, value: Any, overwrite_schema: bool | DataSlice = False) -> DataSlice` {#DataSlice.with_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing a single updated attribute.

This operator is useful if attr_name cannot be used as a key in keyword
arguments. E.g.: &#34;123-f&#34;, &#34;5&#34;, &#34;%#$&#34;, etc. It still has to be a valid utf-8
unicode.

See kd.with_attrs docstring for more details on the rules and regarding
`overwrite` argument.

Args:
  x: Entity / Object for which the attribute update is being created.
  attr_name: utf-8 unicode representing the attribute name.
  value: new value for attribute `attr_name`.
  overwrite_schema: if True, schema for attribute is always updated.</code></pre>

### `DataSlice.with_attrs(self, *, overwrite_schema: bool | DataSlice = False, **attrs) -> DataSlice` {#DataSlice.with_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `x`.

This is a shorter version of `x.updated(kd.attrs(x, ...))`.

Example:
  x = x.with_attrs(foo=..., bar=...)
  # Or equivalent:
  # x = kd.with_attrs(x, foo=..., bar=...)

In case some attribute &#34;foo&#34; already exists and the update contains &#34;foo&#34;,
either:
  1) the schema of &#34;foo&#34; in the update must be implicitly castable to
     `x.foo.get_schema()`; or
  2) `x` is an OBJECT, in which case schema for &#34;foo&#34; will be overwritten.

An exception to (2) is if it was an Entity that was casted to an OBJECT using
kd.obj, e.g. then update for &#34;foo&#34; also must be castable to
`x.foo.get_schema()`. If this is not the case, an Error is raised.

This behavior can be overwritten by passing `overwrite=True`, which will cause
the schema for attributes to always be updated.

Args:
  x: Entity / Object for which the attributes update is being created.
  overwrite_schema: if True, schema for attributes is always updated.
  **attrs: attrs to set in the update.</code></pre>

### `DataSlice.with_bag(bag, /)` {#DataSlice.with_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with DataBag `db`.</code></pre>

### `DataSlice.with_dict_update(self, keys: Any, values: Any = unspecified) -> DataSlice` {#DataSlice.with_dict_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated dicts.

This operator has two forms:
  kd.with_dict_update(x, keys, values) where keys and values are slices
  kd.with_dict_update(x, dict_updates) where dict_updates is a DataSlice of
    dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to &#39;x&#39;.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.</code></pre>

### `DataSlice.with_list_append_update(self, append: Any) -> DataSlice` {#DataSlice.with_list_append_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated appended lists.

The updated lists are the lists in `x` with the specified items appended at
the end.

`x` and `append` must have compatible shapes.

The resulting lists maintain the same ItemIds. Also see kd.appended_list()
which works similarly but resulting lists have new ItemIds.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  A DataSlice of lists in a new immutable DataBag.</code></pre>

### `DataSlice.with_merged_bag(self) -> DataSlice` {#DataSlice.with_merged_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the DataBag of `ds` merged with its fallbacks.

Note that a DataBag has multiple fallback DataBags and fallback DataBags can
have fallbacks as well. This operator merges all of them into a new immutable
DataBag.

If `ds` has no attached DataBag, it raises an exception. If the DataBag of
`ds` does not have fallback DataBags, it is equivalent to `ds.freeze_bag()`.

Args:
  ds: DataSlice to merge fallback DataBags of.

Returns:
  A new DataSlice with an immutable DataBags.</code></pre>

### `DataSlice.with_name(obj: Any, name: str | Text) -> Any` {#DataSlice.with_name}

Alias for [kd.annotation.with_name](kd/annotation.md#kd.annotation.with_name)

### `DataSlice.with_schema(schema, /)` {#DataSlice.with_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.</code></pre>

### `DataSlice.with_schema_from_obj(self) -> DataSlice` {#DataSlice.with_schema_from_obj}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.</code></pre>

