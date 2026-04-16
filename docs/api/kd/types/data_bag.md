<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types.DataBag API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Base class of all Arolla values in Python.

QValue is immutable. It provides only basic functionality.
Subclasses of this class might have further specialization.
</code></pre>





### `DataBag.adopt(slice, /)` {#kd.types.DataBag.adopt}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Adopts all data reachable from the given slice into this DataBag.

Args:
  slice: DataSlice to adopt data from.

Returns:
  The DataSlice with this DataBag (including adopted data) attached.</code></pre>

### `DataBag.adopt_stub(slice, /)` {#kd.types.DataBag.adopt_stub}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Copies the given DataSlice&#39;s schema stub into this DataBag.

The &#34;schema stub&#34; of a DataSlice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that DataSlice. See kd.stub() for more details.

Args:
  slice: DataSlice to extract the schema stub from.

Returns:
  The &#34;stub&#34; with this DataBag attached.</code></pre>

### `DataBag.concat_lists(self: DataBag, /, *lists: _DataSlice) -> _DataSlice` {#kd.types.DataBag.concat_lists}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of Lists concatenated from the List items of `lists`.

Each input DataSlice must contain only present List items, and the item
schemas of each input must be compatible. Input DataSlices are aligned (see
`kd.align`) automatically before concatenation.

If `lists` is empty, this returns a single empty list.

The specified `db` is used to create the new concatenated lists, and is the
DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
is created for this purpose.

Args:
  *lists: the DataSlices of Lists to concatenate
  db: optional DataBag to populate with the result

Returns:
  DataSlice of concatenated Lists</code></pre>

### `DataBag.contents_repr(self: DataBag, /, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#kd.types.DataBag.contents_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of the DataBag contents.</code></pre>

### `DataBag.data_triples_repr(self: DataBag, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#kd.types.DataBag.data_triples_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of the DataBag contents, omitting schema triples.</code></pre>

### `DataBag.dict(self: DataBag, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.dict}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a Koda dict.

Acceptable arguments are:
  1) no argument: a single empty dict
  2) a Python dict whose keys are either primitives or DataItems and values
     are primitives, DataItems, Python list/dict which can be converted to a
     List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
     a single dict
  3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
     shape is the last N-1 dimensions of keys/values DataSlice

Examples:
dict() -&gt; returns a single new dict
dict({1: 2, 3: 4}) -&gt; returns a single new dict
dict({1: [1, 2]}) -&gt; returns a single dict, mapping 1-&gt;List[1, 2]
dict({1: kd.slice([1, 2])}) -&gt; returns a single dict, mapping 1-&gt;List[1, 2]
dict({db.uuobj(x=1, y=2): 3}) -&gt; returns a single dict, mapping uuid-&gt;3
dict(kd.slice([1, 2]), kd.slice([3, 4])) -&gt; returns a dict, mapping 1-&gt;3 and
2-&gt;4
dict(kd.slice([[1], [2]]), kd.slice([3, 4])) -&gt; returns two dicts, one
mapping
  1-&gt;3 and another mapping 2-&gt;4
dict(&#39;key&#39;, 12) -&gt; returns a single dict mapping &#39;key&#39;-&gt;12

Args:
  items_or_keys: a Python dict in case of items and a DataSlice in case of
    keys.
  values: a DataSlice. If provided, `items_or_keys` must be a DataSlice as
    keys.
  key_schema: the schema of the dict keys. If not specified, it will be
    deduced from keys or defaulted to OBJECT.
  value_schema: the schema of the dict values. If not specified, it will be
    deduced from values or defaulted to OBJECT.
  schema: The schema to use for the newly created Dict. If specified, then
    key_schema and value_schema must not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

Returns:
  A DataSlice with the dict.</code></pre>

### `DataBag.dict_like(self: DataBag, shape_and_mask_from: _DataSlice, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.dict_like}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

If items_or_keys and values are not provided, creates empty dicts. Otherwise,
the function assigns the given keys and values to the newly created dicts. So
the keys and values must be either broadcastable to shape_and_mask_from
shape, or one dimension higher.

Args:
  self: the DataBag.
  shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
    dicts.
  items_or_keys: either a Python dict (if `values` is None) or a DataSlice
    with keys. The Python dict case is supported only for scalar
    shape_and_mask_from.
  values: a DataSlice of values, when `items_or_keys` represents keys.
  key_schema: the schema of the dict keys. If not specified, it will be
    deduced from keys or defaulted to OBJECT.
  value_schema: the schema of the dict values. If not specified, it will be
    deduced from values or defaulted to OBJECT.
  schema: The schema to use for the newly created Dict. If specified, then
    key_schema and value_schema must not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

Returns:
  A DataSlice with the dicts.</code></pre>

### `DataBag.dict_schema(key_schema, value_schema)` {#kd.types.DataBag.dict_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a dict schema from the schemas of the keys and values</code></pre>

### `DataBag.dict_shaped(self: DataBag, shape: _jagged_shape.JaggedShape, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.dict_shaped}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda dicts with the given shape.

If items_or_keys and values are not provided, creates empty dicts. Otherwise,
the function assigns the given keys and values to the newly created dicts. So
the keys and values must be either broadcastable to `shape` or one dimension
higher.

Args:
  self: the DataBag.
  shape: the desired shape.
  items_or_keys: either a Python dict (if `values` is None) or a DataSlice
    with keys. The Python dict case is supported only for scalar shape.
  values: a DataSlice of values, when `items_or_keys` represents keys.
  key_schema: the schema of the dict keys. If not specified, it will be
    deduced from keys or defaulted to OBJECT.
  value_schema: the schema of the dict values. If not specified, it will be
    deduced from values or defaulted to OBJECT.
  schema: The schema to use for the newly created Dict. If specified, then
    key_schema and value_schema must not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

Returns:
  A DataSlice with the dicts.</code></pre>

### `DataBag.empty()` {#kd.types.DataBag.empty}
Aliases:

- [kd.bags.new](../bags.md#kd.bags.new)

- [kd.bag](../../kd.md#kd.bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an empty immutable DataBag.</code></pre>

### `DataBag.empty_mutable()` {#kd.types.DataBag.empty_mutable}
Aliases:

- [kd.mutable_bag](../../kd.md#kd.mutable_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an empty mutable DataBag. Only works in eager mode.</code></pre>

### `DataBag.fork(mutable=True)` {#kd.types.DataBag.fork}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a newly created DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.

Args:
  mutable: If true (default), returns a mutable DataBag. If false, the DataBag
    will be immutable.
Returns:
  data_bag.DataBag</code></pre>

### `DataBag.freeze(self: DataBag) -> DataBag` {#kd.types.DataBag.freeze}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen DataBag equivalent to `self`.</code></pre>

### `DataBag.get_approx_byte_size()` {#kd.types.DataBag.get_approx_byte_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns approximate size of the DataBag in bytes.</code></pre>

### `DataBag.get_approx_size()` {#kd.types.DataBag.get_approx_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns approximate size of the DataBag in triples.</code></pre>

### `DataBag.implode(self: DataBag, x: _DataSlice, /, ndim: int | _DataSlice = 1, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.implode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Implodes a Dataslice `x` a specified number of times.

A single list &#34;implosion&#34; converts a rank-(K+1) DataSlice of T to a rank-K
DataSlice of LIST[T], by folding the items in the last dimension of the
original DataSlice into newly-created Lists.

If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

If `ndim` is set to a negative integer, implodes as many times as possible,
until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
nested List.

The specified `db` is used to create any new Lists, and is the DataBag of the
result DataSlice. If `db` is not specified, a new, empty DataBag is created
for this purpose.

Args:
  x: the DataSlice to implode
  ndim: the number of implosion operations to perform
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
  db: optional DataBag where Lists are created from

Returns:
  DataSlice of nested Lists</code></pre>

### `DataBag.is_empty()` {#kd.types.DataBag.is_empty}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if the DataBag is empty.</code></pre>

### `DataBag.is_mutable()` {#kd.types.DataBag.is_mutable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataBag is mutable.</code></pre>

### `DataBag.list(self: DataBag, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.list}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates list(s) by collapsing `items`.

If there is no argument, returns an empty Koda List.
If the argument is a Python list, creates a nested Koda List.

Examples:
list() -&gt; a single empty Koda List
list([1, 2, 3]) -&gt; Koda List with items 1, 2, 3
list([[1, 2, 3], [4, 5]]) -&gt; nested Koda List [[1, 2, 3], [4, 5]]
  # items are Koda lists.

Args:
  items: The items to use. If not specified, an empty list of OBJECTs will be
    created.
  item_schema: the schema of the list items. If not specified, it will be
    deduced from `items` or defaulted to OBJECT.
  schema: The schema to use for the list. If specified, then item_schema must
    not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the list/lists.</code></pre>

### `DataBag.list_like(self: DataBag, shape_and_mask_from: _DataSlice, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.list_like}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

Args:
  shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
    lists.
  items: optional items to assign to the newly created lists. If not given,
    the function returns empty lists.
  item_schema: the schema of the list items. If not specified, it will be
    deduced from `items` or defaulted to OBJECT.
  schema: The schema to use for the list. If specified, then item_schema must
    not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the lists.</code></pre>

### `DataBag.list_schema(item_schema)` {#kd.types.DataBag.list_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list schema from the schema of the items</code></pre>

### `DataBag.list_shaped(self: DataBag, shape: _jagged_shape.JaggedShape, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#kd.types.DataBag.list_shaped}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda lists with the given shape.

Args:
  shape: the desired shape.
  items: optional items to assign to the newly created lists. If not given,
    the function returns empty lists.
  item_schema: the schema of the list items. If not specified, it will be
    deduced from `items` or defaulted to OBJECT.
  schema: The schema to use for the list. If specified, then item_schema must
    not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the lists.</code></pre>

### `DataBag.merge_fallbacks()` {#kd.types.DataBag.merge_fallbacks}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag with all the fallbacks merged.</code></pre>

### `DataBag.merge_inplace(self: DataBag, other_bags: DataBag | Iterable[DataBag], /, *, overwrite: bool = True, allow_data_conflicts: bool = True, allow_schema_conflicts: bool = False) -> DataBag` {#kd.types.DataBag.merge_inplace}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Copies all data from `other_bags` to this DataBag.

Args:
  other_bags: Either a DataBag or a list of DataBags to merge into the current
    DataBag.
  overwrite: In case of conflicts, whether the new value (or the rightmost of
    the new values, if multiple) should be used instead of the old value. Note
    that this flag has no effect when allow_data_conflicts=False and
    allow_schema_conflicts=False. Note that db1.fork().inplace_merge(db2,
    overwrite=False) and db2.fork().inplace_merge(db1, overwrite=True) produce
    the same result.
  allow_data_conflicts: Whether we allow the same attribute to have different
    values in the bags being merged. When True, the overwrite= flag controls
    the behavior in case of a conflict. By default, both this flag and
    overwrite= are True, so we overwrite with the new values in case of a
    conflict.
  allow_schema_conflicts: Whether we allow the same attribute to have
    different types in an explicit schema. Note that setting this flag to True
    can be dangerous, as there might be some objects with the old schema that
    are not overwritten, and therefore will end up in an inconsistent state
    with their schema after the overwrite. When True, overwrite= flag controls
    the behavior in case of a conflict.

Returns:
  self, so that multiple DataBag modifications can be chained.</code></pre>

### `DataBag.named_schema(name, /, **attrs)` {#kd.types.DataBag.named_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a named schema with ItemId derived only from its name.</code></pre>

### `DataBag.new(*, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#kd.types.DataBag.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Entities with given attrs.

Args:
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive slice
    if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.new_like(shape_and_mask_from, *, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#kd.types.DataBag.new_like}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Entities with the shape and sparsity from shape_and_mask_from.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.new_schema(**attrs)` {#kd.types.DataBag.new_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new schema object with given types of attrs.</code></pre>

### `DataBag.new_shaped(shape, *, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#kd.types.DataBag.new_shaped}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Entities with the given shape.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.obj(arg, *, itemid=None, **attrs)` {#kd.types.DataBag.obj}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Koda object or Python primitive to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    ItemIds will only be set when the arg is not provided, otherwise an error
      will be raised.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.</code></pre>

### `DataBag.obj_like(shape_and_mask_from, *, itemid=None, **attrs)` {#kd.types.DataBag.obj_like}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  db: optional DataBag where entities are created.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.obj_shaped(shape, *, itemid=None, **attrs)` {#kd.types.DataBag.obj_shaped}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.overwriting_merge_update(other_db)` {#kd.types.DataBag.overwriting_merge_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag with the update from other_db.

db.overwriting_merge_update(other_db) returns a DataBag tht can be passed
instead of `other_db` to `merge_inplace` to get the same result.
db.merge_inplace(other_db, allow_schema_conflicts=True).

Notes about the returned DataBag:
1. It may still contain data that is present in &#34;self&#34;.
2. It may lack schema information and so it couldn&#39;t be used directly.

Args:
  other_db: DataBag to overwrite data and schema from.

Returns:
  DataBag with the update from other_db.</code></pre>

### `DataBag.schema_triples_repr(self: DataBag, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#kd.types.DataBag.schema_triples_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of schema triples in the DataBag.</code></pre>

### `DataBag.uu(seed='', *, schema=None, overwrite_schema=False, **kwargs)` {#kd.types.DataBag.uu}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an item whose ids are uuid(s) with the set attributes.

In order to create a different &#34;Type&#34; from the same arguments, use
`seed` key with the desired value, e.g.

kd.uu(seed=&#39;type_1&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uu(seed=&#39;type_2&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

If &#39;schema&#39; is provided, the resulting DataSlice has the provided schema.
Otherwise, uses the corresponding uuschema instead.

Args:
  seed: (str) Allows different item(s) to have different ids when created
    from the same inputs.
  schema: schema for the resulting DataSlice
  overwrite_schema: if true, will overwrite schema attributes in the schema&#39;s
    corresponding db from the argument values.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
    </code></pre>

### `DataBag.uu_schema(seed='', **attrs)` {#kd.types.DataBag.uu_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new uuschema from given types of attrs.</code></pre>

### `DataBag.uuobj(seed='', **kwargs)` {#kd.types.DataBag.uuobj}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates object(s) whose ids are uuid(s) with the provided attributes.

In order to create a different &#34;Type&#34; from the same arguments, use
`seed` key with the desired value, e.g.

kd.uuobj(seed=&#39;type_1&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uuobj(seed=&#39;type_2&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

Args:
  seed: (str) Allows different uuobj(s) to have different ids when created
    from the same inputs.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
    </code></pre>

### `DataBag.with_name(obj: Any, name: str | Text) -> Any` {#kd.types.DataBag.with_name}
Aliases:

- [kd.types.DataSlice.with_name](data_slice.md#kd.types.DataSlice.with_name)

- [kd.annotation.with_name](../annotation.md#kd.annotation.with_name)

- [kd.with_name](../../kd.md#kd.with_name)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Checks that the `name` is a string and returns `obj` unchanged.

This method is useful in tracing workflows: when tracing, we will assign
the given name to the subexpression computing `obj`. In eager mode, this
method is effectively a no-op.

Args:
  obj: Any object.
  name: The name to be used for this sub-expression when tracing this code.
    Must be a string.

Returns:
  obj unchanged.</code></pre>

