<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.schema API

Schema-related operators.





### `kd.schema.agg_common_schema(x, ndim=unspecified)` {#kd.schema.agg_common_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the common schema of `x` along the last `ndim` dimensions.

The &#34;common schema&#34; is defined according to go/koda-type-promotion.

Examples:
  kd.agg_common_schema(kd.slice([kd.INT32, None, kd.FLOAT32]))
    # -&gt; kd.FLOAT32

  kd.agg_common_schema(kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]))
    # -&gt; kd.slice([kd.INT32, kd.FLOAT64])

  kd.agg_common_schema(
      kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]), ndim=2)
    # -&gt; kd.FLOAT64

Args:
  x: DataSlice of schemas.
  ndim: The number of last dimensions to aggregate over.</code></pre>

### `kd.schema.cast_to(x, schema)` {#kd.schema.cast_to}
Aliases:

- [kd.cast_to](../kd.md#kd.cast_to)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema` using explicit casting rules.

Dispatches to the relevant `kd.to_...` operator. Performs permissive casting,
e.g. allowing FLOAT32 -&gt; INT32 casting through `kd.cast_to(slice, INT32)`.

Note that `x` must be correctly typed with its schema. Thus, if provided
`schema` is equal to `x.get_schema()`, operator does nothing.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to.</code></pre>

### `kd.schema.cast_to_implicit(x, schema)` {#kd.schema.cast_to_implicit}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema` using implicit casting rules.

Note that `schema` must be the common schema of `schema` and `x.get_schema()`
according to go/koda-type-promotion.

Note that `x` must be correctly typed with its schema. Thus, if provided
`schema` is equal to `x.get_schema()`, operator does nothing.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.</code></pre>

### `kd.schema.cast_to_narrow(x, schema)` {#kd.schema.cast_to_narrow}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema`.

Allows for schema narrowing, where OBJECT types can be casted to primitive
schemas as long as the data is implicitly castable to the schema. Follows the
casting rules of `kd.cast_to_implicit` for the narrowed schema.

Note that `x` must be correctly typed with its schema. Thus, if provided
`schema` is equal to `x.get_schema()`, operator does nothing.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.</code></pre>

### `kd.schema.common_schema(x)` {#kd.schema.common_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the common schema as a scalar DataItem of `x`.

The &#34;common schema&#34; is defined according to go/koda-type-promotion.

Args:
  x: DataSlice of schemas.</code></pre>

### `kd.schema.deep_cast_to(x, schema, allow_removing_attrs=False, allow_new_attrs=False)` {#kd.schema.deep_cast_to}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to provided `schema` using explicit casting rules.

In contrast to `kd.cast_to`, this operator always performs deep casting - even
when x.get_schema().get_itemid() == schema.get_itemid().

Args:
  x: DataSlice to cast.
  schema: Schema to cast to.
  allow_removing_attrs: If True, the `schema` may omit attributes that are
    present in `x.get_schema()`. The values of such attributes would be
    omitted from the result.
  allow_new_attrs: If True, the `schema` may have additional attributes that
    are not present in `x.get_schema()`. Additional attributes are set to
    missing values.</code></pre>

### `kd.schema.dict_schema(key_schema, value_schema)` {#kd.schema.dict_schema}
Aliases:

- [kd.dict_schema](../kd.md#kd.dict_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Dict schema with the provided `key_schema` and `value_schema`.</code></pre>

### `kd.schema.get_dtype(ds)` {#kd.schema.get_dtype}
Aliases:

- [kd.schema.get_primitive_schema](#kd.schema.get_primitive_schema)

- [kd.get_dtype](../kd.md#kd.get_dtype)

- [kd.get_primitive_schema](../kd.md#kd.get_primitive_schema)

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

### `kd.schema.get_item_schema(list_schema)` {#kd.schema.get_item_schema}
Aliases:

- [kd.get_item_schema](../kd.md#kd.get_item_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the item schema of a List schema`.</code></pre>

### `kd.schema.get_itemid(x)` {#kd.schema.get_itemid}
Aliases:

- [kd.schema.to_itemid](#kd.schema.to_itemid)

- [kd.get_itemid](../kd.md#kd.get_itemid)

- [kd.to_itemid](../kd.md#kd.to_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to ITEMID using explicit (permissive) casting rules.</code></pre>

### `kd.schema.get_key_schema(dict_schema)` {#kd.schema.get_key_schema}
Aliases:

- [kd.get_key_schema](../kd.md#kd.get_key_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the key schema of a Dict schema`.</code></pre>

### `kd.schema.get_nofollowed_schema(schema)` {#kd.schema.get_nofollowed_schema}
Aliases:

- [kd.get_nofollowed_schema](../kd.md#kd.get_nofollowed_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original schema from nofollow schema.

Requires `nofollow_schema` to be a nofollow schema, i.e. that it wraps some
other schema.

Args:
  schema: nofollow schema DataSlice.</code></pre>

### `kd.schema.get_obj_schema(x)` {#kd.schema.get_obj_schema}
Aliases:

- [kd.get_obj_schema](../kd.md#kd.get_obj_schema)

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

### `kd.schema.get_primitive_schema(ds)` {#kd.schema.get_primitive_schema}

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype)

### `kd.schema.get_repr(schema)` {#kd.schema.get_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a string representation of the schema.

Named schemas are only represented by their name. Other schemas are
represented by their content.

Args:
  schema: A scalar schema DataSlice.
Returns:
  A scalar string DataSlice. A repr of the given schema.</code></pre>

### `kd.schema.get_schema(x)` {#kd.schema.get_schema}
Aliases:

- [kd.get_schema](../kd.md#kd.get_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of `x`.</code></pre>

### `kd.schema.get_value_schema(dict_schema)` {#kd.schema.get_value_schema}
Aliases:

- [kd.get_value_schema](../kd.md#kd.get_value_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value schema of a Dict schema`.</code></pre>

### `kd.schema.internal_maybe_named_schema(name_or_schema)` {#kd.schema.internal_maybe_named_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a string to a named schema, passes through schema otherwise.

The operator also passes through arolla.unspecified, and raises when
it receives anything else except unspecified, string or schema DataItem.

This operator exists to support kd.core.new* family of operators.

Args:
  name_or_schema: The input name or schema.

Returns:
  The schema unchanged, or a named schema with the given name.</code></pre>

### `kd.schema.is_dict_schema(x)` {#kd.schema.is_dict_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `x` is a Dict schema DataItem.</code></pre>

### `kd.schema.is_entity_schema(x)` {#kd.schema.is_entity_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `x` is an Entity schema DataItem.</code></pre>

### `kd.schema.is_list_schema(x)` {#kd.schema.is_list_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `x` is a List schema DataItem.</code></pre>

### `kd.schema.is_primitive_schema(x)` {#kd.schema.is_primitive_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `x` is a primitive schema DataItem.</code></pre>

### `kd.schema.is_struct_schema(x)` {#kd.schema.is_struct_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `x` is a Struct schema DataItem.</code></pre>

### `kd.schema.list_schema(item_schema)` {#kd.schema.list_schema}
Aliases:

- [kd.list_schema](../kd.md#kd.list_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a List schema with the provided `item_schema`.</code></pre>

### `kd.schema.named_schema(name, /, **kwargs)` {#kd.schema.named_schema}
Aliases:

- [kd.named_schema](../kd.md#kd.named_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a named entity schema.

A named schema will have its item id derived only from its name, which means
that two named schemas with the same name will have the same item id, even in
different DataBags, or with different kwargs passed to this method.

Args:
  name: The name to use to derive the item id of the schema.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be schemas themselves.

Returns:
  data_slice.DataSlice with the item id of the required schema and kd.SCHEMA
  schema, with a new immutable DataBag attached containing the provided
  kwargs.</code></pre>

### `kd.schema.new_schema(**kwargs)` {#kd.schema.new_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new allocated schema.

Args:
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be schemas themselves.

Returns:
  (DataSlice) containing the schema id.</code></pre>

### `kd.schema.nofollow_schema(schema)` {#kd.schema.nofollow_schema}
Aliases:

- [kd.nofollow_schema](../kd.md#kd.nofollow_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a NoFollow schema of the provided schema.

`nofollow_schema` is reversible with `get_actual_schema`.

`nofollow_schema` can only be called on implicit and explicit schemas and
OBJECT. It raises an Error if called on primitive schemas, ITEMID, etc.

Args:
  schema: Schema DataSlice to wrap.</code></pre>

### `kd.schema.schema_from_py(tpe: type[Any]) -> SchemaItem` {#kd.schema.schema_from_py}
Aliases:

- [kd.schema_from_py](../kd.md#kd.schema_from_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a Koda entity schema corresponding to the given Python type.

This method supports the following Python types / type annotations
recursively:
- Primitive types: int, float, bool, str, bytes.
- Collections: list[...], dict[...], Sequence[...], Mapping[...], ect.
- Unions: only &#34;smth | None&#34; or &#34;Optional[smth]&#34; is supported.
- Dataclasses.

This can be used in conjunction with kd.from_py to convert lists of Python
objects to efficient Koda DataSlices. Because of the &#39;efficient&#39; goal, we
create an entity schema and do not use kd.OBJECT inside, which also results
in strict type checking. If you do not care
about efficiency or type safety, you can use kd.from_py(..., schema=kd.OBJECT)
directly.

Args:
  tpe: The Python type to create a schema for.

Returns:
  A Koda entity schema corresponding to the given Python type. The returned
  schema is a uu-schema, in other words we always return the same output for
  the same input. For dataclasses, we use the module name and the class name
  to derive the itemid for the uu-schema.</code></pre>

### `kd.schema.to_bool(x)` {#kd.schema.to_bool}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to BOOLEAN using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_bytes(x)` {#kd.schema.to_bytes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to BYTES using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_expr(x)` {#kd.schema.to_expr}
Aliases:

- [kd.to_expr](../kd.md#kd.to_expr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to EXPR using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_float32(x)` {#kd.schema.to_float32}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to FLOAT32 using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_float64(x)` {#kd.schema.to_float64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to FLOAT64 using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_int32(x)` {#kd.schema.to_int32}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to INT32 using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_int64(x)` {#kd.schema.to_int64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to INT64 using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_itemid(x)` {#kd.schema.to_itemid}

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid)

### `kd.schema.to_mask(x)` {#kd.schema.to_mask}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to MASK using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_none(x)` {#kd.schema.to_none}
Aliases:

- [kd.to_none](../kd.md#kd.to_none)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to NONE using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_object(x)` {#kd.schema.to_object}
Aliases:

- [kd.to_object](../kd.md#kd.to_object)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to OBJECT using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_schema(x)` {#kd.schema.to_schema}
Aliases:

- [kd.to_schema](../kd.md#kd.to_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to SCHEMA using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_str(x)` {#kd.schema.to_str}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to STRING using explicit (permissive) casting rules.</code></pre>

### `kd.schema.uu_schema(seed='', **kwargs)` {#kd.schema.uu_schema}
Aliases:

- [kd.uu_schema](../kd.md#kd.uu_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a UUSchema, i.e. a schema keyed by a uuid.

In order to create a different id from the same arguments, use
`seed` argument with the desired value, e.g.

kd.uu_schema(seed=&#39;type_1&#39;, x=kd.INT32, y=kd.FLOAT32)

and

kd.uu_schema(seed=&#39;type_2&#39;, x=kd.INT32, y=kd.FLOAT32)

have different ids.

Args:
  seed: string seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be schemas themselves.

Returns:
  (DataSlice) containing the schema uuid.</code></pre>

### `kd.schema.with_schema(x, schema)` {#kd.schema.with_schema}
Aliases:

- [kd.with_schema](../kd.md#kd.with_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of `x` with the provided `schema`.

If `schema` is an Entity schema, it must have no DataBag or the same DataBag
as `x`. To set schema with a different DataBag, use `kd.set_schema` instead.

It only changes the schemas of `x` and does not change the items in `x`. To
change the items in `x`, use `kd.cast_to` instead. For example,

  kd.with_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -&gt; fails because the items in
      `x` are not compatible with FLOAT32.
  kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -&gt; kd.ds([1.0, 2.0, 3.0])

When items in `x` are primitives or `schemas` is a primitive schema, it checks
items and schema are compatible. When items are ItemIds and `schema` is a
non-primitive schema, it does not check the underlying data matches the
schema. For example,

  kd.with_schema(kd.ds([1, 2, 3], schema=kd.OBJECT), kd.INT32) -&gt;
      kd.ds([1, 2, 3])
  kd.with_schema(kd.ds([1, 2, 3]), kd.INT64) -&gt; fail

  db = kd.bag()
  kd.with_schema(kd.ds(1).with_bag(db), db.new_schema(x=kd.INT32)) -&gt; fail due
      to incompatible schema
  kd.with_schema(db.new(x=1), kd.INT32) -&gt; fail due to incompatible schema
  kd.with_schema(db.new(x=1), kd.schema.new_schema(x=kd.INT32)) -&gt; fail due to
      different DataBag
  kd.with_schema(db.new(x=1), kd.schema.new_schema(x=kd.INT32).no_bag()) -&gt;
  work
  kd.with_schema(db.new(x=1), db.new_schema(x=kd.INT64)) -&gt; work

Args:
  x: DataSlice to change the schema of.
  schema: DataSlice containing the new schema.

Returns:
  DataSlice with the new schema.</code></pre>

### `kd.schema.with_schema_from_obj(x)` {#kd.schema.with_schema_from_obj}
Aliases:

- [kd.with_schema_from_obj](../kd.md#kd.with_schema_from_obj)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.</code></pre>

