<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.entities API

Operators that work solely with entities.





### `kd.entities.like(shape_and_mask_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.like}
Aliases:

- [kd.new_like](../kd.md#kd.new_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Entities with the shape and sparsity from shape_and_mask_from.

Returns immutable Entities.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    You can also pass schema=&#39;name&#39; as a shortcut for
    schema=kd.named_schema(&#39;name&#39;).
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.entities.new(*, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.new}
Aliases:

- [kd.new](../kd.md#kd.new)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Entities with given attrs.

Returns an immutable Entity.

Args:
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    You can also pass schema=&#39;name&#39; as a shortcut for
    schema=kd.named_schema(&#39;name&#39;).
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive
    slice if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.entities.shaped(shape: JaggedShape, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.shaped}
Aliases:

- [kd.new_shaped](../kd.md#kd.new_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Entities with the given shape.

Returns immutable Entities.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    You can also pass schema=&#39;name&#39; as a shortcut for
    schema=kd.named_schema(&#39;name&#39;).
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.entities.shaped_as(shape_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.shaped_as}
Aliases:

- [kd.new_shaped_as](../kd.md#kd.new_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda entities with shape of the given DataSlice.

Returns immutable Entities.

Args:
  shape_from: DataSlice, whose shape the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    You can also pass schema=&#39;name&#39; as a shortcut for
    schema=kd.named_schema(&#39;name&#39;).
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.entities.strict_new(*, schema, overwrite_schema=False, itemid=unspecified, **attrs)` {#kd.entities.strict_new}
Aliases:

- [kd.strict_new](../kd.md#kd.strict_new)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Entities, checking that all provided attrs are in the schema.

Args:
  schema: DataSlice schema.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive
    DataSlice if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.entities.uu(seed: str | None = None, *, schema: DataSlice | None = None, overwrite_schema: bool = False, **attrs: Any) -> DataSlice` {#kd.entities.uu}
Aliases:

- [kd.uu](../kd.md#kd.uu)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates UuEntities with given attrs.

Returns an immutable UU Entity.

Args:
  seed: string to seed the uuid computation with.
  schema: optional DataSlice schema. If not specified, a UuSchema
    will be automatically created based on the schemas of the passed **attrs.
  overwrite_schema: if schema attribute is missing and the attribute is being
    set through `attrs`, schema is successfully updated.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

