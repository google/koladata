<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.objs API

Operators that work solely with objects.





### `kd.objs.like(shape_and_mask_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.objs.like}
Aliases:

- [kd.obj_like](../kd.md#kd.obj_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema and is immutable.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.objs.new(arg: Any = unspecified, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.objs.new}
Aliases:

- [kd.obj](../kd.md#kd.obj)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema and is immutable.

Args:
  arg: optional Python object to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    itemid will only be set when the args is not a primitive or primitive
    slice if args presents.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.</code></pre>

### `kd.objs.shaped(shape: JaggedShape, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.objs.shaped}
Aliases:

- [kd.obj_shaped](../kd.md#kd.obj_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with the given shape.

Returned DataSlice has OBJECT schema and is immutable.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.objs.shaped_as(shape_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.objs.shaped_as}
Aliases:

- [kd.obj_shaped_as](../kd.md#kd.obj_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with the shape of the given DataSlice.

Returned DataSlice has OBJECT schema and is immutable.

Args:
  shape_from: DataSlice, whose shape the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `kd.objs.uu(seed: str | None = None, **attrs: Any) -> DataSlice` {#kd.objs.uu}
Aliases:

- [kd.uuobj](../kd.md#kd.uuobj)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates object(s) whose ids are uuid(s) with the provided attributes.

Returned DataSlice has OBJECT schema and is immutable.

In order to create a different &#34;Type&#34; from the same arguments, use
`seed` key with the desired value, e.g.

kd.uuobj(seed=&#39;type_1&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uuobj(seed=&#39;type_2&#39;, x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

Args:
  seed: (str) Allows different uuobj(s) to have different ids when created
    from the same inputs.
  **attrs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new / kd.obj.

Returns:
  data_slice.DataSlice</code></pre>

