<!-- Note: This file is auto-generated, do not edit manually. -->

# kd API

`kd` module is the common container for all operators whereas `kd.eager` and
`kd.lazy` modules are containers for explicitly eager and lazy operators
respectively.

While most of operators below have both eager and lazy versions (e.g.
`kd.eager.agg_sum` vs `kd.lazy.agg_sum`), some functions
(e.g. `kd.sub(expr, *subs)`) only have eager version. Such functions often take
Exprs or Functors as inputs and does not make sense to have a lazy version.

Note that operators from extension modules (e.g. `kd_ext.npkd`) are not
included in the kd.* namespace.


Subcategory | Description
----------- | ------------
[allocation](kd/allocation.md) | Operators that allocate new ItemIds.
[annotation](kd/annotation.md) | Annotation operators.
[assertion](kd/assertion.md) | Operators that assert properties of DataSlices.
[bags](kd/bags.md) | Operators that work on DataBags.
[bitwise](kd/bitwise.md) | Bitwise operators
[comparison](kd/comparison.md) | Operators that compare DataSlices.
[core](kd/core.md) | Core operators that are not part of other categories.
[curves](kd/curves.md) | Operators working with curves.
[dicts](kd/dicts.md) | Operators working with dictionaries.
[entities](kd/entities.md) | Operators that work solely with entities.
[expr](kd/expr.md) | Expr utilities.
[extension_types](kd/extension_types.md) | Extension type functionality.
[file_io](kd/file_io.md) | File I/O utilities.
[functor](kd/functor.md) | Operators to create and call functors.
[ids](kd/ids.md) | Operators that work with ItemIds.
[iterables](kd/iterables.md) | Operators that work with iterables.
[json](kd/json.md) | JSON serialization operators.
[json_stream](kd/json_stream.md) | JSON text stream transformation operators.
[lists](kd/lists.md) | Operators working with lists.
[masking](kd/masking.md) | Masking operators.
[math](kd/math.md) | Arithmetic operators.
[objs](kd/objs.md) | Operators that work solely with objects.
[optools](kd/optools.md) | Operator definition and registration tooling.
[parallel](kd/parallel.md) | Operators for parallel computation.
[proto](kd/proto.md) | Protocol buffer serialization operators.
[py](kd/py.md) | Operators that call Python functions.
[qtypes](kd/qtypes.md) | Constants for Koda QTypes.
[random](kd/random.md) | Random and sampling operators.
[s11n](kd/s11n.md) | Serialization and deserialization utilities.
[schema](kd/schema.md) | Schema-related operators.
[schema_filters](kd/schema_filters.md) | Schema filter operators and constants.
[shapes](kd/shapes.md) | Operators that work on shapes
[slices](kd/slices.md) | Operators that perform DataSlice transformations.
[streams](kd/streams.md) | Operators that work with streams of items.
[strings](kd/strings.md) | Operators that work with strings data.
[testing](kd/testing.md) | A front-end module for kd.testing.*.
[tuples](kd/tuples.md) | Operators to create tuples.
[type_checking](kd/type_checking.md) | Utilities to annotatate functions with type checking.
[types](kd/types.md) | Types used as type annotations in users&#39;s code.




### `kd.BOOLEAN` {#kd.BOOLEAN}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing booleans.</code></pre>

### `kd.BYTES` {#kd.BYTES}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing byte strings.</code></pre>

### `kd.EXPR` {#kd.EXPR}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing expressions.</code></pre>

### `kd.FLOAT32` {#kd.FLOAT32}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing 32-bit floats.</code></pre>

### `kd.FLOAT64` {#kd.FLOAT64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing 64-bit floats.</code></pre>

### `kd.INT32` {#kd.INT32}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing 32-bit integers.</code></pre>

### `kd.INT64` {#kd.INT64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing 64-bit integers.</code></pre>

### `kd.ITEMID` {#kd.ITEMID}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing ItemIds.</code></pre>

### `kd.MASK` {#kd.MASK}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing masks.</code></pre>

### `kd.NONE` {#kd.NONE}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing the None schema.</code></pre>

### `kd.OBJECT` {#kd.OBJECT}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing generic objects.</code></pre>

### `kd.SCHEMA` {#kd.SCHEMA}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing schemas.</code></pre>

### `kd.STRING` {#kd.STRING}

<pre class="no-copy"><code class="lang-text no-auto-prettify">SchemaItem representing Unicode strings.</code></pre>

### `kd.SWITCH_DEFAULT` {#kd.SWITCH_DEFAULT}
*No description*

### `kd.agg_all(x, ndim=unspecified)` {#kd.agg_all}

Alias for [kd.masking.agg_all](kd/masking.md#kd.masking.agg_all)

### `kd.agg_any(x, ndim=unspecified)` {#kd.agg_any}

Alias for [kd.masking.agg_any](kd/masking.md#kd.masking.agg_any)

### `kd.agg_count(x, ndim=unspecified)` {#kd.agg_count}

Alias for [kd.slices.agg_count](kd/slices.md#kd.slices.agg_count)

### `kd.agg_has(x, ndim=unspecified)` {#kd.agg_has}

Alias for [kd.masking.agg_has](kd/masking.md#kd.masking.agg_has)

### `kd.agg_max(x, ndim=unspecified)` {#kd.agg_max}

Alias for [kd.math.agg_max](kd/math.md#kd.math.agg_max)

### `kd.agg_min(x, ndim=unspecified)` {#kd.agg_min}

Alias for [kd.math.agg_min](kd/math.md#kd.math.agg_min)

### `kd.agg_size(x, ndim=unspecified)` {#kd.agg_size}

Alias for [kd.slices.agg_size](kd/slices.md#kd.slices.agg_size)

### `kd.agg_sum(x, ndim=unspecified)` {#kd.agg_sum}

Alias for [kd.math.agg_sum](kd/math.md#kd.math.agg_sum)

### `kd.agg_uuid(x, ndim=unspecified)` {#kd.agg_uuid}

Alias for [kd.ids.agg_uuid](kd/ids.md#kd.ids.agg_uuid)

### `kd.align(*args)` {#kd.align}

Alias for [kd.slices.align](kd/slices.md#kd.slices.align)

### `kd.all(x)` {#kd.all}

Alias for [kd.masking.all](kd/masking.md#kd.masking.all)

### `kd.any(x)` {#kd.any}

Alias for [kd.masking.any](kd/masking.md#kd.masking.any)

### `kd.appended_list(x, append)` {#kd.appended_list}

Alias for [kd.lists.appended_list](kd/lists.md#kd.lists.appended_list)

### `kd.apply_mask(x, y)` {#kd.apply_mask}

Alias for [kd.masking.apply_mask](kd/masking.md#kd.masking.apply_mask)

### `kd.apply_py(fn, *args, return_type_as=unspecified, **kwargs)` {#kd.apply_py}

Alias for [kd.py.apply_py](kd/py.md#kd.py.apply_py)

### `kd.argmax(x, ndim=unspecified)` {#kd.argmax}

Alias for [kd.math.argmax](kd/math.md#kd.math.argmax)

### `kd.argmin(x, ndim=unspecified)` {#kd.argmin}

Alias for [kd.math.argmin](kd/math.md#kd.math.argmin)

### `kd.at(x, indices)` {#kd.at}

Alias for [kd.slices.at](kd/slices.md#kd.slices.at)

### `kd.attr(x, attr_name, value, overwrite_schema=False)` {#kd.attr}

Alias for [kd.core.attr](kd/core.md#kd.core.attr)

### `kd.attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.attrs}

Alias for [kd.core.attrs](kd/core.md#kd.core.attrs)

### `kd.bag()` {#kd.bag}

Alias for [kd.types.DataBag.empty](kd/types/data_bag.md#kd.types.DataBag.empty)

### `kd.bind(fn_def: DataItem, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataItem` {#kd.bind}

Alias for [kd.functor.bind](kd/functor.md#kd.functor.bind)

### `kd.bitwise_and(x, y)` {#kd.bitwise_and}

Alias for [kd.bitwise.bitwise_and](kd/bitwise.md#kd.bitwise.bitwise_and)

### `kd.bitwise_count(x)` {#kd.bitwise_count}

Alias for [kd.bitwise.count](kd/bitwise.md#kd.bitwise.count)

### `kd.bitwise_invert(x)` {#kd.bitwise_invert}

Alias for [kd.bitwise.invert](kd/bitwise.md#kd.bitwise.invert)

### `kd.bitwise_or(x, y)` {#kd.bitwise_or}

Alias for [kd.bitwise.bitwise_or](kd/bitwise.md#kd.bitwise.bitwise_or)

### `kd.bitwise_xor(x, y)` {#kd.bitwise_xor}

Alias for [kd.bitwise.bitwise_xor](kd/bitwise.md#kd.bitwise.bitwise_xor)

### `kd.bool(x: Any) -> DataSlice` {#kd.bool}

Alias for [kd.slices.bool](kd/slices.md#kd.slices.bool)

### `kd.bytes(x: Any) -> DataSlice` {#kd.bytes}

Alias for [kd.slices.bytes](kd/slices.md#kd.slices.bytes)

### `kd.call(fn, *args, return_type_as=None, **kwargs)` {#kd.call}

Alias for [kd.functor.call](kd/functor.md#kd.functor.call)

### `kd.cast_to(x, schema)` {#kd.cast_to}

Alias for [kd.schema.cast_to](kd/schema.md#kd.schema.cast_to)

### `kd.check_inputs(**kw_constraints: TypeConstraint)` {#kd.check_inputs}

Alias for [kd.type_checking.check_inputs](kd/type_checking.md#kd.type_checking.check_inputs)

### `kd.check_output(constraint: TypeConstraint)` {#kd.check_output}

Alias for [kd.type_checking.check_output](kd/type_checking.md#kd.type_checking.check_output)

### `kd.cityhash(x, seed)` {#kd.cityhash}

Alias for [kd.random.cityhash](kd/random.md#kd.random.cityhash)

### `kd.clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.clone}

Alias for [kd.core.clone](kd/core.md#kd.core.clone)

### `kd.clone_as_full(x, /, *, itemid=unspecified, **overrides)` {#kd.clone_as_full}

Alias for [kd.core.clone_as_full](kd/core.md#kd.core.clone_as_full)

### `kd.coalesce(x, y)` {#kd.coalesce}

Alias for [kd.masking.coalesce](kd/masking.md#kd.masking.coalesce)

### `kd.collapse(x, ndim=unspecified)` {#kd.collapse}

Alias for [kd.slices.collapse](kd/slices.md#kd.slices.collapse)

### `kd.concat(*args, ndim=1)` {#kd.concat}

Alias for [kd.slices.concat](kd/slices.md#kd.slices.concat)

### `kd.concat_lists(*lists: DataSlice) -> DataSlice` {#kd.concat_lists}

Alias for [kd.lists.concat](kd/lists.md#kd.lists.concat)

### `kd.cond(condition, yes, no=None)` {#kd.cond}

Alias for [kd.masking.cond](kd/masking.md#kd.masking.cond)

### `kd.count(x)` {#kd.count}

Alias for [kd.slices.count](kd/slices.md#kd.slices.count)

### `kd.cum_count(x, ndim=unspecified)` {#kd.cum_count}

Alias for [kd.slices.cum_count](kd/slices.md#kd.slices.cum_count)

### `kd.cum_max(x, ndim=unspecified)` {#kd.cum_max}

Alias for [kd.math.cum_max](kd/math.md#kd.math.cum_max)

### `kd.decode_itemid(ds)` {#kd.decode_itemid}

Alias for [kd.ids.decode_itemid](kd/ids.md#kd.ids.decode_itemid)

### `kd.deep_clone(x, /, schema=unspecified, **overrides)` {#kd.deep_clone}

Alias for [kd.core.deep_clone](kd/core.md#kd.core.deep_clone)

### `kd.deep_uuid(x, /, schema=unspecified, *, seed='')` {#kd.deep_uuid}

Alias for [kd.ids.deep_uuid](kd/ids.md#kd.ids.deep_uuid)

### `kd.del_attr(x: DataSlice, attr_name: str)` {#kd.del_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Deletes an attribute `attr_name` from `x`.</code></pre>

### `kd.dense_rank(x, descending=False, ndim=unspecified)` {#kd.dense_rank}

Alias for [kd.slices.dense_rank](kd/slices.md#kd.slices.dense_rank)

### `kd.dict(items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict}

Alias for [kd.dicts.new](kd/dicts.md#kd.dicts.new)

### `kd.dict_like(shape_and_mask_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_like}

Alias for [kd.dicts.like](kd/dicts.md#kd.dicts.like)

### `kd.dict_schema(key_schema, value_schema)` {#kd.dict_schema}

Alias for [kd.schema.dict_schema](kd/schema.md#kd.schema.dict_schema)

### `kd.dict_shaped(shape: JaggedShape, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_shaped}

Alias for [kd.dicts.shaped](kd/dicts.md#kd.dicts.shaped)

### `kd.dict_shaped_as(shape_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_shaped_as}

Alias for [kd.dicts.shaped_as](kd/dicts.md#kd.dicts.shaped_as)

### `kd.dict_size(dict_slice)` {#kd.dict_size}

Alias for [kd.dicts.size](kd/dicts.md#kd.dicts.size)

### `kd.dict_update(x, keys, values=unspecified)` {#kd.dict_update}

Alias for [kd.dicts.dict_update](kd/dicts.md#kd.dicts.dict_update)

### `kd.dir(x: DataSlice, *, intersection: bool | None = None) -> list[str]` {#kd.dir}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a sorted list of unique attribute names of the given DataSlice.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  x: A DataSlice.
  intersection: If True, the intersection of all object attributes is
    returned. If False, the union is returned. If not specified, raises an
    error if objects have different attributes.

Returns:
  A list of unique attributes sorted by alphabetical order.</code></pre>

### `kd.disjoint_coalesce(x, y)` {#kd.disjoint_coalesce}

Alias for [kd.masking.disjoint_coalesce](kd/masking.md#kd.masking.disjoint_coalesce)

### `kd.duck_dict(key_constraint: TypeConstraint, value_constraint: TypeConstraint)` {#kd.duck_dict}

Alias for [kd.type_checking.duck_dict](kd/type_checking.md#kd.type_checking.duck_dict)

### `kd.duck_list(item_constraint: TypeConstraint)` {#kd.duck_list}

Alias for [kd.type_checking.duck_list](kd/type_checking.md#kd.type_checking.duck_list)

### `kd.duck_type(**kwargs: TypeConstraint)` {#kd.duck_type}

Alias for [kd.type_checking.duck_type](kd/type_checking.md#kd.type_checking.duck_type)

### `kd.dump(x: DataSlice | DataBag, path: str, /, *, overwrite: bool = False, riegeli_options: str | None = None, fs: Any | None = None) -> None` {#kd.dump}

Alias for [kd.s11n.dump](kd/s11n.md#kd.s11n.dump)

### `kd.dumps(x: DataSlice | DataBag, /, *, riegeli_options: str | None = None) -> bytes` {#kd.dumps}

Alias for [kd.s11n.dumps](kd/s11n.md#kd.s11n.dumps)

### `kd.embed_schema(x: DataSlice) -> DataSlice` {#kd.embed_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as &#39;__schema__&#39; attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.

Args:
  x: (DataSlice) whose schema is embedded.</code></pre>

### `kd.empty_shaped(shape, schema=MASK)` {#kd.empty_shaped}

Alias for [kd.slices.empty_shaped](kd/slices.md#kd.slices.empty_shaped)

### `kd.empty_shaped_as(shape_from, schema=MASK)` {#kd.empty_shaped_as}

Alias for [kd.slices.empty_shaped_as](kd/slices.md#kd.slices.empty_shaped_as)

### `kd.encode_itemid(ds)` {#kd.encode_itemid}

Alias for [kd.ids.encode_itemid](kd/ids.md#kd.ids.encode_itemid)

### `kd.enriched(ds, *bag)` {#kd.enriched}

Alias for [kd.core.enriched](kd/core.md#kd.core.enriched)

### `kd.enriched_bag(*bags)` {#kd.enriched_bag}

Alias for [kd.bags.enriched](kd/bags.md#kd.bags.enriched)

### `kd.equal(x, y)` {#kd.equal}

Alias for [kd.comparison.equal](kd/comparison.md#kd.comparison.equal)

### `kd.eval(expr: Any, self_input: Any = UNSPECIFIED_SELF_INPUT, /, **input_values: Any) -> Any` {#kd.eval}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the expr evaluated on the given `input_values`.

Only Koda Inputs from container `I` (e.g. `I.x`) can be evaluated. Other
input types must be substituted before calling this function.

Args:
  expr: Koda expression with inputs from container `I`.
  self_input: The value for I.self input. When not provided, it will still
    have a default value that can be passed to a subroutine.
  **input_values: Values to evaluate `expr` with. Note that all inputs in
    `expr` must be present in the input values. All input values should either
    be DataSlices or convertible to DataSlices.</code></pre>

### `kd.expand_to(x, target, ndim=unspecified)` {#kd.expand_to}

Alias for [kd.slices.expand_to](kd/slices.md#kd.slices.expand_to)

### `kd.expand_to_shape(x, shape, ndim=unspecified)` {#kd.expand_to_shape}

Alias for [kd.shapes.expand_to_shape](kd/shapes.md#kd.shapes.expand_to_shape)

### `kd.experimental_safer_loads(x: bytes) -> Any` {#kd.experimental_safer_loads}

Alias for [kd.s11n.experimental_safer_loads](kd/s11n.md#kd.s11n.experimental_safer_loads)

### `kd.explode(x, ndim=1)` {#kd.explode}

Alias for [kd.lists.explode](kd/lists.md#kd.lists.explode)

### `kd.expr_quote(x: Any) -> DataSlice` {#kd.expr_quote}

Alias for [kd.slices.expr_quote](kd/slices.md#kd.slices.expr_quote)

### `kd.extension_type(unsafe_override=False) -> Callable[[type[Any]], type[Any]]` {#kd.extension_type}

Alias for [kd.extension_types.extension_type](kd/extension_types.md#kd.extension_types.extension_type)

### `kd.extract(ds, schema=unspecified)` {#kd.extract}

Alias for [kd.core.extract](kd/core.md#kd.core.extract)

### `kd.extract_update(ds, schema=unspecified)` {#kd.extract_update}

Alias for [kd.core.extract_update](kd/core.md#kd.core.extract_update)

### `kd.flat_map_chain(iterable, fn, value_type_as=None)` {#kd.flat_map_chain}

Alias for [kd.functor.flat_map_chain](kd/functor.md#kd.functor.flat_map_chain)

### `kd.flat_map_interleaved(iterable, fn, value_type_as=None)` {#kd.flat_map_interleaved}

Alias for [kd.functor.flat_map_interleaved](kd/functor.md#kd.functor.flat_map_interleaved)

### `kd.flatten(x, from_dim=0, to_dim=unspecified)` {#kd.flatten}

Alias for [kd.shapes.flatten](kd/shapes.md#kd.shapes.flatten)

### `kd.flatten_end(x, n_times=1)` {#kd.flatten_end}

Alias for [kd.shapes.flatten_end](kd/shapes.md#kd.shapes.flatten_end)

### `kd.float32(x: Any) -> DataSlice` {#kd.float32}

Alias for [kd.slices.float32](kd/slices.md#kd.slices.float32)

### `kd.float64(x: Any) -> DataSlice` {#kd.float64}

Alias for [kd.slices.float64](kd/slices.md#kd.slices.float64)

### `kd.fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataItem` {#kd.fn}

Alias for [kd.functor.fn](kd/functor.md#kd.functor.fn)

### `kd.follow(x)` {#kd.follow}

Alias for [kd.core.follow](kd/core.md#kd.core.follow)

### `kd.for_(iterable, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.for_}

Alias for [kd.functor.for_](kd/functor.md#kd.functor.for_)

### `kd.format(fmt, /, **kwargs)` {#kd.format}

Alias for [kd.strings.format](kd/strings.md#kd.strings.format)

### `kd.freeze(x)` {#kd.freeze}

Alias for [kd.core.freeze](kd/core.md#kd.core.freeze)

### `kd.freeze_bag(x)` {#kd.freeze_bag}

Alias for [kd.core.freeze_bag](kd/core.md#kd.core.freeze_bag)

### `kd.from_json(x, /, schema=OBJECT, default_number_schema=OBJECT, *, on_invalid=[], keys_attr='json_object_keys', values_attr='json_object_values')` {#kd.from_json}

Alias for [kd.json.from_json](kd/json.md#kd.json.from_json)

### `kd.from_proto(messages: Message | list[_NestedMessageContainer] | tuple[_NestedMessageContainer, ...] | None, /, *, extensions: list[str] | None = None, itemid: DataSlice | None = None, schema: DataSlice | None = None) -> DataSlice` {#kd.from_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice representing proto data.

Messages, primitive fields, repeated fields, and maps are converted to
equivalent Koda structures: objects/entities, primitives, lists, and dicts,
respectively. Enums are converted to INT32. The attribute names on the Koda
objects match the field names in the proto definition. See below for methods
to convert proto extensions to attributes alongside regular fields.

If schema is not specified or schema is kd.OBJECT, only present fields in
`messages` are loaded and included in the converted schema. To get a schema
that contains all fields independent of the data, use `kd.schema_from_proto`.

Proto extensions are ignored by default unless `extensions` is specified, or
if an explicit entity schema with parenthesized attr names is specified. If
both are specified, we use the union of the two extension sets.

The format of each extension specified in `extensions` is a dot-separated
sequence of field names and/or extension names, where extension names are
fully-qualified extension paths surrounded by parentheses. This sequence of
fields and extensions is traversed during conversion, in addition to the
default behavior of traversing all fields. For example:

  &#34;path.to.field.(package_name.some_extension)&#34;
  &#34;path.to.repeated_field.(package_name.some_extension)&#34;
  &#34;path.to.map_field.values.(package_name.some_extension)&#34;
  &#34;path.(package_name.some_extension).(package_name2.nested_extension)&#34;

If an explicit entity schema attr name starts with &#34;(&#34; and ends with &#34;)&#34; it is
also interpreted as an extension name.

Extensions are looked up using the C++ generated descriptor pool, using
`DescriptorPool::FindExtensionByName`, which requires that all extensions are
compiled in as C++ protos. The Koda attribute names for the extension fields
are parenthesized fully-qualified extension paths (e.g.
&#34;(package_name.some_extension)&#34; or
&#34;(package_name.SomeMessage.some_extension)&#34;.) As the names contain &#39;()&#39; and
&#39;.&#39; characters, they cannot be directly accessed using &#39;.name&#39; syntax but can
be accessed using `.get_attr(name)&#39;. For example,

  ds.get_attr(&#39;(package_name.AbcExtension.abc_extension)&#39;)
  ds.optional_field.get_attr(&#39;(package_name.DefExtension.def_extension)&#39;)

If `messages` is a single proto Message, the result is a DataItem. If it is a
nested list of proto Messages, the result is a DataSlice with the same number
of dimensions as the nesting level.

Args:
  messages: Message or nested list/tuple of Message of the same type. Any of
    the messages may be None, which will produce missing items in the result.
  extensions: List of proto extension paths.
  itemid: The ItemId(s) to use for the root object(s). If not specified, will
    allocate new id(s). If specified, will also infer the ItemIds for all
    child items such as List items from this id, so that repeated calls to
    this method on the same input will produce the same id(s) for everything.
    Use this with care to avoid unexpected collisions.
  schema: The schema to use for the return value. Can be set to kd.OBJECT to
    (recursively) create an object schema. Can be set to None (default) to
    create an uuschema based on the proto descriptor. When set to an entity
    schema, some fields may be set to kd.OBJECT to create objects from that
    point.

Returns:
  A DataSlice representing the proto data.</code></pre>

### `kd.from_proto_any(messages: Any | list[_NestedAnyMessageContainer] | tuple[_NestedAnyMessageContainer, ...] | None, /, *, extensions: list[str] | None = None, itemid: DataSlice | None = None, schema: DataSlice | None = None, message_type: type[Message] | None = None, descriptor_pool: DescriptorPool | None = None) -> DataSlice` {#kd.from_proto_any}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice converted from a nested list of proto Any messages.

This function is similar to `from_proto`, but it first unpacks the Any
messages before converting them. Only the top-level Any message is unpacked:
if there are Any fields inside of the unpacked message, they are treated the
same as in `from_proto`.

If `message_type` is provided, all Any messages are unpacked into this type.
Otherwise, the type is inferred from the type URL in each Any message, and
the messages are looked up in the `descriptor_pool`. If `descriptor_pool` is
not provided, the default descriptor pool is used.

If `schema` is not explicitly provided, the resulting DataSlice will have an
OBJECT schema so that inputs with differing message types can be represented.

Args:
  messages: google.protobuf.Any message or nested list/tuple of
    google.protobuf.Any messages. Any of the messages may be None, which will
    produce missing items in the result.
  extensions: See `from_proto` for more details.
  itemid: See `from_proto` for more details.
  schema: See `from_proto` for more details.
  message_type: The type to unpack the Any messages into. If None, the type is
    inferred from the Any messages.
  descriptor_pool: The descriptor pool to use for looking up message types. If
    None, the default descriptor pool is used.

Returns:
  A DataSlice representing the unpacked and converted proto data.</code></pre>

### `kd.from_proto_bytes(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.from_proto_bytes}

Alias for [kd.proto.from_proto_bytes](kd/proto.md#kd.proto.from_proto_bytes)

### `kd.from_proto_json(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.from_proto_json}

Alias for [kd.proto.from_proto_json](kd/proto.md#kd.proto.from_proto_json)

### `kd.from_py(py_obj: Any, *, dict_as_obj: bool = False, itemid: DataSlice | None = None, schema: DataSlice | None = OBJECT, from_dim: int = 0) -> DataSlice` {#kd.from_py}
Aliases:

- [kd.from_pytree](#kd.from_pytree)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python object into DataSlice.

Can convert nested lists/dicts into Koda objects recursively as well.

Args:
  py_obj: Python object to convert.
  dict_as_obj: If True, will convert dicts with string keys into Koda objects
    instead of Koda dicts.
  itemid: The ItemId to use for the root object. If not specified, will
    allocate a new id. If specified, will also infer the ItemIds for all child
    items such as list items from this id, so that repeated calls to this
    method on the same input will produce the same id for everything. Use this
    with care to avoid unexpected collisions.
  schema: The schema to use for the return value. When this schema or one of
    its attributes is OBJECT (which is also the default), recursively creates
    objects from that point on.
  from_dim: The dimension to start creating Koda objects/lists/dicts from.
    `py_obj` must be a nested list of at least from_dim depth, and the outer
    from_dim dimensions will become the returned DataSlice dimensions. When
    from_dim is 0, the return value is therefore a DataItem.

Returns:
  A DataItem with the converted data.</code></pre>

### `kd.from_pytree(py_obj: Any, *, dict_as_obj: bool = False, itemid: DataSlice | None = None, schema: DataSlice | None = OBJECT, from_dim: int = 0) -> DataSlice` {#kd.from_pytree}

Alias for [kd.from_py](#kd.from_py)

### `kd.fstr(x)` {#kd.fstr}

Alias for [kd.strings.fstr](kd/strings.md#kd.strings.fstr)

### `kd.full_equal(x, y)` {#kd.full_equal}

Alias for [kd.comparison.full_equal](kd/comparison.md#kd.comparison.full_equal)

### `kd.get_attr(x, attr_name, default=unspecified)` {#kd.get_attr}

Alias for [kd.core.get_attr](kd/core.md#kd.core.get_attr)

### `kd.get_attr_names(x)` {#kd.get_attr_names}

Alias for [kd.core.get_attr_names](kd/core.md#kd.core.get_attr_names)

### `kd.get_bag(ds)` {#kd.get_bag}

Alias for [kd.core.get_bag](kd/core.md#kd.core.get_bag)

### `kd.get_dtype(ds)` {#kd.get_dtype}

Alias for [kd.schema.get_dtype](kd/schema.md#kd.schema.get_dtype)

### `kd.get_item(x, key_or_index)` {#kd.get_item}

Alias for [kd.core.get_item](kd/core.md#kd.core.get_item)

### `kd.get_item_schema(list_schema)` {#kd.get_item_schema}

Alias for [kd.schema.get_item_schema](kd/schema.md#kd.schema.get_item_schema)

### `kd.get_itemid(x)` {#kd.get_itemid}

Alias for [kd.schema.get_itemid](kd/schema.md#kd.schema.get_itemid)

### `kd.get_key_schema(dict_schema)` {#kd.get_key_schema}

Alias for [kd.schema.get_key_schema](kd/schema.md#kd.schema.get_key_schema)

### `kd.get_keys(dict_ds)` {#kd.get_keys}

Alias for [kd.dicts.get_keys](kd/dicts.md#kd.dicts.get_keys)

### `kd.get_metadata(x)` {#kd.get_metadata}

Alias for [kd.core.get_metadata](kd/core.md#kd.core.get_metadata)

### `kd.get_ndim(x)` {#kd.get_ndim}

Alias for [kd.slices.get_ndim](kd/slices.md#kd.slices.get_ndim)

### `kd.get_nofollowed_schema(schema)` {#kd.get_nofollowed_schema}

Alias for [kd.schema.get_nofollowed_schema](kd/schema.md#kd.schema.get_nofollowed_schema)

### `kd.get_obj_schema(x)` {#kd.get_obj_schema}

Alias for [kd.schema.get_obj_schema](kd/schema.md#kd.schema.get_obj_schema)

### `kd.get_primitive_schema(ds)` {#kd.get_primitive_schema}

Alias for [kd.schema.get_dtype](kd/schema.md#kd.schema.get_dtype)

### `kd.get_proto_attr(x, field_name)` {#kd.get_proto_attr}

Alias for [kd.proto.get_proto_attr](kd/proto.md#kd.proto.get_proto_attr)

### `kd.get_repr(x, /, *, depth=25, item_limit=200, item_limit_per_dimension=25, format_html=False, max_str_len=100, max_expr_quote_len=10000, show_attributes=True, show_databag_id=False, show_shape=False, show_schema=False, show_item_id=False, show_present_count=False)` {#kd.get_repr}

Alias for [kd.slices.get_repr](kd/slices.md#kd.slices.get_repr)

### `kd.get_schema(x)` {#kd.get_schema}

Alias for [kd.schema.get_schema](kd/schema.md#kd.schema.get_schema)

### `kd.get_shape(x)` {#kd.get_shape}

Alias for [kd.shapes.get_shape](kd/shapes.md#kd.shapes.get_shape)

### `kd.get_value_schema(dict_schema)` {#kd.get_value_schema}

Alias for [kd.schema.get_value_schema](kd/schema.md#kd.schema.get_value_schema)

### `kd.get_values(dict_ds, key_ds=unspecified)` {#kd.get_values}

Alias for [kd.dicts.get_values](kd/dicts.md#kd.dicts.get_values)

### `kd.greater(x, y)` {#kd.greater}

Alias for [kd.comparison.greater](kd/comparison.md#kd.comparison.greater)

### `kd.greater_equal(x, y)` {#kd.greater_equal}

Alias for [kd.comparison.greater_equal](kd/comparison.md#kd.comparison.greater_equal)

### `kd.group_by(x, *keys, sort=False)` {#kd.group_by}

Alias for [kd.slices.group_by](kd/slices.md#kd.slices.group_by)

### `kd.group_by_indices(*keys, sort=False)` {#kd.group_by_indices}

Alias for [kd.slices.group_by_indices](kd/slices.md#kd.slices.group_by_indices)

### `kd.has(x)` {#kd.has}

Alias for [kd.masking.has](kd/masking.md#kd.masking.has)

### `kd.has_attr(x, attr_name)` {#kd.has_attr}

Alias for [kd.core.has_attr](kd/core.md#kd.core.has_attr)

### `kd.has_bag(ds)` {#kd.has_bag}

Alias for [kd.core.has_bag](kd/core.md#kd.core.has_bag)

### `kd.has_dict(x)` {#kd.has_dict}

Alias for [kd.dicts.has_dict](kd/dicts.md#kd.dicts.has_dict)

### `kd.has_entity(x)` {#kd.has_entity}

Alias for [kd.core.has_entity](kd/core.md#kd.core.has_entity)

### `kd.has_fn(x)` {#kd.has_fn}

Alias for [kd.functor.has_fn](kd/functor.md#kd.functor.has_fn)

### `kd.has_list(x)` {#kd.has_list}

Alias for [kd.lists.has_list](kd/lists.md#kd.lists.has_list)

### `kd.has_not(x)` {#kd.has_not}

Alias for [kd.masking.has_not](kd/masking.md#kd.masking.has_not)

### `kd.has_primitive(x)` {#kd.has_primitive}

Alias for [kd.core.has_primitive](kd/core.md#kd.core.has_primitive)

### `kd.hash_itemid(x)` {#kd.hash_itemid}

Alias for [kd.ids.hash_itemid](kd/ids.md#kd.ids.hash_itemid)

### `kd.if_(cond, yes_fn, no_fn, *args, return_type_as=None, **kwargs)` {#kd.if_}

Alias for [kd.functor.if_](kd/functor.md#kd.functor.if_)

### `kd.implode(x: DataSlice, /, ndim: int | DataSlice = 1, itemid: DataSlice | None = None) -> DataSlice` {#kd.implode}

Alias for [kd.lists.implode](kd/lists.md#kd.lists.implode)

### `kd.index(x, dim=-1)` {#kd.index}

Alias for [kd.slices.index](kd/slices.md#kd.slices.index)

### `kd.int32(x: Any) -> DataSlice` {#kd.int32}

Alias for [kd.slices.int32](kd/slices.md#kd.slices.int32)

### `kd.int64(x: Any) -> DataSlice` {#kd.int64}

Alias for [kd.slices.int64](kd/slices.md#kd.slices.int64)

### `kd.inverse_mapping(x, ndim=unspecified)` {#kd.inverse_mapping}

Alias for [kd.slices.inverse_mapping](kd/slices.md#kd.slices.inverse_mapping)

### `kd.inverse_select(ds, fltr)` {#kd.inverse_select}

Alias for [kd.slices.inverse_select](kd/slices.md#kd.slices.inverse_select)

### `kd.is_dict(x)` {#kd.is_dict}

Alias for [kd.dicts.is_dict](kd/dicts.md#kd.dicts.is_dict)

### `kd.is_empty(x)` {#kd.is_empty}

Alias for [kd.slices.is_empty](kd/slices.md#kd.slices.is_empty)

### `kd.is_entity(x)` {#kd.is_entity}

Alias for [kd.core.is_entity](kd/core.md#kd.core.is_entity)

### `kd.is_expandable_to(x, target, ndim=unspecified)` {#kd.is_expandable_to}

Alias for [kd.slices.is_expandable_to](kd/slices.md#kd.slices.is_expandable_to)

### `kd.is_expr(obj: Any) -> DataSlice` {#kd.is_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is an Expr and kd.missing otherwise.</code></pre>

### `kd.is_fn(obj: Any) -> DataItem` {#kd.is_fn}

Alias for [kd.functor.is_fn](kd/functor.md#kd.functor.is_fn)

### `kd.is_item(obj: Any) -> DataSlice` {#kd.is_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is a scalar DataItem and kd.missing otherwise.</code></pre>

### `kd.is_list(x)` {#kd.is_list}

Alias for [kd.lists.is_list](kd/lists.md#kd.lists.is_list)

### `kd.is_nan(x)` {#kd.is_nan}

Alias for [kd.math.is_nan](kd/math.md#kd.math.is_nan)

### `kd.is_null_bag(bag)` {#kd.is_null_bag}

Alias for [kd.bags.is_null_bag](kd/bags.md#kd.bags.is_null_bag)

### `kd.is_primitive(x)` {#kd.is_primitive}

Alias for [kd.core.is_primitive](kd/core.md#kd.core.is_primitive)

### `kd.is_shape_compatible(x, y)` {#kd.is_shape_compatible}

Alias for [kd.slices.is_shape_compatible](kd/slices.md#kd.slices.is_shape_compatible)

### `kd.is_slice(obj: Any) -> DataSlice` {#kd.is_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is a DataSlice and kd.missing otherwise.</code></pre>

### `kd.isin(x, y)` {#kd.isin}

Alias for [kd.slices.isin](kd/slices.md#kd.slices.isin)

### `kd.item(x, /, schema=None)` {#kd.item}

Alias for [kd.types.DataItem.from_vals](kd/types/data_item.md#kd.types.DataItem.from_vals)

### `kd.less(x, y)` {#kd.less}

Alias for [kd.comparison.less](kd/comparison.md#kd.comparison.less)

### `kd.less_equal(x, y)` {#kd.less_equal}

Alias for [kd.comparison.less_equal](kd/comparison.md#kd.comparison.less_equal)

### `kd.list(items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates list(s) by collapsing `items` into an immutable list.

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
  The slice with list/lists.</code></pre>

### `kd.list_append_update(x, append)` {#kd.list_append_update}

Alias for [kd.lists.list_append_update](kd/lists.md#kd.lists.list_append_update)

### `kd.list_like(shape_and_mask_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_like}

Alias for [kd.lists.like](kd/lists.md#kd.lists.like)

### `kd.list_schema(item_schema)` {#kd.list_schema}

Alias for [kd.schema.list_schema](kd/schema.md#kd.schema.list_schema)

### `kd.list_shaped(shape: JaggedShape, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_shaped}

Alias for [kd.lists.shaped](kd/lists.md#kd.lists.shaped)

### `kd.list_shaped_as(shape_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_shaped_as}

Alias for [kd.lists.shaped_as](kd/lists.md#kd.lists.shaped_as)

### `kd.list_size(list_slice)` {#kd.list_size}

Alias for [kd.lists.size](kd/lists.md#kd.lists.size)

### `kd.load(path: str, /, *, fs: Any | None = None) -> Any` {#kd.load}

Alias for [kd.s11n.load](kd/s11n.md#kd.s11n.load)

### `kd.loads(x: bytes) -> Any` {#kd.loads}

Alias for [kd.s11n.loads](kd/s11n.md#kd.s11n.loads)

### `kd.map(fn, *args, include_missing=False, **kwargs)` {#kd.map}

Alias for [kd.functor.map](kd/functor.md#kd.functor.map)

### `kd.map_py(fn, *args, schema=None, max_threads=1, ndim=0, include_missing=None, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.map_py}

Alias for [kd.py.map_py](kd/py.md#kd.py.map_py)

### `kd.map_py_on_cond(true_fn, false_fn, cond, *args, schema=None, max_threads=1, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.map_py_on_cond}

Alias for [kd.py.map_py_on_cond](kd/py.md#kd.py.map_py_on_cond)

### `kd.map_py_on_selected(fn, cond, *args, schema=None, max_threads=1, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.map_py_on_selected}

Alias for [kd.py.map_py_on_selected](kd/py.md#kd.py.map_py_on_selected)

### `kd.mask(x: Any) -> DataSlice` {#kd.mask}

Alias for [kd.slices.mask](kd/slices.md#kd.slices.mask)

### `kd.mask_and(x, y)` {#kd.mask_and}

Alias for [kd.masking.mask_and](kd/masking.md#kd.masking.mask_and)

### `kd.mask_equal(x, y)` {#kd.mask_equal}

Alias for [kd.masking.mask_equal](kd/masking.md#kd.masking.mask_equal)

### `kd.mask_not_equal(x, y)` {#kd.mask_not_equal}

Alias for [kd.masking.mask_not_equal](kd/masking.md#kd.masking.mask_not_equal)

### `kd.mask_or(x, y)` {#kd.mask_or}

Alias for [kd.masking.mask_or](kd/masking.md#kd.masking.mask_or)

### `kd.max(x)` {#kd.max}

Alias for [kd.math.max](kd/math.md#kd.math.max)

### `kd.maximum(x, y)` {#kd.maximum}

Alias for [kd.math.maximum](kd/math.md#kd.math.maximum)

### `kd.maybe(x, attr_name)` {#kd.maybe}

Alias for [kd.core.maybe](kd/core.md#kd.core.maybe)

### `kd.metadata(x, /, **attrs)` {#kd.metadata}

Alias for [kd.core.metadata](kd/core.md#kd.core.metadata)

### `kd.min(x)` {#kd.min}

Alias for [kd.math.min](kd/math.md#kd.math.min)

### `kd.minimum(x, y)` {#kd.minimum}

Alias for [kd.math.minimum](kd/math.md#kd.math.minimum)

### `kd.missing` {#kd.missing}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A mask value representing absence.</code></pre>

### `kd.mutable_bag()` {#kd.mutable_bag}

Alias for [kd.types.DataBag.empty_mutable](kd/types/data_bag.md#kd.types.DataBag.empty_mutable)

### `kd.named_container()` {#kd.named_container}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A container that automatically names expressions.

In eager mode, non-expression inputs are stored as-is. In tracing mode,
they are converted to expressions (functions and lambdas are automatically
traced).

Example:
  c = kd.named_container()

  # 1. Non-tracing mode
  # Storing a value:
  c.foo = 5
  c.foo       # Returns 5

  # Storing an expression:
  c.x_plus_y = I.x + I.y
  c.x_plus_y  # Returns (I.x + I.y).with_name(&#39;x_plus_y&#39;)

  # Listing stored items:
  vars(c)  # Returns {&#39;foo&#39;: 5, &#39;x_plus_y&#39;: (I.x + I.y).with_name(&#39;x_plus_y&#39;)}

  # 2. Tracing mode
  def my_fn(x):
    c = kd.named_container()
    c.a = 2
    c.b = 1
    return c.a * x + c.b

  fn = kd.fn(my_fn)
  fn.a  # Returns 2 (accessible because it was named by the container)
  fn(x=5)  # Returns 11</code></pre>

### `kd.named_schema(name, /, **kwargs)` {#kd.named_schema}

Alias for [kd.schema.named_schema](kd/schema.md#kd.schema.named_schema)

### `kd.namedtuple(**kwargs)` {#kd.namedtuple}

Alias for [kd.tuples.namedtuple](kd/tuples.md#kd.tuples.namedtuple)

### `kd.new(*, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new}

Alias for [kd.entities.new](kd/entities.md#kd.entities.new)

### `kd.new_dictid()` {#kd.new_dictid}

Alias for [kd.allocation.new_dictid](kd/allocation.md#kd.allocation.new_dictid)

### `kd.new_dictid_like(shape_and_mask_from)` {#kd.new_dictid_like}

Alias for [kd.allocation.new_dictid_like](kd/allocation.md#kd.allocation.new_dictid_like)

### `kd.new_dictid_shaped(shape)` {#kd.new_dictid_shaped}

Alias for [kd.allocation.new_dictid_shaped](kd/allocation.md#kd.allocation.new_dictid_shaped)

### `kd.new_dictid_shaped_as(shape_from)` {#kd.new_dictid_shaped_as}

Alias for [kd.allocation.new_dictid_shaped_as](kd/allocation.md#kd.allocation.new_dictid_shaped_as)

### `kd.new_itemid()` {#kd.new_itemid}

Alias for [kd.allocation.new_itemid](kd/allocation.md#kd.allocation.new_itemid)

### `kd.new_itemid_like(shape_and_mask_from)` {#kd.new_itemid_like}

Alias for [kd.allocation.new_itemid_like](kd/allocation.md#kd.allocation.new_itemid_like)

### `kd.new_itemid_shaped(shape)` {#kd.new_itemid_shaped}

Alias for [kd.allocation.new_itemid_shaped](kd/allocation.md#kd.allocation.new_itemid_shaped)

### `kd.new_itemid_shaped_as(shape_from)` {#kd.new_itemid_shaped_as}

Alias for [kd.allocation.new_itemid_shaped_as](kd/allocation.md#kd.allocation.new_itemid_shaped_as)

### `kd.new_like(shape_and_mask_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_like}

Alias for [kd.entities.like](kd/entities.md#kd.entities.like)

### `kd.new_listid()` {#kd.new_listid}

Alias for [kd.allocation.new_listid](kd/allocation.md#kd.allocation.new_listid)

### `kd.new_listid_like(shape_and_mask_from)` {#kd.new_listid_like}

Alias for [kd.allocation.new_listid_like](kd/allocation.md#kd.allocation.new_listid_like)

### `kd.new_listid_shaped(shape)` {#kd.new_listid_shaped}

Alias for [kd.allocation.new_listid_shaped](kd/allocation.md#kd.allocation.new_listid_shaped)

### `kd.new_listid_shaped_as(shape_from)` {#kd.new_listid_shaped_as}

Alias for [kd.allocation.new_listid_shaped_as](kd/allocation.md#kd.allocation.new_listid_shaped_as)

### `kd.new_shaped(shape: JaggedShape, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_shaped}

Alias for [kd.entities.shaped](kd/entities.md#kd.entities.shaped)

### `kd.new_shaped_as(shape_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_shaped_as}

Alias for [kd.entities.shaped_as](kd/entities.md#kd.entities.shaped_as)

### `kd.no_bag(ds)` {#kd.no_bag}

Alias for [kd.core.no_bag](kd/core.md#kd.core.no_bag)

### `kd.nofollow(x)` {#kd.nofollow}

Alias for [kd.core.nofollow](kd/core.md#kd.core.nofollow)

### `kd.nofollow_schema(schema)` {#kd.nofollow_schema}

Alias for [kd.schema.nofollow_schema](kd/schema.md#kd.schema.nofollow_schema)

### `kd.not_equal(x, y)` {#kd.not_equal}

Alias for [kd.comparison.not_equal](kd/comparison.md#kd.comparison.not_equal)

### `kd.obj(arg: Any = unspecified, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj}

Alias for [kd.objs.new](kd/objs.md#kd.objs.new)

### `kd.obj_like(shape_and_mask_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_like}

Alias for [kd.objs.like](kd/objs.md#kd.objs.like)

### `kd.obj_shaped(shape: JaggedShape, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_shaped}

Alias for [kd.objs.shaped](kd/objs.md#kd.objs.shaped)

### `kd.obj_shaped_as(shape_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_shaped_as}

Alias for [kd.objs.shaped_as](kd/objs.md#kd.objs.shaped_as)

### `kd.ordinal_rank(x, tie_breaker=unspecified, descending=False, ndim=unspecified)` {#kd.ordinal_rank}

Alias for [kd.slices.ordinal_rank](kd/slices.md#kd.slices.ordinal_rank)

### `kd.present` {#kd.present}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A mask value representing presence.</code></pre>

### `kd.present_like(x)` {#kd.present_like}

Alias for [kd.masking.present_like](kd/masking.md#kd.masking.present_like)

### `kd.present_shaped(shape)` {#kd.present_shaped}

Alias for [kd.masking.present_shaped](kd/masking.md#kd.masking.present_shaped)

### `kd.present_shaped_as(x)` {#kd.present_shaped_as}

Alias for [kd.masking.present_shaped_as](kd/masking.md#kd.masking.present_shaped_as)

### `kd.pwl_curve(p, adjustments)` {#kd.pwl_curve}

Alias for [kd.curves.pwl_curve](kd/curves.md#kd.curves.pwl_curve)

### `kd.py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataItem` {#kd.py_fn}

Alias for [kd.functor.py_fn](kd/functor.md#kd.functor.py_fn)

### `kd.py_reference(obj: Any) -> PyObject` {#kd.py_reference}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wraps into a Arolla QValue using reference for serialization.

py_reference can be used to pass arbitrary python objects through
kd.apply_py/kd.py_fn.

Note that using reference for serialization means that the resulting
QValue (and Exprs created using it) will only be valid within the
same process. Trying to deserialize it in a different process
will result in an exception.

Args:
  obj: the python object to wrap.
Returns:
  The wrapped python object as Arolla QValue.</code></pre>

### `kd.randint_like(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_like}

Alias for [kd.random.randint_like](kd/random.md#kd.random.randint_like)

### `kd.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped}

Alias for [kd.random.randint_shaped](kd/random.md#kd.random.randint_shaped)

### `kd.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped_as}

Alias for [kd.random.randint_shaped_as](kd/random.md#kd.random.randint_shaped_as)

### `kd.range(start, end=unspecified)` {#kd.range}

Alias for [kd.slices.range](kd/slices.md#kd.slices.range)

### `kd.ref(ds)` {#kd.ref}

Alias for [kd.core.ref](kd/core.md#kd.core.ref)

### `kd.register_py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, unsafe_override: bool = False, **defaults: Any) -> DataItem` {#kd.register_py_fn}

Alias for [kd.functor.register_py_fn](kd/functor.md#kd.functor.register_py_fn)

### `kd.reify(ds, source)` {#kd.reify}

Alias for [kd.core.reify](kd/core.md#kd.core.reify)

### `kd.repeat(x, sizes)` {#kd.repeat}

Alias for [kd.slices.repeat](kd/slices.md#kd.slices.repeat)

### `kd.repeat_present(x, sizes)` {#kd.repeat_present}

Alias for [kd.slices.repeat_present](kd/slices.md#kd.slices.repeat_present)

### `kd.reshape(x, shape)` {#kd.reshape}

Alias for [kd.shapes.reshape](kd/shapes.md#kd.shapes.reshape)

### `kd.reshape_as(x, shape_from)` {#kd.reshape_as}

Alias for [kd.shapes.reshape_as](kd/shapes.md#kd.shapes.reshape_as)

### `kd.reverse(ds)` {#kd.reverse}

Alias for [kd.slices.reverse](kd/slices.md#kd.slices.reverse)

### `kd.reverse_select(ds, fltr)` {#kd.reverse_select}

Alias for [kd.slices.inverse_select](kd/slices.md#kd.slices.inverse_select)

### `kd.sample(x, ratio, seed, key=unspecified)` {#kd.sample}

Alias for [kd.random.sample](kd/random.md#kd.random.sample)

### `kd.sample_n(x, n, seed, key=unspecified)` {#kd.sample_n}

Alias for [kd.random.sample_n](kd/random.md#kd.random.sample_n)

### `kd.schema_from_proto(message_class: type[Message], /, *, extensions: list[str] | None = None) -> SchemaItem` {#kd.schema_from_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda schema representing a proto message class.

This is similar to `from_proto(x).get_schema()` when `x` is an instance of
`message_class`, except that it eagerly adds all non-extension fields to the
schema instead of only adding fields that have data populated in `x`.

The returned schema is a uuschema whose itemid is a function of the proto
message class&#39; fully qualified name, and any child message classes&#39; schemas
are also uuschemas derived in the same way. The returned schema has the same
itemid as `from_proto(message_class()).get_schema()`.

The format of each extension specified in `extensions` is a dot-separated
sequence of field names and/or extension names, where extension names are
fully-qualified extension paths surrounded by parentheses. For example:

  &#34;path.to.field.(package_name.some_extension)&#34;
  &#34;path.to.repeated_field.(package_name.some_extension)&#34;
  &#34;path.to.map_field.values.(package_name.some_extension)&#34;
  &#34;path.(package_name.some_extension).(package_name2.nested_extension)&#34;

Args:
  message_class: A proto message class to convert.
  extensions: List of proto extension paths.

Returns:
  A SchemaItem containing the converted schema.</code></pre>

### `kd.schema_from_proto_path(proto_path, /, *, extensions=Entity:#5ikYYvXepp19g47QDLnJR2)` {#kd.schema_from_proto_path}

Alias for [kd.proto.schema_from_proto_path](kd/proto.md#kd.proto.schema_from_proto_path)

### `kd.schema_from_py(tpe: type[Any]) -> SchemaItem` {#kd.schema_from_py}

Alias for [kd.schema.schema_from_py](kd/schema.md#kd.schema.schema_from_py)

### `kd.select(ds, fltr, expand_filter=True)` {#kd.select}

Alias for [kd.slices.select](kd/slices.md#kd.slices.select)

### `kd.select_items(ds, fltr)` {#kd.select_items}

Alias for [kd.lists.select_items](kd/lists.md#kd.lists.select_items)

### `kd.select_keys(ds, fltr)` {#kd.select_keys}

Alias for [kd.dicts.select_keys](kd/dicts.md#kd.dicts.select_keys)

### `kd.select_present(ds)` {#kd.select_present}

Alias for [kd.slices.select_present](kd/slices.md#kd.slices.select_present)

### `kd.select_values(ds, fltr)` {#kd.select_values}

Alias for [kd.dicts.select_values](kd/dicts.md#kd.dicts.select_values)

### `kd.set_attr(x: DataSlice, attr_name: str, value: Any, overwrite_schema: bool = False)` {#kd.set_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets an attribute `attr_name` to `value`.

If `overwrite_schema` is True and `x` is either an Entity with explicit schema
or an Object where some items are entities with explicit schema, it will get
updated with `value`&#39;s schema first.

Args:
  x: a DataSlice on which to set the attribute. Must have DataBag attached.
  attr_name: attribute name
  value: a DataSlice or convertible to a DataSlice that will be assigned as an
    attribute.
  overwrite_schema: whether to overwrite the schema before setting an
    attribute.</code></pre>

### `kd.set_attrs(x: DataSlice, *, overwrite_schema: bool = False, **attrs: Any)` {#kd.set_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets multiple attributes on an object / entity.

Args:
  x: a DataSlice on which attributes are set. Must have DataBag attached.
  overwrite_schema: whether to overwrite the schema before setting an
    attribute.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.</code></pre>

### `kd.set_schema(x: DataSlice, schema: DataSlice) -> DataSlice` {#kd.set_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of `x` with the provided `schema`.

If `schema` is an Entity schema and has a different DataBag than `x`, it is
merged into the DataBag of `x`.

It only changes the schemas of `x` and does not change the items in `x`. To
change the items in `x`, use `kd.cast_to` instead. For example,

  kd.set_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -&gt; fails because the items in
      `x` are not compatible with FLOAT32.
  kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -&gt; kd.ds([1.0, 2.0, 3.0])

When items in `x` are primitives or `schemas` is a primitive schema, it checks
items and schema are compatible. When items are ItemIds and `schema` is a
non-primitive schema, it does not check the underlying data matches the
schema. For example,

  kd.set_schema(kd.ds([1, 2, 3], schema=kd.OBJECT), kd.INT32)
    -&gt; kd.ds([1, 2, 3])
  kd.set_schema(kd.ds([1, 2, 3]), kd.INT64) -&gt; fail
  kd.set_schema(kd.ds(1).with_bag(kd.bag()), kd.schema.new_schema(x=kd.INT32))
  -&gt;
  fail
  kd.set_schema(kd.new(x=1), kd.INT32) -&gt; fail
  kd.set_schema(kd.new(x=1), kd.schema.new_schema(x=kd.INT64)) -&gt; work

Args:
  x: DataSlice to change the schema of.
  schema: DataSlice containing the new schema.

Returns:
  DataSlice with the new schema.</code></pre>

### `kd.shallow_clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.shallow_clone}

Alias for [kd.core.shallow_clone](kd/core.md#kd.core.shallow_clone)

### `kd.shuffle(x, /, ndim=unspecified, seed=unspecified)` {#kd.shuffle}

Alias for [kd.random.shuffle](kd/random.md#kd.random.shuffle)

### `kd.size(x)` {#kd.size}

Alias for [kd.slices.size](kd/slices.md#kd.slices.size)

### `kd.slice(x, /, schema=None)` {#kd.slice}

Alias for [kd.types.DataSlice.from_vals](kd/types/data_slice.md#kd.types.DataSlice.from_vals)

### `kd.sort(x, sort_by=unspecified, descending=False)` {#kd.sort}

Alias for [kd.slices.sort](kd/slices.md#kd.slices.sort)

### `kd.stack(*args, ndim=0)` {#kd.stack}

Alias for [kd.slices.stack](kd/slices.md#kd.slices.stack)

### `kd.static_when_tracing(base_type: TypeConstraint | None = None) -> _StaticWhenTraced` {#kd.static_when_tracing}

Alias for [kd.type_checking.static_when_tracing](kd/type_checking.md#kd.type_checking.static_when_tracing)

### `kd.str(x: Any) -> DataSlice` {#kd.str}

Alias for [kd.slices.str](kd/slices.md#kd.slices.str)

### `kd.strict_attrs(x, /, **attrs)` {#kd.strict_attrs}

Alias for [kd.core.strict_attrs](kd/core.md#kd.core.strict_attrs)

### `kd.strict_new(*, schema, overwrite_schema=False, itemid=unspecified, **attrs)` {#kd.strict_new}

Alias for [kd.entities.strict_new](kd/entities.md#kd.entities.strict_new)

### `kd.strict_with_attrs(x, /, **attrs)` {#kd.strict_with_attrs}

Alias for [kd.core.strict_with_attrs](kd/core.md#kd.core.strict_with_attrs)

### `kd.stub(x, attrs=[])` {#kd.stub}

Alias for [kd.core.stub](kd/core.md#kd.core.stub)

### `kd.subslice(x, *slices)` {#kd.subslice}

Alias for [kd.slices.subslice](kd/slices.md#kd.slices.subslice)

### `kd.sum(x)` {#kd.sum}

Alias for [kd.math.sum](kd/math.md#kd.math.sum)

### `kd.switch(key, cases, *args, return_type_as=None, **kwargs)` {#kd.switch}

Alias for [kd.functor.switch](kd/functor.md#kd.functor.switch)

### `kd.take(x, indices)` {#kd.take}

Alias for [kd.slices.at](kd/slices.md#kd.slices.at)

### `kd.tile(x, shape)` {#kd.tile}

Alias for [kd.slices.tile](kd/slices.md#kd.slices.tile)

### `kd.to_expr(x)` {#kd.to_expr}

Alias for [kd.schema.to_expr](kd/schema.md#kd.schema.to_expr)

### `kd.to_itemid(x)` {#kd.to_itemid}

Alias for [kd.schema.get_itemid](kd/schema.md#kd.schema.get_itemid)

### `kd.to_json(x, /, *, indent=None, ensure_ascii=True, keys_attr='json_object_keys', values_attr='json_object_values', include_missing_values=True)` {#kd.to_json}

Alias for [kd.json.to_json](kd/json.md#kd.json.to_json)

### `kd.to_none(x)` {#kd.to_none}

Alias for [kd.schema.to_none](kd/schema.md#kd.schema.to_none)

### `kd.to_object(x)` {#kd.to_object}

Alias for [kd.schema.to_object](kd/schema.md#kd.schema.to_object)

### `kd.to_proto(x: DataSlice, /, message_class: type[Message]) -> Message | list[_NestedMessageList] | None` {#kd.to_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a DataSlice or DataItem to one or more proto messages.

If `x` is a DataItem, this returns a single proto message object. Otherwise,
this returns a nested list of proto message objects with the same size and
shape as the input. Missing items in the input are returned as python None in
place of a message.

Koda data structures are converted to equivalent proto messages, primitive
fields, repeated fields, maps, and enums, based on the proto schema. Koda
entity attributes are converted to message fields with the same name, if
those fields exist, otherwise they are ignored.

Koda slices with mixed underlying dtypes are tolerated wherever the proto
conversion is defined for all dtypes, regardless of schema.

Koda entity attributes that are parenthesized fully-qualified extension
paths (e.g. &#34;(package_name.some_extension)&#34;) are converted to extensions,
if those extensions exist in the descriptor pool of the messages&#39; common
descriptor, otherwise they are ignored.

Args:
  x: DataSlice to convert.
  message_class: A proto message class.

Returns:
  A converted proto message or list of converted proto messages.</code></pre>

### `kd.to_proto_any(x: DataSlice, *, descriptor_pool: DescriptorPool | None = None, deterministic: bool = False) -> Any | list[_NestedAnyMessageList] | None` {#kd.to_proto_any}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a DataSlice or DataItem to proto Any messages.

The schemas of all present values in `x` must have been derived from a proto
schema using `from_proto` or `schema_from_proto`, so that the original names
of the message types are embedded in the schema. Otherwise, this will fail.

Args:
  x: DataSlice to convert.
  descriptor_pool: Overrides the descriptor pool used to look up python proto
    message classes based on proto message type full name. If None, the
    default descriptor pool is used.
  deterministic: Passed to Any.Pack.

Returns:
  A proto Any message or nested list of proto Any messages with the same
  shape as the input. Missing elements in the input are None in the output.</code></pre>

### `kd.to_proto_bytes(x, proto_path, /)` {#kd.to_proto_bytes}

Alias for [kd.proto.to_proto_bytes](kd/proto.md#kd.proto.to_proto_bytes)

### `kd.to_proto_json(x, proto_path, /)` {#kd.to_proto_json}

Alias for [kd.proto.to_proto_json](kd/proto.md#kd.proto.to_proto_json)

### `kd.to_py(ds: DataSlice, max_depth: int = 2, obj_as_dict: bool = False, include_missing_attrs: bool = True, output_class: type[Any] | None = None) -> Any` {#kd.to_py}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a readable python object from a DataSlice.

Attributes, lists, and dicts are recursively converted to Python objects.

Args:
  ds: A DataSlice
  max_depth: Maximum depth for recursive conversion. Each attribute, list item
    and dict keys / values access represent 1 depth increment. Use -1 for
    unlimited depth.
  obj_as_dict: Whether to convert objects to python dicts. By default objects
    are converted to automatically constructed &#39;Obj&#39; dataclass instances.
  include_missing_attrs: whether to include attributes with None value in
    objects.
  output_class: If not None, will be used recursively as the output type.</code></pre>

### `kd.to_pylist(x: DataSlice) -> list[Any]` {#kd.to_pylist}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands the outermost DataSlice dimension into a list of DataSlices.</code></pre>

### `kd.to_pytree(ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True) -> Any` {#kd.to_pytree}
*No description*

### `kd.to_schema(x)` {#kd.to_schema}

Alias for [kd.schema.to_schema](kd/schema.md#kd.schema.to_schema)

### `kd.trace_as_fn(*, name: str | None = None, return_type_as: Any = None, functor_factory: FunctorFactory | None = None)` {#kd.trace_as_fn}

Alias for [kd.functor.trace_as_fn](kd/functor.md#kd.functor.trace_as_fn)

### `kd.trace_py_fn(f: Callable[..., Any], *, auto_variables: bool = True, **defaults: Any) -> DataItem` {#kd.trace_py_fn}

Alias for [kd.functor.trace_py_fn](kd/functor.md#kd.functor.trace_py_fn)

### `kd.translate(keys_to, keys_from, values_from)` {#kd.translate}

Alias for [kd.slices.translate](kd/slices.md#kd.slices.translate)

### `kd.translate_group(keys_to, keys_from, values_from)` {#kd.translate_group}

Alias for [kd.slices.translate_group](kd/slices.md#kd.slices.translate_group)

### `kd.tuple(*args)` {#kd.tuple}

Alias for [kd.tuples.tuple](kd/tuples.md#kd.tuples.tuple)

### `kd.unique(x, sort=False)` {#kd.unique}

Alias for [kd.slices.unique](kd/slices.md#kd.slices.unique)

### `kd.update_schema(obj: DataSlice, **attr_schemas: Any) -> DataSlice` {#kd.update_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Updates the schema of `obj` DataSlice using given schemas for attrs.</code></pre>

### `kd.updated(ds, *bag)` {#kd.updated}

Alias for [kd.core.updated](kd/core.md#kd.core.updated)

### `kd.updated_bag(*bags)` {#kd.updated_bag}

Alias for [kd.bags.updated](kd/bags.md#kd.bags.updated)

### `kd.uu(seed: str | None = None, *, schema: DataSlice | None = None, overwrite_schema: bool = False, **attrs: Any) -> DataSlice` {#kd.uu}

Alias for [kd.entities.uu](kd/entities.md#kd.entities.uu)

### `kd.uu_schema(seed='', **kwargs)` {#kd.uu_schema}

Alias for [kd.schema.uu_schema](kd/schema.md#kd.schema.uu_schema)

### `kd.uudict(items_or_keys, values=unspecified, *, key_schema=unspecified, value_schema=unspecified, schema=unspecified, seed='')` {#kd.uudict}

Alias for [kd.dicts.uu](kd/dicts.md#kd.dicts.uu)

### `kd.uuid(seed='', **kwargs)` {#kd.uuid}

Alias for [kd.ids.uuid](kd/ids.md#kd.ids.uuid)

### `kd.uuid_for_dict(seed='', **kwargs)` {#kd.uuid_for_dict}

Alias for [kd.ids.uuid_for_dict](kd/ids.md#kd.ids.uuid_for_dict)

### `kd.uuid_for_list(seed='', **kwargs)` {#kd.uuid_for_list}

Alias for [kd.ids.uuid_for_list](kd/ids.md#kd.ids.uuid_for_list)

### `kd.uuids_with_allocation_size(seed='', *, size)` {#kd.uuids_with_allocation_size}

Alias for [kd.ids.uuids_with_allocation_size](kd/ids.md#kd.ids.uuids_with_allocation_size)

### `kd.uulist(items=unspecified, *, item_schema=unspecified, schema=unspecified, seed='')` {#kd.uulist}

Alias for [kd.lists.uu](kd/lists.md#kd.lists.uu)

### `kd.uuobj(seed: str | None = None, **attrs: Any) -> DataSlice` {#kd.uuobj}

Alias for [kd.objs.uu](kd/objs.md#kd.objs.uu)

### `kd.val_like(x, val)` {#kd.val_like}

Alias for [kd.slices.val_like](kd/slices.md#kd.slices.val_like)

### `kd.val_shaped(shape, val)` {#kd.val_shaped}

Alias for [kd.slices.val_shaped](kd/slices.md#kd.slices.val_shaped)

### `kd.val_shaped_as(x, val)` {#kd.val_shaped_as}

Alias for [kd.slices.val_shaped_as](kd/slices.md#kd.slices.val_shaped_as)

### `kd.while_(condition_fn, body_fn, *, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.while_}

Alias for [kd.functor.while_](kd/functor.md#kd.functor.while_)

### `kd.with_attr(x, attr_name, value, overwrite_schema=False)` {#kd.with_attr}

Alias for [kd.core.with_attr](kd/core.md#kd.core.with_attr)

### `kd.with_attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.with_attrs}

Alias for [kd.core.with_attrs](kd/core.md#kd.core.with_attrs)

### `kd.with_bag(ds, bag)` {#kd.with_bag}

Alias for [kd.core.with_bag](kd/core.md#kd.core.with_bag)

### `kd.with_dict_update(x, keys, values=unspecified)` {#kd.with_dict_update}

Alias for [kd.dicts.with_dict_update](kd/dicts.md#kd.dicts.with_dict_update)

### `kd.with_list_append_update(x, append)` {#kd.with_list_append_update}

Alias for [kd.lists.with_list_append_update](kd/lists.md#kd.lists.with_list_append_update)

### `kd.with_merged_bag(ds)` {#kd.with_merged_bag}

Alias for [kd.core.with_merged_bag](kd/core.md#kd.core.with_merged_bag)

### `kd.with_metadata(x, /, **attrs)` {#kd.with_metadata}

Alias for [kd.core.with_metadata](kd/core.md#kd.core.with_metadata)

### `kd.with_name(obj: Any, name: str | Text) -> Any` {#kd.with_name}

Alias for [kd.types.DataBag.with_name](kd/types/data_bag.md#kd.types.DataBag.with_name)

### `kd.with_print(x, *args, sep=' ', end='\n')` {#kd.with_print}

Alias for [kd.core.with_print](kd/core.md#kd.core.with_print)

### `kd.with_schema(x, schema)` {#kd.with_schema}

Alias for [kd.schema.with_schema](kd/schema.md#kd.schema.with_schema)

### `kd.with_schema_from_obj(x)` {#kd.with_schema_from_obj}

Alias for [kd.schema.with_schema_from_obj](kd/schema.md#kd.schema.with_schema_from_obj)

### `kd.with_timestamp(x)` {#kd.with_timestamp}

Alias for [kd.core.with_timestamp](kd/core.md#kd.core.with_timestamp)

### `kd.xor(x, y)` {#kd.xor}

Alias for [kd.masking.xor](kd/masking.md#kd.masking.xor)

### `kd.zip(*args)` {#kd.zip}

Alias for [kd.slices.zip](kd/slices.md#kd.slices.zip)

