<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.dicts API

Operators working with dictionaries.





### `kd.dicts.dict_update(x, keys, values=unspecified)` {#kd.dicts.dict_update}
Aliases:

- [kd.dict_update](../kd.md#kd.dict_update)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns DataBag containing updates to a DataSlice of dicts.

This operator has two forms:
  kd.dict_update(x, keys, values) where keys and values are slices
  kd.dict_update(x, dict_updates) where dict_updates is a DataSlice of dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to &#39;x&#39;.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.</code></pre>

### `kd.dicts.get_item(x, key_or_index)` {#kd.dicts.get_item}

Alias for [kd.core.get_item](core.md#kd.core.get_item)

### `kd.dicts.get_keys(dict_ds)` {#kd.dicts.get_keys}
Aliases:

- [kd.get_keys](../kd.md#kd.get_keys)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns keys of all Dicts in `dict_ds`.

The result DataSlice has one more dimension used to represent keys in each
dict than `dict_ds`. While the order of keys within a dict is arbitrary, it is
the same as get_values().

Args:
  dict_ds: DataSlice of Dicts.

Returns:
  A DataSlice of keys.</code></pre>

### `kd.dicts.get_values(dict_ds, key_ds=unspecified)` {#kd.dicts.get_values}
Aliases:

- [kd.get_values](../kd.md#kd.get_values)

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

### `kd.dicts.has_dict(x)` {#kd.dicts.has_dict}
Aliases:

- [kd.has_dict](../kd.md#kd.has_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that is Dict.

Note that this is a pointwise operation.

Also see `kd.is_dict` for checking if `x` is a Dict DataSlice. But note that
`kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
example,

  kd.is_dict(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_dict(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.dicts.is_dict(x)` {#kd.dicts.is_dict}
Aliases:

- [kd.is_dict](../kd.md#kd.is_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is a Dict DataSlice.

`x` is a Dict DataSlice if it meets one of the following conditions:
  1) it has a Dict schema
  2) it has OBJECT schema and only has Dict items

Also see `kd.has_dict` for a pointwise version. But note that
`kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
example,

  kd.is_dict(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_dict(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `kd.dicts.like(shape_and_mask_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dicts.like}
Aliases:

- [kd.dict_like](../kd.md#kd.dict_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

Returns immutable dicts.

If items_or_keys and values are not provided, creates empty dicts. Otherwise,
the function assigns the given keys and values to the newly created dicts. So
the keys and values must be either broadcastable to shape_and_mask_from
shape, or one dimension higher.

Args:
  shape_and_mask_from: a DataSlice with the shape and sparsity for the
    desired dicts.
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
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the dicts.</code></pre>

### `kd.dicts.new(items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dicts.new}
Aliases:

- [kd.dict](../kd.md#kd.dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a Koda dict.

Returns an immutable dict.

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
dict(kd.slice([1, 2]), kd.slice([3, 4]))
  -&gt; returns a dict ({1: 3, 2: 4})
dict(kd.slice([[1], [2]]), kd.slice([3, 4]))
  -&gt; returns a 1-D DataSlice that holds two dicts ({1: 3} and {2: 4})
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
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the dict.</code></pre>

### `kd.dicts.select_keys(ds, fltr)` {#kd.dicts.select_keys}
Aliases:

- [kd.select_keys](../kd.md#kd.select_keys)

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

### `kd.dicts.select_values(ds, fltr)` {#kd.dicts.select_values}
Aliases:

- [kd.select_values](../kd.md#kd.select_values)

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

### `kd.dicts.shaped(shape: JaggedShape, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dicts.shaped}
Aliases:

- [kd.dict_shaped](../kd.md#kd.dict_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda dicts with the given shape.

Returns immutable dicts.

If items_or_keys and values are not provided, creates empty dicts. Otherwise,
the function assigns the given keys and values to the newly created dicts. So
the keys and values must be either broadcastable to `shape` or one dimension
higher.

Args:
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
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the dicts.</code></pre>

### `kd.dicts.shaped_as(shape_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dicts.shaped_as}
Aliases:

- [kd.dict_shaped_as](../kd.md#kd.dict_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda dicts with shape of the given DataSlice.

Returns immutable dicts.

If items_or_keys and values are not provided, creates empty dicts. Otherwise,
the function assigns the given keys and values to the newly created dicts. So
the keys and values must be either broadcastable to `shape` or one dimension
higher.

Args:
  shape_from: mandatory DataSlice, whose shape the returned DataSlice will
    have.
  items_or_keys: either a Python dict (if `values` is None) or a DataSlice
    with keys. The Python dict case is supported only for scalar shape.
  values: a DataSlice of values, when `items_or_keys` represents keys.
  key_schema: the schema of the dict keys. If not specified, it will be
    deduced from keys or defaulted to OBJECT.
  value_schema: the schema of the dict values. If not specified, it will be
    deduced from values or defaulted to OBJECT.
  schema: The schema to use for the newly created Dict. If specified, then
    key_schema and value_schema must not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the dicts.</code></pre>

### `kd.dicts.size(dict_slice)` {#kd.dicts.size}
Aliases:

- [kd.dict_size](../kd.md#kd.dict_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a Dict.</code></pre>

### `kd.dicts.uu(items_or_keys, values=unspecified, *, key_schema=unspecified, value_schema=unspecified, schema=unspecified, seed='')` {#kd.dicts.uu}
Aliases:

- [kd.uudict](../kd.md#kd.uudict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a dict with itemid determined deterministically.

The dict&#39;s ItemId is computed deterministically from the seed, keys, and
values using uuid_for_dict.

Examples:
uudict({1: 2, 3: 4}) -&gt; returns a deterministic dict ({1: 2, 3: 4})
uudict(kd.slice([1, 2]), kd.slice([3, 4]))
  -&gt; returns a deterministic dict ({1: 3, 2: 4})
uudict(kd.slice([1, 2]), kd.slice([3, 4]), seed=&#39;my_seed&#39;)
  -&gt; returns a deterministic dict with a different id than above

Args:
  items_or_keys: a Python dict, or a DataSlice with keys.
  values: a DataSlice with values. Must not be specified if items_or_keys is a
    Python dict.
  key_schema: the schema of the dict keys. If not specified, it will be
    deduced from keys or defaulted to OBJECT.
  value_schema: the schema of the dict values. If not specified, it will be
    deduced from values or defaulted to OBJECT.
  schema: the schema to use for the newly created Dict. If specified, then
    key_schema and value_schema must not be specified.
  seed: text seed for the uuid computation.

Returns:
  A DataSlice with the dict.</code></pre>

### `kd.dicts.with_dict_update(x, keys, values=unspecified)` {#kd.dicts.with_dict_update}
Aliases:

- [kd.with_dict_update](../kd.md#kd.with_dict_update)

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

