<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.core API

Core operators that are not part of other categories.





### `kd.core.attr(x, attr_name, value, overwrite_schema=False)` {#kd.core.attr}
Aliases:

- [kd.attr](../kd.md#kd.attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag containing attribute `attr_name` update for `x`.

This operator is useful if attr_name cannot be used as a key in keyword
arguments. E.g.: &#34;123-f&#34;, &#34;5&#34;, &#34;%#$&#34;, etc. It still has to be a valid utf-8
unicode.

See kd.attrs docstring for more details on the rules and regarding `overwrite`
argument.

Args:
  x: Entity / Object for which the attribute update is being created.
  attr_name: utf-8 unicode representing the attribute name.
  value: new value for attribute `attr_name`.
  overwrite_schema: if True, schema for attribute is always updated.</code></pre>

### `kd.core.attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.core.attrs}
Aliases:

- [kd.attrs](../kd.md#kd.attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag containing attribute updates for `x`.

Most common usage is to build an update using kd.attrs and than attach it as a
DataBag update to the DataSlice.

Example:
  x = ...
  attr_update = kd.attrs(x, foo=..., bar=...)
  x = x.updated(attr_update)

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

### `kd.core.clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.core.clone}
Aliases:

- [kd.clone](../kd.md#kd.clone)

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

### `kd.core.clone_as_full(x, /, *, itemid=unspecified, **overrides)` {#kd.core.clone_as_full}
Aliases:

- [kd.clone_as_full](../kd.md#kd.clone_as_full)

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

### `kd.core.deep_clone(x, /, schema=unspecified, **overrides)` {#kd.core.deep_clone}
Aliases:

- [kd.deep_clone](../kd.md#kd.deep_clone)

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

### `kd.core.enriched(ds, *bag)` {#kd.core.enriched}
Aliases:

- [kd.enriched](../kd.md#kd.enriched)

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

### `kd.core.extract(ds, schema=unspecified)` {#kd.core.extract}
Aliases:

- [kd.extract](../kd.md#kd.extract)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.</code></pre>

### `kd.core.extract_update(ds, schema=unspecified)` {#kd.core.extract_update}
Aliases:

- [kd.extract_update](../kd.md#kd.extract_update)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataBag containing only reachable attrs from &#39;ds&#39;.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from &#39;ds&#39;.</code></pre>

### `kd.core.follow(x)` {#kd.core.follow}
Aliases:

- [kd.follow](../kd.md#kd.follow)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it&#39;s attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.</code></pre>

### `kd.core.freeze(x)` {#kd.core.freeze}
Aliases:

- [kd.freeze](../kd.md#kd.freeze)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen version of `x`.</code></pre>

### `kd.core.freeze_bag(x)` {#kd.core.freeze_bag}
Aliases:

- [kd.freeze_bag](../kd.md#kd.freeze_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with an immutable DataBag with the same data.</code></pre>

### `kd.core.get_attr(x, attr_name, default=unspecified)` {#kd.core.get_attr}
Aliases:

- [kd.get_attr](../kd.md#kd.get_attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Resolves (ObjectId(s), attr_name) =&gt; (Value|ObjectId)s.

In case attr points to Lists or Maps, the result is a DataSlice that
contains &#34;pointers&#34; to the beginning of lists/dicts.

For simple values ((entity, attr) =&gt; values), just returns
DataSlice(primitive values)

Args:
  x: DataSlice to get attribute from.
  attr_name: name of the attribute to access.
  default: default value to use when `x` does not have such attribute. In case
    default is specified, this will not warn/raise if the attribute does not
    exist in the schema, so one can use `default=None` to suppress the missing
    attribute warning/error. When `default=None` and the attribute is missing
    on all entities, this will return an empty slices with NONE schema.

Returns:
  DataSlice</code></pre>

### `kd.core.get_attr_names(x)` {#kd.core.get_attr_names}
Aliases:

- [kd.get_attr_names](../kd.md#kd.get_attr_names)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with sorted attribute names for each item in `x`.

The result has a new dimension with the attribute names.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty slice is returned.

Args:
  x: A DataSlice.</code></pre>

### `kd.core.get_bag(ds)` {#kd.core.get_bag}
Aliases:

- [kd.get_bag](../kd.md#kd.get_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attached DataBag.

It raises an Error if there is no DataBag attached.

Args:
  ds: DataSlice to get DataBag from.

Returns:
  The attached DataBag.</code></pre>

### `kd.core.get_item(x, key_or_index)` {#kd.core.get_item}
Aliases:

- [kd.dicts.get_item](dicts.md#kd.dicts.get_item)

- [kd.lists.get_item](lists.md#kd.lists.get_item)

- [kd.get_item](../kd.md#kd.get_item)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Get items from Lists or Dicts in `x` by `key_or_index`.

Examples:
l = kd.list([1, 2, 3])
# Get List items by range slice from 1 to -1
kd.get_item(l, slice(1, -1)) -&gt; kd.slice([2, 3])
# Get List items by indices
kd.get_item(l, kd.slice([2, 5])) -&gt; kd.slice([3, None])

d = kd.dict({&#39;a&#39;: 1, &#39;b&#39;: 2})
# Get Dict values by keys
kd.get_item(d, kd.slice([&#39;a&#39;, &#39;c&#39;])) -&gt; kd.slice([1, None])

Args:
  x: List or Dict DataSlice.
  key_or_index: DataSlice or Slice.

Returns:
  Result DataSlice.</code></pre>

### `kd.core.get_metadata(x)` {#kd.core.get_metadata}
Aliases:

- [kd.get_metadata](../kd.md#kd.get_metadata)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Gets a metadata from a DataSlice.

Args:
  x: DataSlice to get metadata from.

Returns:
  Metadata DataSlice.</code></pre>

### `kd.core.has_attr(x, attr_name)` {#kd.core.has_attr}
Aliases:

- [kd.has_attr](../kd.md#kd.has_attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Indicates whether the items in `x` DataSlice have the given attribute.

This function checks for attributes based on data rather than &#34;schema&#34; and may
be slow in some cases.

Args:
  x: DataSlice
  attr_name: Name of the attribute to check.

Returns:
  A MASK DataSlice with the same shape as `x` that contains present if the
  attribute exists for the corresponding item.</code></pre>

### `kd.core.has_bag(ds)` {#kd.core.has_bag}
Aliases:

- [kd.has_bag](../kd.md#kd.has_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataSlice `ds` has a DataBag attached.</code></pre>

### `kd.core.has_entity(x)` {#kd.core.has_entity}
Aliases:

- [kd.has_entity](../kd.md#kd.has_entity)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that is an Entity.

Note that this is a pointwise operation.

Also see `kd.is_entity` for checking if `x` is an Entity DataSlice. But
note that `kd.all(kd.has_entity(x))` is not always equivalent to
`kd.is_entity(x)`. For example,

  kd.is_entity(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_entity(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.core.has_primitive(x)` {#kd.core.has_primitive}
Aliases:

- [kd.has_primitive](../kd.md#kd.has_primitive)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that is primitive.

Note that this is a pointwise operation.

Also see `kd.is_primitive` for checking if `x` is a primitive DataSlice. But
note that `kd.all(kd.has_primitive(x))` is not always equivalent to
`kd.is_primitive(x)`. For example,

  kd.is_primitive(kd.int32(None)) -&gt; kd.present
  kd.all(kd.has_primitive(kd.int32(None))) -&gt; invalid for kd.all
  kd.is_primitive(kd.int32([None])) -&gt; kd.present
  kd.all(kd.has_primitive(kd.int32([None]))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.core.is_entity(x)` {#kd.core.is_entity}
Aliases:

- [kd.is_entity](../kd.md#kd.is_entity)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is an Entity DataSlice.

`x` is an Entity DataSlice if it meets one of the following conditions:
  1) it has an Entity schema
  2) it has OBJECT schema and only has Entity items

Also see `kd.has_entity` for a pointwise version. But note that
`kd.all(kd.has_entity(x))` is not always equivalent to
`kd.is_entity(x)`. For example,

  kd.is_entity(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_entity(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `kd.core.is_primitive(x)` {#kd.core.is_primitive}
Aliases:

- [kd.is_primitive](../kd.md#kd.is_primitive)

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

### `kd.core.maybe(x, attr_name)` {#kd.core.maybe}
Aliases:

- [kd.maybe](../kd.md#kd.maybe)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A shortcut for kd.get_attr(x, attr_name, default=None).</code></pre>

### `kd.core.metadata(x, /, **attrs)` {#kd.core.metadata}
Aliases:

- [kd.metadata](../kd.md#kd.metadata)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag containing metadata updates for `x`.

Most common usage is to build an update using kd.metadata and than attach it
as a DataBag update to the DataSlice.

Example:
  x = ...
  metadata_update = kd.metadata(x, foo=..., bar=...)
  x = x.updated(metadata_update)

Note that if the metadata attribute name is not a valid Python identifier, it
might be set by `with_attr` instead:
  metadata_update = kd.metadata(x).with_attr(&#39;123&#39;, value)

Args:
  x: Schema for which the metadata update is being created.
  **attrs: attrs to set in the metadata update.</code></pre>

### `kd.core.no_bag(ds)` {#kd.core.no_bag}
Aliases:

- [kd.no_bag](../kd.md#kd.no_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns DataSlice without any DataBag attached.</code></pre>

### `kd.core.nofollow(x)` {#kd.core.nofollow}
Aliases:

- [kd.nofollow](../kd.md#kd.nofollow)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a nofollow DataSlice targeting the given slice.

When a slice is wrapped into a nofollow, it&#39;s attributes are not further
traversed during extract, clone, deep_clone, etc.

`nofollow` is reversible.

Args:
  x: DataSlice to wrap.</code></pre>

### `kd.core.ref(ds)` {#kd.core.ref}
Aliases:

- [kd.ref](../kd.md#kd.ref)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.</code></pre>

### `kd.core.reify(ds, source)` {#kd.core.reify}
Aliases:

- [kd.reify](../kd.md#kd.reify)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns a bag and schema from `source` to the slice `ds`.</code></pre>

### `kd.core.shallow_clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.core.shallow_clone}
Aliases:

- [kd.shallow_clone](../kd.md#kd.shallow_clone)

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

### `kd.core.strict_attrs(x, /, **attrs)` {#kd.core.strict_attrs}
Aliases:

- [kd.strict_attrs](../kd.md#kd.strict_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag containing attribute updates for `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `kd.core.strict_with_attrs(x, /, **attrs)` {#kd.core.strict_with_attrs}
Aliases:

- [kd.strict_with_attrs](../kd.md#kd.strict_with_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `kd.core.stub(x, attrs=DataSlice([], schema: NONE, present: 0/0))` {#kd.core.stub}
Aliases:

- [kd.stub](../kd.md#kd.stub)

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

### `kd.core.updated(ds, *bag)` {#kd.core.updated}
Aliases:

- [kd.updated](../kd.md#kd.updated)

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

### `kd.core.with_attr(x, attr_name, value, overwrite_schema=False)` {#kd.core.with_attr}
Aliases:

- [kd.with_attr](../kd.md#kd.with_attr)

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

### `kd.core.with_attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.core.with_attrs}
Aliases:

- [kd.with_attrs](../kd.md#kd.with_attrs)

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

### `kd.core.with_bag(ds, bag)` {#kd.core.with_bag}
Aliases:

- [kd.with_bag](../kd.md#kd.with_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the given DataBatg attached.</code></pre>

### `kd.core.with_merged_bag(ds)` {#kd.core.with_merged_bag}
Aliases:

- [kd.with_merged_bag](../kd.md#kd.with_merged_bag)

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

### `kd.core.with_metadata(x, /, **attrs)` {#kd.core.with_metadata}
Aliases:

- [kd.with_metadata](../kd.md#kd.with_metadata)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated metadata for `x`.

This is a shorter version of `x.updated(kd.metadata(x, ...))`.

Example:
  x = kd.with_metadata(x, foo=..., bar=...)

Note that if the metadata attribute name is not a valid Python identifier, it
might be set by `with_attr` instead:
  x = kd.with_metadata(x).with_attr(&#39;123&#39;, value)

Args:
  x: Entity / Object for which the metadata update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `kd.core.with_print(x, *args, sep=' ', end='\n')` {#kd.core.with_print}
Aliases:

- [kd.with_print](../kd.md#kd.with_print)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Prints *args to stdout and returns `x`.

The operator uses str(arg) for each of the *args, i.e. it is not pointwise,
and too long arguments may be truncated.

Args:
  x: Value to propagate (unchanged).
  *args: DataSlice(s) to print.
  sep: Separator to use between DataSlice(s).
  end: End string to use after the last DataSlice.</code></pre>

### `kd.core.with_timestamp(x)` {#kd.core.with_timestamp}
Aliases:

- [kd.with_timestamp](../kd.md#kd.with_timestamp)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a tuple of `x` and a timestamp from the operator execution.

Args:
  x: Value to propagate (unchanged).
Returns:
  A tuple of `x` and a FLOAT64 DataSlice containing the timestamp in seconds
  since the Unix epoch.</code></pre>

