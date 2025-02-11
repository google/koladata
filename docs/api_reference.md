<!-- Note: This file is auto-generated, do not edit manually. -->

# Koda API Reference

This document lists public **Koda** APIs, including operators (accessible from
`kd` and `kde` packages) and methods of main abstractions (e.g.
DataSlice, DataBag, etc.).

Category  | Subcategory | Description
--------- | ----------- | ------------
[kd](#kd_category) | | `kd` and `kde` operators
 | [allocation](#kd.allocation) | Operators that allocate new ItemIds.
 | [annotation](#kd.annotation) | Annotation operators.
 | [assertion](#kd.assertion) | Operators that assert properties of DataSlices.
 | [bags](#kd.bags) | Operators that work on DataBags.
 | [comparison](#kd.comparison) | Operators that compare DataSlices.
 | [core](#kd.core) | Core operators that are not part of other categories.
 | [entities](#kd.entities) | Operators that work solely with entities.
 | [dicts](#kd.dicts) | Operators working with dictionaries.
 | [functor](#kd.functor) | Operators to create and call functors.
 | [ids](#kd.ids) | Operators that work with ItemIds.
 | [json](#kd.json) | JSON serialization operators.
 | [lists](#kd.lists) | Operators working with lists.
 | [masking](#kd.masking) | Masking operators.
 | [math](#kd.math) | Arithmetic operators.
 | [objs](#kd.objs) | Operators that work solely with objects.
 | [py](#kd.py) | Operators that call Python functions.
 | [random](#kd.random) | Random and sampling operators.
 | [schema](#kd.schema) | Schema-related operators.
 | [shapes](#kd.shapes) | Operators that work on shapes
 | [slices](#kd.slices) | Operators that perform DataSlice transformations.
 | [strings](#kd.strings) | Operators that work with strings data.
 | [tuple](#kd.tuple) | Operators to create tuples.
[kd_ext](#kd_ext_category) | | `kd_ext` operators
[DataSlice](#DataSlice_category) | | `DataSlice` methods
[DataBag](#DataBag_category) | | `DataBag` methods
[DataItem](#DataItem_category) | | `DataItem` methods

## `kd` and `kde` operators {#kd_category}

`kd` and `kde` modules are containers for eager and lazy operators respectively.

While most of operators below have both eager and lazy versions (e.g.
`kd.agg_sum` vs `kde.agg_sum`), some operators (e.g. `kd.sub(expr, *subs)`) only
have eager version. Such operators often take Exprs or Functors as inputs and
does not make sense to have a lazy version.

Note that operators from extension modules (e.g. `kd_ext.npkd`) are not
included.


<section class="zippy open">

**Namespaces**

### kd.allocation {#kd.allocation}

Operators that allocate new ItemIds.

<section class="zippy closed">

**Operators**

### `kd.allocation.new_dictid()` {#kd.allocation.new_dictid}
Aliases:

- [kd.new_dictid](#kd.new_dictid)

``` {.no-copy}
Allocates new Dict ItemId.
```

### `kd.allocation.new_dictid_like(shape_and_mask_from)` {#kd.allocation.new_dictid_like}
Aliases:

- [kd.new_dictid_like](#kd.new_dictid_like)

``` {.no-copy}
Allocates new Dict ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `kd.allocation.new_dictid_shaped(shape)` {#kd.allocation.new_dictid_shaped}
Aliases:

- [kd.new_dictid_shaped](#kd.new_dictid_shaped)

``` {.no-copy}
Allocates new Dict ItemIds of the given shape.
```

### `kd.allocation.new_dictid_shaped_as(shape_from)` {#kd.allocation.new_dictid_shaped_as}
Aliases:

- [kd.new_dictid_shaped_as](#kd.new_dictid_shaped_as)

``` {.no-copy}
Allocates new Dict ItemIds with the shape of shape_from.
```

### `kd.allocation.new_itemid()` {#kd.allocation.new_itemid}
Aliases:

- [kd.new_itemid](#kd.new_itemid)

``` {.no-copy}
Allocates new ItemId.
```

### `kd.allocation.new_itemid_like(shape_and_mask_from)` {#kd.allocation.new_itemid_like}
Aliases:

- [kd.new_itemid_like](#kd.new_itemid_like)

``` {.no-copy}
Allocates new ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `kd.allocation.new_itemid_shaped(shape)` {#kd.allocation.new_itemid_shaped}
Aliases:

- [kd.new_itemid_shaped](#kd.new_itemid_shaped)

``` {.no-copy}
Allocates new ItemIds of the given shape without any DataBag attached.
```

### `kd.allocation.new_itemid_shaped_as(shape_from)` {#kd.allocation.new_itemid_shaped_as}
Aliases:

- [kd.new_itemid_shaped_as](#kd.new_itemid_shaped_as)

``` {.no-copy}
Allocates new ItemIds with the shape of shape_from.
```

### `kd.allocation.new_listid()` {#kd.allocation.new_listid}
Aliases:

- [kd.new_listid](#kd.new_listid)

``` {.no-copy}
Allocates new List ItemId.
```

### `kd.allocation.new_listid_like(shape_and_mask_from)` {#kd.allocation.new_listid_like}
Aliases:

- [kd.new_listid_like](#kd.new_listid_like)

``` {.no-copy}
Allocates new List ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `kd.allocation.new_listid_shaped(shape)` {#kd.allocation.new_listid_shaped}
Aliases:

- [kd.new_listid_shaped](#kd.new_listid_shaped)

``` {.no-copy}
Allocates new List ItemIds of the given shape.
```

### `kd.allocation.new_listid_shaped_as(shape_from)` {#kd.allocation.new_listid_shaped_as}
Aliases:

- [kd.new_listid_shaped_as](#kd.new_listid_shaped_as)

``` {.no-copy}
Allocates new List ItemIds with the shape of shape_from.
```

</section>

### kd.annotation {#kd.annotation}

Annotation operators.

<section class="zippy closed">

**Operators**

### `kd.annotation.with_name(obj, name)` {#kd.annotation.with_name}
Aliases:

- [kd.with_name](#kd.with_name)

- [DataSlice.with_name](#DataSlice.with_name)

- [DataBag.with_name](#DataBag.with_name)

- [DataItem.with_name](#DataItem.with_name)

``` {.no-copy}
Checks that the `name` is a string and returns `obj` unchanged.

  This method is useful in tracing workflows: when tracing, we will assign
  the given name to the subexpression computing `obj`. In eager mode, this
  method is effectively a no-op.

  Args:
    obj: Any object.
    name: The name to be used for this sub-expression when tracing this code.
      Must be a string.

  Returns:
    obj unchanged.
```

</section>

### kd.assertion {#kd.assertion}

Operators that assert properties of DataSlices.

<section class="zippy closed">

**Operators**

### `kd.assertion.assert_ds_has_primitives_of(ds, primitive_schema, message)` {#kd.assertion.assert_ds_has_primitives_of}

``` {.no-copy}
Returns `ds` if it matches `primitive_schema`, or raises an exception.

It raises an exception if:
  1) `ds`'s schema is not primitive_schema, OBJECT or ANY
  2) `ds` has present items and not all of them match `primitive_schema`

The following examples will pass:
  assert_ds_has_primitives_of(kd.present, kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice([kd.present, kd.missing]), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice(None, schema=kd.OBJECT), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice(None, schema=kd.ANY), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice([], schema=kd.OBJECT), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice([], schema=kd.ANY), kd.MASK, '')

The following examples will fail:
  assert_ds_has_primitives_of(1, kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice([kd.present, 1]), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice(1, schema=kd.OBJECT), kd.MASK, '')
  assert_ds_has_primitives_of(kd.slice(1, schema=kd.ANY), kd.MASK, '')

Args:
  ds: DataSlice to assert the dtype of.
  primitive_schema: The expected primitive schema.
  message: The error message to raise if the primitive schemas do not match.

Returns:
  `ds` if the primitive schemas match.
```

### `kd.assertion.with_assertion(x, condition, message)` {#kd.assertion.with_assertion}

``` {.no-copy}
Returns `x` if `condition` is present, else raises error `message`.

Example:
  x = kd.slice(1)
  y = kd.slice(2)
  kd.assertion.with_assertion(x, x < y, 'x must be less than y') -> x.
  kd.assertion.with_assertion(x, x > y, 'x must be greater than y') -> error.

Args:
  x: The value to return if `condition` is present.
  condition: A unit scalar, unit optional, or DataItem holding a mask.
  message: The error message to raise if `condition` is not present.
```

</section>

### kd.bags {#kd.bags}

Operators that work on DataBags.

<section class="zippy closed">

**Operators**

### `kd.bags.enriched(*bags)` {#kd.bags.enriched}
Aliases:

- [kd.enriched_bag](#kd.enriched_bag)

``` {.no-copy}
Creates a new immutable DataBag enriched by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags earlier in the list have higher priority.
 `enriched_bag(bag1, bag2, bag3)` is equivalent to
 `enriched_bag(enriched_bag(bag1, bag2), bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for enriching.

Returns:
  An immutable DataBag enriched by `bags`.
```

### `kd.bags.new()` {#kd.bags.new}
Aliases:

- [kd.bag](#kd.bag)

- [DataBag.empty](#DataBag.empty)

``` {.no-copy}
Returns an empty DataBag.
```

### `kd.bags.updated(*bags)` {#kd.bags.updated}
Aliases:

- [kd.updated_bag](#kd.updated_bag)

``` {.no-copy}
Creates a new immutable DataBag updated by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags later in the list have higher priority.
 `updated_bag(bag1, bag2, bag3)` is equivalent to
 `updated_bag(bag1, updated_bag(bag2, bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for updating.

Returns:
  An immutable DataBag updated by `bags`.
```

</section>

### kd.comparison {#kd.comparison}

Operators that compare DataSlices.

<section class="zippy closed">

**Operators**

### `kd.comparison.equal(x, y)` {#kd.comparison.equal}
Aliases:

- [kd.equal](#kd.equal)

``` {.no-copy}
Returns present iff `x` and `y` are equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are equal. Returns `kd.present` for equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.full_equal(x, y)` {#kd.comparison.full_equal}
Aliases:

- [kd.full_equal](#kd.full_equal)

``` {.no-copy}
Returns present iff all present items in `x` and `y` are equal.

The result is a zero-dimensional DataItem. Note that it is different from
`kd.all(x == y)`.

For example,
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, 3])) -> kd.present
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, None])) -> kd.missing
  kd.full_equal(kd.slice([1, 2, None]), kd.slice([1, 2, None])) -> kd.present

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.greater(x, y)` {#kd.comparison.greater}
Aliases:

- [kd.greater](#kd.greater)

``` {.no-copy}
Returns present iff `x` is greater than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than `y`. Returns `kd.present` when `x` is greater and
`kd.missing` when `x` is less than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.greater_equal(x, y)` {#kd.comparison.greater_equal}
Aliases:

- [kd.greater_equal](#kd.greater_equal)

``` {.no-copy}
Returns present iff `x` is greater than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than or equal to `y`. Returns `kd.present` when `x` is
greater than or equal to `y` and `kd.missing` when `x` is less than `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.less(x, y)` {#kd.comparison.less}
Aliases:

- [kd.less](#kd.less)

``` {.no-copy}
Returns present iff `x` is less than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than `y`. Returns `kd.present` when `x` is less and
`kd.missing` when `x` is greater than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.less_equal(x, y)` {#kd.comparison.less_equal}
Aliases:

- [kd.less_equal](#kd.less_equal)

``` {.no-copy}
Returns present iff `x` is less than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than or equal to `y`. Returns `kd.present` when `x` is
less than or equal to `y` and `kd.missing` when `x` is greater than `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `kd.comparison.not_equal(x, y)` {#kd.comparison.not_equal}
Aliases:

- [kd.not_equal](#kd.not_equal)

``` {.no-copy}
Returns present iff `x` and `y` are not equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are not equal. Returns `kd.present` for not equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.
```

</section>

### kd.core {#kd.core}

Core operators that are not part of other categories.

<section class="zippy closed">

**Operators**

### `kd.core.attr(x, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#kd.core.attr}
Aliases:

- [kd.attr](#kd.attr)

``` {.no-copy}
Returns a new DataBag containing attribute `attr_name` update for `x`.
```

### `kd.core.attrs(x, /, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#kd.core.attrs}
Aliases:

- [kd.attrs](#kd.attrs)

``` {.no-copy}
Returns a new DataBag containing attribute updates for `x`.
```

### `kd.core.clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.core.clone}
Aliases:

- [kd.clone](#kd.clone)

``` {.no-copy}
Creates a DataSlice with clones of provided entities in a new DataBag.

The entities themselves and their top-level attributes are cloned (with new
ItemIds) and non-top-level attributes are extracted (with the same ItemIds).

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
  A copy of the entities where all top-level attributes are cloned (new
  ItemIds) and all of the rest extracted.
```

### `kd.core.container(*, db=None, **attrs)` {#kd.core.container}
Aliases:

- [kd.container](#kd.container)

``` {.no-copy}
Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema and mutable DataBag.

  Args:
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `kd.core.deep_clone(x, /, schema=unspecified, **overrides)` {#kd.core.deep_clone}
Aliases:

- [kd.deep_clone](#kd.deep_clone)

``` {.no-copy}
Creates a slice with a (deep) copy of the given slice.

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
    schema of 'x'.
  **overrides: attribute overrides.

Returns:
  A (deep) copy of the given DataSlice.
  All referenced entities will be copied with newly allocated ItemIds. Note
  that UUIDs will be copied as ItemIds.
```

### `kd.core.enriched(ds, *bag)` {#kd.core.enriched}
Aliases:

- [kd.enriched](#kd.enriched)

``` {.no-copy}
Returns a copy of a DataSlice with a additional fallback DataBag(s).

Values in the original DataBag of `ds` take precedence over the ones in
`*bag`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`enriched(x, a, b)` is equivalent to `enriched(enriched(x, a), b)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: additional fallback DataBag(s).

Returns:
  DataSlice with additional fallbacks.
```

### `kd.core.extract(ds, schema=unspecified)` {#kd.core.extract}
Aliases:

- [kd.extract](#kd.extract)

``` {.no-copy}
Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.
```

### `kd.core.extract_bag(ds, schema=unspecified)` {#kd.core.extract_bag}
Aliases:

- [kd.extract_bag](#kd.extract_bag)

``` {.no-copy}
Creates a new DataBag containing only reachable attrs from 'ds'.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from 'ds'.
```

### `kd.core.follow(x)` {#kd.core.follow}
Aliases:

- [kd.follow](#kd.follow)

``` {.no-copy}
Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it's attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.
```

### `kd.core.freeze(x)` {#kd.core.freeze}
Aliases:

- [kd.freeze](#kd.freeze)

``` {.no-copy}
Returns a frozen version of `x`.
```

### `kd.core.freeze_bag(x)` {#kd.core.freeze_bag}
Aliases:

- [kd.freeze_bag](#kd.freeze_bag)

``` {.no-copy}
Returns a DataSlice with an immutable DataBag with the same data.
```

### `kd.core.get_attr(x, attr_name, default=unspecified)` {#kd.core.get_attr}
Aliases:

- [kd.get_attr](#kd.get_attr)

``` {.no-copy}
Resolves (ObjectId(s), attr_name) => (Value|ObjectId)s.

In case attr points to Lists or Maps, the result is a DataSlice that
contains "pointers" to the beginning of lists/dicts.

For simple values ((entity, attr) => values), just returns
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
  DataSlice
```

### `kd.core.get_attr_names(x, intersection)` {#kd.core.get_attr_names}

``` {.no-copy}
Returns a DataSlice with sorted unique attribute names of `x`.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  x: A DataSlice.
  intersection: If True, the intersection of all object attributes is
    returned. Otherwise, the union is returned.
```

### `kd.core.get_bag(ds)` {#kd.core.get_bag}
Aliases:

- [kd.get_bag](#kd.get_bag)

``` {.no-copy}
Returns the attached DataBag.

It raises an Error if there is no DataBag attached.

Args:
  ds: DataSlice to get DataBag from.

Returns:
  The attached DataBag.
```

### `kd.core.get_item(x, key_or_index)` {#kd.core.get_item}
Aliases:

- [kd.dicts.get_item](#kd.dicts.get_item)

- [kd.lists.get_item](#kd.lists.get_item)

- [kd.get_item](#kd.get_item)

``` {.no-copy}
Get items from Lists or Dicts in `x` by `key_or_index`.

Examples:
l = kd.list([1, 2, 3])
# Get List items by range slice from 1 to -1
kd.get_item(l, slice(1, -1)) -> kd.slice([2, 3])
# Get List items by indices
kd.get_item(l, kd.slice([2, 5])) -> kd.slice([3, None])

d = kd.dict({'a': 1, 'b': 2})
# Get Dict values by keys
kd.get_item(d, kd.slice(['a', 'c'])) -> kd.slice([1, None])

Args:
  x: List or Dict DataSlice.
  key_or_index: DataSlice or Slice.

Returns:
  Result DataSlice.
```

### `kd.core.has_attr(x, attr_name)` {#kd.core.has_attr}
Aliases:

- [kd.has_attr](#kd.has_attr)

``` {.no-copy}
Indicates whether the items in `x` DataSlice have the given attribute.

This function checks for attributes based on data rather than "schema" and may
be slow in some cases.

Args:
  x: DataSlice
  attr_name: Name of the attribute to check.

Returns:
  A MASK DataSlice with the same shape as `x` that contains present if the
  attribute exists for the corresponding item.
```

### `kd.core.has_entity(x)` {#kd.core.has_entity}
Aliases:

- [kd.has_entity](#kd.has_entity)

``` {.no-copy}
Returns present for each item in `x` that is an Entity.

Note that this is a pointwise operation.

Also see `kd.is_entity` for checking if `x` is an Entity DataSlice. But
note that `kd.all(kd.has_entity(x))` is not always equivalent to
`kd.is_entity(x)`. For example,

  kd.is_entity(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_entity(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.
```

### `kd.core.has_primitive(x)` {#kd.core.has_primitive}
Aliases:

- [kd.has_primitive](#kd.has_primitive)

``` {.no-copy}
Returns present for each item in `x` that is primitive.

Note that this is a pointwise operation.

Also see `kd.is_primitive` for checking if `x` is a primitive DataSlice. But
note that `kd.all(kd.has_primitive(x))` is not always equivalent to
`kd.is_primitive(x)`. For example,

  kd.is_primitive(kd.int32(None)) -> kd.present
  kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
  kd.is_primitive(kd.int32([None])) -> kd.present
  kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.
```

### `kd.core.is_entity(x)` {#kd.core.is_entity}
Aliases:

- [kd.is_entity](#kd.is_entity)

``` {.no-copy}
Returns whether x is an Entity DataSlice.

`x` is an Entity DataSlice if it meets one of the following conditions:
  1) it has an Entity schema
  2) it has OBJECT/ANY schema and only has Entity items

Also see `kd.has_entity` for a pointwise version. But note that
`kd.all(kd.has_entity(x))` is not always equivalent to
`kd.is_entity(x)`. For example,

  kd.is_entity(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_entity(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.
```

### `kd.core.is_primitive(x)` {#kd.core.is_primitive}
Aliases:

- [kd.is_primitive](#kd.is_primitive)

``` {.no-copy}
Returns whether x is a primitive DataSlice.

`x` is a primitive DataSlice if it meets one of the following conditions:
  1) it has a primitive schema
  2) it has OBJECT/ANY/SCHEMA schema and only has primitives

Also see `kd.has_primitive` for a pointwise version. But note that
`kd.all(kd.has_primitive(x))` is not always equivalent to
`kd.is_primitive(x)`. For example,

  kd.is_primitive(kd.int32(None)) -> kd.present
  kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
  kd.is_primitive(kd.int32([None])) -> kd.present
  kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.
```

### `kd.core.maybe(x, attr_name)` {#kd.core.maybe}
Aliases:

- [kd.maybe](#kd.maybe)

``` {.no-copy}
A shortcut for kd.get_attr(x, attr_name, default=None).
```

### `kd.core.no_bag(ds)` {#kd.core.no_bag}
Aliases:

- [kd.no_bag](#kd.no_bag)

``` {.no-copy}
Returns DataSlice without any DataBag attached.
```

### `kd.core.nofollow(x)` {#kd.core.nofollow}
Aliases:

- [kd.nofollow](#kd.nofollow)

``` {.no-copy}
Returns a nofollow DataSlice targeting the given slice.

When a slice is wrapped into a nofollow, it's attributes are not further
traversed during extract, clone, deep_clone, etc.

`nofollow` is reversible.

Args:
  x: DataSlice to wrap.
```

### `kd.core.ref(ds)` {#kd.core.ref}
Aliases:

- [kd.ref](#kd.ref)

``` {.no-copy}
Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.
```

### `kd.core.reify(ds, source)` {#kd.core.reify}
Aliases:

- [kd.reify](#kd.reify)

``` {.no-copy}
Assigns a bag and schema from `source` to the slice `ds`.
```

### `kd.core.shallow_clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.core.shallow_clone}
Aliases:

- [kd.shallow_clone](#kd.shallow_clone)

``` {.no-copy}
Creates a DataSlice with shallow clones of immediate attributes.

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
    the resulting DataSlice. If not specified, will use the schema of 'x'.
  **overrides: attribute overrides.

Returns:
  A copy of the entities with new ItemIds where all top-level attributes are
  copied by reference.
```

### `kd.core.stub(x, attrs=DataSlice([], schema: NONE, ndims: 1, size: 0))` {#kd.core.stub}
Aliases:

- [kd.stub](#kd.stub)

``` {.no-copy}
Copies a DataSlice's schema stub to a new DataBag.

The "schema stub" of a DataSlice is a subset of its schema (including embedded
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
  DataSlice with the same schema stub in the new DataBag.
```

### `kd.core.updated(ds, *bag)` {#kd.core.updated}
Aliases:

- [kd.updated](#kd.updated)

``` {.no-copy}
Returns a copy of a DataSlice with DataBag(s) of updates applied.

Values in `*bag` take precedence over the ones in the original DataBag of
`ds`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`updated(x, a, b)` is equivalent to `updated(updated(x, b), a)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: DataBag(s) of updates.

Returns:
  DataSlice with additional fallbacks.
```

### `kd.core.with_attr(x, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#kd.core.with_attr}
Aliases:

- [kd.with_attr](#kd.with_attr)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing a single updated attribute.
```

### `kd.core.with_attrs(x, /, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#kd.core.with_attrs}
Aliases:

- [kd.with_attrs](#kd.with_attrs)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated attributes.
```

### `kd.core.with_bag(ds, bag)` {#kd.core.with_bag}
Aliases:

- [kd.with_bag](#kd.with_bag)

``` {.no-copy}
Returns a DataSlice with the given DataBatg attached.
```

### `kd.core.with_merged_bag(ds)` {#kd.core.with_merged_bag}
Aliases:

- [kd.with_merged_bag](#kd.with_merged_bag)

``` {.no-copy}
Returns a DataSlice with the DataBag of `ds` merged with its fallbacks.

Note that a DataBag has multiple fallback DataBags and fallback DataBags can
have fallbacks as well. This operator merges all of them into a new immutable
DataBag.

If `ds` has no attached DataBag, it raises an exception. If the DataBag of
`ds` does not have fallback DataBags, it is equivalent to `ds.freeze_bag()`.

Args:
  ds: DataSlice to merge fallback DataBags of.

Returns:
  A new DataSlice with an immutable DataBags.
```

</section>

### kd.entities {#kd.entities}

Operators that work solely with entities.

<section class="zippy closed">

**Operators**

### `kd.entities.like(shape_and_mask_from, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.entities.like}
Aliases:

- [kd.new_like](#kd.new_like)

``` {.no-copy}
Creates new Entities with the shape and sparsity from shape_and_mask_from.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.entities.new(arg=unspecified, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.entities.new}
Aliases:

- [kd.new](#kd.new)

``` {.no-copy}
Creates Entities with given attrs.

  Returns an immutable Entity if `db` is not provided.

  Args:
    arg: optional Python object to be converted to an Entity.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
      itemid will only be set when the args is not a primitive or primitive
      slice if args present.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.entities.shaped(shape, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.entities.shaped}
Aliases:

- [kd.new_shaped](#kd.new_shaped)

``` {.no-copy}
Creates new Entities with the given shape.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.entities.shaped_as(shape_from, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.entities.shaped_as}
Aliases:

- [kd.new_shaped_as](#kd.new_shaped_as)

``` {.no-copy}
Creates new Koda entities with shape of the given DataSlice.

  Returns immutable Entities if `db` is not provided.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead. You can also pass schema='name' as a shortcut for
      schema=kd.named_schema('name').
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.entities.uu(seed=None, *, schema=None, update_schema=False, db=None, **attrs)` {#kd.entities.uu}
Aliases:

- [kd.uu](#kd.uu)

``` {.no-copy}
Creates UuEntities with given attrs.

  Returns an immutable UU Entity if `db` is not provided.

  Args:
    seed: string to seed the uuid computation with.
    schema: optional DataSlice schema. If not specified, a UuSchema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

</section>

### kd.dicts {#kd.dicts}

Operators working with dictionaries.

<section class="zippy closed">

**Operators**

### `kd.dicts.dict_update(x, keys, values=unspecified)` {#kd.dicts.dict_update}
Aliases:

- [kd.dict_update](#kd.dict_update)

``` {.no-copy}
Returns DataBag containing updates to a DataSlice of dicts.

This operator has two forms:
  kd.dict_update(x, keys, values) where keys and values are slices
  kd.dict_update(x, dict_updates) where dict_updates is a DataSlice of dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

### `kd.dicts.get_item(x, key_or_index)` {#kd.dicts.get_item}

Alias for [kd.core.get_item](#kd.core.get_item) operator.

### `kd.dicts.get_keys(dict_ds)` {#kd.dicts.get_keys}
Aliases:

- [kd.get_keys](#kd.get_keys)

``` {.no-copy}
Returns keys of all Dicts in `dict_ds`.

The result DataSlice has one more dimension used to represent keys in each
dict than `dict_ds`. While the order of keys within a dict is arbitrary, it is
the same as get_values().

Args:
  dict_ds: DataSlice of Dicts.

Returns:
  A DataSlice of keys.
```

### `kd.dicts.get_values(dict_ds, key_ds=unspecified)` {#kd.dicts.get_values}
Aliases:

- [kd.get_values](#kd.get_values)

``` {.no-copy}
Returns values corresponding to `key_ds` for dicts in `dict_ds`.

When `key_ds` is specified, it is equivalent to dict_ds[key_ds].

When `key_ds` is unspecified, it returns all values in `dict_ds`. The result
DataSlice has one more dimension used to represent values in each dict than
`dict_ds`. While the order of values within a dict is arbitrary, it is the
same as get_keys().

Args:
  dict_ds: DataSlice of Dicts.
  key_ds: DataSlice of keys or unspecified.

Returns:
  A DataSlice of values.
```

### `kd.dicts.has_dict(x)` {#kd.dicts.has_dict}
Aliases:

- [kd.has_dict](#kd.has_dict)

``` {.no-copy}
Returns present for each item in `x` that is Dict.

Note that this is a pointwise operation.

Also see `kd.is_dict` for checking if `x` is a Dict DataSlice. But note that
`kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
example,

  kd.is_dict(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_dict(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.
```

### `kd.dicts.is_dict(x)` {#kd.dicts.is_dict}
Aliases:

- [kd.is_dict](#kd.is_dict)

``` {.no-copy}
Returns whether x is a Dict DataSlice.

`x` is a Dict DataSlice if it meets one of the following conditions:
  1) it has a Dict schema
  2) it has OBJECT/ANY schema and only has Dict items

Also see `kd.has_dict` for a pointwise version. But note that
`kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
example,

  kd.is_dict(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_dict(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.
```

### `kd.dicts.like(shape_and_mask_from, /, items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dicts.like}
Aliases:

- [kd.dict_like](#kd.dict_like)

``` {.no-copy}
Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  Returns immutable dicts if `db` is not provided.

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
    db: optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `kd.dicts.new(items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dicts.new}
Aliases:

- [kd.dict](#kd.dict)

``` {.no-copy}
Creates a Koda dict.

  Returns an immutable dict if `db` is not provided.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) a Python dict whose keys are either primitives or DataItems and values
       are primitives, DataItems, Python list/dict which can be converted to a
       List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
       a single dict
    3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict({1: 2, 3: 4}) -> returns a single new dict
  dict({1: [1, 2]}) -> returns a single dict, mapping 1->List[1, 2]
  dict({1: kd.slice([1, 2])}) -> returns a single dict, mapping 1->List[1, 2]
  dict({db.uuobj(x=1, y=2): 3}) -> returns a single dict, mapping uuid->3
  dict(kd.slice([1, 2]), kd.slice([3, 4]))
    -> returns a dict ({1: 3, 2: 4})
  dict(kd.slice([[1], [2]]), kd.slice([3, 4]))
    -> returns a 1-D DataSlice that holds two dicts ({1: 3} and {2: 4})
  dict('key', 12) -> returns a single dict mapping 'key'->12

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
    db: optional DataBag where dict(s) are created.

  Returns:
    A DataSlice with the dict.
```

### `kd.dicts.select_keys(ds, fltr)` {#kd.dicts.select_keys}
Aliases:

- [kd.select_keys](#kd.select_keys)

``` {.no-copy}
Selects Dict keys by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `kd.dicts.select_values(ds, fltr)` {#kd.dicts.select_values}
Aliases:

- [kd.select_values](#kd.select_values)

``` {.no-copy}
Selects Dict values by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `kd.dicts.shaped(shape, /, items_or_keys=None, values=None, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dicts.shaped}
Aliases:

- [kd.dict_shaped](#kd.dict_shaped)

``` {.no-copy}
Creates new Koda dicts with the given shape.

  Returns immutable dicts if `db` is not provided.

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
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `kd.dicts.shaped_as(shape_from, /, items_or_keys=None, values=None, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dicts.shaped_as}
Aliases:

- [kd.dict_shaped_as](#kd.dict_shaped_as)

``` {.no-copy}
Creates new Koda dicts with shape of the given DataSlice.

  Returns immutable dicts if `db` is not provided.

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
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `kd.dicts.size(dict_slice)` {#kd.dicts.size}
Aliases:

- [kd.dict_size](#kd.dict_size)

``` {.no-copy}
Returns size of a Dict.
```

### `kd.dicts.with_dict_update(x, keys, values=unspecified)` {#kd.dicts.with_dict_update}
Aliases:

- [kd.with_dict_update](#kd.with_dict_update)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated dicts.

This operator has two forms:
  kd.with_dict_update(x, keys, values) where keys and values are slices
  kd.with_dict_update(x, dict_updates) where dict_updates is a DataSlice of
    dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

</section>

### kd.functor {#kd.functor}

Operators to create and call functors.

<section class="zippy closed">

**Operators**

### `kd.functor.TypeTracingConfig()` {#kd.functor.TypeTracingConfig}

``` {.no-copy}
Describes handling a given user Python type as input/output when tracing.
```

### `kd.functor.allow_arbitrary_unused_inputs(fn_def)` {#kd.functor.allow_arbitrary_unused_inputs}

``` {.no-copy}
Returns a functor that allows unused inputs but otherwise behaves the same.

  This is done by adding a `**__extra_inputs__` argument to the signature if
  there is no existing variadic keyword argument there. If there is a variadic
  keyword argument, this function will return the original functor.

  This means that if the functor already accepts arbitrary inputs but fails
  on unknown inputs further down the line (for example, when calling another
  functor), this method will not fix it. In particular, this method has no
  effect on the return values of kd.py_fn or kd.bind. It does however work
  on the output of kd.trace_py_fn.

  Args:
    fn_def: The input functor.

  Returns:
    The input functor if it already has a variadic keyword argument, or its copy
    but with an additional `**__extra_inputs__` variadic keyword argument if
    there is no existing variadic keyword argument.
```

### `kd.functor.bind(fn_def, /, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **kwargs)` {#kd.functor.bind}
Aliases:

- [kd.bind](#kd.bind)

``` {.no-copy}
Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  You can pass expressions with their own inputs as values in `kwargs`. Those
  inputs will become inputs of the resulting functor, will be used to compute
  those expressions, _and_ they will also be passed to the underying functor.
  Use kd.functor.call_fn for a more clear separation of those inputs.

  Example:
    f = kd.bind(kd.fn(I.x + I.y), x=0)
    kd.call(f, y=1)  # 1

  Args:
    fn_def: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **kwargs: Partial parameter binding. The values in this map may be Koda
      expressions or DataItems. When they are expressions, they must evaluate to
      a DataSlice/DataItem or a primitive that will be automatically wrapped
      into a DataItem. This function creates auxiliary variables with names
      starting with '_aux_fn', so it is not recommended to pass variables with
      such names.

  Returns:
    A new Koda functor with some parameters bound.
```

### `kd.functor.call(fn, *args, return_type_as=DataItem(None, schema: NONE), **kwargs)` {#kd.functor.call}
Aliases:

- [kd.call](#kd.call)

``` {.no-copy}
Calls a functor.

See the docstring of `kd.fn` on how to create a functor.

Example:
  kd.call(kd.fn(I.x + I.y), x=2, y=3)
  # returns kd.item(5)

  kd.lazy.call(I.fn, x=2, y=3).eval(fn=kd.fn(I.x + I.y))
  # returns kd.item(5)

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also
    be passed here.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The result of the call.
```

### `kd.functor.expr_fn(returns, *, signature=None, auto_variables=False, **variables)` {#kd.functor.expr_fn}

``` {.no-copy}
Creates a functor.

  Args:
    returns: What should calling a functor return. Will typically be an Expr to
      be evaluated, but can also be a DataItem in which case calling will just
      return this DataItem, or a primitive that will be wrapped as a DataItem.
      When this is an Expr, it either must evaluate to a DataSlice/DataItem, or
      the return_type_as= argument should be specified at kd.call time.
    signature: The signature of the functor. Will be used to map from args/
      kwargs passed at calling time to I.smth inputs of the expressions. When
      None, the default signature will be created based on the inputs from the
      expressions involved.
    auto_variables: When true, we create additional variables automatically
      based on the provided expressions for 'returns' and user-provided
      variables. All non-scalar DataSlice literals become their own variables,
      and all named subexpressions become their own variables. This helps
      readability and manipulation of the resulting functor.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataItem representing the functor.
```

### `kd.functor.fn(f, *, use_tracing=True, **kwargs)` {#kd.functor.fn}
Aliases:

- [kd.fn](#kd.fn)

- [kd_ext.Fn](#kd_ext.Fn)

``` {.no-copy}
Returns a Koda functor representing `f`.

  This is the most generic version of the functools builder functions.
  It accepts all functools supported function types including python functions,
  Koda Expr.

  Args:
    f: Python function, Koda Expr, Expr packed into a DataItem, or a Koda
      functor (the latter will be just returned unchanged).
    use_tracing: Whether tracing should be used for Python functions.
    **kwargs: Either variables or defaults to pass to the function. See the
      documentation of `expr_fn` and `py_fn` for more details.

  Returns:
    A Koda functor representing `f`.
```

### `kd.functor.fstr_fn(returns, **kwargs)` {#kd.functor.fstr_fn}

``` {.no-copy}
Returns a Koda functor from format string.

  Format-string must be created via Python f-string syntax. It must contain at
  least one formatted expression.

  kwargs are used to assign values to the functor variables and can be used in
  the formatted expression using V. syntax.

  Each formatted expression must have custom format specification,
  e.g. `{I.x:s}` or `{V.y:.2f}`.

  Examples:
    kd.call(fstr_fn(f'{I.x:s} {I.y:s}'), x=1, y=2)  # kd.slice('1 2')
    kd.call(fstr_fn(f'{V.x:s} {I.y:s}', x=1), y=2)  # kd.slice('1 2')
    kd.call(fstr_fn(f'{(I.x + I.y):s}'), x=1, y=2)  # kd.slice('3')
    kd.call(fstr_fn('abc'))  # error - no substitutions
    kd.call(fstr_fn('{I.x}'), x=1)  # error - format should be f-string

  Args:
    returns: A format string.
    **kwargs: variable assignments.
```

### `kd.functor.get_signature(fn_def)` {#kd.functor.get_signature}

``` {.no-copy}
Retrieves the signature attached to the given functor.

  Args:
    fn_def: The functor to retrieve the signature for, or a slice thereof.

  Returns:
    The signature(s) attached to the functor(s).
```

### `kd.functor.is_fn(obj)` {#kd.functor.is_fn}
Aliases:

- [kd.is_fn](#kd.is_fn)

``` {.no-copy}
Checks if `obj` represents a functor.

  Args:
    obj: The value to check.

  Returns:
    kd.present if `obj` is a DataSlice representing a functor, kd.missing
    otherwise (for example if obj has wrong type).
```

### `kd.functor.map(fn, *args, include_missing=DataItem(False, schema: BOOLEAN), **kwargs)` {#kd.functor.map}
Aliases:

- [kd.map](#kd.map)

``` {.no-copy}
Aligns fn and args/kwargs and calls corresponding fn on corresponding arg.

If certain items of `fn` are missing, the corresponding items of the result
will also be missing.
If certain items of `args`/`kwargs` are missing then:
- when include_missing=False (the default), the corresponding item of the
  result will be missing.
- when include_missing=True, we are still calling the functor on those missing
  args/kwargs.

`fn`, `args`, `kwargs` will all be broadcast to the common shape. The return
values of the functors will be converted to a common schema, or an exception
will be raised if the schemas are not compatible. In that case, you can add
the appropriate cast inside the functor.

Example:
  fn1 = kdf.fn(lambda x, y: x + y)
  fn2 = kdf.fn(lambda x, y: x - y)
  fn = kd.slice([fn1, fn2])
  x = kd.slice([[1, None, 3], [4, 5, 6]])
  y = kd.slice(1)

  kd.map(kd.slice([fn1, fn2]), x=x, y=y)
  # returns kd.slice([[2, None, 4], [3, 4, 5]])

  kd.map(kd.slice([fn1, None]), x=x, y=y)
  # returns kd.slice([[2, None, 4], [None, None, None]])

Args:
  fn: DataSlice containing the functor(s) to evaluate. All functors must
    return a DataItem.
  *args: The positional argument(s) to pass to the functors.
  include_missing: Whether to call the functors on missing items of
    args/kwargs.
  **kwargs: The keyword argument(s) to pass to the functors.

Returns:
  The evaluation result.
```

### `kd.functor.map_py_fn(f, *, schema=None, max_threads=1, ndim=0, include_missing=None, **defaults)` {#kd.functor.map_py_fn}

``` {.no-copy}
Returns a Koda functor wrapping a python function for kd.map_py.

  See kd.map_py for detailed APIs, and kd.py_fn for details about function
  wrapping. schema, max_threads and ndims cannot be Koda Expr or Koda functor.

  Args:
    f: Python function.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    ndim: Dimensionality of items to pass to `f`.
    include_missing: Specifies whether `f` applies to all items (`=True`) or
      only to items present in all `args` and `kwargs` (`=False`, valid only
      when `ndim=0`); defaults to `False` when `ndim=0`.
    **defaults: Keyword defaults to pass to the function. The values in this map
      may be kde expressions, format strings, or 0-dim DataSlices. See the
      docstring for py_fn for more details.
```

### `kd.functor.py_fn(f, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **defaults)` {#kd.functor.py_fn}
Aliases:

- [kd.py_fn](#kd.py_fn)

- [kd_ext.PyFn](#kd_ext.PyFn)

``` {.no-copy}
Returns a Koda functor wrapping a python function.

  This is the most flexible way to wrap a python function and is recommended
  for large, complex code.

  Functions wrapped with py_fn are not serializable.

  Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
  functor
  will have exactly the same signature as the original function. In particular,
  if the original function does not accept variadic keyword arguments and
  and unknown argument is passed when calling the functor, an exception will
  occur.

  Args:
    f: Python function. It is required that this function returns a
      DataSlice/DataItem or a primitive that will be automatically wrapped into
      a DataItem.
    return_type_as: The return type of the function is expected to be the same
      as the type of this value. This needs to be specified if the function does
      not return a DataSlice/DataItem or a primitive that would be auto-boxed
      into a DataItem. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kd.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
```

### `kd.functor.trace_as_fn(*, name=None, py_fn=False, return_type_as=None, wrapper=None)` {#kd.functor.trace_as_fn}
Aliases:

- [kd.trace_as_fn](#kd.trace_as_fn)

``` {.no-copy}
A decorator to customize the tracing behavior for a particular function.

  A function with this decorator is converted to an internally-stored functor.
  In traced expressions that call the function, that functor is invoked as a
  sub-functor via by 'kde.call', rather than the function being re-traced.
  Additionally, the functor passed to 'kde.call' is assigned a name, so that
  when auto_variables=True is used (which is the default in kd.trace_py_fn),
  the functor for the decorated function will become an attribute of the
  functor for the outer function being traced.
  The result of 'kde.call' is also assigned a name with a '_result' suffix, so
  that it also becomes an separate variable in the outer function being traced.
  This is useful for debugging, and also to use kd_ext.call_multithreaded.

  This can be used to avoid excessive re-tracing and recompilation of shared
  python functions, to quickly add structure to the functor produced by tracing
  for complex computations, or to conveniently embed a py_fn into a traced
  expression.

  This decorator is intended to be applied to standalone functions.

  When applying it to a lambda, consider specifying an explicit name, otherwise
  it will be called '<lambda>' or '<lambda>_0' etc, which is not very useful.

  When applying it to a class method, it is likely to fail in tracing mode
  because it will try to auto-box the class instance into an expr, which is
  likely not supported.

  If the function accepts or returns a type that is not supported by Koda
  natively, the corresponding argument/return value must be annotated with a
  type that has a _koladata_type_tracing_config_() classmethod that returns an
  instance of TypeTracingConfig to describe how to convert the value to/from
  Koda.

  Note that for _koladata_type_tracing_config_ to work, the file must _not_
  do "from __future__ import annotations", as that makes the type annotations
  unresolved at the decoration time.

  When executing the resulting function in eager mode, we will evaluate the
  underlying function directly instead of evaluating the functor, to have
  nicer stack traces in case of an exception. However, we will still apply
  the boxing rules on the returned value (for example, convert Python primitives
  to DataItems), and the to/from Koda conversions defined by
  _koladata_type_tracing_config_, if any, to better emulate what will happen in
  tracing
  mode.
```

### `kd.functor.trace_py_fn(f, *, auto_variables=True, **defaults)` {#kd.functor.trace_py_fn}
Aliases:

- [kd.trace_py_fn](#kd.trace_py_fn)

``` {.no-copy}
Returns a Koda functor created by tracing a given Python function.

  When 'f' has variadic positional (*args) or variadic keyword
  (**kwargs) arguments, their name must start with 'unused', and they
  must actually be unused inside 'f'.
  'f' must not use Python control flow operations such as if or for.

  Args:
    f: Python function.
    auto_variables: When true, we create additional variables automatically
      based on the traced expression. All DataSlice literals become their own
      variables, and all named subexpressions become their own variables. This
      helps readability and manipulation of the resulting functor. Note that
      this defaults to True here, while it defaults to False in
      kd.functor.expr_fn.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kd.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
```

</section>

### kd.ids {#kd.ids}

Operators that work with ItemIds.

<section class="zippy closed">

**Operators**

### `kd.ids.agg_uuid(x, ndim=unspecified)` {#kd.ids.agg_uuid}
Aliases:

- [kd.agg_uuid](#kd.agg_uuid)

``` {.no-copy}
Computes aggregated uuid of elements over the last `ndim` dimensions.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  DataSlice with that has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.
```

### `kd.ids.decode_itemid(ds)` {#kd.ids.decode_itemid}
Aliases:

- [kd.decode_itemid](#kd.decode_itemid)

``` {.no-copy}
Returns ItemIds decoded from the base62 strings.
```

### `kd.ids.deep_uuid(x, /, schema=unspecified, *, seed=DataItem('', schema: STRING))` {#kd.ids.deep_uuid}
Aliases:

- [kd.deep_uuid](#kd.deep_uuid)

``` {.no-copy}
Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve '*' and '**' tokens. If not specified,
    will use the schema of the 'x' DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.
```

### `kd.ids.encode_itemid(ds)` {#kd.ids.encode_itemid}
Aliases:

- [kd.encode_itemid](#kd.encode_itemid)

``` {.no-copy}
Returns the base62 encoded ItemIds in `ds` as strings.
```

### `kd.ids.hash_itemid(x)` {#kd.ids.hash_itemid}
Aliases:

- [kd.hash_itemid](#kd.hash_itemid)

``` {.no-copy}
Returns a INT64 DataSlice of hash values of `x`.

The hash values are in the range of [-2**63, 2**63-1].

The hash algorithm is subject to change. It is not guaranteed to be stable in
future releases.

Args:
  x: DataSlice of ItemIds.

Returns:
  A DataSlice of INT64 hash values.
```

### `kd.ids.uuid(seed=DataItem('', schema: STRING), **kwargs)` {#kd.ids.uuid}
Aliases:

- [kd.uuid](#kd.uuid)

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `kd.ids.uuid_for_dict(seed=DataItem('', schema: STRING), **kwargs)` {#kd.ids.uuid_for_dict}
Aliases:

- [kd.uuid_for_dict](#kd.uuid_for_dict)

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying dict items.

e.g.

kd.dict(['a', 'b'], [1, 2], itemid=kd.uuid_for_dict(seed='seed', a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `kd.ids.uuid_for_list(seed=DataItem('', schema: STRING), **kwargs)` {#kd.ids.uuid_for_list}
Aliases:

- [kd.uuid_for_list](#kd.uuid_for_list)

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying list items.

e.g.

kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed='seed', a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `kd.ids.uuids_with_allocation_size(seed=DataItem('', schema: STRING), *, size)` {#kd.ids.uuids_with_allocation_size}
Aliases:

- [kd.uuids_with_allocation_size](#kd.uuids_with_allocation_size)

``` {.no-copy}
Creates a DataSlice whose items are uuids.

The uuids are allocated in a single allocation. They are all distinct.
You can think of the result as a DataSlice created with:
[fingerprint(seed, size, i) for i in range(size)]

Args:
  seed: text seed for the uuid computation.
  size: the size of the allocation. It will also be used for the uuid
    computation.

Returns:
  A 1-dimensional DataSlice with `size` distinct uuids.
```

</section>

### kd.json {#kd.json}

JSON serialization operators.

<section class="zippy closed">

**Operators**

### `kd.json.from_json(x, /, schema=DataItem(OBJECT, schema: SCHEMA), default_number_schema=DataItem(OBJECT, schema: SCHEMA), *, on_invalid=DataSlice([], schema: NONE, ndims: 1, size: 0), keys_attr=DataItem('json_object_keys', schema: STRING), values_attr=DataItem('json_object_values', schema: STRING))` {#kd.json.from_json}
Aliases:

- [kd.from_json](#kd.from_json)

``` {.no-copy}
Parses a DataSlice `x` of JSON strings.

The result will have the same shape as `x`, and missing items in `x` will be
missing in the result. The result will use a new immutable DataBag.

If `schema` is OBJECT (the default), the schema is inferred from the JSON
data, and the result will have an OBJECT schema. The decoded data will only
have BOOLEAN, numeric, STRING, LIST[OBJECT], and entity schemas, corresponding
to JSON primitives, arrays, and objects.

If `default_number_schema` is OBJECT (the default), then the inferred schema
of each JSON number will be INT32, INT64, or FLOAT32, depending on its value
and on whether it contains a decimal point or exponent, matching the combined
behavior of python json and `kd.from_py`. Otherwise, `default_number_schema`
must be a numeric schema, and the inferred schema of all JSON numbers will be
that schema.

For example:

  kd.from_json(None) -> kd.obj(None)
  kd.from_json('null') -> kd.obj(None)
  kd.from_json('true') -> kd.obj(True)
  kd.from_json('[true, false, null]') -> kd.obj([True, False, None])
  kd.from_json('[1, 2.0]') -> kd.obj([1, 2.0])
  kd.from_json('[1, 2.0]', kd.OBJECT, kd.FLOAT64)
    -> kd.obj([kd.float64(1.0), kd.float64(2.0)])

JSON objects parsed using an OBJECT schema will record the object key order on
the attribute specified by `keys_attr` as a LIST[STRING], and also redundantly
record a copy of the object values as a parallel LIST on the attribute
specified by `values_attr`. If there are duplicate keys, the last value is the
one stored on the Koda object attribute. If a key conflicts with `keys_attr`
or `values_attr`, it is only available in the `values_attr` list. These
behaviors can be disabled by setting `keys_attr` and/or `values_attr` to None.

For example:

  kd.from_json('{"a": 1, "b": "y", "c": null}') ->
      kd.obj(a=1.0, b='y', c=None,
             json_object_keys=kd.list(['a', 'b', 'c']),
             json_object_values=kd.list([1.0, 'y', None]))
  kd.from_json('{"a": 1, "b": "y", "c": null}',
               keys_attr=None, values_attr=None) ->
      kd.obj(a=1.0, b='y', c=None)
  kd.from_json('{"a": 1, "b": "y", "c": null}',
               keys_attr='my_keys', values_attr='my_values') ->
      kd.obj(a=1.0, b='y', c=None,
             my_keys=kd.list(['a', 'b', 'c']),
             my_values=kd.list([1.0, 'y', None]))
  kd.from_json('{"a": 1, "a": 2", "a": 3}') ->
      kd.obj(a=3.0,
             json_object_keys=kd.list(['a', 'a', 'a']),
             json_object_values=kd.list([1.0, 2.0, 3.0]))
  kd.from_json('{"json_object_keys": ["x", "y"]}') ->
      kd.obj(json_object_keys=kd.list(['json_object_keys']),
             json_object_values=kd.list([["x", "y"]]))

If `schema` is explicitly specified, it is used to validate the JSON data,
and the result DataSlice will have `schema` as its schema.

OBJECT schemas inside subtrees of `schema` are allowed, and will use the
inference behavior described above.

Primitive schemas in `schema` will attempt to cast any JSON primitives using
normal Koda explicit casting rules.

If entity schemas in `schema` have attributes matching `keys_attr` and/or
`values_attr`, then the object key and/or value order (respectively) will be
recorded as lists on those attributes, similar to the behavior for OBJECT
described above. These attributes must have schemas LIST[STRING] and
LIST[T] (for a T compatible with the contained values) if present.

For example:

  kd.from_json('null', kd.MASK) -> kd.missing
  kd.from_json('null', kd.STRING) -> kd.str(None)
  kd.from_json('123', kd.INT32) -> kd.int32(123)
  kd.from_json('123', kd.FLOAT32) -> kd.int32(123.0)
  kd.from_json('"123"', kd.STRING) -> kd.string('123')
  kd.from_json('"123"', kd.INT32) -> kd.int32(123)
  kd.from_json('"123"', kd.FLOAT32) -> kd.float32(123.0)
  kd.from_json('"inf"', kd.FLOAT32) -> kd.float32(float('inf'))
  kd.from_json('"1e100"', kd.FLOAT32) -> kd.float32(float('inf'))
  kd.from_json('[1, 2, 3]', kd.list_schema(kd.INT32)) -> kd.list([1, 2, 3])
  kd.from_json('{"a": 1}', kd.schema.new_schema(a=kd.INT32)) -> kd.new(a=1)
  kd.from_json('{"a": 1}', kd.dict_schema(kd.STRING, kd.INT32)
    -> kd.dict({"a": 1})

  kd.from_json('{"b": 1, "a": 2}',
               kd.new_schema(
                   a=kd.INT32, json_object_keys=kd.list_schema(kd.STRING))) ->
    kd.new(a=1, json_object_keys=kd.list(['b', 'a', 'c']))
  kd.from_json('{"b": 1, "a": 2, "c": 3}',
               kd.new_schema(a=kd.INT32,
                             json_object_keys=kd.list_schema(kd.STRING),
                             json_object_values=kd.list_schema(kd.OBJECT))) ->
    kd.new(a=1, c=3.0,
           json_object_keys=kd.list(['b', 'a', 'c']),
           json_object_values=kd.list([1, 2.0, 3.0]))

In general:

  `kd.to_json(kd.from_json(x))` is equivalent to `x`, ignoring differences in
  JSON number formatting and padding.

  `kd.from_json(kd.to_json(x), kd.get_schema(x))` is equivalent to `x` if `x`
  has a concrete (no OBJECT) schema, ignoring differences in Koda itemids.
  In other words, `to_json` doesn't capture the full information of `x`, but
  the original schema of `x` has enough additional information to recover it.

Args:
  x: A DataSlice of STRING containing JSON strings to parse.
  schema: A SCHEMA DataItem containing the desired result schema. Defaults to
    kd.OBJECT.
  default_number_schema: A SCHEMA DataItem containing a numeric schema, or
    None to infer all number schemas using python-boxing-like rules.
  on_invalid: If specified, a DataItem to use in the result wherever the
    corresponding JSON string in `x` was invalid. If unspecified, any invalid
    JSON strings in `x` will cause an operator error.
  keys_attr: A STRING DataItem that controls which entity attribute is used to
    record json object key order, if it is present on the schema.
  values_attr: A STRING DataItem that controls which entity attribute is used
    to record json object values, if it is present on the schema.

Returns:
  A DataSlice with the same shape as `x` and schema `schema`.
```

### `kd.json.to_json(x, /, *, indent=DataItem(None, schema: NONE), ensure_ascii=DataItem(True, schema: BOOLEAN), keys_attr=DataItem('json_object_keys', schema: STRING), values_attr=DataItem('json_object_values', schema: STRING))` {#kd.json.to_json}
Aliases:

- [kd.to_json](#kd.to_json)

``` {.no-copy}
Converts `x` to a DataSlice of JSON strings.

Data with STRING, numeric, MASK, BOOLEAN, LIST, STRING-key DICT, and entity
schemas are allowed, along with OBJECT schemas that resolve to those schemas.
Itemid cycles are not allowed.

Missing DataSlice items in the input are missing in the result. Missing values
inside of lists/entities/etc. are encoded as JSON `null`, except for
`kd.missing`, which is encoded as `false`.

For example:

  kd.to_json(None) -> kd.str(None)
  kd.to_json(kd.missing) -> kd.str(None)
  kd.to_json(kd.present) -> 'true'
  kd.to_json(True) -> 'true'
  kd.to_json(kd.slice([1, None, 3])) -> ['1', None, '3']
  kd.to_json(kd.list([1, None, 3])) -> '[1, null, 3]'
  kd.to_json(kd.dict({'a': 1, 'b':'2'}) -> '{"a": 1, "b": "2"}'
  kd.to_json(kd.new(a=1, b='2')) -> '{"a": 1, "b": "2"}'
  kd.to_json(kd.new(x=None)) -> '{"x": null}'
  kd.to_json(kd.new(x=kd.missing)) -> '{"x": false}'

Integers are always stored exactly in decimal. Finite floating point values
are formatted similar to python format string `%.17g`, except that a decimal
point and at least one decimal digit are always present if the format doesn't
use scientific notation. This appears to match the behavior of python json.

Non-finite floating point values are stored as the strings "inf", "-inf" and
"nan". This differs from python json, which emits non-standard JSON tokens
`Infinity` and `NaN`. This also differs from javascript, which stores these
values as `null`, which would be ambiguous with Koda missing values. There is
unfortunately no standard way to express these values in JSON.

By default, JSON objects are written with keys in sorted order. However, it is
also possible to control the key order of JSON objects using the `keys_attr`
argument. If an entity has the attribute specified by `keys_attr`, then that
attribute must have schema LIST[STRING], and the JSON object will have exactly
the key order specified in that list, including duplicate keys.

To write duplicate JSON object keys with different values, use `values_attr`
to designate an attribute to hold a parallel list of values to write.

For example:

  kd.to_json(kd.new(x=1, y=2)) -> '{"x": 2, "y": 1}'
  kd.to_json(kd.new(x=1, y=2, json_object_keys=kd.list(['y', 'x'])))
    -> '{"y": 2, "x": 1}'
  kd.to_json(kd.new(x=1, y=2, foo=kd.list(['y', 'x'])), keys_attr='foo')
    -> '{"y": 2, "x": 1}'
  kd.to_json(kd.new(x=1, y=2, z=3, json_object_keys=kd.list(['x', 'z', 'x'])))
    -> '{"x": 1, "z": 3, "x": 1}'

  kd.to_json(kd.new(json_object_keys=kd.list(['x', 'z', 'x']),
                    json_object_values=kd.list([1, 2, 3])))
    -> '{"x": 1, "z": 2, "x": 3}'
  kd.to_json(kd.new(a=kd.list(['x', 'z', 'x']), b=kd.list([1, 2, 3])),
             keys_attr='a', values_attr='b')
    -> '{"x": 1, "z": 2, "x": 3}'


The `indent` and `ensure_ascii` arguments control JSON formatting:
- If `indent` is negative, then the JSON is formatted without any whitespace.
- If `indent` is None (the default), the JSON is formatted with a single
  padding space only after ',' and ':' and no other whitespace.
- If `indent` is zero or positive, the JSON is pretty-printed, with that
  number of spaces used for indenting each level.
- If `ensure_ascii` is True (the default) then all non-ASCII code points in
  strings will be escaped, and the result strings will be ASCII-only.
  Otherwise, they will be left as-is.

For example:

  kd.to_json(kd.list([1, 2, 3]), indent=-1) -> '[1,2,3]'
  kd.to_json(kd.list([1, 2, 3]), indent=2) -> '[\n  1,\n  2,\n  3\n]'

  kd.to_json('✨', ensure_ascii=True) -> '"\\u2728"'
  kd.to_json('✨', ensure_ascii=False) -> '"✨"'

Args:
  x: The DataSlice to convert.
  indent: An INT32 DataItem that describes how the result should be indented.
  ensure_ascii: A BOOLEAN DataItem that controls non-ASCII escaping.
  keys_attr: A STRING DataItem that controls which entity attribute controls
    json object key order, or None to always use sorted order. Defaults to
    `json_object_keys`.
  values_attr: A STRING DataItem that can be used with `keys_attr` to give
    full control over json object contents. Defaults to `json_object_values`.
```

</section>

### kd.lists {#kd.lists}

Operators working with lists.

<section class="zippy closed">

**Operators**

### `kd.lists.appended_list(x, append)` {#kd.lists.appended_list}
Aliases:

- [kd.appended_list](#kd.appended_list)

``` {.no-copy}
Appends items in `append` to the end of each list in `x`.

`x` and `append` must have compatible shapes.

The resulting lists have different ItemIds from the original lists.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  DataSlice of lists with new itemd ids in a new immutable DataBag.
```

### `kd.lists.concat(*lists, db=None)` {#kd.lists.concat}
Aliases:

- [kd.concat_lists](#kd.concat_lists)

``` {.no-copy}
Returns a DataSlice of Lists concatenated from the List items of `lists`.

  Returned lists are immutable if `db` is not provided.

  Each input DataSlice must contain only present List items, and the item
  schemas of each input must be compatible. Input DataSlices are aligned (see
  `kd.align`) automatically before concatenation.

  If `lists` is empty, this returns a single empty list with OBJECT item schema.

  The specified `db` is used to create the new concatenated lists, and is the
  DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
  is created for this purpose.

  Args:
    *lists: the DataSlices of Lists to concatenate
    db: optional DataBag to populate with the result

  Returns:
    DataSlice of concatenated Lists
```

### `kd.lists.concat_lists(arg0, *args)` {#kd.lists.concat_lists}

``` {.no-copy}
Implementation of kde.lists.concat_lists.
```

### `kd.lists.explode(x, ndim=DataItem(1, schema: INT64))` {#kd.lists.explode}
Aliases:

- [kd.explode](#kd.explode)

``` {.no-copy}
Explodes a List DataSlice `x` a specified number of times.

A single list "explosion" converts a rank-K DataSlice of LIST[T] to a
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
  DataSlice
```

### `kd.lists.get_item(x, key_or_index)` {#kd.lists.get_item}

Alias for [kd.core.get_item](#kd.core.get_item) operator.

### `kd.lists.has_list(x)` {#kd.lists.has_list}
Aliases:

- [kd.has_list](#kd.has_list)

``` {.no-copy}
Returns present for each item in `x` that is List.

Note that this is a pointwise operation.

Also see `kd.is_list` for checking if `x` is a List DataSlice. But note that
`kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
example,

  kd.is_list(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_list(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.
```

### `kd.lists.implode(x, /, ndim=1, itemid=None, db=None)` {#kd.lists.implode}
Aliases:

- [kd.implode](#kd.implode)

``` {.no-copy}
Implodes a Dataslice `x` a specified number of times.

  Returned lists are immutable if `db` is not provided.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  A single list implosion is equivalent to `kd.list(x, db)`.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  The specified `db` is used to create any new Lists, and is the DataBag of the
  result DataSlice. If `db` is not specified, a new DataBag is created for this
  purpose.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where Lists are created from

  Returns:
    DataSlice of nested Lists
```

### `kd.lists.is_list(x)` {#kd.lists.is_list}
Aliases:

- [kd.is_list](#kd.is_list)

``` {.no-copy}
Returns whether x is a List DataSlice.

`x` is a List DataSlice if it meets one of the following conditions:
  1) it has a List schema
  2) it has OBJECT/ANY schema and only has List items

Also see `kd.has_list` for a pointwise version. But note that
`kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
example,

  kd.is_list(kd.item(None, kd.OBJECT)) -> kd.present
  kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -> invalid for kd.all
  kd.is_list(kd.item([None], kd.OBJECT)) -> kd.present
  kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.
```

### `kd.lists.like(shape_and_mask_from, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.lists.like}
Aliases:

- [kd.list_like](#kd.list_like)

``` {.no-copy}
Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Returns immutable lists if `db` is not provided.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired lists.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `kd.lists.list_append_update(x, append)` {#kd.lists.list_append_update}
Aliases:

- [kd.list_append_update](#kd.list_append_update)

``` {.no-copy}
Returns a DataBag containing an update to a DataSlice of lists.

The updated lists are the lists in `x` with the specified items appended at
the end.

`x` and `append` must have compatible shapes.

The resulting lists maintain the same ItemIds. Also see kd.appended_list()
which works similarly but resulting lists have new ItemIds.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  A new immutable DataBag containing the list with the appended items.
```

### `kd.lists.select_items(ds, fltr)` {#kd.lists.select_items}
Aliases:

- [kd.select_items](#kd.select_items)

``` {.no-copy}
Selects List items by filtering out missing items in fltr.

Also see kd.select.

Args:
  ds: List DataSlice to be filtered
  fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
    Functor or a Python function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `kd.lists.shaped(shape, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.lists.shaped}
Aliases:

- [kd.list_shaped](#kd.list_shaped)

``` {.no-copy}
Creates new Koda lists with the given shape.

  Returns immutable lists if `db` is not provided.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `kd.lists.shaped_as(shape_from, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.lists.shaped_as}
Aliases:

- [kd.list_shaped_as](#kd.list_shaped_as)

``` {.no-copy}
Creates new Koda lists with shape of the given DataSlice.

  Returns immutable lists if `db` is not provided.

  Args:
    shape_from: mandatory DataSlice, whose shape the returned DataSlice will
      have.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `kd.lists.size(list_slice)` {#kd.lists.size}
Aliases:

- [kd.list_size](#kd.list_size)

``` {.no-copy}
Returns size of a List.
```

### `kd.lists.with_list_append_update(x, append)` {#kd.lists.with_list_append_update}
Aliases:

- [kd.with_list_append_update](#kd.with_list_append_update)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated appended lists.

The updated lists are the lists in `x` with the specified items appended at
the end.

`x` and `append` must have compatible shapes.

The resulting lists maintain the same ItemIds. Also see kd.appended_list()
which works similarly but resulting lists have new ItemIds.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  A DataSlice of lists in a new immutable DataBag.
```

</section>

### kd.masking {#kd.masking}

Masking operators.

<section class="zippy closed">

**Operators**

### `kd.masking.agg_all(x, ndim=unspecified)` {#kd.masking.agg_all}
Aliases:

- [kd.agg_all](#kd.agg_all)

``` {.no-copy}
Returns present if all elements are present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.masking.agg_any(x, ndim=unspecified)` {#kd.masking.agg_any}
Aliases:

- [kd.agg_any](#kd.agg_any)

``` {.no-copy}
Returns present if any element is present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.masking.agg_has(x, ndim=unspecified)` {#kd.masking.agg_has}
Aliases:

- [kd.agg_has](#kd.agg_has)

``` {.no-copy}
Returns present iff any element is present along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

It is equivalent to `kd.agg_any(kd.has(x))`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.masking.all(x)` {#kd.masking.all}
Aliases:

- [kd.all](#kd.all)

``` {.no-copy}
Returns present iff all elements are present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.
```

### `kd.masking.any(x)` {#kd.masking.any}
Aliases:

- [kd.any](#kd.any)

``` {.no-copy}
Returns present iff any element is present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.
```

### `kd.masking.apply_mask(x, y)` {#kd.masking.apply_mask}
Aliases:

- [kd.apply_mask](#kd.apply_mask)

``` {.no-copy}
Filters `x` to items where `y` is present.

Pointwise masking operator that replaces items in DataSlice `x` by None
if corresponding items in DataSlice `y` of MASK dtype is `kd.missing`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  Masked DataSlice.
```

### `kd.masking.coalesce(x, y)` {#kd.masking.coalesce}
Aliases:

- [kd.coalesce](#kd.coalesce)

``` {.no-copy}
Fills in missing values of `x` with values of `y`.

Pointwise masking operator that replaces missing items (i.e. None) in
DataSlice `x` by corresponding items in DataSlice y`.
`x` and `y` do not need to have the same type.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.
```

### `kd.masking.cond(condition, yes, no=DataItem(None, schema: NONE))` {#kd.masking.cond}
Aliases:

- [kd.cond](#kd.cond)

``` {.no-copy}
Returns `yes` where `condition` is present, otherwise `no`.

Pointwise operator selects items in `yes` if corresponding items are
`kd.present` or items in `no` otherwise. `condition` must have MASK dtype.

If `no` is unspecified corresponding items in result are missing.

Args:
  condition: DataSlice.
  yes: DataSlice.
  no: DataSlice or unspecified.

Returns:
  DataSlice of items from `yes` and `no` based on `condition`.
```

### `kd.masking.disjoint_coalesce(x, y)` {#kd.masking.disjoint_coalesce}
Aliases:

- [kd.disjoint_coalesce](#kd.disjoint_coalesce)

``` {.no-copy}
Fills in missing values of `x` with values of `y`.

Raises if `x` and `y` intersect. It is equivalent to `x | y` with additional
assertion that `x` and `y` are disjoint.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.
```

### `kd.masking.empty_shaped(shape, /, *, schema=DataItem(MASK, schema: SCHEMA), db=None)` {#kd.masking.empty_shaped}
Aliases:

- [kd.empty_shaped](#kd.empty_shaped)

``` {.no-copy}
Creates a DataSlice of missing items with the given shape.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape: Shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the given shape.
```

### `kd.masking.empty_shaped_as(shape_from, /, *, schema=DataItem(MASK, schema: SCHEMA), db=None)` {#kd.masking.empty_shaped_as}
Aliases:

- [kd.empty_shaped_as](#kd.empty_shaped_as)

``` {.no-copy}
Creates a DataSlice of missing items with the shape of `shape_from`.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape_from: used for the shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the shape of the given DataSlice.
```

### `kd.masking.has(x)` {#kd.masking.has}
Aliases:

- [kd.has](#kd.has)

``` {.no-copy}
Returns presence of `x`.

Pointwise operator which take a DataSlice and return a MASK indicating the
presence of each item in `x`. Returns `kd.present` for present items and
`kd.missing` for missing items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the presence of `x`.
```

### `kd.masking.has_not(x)` {#kd.masking.has_not}
Aliases:

- [kd.has_not](#kd.has_not)

``` {.no-copy}
Returns present iff `x` is missing element-wise.

Pointwise operator which take a DataSlice and return a MASK indicating
iff `x` is missing element-wise. Returns `kd.present` for missing
items and `kd.missing` for present items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the non-presence of `x`.
```

### `kd.masking.mask_and(x, y)` {#kd.masking.mask_and}
Aliases:

- [kd.mask_and](#kd.mask_and)

``` {.no-copy}
Applies pointwise MASK_AND operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_AND operation is defined as:
  kd.mask_and(kd.present, kd.present) -> kd.present
  kd.mask_and(kd.present, kd.missing) -> kd.missing
  kd.mask_and(kd.missing, kd.present) -> kd.missing
  kd.mask_and(kd.missing, kd.missing) -> kd.missing

It is equivalent to `x & y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `kd.masking.mask_equal(x, y)` {#kd.masking.mask_equal}
Aliases:

- [kd.mask_equal](#kd.mask_equal)

``` {.no-copy}
Applies pointwise MASK_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_EQUAL operation is defined as:
  kd.mask_equal(kd.present, kd.present) -> kd.present
  kd.mask_equal(kd.present, kd.missing) -> kd.missing
  kd.mask_equal(kd.missing, kd.present) -> kd.missing
  kd.mask_equal(kd.missing, kd.missing) -> kd.present

Note that this is different from `x == y`. For example,
  kd.missing == kd.missing -> kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `kd.masking.mask_not_equal(x, y)` {#kd.masking.mask_not_equal}
Aliases:

- [kd.mask_not_equal](#kd.mask_not_equal)

``` {.no-copy}
Applies pointwise MASK_NOT_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_NOT_EQUAL operation is defined as:
  kd.mask_not_equal(kd.present, kd.present) -> kd.missing
  kd.mask_not_equal(kd.present, kd.missing) -> kd.present
  kd.mask_not_equal(kd.missing, kd.present) -> kd.present
  kd.mask_not_equal(kd.missing, kd.missing) -> kd.missing

Note that this is different from `x != y`. For example,
  kd.present != kd.missing -> kd.missing
  kd.missing != kd.present -> kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `kd.masking.mask_or(x, y)` {#kd.masking.mask_or}

``` {.no-copy}
Applies pointwise MASK_OR operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_OR operation is defined as:
  kd.mask_or(kd.present, kd.present) -> kd.present
  kd.mask_or(kd.present, kd.missing) -> kd.present
  kd.mask_or(kd.missing, kd.present) -> kd.present
  kd.mask_or(kd.missing, kd.missing) -> kd.missing

It is equivalent to `x | y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `kd.masking.present_like(x)` {#kd.masking.present_like}
Aliases:

- [kd.present_like](#kd.present_like)

``` {.no-copy}
Creates a DataSlice of present masks with the shape and sparsity of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.present_like(x) -> kd.slice([[present], [present, None]])

Args:
  x: DataSlice to match the shape and sparsity of.

Returns:
  A DataSlice with the same shape and sparsity as `x`.
```

### `kd.masking.present_shaped(shape)` {#kd.masking.present_shaped}
Aliases:

- [kd.present_shaped](#kd.present_shaped)

``` {.no-copy}
Creates a DataSlice of present masks with the given shape.

Example:
  shape = kd.shapes.new([2], [1, 2])
  kd.masking.present_shaped(shape) -> kd.slice([[present], [present,
  present]])

Args:
  shape: shape to expand to.

Returns:
  A DataSlice with the same shape as `shape`.
```

### `kd.masking.present_shaped_as(x)` {#kd.masking.present_shaped_as}
Aliases:

- [kd.present_shaped_as](#kd.present_shaped_as)

``` {.no-copy}
Creates a DataSlice of present masks with the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.masking.present_shaped_as(x) -> kd.slice([[present], [present, present]])

Args:
  x: DataSlice to match the shape of.

Returns:
  A DataSlice with the same shape as `x`.
```

</section>

### kd.math {#kd.math}

Arithmetic operators.

<section class="zippy closed">

**Operators**

### `kd.math.abs(x)` {#kd.math.abs}

``` {.no-copy}
Computes pointwise absolute value of the input.
```

### `kd.math.add(x, y)` {#kd.math.add}
Aliases:

- [kd.add](#kd.add)

``` {.no-copy}
Computes pointwise x + y.
```

### `kd.math.agg_inverse_cdf(x, cdf_arg, ndim=unspecified)` {#kd.math.agg_inverse_cdf}

``` {.no-copy}
Returns the value with CDF (in [0, 1]) approximately equal to the input.

The value is computed along the last ndim dimensions.

The return value will have an offset of floor((cdf - 1e-6) * size()) in the
(ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.
  ndim: The number of dimensions to compute inverse CDF over. Requires 0 <=
    ndim <= get_ndim(x).
```

### `kd.math.agg_max(x, ndim=unspecified)` {#kd.math.agg_max}
Aliases:

- [kd.agg_max](#kd.agg_max)

``` {.no-copy}
Returns the maximum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_max(ds)  # -> kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=1)  # -> kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=2)  # -> kd.slice(4)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_mean(x, ndim=unspecified)` {#kd.math.agg_mean}

``` {.no-copy}
Returns the means along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, None], [3, 4], [None, None]])
  kd.agg_mean(ds)  # -> kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=1)  # -> kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=2)  # -> kd.slice(2.6666666666666) # (1 + 3 + 4) / 3)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_median(x, ndim=unspecified)` {#kd.math.agg_median}

``` {.no-copy}
Returns the medians along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_min(x, ndim=unspecified)` {#kd.math.agg_min}
Aliases:

- [kd.agg_min](#kd.agg_min)

``` {.no-copy}
Returns the minimum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_min(ds)  # -> kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=1)  # -> kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=2)  # -> kd.slice(1)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_std(x, unbiased=DataItem(True, schema: BOOLEAN), ndim=unspecified)` {#kd.math.agg_std}

``` {.no-copy}
Returns the standard deviation along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_std(ds)  # -> kd.slice(1.0)
  kd.agg_std(ds, unbiased=False)  # -> kd.slice(0.8164966)

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_sum(x, ndim=unspecified)` {#kd.math.agg_sum}
Aliases:

- [kd.agg_sum](#kd.agg_sum)

``` {.no-copy}
Returns the sums along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4], [None, None]])
  kd.agg_sum(ds)  # -> kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=1)  # -> kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=2)  # -> kd.slice(9)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.agg_var(x, unbiased=DataItem(True, schema: BOOLEAN), ndim=unspecified)` {#kd.math.agg_var}

``` {.no-copy}
Returns the variance along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_var(ds)  # -> kd.slice(1.0)
  kd.agg_var(ds, unbiased=False)  # -> kd.slice([0.6666667])

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.math.cdf(x, weights=unspecified, ndim=unspecified)` {#kd.math.cdf}

``` {.no-copy}
Returns the CDF of x in the last ndim dimensions of x element-wise.

The CDF is an array of floating-point values of the same shape as x and
weights, where each element represents which percentile the corresponding
element in x is situated at in its sorted group, i.e. the percentage of values
in the group that are smaller than or equal to it.

Args:
  x: a DataSlice of numbers.
  weights: if provided, will compute weighted CDF: each output value will
    correspond to the weight percentage of values smaller than or equal to x.
  ndim: The number of dimensions to compute CDF over.
```

### `kd.math.ceil(x)` {#kd.math.ceil}

``` {.no-copy}
Computes pointwise ceiling of the input, e.g.

rounding up: returns the smallest integer value that is not less than the
input.
```

### `kd.math.cum_max(x, ndim=unspecified)` {#kd.math.cum_max}
Aliases:

- [kd.cum_max](#kd.cum_max)

``` {.no-copy}
Returns the cumulative max of items along the last ndim dimensions.
```

### `kd.math.cum_min(x, ndim=unspecified)` {#kd.math.cum_min}

``` {.no-copy}
Returns the cumulative minimum of items along the last ndim dimensions.
```

### `kd.math.cum_sum(x, ndim=unspecified)` {#kd.math.cum_sum}

``` {.no-copy}
Returns the cumulative sum of items along the last ndim dimensions.
```

### `kd.math.divide(x, y)` {#kd.math.divide}

``` {.no-copy}
Computes pointwise x / y.
```

### `kd.math.exp(x)` {#kd.math.exp}

``` {.no-copy}
Computes pointwise exponential of the input.
```

### `kd.math.floor(x)` {#kd.math.floor}

``` {.no-copy}
Computes pointwise floor of the input, e.g.

rounding down: returns the largest integer value that is not greater than the
input.
```

### `kd.math.floordiv(x, y)` {#kd.math.floordiv}

``` {.no-copy}
Computes pointwise x // y.
```

### `kd.math.inverse_cdf(x, cdf_arg)` {#kd.math.inverse_cdf}

``` {.no-copy}
Returns the value with CDF (in [0, 1]) approximately equal to the input.

The return value is computed over all dimensions. It will have an offset of
floor((cdf - 1e-6) * size()) in the (ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.
```

### `kd.math.log(x)` {#kd.math.log}

``` {.no-copy}
Computes pointwise natural logarithm of the input.
```

### `kd.math.log10(x)` {#kd.math.log10}

``` {.no-copy}
Computes pointwise logarithm in base 10 of the input.
```

### `kd.math.max(x)` {#kd.math.max}
Aliases:

- [kd.max](#kd.max)

``` {.no-copy}
Returns the maximum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `kd.math.maximum(x, y)` {#kd.math.maximum}
Aliases:

- [kd.maximum](#kd.maximum)

``` {.no-copy}
Computes pointwise max(x, y).
```

### `kd.math.mean(x)` {#kd.math.mean}

``` {.no-copy}
Returns the mean of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `kd.math.median(x)` {#kd.math.median}

``` {.no-copy}
Returns the median of elements over all dimensions.

The result is a zero-dimensional DataItem.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
```

### `kd.math.min(x)` {#kd.math.min}
Aliases:

- [kd.min](#kd.min)

``` {.no-copy}
Returns the minimum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `kd.math.minimum(x, y)` {#kd.math.minimum}
Aliases:

- [kd.minimum](#kd.minimum)

``` {.no-copy}
Computes pointwise min(x, y).
```

### `kd.math.mod(x, y)` {#kd.math.mod}

``` {.no-copy}
Computes pointwise x % y.
```

### `kd.math.multiply(x, y)` {#kd.math.multiply}

``` {.no-copy}
Computes pointwise x * y.
```

### `kd.math.neg(x)` {#kd.math.neg}

``` {.no-copy}
Computes pointwise negation of the input, i.e. -x.
```

### `kd.math.pos(x)` {#kd.math.pos}

``` {.no-copy}
Computes pointwise positive of the input, i.e. +x.
```

### `kd.math.pow(x, y)` {#kd.math.pow}

``` {.no-copy}
Computes pointwise x ** y.
```

### `kd.math.round(x)` {#kd.math.round}

``` {.no-copy}
Computes pointwise rounding of the input.

Please note that this is NOT bankers rounding, unlike Python built-in or
Tensorflow round(). If the first decimal is exactly  0.5, the result is
rounded to the number with a higher absolute value:
round(1.4) == 1.0
round(1.5) == 2.0
round(1.6) == 2.0
round(2.5) == 3.0 # not 2.0
round(-1.4) == -1.0
round(-1.5) == -2.0
round(-1.6) == -2.0
round(-2.5) == -3.0 # not -2.0
```

### `kd.math.sigmoid(x, half=DataItem(0.0, schema: FLOAT32), slope=DataItem(1.0, schema: FLOAT32))` {#kd.math.sigmoid}

``` {.no-copy}
Computes sigmoid of the input.

sigmoid(x) = 1 / (1 + exp(-slope * (x - half)))

Args:
  x: A DataSlice of numbers.
  half: A DataSlice of numbers.
  slope: A DataSlice of numbers.

Return:
  sigmoid(x) computed with the formula above.
```

### `kd.math.sign(x)` {#kd.math.sign}

``` {.no-copy}
Computes the sign of the input.

Args:
  x: A DataSlice of numbers.

Returns:
  A dataslice of with {-1, 0, 1} of the same shape and type as the input.
```

### `kd.math.softmax(x, beta=DataItem(1.0, schema: FLOAT32), ndim=unspecified)` {#kd.math.softmax}

``` {.no-copy}
Returns the softmax of x alon the last ndim dimensions.

The softmax represents Exp(x * beta) / Sum(Exp(x * beta)) over last ndim
dimensions of x.

Args:
  x: An array of numbers.
  beta: A floating point scalar number that controls the smooth of the
    softmax.
  ndim: The number of last dimensions to compute softmax over.
```

### `kd.math.subtract(x, y)` {#kd.math.subtract}

``` {.no-copy}
Computes pointwise x - y.
```

### `kd.math.sum(x)` {#kd.math.sum}
Aliases:

- [kd.sum](#kd.sum)

``` {.no-copy}
Returns the sum of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

</section>

### kd.objs {#kd.objs}

Operators that work solely with objects.

<section class="zippy closed">

**Operators**

### `kd.objs.like(shape_and_mask_from, /, *, itemid=None, db=None, **attrs)` {#kd.objs.like}
Aliases:

- [kd.obj_like](#kd.obj_like)

``` {.no-copy}
Creates Objects with shape and sparsity from shape_and_mask_from.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.objs.new(arg=unspecified, /, *, itemid=None, db=None, **attrs)` {#kd.objs.new}
Aliases:

- [kd.obj](#kd.obj)

``` {.no-copy}
Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    arg: optional Python object to be converted to an Object.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
      itemid will only be set when the args is not a primitive or primitive
      slice if args presents.
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `kd.objs.shaped(shape, /, *, itemid=None, db=None, **attrs)` {#kd.objs.shaped}
Aliases:

- [kd.obj_shaped](#kd.obj_shaped)

``` {.no-copy}
Creates Objects with the given shape.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.objs.shaped_as(shape_from, /, *, itemid=None, db=None, **attrs)` {#kd.objs.shaped_as}
Aliases:

- [kd.obj_shaped_as](#kd.obj_shaped_as)

``` {.no-copy}
Creates Objects with the shape of the given DataSlice.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `kd.objs.uu(seed=None, *, db=None, **attrs)` {#kd.objs.uu}
Aliases:

- [kd.uuobj](#kd.uuobj)

``` {.no-copy}
Creates object(s) whose ids are uuid(s) with the provided attributes.

  Returned DataSlice has OBJECT schema and is immutable if `db` is not provided.

  In order to create a different "Type" from the same arguments, use
  `seed` key with the desired value, e.g.

  kd.uuobj(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

  and

  kd.uuobj(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

  have different ids.

  Args:
    seed: (str) Allows different uuobj(s) to have different ids when created
      from the same inputs.
    db: optional DataBag where entities are created.
    **attrs: key-value pairs of object attributes where values are DataSlices
      or can be converted to DataSlices using kd.new / kd.obj.

  Returns:
    data_slice.DataSlice
```

</section>

### kd.py {#kd.py}

Operators that call Python functions.

<section class="zippy closed">

**Operators**

### `kd.py.apply_py(fn, *args, return_type_as=unspecified, **kwargs)` {#kd.py.apply_py}
Aliases:

- [kd.apply_py](#kd.apply_py)

``` {.no-copy}
Applies Python function `fn` on args.

It is equivalent to fn(*args, **kwargs).

Args:
  fn: function to apply to `*args` and `**kwargs`. It is required that this
    function returns a DataSlice/DataItem or a primitive that will be
    automatically wrapped into a DataItem.
  *args: positional arguments to pass to `fn`.
  return_type_as: The return type of the function is expected to be the same
    as the return type of this expression. In most cases, this will be a
    literal of the corresponding type. This needs to be specified if the
    function does not return a DataSlice/DataItem or a primitive that would be
    auto-boxed into a DataItem. kd.types.DataSlice and kd.types.DataBag can
    also be passed here.
  **kwargs: keyword arguments to pass to `fn`.

Returns:
  Result of fn applied on the arguments.
```

### `kd.py.apply_py_on_cond(yes_fn, no_fn, cond, *args, **kwargs)` {#kd.py.apply_py_on_cond}
Aliases:

- [kd.apply_py_on_cond](#kd.apply_py_on_cond)

``` {.no-copy}
Applies Python functions on args filtered with `cond` and `~cond`.

It is equivalent to

  yes_fn(
      *( x & cond for x in args ),
      **{ k: (v & cond) for k, v in kwargs.items() },
  ) | no_fn(
      *( x & ~cond for x in args ),
      **{ k: (v & ~cond) for k, v in kwargs.items() },
  )

Args:
  yes_fn: function to apply on filtered args.
  no_fn: function to apply on inverse filtered args (this parameter can be
    None).
  cond: filter dataslice.
  *args: arguments to filter and then pass to yes_fn and no_fn.
  **kwargs: keyword arguments to filter and then pass to yes_fn and no_fn.

Returns:
  The union of results of yes_fn and no_fn applied on filtered args.
```

### `kd.py.apply_py_on_selected(fn, cond, *args, **kwargs)` {#kd.py.apply_py_on_selected}
Aliases:

- [kd.apply_py_on_selected](#kd.apply_py_on_selected)

``` {.no-copy}
Applies Python function `fn` on args filtered with cond.

It is equivalent to

  fn(
      *( x & cond for x in args ),
      **{ k: (v & cond) for k, v in kwargs.items() },
  )

Args:
  fn: function to apply on filtered args.
  cond: filter dataslice.
  *args: arguments to filter and then pass to fn.
  **kwargs: keyword arguments to filter and then pass to fn.

Returns:
  Result of fn applied on filtered args.
```

### `kd.py.map_py(fn, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), ndim=DataItem(0, schema: INT32), include_missing=DataItem(None, schema: NONE), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.py.map_py}
Aliases:

- [kd.map_py](#kd.map_py)

``` {.no-copy}
Apply the python function `fn` on provided `args` and `kwargs`.

Example:
  def my_fn(x, y):
    if x is None or y is None:
      return None
    return x * y

  kd.map_py(my_fn, slice_1, slice_2)
  # Via keyword
  kd.map_py(my_fn, x=slice_1, y=slice_2)

All DataSlices in `args` and `kwargs` must have compatible shapes.

Lambdas also work for object inputs/outputs.
In this case, objects are wrapped as DataSlices.
For example:
  def my_fn_object_inputs(x):
    return x.y + x.z

  def my_fn_object_outputs(x):
    return db.obj(x=1, y=2) if x.z > 3 else db.obj(x=2, y=1)

The `ndim` argument controls how many dimensions should be passed to `fn` in
each call. If `ndim = 0` then `0`-dimensional values will be passed, if
`ndim = 1` then python `list`s will be passed, if `ndim = 2` then lists of
python `list`s will be passed and so on.

`0`-dimensional (non-`list`) values passed to `fn` are either python
primitives (`float`, `int`, `str`, etc.) or single-valued `DataSlices`
containing `ItemId`s in the non-primitive case.

In this way, `ndim` can be used for aggregation.
For example:
  def my_agg_count(x):
    return len([i for i in x if i is not None])

  kd.map_py(my_agg_count, data_slice, ndim=1)

`fn` may return any objects that kd.from_py can handle, in other words
primitives, lists, dicts and dataslices. They will be converted to
the corresponding Koda data structures.

For example:
  def my_expansion(x):
    return [[y, y] for y in x]

  res = kd.map_py(my_expansion, data_slice, ndim=1)
  # Each item of res is a list of lists, so we can get a slice with
  # the inner items like this:
  print(res[:][:])

It's also possible to set custom serialization for the fn (i.e. if you want to
serialize the expression and later deserialize it in the different process).

For example to serialize the function using cloudpickle you can use
`kd_ext.py_cloudpickle(fn)` instead of fn.

Args:
  fn: Function.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  ndim: Dimensionality of items to pass to `fn`.
  include_missing: Specifies whether `fn` applies to all items (`=True`) or
    only to items present in all `args` and `kwargs` (`=False`, valid only
    when `ndim=0`); defaults to `False` when `ndim=0`.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called `map_py`
    in case `max_threads` is greater than 1, as we rely on this property for
    cases like progress reporting. As such, it can not be attached to the `fn`
    itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `kd.py.map_py_on_cond(true_fn, false_fn, cond, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.py.map_py_on_cond}
Aliases:

- [kd.map_py_on_cond](#kd.map_py_on_cond)

``` {.no-copy}
Apply python functions on `args` and `kwargs` based on `cond`.

`cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
dimensions than `args` or `kwargs`.

Also see kd.map_py().

This function supports only pointwise, not aggregational, operations.
`true_fn` is applied when `cond` is kd.present. Otherwise, `false_fn` is
applied.

Args:
  true_fn: Function.
  false_fn: Function.
  cond: Conditional DataSlice.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_cond` in case `max_threads` is greater than 1, as we rely on
    this property for cases like progress reporting. As such, it can not be
    attached to the `true_fn` and `false_fn` themselves.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `kd.py.map_py_on_selected(fn, cond, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.py.map_py_on_selected}
Aliases:

- [kd.map_py_on_selected](#kd.map_py_on_selected)

``` {.no-copy}
Apply python function `fn` on `args` and `kwargs` based on `cond`.

`cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
dimensions than `args` or `kwargs`.

Also see kd.map_py().

This function supports only pointwise, not aggregational, operations. `fn` is
applied when `cond` is kd.present.

Args:
  fn: Function.
  cond: Conditional DataSlice.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_selected` in case `max_threads` is greater than 1, as we rely
    on this property for cases like progress reporting. As such, it can not be
    attached to the `fn` itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

</section>

### kd.random {#kd.random}

Random and sampling operators.

<section class="zippy closed">

**Operators**

### `kd.random.randint_like(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_like}
Aliases:

- [kd.randint_like](#kd.randint_like)

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the same sparsity as `x`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  x: used to determine the shape and sparsity of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.
```

### `kd.random.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped}
Aliases:

- [kd.randint_shaped](#kd.randint_shaped)

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the given shape.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  shape: used for the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.
```

### `kd.random.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped_as}
Aliases:

- [kd.randint_shaped_as](#kd.randint_shaped_as)

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the same shape as `x`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

Args:
  x: used to determine the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  A DataSlice of random numbers.
```

### `kd.random.sample(x, ratio, seed, key=unspecified)` {#kd.random.sample}
Aliases:

- [kd.sample](#kd.sample)

``` {.no-copy}
Randomly sample items in `x` based on ratio.

The sampling is performed on flatten `x` rather than on the last dimension.

All items including missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) < ratio * 2^63
Therefore, exact sampled count is not guaranteed. E,g, result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample(ds, 0.5, 123) -> kd.slice([[None, 4], [None, 8]])

  # Use 'key' for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample(ds_1, 0.5, 123, key_1) -> kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample(ds_2, 0.5, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.
```

### `kd.random.sample_n(x, n, seed, key=unspecified)` {#kd.random.sample_n}
Aliases:

- [kd.sample_n](#kd.sample_n)

``` {.no-copy}
Randomly sample n items in `x` from the last dimension.

The sampling is performed over the last dimension rather than on flatten `x`.

`n` can either can be a scalar integer or DataSlice. If it is a DataSlice, it
must have compatible shape with `x.get_shape()[:-1]`. All items including
missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set.
Otherwise, the indices of `x` are used.

Examples:
  # Select 2 items from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, 2, 123) -> kd.slice([[2, 4], [None, 8]])

  # Select 1 item from the first and 2 items from the second.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, [1, 2], 123) -> kd.slice([[4], [None, 5]])

  # Use 'key' for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample_n(ds_1, 2, 123, key_1) -> kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample_n(ds_2, 2, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  n: number of items to sample. Either an integer or a DataSlice.
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.
```

</section>

### kd.schema {#kd.schema}

Schema-related operators.

<section class="zippy closed">

**Operators**

### `kd.schema.agg_common_schema(x, ndim=unspecified)` {#kd.schema.agg_common_schema}

``` {.no-copy}
Returns the common schema of `x` along the last `ndim` dimensions.

The "common schema" is defined according to go/koda-type-promotion.

Examples:
  kd.agg_common_schema(kd.slice([kd.INT32, None, kd.FLOAT32]))
    # -> kd.FLOAT32

  kd.agg_common_schema(kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]))
    # -> kd.slice([kd.INT32, kd.FLOAT64])

  kd.agg_common_schema(
      kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]), ndim=2)
    # -> kd.FLOAT64

Args:
  x: DataSlice of schemas.
  ndim: The number of last dimensions to aggregate over.
```

### `kd.schema.as_any(x)` {#kd.schema.as_any}
Aliases:

- [kd.schema.to_any](#kd.schema.to_any)

- [kd.as_any](#kd.as_any)

- [kd.to_any](#kd.to_any)

``` {.no-copy}
Casts `x` to ANY using explicit (permissive) casting rules.
```

### `kd.schema.cast_to(x, schema)` {#kd.schema.cast_to}
Aliases:

- [kd.cast_to](#kd.cast_to)

``` {.no-copy}
Returns `x` casted to the provided `schema` using explicit casting rules.

Dispatches to the relevant `kd.to_...` operator. Performs permissive casting,
e.g. allowing FLOAT32 -> INT32 casting through `kd.cast_to(slice, INT32)`.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.
```

### `kd.schema.cast_to_implicit(x, schema)` {#kd.schema.cast_to_implicit}

``` {.no-copy}
Returns `x` casted to the provided `schema` using implicit casting rules.

Note that `schema` must be the common schema of `schema` and `x.get_schema()`
according to go/koda-type-promotion.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.
```

### `kd.schema.cast_to_narrow(x, schema)` {#kd.schema.cast_to_narrow}

``` {.no-copy}
Returns `x` casted to the provided `schema`.

Allows for schema narrowing, where OBJECT and ANY types can be casted to
primitive schemas as long as the data is implicitly castable to the schema.
Follows the casting rules of `kd.cast_to_implicit` for the narrowed schema.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.
```

### `kd.schema.common_schema(x)` {#kd.schema.common_schema}

``` {.no-copy}
Returns the common schema as a scalar DataItem of `x`.

The "common schema" is defined according to go/koda-type-promotion.

Args:
  x: DataSlice of schemas.
```

### `kd.schema.dict_schema(key_schema, value_schema, db=None)` {#kd.schema.dict_schema}
Aliases:

- [kd.dict_schema](#kd.dict_schema)

``` {.no-copy}
Creates a dict schema in the given DataBag.

  Returned schema is immutable if `db` is not provided.

  Args:
    key_schema: schema of the keys in the list.
    value_schema: schema of the values in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a dict schema.
```

### `kd.schema.get_dtype(ds)` {#kd.schema.get_dtype}
Aliases:

- [kd.schema.get_primitive_schema](#kd.schema.get_primitive_schema)

- [kd.get_dtype](#kd.get_dtype)

- [kd.get_primitive_schema](#kd.get_primitive_schema)

``` {.no-copy}
Returns a primitive schema representing the underlying items' dtype.

If `ds` has a primitive schema, this returns that primitive schema, even if
all items in `ds` are missing. If `ds` has an OBJECT/ANY schema but contains
primitive values of a single dtype, it returns the schema for that primitive
dtype.

In case of items in `ds` have non-primitive types or mixed dtypes, returns
a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

Examples:
  kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
  kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.ANY)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
  kd.get_primitive_schema(kd.obj())) -> missing schema

Args:
  ds: DataSlice to get dtype from.

Returns:
  a primitive schema DataSlice.
```

### `kd.schema.get_item_schema(list_schema)` {#kd.schema.get_item_schema}
Aliases:

- [kd.get_item_schema](#kd.get_item_schema)

``` {.no-copy}
Returns the item schema of a List schema`.
```

### `kd.schema.get_itemid(x)` {#kd.schema.get_itemid}
Aliases:

- [kd.schema.to_itemid](#kd.schema.to_itemid)

- [kd.get_itemid](#kd.get_itemid)

- [kd.to_itemid](#kd.to_itemid)

``` {.no-copy}
Casts `x` to ITEMID using explicit (permissive) casting rules.
```

### `kd.schema.get_key_schema(dict_schema)` {#kd.schema.get_key_schema}
Aliases:

- [kd.get_key_schema](#kd.get_key_schema)

``` {.no-copy}
Returns the key schema of a Dict schema`.
```

### `kd.schema.get_nofollowed_schema(schema)` {#kd.schema.get_nofollowed_schema}
Aliases:

- [kd.get_nofollowed_schema](#kd.get_nofollowed_schema)

``` {.no-copy}
Returns the original schema from nofollow schema.

Requires `nofollow_schema` to be a nofollow schema, i.e. that it wraps some
other schema.

Args:
  schema: nofollow schema DataSlice.
```

### `kd.schema.get_obj_schema(x)` {#kd.schema.get_obj_schema}
Aliases:

- [kd.get_obj_schema](#kd.get_obj_schema)

``` {.no-copy}
Returns a DataSlice of schemas for Objects and primitives in `x`.

DataSlice `x` must have OBJECT schema.

Examples:
  db = kd.bag()
  s = db.new_schema(a=kd.INT32)
  obj = s(a=1).embed_schema()
  kd.get_obj_schema(kd.slice([1, None, 2.0, obj]))
    -> kd.slice([kd.INT32, NONE, kd.FLOAT32, s])

Args:
  x: OBJECT DataSlice

Returns:
  A DataSlice of schemas.
```

### `kd.schema.get_primitive_schema(ds)` {#kd.schema.get_primitive_schema}

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype) operator.

### `kd.schema.get_schema(x)` {#kd.schema.get_schema}
Aliases:

- [kd.get_schema](#kd.get_schema)

``` {.no-copy}
Returns the schema of `x`.
```

### `kd.schema.get_value_schema(dict_schema)` {#kd.schema.get_value_schema}
Aliases:

- [kd.get_value_schema](#kd.get_value_schema)

``` {.no-copy}
Returns the value schema of a Dict schema`.
```

### `kd.schema.internal_maybe_named_schema(name_or_schema)` {#kd.schema.internal_maybe_named_schema}

``` {.no-copy}
Converts a string to a named schema, passes through schema otherwise.

The operator also passes through arolla.unspecified, and raises when
it receives anything else except unspecified, string or schema DataItem.

This operator exists to support kd.core.new* family of operators.

Args:
  name_or_schema: The input name or schema.

Returns:
  The schema unchanged, or a named schema with the given name.
```

### `kd.schema.is_dict_schema(x)` {#kd.schema.is_dict_schema}

``` {.no-copy}
Returns true iff `x` is a Dict schema DataItem.
```

### `kd.schema.is_entity_schema(x)` {#kd.schema.is_entity_schema}

``` {.no-copy}
Returns true iff `x` is an Entity schema DataItem.
```

### `kd.schema.is_list_schema(x)` {#kd.schema.is_list_schema}

``` {.no-copy}
Returns true iff `x` is a List schema DataItem.
```

### `kd.schema.is_primitive_schema(x)` {#kd.schema.is_primitive_schema}

``` {.no-copy}
Returns true iff `x` is a primitive schema DataItem.
```

### `kd.schema.is_struct_schema(x)` {#kd.schema.is_struct_schema}

``` {.no-copy}
Returns true iff `x` is a Struct schema DataItem.
```

### `kd.schema.list_schema(item_schema, db=None)` {#kd.schema.list_schema}
Aliases:

- [kd.list_schema](#kd.list_schema)

``` {.no-copy}
Creates a list schema in the given DataBag.

  Returned schema is immutable if `db` is not provided.

  Args:
    item_schema: schema of the items in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a list schema.
```

### `kd.schema.named_schema(name, *, db=None, **attrs)` {#kd.schema.named_schema}
Aliases:

- [kd.named_schema](#kd.named_schema)

``` {.no-copy}
Creates a named entity schema in the given DataBag.

  Returned schema is immutable if `db` is not provided.

  A named schema will have its item id derived only from its name, which means
  that two named schemas with the same name will have the same item id, even in
  different DataBags, or with different kwargs passed to this method.

  Args:
    name: The name to use to derive the item id of the schema.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: A mapping of attribute names to DataSlices. The DataSlice values
      must be schemas themselves.

  Returns:
    data_slice.DataSlice with the item id of the required schema and kd.SCHEMA
    schema, with the DataBag attached containing the provided
    attrs.
```

### `kd.schema.new_schema(db=None, **attrs)` {#kd.schema.new_schema}

``` {.no-copy}
Creates new schema in the given DataBag.

  Returned schema is immutable if `db` is not provided.

  Args:
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
```

### `kd.schema.nofollow_schema(schema)` {#kd.schema.nofollow_schema}
Aliases:

- [kd.nofollow_schema](#kd.nofollow_schema)

``` {.no-copy}
Returns a NoFollow schema of the provided schema.

`nofollow_schema` is reversible with `get_actual_schema`.

`nofollow_schema` can only be called on implicit and explicit schemas and
OBJECT. It raises an Error if called on ANY, primitive schemas, ITEMID, etc.

Args:
  schema: Schema DataSlice to wrap.
```

### `kd.schema.schema_from_py(tpe)` {#kd.schema.schema_from_py}
Aliases:

- [kd.schema_from_py](#kd.schema_from_py)

``` {.no-copy}
Creates a Koda entity schema corresponding to the given Python type.

  This method supports the following Python types / type annotations
  recursively:
  - Primitive types: int, float, bool, str, bytes.
  - Collections: list[...], dict[...], Sequence[...], Mapping[...], ect.
  - Unions: only "smth | None" or "Optional[smth]" is supported.
  - Dataclasses.

  This can be used in conjunction with kd.from_py to convert lists of Python
  objects to efficient Koda DataSlices. Because of the 'efficient' goal, we
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
    to derive the itemid for the uu-schema.
```

### `kd.schema.to_any(x)` {#kd.schema.to_any}

Alias for [kd.schema.as_any](#kd.schema.as_any) operator.

### `kd.schema.to_bool(x)` {#kd.schema.to_bool}
Aliases:

- [kd.to_bool](#kd.to_bool)

``` {.no-copy}
Casts `x` to BOOLEAN using explicit (permissive) casting rules.
```

### `kd.schema.to_bytes(x)` {#kd.schema.to_bytes}
Aliases:

- [kd.to_bytes](#kd.to_bytes)

``` {.no-copy}
Casts `x` to BYTES using explicit (permissive) casting rules.
```

### `kd.schema.to_expr(x)` {#kd.schema.to_expr}
Aliases:

- [kd.to_expr](#kd.to_expr)

``` {.no-copy}
Casts `x` to EXPR using explicit (permissive) casting rules.
```

### `kd.schema.to_float32(x)` {#kd.schema.to_float32}
Aliases:

- [kd.to_float32](#kd.to_float32)

``` {.no-copy}
Casts `x` to FLOAT32 using explicit (permissive) casting rules.
```

### `kd.schema.to_float64(x)` {#kd.schema.to_float64}
Aliases:

- [kd.to_float64](#kd.to_float64)

``` {.no-copy}
Casts `x` to FLOAT64 using explicit (permissive) casting rules.
```

### `kd.schema.to_int32(x)` {#kd.schema.to_int32}
Aliases:

- [kd.to_int32](#kd.to_int32)

``` {.no-copy}
Casts `x` to INT32 using explicit (permissive) casting rules.
```

### `kd.schema.to_int64(x)` {#kd.schema.to_int64}
Aliases:

- [kd.to_int64](#kd.to_int64)

``` {.no-copy}
Casts `x` to INT64 using explicit (permissive) casting rules.
```

### `kd.schema.to_itemid(x)` {#kd.schema.to_itemid}

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid) operator.

### `kd.schema.to_mask(x)` {#kd.schema.to_mask}
Aliases:

- [kd.to_mask](#kd.to_mask)

``` {.no-copy}
Casts `x` to MASK using explicit (permissive) casting rules.
```

### `kd.schema.to_none(x)` {#kd.schema.to_none}
Aliases:

- [kd.to_none](#kd.to_none)

``` {.no-copy}
Casts `x` to NONE using explicit (permissive) casting rules.
```

### `kd.schema.to_object(x)` {#kd.schema.to_object}
Aliases:

- [kd.to_object](#kd.to_object)

``` {.no-copy}
Casts `x` to OBJECT using explicit (permissive) casting rules.
```

### `kd.schema.to_schema(x)` {#kd.schema.to_schema}
Aliases:

- [kd.to_schema](#kd.to_schema)

``` {.no-copy}
Casts `x` to SCHEMA using explicit (permissive) casting rules.
```

### `kd.schema.to_str(x)` {#kd.schema.to_str}
Aliases:

- [kd.to_str](#kd.to_str)

``` {.no-copy}
Casts `x` to STRING using explicit (permissive) casting rules.
```

### `kd.schema.uu_schema(seed='', *, db=None, **attrs)` {#kd.schema.uu_schema}
Aliases:

- [kd.uu_schema](#kd.uu_schema)

``` {.no-copy}
Creates a uu_schema in the given DataBag.

  Returned schema is immutable if `db` is not provided.

  Args:
    seed: optional string to seed the uuid computation with.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
```

### `kd.schema.with_schema(x, schema)` {#kd.schema.with_schema}
Aliases:

- [kd.with_schema](#kd.with_schema)

``` {.no-copy}
Returns a copy of `x` with the provided `schema`.

If `schema` is an Entity schema, it must have no DataBag or the same DataBag
as `x`. To set schema with a different DataBag, use `kd.set_schema` instead.

It only changes the schemas of `x` and does not change the items in `x`. To
change the items in `x`, use `kd.cast_to` instead. For example,

  kd.with_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
      `x` are not compatible with FLOAT32.
  kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

When items in `x` are primitives or `schemas` is a primitive schema, it checks
items and schema are compatible. When items are ItemIds and `schema` is a
non-primitive schema, it does not check the underlying data matches the
schema. For example,

  kd.with_schema(kd.ds([1, 2, 3], schema=kd.ANY), kd.INT32) ->
      kd.ds([1, 2, 3])
  kd.with_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail

  db = kd.bag()
  kd.with_schema(kd.ds(1).with_bag(db), db.new_schema(x=kd.INT32)) -> fail due
      to incompatible schema
  kd.with_schema(db.new(x=1), kd.INT32) -> fail due to incompatible schema
  kd.with_schema(db.new(x=1), kd.schema.new_schema(x=kd.INT32)) -> fail due to
      different DataBag
  kd.with_schema(db.new(x=1), kd.schema.new_schema(x=kd.INT32).no_bag()) ->
  work
  kd.with_schema(db.new(x=1), db.new_schema(x=kd.INT64)) -> work

Args:
  x: DataSlice to change the schema of.
  schema: DataSlice containing the new schema.

Returns:
  DataSlice with the new schema.
```

### `kd.schema.with_schema_from_obj(x)` {#kd.schema.with_schema_from_obj}
Aliases:

- [kd.with_schema_from_obj](#kd.with_schema_from_obj)

``` {.no-copy}
Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.
```

</section>

### kd.shapes {#kd.shapes}

Operators that work on shapes

<section class="zippy closed">

**Operators**

### `kd.shapes.dim_mapping(shape, dim)` {#kd.shapes.dim_mapping}

``` {.no-copy}
Returns the parent-to-child mapping of the dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [3, 2], [1, 2, 0, 2, 1])
  kd.shapes.dim_mapping(shape, 0) # -> kd.slice([0, 0])
  kd.shapes.dim_mapping(shape, 1) # -> kd.slice([0, 0, 0, 1, 1])
  kd.shapes.dim_mapping(shape, 2) # -> kd.slice([0, 1, 1, 3, 3, 4])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the parent-to-child mapping for.
```

### `kd.shapes.dim_sizes(shape, dim)` {#kd.shapes.dim_sizes}

``` {.no-copy}
Returns the row sizes at the provided dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [2, 1])
  kd.shapes.dim_sizes(shape, 0)  # -> kd.slice([2])
  kd.shapes.dim_sizes(shape, 1)  # -> kd.slice([2, 1])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the sizes for.
```

### `kd.shapes.expand_to_shape(x, shape, ndim=unspecified)` {#kd.shapes.expand_to_shape}
Aliases:

- [kd.expand_to_shape](#kd.expand_to_shape)

``` {.no-copy}
Expands `x` based on the provided `shape`.

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
  Expanded DataSlice
```

### `kd.shapes.flatten(x, from_dim=DataItem(0, schema: INT64), to_dim=unspecified)` {#kd.shapes.flatten}
Aliases:

- [kd.flatten](#kd.flatten)

``` {.no-copy}
Returns `x` with dimensions `[from_dim:to_dim]` flattened.

Indexing works as in python:
* If `to_dim` is unspecified, `to_dim = rank()` is used.
* If `to_dim < from_dim`, `to_dim = from_dim` is used.
* If `to_dim < 0`, `max(0, to_dim + rank())` is used. The same goes for
  `from_dim`.
* If `to_dim > rank()`, `rank()` is used. The same goes for `from_dim`.

The above-mentioned adjustments places both `from_dim` and `to_dim` in the
range `[0, rank()]`. After adjustments, the new DataSlice has `rank() ==
old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
"unit" dimension is inserted at `from_dim`.

Example:
  # Flatten the last two dimensions into a single dimension, producing a
  # DataSlice with `rank = old_rank - 1`.
  kd.get_shape(x)  # -> JaggedShape(..., [2, 1], [7, 5, 3])
  flat_x = kd.flatten(x, -2)
  kd.get_shape(flat_x)  # -> JaggedShape(..., [12, 3])

  # Flatten all dimensions except the last, producing a DataSlice with
  # `rank = 2`.
  kd.get_shape(x)  # -> jaggedShape(..., [7, 5, 3])
  flat_x = kd.flatten(x, 0, -1)
  kd.get_shape(flat_x)  # -> JaggedShape([3], [7, 5, 3])

  # Flatten all dimensions.
  kd.get_shape(x)  # -> JaggedShape([3], [7, 5, 3])
  flat_x = kd.flatten(x)
  kd.get_shape(flat_x)  # -> JaggedShape([15])

Args:
  x: a DataSlice.
  from_dim: start of dimensions to flatten. Defaults to `0` if unspecified.
  to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.
```

### `kd.shapes.get_shape(x)` {#kd.shapes.get_shape}
Aliases:

- [kd.get_shape](#kd.get_shape)

``` {.no-copy}
Returns the shape of `x`.
```

### `kd.shapes.is_expandable_to_shape(x, target_shape, ndim=unspecified)` {#kd.shapes.is_expandable_to_shape}

``` {.no-copy}
Returns true if `x` is expandable to `target_shape`.

See `expand_to_shape` for a detailed description of expansion.

Args:
  x: DataSlice that would be expanded.
  target_shape: JaggedShape that would be expanded to.
  ndim: The number of dimensions to implode before expansion. If unset,
    defaults to 0.
```

### `kd.shapes.ndim(shape)` {#kd.shapes.ndim}
Aliases:

- [kd.shapes.rank](#kd.shapes.rank)

``` {.no-copy}
Returns the rank of the jagged shape.
```

### `kd.shapes.new(*dimensions)` {#kd.shapes.new}

``` {.no-copy}
Returns a JaggedShape from the provided dimensions.

Example:
  # Creates a scalar shape (i.e. no dimension).
  kd.shapes.new()  # -> JaggedShape()

  # Creates a 3-dimensional shape with all uniform dimensions.
  kd.shapes.new(2, 3, 1)  # -> JaggedShape(2, 3, 1)

  # Creates a 3-dimensional shape with 2 sub-values in the first dimension.
  #
  # The second dimension is jagged with 2 values. The first value in the
  # second dimension has 2 sub-values, and the second value has 1 sub-value.
  #
  # The third dimension is jagged with 3 values. The first value in the third
  # dimension has 1 sub-value, the second has 2 sub-values, and the third has
  # 3 sub-values.
  kd.shapes.new(2, [2, 1], [1, 2, 3])
      # -> JaggedShape(2, [2, 1], [1, 2, 3])

Args:
  *dimensions: A combination of Edges and DataSlices representing the
    dimensions of the JaggedShape. Edges are used as is, while DataSlices are
    treated as sizes. DataItems (of ints) are interpreted as uniform
    dimensions which have the same child size for all parent elements.
    DataSlices (of ints) are interpreted as a list of sizes, where `ds[i]` is
    the child size of parent `i`. Only rank-0 or rank-1 int DataSlices are
    supported.
```

### `kd.shapes.rank(shape)` {#kd.shapes.rank}

Alias for [kd.shapes.ndim](#kd.shapes.ndim) operator.

### `kd.shapes.reshape(x, shape)` {#kd.shapes.reshape}
Aliases:

- [kd.reshape](#kd.reshape)

``` {.no-copy}
Returns a DataSlice with the provided shape.

Examples:
  x = kd.slice([1, 2, 3, 4])

  # Using a shape.
  kd.reshape(x, kd.shapes.new(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes.
  kd.reshape(x, kd.make_tuple(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of slices and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, kd.slice([3, 1])))
      # -> kd.slice([[1, 2, 3], [4]])

  # Reshaping a scalar.
  kd.reshape(1, kd.make_tuple(1, 1))  # -> kd.slice([[1]])

  # Reshaping an empty slice.
  kd.reshape(kd.slice([]), kd.make_tuple(2, 0))  # -> kd.slice([[], []])

Args:
  x: a DataSlice.
  shape: a JaggedShape or a tuple of dimensions that forms a shape through
    `kd.shapes.new`, with additional support for a `-1` placeholder dimension.
```

### `kd.shapes.reshape_as(x, shape_from)` {#kd.shapes.reshape_as}
Aliases:

- [kd.reshape_as](#kd.reshape_as)

``` {.no-copy}
Returns a DataSlice x reshaped to the shape of DataSlice shape_from.
```

### `kd.shapes.size(shape)` {#kd.shapes.size}

``` {.no-copy}
Returns the total number of elements the jagged shape represents.
```

</section>

### kd.slices {#kd.slices}

Operators that perform DataSlice transformations.

<section class="zippy closed">

**Operators**

### `kd.slices.agg_count(x, ndim=unspecified)` {#kd.slices.agg_count}
Aliases:

- [kd.agg_count](#kd.agg_count)

``` {.no-copy}
Returns counts of present items over the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_count(ds)  # -> kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=1)  # -> kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=2)  # -> kd.slice(5)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).
```

### `kd.slices.agg_size(x, ndim=unspecified)` {#kd.slices.agg_size}
Aliases:

- [kd.agg_size](#kd.agg_size)

``` {.no-copy}
Returns number of items in `x` over the last ndim dimensions.

Note that it counts missing items, which is different from `kd.count`.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_size(ds)  # -> kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=1)  # -> kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=2)  # -> kd.slice(8)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  A DataSlice of number of items in `x` over the last `ndim` dimensions.
```

### `kd.slices.align(*args)` {#kd.slices.align}
Aliases:

- [kd.align](#kd.align)

``` {.no-copy}
Expands all of the DataSlices in `args` to the same common shape.

All DataSlices must be expandable to the shape of the DataSlice with the
largest number of dimensions.

Example:
  kd.align(kd.slice([[1, 2, 3], [4, 5]]), kd.slice('a'), kd.slice([1, 2]))
  # Returns:
  # (
  #   kd.slice([[1, 2, 3], [4, 5]]),
  #   kd.slice([['a', 'a', 'a'], ['a', 'a']]),
  #   kd.slice([[1, 1, 1], [2, 2]]),
  # )

Args:
  *args: DataSlices to align.

Returns:
  A tuple of aligned DataSlices, matching `args`.
```

### `kd.slices.at(x, indices)` {#kd.slices.at}
Aliases:

- [kd.slices.take](#kd.slices.take)

- [kd.at](#kd.at)

- [kd.take](#kd.take)

``` {.no-copy}
Returns a new DataSlice with items at provided indices.

`indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
INT64 items.

Indices in the DataSlice `indices` are based on the last dimension of the
DataSlice `x`. Negative indices are supported and out-of-bound indices result
in missing items.

If ndim(x) - 1 > ndim(indices), indices are broadcasted to shape(x)[:-1].
If ndim(x) <= ndim(indices), indices are unchanged but shape(x)[:-1] must be
broadcastable to shape(indices).

Example:
  x = kd.slice([[1, None, 2], [3, 4]])
  kd.take(x, kd.item(1))  # -> kd.slice([[None, 4]])
  kd.take(x, kd.slice([0, 1]))  # -> kd.slice([1, 4])
  kd.take(x, kd.slice([[0, 1], [1]]))  # -> kd.slice([[1, None], [4]])
  kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
    # -> kd.slice([[[1, None]], []], [[4], [3]]])
  kd.take(x, kd.slice([3, -3]))  # -> kd.slice([None, None])
  kd.take(x, kd.slice([-1, -2]))  # -> kd.slice([2, 3])
  kd.take(x, kd.slice('1')) # -> dtype mismatch error
  kd.take(x, kd.slice([1, 2, 3])) -> incompatible shape

Args:
  x: DataSlice to be indexed
  indices: indices used to select items

Returns:
  A new DataSlice with items selected by indices.
```

### `kd.slices.bool(x)` {#kd.slices.bool}
Aliases:

- [kd.bool](#kd.bool)

``` {.no-copy}
Returns kd.slice(x, kd.BOOLEAN).
```

### `kd.slices.bytes(x)` {#kd.slices.bytes}
Aliases:

- [kd.bytes](#kd.bytes)

``` {.no-copy}
Returns kd.slice(x, kd.BYTES).
```

### `kd.slices.collapse(x, ndim=unspecified)` {#kd.slices.collapse}
Aliases:

- [kd.collapse](#kd.collapse)

``` {.no-copy}
Collapses the same items over the last ndim dimensions.

Missing items are ignored. For each collapse aggregation, the result is
present if and only if there is at least one present item and all present
items are the same.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.collapse(ds)  # -> kd.slice([1, None, None])
  kd.collapse(ds, ndim=1)  # -> kd.slice([1, None, None])
  kd.collapse(ds, ndim=2)  # -> kd.slice(None)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to collapse into. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  Collapsed DataSlice.
```

### `kd.slices.concat(*args, ndim=DataItem(1, schema: INT32))` {#kd.slices.concat}
Aliases:

- [kd.concat](#kd.concat)

``` {.no-copy}
Returns the concatenation of the given DataSlices on dimension `rank-ndim`.

All given DataSlices must have the same rank, and the shapes of the first
`rank-ndim` dimensions must match. If they have incompatible shapes, consider
using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
to bring them to compatible shapes first.

The shape of the concatenated result is the following:
  1) the shape of the first `rank-ndim` dimensions remains the same
  2) the shape of the concatenation dimension is the element-wise sum of the
    shapes of the arguments' concatenation dimensions
  3) the shapes of the last `ndim-1` dimensions are interleaved within the
    groups implied by the concatenation dimension

Alteratively, if we think of each input DataSlice as a nested Python list,
this operator simultaneously iterates over the inputs at depth `rank-ndim`,
concatenating the root lists of the corresponding nested sub-lists from each
input.

For example,
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[[1], [2]], [[3], [4]]])

kd.concat(a, b, ndim=1) -> [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
kd.concat(a, b, ndim=2) -> [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
kd.concat(a, b, ndim=3) -> [[[1, 2], [3]], [[5], [7, 8]],
                            [[1], [2]], [[3], [4]]]
kd.concat(a, b, ndim=4) -> raise an exception
kd.concat(a, b) -> the same as kd.concat(a, b, ndim=1)

The reason auto-broadcasting is not supported is that such behavior can be
confusing and often not what users want. For example,

a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[1, 2], [3, 4]])
kd.concat(a, b) -> should it be which of the following?
  [[[1, 2, 1, 2], [3, 1, 2]], [[5, 3, 4], [7, 8, 3, 4]]]
  [[[1, 2, 1, 1], [3, 2]], [[5, 3], [7, 8, 4, 4]]]
  [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]

Args:
  *args: The DataSlices to concatenate.
  ndim: The number of last dimensions to concatenate (default 1).

Returns:
  The contatenation of the input DataSlices on dimension `rank-ndim`. In case
  the input DataSlices come from different DataBags, this will refer to a
  new merged immutable DataBag.
```

### `kd.slices.count(x)` {#kd.slices.count}
Aliases:

- [kd.count](#kd.count)

``` {.no-copy}
Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `kd.slices.cum_count(x, ndim=unspecified)` {#kd.slices.cum_count}
Aliases:

- [kd.cum_count](#kd.cum_count)

``` {.no-copy}
Computes a partial count of present items over the last `ndim` dimensions.

If `ndim` isn't specified, it defaults to 1 (count over the last dimension).

Example:
  x = kd.slice([[1, None, 1, 1], [3, 4, 5]])
  kd.cum_count(x, ndim=1)  # -> kd.slice([[1, None, 2, 3], [1, 2, 3]])
  kd.cum_count(x, ndim=2)  # -> kd.slice([[1, None, 2, 3], [4, 5, 6]])

Args:
  x: A DataSlice.
  ndim: The number of trailing dimensions to count within. Requires 0 <= ndim
    <= get_ndim(x).

Returns:
  A DataSlice of INT64 with the same shape and sparsity as `x`.
```

### `kd.slices.dense_rank(x, descending=DataItem(False, schema: BOOLEAN), ndim=unspecified)` {#kd.slices.dense_rank}
Aliases:

- [kd.dense_rank](#kd.dense_rank)

``` {.no-copy}
Returns dense ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By dense ranking ("1 2 2 3" ranking), equal items are assigned to the same
rank and the next items are assigned to that rank plus one (i.e. no gap
between the rank numbers).

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result.

Example:

  ds = kd.slice([[4, 3, None, 3], [3, None, 2, 1]])
  kd.dense_rank(x) -> kd.slice([[1, 0, None, 0], [2, None, 1, 0]])

  kd.dense_rank(x, descending=True) ->
      kd.slice([[0, 1, None, 1], [0, None, 1, 2]])

  kd.dense_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.dense_rank(x, ndim=2) -> kd.slice([[3, 2, None, 2], [2, None, 1, 0]])

Args:
  x: DataSlice to rank.
  descending: If true, items are compared in descending order.
  ndim: The number of dimensions to rank over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  A DataSlice of dense ranks.
```

### `kd.slices.expand_to(x, target, ndim=unspecified)` {#kd.slices.expand_to}
Aliases:

- [kd.expand_to](#kd.expand_to)

``` {.no-copy}
Expands `x` based on the shape of `target`.

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
  Expanded DataSlice
```

### `kd.slices.expr_quote(x)` {#kd.slices.expr_quote}
Aliases:

- [kd.expr_quote](#kd.expr_quote)

``` {.no-copy}
Returns kd.slice(x, kd.EXPR).
```

### `kd.slices.float32(x)` {#kd.slices.float32}
Aliases:

- [kd.float32](#kd.float32)

``` {.no-copy}
Returns kd.slice(x, kd.FLOAT32).
```

### `kd.slices.float64(x)` {#kd.slices.float64}
Aliases:

- [kd.float64](#kd.float64)

``` {.no-copy}
Returns kd.slice(x, kd.FLOAT64).
```

### `kd.slices.get_ndim(x)` {#kd.slices.get_ndim}
Aliases:

- [kd.get_ndim](#kd.get_ndim)

``` {.no-copy}
Returns the number of dimensions of DataSlice `x`.
```

### `kd.slices.group_by(x, *args, sort=DataItem(False, schema: BOOLEAN))` {#kd.slices.group_by}
Aliases:

- [kd.group_by](#kd.group_by)

``` {.no-copy}
Returns permutation of `x` with injected grouped_by dimension.

The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions corresponds to the groups
and the items within the groups.

Values of the result is a permutation of `x`. `args` are used for the grouping
keys. If length of `args` is greater than 1, the key is a tuple.
If `args` is empty, the key is `x`.

If sort=True groups are ordered by value, otherwise groups are ordered by the
appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[1, 1, 1], [3, 3, 3], [2, 2]])

Example 2:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3], sort=True)
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

  When *args is present, `x` is not used for the key.

Example 6:
  x: kd.slice([1, 2, 3, 4, None, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4,    0, 7, None]),
  result: kd.slice([[1, 7], [2, None], [3, 6], [4]])

  Items with missing key is not listed in the result.
  Missing `x` values are missing in the result.

Example 7:
  x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  z: kd.slice([A, D, B, A, D, C, A, B]),
  result: kd.slice([[1, 7], [2, 5], [3], [4], [6]])

  When *args has two or more values, the  key is a tuple.
  In this example we have the following groups:
  (7, A), (4, D), (0, B), (9, A), (0, C)

Args:
  x: DataSlice to group.
  *args: DataSlices keys to group by. All data slices must have the same shape
    as x. Scalar DataSlices are not supported. If not present, `x` is used as
    the key.
  sort: Whether groups should be ordered by value.

Returns:
  DataSlice with the same shape and schema as `x` with injected grouped
  by dimension.
```

### `kd.slices.group_by_indices(*args, sort=DataItem(False, schema: BOOLEAN))` {#kd.slices.group_by_indices}
Aliases:

- [kd.group_by_indices](#kd.group_by_indices)

``` {.no-copy}
Returns a indices DataSlice with injected grouped_by dimension.

The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions corresponds to the groups
and the items within the groups.

Values of the DataSlice are the indices of the items within the parent
dimension. `kd.take(x, kd.group_by_indices(x))` would group the items in
`x` by their values.

If sort=True groups are ordered by value, otherwise groups are ordered by the
appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[0, 3, 6], [1, 5, 7], [2, 4]])

  We have three groups in order: 1, 3, 2. Each sublist contains the indices of
  the items in the original DataSlice.

Example 2:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3], sort=True)
  result: kd.slice([[0, 3, 6], [2, 4], [1, 5, 7]])

  Groups are now ordered by value.

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
  *args: DataSlices keys to group by. All data slices must have the same
    shape. Scalar DataSlices are not supported.
  sort: Whether groups should be ordered by value.

Returns:
  INT64 DataSlice with indices and injected grouped_by dimension.
```

### `kd.slices.index(x, dim=DataItem(-1, schema: INT32))` {#kd.slices.index}
Aliases:

- [kd.index](#kd.index)

``` {.no-copy}
Returns the indices of the elements computed over dimension `dim`.

The resulting slice has the same shape as the input.

Example:
  ds = kd.slice([
      [
          ['a', None, 'c'],
          ['d', 'e']
      ],
      [
          [None, 'g'],
          ['h', 'i', 'j']
      ]
  ])
  kd.index(ds, dim=0)
    # -> kd.slice([[[0, None, 0], [0, 0]], [[None, 1], [1, 1, 1]]])
  kd.index(ds, dim=1)
    # -> kd.slice([[[0, None, 0], [1, 1]], [[None, 0], [1, 1, 1]]])
  kd.index(ds, dim=2)  # (same as kd.index(ds, -1) or kd.index(ds))
    # -> kd.slice([[[0, None, 2], [0, 1]], [[None, 1], [0, 1, 2]]])

  kd.index(ds) -> kd.index(ds, dim=ds.get_ndim() - 1)

Args:
  x: A DataSlice.
  dim: The dimension to compute indices over.
    Requires -get_ndim(x) <= dim < get_ndim(x).
    If dim < 0 then dim = get_ndim(x) + dim.
```

### `kd.slices.int32(x)` {#kd.slices.int32}
Aliases:

- [kd.int32](#kd.int32)

``` {.no-copy}
Returns kd.slice(x, kd.INT32).
```

### `kd.slices.int64(x)` {#kd.slices.int64}
Aliases:

- [kd.int64](#kd.int64)

``` {.no-copy}
Returns kd.slice(x, kd.INT64).
```

### `kd.slices.inverse_mapping(x, ndim=unspecified)` {#kd.slices.inverse_mapping}
Aliases:

- [kd.inverse_mapping](#kd.inverse_mapping)

``` {.no-copy}
Returns inverse permutations of indices over the last `ndim` dimension.

It interprets `indices` over the last `ndim` dimension as a permutation and
substitute with the corresponding inverse permutation. `ndim` is set to 1 by
default if unspecified. It fails when `indices` is not a valid permutation.

Example:
  indices = kd.slice([[1, 2, 0], [1, None]])
  kd.inverse_mapping(indices)  ->  kd.slice([[2, 0, 1], [None, 0]])

  Explanation:
    indices      = [[1, 2, 0], [1, None]]
    inverse_permutation[1, 2, 0] = [2, 0, 1]
    inverse_permutation[1, None] = [None, 0]

  kd.inverse_mapping(indices, ndim=1) -> raise

  indices = kd.slice([[1, 2, 0], [3, None]])
  kd.inverse_mapping(indices, ndim=2)  ->  kd.slice([[2, 0, 1], [3, None]])

Args:
  x: A DataSlice of indices.
  ndim: The number of dimensions to compute inverse permutations over.
    Requires 0 <= ndim <= get_ndim(x).

Returns:
  An inverse permutation of indices.
```

### `kd.slices.inverse_select(ds, fltr)` {#kd.slices.inverse_select}
Aliases:

- [kd.slices.reverse_select](#kd.slices.reverse_select)

- [kd.inverse_select](#kd.inverse_select)

- [kd.reverse_select](#kd.reverse_select)

``` {.no-copy}
Creates a DataSlice by putting items in ds to present positions in fltr.

The shape of `ds` and the shape of `fltr` must have the same rank and the same
first N-1 dimensions. That is, only the last dimension can be different. The
shape of `ds` must be the same as the shape of the DataSlice after applying
`fltr` using kd.select. That is,
ds.get_shape() == kd.select(fltr, fltr).get_shape().

Example:
  ds = kd.slice([[1, None], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> [[None, 1, None], [2, None]]

  ds = kd.slice([1, None, 2])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different ranks

  ds = kd.slice([[1, None, 2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different N-1 dimensions

  ds = kd.slice([[1], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to incompatible shapes

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
  Reverse filtered DataSlice.
```

### `kd.slices.is_empty(x)` {#kd.slices.is_empty}
Aliases:

- [kd.is_empty](#kd.is_empty)

``` {.no-copy}
Returns kd.present if all items in the DataSlice are missing.
```

### `kd.slices.is_expandable_to(x, target, ndim=unspecified)` {#kd.slices.is_expandable_to}
Aliases:

- [kd.is_expandable_to](#kd.is_expandable_to)

``` {.no-copy}
Returns true if `x` is expandable to `target`.

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode before expansion.

See `expand_to` for a detailed description of expansion.
```

### `kd.slices.is_shape_compatible(x, y)` {#kd.slices.is_shape_compatible}
Aliases:

- [kd.is_shape_compatible](#kd.is_shape_compatible)

``` {.no-copy}
Returns present if the shapes of `x` and `y` are compatible.

Two DataSlices have compatible shapes if dimensions of one DataSlice equal or
are prefix of dimensions of another DataSlice.

Args:
  x: DataSlice to check.
  y: DataSlice to check.

Returns:
  A MASK DataItem indicating whether 'x' and 'y' are compatible.
```

### `kd.slices.isin(x, y)` {#kd.slices.isin}
Aliases:

- [kd.isin](#kd.isin)

``` {.no-copy}
Returns a DataItem indicating whether DataItem x is present in y.
```

### `kd.slices.item(x, /, schema=None)` {#kd.slices.item}
Aliases:

- [kd.item](#kd.item)

- [DataItem.from_vals](#DataItem.from_vals)

``` {.no-copy}
Returns a DataItem created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`. Python value must be convertible to Koda scalar and the result cannot
be multidimensional DataSlice.

Args:
  x: a Python value or a DataItem.
  schema: schema DataItem to set. If `x` is already a DataItem, this will cast
    it to the given schema.
```

### `kd.slices.mask(x)` {#kd.slices.mask}
Aliases:

- [kd.mask](#kd.mask)

``` {.no-copy}
Returns kd.slice(x, kd.MASK).
```

### `kd.slices.ordinal_rank(x, tie_breaker=unspecified, descending=DataItem(False, schema: BOOLEAN), ndim=unspecified)` {#kd.slices.ordinal_rank}
Aliases:

- [kd.ordinal_rank](#kd.ordinal_rank)

``` {.no-copy}
Returns ordinal ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By ordinal ranking ("1 2 3 4" ranking), equal items receive distinct ranks.
Items are compared by the triple (value, tie_breaker, position) to resolve
ties. When descending=True, values are ranked in descending order but
tie_breaker and position are ranked in ascending order.

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result. If `tie_breaker` is specified, it
cannot be more sparse than `x`.

Example:

  ds = kd.slice([[0, 3, None, 6], [5, None, 2, 1]])
  kd.ordinal_rank(x) -> kd.slice([[0, 1, None, 2], [2, None, 1, 0]])

  kd.ordinal_rank(x, descending=True) ->
      kd.slice([[2, 1, None, 0], [0, None, 1, 2]])

  kd.ordinal_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.ordinal_rank(x, ndim=2) -> kd.slice([[0, 3, None, 5], [4, None, 2, 1]])

Args:
  x: DataSlice to rank.
  tie_breaker: If specified, used to break ties. If `tie_breaker` does not
    fully resolve all ties, then the remaining ties are resolved by their
    positions in the DataSlice.
  descending: If true, items are compared in descending order. Does not affect
    the order of tie breaker and position in tie-breaking compairson.
  ndim: The number of dimensions to rank over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  A DataSlice of ordinal ranks.
```

### `kd.slices.range(start, end=unspecified)` {#kd.slices.range}
Aliases:

- [kd.range](#kd.range)

``` {.no-copy}
Returns a DataSlice of INT64s with range [start, end).

`start` and `end` must be broadcastable to the same shape. The resulting
DataSlice has one more dimension than the broadcasted shape.

When `end` is unspecified, `start` is used as `end` and 0 is used as `start`.
For example,

  kd.range(5) -> kd.slice([0, 1, 2, 3, 4])
  kd.range(2, 5) -> kd.slice([2, 3, 4])
  kd.range(5, 2) -> kd.slice([])  # empty range
  kd.range(kd.slice([2, 4])) -> kd.slice([[0, 1], [0, 1, 2, 3])
  kd.range(kd.slice([2, 4]), 6) -> kd.slice([[2, 3, 4, 5], [4, 5])

Args:
  start: A DataSlice for start (inclusive) of intervals (unless `end` is
    unspecified, in which case this parameter is used as `end`).
  end: A DataSlice for end (exclusive) of intervals.

Returns:
  A DataSlice of INT64s with range [start, end).
```

### `kd.slices.repeat(x, sizes)` {#kd.slices.repeat}
Aliases:

- [kd.repeat](#kd.repeat)

``` {.no-copy}
Returns `x` with values repeated according to `sizes`.

The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
broadcasted to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.repeat(ds, sizes)  # -> kd.slice([[[1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.repeat(ds, sizes)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.repeat(ds, size)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `kd.slices.repeat_present(x, sizes)` {#kd.slices.repeat_present}
Aliases:

- [kd.repeat_present](#kd.repeat_present)

``` {.no-copy}
Returns `x` with present values repeated according to `sizes`.

The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
broadcasted to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.repeat_present(ds, sizes)  # -> kd.slice([[[1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.repeat_present(ds, sizes)  # -> kd.slice([[[1, 1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.repeat_present(ds, size)  # -> kd.slice([[[1, 1], []], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `kd.slices.reverse(ds)` {#kd.slices.reverse}
Aliases:

- [kd.reverse](#kd.reverse)

``` {.no-copy}
Returns a DataSlice with items reversed on the last dimension.

Example:
  ds = kd.slice([[1, None], [2, 3, 4]])
  kd.reverse(ds) -> [[None, 1], [4, 3, 2]]

  ds = kd.slice([1, None, 2])
  kd.reverse(ds) -> [2, None, 1]

Args:
  ds: DataSlice to be reversed.

Returns:
  Reversed on the last dimension DataSlice.
```

### `kd.slices.reverse_select(ds, fltr)` {#kd.slices.reverse_select}

Alias for [kd.slices.inverse_select](#kd.slices.inverse_select) operator.

### `kd.slices.select(ds, fltr, expand_filter=DataItem(True, schema: BOOLEAN))` {#kd.slices.select}
Aliases:

- [kd.select](#kd.select)

``` {.no-copy}
Creates a new DataSlice by filtering out missing items in fltr.

The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
missing items in `fltr` are removed. The last dimension of the resulting
DataSlice is changed while the first N-1 dimensions are the same as those in
`ds`.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select(val, val > 3) -> [[4], [], [8]]

  fltr = kd.slice(
      [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
  kd.select(val, fltr) -> [[None, 4], [None], [2]]

  fltr = kd.slice([kd.present, kd.present, None])
  kd.select(val, fltr) -> [[1, None, 4], [None], []]
  kd.select(val, fltr, expand_filter=False) -> [[1, None, 4], [None]]

Args:
  ds: DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK.
  expand_filter: flag indicating if the 'filter' should be expanded to 'ds'

Returns:
  Filtered DataSlice.
```

### `kd.slices.select_present(ds)` {#kd.slices.select_present}
Aliases:

- [kd.select_present](#kd.select_present)

``` {.no-copy}
Creates a new DataSlice by removing missing items.
```

### `kd.slices.size(x)` {#kd.slices.size}
Aliases:

- [kd.size](#kd.size)

``` {.no-copy}
Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.
```

### `kd.slices.slice(x, /, schema=None)` {#kd.slices.slice}
Aliases:

- [kd.slice](#kd.slice)

- [DataSlice.from_vals](#DataSlice.from_vals)

``` {.no-copy}
Returns a DataSlice created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`.

Args:
  x: a Python value or a DataSlice. If it is a (nested) Python list or tuple,
    a multidimensional DataSlice is created.
  schema: schema DataItem to set. If `x` is already a DataSlice, this will
    cast it to the given schema.
```

### `kd.slices.sort(x, sort_by=unspecified, descending=DataItem(False, schema: BOOLEAN))` {#kd.slices.sort}
Aliases:

- [kd.sort](#kd.sort)

``` {.no-copy}
Sorts the items in `x` over the last dimension.

When `sort_by` is specified, it is used to sort items in `x`. `sort_by` must
have the same shape as `x` and cannot be more sparse than `x`. Otherwise,
items in `x` are compared by their values. Missing items are put in the end of
the sorted list regardless of the value of `descending`.

Examples:
  ds = kd.slice([[[2, 1, None, 4], [4, 1]], [[5, 4, None]]])

  kd.sort(ds) -> kd.slice([[[1, 2, 4, None], [1, 4]], [[4, 5, None]]])

  kd.sort(ds, descending=True) ->
      kd.slice([[[4, 2, 1, None], [4, 1]], [[5, 4, None]]])

  sort_by = kd.slice([[[9, 2, 1, 3], [2, 3]], [[9, 7, 9]]])
  kd.sort(ds, sort_by) ->
      kd.slice([[[None, 1, 4, 2], [4, 1]], [[4, 5, None]]])

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4])) ->
      raise due to different shapes

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4, None])) ->
      raise as `sort_by` is more sparse than `x`

Args:
  x: DataSlice to sort.
  sort_by: DataSlice used for comparisons.
  descending: whether to do descending sort.

Returns:
  DataSlice with last dimension sorted.
```

### `kd.slices.stack(*args, ndim=DataItem(0, schema: INT32))` {#kd.slices.stack}
Aliases:

- [kd.stack](#kd.stack)

``` {.no-copy}
Stacks the given DataSlices, creating a new dimension at index `rank-ndim`.

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

kd.stack(a, b, ndim=0) -> [[[1, 7], [None, 7], [3, 7]], [[4, 7]]]
kd.stack(a, b, ndim=1) -> [[[1, None, 3], [7, 7, 7]], [[4], [7]]]
kd.stack(a, b, ndim=2) -> [[[1, None, 3], [4]], [[7, 7, 7], [7]]]
kd.stack(a, b, ndim=4) -> raise an exception
kd.stack(a, b) -> the same as kd.stack(a, b, ndim=0)

Args:
  *args: The DataSlices to stack.
  ndim: The number of last dimensions to stack (default 0).

Returns:
  The stacked DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.
```

### `kd.slices.str(x)` {#kd.slices.str}
Aliases:

- [kd.str](#kd.str)

``` {.no-copy}
Returns kd.slice(x, kd.STRING).
```

### `kd.slices.subslice(x, *slices)` {#kd.slices.subslice}
Aliases:

- [kd.subslice](#kd.subslice)

``` {.no-copy}
Slices `x` across all of its dimensions based on the provided `slices`.

`slices` is a variadic argument for slicing arguments where individual
slicing argument can be one of the following:

  1) INT32/INT64 DataItem or Python integer wrapped into INT32 DataItem. It is
     used to select a single item in one dimension. It reduces the number of
     dimensions in the resulting DataSlice by 1.
  2) INT32/INT64 DataSlice. It is used to select multiple items in one
  dimension.
  3) Python slice (e.g. slice(1), slice(1, 3), slice(2, -1)). It is used to
     select a slice of items in one dimension. 'step' is not supported and it
     results in no item if 'start' is larger than or equal to 'stop'. 'start'
     and 'stop' can be either Python integers, DataItems or DataSlices, in
     the latter case we can select a different range for different items,
     or even select multiple ranges for the same item if the 'start'
     or 'stop' have more dimensions. If an item is missing either in 'start'
     or in 'stop', the corresponding slice is considered empty.
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
    => kd.slice([[1, 3], [4], [7, 8]])

Example 2:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 0, 1, kd.item(0))
    => kd.item(3)

Example 3:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1))
    => kd.slice([[[1], []], [[4, 5]], [[], [8]]])

Example 4:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
     => kd.slice([[[2], []], [[5, 6]]])

Example 5 (also see Example 6/7 for using DataSlices for subslicing):
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), kd.slice(0))
    => kd.slice([[4, 4], [8, 7]])

Example 6:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), ...)
    => kd.slice([[[4, 5, 6]], [[7], [8, 9]]])

Example 7:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), ...)
    => kd.slice([[[4, 5, 6], [4, 5, 6]], [[8, 9], [7]]])

Example 8:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., slice(1, None))
    => kd.slice([[[2], []], [[5, 6]], [[], [9]]])

Example 9:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 2, ..., slice(1, None))
    => kd.slice([[], [9]])

Example 10:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., 2, ...)
    => error as ellipsis can only appear once

Example 11:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 1, 2, 3, 4)
    => error as at most 3 slicing arguments can be provided

Example 12:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(kd.slice([0, 1, 2]), None))
    => kd.slice([[[1, 2], [3]], [[5, 6]], [[], []]])

Example 13:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(kd.slice([0, 1, 2]), kd.slice([2, 3, None])), ...)
    => kd.slice([[[[1, 2], [3]], [[4, 5, 6]]], [[[4, 5, 6]], [[7], [8, 9]]],
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
  A DataSlice with selected items
```

### `kd.slices.take(x, indices)` {#kd.slices.take}

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.slices.tile(x, shape)` {#kd.slices.tile}
Aliases:

- [kd.tile](#kd.tile)

``` {.no-copy}
Nests the whole `x` under `shape`.

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
  Expanded DataSlice.
```

### `kd.slices.translate(keys_to, keys_from, values_from)` {#kd.slices.translate}
Aliases:

- [kd.translate](#kd.translate)

``` {.no-copy}
Translates `keys_to` based on `keys_from`->`values_from` mapping.

The translation is done by matching keys from `keys_from` to `keys_to` over
the last dimension of `keys_to`. `keys_from` cannot have duplicate keys within
each group of the last dimension. Also see kd.translate_group.

`values_from` is first broadcasted to `keys_from` and the first N-1 dimensions
of `keys_from` and `keys_to` must be the same. The resulting DataSlice has the
same shape as `keys_to` and the same DataBag as `values_from`.

`keys_from` and `keys_to` must have the same schema.

Missing items or items with no matching keys in `keys_from` result in missing
items in the resulting DataSlice.

For example:

keys_to = kd.slice([['a', 'd'], ['c', None]])
keys_from = kd.slice([['a', 'b'], ['c', None]])
values_from = kd.slice([[1, 2], [3, 4]])
kd.translate(keys_to, keys_from, values_from) ->
    kd.slice([[1, None], [3, None]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.
```

### `kd.slices.translate_group(keys_to, keys_from, values_from)` {#kd.slices.translate_group}
Aliases:

- [kd.translate_group](#kd.translate_group)

``` {.no-copy}
Translates `keys_to` based on `keys_from`->`values_from` mapping.

The translation is done by creating an additional dimension under `keys_to`
and putting items in `values_from` to this dimension by matching keys from
`keys_from` to `keys_to` over the last dimension of `keys_to`.
`keys_to` can have duplicate keys within each group of the last
dimension.

`values_from` and `keys_from` must have the same shape and the first N-1
dimensions of `keys_from` and `keys_to` must be the same. The shape of
resulting DataSlice is the combination of the shape of `keys_to` and an
injected group_by dimension.

`keys_from` and `keys_to` must have the same schema.

Missing items or items with no matching keys in `keys_from` result in empty
groups in the resulting DataSlice.

For example:

keys_to = kd.slice(['a', 'c', None, 'd', 'e'])
keys_from = kd.slice(['a', 'c', 'b', 'c', 'a', 'e'])
values_from = kd.slice([1, 2, 3, 4, 5, 6])
kd.translate_group(keys_to, keys_from, values_from) ->
  kd.slice([[1, 5], [2, 4], [], [], [6]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.
```

### `kd.slices.unique(x, sort=DataItem(False, schema: BOOLEAN))` {#kd.slices.unique}
Aliases:

- [kd.unique](#kd.unique)

``` {.no-copy}
Returns a DataSlice with unique values within each dimension.

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
  last dimension.
```

### `kd.slices.val_like(x, val)` {#kd.slices.val_like}
Aliases:

- [kd.val_like](#kd.val_like)

``` {.no-copy}
Creates a DataSlice with `val` masked and expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.slices.val_like(x, 1) -> kd.slice([[1], [1, None]])
  kd.slices.val_like(x, kd.slice([1, 2])) -> kd.slice([[1], [2, None]])
  kd.slices.val_like(x, kd.slice([None, 2])) -> kd.slice([[None], [2, None]])

Args:
  x: DataSlice to match the shape and sparsity of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x` and masked by `x`.
```

### `kd.slices.val_shaped(shape, val)` {#kd.slices.val_shaped}
Aliases:

- [kd.val_shaped](#kd.val_shaped)

``` {.no-copy}
Creates a DataSlice with `val` expanded to the given shape.

Example:
  shape = kd.shapes.new([2], [1, 2])
  kd.slices.val_shaped(shape, 1) -> kd.slice([[1], [1, 1]])
  kd.slices.val_shaped(shape, kd.slice([None, 2])) -> kd.slice([[None], [2,
  2]])

Args:
  shape: shape to expand to.
  val: value to expand.

Returns:
  A DataSlice with the same shape as `shape`.
```

### `kd.slices.val_shaped_as(x, val)` {#kd.slices.val_shaped_as}
Aliases:

- [kd.val_shaped_as](#kd.val_shaped_as)

``` {.no-copy}
Creates a DataSlice with `val` expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.slices.val_shaped_as(x, 1) -> kd.slice([[1], [1, 1]])
  kd.slices.val_shaped_as(x, kd.slice([None, 2])) -> kd.slice([[None], [2,
  2]])

Args:
  x: DataSlice to match the shape of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x`.
```

### `kd.slices.zip(*args)` {#kd.slices.zip}
Aliases:

- [kd.zip](#kd.zip)

``` {.no-copy}
Zips the given DataSlices into a new DataSlice with a new last dimension.

Input DataSlices are automatically aligned. The result has the shape of the
aligned inputs, plus a new last dimension with uniform shape `len(args)`
containing the values from each input.

For example,
a = kd.slice([1, 2, 3, 4])
b = kd.slice([5, 6, 7, 8])
c = kd.slice(['a', 'b', 'c', 'd'])
kd.zip(a, b, c) -> [[1, 5, 'a'], [2, 6, 'b'], [3, 7, 'c'], [4, 8, 'd']]

a = kd.slice([[1, None, 3], [4]])
b = kd.slice([7, None])
kd.zip(a, b) ->  [[[1, 7], [None, 7], [3, 7]], [[4, None]]]

Args:
  *args: The DataSlices to zip.

Returns:
  The zipped DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.
```

</section>

### kd.strings {#kd.strings}

Operators that work with strings data.

<section class="zippy closed">

**Operators**

### `kd.strings.agg_join(x, sep=DataItem(None, schema: NONE), ndim=unspecified)` {#kd.strings.agg_join}

``` {.no-copy}
Returns a DataSlice of strings joined on last ndim dimensions.

Example:
  ds = kd.slice([['el', 'psy', 'congroo'], ['a', 'b', 'c']))
  kd.agg_join(ds, ' ')  # -> kd.slice(['el psy congroo', 'a b c'])
  kd.agg_join(ds, ' ', ndim=2)  # -> kd.slice('el psy congroo a b c')

Args:
  x: String or bytes DataSlice
  sep: If specified, will join by the specified string, otherwise will be
    empty string.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `kd.strings.contains(s, substr)` {#kd.strings.contains}

``` {.no-copy}
Returns present iff `s` contains `substr`.

Examples:
  kd.strings.constains(kd.slice(['Hello', 'Goodbye']), 'lo')
    # -> kd.slice([kd.present, kd.missing])
  kd.strings.contains(
    kd.slice([b'Hello', b'Goodbye']),
    kd.slice([b'lo', b'Go']))
    # -> kd.slice([kd.present, kd.present])

Args:
  s: The strings to consider. Must have schema STRING or BYTES.
  substr: The substrings to look for in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of present/missing values with schema MASK.
```

### `kd.strings.count(s, substr)` {#kd.strings.count}

``` {.no-copy}
Counts the number of occurrences of `substr` in `s`.

Examples:
  kd.strings.count(kd.slice(['Hello', 'Goodbye']), 'l')
    # -> kd.slice([2, 0])
  kd.strings.count(
    kd.slice([b'Hello', b'Goodbye']),
    kd.slice([b'Hell', b'o']))
    # -> kd.slice([1, 2])

Args:
  s: The strings to consider.
  substr: The substrings to count in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of INT32 counts.
```

### `kd.strings.decode(x)` {#kd.strings.decode}

``` {.no-copy}
Decodes `x` as STRING using UTF-8 decoding.
```

### `kd.strings.decode_base64(x, /, *, on_invalid=unspecified)` {#kd.strings.decode_base64}

``` {.no-copy}
Decodes BYTES from `x` using base64 encoding (RFC 4648 section 4).

The input strings may either have no padding, or must have the correct amount
of padding. ASCII whitespace characters anywhere in the string are ignored.

Args:
  x: DataSlice of STRING or BYTES containing base64-encoded strings.
  on_invalid: If unspecified (the default), any invalid base64 strings in `x`
    will cause an error. Otherwise, this must be a DataSlice broadcastable to
    `x` with a schema compatible with BYTES, and will be used in the result
    wherever the input string was not valid base64.

Returns:
  DataSlice of BYTES.
```

### `kd.strings.encode(x)` {#kd.strings.encode}

``` {.no-copy}
Encodes `x` as BYTES using UTF-8 encoding.
```

### `kd.strings.encode_base64(x)` {#kd.strings.encode_base64}

``` {.no-copy}
Encodes BYTES `x` using base64 encoding (RFC 4648 section 4), with padding.

Args:
  x: DataSlice of BYTES to encode.

Returns:
  DataSlice of STRING.
```

### `kd.strings.find(s, substr, start=DataItem(0, schema: INT64), end=DataItem(None, schema: INT64))` {#kd.strings.find}

``` {.no-copy}
Returns the offset of the first occurrence of `substr` in `s`.

The units of `start`, `end`, and the return value are all byte offsets if `s`
is `BYTES` and codepoint offsets if `s` is `STRING`.

Args:
 s: (STRING or BYTES) Strings to search in.
 substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
   dtype as `s`.
 start: (optional int) Offset to start the search, defaults to 0.
 end: (optional int) Offset to stop the search, defaults to end of the string.

Returns:
  The offset of the last occurrence of `substr` in `s`, or missing if there
  are no occurrences.
```

### `kd.strings.format(fmt, /, **kwargs)` {#kd.strings.format}
Aliases:

- [kd.format](#kd.format)

``` {.no-copy}
Formats strings according to python str.format style.

Format support is slightly different from Python:
1. {x:v} is equivalent to {x} and supported for all types as default string
   format.
2. Only float and integers support other format specifiers.
  E.g., {x:.1f} and {x:04d}.
3. If format is missing type specifier `f` or `d` at the end, we are
   adding it automatically based on the type of the argument.

Note: only keyword arguments are supported.

Examples:
  kd.strings.format(kd.slice(['Hello {n}!', 'Goodbye {n}!']), n='World')
    # -> kd.slice(['Hello World!', 'Goodbye World!'])
  kd.strings.format('{a} + {b} = {c}', a=1, b=2, c=3)
    # -> kd.slice('1 + 2 = 3')
  kd.strings.format(
      '{a} + {b} = {c}',
      a=kd.slice([1, 2]),
      b=kd.slice([2, 3]),
      c=kd.slice([3, 5]))
    # -> kd.slice(['1 + 2 = 3', '2 + 3 = 5'])
  kd.strings.format(
      '({a:03} + {b:e}) * {c:.2f} ='
      ' {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}'
      a=5, b=5.7, c=75)
    # -> kd.slice(
    #        '(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000')

Args:
  fmt: Format string (String or Bytes).
  **kwargs: Arguments to format.

Returns:
  The formatted string.
```

### `kd.strings.fstr(x)` {#kd.strings.fstr}
Aliases:

- [kd.fstr](#kd.fstr)

``` {.no-copy}
Evaluates Koladata f-string into DataSlice.

  f-string must be created via Python f-string syntax. It must contain at least
  one formatted DataSlice.
  Each DataSlice must have custom format specification,
  e.g. `{ds:s}` or `{ds:.2f}`.
  Find more about format specification in kd.strings.format docs.

  NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

  Examples:
    countries = kd.slice(['USA', 'Schweiz'])
    kd.fstr(f'Hello, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Hello, Schweiz!'])

    greetings = kd.slice(['Hello', 'Gruezi'])
    kd.fstr(f'{greetings:s}, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Gruezi, Schweiz!'])

    states = kd.slice([['California', 'Arizona', 'Nevada'], ['Zurich', 'Bern']])
    kd.fstr(f'{greetings:s}, {states:s} in {countries:s}!')
      # -> kd.slice([
               ['Hello, California in USA!',
                'Hello, Arizona in USA!',
                'Hello, Nevada in USA!'],
               ['Gruezi, Zurich in Schweiz!',
                'Gruezi, Bern in Schweiz!']]),

    prices = kd.slice([35.5, 49.2])
    currencies = kd.slice(['USD', 'CHF'])
    kd.fstr(f'Lunch price in {countries:s} is {prices:.2f} {currencies:s}.')
      # -> kd.slice(['Lunch price in USA is 35.50 USD.',
                     'Lunch price in Schweiz is 49.20 CHF.'])

  Args:
    s: f-string to evaluate.
  Returns:
    DataSlice with evaluated f-string.
```

### `kd.strings.join(*args)` {#kd.strings.join}

``` {.no-copy}
Concatenates the given strings.

Examples:
  kd.strings.join(kd.slice(['Hello ', 'Goodbye ']), 'World')
    # -> kd.slice(['Hello World', 'Goodbye World'])
  kd.strings.join(kd.slice([b'foo']), kd.slice([b' ']), kd.slice([b'bar']))
    # -> kd.slice([b'foo bar'])

Args:
  *args: The inputs to concatenate in the given order.

Returns:
  The string concatenation of all the inputs.
```

### `kd.strings.length(x)` {#kd.strings.length}

``` {.no-copy}
Returns a DataSlice of lengths in bytes for Byte or codepoints for String.

For example,
  kd.strings.length(kd.slice(['abc', None, ''])) -> kd.slice([3, None, 0])
  kd.strings.length(kd.slice([b'abc', None, b''])) -> kd.slice([3, None, 0])
  kd.strings.length(kd.item('你好')) -> kd.item(2)
  kd.strings.length(kd.item('你好'.encode())) -> kd.item(6)

Note that the result DataSlice always has INT32 schema.

Args:
  x: String or Bytes DataSlice.

Returns:
  A DataSlice of lengths.
```

### `kd.strings.lower(x)` {#kd.strings.lower}

``` {.no-copy}
Returns a DataSlice with the lowercase version of each string in the input.

For example,
  kd.strings.lower(kd.slice(['AbC', None, ''])) -> kd.slice(['abc', None, ''])
  kd.strings.lower(kd.item('FOO')) -> kd.item('foo')

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of lowercase strings.
```

### `kd.strings.lstrip(s, chars=DataItem(None, schema: NONE))` {#kd.strings.lstrip}

``` {.no-copy}
Strips whitespaces or the specified characters from the left side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading characters from `s`
that are present in the `chars` set.

Examples:
  kd.strings.lstrip(kd.slice(['   spacious   ', '\t text \n']))
    # -> kd.slice(['spacious   ', 'text \n'])
  kd.strings.lstrip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
    # -> kd.slice(['example.com'])
  kd.strings.lstrip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
      kd.slice('.#! '))
    # -> kd.slice([['Section 3.1 Issue #32 ...'], ['']])

Args:
  s: (STRING or BYTES) Original string.
  chars: (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.
```

### `kd.strings.printf(fmt, *args)` {#kd.strings.printf}

``` {.no-copy}
Formats strings according to printf-style (C++) format strings.

See absl::StrFormat documentation for the format string details.

Example:
  kd.strings.printf(kd.slice(['Hello %s!', 'Goodbye %s!']), 'World')
    # -> kd.slice(['Hello World!', 'Goodbye World!'])
  kd.strings.printf('%v + %v = %v', 1, 2, 3)  # -> kd.slice('1 + 2 = 3')

Args:
  fmt: Format string (String or Bytes).
  *args: Arguments to format (primitive types compatible with `fmt`).

Returns:
  The formatted string.
```

### `kd.strings.regex_extract(text, regex)` {#kd.strings.regex_extract}

``` {.no-copy}
Extracts a substring from `text` with the capturing group of `regex`.

Regular expression matches are partial, which means `regex` is matched against
a substring of `text`.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.
The pattern must contain exactly one capturing group.

Examples:
  kd.strings.regex_extract(kd.item('foo'), kd.item('f(.)'))
    # kd.item('o')
  kd.strings.regex_extract(kd.item('foobar'), kd.item('o(..)'))
    # kd.item('ob')
  kd.strings.regex_extract(kd.item('foobar'), kd.item('^o(..)$'))
    # kd.item(None).with_schema(kd.STRING)
  kd.strings.regex_extract(kd.item('foobar'), kd.item('^.o(..)a.$'))
    # kd.item('ob')
  kd.strings.regex_extract(kd.item('foobar'), kd.item('.*(b.*r)$'))
    # kd.item('bar')
  kd.strings.regex_extract(kd.slice(['abcd', None, '']), kd.slice('b(.*)'))
    # -> kd.slice(['cd', None, None])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax) with exactly one capturing group.

Returns:
  For the first partial match of `regex` and `text`, returns the substring of
  `text` that matches the capturing group of `regex`.
```

### `kd.strings.regex_match(text, regex)` {#kd.strings.regex_match}

``` {.no-copy}
Returns `present` if `text` matches the regular expression `regex`.

Matches are partial, which means a substring of `text` matches the pattern.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.

Examples:
  kd.strings.regex_match(kd.item('foo'), kd.item('oo'))
    # -> kd.present
  kd.strings.regex_match(kd.item('foo'), '^oo$')
    # -> kd.missing
  kd.strings.regex_match(kd.item('foo), '^foo$')
    # -> kd.present
  kd.strings.regex_match(kd.slice(['abc', None, '']), 'b')
    # -> kd.slice([kd.present, kd.missing, kd.missing])
  kd.strings.regex_match(kd.slice(['abcd', None, '']), kd.slice('b.d'))
    # -> kd.slice([kd.present, kd.missing, kd.missing])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).

Returns:
  `present` if `text` matches `regex`.
```

### `kd.strings.replace(s, old, new, max_subs=DataItem(None, schema: INT32))` {#kd.strings.replace}

``` {.no-copy}
Replaces up to `max_subs` occurrences of `old` within `s` with `new`.

If `max_subs` is missing or negative, then there is no limit on the number of
substitutions. If it is zero, then `s` is returned unchanged.

If the search string is empty, the original string is fenced with the
replacement string, for example: replace("ab", "", "-") returns "-a-b-". That
behavior is similar to Python's string replace.

Args:
 s: (STRING or BYTES) Original string.
 old: (STRING or BYTES, the same as `s`) String to replace.
 new: (STRING or BYTES, the same as `s`) Replacement string.
 max_subs: (optional INT32) Max number of substitutions. If unspecified or
   negative, then there is no limit on the number of substitutions.

Returns:
  String with applied substitutions.
```

### `kd.strings.rfind(s, substr, start=DataItem(0, schema: INT64), end=DataItem(None, schema: INT64))` {#kd.strings.rfind}

``` {.no-copy}
Returns the offset of the last occurrence of `substr` in `s`.

The units of `start`, `end`, and the return value are all byte offsets if `s`
is `BYTES` and codepoint offsets if `s` is `STRING`.

Args:
 s: (STRING or BYTES) Strings to search in.
 substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
   dtype as `s`.
 start: (optional int) Offset to start the search, defaults to 0.
 end: (optional int) Offset to stop the search, defaults to end of the string.

Returns:
  The offset of the last occurrence of `substr` in `s`, or missing if there
  are no occurrences.
```

### `kd.strings.rstrip(s, chars=DataItem(None, schema: NONE))` {#kd.strings.rstrip}

``` {.no-copy}
Strips whitespaces or the specified characters from the right side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all tailing characters from `s` that
are present in the `chars` set.

Examples:
  kd.strings.rstrip(kd.slice(['   spacious   ', '\t text \n']))
    # -> kd.slice(['   spacious', '\t text'])
  kd.strings.rstrip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
    # -> kd.slice(['www.example'])
  kd.strings.rstrip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
      kd.slice('.#! '))
    # -> kd.slice([['#... Section 3.1 Issue #32'], ['']])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.
```

### `kd.strings.split(x, sep=DataItem(None, schema: NONE))` {#kd.strings.split}

``` {.no-copy}
Returns x split by the provided separator.

Example:
  ds = kd.slice(['Hello world!', 'Goodbye world!'])
  kd.split(ds)  # -> kd.slice([['Hello', 'world!'], ['Goodbye', 'world!']])

Args:
  x: DataSlice: (can be text or bytes)
  sep: If specified, will split by the specified string not omitting empty
    strings, otherwise will split by whitespaces while omitting empty strings.
```

### `kd.strings.strip(s, chars=DataItem(None, schema: NONE))` {#kd.strings.strip}

``` {.no-copy}
Strips whitespaces or the specified characters from both sides of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading and tailing characters
from `s` that are present in the `chars` set.

Examples:
  kd.strings.strip(kd.slice(['   spacious   ', '\t text \n']))
    # -> kd.slice(['spacious', 'text'])
  kd.strings.strip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
    # -> kd.slice(['example'])
  kd.strings.strip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
      kd.slice('.#! '))
    # -> kd.slice([['Section 3.1 Issue #32'], ['']])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.
```

### `kd.strings.substr(x, start=DataItem(0, schema: INT64), end=DataItem(None, schema: INT64))` {#kd.strings.substr}

``` {.no-copy}
Returns a DataSlice of substrings with indices [start, end).

The usual Python rules apply:
  * A negative index is computed from the end of the string.
  * An empty range yields an empty string, for example when start >= end and
    both are positive.

The result is broadcasted to the common shape of all inputs.

Examples:
  ds = kd.slice([['Hello World!', 'Ciao bella'], ['Dolly!']])
  kd.substr(ds)         # -> kd.slice([['Hello World!', 'Ciao bella'],
                                       ['Dolly!']])
  kd.substr(ds, 5)      # -> kd.slice([[' World!', 'bella'], ['!']])
  kd.substr(ds, -2)     # -> kd.slice([['d!', 'la'], ['y!']])
  kd.substr(ds, 1, 5)   # -> kd.slice([['ello', 'iao '], ['olly']])
  kd.substr(ds, 5, -1)  # -> kd.slice([[' World', 'bell'], ['']])
  kd.substr(ds, 4, 100) # -> kd.slice([['o World!', ' bella'], ['y!']])
  kd.substr(ds, -1, -2) # -> kd.slice([['', ''], ['']])
  kd.substr(ds, -2, -1) # -> kd.slice([['d', 'l'], ['y']])

  # Start and end may also be multidimensional.
  ds = kd.slice('Hello World!')
  start = kd.slice([1, 2])
  end = kd.slice([[2, 3], [4]])
  kd.substr(ds, start, end) # -> kd.slice([['e', 'el'], ['ll']])

Args:
  x: Text or Bytes DataSlice. If text, then `start` and `end` are codepoint
    offsets. If bytes, then `start` and `end` are byte offsets.
  start: The start index of the substring. Inclusive. Assumed to be 0 if
    unspecified.
  end: The end index of the substring. Exclusive. Assumed to be the length of
    the string if unspecified.
```

### `kd.strings.upper(x)` {#kd.strings.upper}

``` {.no-copy}
Returns a DataSlice with the uppercase version of each string in the input.

For example,
  kd.strings.upper(kd.slice(['abc', None, ''])) -> kd.slice(['ABC', None, ''])
  kd.strings.upper(kd.item('foo')) -> kd.item('FOO')

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of uppercase strings.
```

</section>

### kd.tuple {#kd.tuple}

Operators to create tuples.

<section class="zippy closed">

**Operators**

### `kd.tuple.get_namedtuple_field(namedtuple, field_name)` {#kd.tuple.get_namedtuple_field}

``` {.no-copy}
Returns the value of the specified `field_name` from the `namedtuple`.

Note that `field_name` _must_ be a literal (foldable) string.

Args:
  namedtuple: a namedtuple.
  field_name: the name of the field to return. _Must_ be a literal (foldable)
    string.
```

### `kd.tuple.get_nth(x, n)` {#kd.tuple.get_nth}

``` {.no-copy}
Returns the nth element of the tuple `x`.

Note that `n` _must_ be a literal integer in [0, len(x)).

Args:
  x: a tuple.
  n: the index of the element to return. _Must_ be a literal integer in the
    range [0, len(x)).
```

### `kd.tuple.make_namedtuple(**kwargs)` {#kd.tuple.make_namedtuple}
Aliases:

- [kd.make_namedtuple](#kd.make_namedtuple)

``` {.no-copy}
Returns a namedtuple-like object containing the given `**kwargs`.
```

### `kd.tuple.make_slice(start=unspecified, stop=unspecified, step=unspecified)` {#kd.tuple.make_slice}

``` {.no-copy}
Returns a slice for the Python indexing syntax foo[start:stop:step].

Args:
  start: (optional) Indexing start.
  stop: (optional) Indexing stop.
  step: (optional) Indexing step size.
```

### `kd.tuple.make_tuple(*args)` {#kd.tuple.make_tuple}
Aliases:

- [kd.make_tuple](#kd.make_tuple)

``` {.no-copy}
Returns a tuple-like object containing the given `*args`.
```

</section>
</section>
<section class="zippy closed">

**Operators**

### `kd.add(x, y)` {#kd.add}

Alias for [kd.math.add](#kd.math.add) operator.

### `kd.agg_all(x, ndim=unspecified)` {#kd.agg_all}

Alias for [kd.masking.agg_all](#kd.masking.agg_all) operator.

### `kd.agg_any(x, ndim=unspecified)` {#kd.agg_any}

Alias for [kd.masking.agg_any](#kd.masking.agg_any) operator.

### `kd.agg_count(x, ndim=unspecified)` {#kd.agg_count}

Alias for [kd.slices.agg_count](#kd.slices.agg_count) operator.

### `kd.agg_has(x, ndim=unspecified)` {#kd.agg_has}

Alias for [kd.masking.agg_has](#kd.masking.agg_has) operator.

### `kd.agg_max(x, ndim=unspecified)` {#kd.agg_max}

Alias for [kd.math.agg_max](#kd.math.agg_max) operator.

### `kd.agg_min(x, ndim=unspecified)` {#kd.agg_min}

Alias for [kd.math.agg_min](#kd.math.agg_min) operator.

### `kd.agg_size(x, ndim=unspecified)` {#kd.agg_size}

Alias for [kd.slices.agg_size](#kd.slices.agg_size) operator.

### `kd.agg_sum(x, ndim=unspecified)` {#kd.agg_sum}

Alias for [kd.math.agg_sum](#kd.math.agg_sum) operator.

### `kd.agg_uuid(x, ndim=unspecified)` {#kd.agg_uuid}

Alias for [kd.ids.agg_uuid](#kd.ids.agg_uuid) operator.

### `kd.align(*args)` {#kd.align}

Alias for [kd.slices.align](#kd.slices.align) operator.

### `kd.all(x)` {#kd.all}

Alias for [kd.masking.all](#kd.masking.all) operator.

### `kd.any(x)` {#kd.any}

Alias for [kd.masking.any](#kd.masking.any) operator.

### `kd.appended_list(x, append)` {#kd.appended_list}

Alias for [kd.lists.appended_list](#kd.lists.appended_list) operator.

### `kd.apply_mask(x, y)` {#kd.apply_mask}

Alias for [kd.masking.apply_mask](#kd.masking.apply_mask) operator.

### `kd.apply_py(fn, *args, return_type_as=unspecified, **kwargs)` {#kd.apply_py}

Alias for [kd.py.apply_py](#kd.py.apply_py) operator.

### `kd.apply_py_on_cond(yes_fn, no_fn, cond, *args, **kwargs)` {#kd.apply_py_on_cond}

Alias for [kd.py.apply_py_on_cond](#kd.py.apply_py_on_cond) operator.

### `kd.apply_py_on_selected(fn, cond, *args, **kwargs)` {#kd.apply_py_on_selected}

Alias for [kd.py.apply_py_on_selected](#kd.py.apply_py_on_selected) operator.

### `kd.as_any(x)` {#kd.as_any}

Alias for [kd.schema.as_any](#kd.schema.as_any) operator.

### `kd.at(x, indices)` {#kd.at}

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.attr(x, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#kd.attr}

Alias for [kd.core.attr](#kd.core.attr) operator.

### `kd.attrs(x, /, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#kd.attrs}

Alias for [kd.core.attrs](#kd.core.attrs) operator.

### `kd.bag()` {#kd.bag}

Alias for [kd.bags.new](#kd.bags.new) operator.

### `kd.bind(fn_def, /, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **kwargs)` {#kd.bind}

Alias for [kd.functor.bind](#kd.functor.bind) operator.

### `kd.bool(x)` {#kd.bool}

Alias for [kd.slices.bool](#kd.slices.bool) operator.

### `kd.bytes(x)` {#kd.bytes}

Alias for [kd.slices.bytes](#kd.slices.bytes) operator.

### `kd.call(fn, *args, return_type_as=DataItem(None, schema: NONE), **kwargs)` {#kd.call}

Alias for [kd.functor.call](#kd.functor.call) operator.

### `kd.cast_to(x, schema)` {#kd.cast_to}

Alias for [kd.schema.cast_to](#kd.schema.cast_to) operator.

### `kd.clear_eval_cache()` {#kd.clear_eval_cache}

``` {.no-copy}
Clears Koda specific eval caches.
```

### `kd.clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.clone}

Alias for [kd.core.clone](#kd.core.clone) operator.

### `kd.coalesce(x, y)` {#kd.coalesce}

Alias for [kd.masking.coalesce](#kd.masking.coalesce) operator.

### `kd.collapse(x, ndim=unspecified)` {#kd.collapse}

Alias for [kd.slices.collapse](#kd.slices.collapse) operator.

### `kd.concat(*args, ndim=DataItem(1, schema: INT32))` {#kd.concat}

Alias for [kd.slices.concat](#kd.slices.concat) operator.

### `kd.concat_lists(*lists, db=None)` {#kd.concat_lists}

Alias for [kd.lists.concat](#kd.lists.concat) operator.

### `kd.cond(condition, yes, no=DataItem(None, schema: NONE))` {#kd.cond}

Alias for [kd.masking.cond](#kd.masking.cond) operator.

### `kd.container(*, db=None, **attrs)` {#kd.container}

Alias for [kd.core.container](#kd.core.container) operator.

### `kd.count(x)` {#kd.count}

Alias for [kd.slices.count](#kd.slices.count) operator.

### `kd.cum_count(x, ndim=unspecified)` {#kd.cum_count}

Alias for [kd.slices.cum_count](#kd.slices.cum_count) operator.

### `kd.cum_max(x, ndim=unspecified)` {#kd.cum_max}

Alias for [kd.math.cum_max](#kd.math.cum_max) operator.

### `kd.decode_itemid(ds)` {#kd.decode_itemid}

Alias for [kd.ids.decode_itemid](#kd.ids.decode_itemid) operator.

### `kd.deep_clone(x, /, schema=unspecified, **overrides)` {#kd.deep_clone}

Alias for [kd.core.deep_clone](#kd.core.deep_clone) operator.

### `kd.deep_uuid(x, /, schema=unspecified, *, seed=DataItem('', schema: STRING))` {#kd.deep_uuid}

Alias for [kd.ids.deep_uuid](#kd.ids.deep_uuid) operator.

### `kd.del_attr(x, attr_name)` {#kd.del_attr}

``` {.no-copy}
Deletes an attribute `attr_name` from `x`.
```

### `kd.dense_rank(x, descending=DataItem(False, schema: BOOLEAN), ndim=unspecified)` {#kd.dense_rank}

Alias for [kd.slices.dense_rank](#kd.slices.dense_rank) operator.

### `kd.dict(items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dict}

Alias for [kd.dicts.new](#kd.dicts.new) operator.

### `kd.dict_like(shape_and_mask_from, /, items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dict_like}

Alias for [kd.dicts.like](#kd.dicts.like) operator.

### `kd.dict_schema(key_schema, value_schema, db=None)` {#kd.dict_schema}

Alias for [kd.schema.dict_schema](#kd.schema.dict_schema) operator.

### `kd.dict_shaped(shape, /, items_or_keys=None, values=None, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dict_shaped}

Alias for [kd.dicts.shaped](#kd.dicts.shaped) operator.

### `kd.dict_shaped_as(shape_from, /, items_or_keys=None, values=None, key_schema=None, value_schema=None, schema=None, itemid=None, db=None)` {#kd.dict_shaped_as}

Alias for [kd.dicts.shaped_as](#kd.dicts.shaped_as) operator.

### `kd.dict_size(dict_slice)` {#kd.dict_size}

Alias for [kd.dicts.size](#kd.dicts.size) operator.

### `kd.dict_update(x, keys, values=unspecified)` {#kd.dict_update}

Alias for [kd.dicts.dict_update](#kd.dicts.dict_update) operator.

### `kd.dir(x)` {#kd.dir}

``` {.no-copy}
Returns a sorted list of unique attribute names of the given DataSlice.

  This is equivalent to `kd.get_attr_names(ds, intersection=True)`. For more
  finegrained control, use `kd.get_attr_names` directly instead.

  In case of OBJECT schema, attribute names are fetched from the `__schema__`
  attribute. In case of Entity schema, the attribute names are fetched from the
  schema. In case of ANY (or primitives), an empty list is returned.

  Args:
    x: A DataSlice.

  Returns:
    A list of unique attributes sorted by alphabetical order.
```

### `kd.disjoint_coalesce(x, y)` {#kd.disjoint_coalesce}

Alias for [kd.masking.disjoint_coalesce](#kd.masking.disjoint_coalesce) operator.

### `kd.dumps(x, /, *, riegeli_options='')` {#kd.dumps}

``` {.no-copy}
Serializes a DataSlice or a DataBag.

  In case of a DataSlice, we try to use `x.extract()` to avoid serializing
  unnecessary DataBag data. If this is undesirable, consider serializing the
  DataBag directly.

  Due to current limitations of the underlying implementation, this can
  only serialize data slices with up to roughly 10**8 items.

  Args:
    x: DataSlice or DataBag to serialize.
    riegeli_options: A string with riegeli/records writer options. See
      https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
        for details. If not provided, default options will be used.

  Returns:
    Serialized data.
```

### `kd.embed_schema(x)` {#kd.embed_schema}

``` {.no-copy}
Returns a DataSlice with OBJECT schema.

  * For primitives no data change is done.
  * For Entities schema is stored as '__schema__' attribute.
  * Embedding Entities requires a DataSlice to be associated with a DataBag.

  Args:
    x: (DataSlice) whose schema is embedded.
```

### `kd.empty_shaped(shape, /, *, schema=DataItem(MASK, schema: SCHEMA), db=None)` {#kd.empty_shaped}

Alias for [kd.masking.empty_shaped](#kd.masking.empty_shaped) operator.

### `kd.empty_shaped_as(shape_from, /, *, schema=DataItem(MASK, schema: SCHEMA), db=None)` {#kd.empty_shaped_as}

Alias for [kd.masking.empty_shaped_as](#kd.masking.empty_shaped_as) operator.

### `kd.encode_itemid(ds)` {#kd.encode_itemid}

Alias for [kd.ids.encode_itemid](#kd.ids.encode_itemid) operator.

### `kd.enriched(ds, *bag)` {#kd.enriched}

Alias for [kd.core.enriched](#kd.core.enriched) operator.

### `kd.enriched_bag(*bags)` {#kd.enriched_bag}

Alias for [kd.bags.enriched](#kd.bags.enriched) operator.

### `kd.equal(x, y)` {#kd.equal}

Alias for [kd.comparison.equal](#kd.comparison.equal) operator.

### `kd.eval(expr, self_input=DataItem(Entity(self_not_specified=present), schema: SCHEMA(self_not_specified=MASK)), /, **input_values)` {#kd.eval}

``` {.no-copy}
Returns the expr evaluated on the given `input_values`.

  Only Koda Inputs from container `I` (e.g. `I.x`) can be evaluated. Other
  input types must be substituted before calling this function.

  Args:
    expr: Koda expression with inputs from container `I`.
    self_input: The value for I.self input. When not provided, it will still
      have a default value that can be passed to a subroutine.
    **input_values: Values to evaluate `expr` with. Note that all inputs in
      `expr` must be present in the input values. All input values should either
      be DataSlices or convertible to DataSlices.
```

### `kd.expand_to(x, target, ndim=unspecified)` {#kd.expand_to}

Alias for [kd.slices.expand_to](#kd.slices.expand_to) operator.

### `kd.expand_to_shape(x, shape, ndim=unspecified)` {#kd.expand_to_shape}

Alias for [kd.shapes.expand_to_shape](#kd.shapes.expand_to_shape) operator.

### `kd.explode(x, ndim=DataItem(1, schema: INT64))` {#kd.explode}

Alias for [kd.lists.explode](#kd.lists.explode) operator.

### `kd.expr_quote(x)` {#kd.expr_quote}

Alias for [kd.slices.expr_quote](#kd.slices.expr_quote) operator.

### `kd.extract(ds, schema=unspecified)` {#kd.extract}

Alias for [kd.core.extract](#kd.core.extract) operator.

### `kd.extract_bag(ds, schema=unspecified)` {#kd.extract_bag}

Alias for [kd.core.extract_bag](#kd.core.extract_bag) operator.

### `kd.flatten(x, from_dim=DataItem(0, schema: INT64), to_dim=unspecified)` {#kd.flatten}

Alias for [kd.shapes.flatten](#kd.shapes.flatten) operator.

### `kd.float32(x)` {#kd.float32}

Alias for [kd.slices.float32](#kd.slices.float32) operator.

### `kd.float64(x)` {#kd.float64}

Alias for [kd.slices.float64](#kd.slices.float64) operator.

### `kd.fn(f, *, use_tracing=True, **kwargs)` {#kd.fn}

Alias for [kd.functor.fn](#kd.functor.fn) operator.

### `kd.follow(x)` {#kd.follow}

Alias for [kd.core.follow](#kd.core.follow) operator.

### `kd.format(fmt, /, **kwargs)` {#kd.format}

Alias for [kd.strings.format](#kd.strings.format) operator.

### `kd.freeze(x)` {#kd.freeze}

Alias for [kd.core.freeze](#kd.core.freeze) operator.

### `kd.freeze_bag(x)` {#kd.freeze_bag}

Alias for [kd.core.freeze_bag](#kd.core.freeze_bag) operator.

### `kd.from_json(x, /, schema=DataItem(OBJECT, schema: SCHEMA), default_number_schema=DataItem(OBJECT, schema: SCHEMA), *, on_invalid=DataSlice([], schema: NONE, ndims: 1, size: 0), keys_attr=DataItem('json_object_keys', schema: STRING), values_attr=DataItem('json_object_values', schema: STRING))` {#kd.from_json}

Alias for [kd.json.from_json](#kd.json.from_json) operator.

### `kd.from_proto(messages, /, *, extensions=None, itemid=None, schema=None, db=None)` {#kd.from_proto}

``` {.no-copy}
Returns a DataSlice representing proto data.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures: objects/entities, primitives, lists, and dicts,
  respectively. Enums are converted to INT32. The attribute names on the Koda
  objects match the field names in the proto definition. See below for methods
  to convert proto extensions to attributes alongside regular fields.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures. Enums are converted to ints.

  Only present values in `messages` are added. Default and missing values are
  not used.

  Proto extensions are ignored by default unless `extensions` is specified (or
  if an explicit entity schema with parenthesized attrs is used).
  The format of each extension specified in `extensions` is a dot-separated
  sequence of field names and/or extension names, where extension names are
  fully-qualified extension paths surrounded by parentheses. This sequence of
  fields and extensions is traversed during conversion, in addition to the
  default behavior of traversing all fields. For example:

    "path.to.field.(package_name.some_extension)"
    "path.to.repeated_field.(package_name.some_extension)"
    "path.to.map_field.values.(package_name.some_extension)"
    "path.(package_name.some_extension).(package_name2.nested_extension)"

  Extensions are looked up using the C++ generated descriptor pool, using
  `DescriptorPool::FindExtensionByName`, which requires that all extensions are
  compiled in as C++ protos. The Koda attribute names for the extension fields
  are parenthesized fully-qualified extension paths (e.g.
  "(package_name.some_extension)" or
  "(package_name.SomeMessage.some_extension)".) As the names contain '()' and
  '.' characters, they cannot be directly accessed using '.name' syntax but can
  be accessed using `.get_attr(name)'. For example,

    ds.get_attr('(package_name.AbcExtension.abc_extension)')
    ds.optional_field.get_attr('(package_name.DefExtension.def_extension)')

  If `messages` is a single proto Message, the result is a DataItem. If it is a
  list of proto Messages, the result is an 1D DataSlice.

  Args:
    messages: Message or list of Message of the same type. Any of the messages
      may be None, which will produce missing items in the result.
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
    db: The DataBag to use for the result, or None to use a new DataBag.

  Returns:
    A DataSlice representing the proto data.
```

### `kd.from_py(py_obj, *, dict_as_obj=False, itemid=None, schema=None, from_dim=0)` {#kd.from_py}
Aliases:

- [kd.from_pytree](#kd.from_pytree)

``` {.no-copy}
Converts Python object into DataSlice.

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
    A DataItem with the converted data.
```

### `kd.from_pytree(py_obj, *, dict_as_obj=False, itemid=None, schema=None, from_dim=0)` {#kd.from_pytree}

Alias for [kd.from_py](#kd.from_py) operator.

### `kd.fstr(x)` {#kd.fstr}

Alias for [kd.strings.fstr](#kd.strings.fstr) operator.

### `kd.full_equal(x, y)` {#kd.full_equal}

Alias for [kd.comparison.full_equal](#kd.comparison.full_equal) operator.

### `kd.get_attr(x, attr_name, default=unspecified)` {#kd.get_attr}

Alias for [kd.core.get_attr](#kd.core.get_attr) operator.

### `kd.get_attr_names(x, *, intersection)` {#kd.get_attr_names}

``` {.no-copy}
Returns a sorted list of unique attribute names of the given DataSlice.

  In case of OBJECT schema, attribute names are fetched from the `__schema__`
  attribute. In case of Entity schema, the attribute names are fetched from the
  schema. In case of ANY (or primitives), an empty list is returned.

  Args:
    x: A DataSlice.
    intersection: If True, the intersection of all object attributes is
      returned. Otherwise, the union is returned.

  Returns:
    A list of unique attributes sorted by alphabetical order.
```

### `kd.get_bag(ds)` {#kd.get_bag}

Alias for [kd.core.get_bag](#kd.core.get_bag) operator.

### `kd.get_dtype(ds)` {#kd.get_dtype}

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype) operator.

### `kd.get_item(x, key_or_index)` {#kd.get_item}

Alias for [kd.core.get_item](#kd.core.get_item) operator.

### `kd.get_item_schema(list_schema)` {#kd.get_item_schema}

Alias for [kd.schema.get_item_schema](#kd.schema.get_item_schema) operator.

### `kd.get_itemid(x)` {#kd.get_itemid}

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid) operator.

### `kd.get_key_schema(dict_schema)` {#kd.get_key_schema}

Alias for [kd.schema.get_key_schema](#kd.schema.get_key_schema) operator.

### `kd.get_keys(dict_ds)` {#kd.get_keys}

Alias for [kd.dicts.get_keys](#kd.dicts.get_keys) operator.

### `kd.get_ndim(x)` {#kd.get_ndim}

Alias for [kd.slices.get_ndim](#kd.slices.get_ndim) operator.

### `kd.get_nofollowed_schema(schema)` {#kd.get_nofollowed_schema}

Alias for [kd.schema.get_nofollowed_schema](#kd.schema.get_nofollowed_schema) operator.

### `kd.get_obj_schema(x)` {#kd.get_obj_schema}

Alias for [kd.schema.get_obj_schema](#kd.schema.get_obj_schema) operator.

### `kd.get_primitive_schema(ds)` {#kd.get_primitive_schema}

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype) operator.

### `kd.get_schema(x)` {#kd.get_schema}

Alias for [kd.schema.get_schema](#kd.schema.get_schema) operator.

### `kd.get_shape(x)` {#kd.get_shape}

Alias for [kd.shapes.get_shape](#kd.shapes.get_shape) operator.

### `kd.get_value_schema(dict_schema)` {#kd.get_value_schema}

Alias for [kd.schema.get_value_schema](#kd.schema.get_value_schema) operator.

### `kd.get_values(dict_ds, key_ds=unspecified)` {#kd.get_values}

Alias for [kd.dicts.get_values](#kd.dicts.get_values) operator.

### `kd.greater(x, y)` {#kd.greater}

Alias for [kd.comparison.greater](#kd.comparison.greater) operator.

### `kd.greater_equal(x, y)` {#kd.greater_equal}

Alias for [kd.comparison.greater_equal](#kd.comparison.greater_equal) operator.

### `kd.group_by(x, *args, sort=DataItem(False, schema: BOOLEAN))` {#kd.group_by}

Alias for [kd.slices.group_by](#kd.slices.group_by) operator.

### `kd.group_by_indices(*args, sort=DataItem(False, schema: BOOLEAN))` {#kd.group_by_indices}

Alias for [kd.slices.group_by_indices](#kd.slices.group_by_indices) operator.

### `kd.has(x)` {#kd.has}

Alias for [kd.masking.has](#kd.masking.has) operator.

### `kd.has_attr(x, attr_name)` {#kd.has_attr}

Alias for [kd.core.has_attr](#kd.core.has_attr) operator.

### `kd.has_dict(x)` {#kd.has_dict}

Alias for [kd.dicts.has_dict](#kd.dicts.has_dict) operator.

### `kd.has_entity(x)` {#kd.has_entity}

Alias for [kd.core.has_entity](#kd.core.has_entity) operator.

### `kd.has_list(x)` {#kd.has_list}

Alias for [kd.lists.has_list](#kd.lists.has_list) operator.

### `kd.has_not(x)` {#kd.has_not}

Alias for [kd.masking.has_not](#kd.masking.has_not) operator.

### `kd.has_primitive(x)` {#kd.has_primitive}

Alias for [kd.core.has_primitive](#kd.core.has_primitive) operator.

### `kd.hash_itemid(x)` {#kd.hash_itemid}

Alias for [kd.ids.hash_itemid](#kd.ids.hash_itemid) operator.

### `kd.implode(x, /, ndim=1, itemid=None, db=None)` {#kd.implode}

Alias for [kd.lists.implode](#kd.lists.implode) operator.

### `kd.index(x, dim=DataItem(-1, schema: INT32))` {#kd.index}

Alias for [kd.slices.index](#kd.slices.index) operator.

### `kd.int32(x)` {#kd.int32}

Alias for [kd.slices.int32](#kd.slices.int32) operator.

### `kd.int64(x)` {#kd.int64}

Alias for [kd.slices.int64](#kd.slices.int64) operator.

### `kd.inverse_mapping(x, ndim=unspecified)` {#kd.inverse_mapping}

Alias for [kd.slices.inverse_mapping](#kd.slices.inverse_mapping) operator.

### `kd.inverse_select(ds, fltr)` {#kd.inverse_select}

Alias for [kd.slices.inverse_select](#kd.slices.inverse_select) operator.

### `kd.is_dict(x)` {#kd.is_dict}

Alias for [kd.dicts.is_dict](#kd.dicts.is_dict) operator.

### `kd.is_empty(x)` {#kd.is_empty}

Alias for [kd.slices.is_empty](#kd.slices.is_empty) operator.

### `kd.is_entity(x)` {#kd.is_entity}

Alias for [kd.core.is_entity](#kd.core.is_entity) operator.

### `kd.is_expandable_to(x, target, ndim=unspecified)` {#kd.is_expandable_to}

Alias for [kd.slices.is_expandable_to](#kd.slices.is_expandable_to) operator.

### `kd.is_expr(obj)` {#kd.is_expr}

``` {.no-copy}
Returns kd.present if the given object is an Expr and kd.missing otherwise.
```

### `kd.is_fn(obj)` {#kd.is_fn}

Alias for [kd.functor.is_fn](#kd.functor.is_fn) operator.

### `kd.is_item(obj)` {#kd.is_item}

``` {.no-copy}
Returns kd.present if the given object is a scalar DataItem and kd.missing otherwise.
```

### `kd.is_list(x)` {#kd.is_list}

Alias for [kd.lists.is_list](#kd.lists.is_list) operator.

### `kd.is_primitive(x)` {#kd.is_primitive}

Alias for [kd.core.is_primitive](#kd.core.is_primitive) operator.

### `kd.is_shape_compatible(x, y)` {#kd.is_shape_compatible}

Alias for [kd.slices.is_shape_compatible](#kd.slices.is_shape_compatible) operator.

### `kd.is_slice(obj)` {#kd.is_slice}

``` {.no-copy}
Returns kd.present if the given object is a DataSlice and kd.missing otherwise.
```

### `kd.isin(x, y)` {#kd.isin}

Alias for [kd.slices.isin](#kd.slices.isin) operator.

### `kd.item(x, /, schema=None)` {#kd.item}

Alias for [kd.slices.item](#kd.slices.item) operator.

### `kd.less(x, y)` {#kd.less}

Alias for [kd.comparison.less](#kd.comparison.less) operator.

### `kd.less_equal(x, y)` {#kd.less_equal}

Alias for [kd.comparison.less_equal](#kd.comparison.less_equal) operator.

### `kd.list(items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.list}

``` {.no-copy}
Creates list(s) by collapsing `items`.

  Returns an immutable list if `db` is not provided.

  If there is no argument, returns an empty Koda List.
  If the argument is a Python list, creates a nested Koda List.

  Examples:
  list() -> a single empty Koda List
  list([1, 2, 3]) -> Koda List with items 1, 2, 3
  list([[1, 2, 3], [4, 5]]) -> nested Koda List [[1, 2, 3], [4, 5]]
    # items are Koda lists.

  Args:
    items: The items to use. If not specified, an empty list of OBJECTs will be
      created.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where list(s) are created.

  Returns:
    The slice with list/lists.
```

### `kd.list_append_update(x, append)` {#kd.list_append_update}

Alias for [kd.lists.list_append_update](#kd.lists.list_append_update) operator.

### `kd.list_like(shape_and_mask_from, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.list_like}

Alias for [kd.lists.like](#kd.lists.like) operator.

### `kd.list_schema(item_schema, db=None)` {#kd.list_schema}

Alias for [kd.schema.list_schema](#kd.schema.list_schema) operator.

### `kd.list_shaped(shape, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.list_shaped}

Alias for [kd.lists.shaped](#kd.lists.shaped) operator.

### `kd.list_shaped_as(shape_from, /, items=None, *, item_schema=None, schema=None, itemid=None, db=None)` {#kd.list_shaped_as}

Alias for [kd.lists.shaped_as](#kd.lists.shaped_as) operator.

### `kd.list_size(list_slice)` {#kd.list_size}

Alias for [kd.lists.size](#kd.lists.size) operator.

### `kd.loads(x)` {#kd.loads}

``` {.no-copy}
Deserializes a DataSlice or a DataBag.
```

### `kd.make_namedtuple(**kwargs)` {#kd.make_namedtuple}

Alias for [kd.tuple.make_namedtuple](#kd.tuple.make_namedtuple) operator.

### `kd.make_tuple(*args)` {#kd.make_tuple}

Alias for [kd.tuple.make_tuple](#kd.tuple.make_tuple) operator.

### `kd.map(fn, *args, include_missing=DataItem(False, schema: BOOLEAN), **kwargs)` {#kd.map}

Alias for [kd.functor.map](#kd.functor.map) operator.

### `kd.map_py(fn, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), ndim=DataItem(0, schema: INT32), include_missing=DataItem(None, schema: NONE), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.map_py}

Alias for [kd.py.map_py](#kd.py.map_py) operator.

### `kd.map_py_on_cond(true_fn, false_fn, cond, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.map_py_on_cond}

Alias for [kd.py.map_py_on_cond](#kd.py.map_py_on_cond) operator.

### `kd.map_py_on_selected(fn, cond, *args, schema=DataItem(None, schema: NONE), max_threads=DataItem(1, schema: INT32), item_completed_callback=DataItem(None, schema: NONE), **kwargs)` {#kd.map_py_on_selected}

Alias for [kd.py.map_py_on_selected](#kd.py.map_py_on_selected) operator.

### `kd.mask(x)` {#kd.mask}

Alias for [kd.slices.mask](#kd.slices.mask) operator.

### `kd.mask_and(x, y)` {#kd.mask_and}

Alias for [kd.masking.mask_and](#kd.masking.mask_and) operator.

### `kd.mask_equal(x, y)` {#kd.mask_equal}

Alias for [kd.masking.mask_equal](#kd.masking.mask_equal) operator.

### `kd.mask_not_equal(x, y)` {#kd.mask_not_equal}

Alias for [kd.masking.mask_not_equal](#kd.masking.mask_not_equal) operator.

### `kd.max(x)` {#kd.max}

Alias for [kd.math.max](#kd.math.max) operator.

### `kd.maximum(x, y)` {#kd.maximum}

Alias for [kd.math.maximum](#kd.math.maximum) operator.

### `kd.maybe(x, attr_name)` {#kd.maybe}

Alias for [kd.core.maybe](#kd.core.maybe) operator.

### `kd.min(x)` {#kd.min}

Alias for [kd.math.min](#kd.math.min) operator.

### `kd.minimum(x, y)` {#kd.minimum}

Alias for [kd.math.minimum](#kd.math.minimum) operator.

### `kd.named_schema(name, *, db=None, **attrs)` {#kd.named_schema}

Alias for [kd.schema.named_schema](#kd.schema.named_schema) operator.

### `kd.new(arg=unspecified, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.new}

Alias for [kd.entities.new](#kd.entities.new) operator.

### `kd.new_dictid()` {#kd.new_dictid}

Alias for [kd.allocation.new_dictid](#kd.allocation.new_dictid) operator.

### `kd.new_dictid_like(shape_and_mask_from)` {#kd.new_dictid_like}

Alias for [kd.allocation.new_dictid_like](#kd.allocation.new_dictid_like) operator.

### `kd.new_dictid_shaped(shape)` {#kd.new_dictid_shaped}

Alias for [kd.allocation.new_dictid_shaped](#kd.allocation.new_dictid_shaped) operator.

### `kd.new_dictid_shaped_as(shape_from)` {#kd.new_dictid_shaped_as}

Alias for [kd.allocation.new_dictid_shaped_as](#kd.allocation.new_dictid_shaped_as) operator.

### `kd.new_itemid()` {#kd.new_itemid}

Alias for [kd.allocation.new_itemid](#kd.allocation.new_itemid) operator.

### `kd.new_itemid_like(shape_and_mask_from)` {#kd.new_itemid_like}

Alias for [kd.allocation.new_itemid_like](#kd.allocation.new_itemid_like) operator.

### `kd.new_itemid_shaped(shape)` {#kd.new_itemid_shaped}

Alias for [kd.allocation.new_itemid_shaped](#kd.allocation.new_itemid_shaped) operator.

### `kd.new_itemid_shaped_as(shape_from)` {#kd.new_itemid_shaped_as}

Alias for [kd.allocation.new_itemid_shaped_as](#kd.allocation.new_itemid_shaped_as) operator.

### `kd.new_like(shape_and_mask_from, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.new_like}

Alias for [kd.entities.like](#kd.entities.like) operator.

### `kd.new_listid()` {#kd.new_listid}

Alias for [kd.allocation.new_listid](#kd.allocation.new_listid) operator.

### `kd.new_listid_like(shape_and_mask_from)` {#kd.new_listid_like}

Alias for [kd.allocation.new_listid_like](#kd.allocation.new_listid_like) operator.

### `kd.new_listid_shaped(shape)` {#kd.new_listid_shaped}

Alias for [kd.allocation.new_listid_shaped](#kd.allocation.new_listid_shaped) operator.

### `kd.new_listid_shaped_as(shape_from)` {#kd.new_listid_shaped_as}

Alias for [kd.allocation.new_listid_shaped_as](#kd.allocation.new_listid_shaped_as) operator.

### `kd.new_shaped(shape, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.new_shaped}

Alias for [kd.entities.shaped](#kd.entities.shaped) operator.

### `kd.new_shaped_as(shape_from, /, *, schema=None, update_schema=False, itemid=None, db=None, **attrs)` {#kd.new_shaped_as}

Alias for [kd.entities.shaped_as](#kd.entities.shaped_as) operator.

### `kd.no_bag(ds)` {#kd.no_bag}

Alias for [kd.core.no_bag](#kd.core.no_bag) operator.

### `kd.nofollow(x)` {#kd.nofollow}

Alias for [kd.core.nofollow](#kd.core.nofollow) operator.

### `kd.nofollow_schema(schema)` {#kd.nofollow_schema}

Alias for [kd.schema.nofollow_schema](#kd.schema.nofollow_schema) operator.

### `kd.not_equal(x, y)` {#kd.not_equal}

Alias for [kd.comparison.not_equal](#kd.comparison.not_equal) operator.

### `kd.obj(arg=unspecified, /, *, itemid=None, db=None, **attrs)` {#kd.obj}

Alias for [kd.objs.new](#kd.objs.new) operator.

### `kd.obj_like(shape_and_mask_from, /, *, itemid=None, db=None, **attrs)` {#kd.obj_like}

Alias for [kd.objs.like](#kd.objs.like) operator.

### `kd.obj_shaped(shape, /, *, itemid=None, db=None, **attrs)` {#kd.obj_shaped}

Alias for [kd.objs.shaped](#kd.objs.shaped) operator.

### `kd.obj_shaped_as(shape_from, /, *, itemid=None, db=None, **attrs)` {#kd.obj_shaped_as}

Alias for [kd.objs.shaped_as](#kd.objs.shaped_as) operator.

### `kd.ordinal_rank(x, tie_breaker=unspecified, descending=DataItem(False, schema: BOOLEAN), ndim=unspecified)` {#kd.ordinal_rank}

Alias for [kd.slices.ordinal_rank](#kd.slices.ordinal_rank) operator.

### `kd.present_like(x)` {#kd.present_like}

Alias for [kd.masking.present_like](#kd.masking.present_like) operator.

### `kd.present_shaped(shape)` {#kd.present_shaped}

Alias for [kd.masking.present_shaped](#kd.masking.present_shaped) operator.

### `kd.present_shaped_as(x)` {#kd.present_shaped_as}

Alias for [kd.masking.present_shaped_as](#kd.masking.present_shaped_as) operator.

### `kd.py_fn(f, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **defaults)` {#kd.py_fn}

Alias for [kd.functor.py_fn](#kd.functor.py_fn) operator.

### `kd.py_reference(obj)` {#kd.py_reference}

``` {.no-copy}
Wraps into a Arolla QValue using reference for serialization.

  py_reference can be used to pass arbitrary python objects through
  kd.apply_py/kd.py_fn.

  Note that using reference for serialization means that the resulting
  QValue (and Exprs created using it) will only be valid within the
  same process. Trying to deserialize it in a different process
  will result in an exception.

  Args:
    obj: the python object to wrap.
  Returns:
    The wrapped python object as Arolla QValue.
```

### `kd.randint_like(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_like}

Alias for [kd.random.randint_like](#kd.random.randint_like) operator.

### `kd.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped}

Alias for [kd.random.randint_shaped](#kd.random.randint_shaped) operator.

### `kd.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped_as}

Alias for [kd.random.randint_shaped_as](#kd.random.randint_shaped_as) operator.

### `kd.range(start, end=unspecified)` {#kd.range}

Alias for [kd.slices.range](#kd.slices.range) operator.

### `kd.ref(ds)` {#kd.ref}

Alias for [kd.core.ref](#kd.core.ref) operator.

### `kd.reify(ds, source)` {#kd.reify}

Alias for [kd.core.reify](#kd.core.reify) operator.

### `kd.repeat(x, sizes)` {#kd.repeat}

Alias for [kd.slices.repeat](#kd.slices.repeat) operator.

### `kd.repeat_present(x, sizes)` {#kd.repeat_present}

Alias for [kd.slices.repeat_present](#kd.slices.repeat_present) operator.

### `kd.reshape(x, shape)` {#kd.reshape}

Alias for [kd.shapes.reshape](#kd.shapes.reshape) operator.

### `kd.reshape_as(x, shape_from)` {#kd.reshape_as}

Alias for [kd.shapes.reshape_as](#kd.shapes.reshape_as) operator.

### `kd.reverse(ds)` {#kd.reverse}

Alias for [kd.slices.reverse](#kd.slices.reverse) operator.

### `kd.reverse_select(ds, fltr)` {#kd.reverse_select}

Alias for [kd.slices.inverse_select](#kd.slices.inverse_select) operator.

### `kd.sample(x, ratio, seed, key=unspecified)` {#kd.sample}

Alias for [kd.random.sample](#kd.random.sample) operator.

### `kd.sample_n(x, n, seed, key=unspecified)` {#kd.sample_n}

Alias for [kd.random.sample_n](#kd.random.sample_n) operator.

### `kd.schema_from_py(tpe)` {#kd.schema_from_py}

Alias for [kd.schema.schema_from_py](#kd.schema.schema_from_py) operator.

### `kd.select(ds, fltr, expand_filter=DataItem(True, schema: BOOLEAN))` {#kd.select}

Alias for [kd.slices.select](#kd.slices.select) operator.

### `kd.select_items(ds, fltr)` {#kd.select_items}

Alias for [kd.lists.select_items](#kd.lists.select_items) operator.

### `kd.select_keys(ds, fltr)` {#kd.select_keys}

Alias for [kd.dicts.select_keys](#kd.dicts.select_keys) operator.

### `kd.select_present(ds)` {#kd.select_present}

Alias for [kd.slices.select_present](#kd.slices.select_present) operator.

### `kd.select_values(ds, fltr)` {#kd.select_values}

Alias for [kd.dicts.select_values](#kd.dicts.select_values) operator.

### `kd.set_attr(x, attr_name, value, update_schema=False)` {#kd.set_attr}

``` {.no-copy}
Sets an attribute `attr_name` to `value`.

  If `update_schema` is True and `x` is either an Entity with explicit schema
  or an Object where some items are entities with explicit schema, it will get
  updated with `value`'s schema first.

  Args:
    x: a DataSlice on which to set the attribute. Must have DataBag attached.
    attr_name: attribute name
    value: a DataSlice or convertible to a DataSlice that will be assigned as an
      attribute.
    update_schema: whether to update the schema before setting an attribute.
```

### `kd.set_attrs(x, *, update_schema=False, **attrs)` {#kd.set_attrs}

``` {.no-copy}
Sets multiple attributes on an object / entity.

  Args:
    x: a DataSlice on which attributes are set. Must have DataBag attached.
    update_schema: (bool) overwrite schema if attribute schema is missing or
      incompatible.
    **attrs: attribute values that are converted to DataSlices with DataBag
      adoption.
```

### `kd.set_schema(x, schema)` {#kd.set_schema}

``` {.no-copy}
Returns a copy of `x` with the provided `schema`.

  If `schema` is an Entity schema and has a different DataBag than `x`, it is
  merged into the DataBag of `x`.

  It only changes the schemas of `x` and does not change the items in `x`. To
  change the items in `x`, use `kd.cast_to` instead. For example,

    kd.set_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
        `x` are not compatible with FLOAT32.
    kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

  When items in `x` are primitives or `schemas` is a primitive schema, it checks
  items and schema are compatible. When items are ItemIds and `schema` is a
  non-primitive schema, it does not check the underlying data matches the
  schema. For example,

    kd.set_schema(kd.ds([1, 2, 3], schema=kd.ANY), kd.INT32) -> kd.ds([1, 2, 3])
    kd.set_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail
    kd.set_schema(kd.ds(1).with_bag(kd.bag()), kd.schema.new_schema(x=kd.INT32))
    ->
    fail
    kd.set_schema(kd.new(x=1), kd.INT32) -> fail
    kd.set_schema(kd.new(x=1), kd.schema.new_schema(x=kd.INT64)) -> work

  Args:
    x: DataSlice to change the schema of.
    schema: DataSlice containing the new schema.

  Returns:
    DataSlice with the new schema.
```

### `kd.shallow_clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.shallow_clone}

Alias for [kd.core.shallow_clone](#kd.core.shallow_clone) operator.

### `kd.size(x)` {#kd.size}

Alias for [kd.slices.size](#kd.slices.size) operator.

### `kd.slice(x, /, schema=None)` {#kd.slice}

Alias for [kd.slices.slice](#kd.slices.slice) operator.

### `kd.sort(x, sort_by=unspecified, descending=DataItem(False, schema: BOOLEAN))` {#kd.sort}

Alias for [kd.slices.sort](#kd.slices.sort) operator.

### `kd.stack(*args, ndim=DataItem(0, schema: INT32))` {#kd.stack}

Alias for [kd.slices.stack](#kd.slices.stack) operator.

### `kd.str(x)` {#kd.str}

Alias for [kd.slices.str](#kd.slices.str) operator.

### `kd.stub(x, attrs=DataSlice([], schema: NONE, ndims: 1, size: 0))` {#kd.stub}

Alias for [kd.core.stub](#kd.core.stub) operator.

### `kd.subslice(x, *slices)` {#kd.subslice}

Alias for [kd.slices.subslice](#kd.slices.subslice) operator.

### `kd.sum(x)` {#kd.sum}

Alias for [kd.math.sum](#kd.math.sum) operator.

### `kd.take(x, indices)` {#kd.take}

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.tile(x, shape)` {#kd.tile}

Alias for [kd.slices.tile](#kd.slices.tile) operator.

### `kd.to_any(x)` {#kd.to_any}

Alias for [kd.schema.as_any](#kd.schema.as_any) operator.

### `kd.to_bool(x)` {#kd.to_bool}

Alias for [kd.schema.to_bool](#kd.schema.to_bool) operator.

### `kd.to_bytes(x)` {#kd.to_bytes}

Alias for [kd.schema.to_bytes](#kd.schema.to_bytes) operator.

### `kd.to_expr(x)` {#kd.to_expr}

Alias for [kd.schema.to_expr](#kd.schema.to_expr) operator.

### `kd.to_float32(x)` {#kd.to_float32}

Alias for [kd.schema.to_float32](#kd.schema.to_float32) operator.

### `kd.to_float64(x)` {#kd.to_float64}

Alias for [kd.schema.to_float64](#kd.schema.to_float64) operator.

### `kd.to_int32(x)` {#kd.to_int32}

Alias for [kd.schema.to_int32](#kd.schema.to_int32) operator.

### `kd.to_int64(x)` {#kd.to_int64}

Alias for [kd.schema.to_int64](#kd.schema.to_int64) operator.

### `kd.to_itemid(x)` {#kd.to_itemid}

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid) operator.

### `kd.to_json(x, /, *, indent=DataItem(None, schema: NONE), ensure_ascii=DataItem(True, schema: BOOLEAN), keys_attr=DataItem('json_object_keys', schema: STRING), values_attr=DataItem('json_object_values', schema: STRING))` {#kd.to_json}

Alias for [kd.json.to_json](#kd.json.to_json) operator.

### `kd.to_mask(x)` {#kd.to_mask}

Alias for [kd.schema.to_mask](#kd.schema.to_mask) operator.

### `kd.to_none(x)` {#kd.to_none}

Alias for [kd.schema.to_none](#kd.schema.to_none) operator.

### `kd.to_object(x)` {#kd.to_object}

Alias for [kd.schema.to_object](#kd.schema.to_object) operator.

### `kd.to_proto(x, /, message_class)` {#kd.to_proto}

``` {.no-copy}
Converts a DataSlice or DataItem to one or more proto messages.

  If `x` is a DataItem, this returns a single proto message object. Otherwise,
  `x` must be a 1-D DataSlice, and this returns a list of proto message objects
  with the same size as the input. Missing items in the input are returned as
  python None in place of a message.

  Koda data structures are converted to equivalent proto messages, primitive
  fields, repeated fields, maps, and enums, based on the proto schema. Koda
  entity attributes are converted to message fields with the same name, if
  those fields exist, otherwise they are ignored.

  Koda slices with mixed underlying dtypes are tolerated wherever the proto
  conversion is defined for all dtypes, regardless of schema.

  Koda entity attributes that are parenthesized fully-qualified extension
  paths (e.g. "(package_name.some_extension)") are converted to extensions,
  if those extensions exist in the descriptor pool of the messages' common
  descriptor, otherwise they are ignored.

  Args:
    x: DataSlice to convert.
    message_class: A proto message class.

  Returns:
    A converted proto message or list of converted proto messages.
```

### `kd.to_py(ds, max_depth=2, obj_as_dict=False, include_missing_attrs=True)` {#kd.to_py}
*No description*

### `kd.to_pylist(x)` {#kd.to_pylist}

``` {.no-copy}
Expands the outermost DataSlice dimension into a list of DataSlices.
```

### `kd.to_pytree(ds, max_depth=2, include_missing_attrs=True)` {#kd.to_pytree}
*No description*

### `kd.to_schema(x)` {#kd.to_schema}

Alias for [kd.schema.to_schema](#kd.schema.to_schema) operator.

### `kd.to_str(x)` {#kd.to_str}

Alias for [kd.schema.to_str](#kd.schema.to_str) operator.

### `kd.trace_as_fn(*, name=None, py_fn=False, return_type_as=None, wrapper=None)` {#kd.trace_as_fn}

Alias for [kd.functor.trace_as_fn](#kd.functor.trace_as_fn) operator.

### `kd.trace_py_fn(f, *, auto_variables=True, **defaults)` {#kd.trace_py_fn}

Alias for [kd.functor.trace_py_fn](#kd.functor.trace_py_fn) operator.

### `kd.translate(keys_to, keys_from, values_from)` {#kd.translate}

Alias for [kd.slices.translate](#kd.slices.translate) operator.

### `kd.translate_group(keys_to, keys_from, values_from)` {#kd.translate_group}

Alias for [kd.slices.translate_group](#kd.slices.translate_group) operator.

### `kd.unique(x, sort=DataItem(False, schema: BOOLEAN))` {#kd.unique}

Alias for [kd.slices.unique](#kd.slices.unique) operator.

### `kd.update_schema(obj, **attr_schemas)` {#kd.update_schema}

``` {.no-copy}
Updates the schema of `obj` DataSlice using given schemas for attrs.
```

### `kd.updated(ds, *bag)` {#kd.updated}

Alias for [kd.core.updated](#kd.core.updated) operator.

### `kd.updated_bag(*bags)` {#kd.updated_bag}

Alias for [kd.bags.updated](#kd.bags.updated) operator.

### `kd.uu(seed=None, *, schema=None, update_schema=False, db=None, **attrs)` {#kd.uu}

Alias for [kd.entities.uu](#kd.entities.uu) operator.

### `kd.uu_schema(seed='', *, db=None, **attrs)` {#kd.uu_schema}

Alias for [kd.schema.uu_schema](#kd.schema.uu_schema) operator.

### `kd.uuid(seed=DataItem('', schema: STRING), **kwargs)` {#kd.uuid}

Alias for [kd.ids.uuid](#kd.ids.uuid) operator.

### `kd.uuid_for_dict(seed=DataItem('', schema: STRING), **kwargs)` {#kd.uuid_for_dict}

Alias for [kd.ids.uuid_for_dict](#kd.ids.uuid_for_dict) operator.

### `kd.uuid_for_list(seed=DataItem('', schema: STRING), **kwargs)` {#kd.uuid_for_list}

Alias for [kd.ids.uuid_for_list](#kd.ids.uuid_for_list) operator.

### `kd.uuids_with_allocation_size(seed=DataItem('', schema: STRING), *, size)` {#kd.uuids_with_allocation_size}

Alias for [kd.ids.uuids_with_allocation_size](#kd.ids.uuids_with_allocation_size) operator.

### `kd.uuobj(seed=None, *, db=None, **attrs)` {#kd.uuobj}

Alias for [kd.objs.uu](#kd.objs.uu) operator.

### `kd.val_like(x, val)` {#kd.val_like}

Alias for [kd.slices.val_like](#kd.slices.val_like) operator.

### `kd.val_shaped(shape, val)` {#kd.val_shaped}

Alias for [kd.slices.val_shaped](#kd.slices.val_shaped) operator.

### `kd.val_shaped_as(x, val)` {#kd.val_shaped_as}

Alias for [kd.slices.val_shaped_as](#kd.slices.val_shaped_as) operator.

### `kd.with_attr(x, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#kd.with_attr}

Alias for [kd.core.with_attr](#kd.core.with_attr) operator.

### `kd.with_attrs(x, /, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#kd.with_attrs}

Alias for [kd.core.with_attrs](#kd.core.with_attrs) operator.

### `kd.with_bag(ds, bag)` {#kd.with_bag}

Alias for [kd.core.with_bag](#kd.core.with_bag) operator.

### `kd.with_dict_update(x, keys, values=unspecified)` {#kd.with_dict_update}

Alias for [kd.dicts.with_dict_update](#kd.dicts.with_dict_update) operator.

### `kd.with_list_append_update(x, append)` {#kd.with_list_append_update}

Alias for [kd.lists.with_list_append_update](#kd.lists.with_list_append_update) operator.

### `kd.with_merged_bag(ds)` {#kd.with_merged_bag}

Alias for [kd.core.with_merged_bag](#kd.core.with_merged_bag) operator.

### `kd.with_name(obj, name)` {#kd.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `kd.with_schema(x, schema)` {#kd.with_schema}

Alias for [kd.schema.with_schema](#kd.schema.with_schema) operator.

### `kd.with_schema_from_obj(x)` {#kd.with_schema_from_obj}

Alias for [kd.schema.with_schema_from_obj](#kd.schema.with_schema_from_obj) operator.

### `kd.zip(*args)` {#kd.zip}

Alias for [kd.slices.zip](#kd.slices.zip) operator.

</section>

## `kd_ext` operators {#kd_ext_category}

Operators under the `kd_ext.xxx` modules for extension utilities. Importing from
the following module is needed:
`from koladata import kd_ext`

<section class="zippy closed">

**Operators**

### `kd_ext.experimental.call_multithreaded(fn, /, *args, max_threads=100, **kwargs)` {#kd_ext.experimental.call_multithreaded}

``` {.no-copy}
Calls a functor with the given arguments.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other.

  If this functor calls sub-functors (has kd.call operators in its expressions),
  every kd.call operator usage must be directly assigned to a variable.
  This can be achieved, for example, by using @kd.trace_as_fn to create
  and call the sub-functors, or by explicitly calling .with_name() on the result
  of kd.call() when tracing.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    max_threads: The maximum number of threads to use.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    The result of the call.
```

### `kd_ext.experimental.call_multithreaded_with_debug(fn, /, *args, max_threads=100, **kwargs)` {#kd_ext.experimental.call_multithreaded_with_debug}

``` {.no-copy}
Calls a functor with the given arguments.

  Variables of the functor or of its sub-functors will be computed in parallel
  when they don't depend on each other.

  If this functor calls sub-functors (has kd.call operators in its expressions),
  every kd.call operator usage must be directly assigned to a variable.
  This can be achieved, for example, by using @kd.trace_as_fn to create
  and call the sub-functors, or by explicitly calling .with_name() on the result
  of kd.call() when tracing.

  Args:
    fn: The functor to call.
    *args: The positional arguments to pass to the functor.
    max_threads: The maximum number of threads to use.
    **kwargs: The keyword arguments to pass to the functor.

  Returns:
    A tuple of the result of the call and the debug object. The debug object
    will contain `start_time` and `end_time` attributes for the computation
    (times in the sense of Python time.time() function), and a `children`
    attribute with a list of children that have the same structure
    recursively.
```

### `kd_ext.nested_data.selected_path_update(root_ds, selection_ds_path, selection_ds)` {#kd_ext.nested_data.selected_path_update}

```` {.no-copy}
Returns a DataBag where only the selected items are present in child lists.

  The selection_ds_path must contain at least one list attribute. In general,
  all lists must use an explicit list schema; this function does not work for
  lists stored as kd.OBJECT.

  Example:
    ```
    selection_ds = root_ds.a[:].b.c[:].x > 1
    ds = root_ds.updated(selected_path(root_ds, ['a', 'b', 'c'], selection_ds))
    assert not kd.any(ds.a[:].b.c[:].x <= 1)
    ```

  Args:
    root_ds: the DataSlice to be filtered / selected.
    selection_ds_path: the path in root_ds where selection_ds should be applied.
    selection_ds: the DataSlice defining what is filtered / selected, or a
      functor or a Python function that can be evaluated to this DataSlice
      passing the given root_ds as its argument.

  Returns:
    A DataBag where child items along the given path are filtered according to
    the @selection_ds. When all items at a level are removed, their parent is
    also removed. The output DataBag only contains modified lists, and it may
    need to be combined with the @root_ds via
    @root_ds.updated(selected_path(....)).
````

### `kd_ext.npkd.from_array(arr)` {#kd_ext.npkd.from_array}

``` {.no-copy}
Converts a numpy array to a DataSlice.
```

### `kd_ext.npkd.get_elements_indices_from_ds(ds)` {#kd_ext.npkd.get_elements_indices_from_ds}

``` {.no-copy}
Returns a list of np arrays representing the DataSlice's indices.

  You can consider this as a n-dimensional coordinates of the items, p.ex. for a
  two-dimensional DataSlice:

  [[a, b],
   [],
   [c, d]] -> [[0, 0, 2, 2], [0, 1, 0, 1]]

   Let's explain this:
   - 'a' is in the first row and first column, its coordinates are (0, 0)
   - 'b' is in the first row and second column, its coordinates are (0, 1)
   - 'c' is in the third row and first column, its coordinates are (2, 0)
   - 'd' is in the third row and second column, its coordinates are (2, 1)

  if we write first y-coordinates, then x-coordinates, we get the following:
  [[0, 0, 2, 2], [0, 1, 0, 1]]

  The following conditions are satisfied:
  - result is always a two-dimensional array;
  - number of rows of the result equals the dimensionality of the input;
  - each row of the result has the same length and it corresponds to the total
  number of items in the DataSlice.

  Args:
    ds: DataSlice to get indices for.

  Returns:
    list of np arrays representing the DataSlice's elements indices.
```

### `kd_ext.npkd.reshape_based_on_indices(ds, indices)` {#kd_ext.npkd.reshape_based_on_indices}

``` {.no-copy}
Reshapes a DataSlice corresponding to the given indices.

  Inverse operation to get_elements_indices_from_ds.

  Let's explain this based on the following example:

  ds: [a, b, c, d]
  indices: [[0, 0, 2, 2], [0, 1, 0, 1]]
  result: [[a, b], [], [c, d]]

  Indices represent y- and x-coordinates of the items in the DataSlice.
  - 'a': according to the indices, its coordinates are (0, 0) (first element
  from the first and second row of indices conrrespondingly);
  it will be placed in the first row and first column of the result;
  - 'b': its coordinates are (0, 1); it will be placed in the first row and
  second column of the result;
  - 'c': its coordinates are (2, 0); it will be placed in the third row and
  first column of the result;
  - 'd': its coordinates are (2, 1); it will be placed in the third row and
  second column of the result.

  The result DataSlice will have the same number of items as the original
  DataSlice. Its dimensionality will be equal to the number of rows in the
  indices.

  Args:
    ds: DataSlice to reshape; can only be 1D.
    indices: list of np arrays representing the DataSlice's indices; it has to
      be a list of one-dimensional arrays where each row has equal number of
      elements corresponding to the number of items in the DataSlice.

  Returns:
    DataSlice reshaped based on the given indices.
```

### `kd_ext.npkd.to_array(ds)` {#kd_ext.npkd.to_array}

``` {.no-copy}
Converts a DataSlice to a numpy array.
```

### `kd_ext.pdkd.from_dataframe(df, as_obj=False)` {#kd_ext.pdkd.from_dataframe}

``` {.no-copy}
Creates a DataSlice from the given pandas DataFrame.

  The DataFrame must have at least one column. It will be converted to a
  DataSlice of entities/objects with attributes corresponding to the DataFrame
  columns. Supported column dtypes include all primitive dtypes and ItemId.

  If the DataFrame has MultiIndex, it will be converted to a DataSlice with
  the shape derived from the MultiIndex.

  When `as_obj` is set, the resulting DataSlice will be a DataSlice of objects
  instead of entities.

  Args:
   df: pandas DataFrame to convert.
   as_obj: whether to convert the resulting DataSlice to Objects.

  Returns:
    DataSlice of items with attributes from DataFrame columns.
```

### `kd_ext.pdkd.to_dataframe(ds, cols=None, include_self=False)` {#kd_ext.pdkd.to_dataframe}

``` {.no-copy}
Creates a pandas DataFrame from the given DataSlice.

  If `ds` has no dimension, it will be converted to a single row DataFrame. If
  it has one dimension, it willbe converted an 1D DataFrame. If it has more than
  one dimension, it will be converted to a MultiIndex DataFrame with index
  columns corresponding to each dimension.

  When `cols` is not specified, DataFrame columns are inferred from `ds`.
    1) If `ds` has primitives, lists, dicts or ITEMID/ANY schema, a single
       column named 'self_' is used and items themselves are extracted.
    2) If `ds` has entity schema, all attributes from `ds` are extracted as
       columns.
    3) If `ds` has OBJECT schema, the union of attributes from all objects in
       `ds` are used as columns. Missing values are filled if objects do not
       have corresponding attributes.

  For example,

    ds = kd.slice([1, 2, 3])
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds) -> extract 'x' and 'y'

    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    to_dataframe(ds) -> extract 'x', 'y'

  `cols` can be used to specify which data from the DataSlice should be
  extracted as DataFrame columns. It can contain either the string names of
  attributes or Exprs which can be evaluated on the DataSlice. If `ds` has
  OBJECT schema, specified attributes must present in all objects in `ds`. To
  ignore objects which do not have specific attributes, one can use
  `S.maybe(attr)` in `cols`. For example,

    ds = kd.slice([1, 2, 3])
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [I.x, I.x + I.y]) -> extract 'I.x' and 'I.x + I.y'

    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [I.y]) -> raise an exception as 'y' does not exist in
        kd.obj(x=2)
    to_dataframe(ds, [S.maybe('y')]) -> extract 'y' but ignore items which
        do not have 'x' attribute.

  If extracted column DataSlices have different shapes, they will be aligned to
  the same dimensions. For example,

    ds = kd.new(
        x = kd.slice([1, 2, 3]),
        y=kd.list(kd.new(z=kd.slice([[4], [5], [6]]))),
        z=kd.list(kd.new(z=kd.slice([[4, 5], [], [6]]))),
    )
    to_dataframe(ds, cols=[I.x, I.y[:].z]) -> extract 'I.x' and 'I.y[:].z':
           'x' 'y[:].z'
      0 0   1     4
        1   1     5
      2 0   3     6
    to_dataframe(ds, cols=[I.y[:].z, I.z[:].z]) -> error: shapes mismatch


  Args:
    ds: DataSlice to convert.
    cols: list of columns to extract from DataSlice. If None all attributes will
      be extracted.
    include_self: whether to include the 'self_' column. 'self_' column is
      always included if `cols` is None and `ds` contains primitives/lists/dicts
      or it has ITEMID/ANY schema.

  Returns:
    DataFrame with columns from DataSlice fields.
```

### `kd_ext.Fn(f, *, use_tracing=True, **kwargs)` {#kd_ext.Fn}

Alias for [kd.functor.fn](#kd.functor.fn) operator.

### `kd_ext.PyFn(f, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **defaults)` {#kd_ext.PyFn}

Alias for [kd.functor.py_fn](#kd.functor.py_fn) operator.

### `kd_ext.py_cloudpickle(obj)` {#kd_ext.py_cloudpickle}

``` {.no-copy}
Wraps into a Arolla QValue using cloudpickle for serialization.
```

</section>

## `DataSlice` methods {#DataSlice_category}

`DataSlice` represents a jagged array of items (i.e. primitive values, ItemIds).

<section class="zippy closed">

**Operators**

### `DataSlice.L` {#DataSlice.L}

``` {.no-copy}
ListSlicing helper for DataSlice.

  x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
  of DataSlice x as a a list.
```

### `DataSlice.S` {#DataSlice.S}

``` {.no-copy}
Slicing helper for DataSlice.

  It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices]. For example,
    kd.subslice(x, 0) == x.S[0]
    kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
    kd.subslice(x, slice(0, -1)) == x.S[0:-1]
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
      == x.S[0:-1, 0:1, 1:]
    kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
    kd.subslice(x, slice(1, None)) == x.S[1:]

  Please see kd.subslice for more detailed explanations and examples.
```

### `DataSlice.append(value, /)` {#DataSlice.append}
Aliases:

- [DataItem.append](#DataItem.append)

``` {.no-copy}
Append a value to each list in this DataSlice
```

### `DataSlice.as_any()` {#DataSlice.as_any}
Aliases:

- [DataItem.as_any](#DataItem.as_any)

``` {.no-copy}
Returns a DataSlice with ANY schema.
```

### `DataSlice.clear()` {#DataSlice.clear}
Aliases:

- [DataItem.clear](#DataItem.clear)

``` {.no-copy}
Clears all dicts or lists in this DataSlice
```

### `DataSlice.clone(self, *, itemid=unspecified, schema=unspecified, **overrides)` {#DataSlice.clone}
Aliases:

- [DataItem.clone](#DataItem.clone)

``` {.no-copy}
Creates a DataSlice with clones of provided entities in a new DataBag.

The entities themselves and their top-level attributes are cloned (with new
ItemIds) and non-top-level attributes are extracted (with the same ItemIds).

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
  A copy of the entities where all top-level attributes are cloned (new
  ItemIds) and all of the rest extracted.
```

### `DataSlice.deep_clone(self, schema=unspecified, **overrides)` {#DataSlice.deep_clone}
Aliases:

- [DataItem.deep_clone](#DataItem.deep_clone)

``` {.no-copy}
Creates a slice with a (deep) copy of the given slice.

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
    schema of 'x'.
  **overrides: attribute overrides.

Returns:
  A (deep) copy of the given DataSlice.
  All referenced entities will be copied with newly allocated ItemIds. Note
  that UUIDs will be copied as ItemIds.
```

### `DataSlice.deep_uuid(self, schema=unspecified, *, seed=DataItem('', schema: STRING))` {#DataSlice.deep_uuid}
Aliases:

- [DataItem.deep_uuid](#DataItem.deep_uuid)

``` {.no-copy}
Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve '*' and '**' tokens. If not specified,
    will use the schema of the 'x' DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.
```

### `DataSlice.dict_size(self)` {#DataSlice.dict_size}
Aliases:

- [DataItem.dict_size](#DataItem.dict_size)

``` {.no-copy}
Returns size of a Dict.
```

### `DataSlice.dict_update(self, keys, values=unspecified)` {#DataSlice.dict_update}
Aliases:

- [DataItem.dict_update](#DataItem.dict_update)

``` {.no-copy}
Returns DataBag containing updates to a DataSlice of dicts.

This operator has two forms:
  kd.dict_update(x, keys, values) where keys and values are slices
  kd.dict_update(x, dict_updates) where dict_updates is a DataSlice of dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

### `DataSlice.display(self, options=None)` {#DataSlice.display}
Aliases:

- [DataItem.display](#DataItem.display)

``` {.no-copy}
Visualizes a DataSlice as an html widget.
```

### `DataSlice.embed_schema()` {#DataSlice.embed_schema}
Aliases:

- [DataItem.embed_schema](#DataItem.embed_schema)

``` {.no-copy}
Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as '__schema__' attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.
```

### `DataSlice.enriched(self, *bag)` {#DataSlice.enriched}
Aliases:

- [DataItem.enriched](#DataItem.enriched)

``` {.no-copy}
Returns a copy of a DataSlice with a additional fallback DataBag(s).

Values in the original DataBag of `ds` take precedence over the ones in
`*bag`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`enriched(x, a, b)` is equivalent to `enriched(enriched(x, a), b)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: additional fallback DataBag(s).

Returns:
  DataSlice with additional fallbacks.
```

### `DataSlice.expand_to(self, target, ndim=unspecified)` {#DataSlice.expand_to}
Aliases:

- [DataItem.expand_to](#DataItem.expand_to)

``` {.no-copy}
Expands `x` based on the shape of `target`.

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
  Expanded DataSlice
```

### `DataSlice.explode(self, ndim=DataItem(1, schema: INT64))` {#DataSlice.explode}
Aliases:

- [DataItem.explode](#DataItem.explode)

``` {.no-copy}
Explodes a List DataSlice `x` a specified number of times.

A single list "explosion" converts a rank-K DataSlice of LIST[T] to a
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
  DataSlice
```

### `DataSlice.extract(self, schema=unspecified)` {#DataSlice.extract}
Aliases:

- [DataItem.extract](#DataItem.extract)

``` {.no-copy}
Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.
```

### `DataSlice.extract_bag(self, schema=unspecified)` {#DataSlice.extract_bag}
Aliases:

- [DataItem.extract_bag](#DataItem.extract_bag)

``` {.no-copy}
Creates a new DataBag containing only reachable attrs from 'ds'.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from 'ds'.
```

### `DataSlice.fingerprint` {#DataSlice.fingerprint}

``` {.no-copy}
Unique identifier of the value.
```

### `DataSlice.flatten(self, from_dim=DataItem(0, schema: INT64), to_dim=unspecified)` {#DataSlice.flatten}
Aliases:

- [DataItem.flatten](#DataItem.flatten)

``` {.no-copy}
Returns `x` with dimensions `[from_dim:to_dim]` flattened.

Indexing works as in python:
* If `to_dim` is unspecified, `to_dim = rank()` is used.
* If `to_dim < from_dim`, `to_dim = from_dim` is used.
* If `to_dim < 0`, `max(0, to_dim + rank())` is used. The same goes for
  `from_dim`.
* If `to_dim > rank()`, `rank()` is used. The same goes for `from_dim`.

The above-mentioned adjustments places both `from_dim` and `to_dim` in the
range `[0, rank()]`. After adjustments, the new DataSlice has `rank() ==
old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
"unit" dimension is inserted at `from_dim`.

Example:
  # Flatten the last two dimensions into a single dimension, producing a
  # DataSlice with `rank = old_rank - 1`.
  kd.get_shape(x)  # -> JaggedShape(..., [2, 1], [7, 5, 3])
  flat_x = kd.flatten(x, -2)
  kd.get_shape(flat_x)  # -> JaggedShape(..., [12, 3])

  # Flatten all dimensions except the last, producing a DataSlice with
  # `rank = 2`.
  kd.get_shape(x)  # -> jaggedShape(..., [7, 5, 3])
  flat_x = kd.flatten(x, 0, -1)
  kd.get_shape(flat_x)  # -> JaggedShape([3], [7, 5, 3])

  # Flatten all dimensions.
  kd.get_shape(x)  # -> JaggedShape([3], [7, 5, 3])
  flat_x = kd.flatten(x)
  kd.get_shape(flat_x)  # -> JaggedShape([15])

Args:
  x: a DataSlice.
  from_dim: start of dimensions to flatten. Defaults to `0` if unspecified.
  to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.
```

### `DataSlice.follow(self)` {#DataSlice.follow}
Aliases:

- [DataItem.follow](#DataItem.follow)

``` {.no-copy}
Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it's attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.
```

### `DataSlice.fork_bag(self)` {#DataSlice.fork_bag}
Aliases:

- [DataItem.fork_bag](#DataItem.fork_bag)

``` {.no-copy}
Returns a copy of the DataSlice with a forked mutable DataBag.
```

### `DataSlice.freeze(self)` {#DataSlice.freeze}
Aliases:

- [DataItem.freeze](#DataItem.freeze)

``` {.no-copy}
Deprecated. Use freeze_bag() instead.
```

### `DataSlice.freeze_bag()` {#DataSlice.freeze_bag}
Aliases:

- [DataItem.freeze_bag](#DataItem.freeze_bag)

``` {.no-copy}
Returns a frozen DataSlice equivalent to `self`.
```

### `DataSlice.from_vals(x, /, schema=None)` {#DataSlice.from_vals}

Alias for [kd.slices.slice](#kd.slices.slice) operator.

### `DataSlice.get_attr(attr_name, /, default=None)` {#DataSlice.get_attr}
Aliases:

- [DataItem.get_attr](#DataItem.get_attr)

``` {.no-copy}
Gets attribute `attr_name` where missing items are filled from `default`.

Args:
  attr_name: name of the attribute to get.
  default: optional default value to fill missing items.
           Note that this value can be fully omitted.
```

### `DataSlice.get_attr_names(*, intersection)` {#DataSlice.get_attr_names}
Aliases:

- [DataItem.get_attr_names](#DataItem.get_attr_names)

``` {.no-copy}
Returns a sorted list of unique attribute names of this DataSlice.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of ANY (or primitives), an empty list is returned.

Args:
  intersection: If True, the intersection of all object attributes is returned.
    Otherwise, the union is returned.

Returns:
  A list of unique attributes sorted by alphabetical order.
```

### `DataSlice.get_bag()` {#DataSlice.get_bag}
Aliases:

- [DataItem.get_bag](#DataItem.get_bag)

``` {.no-copy}
Returns the attached DataBag.
```

### `DataSlice.get_dtype(self)` {#DataSlice.get_dtype}
Aliases:

- [DataItem.get_dtype](#DataItem.get_dtype)

``` {.no-copy}
Returns a primitive schema representing the underlying items' dtype.

If `ds` has a primitive schema, this returns that primitive schema, even if
all items in `ds` are missing. If `ds` has an OBJECT/ANY schema but contains
primitive values of a single dtype, it returns the schema for that primitive
dtype.

In case of items in `ds` have non-primitive types or mixed dtypes, returns
a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

Examples:
  kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
  kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.ANY)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
  kd.get_primitive_schema(kd.obj())) -> missing schema

Args:
  ds: DataSlice to get dtype from.

Returns:
  a primitive schema DataSlice.
```

### `DataSlice.get_item_schema(self)` {#DataSlice.get_item_schema}
Aliases:

- [DataItem.get_item_schema](#DataItem.get_item_schema)

``` {.no-copy}
Returns the item schema of a List schema`.
```

### `DataSlice.get_itemid(self)` {#DataSlice.get_itemid}
Aliases:

- [DataItem.get_itemid](#DataItem.get_itemid)

``` {.no-copy}
Casts `x` to ITEMID using explicit (permissive) casting rules.
```

### `DataSlice.get_key_schema(self)` {#DataSlice.get_key_schema}
Aliases:

- [DataItem.get_key_schema](#DataItem.get_key_schema)

``` {.no-copy}
Returns the key schema of a Dict schema`.
```

### `DataSlice.get_keys()` {#DataSlice.get_keys}
Aliases:

- [DataItem.get_keys](#DataItem.get_keys)

``` {.no-copy}
Returns keys of all dicts in this DataSlice.
```

### `DataSlice.get_ndim(self)` {#DataSlice.get_ndim}
Aliases:

- [DataItem.get_ndim](#DataItem.get_ndim)

``` {.no-copy}
Returns the number of dimensions of DataSlice `x`.
```

### `DataSlice.get_obj_schema(self)` {#DataSlice.get_obj_schema}
Aliases:

- [DataItem.get_obj_schema](#DataItem.get_obj_schema)

``` {.no-copy}
Returns a DataSlice of schemas for Objects and primitives in `x`.

DataSlice `x` must have OBJECT schema.

Examples:
  db = kd.bag()
  s = db.new_schema(a=kd.INT32)
  obj = s(a=1).embed_schema()
  kd.get_obj_schema(kd.slice([1, None, 2.0, obj]))
    -> kd.slice([kd.INT32, NONE, kd.FLOAT32, s])

Args:
  x: OBJECT DataSlice

Returns:
  A DataSlice of schemas.
```

### `DataSlice.get_present_count(self)` {#DataSlice.get_present_count}
Aliases:

- [DataItem.get_present_count](#DataItem.get_present_count)

``` {.no-copy}
Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `DataSlice.get_schema()` {#DataSlice.get_schema}
Aliases:

- [DataItem.get_schema](#DataItem.get_schema)

``` {.no-copy}
Returns a schema DataItem with type information about this DataSlice.
```

### `DataSlice.get_shape()` {#DataSlice.get_shape}
Aliases:

- [DataItem.get_shape](#DataItem.get_shape)

``` {.no-copy}
Returns the shape of the DataSlice.
```

### `DataSlice.get_size(self)` {#DataSlice.get_size}
Aliases:

- [DataItem.get_size](#DataItem.get_size)

``` {.no-copy}
Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.
```

### `DataSlice.get_value_schema(self)` {#DataSlice.get_value_schema}
Aliases:

- [DataItem.get_value_schema](#DataItem.get_value_schema)

``` {.no-copy}
Returns the value schema of a Dict schema`.
```

### `DataSlice.get_values()` {#DataSlice.get_values}
Aliases:

- [DataItem.get_values](#DataItem.get_values)

``` {.no-copy}
Returns values of all dicts in this DataSlice.
```

### `DataSlice.has_attr(self, attr_name)` {#DataSlice.has_attr}
Aliases:

- [DataItem.has_attr](#DataItem.has_attr)

``` {.no-copy}
Indicates whether the items in `x` DataSlice have the given attribute.

This function checks for attributes based on data rather than "schema" and may
be slow in some cases.

Args:
  x: DataSlice
  attr_name: Name of the attribute to check.

Returns:
  A MASK DataSlice with the same shape as `x` that contains present if the
  attribute exists for the corresponding item.
```

### `DataSlice.implode(self, ndim=DataItem(1, schema: INT64), itemid=unspecified)` {#DataSlice.implode}
Aliases:

- [DataItem.implode](#DataItem.implode)

``` {.no-copy}
Implodes a Dataslice `x` a specified number of times.

A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
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
  DataSlice of nested Lists
```

### `DataSlice.internal_as_arolla_value()` {#DataSlice.internal_as_arolla_value}
Aliases:

- [DataItem.internal_as_arolla_value](#DataItem.internal_as_arolla_value)

``` {.no-copy}
Converts primitive DataSlice / DataItem into an equivalent Arolla value.
```

### `DataSlice.internal_as_dense_array()` {#DataSlice.internal_as_dense_array}
Aliases:

- [DataItem.internal_as_dense_array](#DataItem.internal_as_dense_array)

``` {.no-copy}
Converts primitive DataSlice to an Arolla DenseArray with appropriate qtype.
```

### `DataSlice.internal_as_py()` {#DataSlice.internal_as_py}
Aliases:

- [DataItem.internal_as_py](#DataItem.internal_as_py)

``` {.no-copy}
Returns a Python object equivalent to this DataSlice.

If the values in this DataSlice represent objects, then the returned python
structure will contain DataItems.
```

### `DataSlice.internal_is_any_schema()` {#DataSlice.internal_is_any_schema}
Aliases:

- [DataItem.internal_is_any_schema](#DataItem.internal_is_any_schema)

``` {.no-copy}
Returns present iff this DataSlice is ANY Schema.
```

### `DataSlice.internal_is_itemid_schema()` {#DataSlice.internal_is_itemid_schema}
Aliases:

- [DataItem.internal_is_itemid_schema](#DataItem.internal_is_itemid_schema)

``` {.no-copy}
Returns present iff this DataSlice is ITEMID Schema.
```

### `DataSlice.is_dict()` {#DataSlice.is_dict}
Aliases:

- [DataItem.is_dict](#DataItem.is_dict)

``` {.no-copy}
Returns present iff this DataSlice has Dict schema or contains only dicts.
```

### `DataSlice.is_dict_schema()` {#DataSlice.is_dict_schema}
Aliases:

- [DataItem.is_dict_schema](#DataItem.is_dict_schema)

``` {.no-copy}
Returns present iff this DataSlice is a Dict Schema.
```

### `DataSlice.is_empty()` {#DataSlice.is_empty}
Aliases:

- [DataItem.is_empty](#DataItem.is_empty)

``` {.no-copy}
Returns present iff this DataSlice is empty.
```

### `DataSlice.is_entity()` {#DataSlice.is_entity}
Aliases:

- [DataItem.is_entity](#DataItem.is_entity)

``` {.no-copy}
Returns present iff this DataSlice has Entity schema or contains only entities.
```

### `DataSlice.is_entity_schema()` {#DataSlice.is_entity_schema}
Aliases:

- [DataItem.is_entity_schema](#DataItem.is_entity_schema)

``` {.no-copy}
Returns present iff this DataSlice represents an Entity Schema.

Note that the Entity schema includes List and Dict schemas.

Returns:
  Present iff this DataSlice represents an Entity Schema.
```

### `DataSlice.is_list()` {#DataSlice.is_list}
Aliases:

- [DataItem.is_list](#DataItem.is_list)

``` {.no-copy}
Returns present iff this DataSlice has List schema or contains only lists.
```

### `DataSlice.is_list_schema()` {#DataSlice.is_list_schema}
Aliases:

- [DataItem.is_list_schema](#DataItem.is_list_schema)

``` {.no-copy}
Returns present iff this DataSlice is a List Schema.
```

### `DataSlice.is_mutable()` {#DataSlice.is_mutable}
Aliases:

- [DataItem.is_mutable](#DataItem.is_mutable)

``` {.no-copy}
Returns present iff the attached DataBag is mutable.
```

### `DataSlice.is_primitive(self)` {#DataSlice.is_primitive}
Aliases:

- [DataItem.is_primitive](#DataItem.is_primitive)

``` {.no-copy}
Returns whether x is a primitive DataSlice.

`x` is a primitive DataSlice if it meets one of the following conditions:
  1) it has a primitive schema
  2) it has OBJECT/ANY/SCHEMA schema and only has primitives

Also see `kd.has_primitive` for a pointwise version. But note that
`kd.all(kd.has_primitive(x))` is not always equivalent to
`kd.is_primitive(x)`. For example,

  kd.is_primitive(kd.int32(None)) -> kd.present
  kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
  kd.is_primitive(kd.int32([None])) -> kd.present
  kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.
```

### `DataSlice.is_primitive_schema()` {#DataSlice.is_primitive_schema}
Aliases:

- [DataItem.is_primitive_schema](#DataItem.is_primitive_schema)

``` {.no-copy}
Returns present iff this DataSlice is a primitive (scalar) Schema.
```

### `DataSlice.list_size(self)` {#DataSlice.list_size}
Aliases:

- [DataItem.list_size](#DataItem.list_size)

``` {.no-copy}
Returns size of a List.
```

### `DataSlice.maybe(self, attr_name)` {#DataSlice.maybe}
Aliases:

- [DataItem.maybe](#DataItem.maybe)

``` {.no-copy}
A shortcut for kd.get_attr(x, attr_name, default=None).
```

### `DataSlice.no_bag()` {#DataSlice.no_bag}
Aliases:

- [DataItem.no_bag](#DataItem.no_bag)

``` {.no-copy}
Returns a copy of DataSlice without DataBag.
```

### `DataSlice.qtype` {#DataSlice.qtype}

``` {.no-copy}
QType of the stored value.
```

### `DataSlice.ref(self)` {#DataSlice.ref}
Aliases:

- [DataItem.ref](#DataItem.ref)

``` {.no-copy}
Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.
```

### `DataSlice.repeat(self, sizes)` {#DataSlice.repeat}
Aliases:

- [DataItem.repeat](#DataItem.repeat)

``` {.no-copy}
Returns `x` with values repeated according to `sizes`.

The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
broadcasted to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.repeat(ds, sizes)  # -> kd.slice([[[1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.repeat(ds, sizes)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.repeat(ds, size)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `DataSlice.reshape(self, shape)` {#DataSlice.reshape}
Aliases:

- [DataItem.reshape](#DataItem.reshape)

``` {.no-copy}
Returns a DataSlice with the provided shape.

Examples:
  x = kd.slice([1, 2, 3, 4])

  # Using a shape.
  kd.reshape(x, kd.shapes.new(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes.
  kd.reshape(x, kd.make_tuple(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of slices and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, kd.slice([3, 1])))
      # -> kd.slice([[1, 2, 3], [4]])

  # Reshaping a scalar.
  kd.reshape(1, kd.make_tuple(1, 1))  # -> kd.slice([[1]])

  # Reshaping an empty slice.
  kd.reshape(kd.slice([]), kd.make_tuple(2, 0))  # -> kd.slice([[], []])

Args:
  x: a DataSlice.
  shape: a JaggedShape or a tuple of dimensions that forms a shape through
    `kd.shapes.new`, with additional support for a `-1` placeholder dimension.
```

### `DataSlice.reshape_as(self, shape_from)` {#DataSlice.reshape_as}
Aliases:

- [DataItem.reshape_as](#DataItem.reshape_as)

``` {.no-copy}
Returns a DataSlice x reshaped to the shape of DataSlice shape_from.
```

### `DataSlice.select(self, fltr, expand_filter=DataItem(True, schema: BOOLEAN))` {#DataSlice.select}
Aliases:

- [DataItem.select](#DataItem.select)

``` {.no-copy}
Creates a new DataSlice by filtering out missing items in fltr.

The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
missing items in `fltr` are removed. The last dimension of the resulting
DataSlice is changed while the first N-1 dimensions are the same as those in
`ds`.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select(val, val > 3) -> [[4], [], [8]]

  fltr = kd.slice(
      [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
  kd.select(val, fltr) -> [[None, 4], [None], [2]]

  fltr = kd.slice([kd.present, kd.present, None])
  kd.select(val, fltr) -> [[1, None, 4], [None], []]
  kd.select(val, fltr, expand_filter=False) -> [[1, None, 4], [None]]

Args:
  ds: DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK.
  expand_filter: flag indicating if the 'filter' should be expanded to 'ds'

Returns:
  Filtered DataSlice.
```

### `DataSlice.select_items(self, fltr)` {#DataSlice.select_items}
Aliases:

- [DataItem.select_items](#DataItem.select_items)

``` {.no-copy}
Selects List items by filtering out missing items in fltr.

Also see kd.select.

Args:
  ds: List DataSlice to be filtered
  fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
    Functor or a Python function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `DataSlice.select_keys(self, fltr)` {#DataSlice.select_keys}
Aliases:

- [DataItem.select_keys](#DataItem.select_keys)

``` {.no-copy}
Selects Dict keys by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `DataSlice.select_present(self)` {#DataSlice.select_present}
Aliases:

- [DataItem.select_present](#DataItem.select_present)

``` {.no-copy}
Creates a new DataSlice by removing missing items.
```

### `DataSlice.select_values(self, fltr)` {#DataSlice.select_values}
Aliases:

- [DataItem.select_values](#DataItem.select_values)

``` {.no-copy}
Selects Dict values by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or a Python
    function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `DataSlice.set_attr(attr_name, value, /, update_schema=False)` {#DataSlice.set_attr}
Aliases:

- [DataItem.set_attr](#DataItem.set_attr)

``` {.no-copy}
Sets an attribute `attr_name` to `value`.
```

### `DataSlice.set_attrs(*, update_schema=False, **attrs)` {#DataSlice.set_attrs}
Aliases:

- [DataItem.set_attrs](#DataItem.set_attrs)

``` {.no-copy}
Sets multiple attributes on an object / entity.

Args:
  update_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.
```

### `DataSlice.set_schema(schema, /)` {#DataSlice.set_schema}
Aliases:

- [DataItem.set_schema](#DataItem.set_schema)

``` {.no-copy}
Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
```

### `DataSlice.shallow_clone(self, *, itemid=unspecified, schema=unspecified, **overrides)` {#DataSlice.shallow_clone}
Aliases:

- [DataItem.shallow_clone](#DataItem.shallow_clone)

``` {.no-copy}
Creates a DataSlice with shallow clones of immediate attributes.

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
    the resulting DataSlice. If not specified, will use the schema of 'x'.
  **overrides: attribute overrides.

Returns:
  A copy of the entities with new ItemIds where all top-level attributes are
  copied by reference.
```

### `DataSlice.stub(self, attrs=DataSlice([], schema: NONE, ndims: 1, size: 0))` {#DataSlice.stub}
Aliases:

- [DataItem.stub](#DataItem.stub)

``` {.no-copy}
Copies a DataSlice's schema stub to a new DataBag.

The "schema stub" of a DataSlice is a subset of its schema (including embedded
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
  DataSlice with the same schema stub in the new DataBag.
```

### `DataSlice.take(self, indices)` {#DataSlice.take}
Aliases:

- [DataItem.take](#DataItem.take)

``` {.no-copy}
Returns a new DataSlice with items at provided indices.

`indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
INT64 items.

Indices in the DataSlice `indices` are based on the last dimension of the
DataSlice `x`. Negative indices are supported and out-of-bound indices result
in missing items.

If ndim(x) - 1 > ndim(indices), indices are broadcasted to shape(x)[:-1].
If ndim(x) <= ndim(indices), indices are unchanged but shape(x)[:-1] must be
broadcastable to shape(indices).

Example:
  x = kd.slice([[1, None, 2], [3, 4]])
  kd.take(x, kd.item(1))  # -> kd.slice([[None, 4]])
  kd.take(x, kd.slice([0, 1]))  # -> kd.slice([1, 4])
  kd.take(x, kd.slice([[0, 1], [1]]))  # -> kd.slice([[1, None], [4]])
  kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
    # -> kd.slice([[[1, None]], []], [[4], [3]]])
  kd.take(x, kd.slice([3, -3]))  # -> kd.slice([None, None])
  kd.take(x, kd.slice([-1, -2]))  # -> kd.slice([2, 3])
  kd.take(x, kd.slice('1')) # -> dtype mismatch error
  kd.take(x, kd.slice([1, 2, 3])) -> incompatible shape

Args:
  x: DataSlice to be indexed
  indices: indices used to select items

Returns:
  A new DataSlice with items selected by indices.
```

### `DataSlice.to_py(ds, max_depth=2, obj_as_dict=False, include_missing_attrs=True)` {#DataSlice.to_py}
Aliases:

- [DataItem.to_py](#DataItem.to_py)

``` {.no-copy}
Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    obj_as_dict: Whether to convert objects to python dicts. By default objects
      are converted to automatically constructed 'Obj' dataclass instances.
    include_missing_attrs: whether to include attributes with None value in
      objects.
```

### `DataSlice.to_pytree(ds, max_depth=2, include_missing_attrs=True)` {#DataSlice.to_pytree}
Aliases:

- [DataItem.to_pytree](#DataItem.to_pytree)

``` {.no-copy}
Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.
  Objects are converted to Python dicts.

  Same as kd.to_py(..., obj_as_dict=True)

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    include_missing_attrs: whether to include attributes with None value in
      objects.
```

### `DataSlice.updated(self, *bag)` {#DataSlice.updated}
Aliases:

- [DataItem.updated](#DataItem.updated)

``` {.no-copy}
Returns a copy of a DataSlice with DataBag(s) of updates applied.

Values in `*bag` take precedence over the ones in the original DataBag of
`ds`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`updated(x, a, b)` is equivalent to `updated(updated(x, b), a)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: DataBag(s) of updates.

Returns:
  DataSlice with additional fallbacks.
```

### `DataSlice.with_attr(self, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#DataSlice.with_attr}
Aliases:

- [DataItem.with_attr](#DataItem.with_attr)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing a single updated attribute.
```

### `DataSlice.with_attrs(self, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#DataSlice.with_attrs}
Aliases:

- [DataItem.with_attrs](#DataItem.with_attrs)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated attributes.
```

### `DataSlice.with_bag(bag, /)` {#DataSlice.with_bag}
Aliases:

- [DataItem.with_bag](#DataItem.with_bag)

``` {.no-copy}
Returns a copy of DataSlice with DataBag `db`.
```

### `DataSlice.with_dict_update(self, keys, values=unspecified)` {#DataSlice.with_dict_update}
Aliases:

- [DataItem.with_dict_update](#DataItem.with_dict_update)

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated dicts.

This operator has two forms:
  kd.with_dict_update(x, keys, values) where keys and values are slices
  kd.with_dict_update(x, dict_updates) where dict_updates is a DataSlice of
    dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

### `DataSlice.with_merged_bag(self)` {#DataSlice.with_merged_bag}
Aliases:

- [DataItem.with_merged_bag](#DataItem.with_merged_bag)

``` {.no-copy}
Returns a DataSlice with the DataBag of `ds` merged with its fallbacks.

Note that a DataBag has multiple fallback DataBags and fallback DataBags can
have fallbacks as well. This operator merges all of them into a new immutable
DataBag.

If `ds` has no attached DataBag, it raises an exception. If the DataBag of
`ds` does not have fallback DataBags, it is equivalent to `ds.freeze_bag()`.

Args:
  ds: DataSlice to merge fallback DataBags of.

Returns:
  A new DataSlice with an immutable DataBags.
```

### `DataSlice.with_name(obj, name)` {#DataSlice.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `DataSlice.with_schema(schema, /)` {#DataSlice.with_schema}
Aliases:

- [DataItem.with_schema](#DataItem.with_schema)

``` {.no-copy}
Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
```

### `DataSlice.with_schema_from_obj(self)` {#DataSlice.with_schema_from_obj}
Aliases:

- [DataItem.with_schema_from_obj](#DataItem.with_schema_from_obj)

``` {.no-copy}
Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.
```

</section>

## `DataBag` methods {#DataBag_category}

`DataBag` is a set of triples (Entity.Attribute => Value).

<section class="zippy closed">

**Operators**

### `DataBag.adopt(slice, /)` {#DataBag.adopt}

``` {.no-copy}
Adopts all data reachable from the given slice into this DataBag.

Args:
  slice: DataSlice to adopt data from.

Returns:
  The DataSlice with this DataBag (including adopted data) attached.
```

### `DataBag.adopt_stub(slice, /)` {#DataBag.adopt_stub}

``` {.no-copy}
Copies the given DataSlice's schema stub into this DataBag.

The "schema stub" of a DataSlice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that DataSlice. See kd.stub() for more details.

Args:
  slice: DataSlice to extract the schema stub from.

Returns:
  The "stub" with this DataBag attached.
```

### `DataBag.concat_lists(self, /, *lists)` {#DataBag.concat_lists}

``` {.no-copy}
Returns a DataSlice of Lists concatenated from the List items of `lists`.

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
    DataSlice of concatenated Lists
```

### `DataBag.contents_repr(self, /, *, triple_limit=1000)` {#DataBag.contents_repr}

``` {.no-copy}
Returns a representation of the DataBag contents.
```

### `DataBag.data_triples_repr(self, *, triple_limit=1000)` {#DataBag.data_triples_repr}

``` {.no-copy}
Returns a representation of the DataBag contents, omitting schema triples.
```

### `DataBag.dict(self, /, items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None)` {#DataBag.dict}

``` {.no-copy}
Creates a Koda dict.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) a Python dict whose keys are either primitives or DataItems and values
       are primitives, DataItems, Python list/dict which can be converted to a
       List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
       a single dict
    3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict({1: 2, 3: 4}) -> returns a single new dict
  dict({1: [1, 2]}) -> returns a single dict, mapping 1->List[1, 2]
  dict({1: kd.slice([1, 2])}) -> returns a single dict, mapping 1->List[1, 2]
  dict({db.uuobj(x=1, y=2): 3}) -> returns a single dict, mapping uuid->3
  dict(kd.slice([1, 2]), kd.slice([3, 4])) -> returns a dict, mapping 1->3 and
  2->4
  dict(kd.slice([[1], [2]]), kd.slice([3, 4])) -> returns two dicts, one
  mapping
    1->3 and another mapping 2->4
  dict('key', 12) -> returns a single dict mapping 'key'->12

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
    A DataSlice with the dict.
```

### `DataBag.dict_like(self, shape_and_mask_from, /, items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None)` {#DataBag.dict_like}

``` {.no-copy}
Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

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
    A DataSlice with the dicts.
```

### `DataBag.dict_schema(key_schema, value_schema)` {#DataBag.dict_schema}

``` {.no-copy}
Returns a dict schema from the schemas of the keys and values
```

### `DataBag.dict_shaped(self, shape, /, items_or_keys=None, values=None, *, key_schema=None, value_schema=None, schema=None, itemid=None)` {#DataBag.dict_shaped}

``` {.no-copy}
Creates new Koda dicts with the given shape.

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
    A DataSlice with the dicts.
```

### `DataBag.empty()` {#DataBag.empty}

Alias for [kd.bags.new](#kd.bags.new) operator.

### `DataBag.fingerprint` {#DataBag.fingerprint}

``` {.no-copy}
Unique identifier of the value.
```

### `DataBag.fork(mutable=True)` {#DataBag.fork}

``` {.no-copy}
Returns a newly created DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.

Args:
  mutable: If true (default), returns a mutable DataBag. If false, the DataBag
    will be immutable.
Returns:
  data_bag.DataBag
```

### `DataBag.freeze(self)` {#DataBag.freeze}

``` {.no-copy}
Returns a frozen DataBag equivalent to `self`.
```

### `DataBag.get_approx_size()` {#DataBag.get_approx_size}

``` {.no-copy}
Returns approximate size of the DataBag.
```

### `DataBag.get_fallbacks()` {#DataBag.get_fallbacks}

``` {.no-copy}
Returns the list of fallback DataBags in this DataBag.

The list will be empty if the DataBag does not have fallbacks.
```

### `DataBag.implode(self, x, /, ndim=1, itemid=None)` {#DataBag.implode}

``` {.no-copy}
Implodes a Dataslice `x` a specified number of times.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
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
    DataSlice of nested Lists
```

### `DataBag.is_mutable()` {#DataBag.is_mutable}

``` {.no-copy}
Returns present iff this DataBag is mutable.
```

### `DataBag.list(self, /, items=None, *, item_schema=None, schema=None, itemid=None)` {#DataBag.list}

``` {.no-copy}
Creates list(s) by collapsing `items`.

  If there is no argument, returns an empty Koda List.
  If the argument is a Python list, creates a nested Koda List.

  Examples:
  list() -> a single empty Koda List
  list([1, 2, 3]) -> Koda List with items 1, 2, 3
  list([[1, 2, 3], [4, 5]]) -> nested Koda List [[1, 2, 3], [4, 5]]
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
    A DataSlice with the list/lists.
```

### `DataBag.list_like(self, shape_and_mask_from, /, items=None, *, item_schema=None, schema=None, itemid=None)` {#DataBag.list_like}

``` {.no-copy}
Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

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
    A DataSlice with the lists.
```

### `DataBag.list_schema(item_schema)` {#DataBag.list_schema}

``` {.no-copy}
Returns a list schema from the schema of the items
```

### `DataBag.list_shaped(self, shape, /, items=None, *, item_schema=None, schema=None, itemid=None)` {#DataBag.list_shaped}

``` {.no-copy}
Creates new Koda lists with the given shape.

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
    A DataSlice with the lists.
```

### `DataBag.merge_fallbacks()` {#DataBag.merge_fallbacks}

``` {.no-copy}
Returns a new DataBag with all the fallbacks merged.
```

### `DataBag.merge_inplace(self, other_bags, /, *, overwrite=True, allow_data_conflicts=True, allow_schema_conflicts=False)` {#DataBag.merge_inplace}

``` {.no-copy}
Copies all data from `other_bags` to this DataBag.

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
    self, so that multiple DataBag modifications can be chained.
```

### `DataBag.named_schema(name, **attrs)` {#DataBag.named_schema}

``` {.no-copy}
Creates a named schema with ItemId derived only from its name.
```

### `DataBag.new(arg, *, schema=None, update_schema=False, itemid=None, **attrs)` {#DataBag.new}

``` {.no-copy}
Creates Entities with given attrs.

Args:
  arg: optional Python object to be converted to an Entity.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive slice
    if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `DataBag.new_like(shape_and_mask_from, *, schema=None, update_schema=False, itemid=None, **attrs)` {#DataBag.new_like}

``` {.no-copy}
Creates new Entities with the shape and sparsity from shape_and_mask_from.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `DataBag.new_schema(**attrs)` {#DataBag.new_schema}

``` {.no-copy}
Creates new schema object with given types of attrs.
```

### `DataBag.new_shaped(shape, *, schema=None, update_schema=False, itemid=None, **attrs)` {#DataBag.new_shaped}

``` {.no-copy}
Creates new Entities with the given shape.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `DataBag.obj(arg, *, itemid=None, **attrs)` {#DataBag.obj}

``` {.no-copy}
Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Python object to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    itemid will only be set when the args is not a primitive or primitive slice
    if args presents.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `DataBag.obj_like(shape_and_mask_from, *, itemid=None, **attrs)` {#DataBag.obj_like}

``` {.no-copy}
Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  db: optional DataBag where entities are created.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `DataBag.obj_shaped(shape, *, itemid=None, **attrs)` {#DataBag.obj_shaped}

``` {.no-copy}
Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `DataBag.qtype` {#DataBag.qtype}

``` {.no-copy}
QType of the stored value.
```

### `DataBag.schema_triples_repr(self, *, triple_limit=1000)` {#DataBag.schema_triples_repr}

``` {.no-copy}
Returns a representation of schema triples in the DataBag.
```

### `DataBag.uu(seed, *, schema=None, update_schema=False, **kwargs)` {#DataBag.uu}

``` {.no-copy}
Creates an item whose ids are uuid(s) with the set attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uu(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uu(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

If 'schema' is provided, the resulting DataSlice has the provided schema.
Otherwise, uses the corresponding uuschema instead.

Args:
  seed: (str) Allows different item(s) to have different ids when created
    from the same inputs.
  schema: schema for the resulting DataSlice
  update_schema: if true, will overwrite schema attributes in the schema's
    corresponding db from the argument values.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
```

### `DataBag.uu_schema(seed, **attrs)` {#DataBag.uu_schema}

``` {.no-copy}
Creates new uuschema from given types of attrs.
```

### `DataBag.uuobj(seed, **kwargs)` {#DataBag.uuobj}

``` {.no-copy}
Creates object(s) whose ids are uuid(s) with the provided attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uuobj(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uuobj(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

Args:
  seed: (str) Allows different uuobj(s) to have different ids when created
    from the same inputs.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
```

### `DataBag.with_name(obj, name)` {#DataBag.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

</section>

## `DataItem` methods {#DataItem_category}

`DataItem` represents a single item (i.e. primitive value, ItemId).

<section class="zippy closed">

**Operators**

### `DataItem.L` {#DataItem.L}

``` {.no-copy}
ListSlicing helper for DataSlice.

  x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
  of DataSlice x as a a list.
```

### `DataItem.S` {#DataItem.S}

``` {.no-copy}
Slicing helper for DataSlice.

  It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices]. For example,
    kd.subslice(x, 0) == x.S[0]
    kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
    kd.subslice(x, slice(0, -1)) == x.S[0:-1]
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
      == x.S[0:-1, 0:1, 1:]
    kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
    kd.subslice(x, slice(1, None)) == x.S[1:]

  Please see kd.subslice for more detailed explanations and examples.
```

### `DataItem.append(value, /)` {#DataItem.append}

Alias for [DataSlice.append](#DataSlice.append) operator.

### `DataItem.as_any()` {#DataItem.as_any}

Alias for [DataSlice.as_any](#DataSlice.as_any) operator.

### `DataItem.bind(self, *, return_type_as=<class 'koladata.types.data_slice.DataSlice'>, **kwargs)` {#DataItem.bind}

``` {.no-copy}
Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  You can pass expressions with their own inputs as values in `kwargs`. Those
  inputs will become inputs of the resulting functor, will be used to compute
  those expressions, _and_ they will also be passed to the underying functor.
  Use kdf.call_fn for a more clear separation of those inputs.

  Example:
    f = kd.fn(I.x + I.y).bind(x=0)
    kd.call(f, y=1)  # 1

  Args:
    self: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **kwargs: Partial parameter binding. The values in this map may be Koda
      expressions or DataItems. When they are expressions, they must evaluate to
      a DataSlice/DataItem or a primitive that will be automatically wrapped
      into a DataItem. This function creates auxiliary variables with names
      starting with '_aux_fn', so it is not recommended to pass variables with
      such names.

  Returns:
    A new Koda functor with some parameters bound.
```

### `DataItem.clear()` {#DataItem.clear}

Alias for [DataSlice.clear](#DataSlice.clear) operator.

### `DataItem.clone(self, *, itemid=unspecified, schema=unspecified, **overrides)` {#DataItem.clone}

Alias for [DataSlice.clone](#DataSlice.clone) operator.

### `DataItem.deep_clone(self, schema=unspecified, **overrides)` {#DataItem.deep_clone}

Alias for [DataSlice.deep_clone](#DataSlice.deep_clone) operator.

### `DataItem.deep_uuid(self, schema=unspecified, *, seed=DataItem('', schema: STRING))` {#DataItem.deep_uuid}

Alias for [DataSlice.deep_uuid](#DataSlice.deep_uuid) operator.

### `DataItem.dict_size(self)` {#DataItem.dict_size}

Alias for [DataSlice.dict_size](#DataSlice.dict_size) operator.

### `DataItem.dict_update(self, keys, values=unspecified)` {#DataItem.dict_update}

Alias for [DataSlice.dict_update](#DataSlice.dict_update) operator.

### `DataItem.display(self, options=None)` {#DataItem.display}

Alias for [DataSlice.display](#DataSlice.display) operator.

### `DataItem.embed_schema()` {#DataItem.embed_schema}

Alias for [DataSlice.embed_schema](#DataSlice.embed_schema) operator.

### `DataItem.enriched(self, *bag)` {#DataItem.enriched}

Alias for [DataSlice.enriched](#DataSlice.enriched) operator.

### `DataItem.expand_to(self, target, ndim=unspecified)` {#DataItem.expand_to}

Alias for [DataSlice.expand_to](#DataSlice.expand_to) operator.

### `DataItem.explode(self, ndim=DataItem(1, schema: INT64))` {#DataItem.explode}

Alias for [DataSlice.explode](#DataSlice.explode) operator.

### `DataItem.extract(self, schema=unspecified)` {#DataItem.extract}

Alias for [DataSlice.extract](#DataSlice.extract) operator.

### `DataItem.extract_bag(self, schema=unspecified)` {#DataItem.extract_bag}

Alias for [DataSlice.extract_bag](#DataSlice.extract_bag) operator.

### `DataItem.fingerprint` {#DataItem.fingerprint}

``` {.no-copy}
Unique identifier of the value.
```

### `DataItem.flatten(self, from_dim=DataItem(0, schema: INT64), to_dim=unspecified)` {#DataItem.flatten}

Alias for [DataSlice.flatten](#DataSlice.flatten) operator.

### `DataItem.follow(self)` {#DataItem.follow}

Alias for [DataSlice.follow](#DataSlice.follow) operator.

### `DataItem.fork_bag(self)` {#DataItem.fork_bag}

Alias for [DataSlice.fork_bag](#DataSlice.fork_bag) operator.

### `DataItem.freeze(self)` {#DataItem.freeze}

Alias for [DataSlice.freeze](#DataSlice.freeze) operator.

### `DataItem.freeze_bag()` {#DataItem.freeze_bag}

Alias for [DataSlice.freeze_bag](#DataSlice.freeze_bag) operator.

### `DataItem.from_vals(x, /, schema=None)` {#DataItem.from_vals}

Alias for [kd.slices.item](#kd.slices.item) operator.

### `DataItem.get_attr(attr_name, /, default=None)` {#DataItem.get_attr}

Alias for [DataSlice.get_attr](#DataSlice.get_attr) operator.

### `DataItem.get_attr_names(*, intersection)` {#DataItem.get_attr_names}

Alias for [DataSlice.get_attr_names](#DataSlice.get_attr_names) operator.

### `DataItem.get_bag()` {#DataItem.get_bag}

Alias for [DataSlice.get_bag](#DataSlice.get_bag) operator.

### `DataItem.get_dtype(self)` {#DataItem.get_dtype}

Alias for [DataSlice.get_dtype](#DataSlice.get_dtype) operator.

### `DataItem.get_item_schema(self)` {#DataItem.get_item_schema}

Alias for [DataSlice.get_item_schema](#DataSlice.get_item_schema) operator.

### `DataItem.get_itemid(self)` {#DataItem.get_itemid}

Alias for [DataSlice.get_itemid](#DataSlice.get_itemid) operator.

### `DataItem.get_key_schema(self)` {#DataItem.get_key_schema}

Alias for [DataSlice.get_key_schema](#DataSlice.get_key_schema) operator.

### `DataItem.get_keys()` {#DataItem.get_keys}

Alias for [DataSlice.get_keys](#DataSlice.get_keys) operator.

### `DataItem.get_ndim(self)` {#DataItem.get_ndim}

Alias for [DataSlice.get_ndim](#DataSlice.get_ndim) operator.

### `DataItem.get_obj_schema(self)` {#DataItem.get_obj_schema}

Alias for [DataSlice.get_obj_schema](#DataSlice.get_obj_schema) operator.

### `DataItem.get_present_count(self)` {#DataItem.get_present_count}

Alias for [DataSlice.get_present_count](#DataSlice.get_present_count) operator.

### `DataItem.get_schema()` {#DataItem.get_schema}

Alias for [DataSlice.get_schema](#DataSlice.get_schema) operator.

### `DataItem.get_shape()` {#DataItem.get_shape}

Alias for [DataSlice.get_shape](#DataSlice.get_shape) operator.

### `DataItem.get_size(self)` {#DataItem.get_size}

Alias for [DataSlice.get_size](#DataSlice.get_size) operator.

### `DataItem.get_value_schema(self)` {#DataItem.get_value_schema}

Alias for [DataSlice.get_value_schema](#DataSlice.get_value_schema) operator.

### `DataItem.get_values()` {#DataItem.get_values}

Alias for [DataSlice.get_values](#DataSlice.get_values) operator.

### `DataItem.has_attr(self, attr_name)` {#DataItem.has_attr}

Alias for [DataSlice.has_attr](#DataSlice.has_attr) operator.

### `DataItem.implode(self, ndim=DataItem(1, schema: INT64), itemid=unspecified)` {#DataItem.implode}

Alias for [DataSlice.implode](#DataSlice.implode) operator.

### `DataItem.internal_as_arolla_value()` {#DataItem.internal_as_arolla_value}

Alias for [DataSlice.internal_as_arolla_value](#DataSlice.internal_as_arolla_value) operator.

### `DataItem.internal_as_dense_array()` {#DataItem.internal_as_dense_array}

Alias for [DataSlice.internal_as_dense_array](#DataSlice.internal_as_dense_array) operator.

### `DataItem.internal_as_py()` {#DataItem.internal_as_py}

Alias for [DataSlice.internal_as_py](#DataSlice.internal_as_py) operator.

### `DataItem.internal_is_any_schema()` {#DataItem.internal_is_any_schema}

Alias for [DataSlice.internal_is_any_schema](#DataSlice.internal_is_any_schema) operator.

### `DataItem.internal_is_itemid_schema()` {#DataItem.internal_is_itemid_schema}

Alias for [DataSlice.internal_is_itemid_schema](#DataSlice.internal_is_itemid_schema) operator.

### `DataItem.is_dict()` {#DataItem.is_dict}

Alias for [DataSlice.is_dict](#DataSlice.is_dict) operator.

### `DataItem.is_dict_schema()` {#DataItem.is_dict_schema}

Alias for [DataSlice.is_dict_schema](#DataSlice.is_dict_schema) operator.

### `DataItem.is_empty()` {#DataItem.is_empty}

Alias for [DataSlice.is_empty](#DataSlice.is_empty) operator.

### `DataItem.is_entity()` {#DataItem.is_entity}

Alias for [DataSlice.is_entity](#DataSlice.is_entity) operator.

### `DataItem.is_entity_schema()` {#DataItem.is_entity_schema}

Alias for [DataSlice.is_entity_schema](#DataSlice.is_entity_schema) operator.

### `DataItem.is_list()` {#DataItem.is_list}

Alias for [DataSlice.is_list](#DataSlice.is_list) operator.

### `DataItem.is_list_schema()` {#DataItem.is_list_schema}

Alias for [DataSlice.is_list_schema](#DataSlice.is_list_schema) operator.

### `DataItem.is_mutable()` {#DataItem.is_mutable}

Alias for [DataSlice.is_mutable](#DataSlice.is_mutable) operator.

### `DataItem.is_primitive(self)` {#DataItem.is_primitive}

Alias for [DataSlice.is_primitive](#DataSlice.is_primitive) operator.

### `DataItem.is_primitive_schema()` {#DataItem.is_primitive_schema}

Alias for [DataSlice.is_primitive_schema](#DataSlice.is_primitive_schema) operator.

### `DataItem.list_size(self)` {#DataItem.list_size}

Alias for [DataSlice.list_size](#DataSlice.list_size) operator.

### `DataItem.maybe(self, attr_name)` {#DataItem.maybe}

Alias for [DataSlice.maybe](#DataSlice.maybe) operator.

### `DataItem.no_bag()` {#DataItem.no_bag}

Alias for [DataSlice.no_bag](#DataSlice.no_bag) operator.

### `DataItem.qtype` {#DataItem.qtype}

``` {.no-copy}
QType of the stored value.
```

### `DataItem.ref(self)` {#DataItem.ref}

Alias for [DataSlice.ref](#DataSlice.ref) operator.

### `DataItem.repeat(self, sizes)` {#DataItem.repeat}

Alias for [DataSlice.repeat](#DataSlice.repeat) operator.

### `DataItem.reshape(self, shape)` {#DataItem.reshape}

Alias for [DataSlice.reshape](#DataSlice.reshape) operator.

### `DataItem.reshape_as(self, shape_from)` {#DataItem.reshape_as}

Alias for [DataSlice.reshape_as](#DataSlice.reshape_as) operator.

### `DataItem.select(self, fltr, expand_filter=DataItem(True, schema: BOOLEAN))` {#DataItem.select}

Alias for [DataSlice.select](#DataSlice.select) operator.

### `DataItem.select_items(self, fltr)` {#DataItem.select_items}

Alias for [DataSlice.select_items](#DataSlice.select_items) operator.

### `DataItem.select_keys(self, fltr)` {#DataItem.select_keys}

Alias for [DataSlice.select_keys](#DataSlice.select_keys) operator.

### `DataItem.select_present(self)` {#DataItem.select_present}

Alias for [DataSlice.select_present](#DataSlice.select_present) operator.

### `DataItem.select_values(self, fltr)` {#DataItem.select_values}

Alias for [DataSlice.select_values](#DataSlice.select_values) operator.

### `DataItem.set_attr(attr_name, value, /, update_schema=False)` {#DataItem.set_attr}

Alias for [DataSlice.set_attr](#DataSlice.set_attr) operator.

### `DataItem.set_attrs(*, update_schema=False, **attrs)` {#DataItem.set_attrs}

Alias for [DataSlice.set_attrs](#DataSlice.set_attrs) operator.

### `DataItem.set_schema(schema, /)` {#DataItem.set_schema}

Alias for [DataSlice.set_schema](#DataSlice.set_schema) operator.

### `DataItem.shallow_clone(self, *, itemid=unspecified, schema=unspecified, **overrides)` {#DataItem.shallow_clone}

Alias for [DataSlice.shallow_clone](#DataSlice.shallow_clone) operator.

### `DataItem.stub(self, attrs=DataSlice([], schema: NONE, ndims: 1, size: 0))` {#DataItem.stub}

Alias for [DataSlice.stub](#DataSlice.stub) operator.

### `DataItem.take(self, indices)` {#DataItem.take}

Alias for [DataSlice.take](#DataSlice.take) operator.

### `DataItem.to_py(ds, max_depth=2, obj_as_dict=False, include_missing_attrs=True)` {#DataItem.to_py}

Alias for [DataSlice.to_py](#DataSlice.to_py) operator.

### `DataItem.to_pytree(ds, max_depth=2, include_missing_attrs=True)` {#DataItem.to_pytree}

Alias for [DataSlice.to_pytree](#DataSlice.to_pytree) operator.

### `DataItem.updated(self, *bag)` {#DataItem.updated}

Alias for [DataSlice.updated](#DataSlice.updated) operator.

### `DataItem.with_attr(self, attr_name, value, update_schema=DataItem(False, schema: BOOLEAN))` {#DataItem.with_attr}

Alias for [DataSlice.with_attr](#DataSlice.with_attr) operator.

### `DataItem.with_attrs(self, *, update_schema=DataItem(False, schema: BOOLEAN), **attrs)` {#DataItem.with_attrs}

Alias for [DataSlice.with_attrs](#DataSlice.with_attrs) operator.

### `DataItem.with_bag(bag, /)` {#DataItem.with_bag}

Alias for [DataSlice.with_bag](#DataSlice.with_bag) operator.

### `DataItem.with_dict_update(self, keys, values=unspecified)` {#DataItem.with_dict_update}

Alias for [DataSlice.with_dict_update](#DataSlice.with_dict_update) operator.

### `DataItem.with_merged_bag(self)` {#DataItem.with_merged_bag}

Alias for [DataSlice.with_merged_bag](#DataSlice.with_merged_bag) operator.

### `DataItem.with_name(obj, name)` {#DataItem.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `DataItem.with_schema(schema, /)` {#DataItem.with_schema}

Alias for [DataSlice.with_schema](#DataSlice.with_schema) operator.

### `DataItem.with_schema_from_obj(self)` {#DataItem.with_schema_from_obj}

Alias for [DataSlice.with_schema_from_obj](#DataSlice.with_schema_from_obj) operator.

</section>
