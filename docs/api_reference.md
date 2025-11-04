<!-- Note: This file is auto-generated, do not edit manually. -->

# Koda API Reference

This document lists public **Koda** APIs, including operators (accessible from
`kd` and `kd.eager` packages) and methods of main abstractions (e.g.
DataSlice, DataBag, etc.).

Category  | Subcategory | Description
--------- | ----------- | ------------
[kd](#kd) | | `kd` and `kde` operators
 | [allocation](#kd.allocation) | Operators that allocate new ItemIds.
 | [annotation](#kd.annotation) | Annotation operators.
 | [assertion](#kd.assertion) | Operators that assert properties of DataSlices.
 | [bitwise](#kd.bitwise) | Bitwise operators
 | [bags](#kd.bags) | Operators that work on DataBags.
 | [comparison](#kd.comparison) | Operators that compare DataSlices.
 | [core](#kd.core) | Core operators that are not part of other categories.
 | [curves](#kd.curves) | Operators working with curves.
 | [dicts](#kd.dicts) | Operators working with dictionaries.
 | [entities](#kd.entities) | Operators that work solely with entities.
 | [expr](#kd.expr) | Expr utilities.
 | [extension_types](#kd.extension_types) | Extension type functionality.
 | [functor](#kd.functor) | Operators to create and call functors.
 | [ids](#kd.ids) | Operators that work with ItemIds.
 | [iterables](#kd.iterables) | Operators that work with iterables. These APIs are in active development and might change often.
 | [json](#kd.json) | JSON serialization operators.
 | [lists](#kd.lists) | Operators working with lists.
 | [masking](#kd.masking) | Masking operators.
 | [math](#kd.math) | Arithmetic operators.
 | [objs](#kd.objs) | Operators that work solely with objects.
 | [optools](#kd.optools) | Operator definition and registration tooling.
 | [proto](#kd.proto) | Protocol buffer serialization operators.
 | [parallel](#kd.parallel) | Operators for parallel computation.
 | [py](#kd.py) | Operators that call Python functions.
 | [random](#kd.random) | Random and sampling operators.
 | [schema](#kd.schema) | Schema-related operators.
 | [shapes](#kd.shapes) | Operators that work on shapes
 | [slices](#kd.slices) | Operators that perform DataSlice transformations.
 | [strings](#kd.strings) | Operators that work with strings data.
 | [streams](#kd.streams) | Operators that work with streams of items. These APIs are in active development and might change often (b/424742492).
 | [tuples](#kd.tuples) | Operators to create tuples.
[kd_ext](#kd_ext) | | `kd_ext` operators
 | [contrib](#kd_ext.contrib) | External contributions not necessarily endorsed by Koda.
 | [nested_data](#kd_ext.nested_data) | Utilities for manipulating nested data.
 | [npkd](#kd_ext.npkd) | Tools for Numpy &lt;-&gt; Koda interoperability.
 | [pdkd](#kd_ext.pdkd) | Tools for Pandas &lt;-&gt; Koda interoperability.
 | [persisted_data](#kd_ext.persisted_data) | Tools for persisted incremental data.
 | [vis](#kd_ext.vis) | Koda visualization functionality.
 | [kv](#kd_ext.kv) | Experimental Koda View API.
[DataSlice](#DataSlice) | | `DataSlice` class
[DataBag](#DataBag) | | `DataBag` class
[DataItem](#DataItem) | | `DataItem` class

## kd {#kd}

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

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new Dict ItemId.</code></pre>

### `kd.allocation.new_dictid_like(shape_and_mask_from)` {#kd.allocation.new_dictid_like}
Aliases:

- [kd.new_dictid_like](#kd.new_dictid_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new Dict ItemIds with the shape and sparsity of shape_and_mask_from.</code></pre>

### `kd.allocation.new_dictid_shaped(shape)` {#kd.allocation.new_dictid_shaped}
Aliases:

- [kd.new_dictid_shaped](#kd.new_dictid_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new Dict ItemIds of the given shape.</code></pre>

### `kd.allocation.new_dictid_shaped_as(shape_from)` {#kd.allocation.new_dictid_shaped_as}
Aliases:

- [kd.new_dictid_shaped_as](#kd.new_dictid_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new Dict ItemIds with the shape of shape_from.</code></pre>

### `kd.allocation.new_itemid()` {#kd.allocation.new_itemid}
Aliases:

- [kd.new_itemid](#kd.new_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new ItemId.</code></pre>

### `kd.allocation.new_itemid_like(shape_and_mask_from)` {#kd.allocation.new_itemid_like}
Aliases:

- [kd.new_itemid_like](#kd.new_itemid_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new ItemIds with the shape and sparsity of shape_and_mask_from.</code></pre>

### `kd.allocation.new_itemid_shaped(shape)` {#kd.allocation.new_itemid_shaped}
Aliases:

- [kd.new_itemid_shaped](#kd.new_itemid_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new ItemIds of the given shape without any DataBag attached.</code></pre>

### `kd.allocation.new_itemid_shaped_as(shape_from)` {#kd.allocation.new_itemid_shaped_as}
Aliases:

- [kd.new_itemid_shaped_as](#kd.new_itemid_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new ItemIds with the shape of shape_from.</code></pre>

### `kd.allocation.new_listid()` {#kd.allocation.new_listid}
Aliases:

- [kd.new_listid](#kd.new_listid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new List ItemId.</code></pre>

### `kd.allocation.new_listid_like(shape_and_mask_from)` {#kd.allocation.new_listid_like}
Aliases:

- [kd.new_listid_like](#kd.new_listid_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new List ItemIds with the shape and sparsity of shape_and_mask_from.</code></pre>

### `kd.allocation.new_listid_shaped(shape)` {#kd.allocation.new_listid_shaped}
Aliases:

- [kd.new_listid_shaped](#kd.new_listid_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new List ItemIds of the given shape.</code></pre>

### `kd.allocation.new_listid_shaped_as(shape_from)` {#kd.allocation.new_listid_shaped_as}
Aliases:

- [kd.new_listid_shaped_as](#kd.new_listid_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Allocates new List ItemIds with the shape of shape_from.</code></pre>

</section>

### kd.annotation {#kd.annotation}

Annotation operators.

<section class="zippy closed">

**Operators**

### `kd.annotation.source_location(expr, function_name, file_name, line, column, line_text)` {#kd.annotation.source_location}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Annotation for source location where the expr node was created.

The annotation is considered as &#34;best effort&#34; so any of the
arguments may be missing.

Args:
  function_name: name of the function where the expr node was
    created
  file_name: name of the file where the expr node was created
  line: line number where the expr node was created. 0 indicates
    an unknown line number.
  column: column number where the expr node was created. 0
    indicates an unknown line number.
 line_text: text of the line where the expr node was created</code></pre>

### `kd.annotation.with_name(obj: Any, name: str | Text) -> Any` {#kd.annotation.with_name}
Aliases:

- [kd.with_name](#kd.with_name)

- [DataSlice.with_name](#DataSlice.with_name)

- [DataBag.with_name](#DataBag.with_name)

- [DataItem.with_name](#DataItem.with_name)

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

</section>

### kd.assertion {#kd.assertion}

Operators that assert properties of DataSlices.

<section class="zippy closed">

**Operators**

### `kd.assertion.assert_present_scalar(arg_name, ds, primitive_schema)` {#kd.assertion.assert_present_scalar}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the present scalar `ds` if it&#39;s implicitly castable to `primitive_schema`.

It raises an exception if:
  1) `ds`&#39;s schema is not primitive_schema (including NONE) or OBJECT
  2) `ds` is not a scalar
  3) `ds` is not present
  4) `ds` is not castable to `primitive_schema`

The following examples will pass:
  assert_present_scalar(&#39;x&#39;, kd.present, kd.MASK)
  assert_present_scalar(&#39;x&#39;, 1, kd.INT32)
  assert_present_scalar(&#39;x&#39;, 1, kd.FLOAT64)

The following examples will fail:
  assert_primitive(&#39;x&#39;, kd.missing, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.present, kd.INT32)

Args:
  arg_name: The name of `ds`.
  ds: DataSlice to assert the dtype, presence and rank of.
  primitive_schema: The expected primitive schema.</code></pre>

### `kd.assertion.assert_primitive(arg_name, ds, primitive_schema)` {#kd.assertion.assert_primitive}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ds` if its data is implicitly castable to `primitive_schema`.

It raises an exception if:
  1) `ds`&#39;s schema is not primitive_schema (including NONE) or OBJECT
  2) `ds` has present items and not all of them are castable to
     `primitive_schema`

The following examples will pass:
  assert_primitive(&#39;x&#39;, kd.present, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present, kd.missing]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice(None, schema=kd.OBJECT), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([], schema=kd.OBJECT), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([1, 3.14], schema=kd.OBJECT), kd.FLOAT32)
  assert_primitive(&#39;x&#39;, kd.slice([1, 2]), kd.FLOAT32)

The following examples will fail:
  assert_primitive(&#39;x&#39;, 1, kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice([kd.present, 1]), kd.MASK)
  assert_primitive(&#39;x&#39;, kd.slice(1, schema=kd.OBJECT), kd.MASK)

Args:
  arg_name: The name of `ds`.
  ds: DataSlice to assert the dtype of.
  primitive_schema: The expected primitive schema.</code></pre>

### `kd.assertion.with_assertion(x, condition, message_or_fn, *args)` {#kd.assertion.with_assertion}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` if `condition` is present, else raises error `message_or_fn`.

`message_or_fn` should either be a STRING message or a functor taking the
provided `*args` and creating an error message from it. If `message_or_fn` is
a STRING, the `*args` should be omitted. If `message_or_fn` is a functor, it
will only be invoked if `condition` is `missing`.

Example:
  x = kd.slice(1)
  y = kd.slice(2)
  kd.assertion.with_assertion(x, x &lt; y, &#39;x must be less than y&#39;) # -&gt; x.
  kd.assertion.with_assertion(
      x, x &gt; y, &#39;x must be greater than y&#39;
  ) # -&gt; error: &#39;x must be greater than y&#39;.
  kd.assertion.with_assertion(
      x, x &gt; y, lambda: &#39;x must be greater than y&#39;
  ) # -&gt; error: &#39;x must be greater than y&#39;.
  kd.assertion.with_assertion(
      x,
      x &gt; y,
      lambda x, y: kd.format(&#39;x={x} must be greater than y={y}&#39;, x=x, y=y),
      x,
      y,
  ) # -&gt; error: &#39;x=1 must be greater than y=2&#39;.

Args:
  x: The value to return if `condition` is present.
  condition: A unit scalar, unit optional, or DataItem holding a mask.
  message_or_fn: The error message to raise if `condition` is not present, or
    a functor producing such an error message.
  *args: Auxiliary data to be passed to the `message_or_fn` functor.</code></pre>

</section>

### kd.bitwise {#kd.bitwise}

Bitwise operators

<section class="zippy closed">

**Operators**

### `kd.bitwise.bitwise_and(x, y)` {#kd.bitwise.bitwise_and}
Aliases:

- [kd.bitwise_and](#kd.bitwise_and)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise bitwise x &amp; y.</code></pre>

### `kd.bitwise.bitwise_or(x, y)` {#kd.bitwise.bitwise_or}
Aliases:

- [kd.bitwise_or](#kd.bitwise_or)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise bitwise x | y.</code></pre>

### `kd.bitwise.bitwise_xor(x, y)` {#kd.bitwise.bitwise_xor}
Aliases:

- [kd.bitwise_xor](#kd.bitwise_xor)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise bitwise x ^ y.</code></pre>

### `kd.bitwise.count(x)` {#kd.bitwise.count}
Aliases:

- [kd.bitwise_count](#kd.bitwise_count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes the number of bits set to 1 in the given input.</code></pre>

### `kd.bitwise.invert(x)` {#kd.bitwise.invert}
Aliases:

- [kd.bitwise_invert](#kd.bitwise_invert)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise bitwise ~x.</code></pre>

</section>

### kd.bags {#kd.bags}

Operators that work on DataBags.

<section class="zippy closed">

**Operators**

### `kd.bags.enriched(*bags)` {#kd.bags.enriched}
Aliases:

- [kd.enriched_bag](#kd.enriched_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new immutable DataBag enriched by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags earlier in the list have higher priority.
 `enriched_bag(bag1, bag2, bag3)` is equivalent to
 `enriched_bag(enriched_bag(bag1, bag2), bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for enriching.

Returns:
  An immutable DataBag enriched by `bags`.</code></pre>

### `kd.bags.is_null_bag(bag)` {#kd.bags.is_null_bag}
Aliases:

- [kd.is_null_bag](#kd.is_null_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataBag `bag` is a NullDataBag.</code></pre>

### `kd.bags.new()` {#kd.bags.new}
Aliases:

- [kd.bag](#kd.bag)

- [DataBag.empty](#DataBag.empty)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an empty immutable DataBag.</code></pre>

### `kd.bags.updated(*bags)` {#kd.bags.updated}
Aliases:

- [kd.updated_bag](#kd.updated_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new immutable DataBag updated by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags later in the list have higher priority.
 `updated_bag(bag1, bag2, bag3)` is equivalent to
 `updated_bag(bag1, updated_bag(bag2, bag3))`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for updating.

Returns:
  An immutable DataBag updated by `bags`.</code></pre>

</section>

### kd.comparison {#kd.comparison}

Operators that compare DataSlices.

<section class="zippy closed">

**Operators**

### `kd.comparison.equal(x, y)` {#kd.comparison.equal}
Aliases:

- [kd.equal](#kd.equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` and `y` are equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are equal. Returns `kd.present` for equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.full_equal(x, y)` {#kd.comparison.full_equal}
Aliases:

- [kd.full_equal](#kd.full_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff all present items in `x` and `y` are equal.

The result is a zero-dimensional DataItem. Note that it is different from
`kd.all(x == y)`.

For example,
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, 3])) -&gt; kd.present
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, None])) -&gt; kd.missing
  kd.full_equal(kd.slice([1, 2, None]), kd.slice([1, 2, None])) -&gt; kd.present

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.greater(x, y)` {#kd.comparison.greater}
Aliases:

- [kd.greater](#kd.greater)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is greater than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than `y`. Returns `kd.present` when `x` is greater and
`kd.missing` when `x` is less than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.greater_equal(x, y)` {#kd.comparison.greater_equal}
Aliases:

- [kd.greater_equal](#kd.greater_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is greater than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than or equal to `y`. Returns `kd.present` when `x` is
greater than or equal to `y` and `kd.missing` when `x` is less than `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.less(x, y)` {#kd.comparison.less}
Aliases:

- [kd.less](#kd.less)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is less than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than `y`. Returns `kd.present` when `x` is less and
`kd.missing` when `x` is greater than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.less_equal(x, y)` {#kd.comparison.less_equal}
Aliases:

- [kd.less_equal](#kd.less_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is less than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than or equal to `y`. Returns `kd.present` when `x` is
less than or equal to `y` and `kd.missing` when `x` is greater than `y`.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

### `kd.comparison.not_equal(x, y)` {#kd.comparison.not_equal}
Aliases:

- [kd.not_equal](#kd.not_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` and `y` are not equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are not equal. Returns `kd.present` for not equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.</code></pre>

</section>

### kd.core {#kd.core}

Core operators that are not part of other categories.

<section class="zippy closed">

**Operators**

### `kd.core.attr(x, attr_name, value, overwrite_schema=False)` {#kd.core.attr}
Aliases:

- [kd.attr](#kd.attr)

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

- [kd.attrs](#kd.attrs)

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

- [kd.clone](#kd.clone)

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

### `kd.core.container(**attrs: Any) -> DataSlice` {#kd.core.container}
Aliases:

- [kd.container](#kd.container)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema and mutable DataBag.

Args:
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.</code></pre>

### `kd.core.deep_clone(x, /, schema=unspecified, **overrides)` {#kd.core.deep_clone}
Aliases:

- [kd.deep_clone](#kd.deep_clone)

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

- [kd.enriched](#kd.enriched)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with a additional fallback DataBag(s).

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
  DataSlice with additional fallbacks.</code></pre>

### `kd.core.extract(ds, schema=unspecified)` {#kd.core.extract}
Aliases:

- [kd.extract](#kd.extract)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.</code></pre>

### `kd.core.extract_bag(ds, schema=unspecified)` {#kd.core.extract_bag}
Aliases:

- [kd.extract_bag](#kd.extract_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataBag containing only reachable attrs from &#39;ds&#39;.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from &#39;ds&#39;.</code></pre>

### `kd.core.flatten_cyclic_references(x, *, max_recursion_depth)` {#kd.core.flatten_cyclic_references}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with tree-like copy of the input DataSlice.

The entities themselves and all their attributes including both top-level and
non-top-level attributes are cloned (with new ItemIds) while creating the
tree-like copy. The max_recursion_depth argument controls the maximum number
of times the same entity can occur on the path from the root to a leaf.
Note: resulting DataBag might have an exponential size, compared to the input
DataBag.

Args:
  x: DataSlice to flatten.
  max_recursion_depth: Maximum recursion depth.

Returns:
  A DataSlice with tree-like attributes structure.</code></pre>

### `kd.core.follow(x)` {#kd.core.follow}
Aliases:

- [kd.follow](#kd.follow)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it&#39;s attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.</code></pre>

### `kd.core.freeze(x)` {#kd.core.freeze}
Aliases:

- [kd.freeze](#kd.freeze)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen version of `x`.</code></pre>

### `kd.core.freeze_bag(x)` {#kd.core.freeze_bag}
Aliases:

- [kd.freeze_bag](#kd.freeze_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with an immutable DataBag with the same data.</code></pre>

### `kd.core.get_attr(x, attr_name, default=unspecified)` {#kd.core.get_attr}
Aliases:

- [kd.get_attr](#kd.get_attr)

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

### `kd.core.get_attr_names(x, intersection)` {#kd.core.get_attr_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with sorted unique attribute names of `x`.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  x: A DataSlice.
  intersection: If True, the intersection of all object attributes is
    returned. Otherwise, the union is returned.</code></pre>

### `kd.core.get_bag(ds)` {#kd.core.get_bag}
Aliases:

- [kd.get_bag](#kd.get_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attached DataBag.

It raises an Error if there is no DataBag attached.

Args:
  ds: DataSlice to get DataBag from.

Returns:
  The attached DataBag.</code></pre>

### `kd.core.get_item(x, key_or_index)` {#kd.core.get_item}
Aliases:

- [kd.dicts.get_item](#kd.dicts.get_item)

- [kd.lists.get_item](#kd.lists.get_item)

- [kd.get_item](#kd.get_item)

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

- [kd.get_metadata](#kd.get_metadata)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Gets a metadata from a DataSlice.

Args:
  x: DataSlice to get metadata from.

Returns:
  Metadata DataSlice.</code></pre>

### `kd.core.has_attr(x, attr_name)` {#kd.core.has_attr}
Aliases:

- [kd.has_attr](#kd.has_attr)

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

- [kd.has_bag](#kd.has_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataSlice `ds` has a DataBag attached.</code></pre>

### `kd.core.has_entity(x)` {#kd.core.has_entity}
Aliases:

- [kd.has_entity](#kd.has_entity)

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

- [kd.has_primitive](#kd.has_primitive)

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

- [kd.is_entity](#kd.is_entity)

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

- [kd.is_primitive](#kd.is_primitive)

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

- [kd.maybe](#kd.maybe)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A shortcut for kd.get_attr(x, attr_name, default=None).</code></pre>

### `kd.core.metadata(x, /, **attrs)` {#kd.core.metadata}
Aliases:

- [kd.metadata](#kd.metadata)

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

- [kd.no_bag](#kd.no_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns DataSlice without any DataBag attached.</code></pre>

### `kd.core.nofollow(x)` {#kd.core.nofollow}
Aliases:

- [kd.nofollow](#kd.nofollow)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a nofollow DataSlice targeting the given slice.

When a slice is wrapped into a nofollow, it&#39;s attributes are not further
traversed during extract, clone, deep_clone, etc.

`nofollow` is reversible.

Args:
  x: DataSlice to wrap.</code></pre>

### `kd.core.ref(ds)` {#kd.core.ref}
Aliases:

- [kd.ref](#kd.ref)

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

- [kd.reify](#kd.reify)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Assigns a bag and schema from `source` to the slice `ds`.</code></pre>

### `kd.core.shallow_clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.core.shallow_clone}
Aliases:

- [kd.shallow_clone](#kd.shallow_clone)

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

- [kd.strict_attrs](#kd.strict_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag containing attribute updates for `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `kd.core.strict_with_attrs(x, /, **attrs)` {#kd.core.strict_with_attrs}
Aliases:

- [kd.strict_with_attrs](#kd.strict_with_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `kd.core.stub(x, attrs=DataSlice([], schema: NONE))` {#kd.core.stub}
Aliases:

- [kd.stub](#kd.stub)

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

- [kd.updated](#kd.updated)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with DataBag(s) of updates applied.

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
  DataSlice with additional fallbacks.</code></pre>

### `kd.core.with_attr(x, attr_name, value, overwrite_schema=False)` {#kd.core.with_attr}
Aliases:

- [kd.with_attr](#kd.with_attr)

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

- [kd.with_attrs](#kd.with_attrs)

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

- [kd.with_bag](#kd.with_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the given DataBatg attached.</code></pre>

### `kd.core.with_merged_bag(ds)` {#kd.core.with_merged_bag}
Aliases:

- [kd.with_merged_bag](#kd.with_merged_bag)

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

- [kd.with_metadata](#kd.with_metadata)

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

- [kd.with_print](#kd.with_print)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Prints *args to stdout and returns `x`.

The operator uses str(arg) for each of the *args, i.e. it is not pointwise,
and too long arguments may be truncated.

Args:
  x: Value to propagate (unchanged).
  *args: DataSlice(s) to print.
  sep: Separator to use between DataSlice(s).
  end: End string to use after the last DataSlice.</code></pre>

</section>

### kd.curves {#kd.curves}

Operators working with curves.

<section class="zippy closed">

**Operators**

### `kd.curves.log_p1_pwl_curve(p, adjustments)` {#kd.curves.log_p1_pwl_curve}
Aliases:

- [kd_g3.curves.log_p1_pwl_curve](#kd_g3.curves.log_p1_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with log(x + 1) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.log_pwl_curve(p, adjustments)` {#kd.curves.log_pwl_curve}
Aliases:

- [kd_g3.curves.log_pwl_curve](#kd_g3.curves.log_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with log(x) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.pwl_curve(p, adjustments)` {#kd.curves.pwl_curve}
Aliases:

- [kd.pwl_curve](#kd.pwl_curve)

- [kd_g3.curves.pwl_curve](#kd_g3.curves.pwl_curve)

- [kd_g3.pwl_curve](#kd_g3.pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Piecewise Linear (PWL) curve interpolation operator.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

### `kd.curves.symmetric_log_p1_pwl_curve(p, adjustments)` {#kd.curves.symmetric_log_p1_pwl_curve}
Aliases:

- [kd_g3.curves.symmetric_log_p1_pwl_curve](#kd_g3.curves.symmetric_log_p1_pwl_curve)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Specialization of PWLCurve with symmetric log(x + 1) transformation.

Args:
 p: (DataSlice) input points to the curve
 adjustments: (DataSlice) 2D data slice with points used for interpolation.
   The second dimension must have regular size of 2. E.g., [[1, 1.7], [2,
   3.6], [7, 5.7]]

Returns:
  FLOAT64 DataSlice with the same dimensions as p with interpolation results.</code></pre>

</section>

### kd.dicts {#kd.dicts}

Operators working with dictionaries.

<section class="zippy closed">

**Operators**

### `kd.dicts.dict_update(x, keys, values=unspecified)` {#kd.dicts.dict_update}
Aliases:

- [kd.dict_update](#kd.dict_update)

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

Alias for [kd.core.get_item](#kd.core.get_item) operator.

### `kd.dicts.get_keys(dict_ds)` {#kd.dicts.get_keys}
Aliases:

- [kd.get_keys](#kd.get_keys)

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

- [kd.get_values](#kd.get_values)

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

- [kd.has_dict](#kd.has_dict)

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

- [kd.is_dict](#kd.is_dict)

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

- [kd.dict_like](#kd.dict_like)

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

- [kd.dict](#kd.dict)

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

- [kd.select_keys](#kd.select_keys)

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

- [kd.select_values](#kd.select_values)

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

- [kd.dict_shaped](#kd.dict_shaped)

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

- [kd.dict_shaped_as](#kd.dict_shaped_as)

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

- [kd.dict_size](#kd.dict_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a Dict.</code></pre>

### `kd.dicts.with_dict_update(x, keys, values=unspecified)` {#kd.dicts.with_dict_update}
Aliases:

- [kd.with_dict_update](#kd.with_dict_update)

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

</section>

### kd.entities {#kd.entities}

Operators that work solely with entities.

<section class="zippy closed">

**Operators**

### `kd.entities.like(shape_and_mask_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.like}
Aliases:

- [kd.new_like](#kd.new_like)

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

### `kd.entities.new(arg: Any = unspecified, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.entities.new}
Aliases:

- [kd.new](#kd.new)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Entities with given attrs.

Returns an immutable Entity.

Args:
  arg: optional Python object to be converted to an Entity.
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

- [kd.new_shaped](#kd.new_shaped)

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

- [kd.new_shaped_as](#kd.new_shaped_as)

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

### `kd.entities.uu(seed: str | None = None, *, schema: DataSlice | None = None, overwrite_schema: bool = False, **attrs: Any) -> DataSlice` {#kd.entities.uu}
Aliases:

- [kd.uu](#kd.uu)

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

</section>

### kd.expr {#kd.expr}

Expr utilities.

<section class="zippy closed">

**Operators**

### `kd.expr.as_expr(arg: Any) -> Expr` {#kd.expr.as_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into Exprs.</code></pre>

### `kd.expr.get_input_names(expr: Expr, container: InputContainer = InputContainer('I')) -> list[str]` {#kd.expr.get_input_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns names of `container` inputs used in `expr`.</code></pre>

### `kd.expr.get_name(expr: Expr) -> str | None` {#kd.expr.get_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the name of the given Expr, or None if it does not have one.</code></pre>

### `kd.expr.is_input(expr: Expr) -> bool` {#kd.expr.is_input}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is an input `I`.</code></pre>

### `kd.expr.is_literal(expr: Expr) -> bool` {#kd.expr.is_literal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is a Koda Literal.</code></pre>

### `kd.expr.is_packed_expr(ds: Any) -> DataSlice` {#kd.expr.is_packed_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the argument is a DataItem containing an Expr.</code></pre>

### `kd.expr.is_variable(expr: Expr) -> bool` {#kd.expr.is_variable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True if `expr` is a variable `V`.</code></pre>

### `kd.expr.literal(value)` {#kd.expr.literal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Constructs an expr with a LiteralOperator wrapping the provided QValue.</code></pre>

### `kd.expr.pack_expr(expr: Expr) -> DataSlice` {#kd.expr.pack_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Packs the given Expr into a DataItem.</code></pre>

### `kd.expr.sub(expr: Expr, *subs: Any | tuple[Expr, Any]) -> Expr` {#kd.expr.sub}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `expr` with provided expressions replaced.

Example usage:
  kd.sub(expr, (from_1, to_1), (from_2, to_2), ...)

For the special case of a single substitution, you can also do:
  kd.sub(expr, from, to)

It does the substitution by traversing &#39;expr&#39; post-order and comparing
fingerprints of sub-Exprs in the original expression and those in in &#39;subs&#39;.
For example,

  kd.sub(I.x + I.y, (I.x, I.z), (I.x + I.y, I.k)) -&gt; I.k

  kd.sub(I.x + I.y, (I.x, I.y), (I.y + I.y, I.z)) -&gt; I.y + I.y

It does not do deep transformation recursively. For example,

  kd.sub(I.x + I.y, (I.x, I.z), (I.y, I.x)) -&gt; I.z + I.x

Args:
  expr: Expr which substitutions are applied to
  *subs: Either zero or more (sub_from, sub_to) tuples, or exactly two
    arguments from and to. The keys should be expressions, and the values
    should be possible to convert to expressions using kd.as_expr.

Returns:
  A new Expr with substitutions.</code></pre>

### `kd.expr.sub_by_name(expr: Expr, /, **subs: Any) -> Expr` {#kd.expr.sub_by_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `expr` with named subexpressions replaced.

Use `kde.with_name(expr, name)` to create a named subexpression.

Example:
  foo = kde.with_name(I.x, &#39;foo&#39;)
  bar = kde.with_name(I.y, &#39;bar&#39;)
  expr = foo + bar
  kd.sub_by_name(expr, foo=I.z)
  # -&gt; I.z + kde.with_name(I.y, &#39;bar&#39;)

Args:
  expr: an expression.
  **subs: mapping from subexpression name to replacement node.</code></pre>

### `kd.expr.sub_inputs(expr: Expr, container: InputContainer = InputContainer('I'), /, **subs: Any) -> Expr` {#kd.expr.sub_inputs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an expression with `container` inputs replaced with Expr(s).</code></pre>

### `kd.expr.unpack_expr(ds: DataSlice) -> Expr` {#kd.expr.unpack_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unpacks an Expr stored in a DataItem.</code></pre>

### `kd.expr.unwrap_named(expr: Expr) -> Expr` {#kd.expr.unwrap_named}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unwraps a named Expr, raising if it is not named.</code></pre>

</section>

### kd.extension_types {#kd.extension_types}

Extension type functionality.

<section class="zippy closed">

**Operators**

### `kd.extension_types.NullableMixin()` {#kd.extension_types.NullableMixin}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Mixin class that adds nullability methods to the extension type.

Adds two methods:
  * `get_null` - Class method that returns a null instance of the extension
    type. Wrapper around `kd.extension_types.make_null`.
  * `is_null` - Returns present iff the object is null. Wrapper around
    `kd.extension_types.is_null`.

A null instance of an extension type has no attributes and calling `getattr`
or `with_attrs` on it will raise an error.

Example:
  @kd.extension_type()
  class A(kd.extension_types.NullableMixin):
    x: kd.INT32

  # Normal usage.
  a = A(1)
  a.x  # -&gt; 1.
  a.is_null()  # kd.missing

  # Null usage.
  a_null = A.get_null()
  a_null.x  # ERROR
  a_null.is_null()  # kd.present</code></pre>

### `kd.extension_types.dynamic_cast(value: Any, qtype: QType) -> Any` {#kd.extension_types.dynamic_cast}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Up-, down-, and side-casts `value` to `qtype`.</code></pre>

### `kd.extension_types.extension_type(unsafe_override=False) -> Callable[[type[Any]], type[Any]]` {#kd.extension_types.extension_type}
Aliases:

- [kd.extension_type](#kd.extension_type)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a Koda extension type from the given original class.

This function is intended to be used as a class decorator. The decorated class
serves as a schema for the new extension type.

Internally, this function creates the following:
-  A new `QType` for the extension type, which is a labeled `QType` on top of
  an arolla::Object.
- A `QValue` class for representing evaluated instances of the extension type.
- An `ExprView` class for representing expressions that will evaluate to the
  extension type.

It replaces the decorated class with a new class that acts as a factory. This
factory&#39;s `__new__` method dispatches to either create an `Expr` or a `QValue`
instance, depending on the types of the arguments provided.

The fields of the dataclass are exposed as properties on both the `QValue` and
`ExprView` classes. Any methods defined on the dataclass are also carried
over.

Note:
- The decorated class must not have its own `__new__` method - it will be
  ignored.
- The decorated class must not have its own `__init__` method - it will be
  ignored.
- The type annotations on the fields of the dataclass are used to determine
  the schema of the underlying `DataSlice` (if relevant).
- All fields must have type annotations.
- Supported annotations include `SchemaItem`, `DataSlice`, `DataBag`,
  `JaggedShape`, and other extension types. Additionally, any QType can be
  used as an annotation.
- The `with_attrs` method is automatically added if not already present,
  allowing for attributes to be dynamically updated.
- If the class implements the `_extension_arg_boxing(self, value, annotation)`
  classmethod, it will be called on all input arguments (including defaults)
  passed to `MyExtension(...)`. The classmethod should return an arolla QValue
  or an Expression. If the class does not implement such a method, a default
  method will be used.
- If the class implements the `_extension_post_init(self)` method, it will be
  called as the final step of instantiating the extension through
  `MyExtension(...)`. The method should take `self`, do the necessary post
  processing, and then return the (potentially modified) `self`. As with other
  methods, it&#39;s required to be traceable in order to function in a tracing
  context.

Example:
  @extension_type()
  class MyPoint:
    x: kd.FLOAT32
    y: kd.FLOAT32

    def norm(self):
      return (self.x**2 + self.y**2)**0.5

  # Creates a QValue instance of MyPoint.
  p1 = MyPoint(x=1.0, y=2.0)

Extension type inheritance is supported through Python inheritance. Passing an
extension type argument to a functor will automatically upcast / downcast the
argument to the correct extension type based on the argument annotation. To
support calling a child class&#39;s methods after upcasting, the parent method
must be annotated with @kd.extension_types.virtual() and the child method
must be annotated with @kd.extension_types.override(). Internally, this traces
the methods into Functors. Virtual methods _require_ proper return
annotations (and if relevant, input argument annotations).

Example:
  @kd.extension_type(unsafe_override=True)
  class A:
    x: kd.INT32

    def fn(self, y):  # Normal method.
      return self.x + y

    @kd.extension_types.virtual()
    def virt_fn(self, y):  # Virtual method.
      return self.x * y

  @kd.extension_type(unsafe_override=True)
  class B(A):  # Inherits from A.
    y: kd.FLOAT32

    def fn(self, y):
      return self.x + self.y + y

    @kd.extension_types.override()
    def virt_fn(self, y):
      return self.x * self.y * y

  @kd.fn
  def call_a_fn(a: A):  # Automatically casts to A.
    return a.fn(4)      # Calls non-virtual method.

  @kd.fn
  def call_a_virt_fn(a: A):  # Automatically casts to A.
    return a.virt_fn(4)      # Calls virtual method.

  b = B(2, 3)
  # -&gt; 6. `fn` is _not_ marked as virtual, so the parent method is invoked.
  call_a_fn(b)
  # -&gt; 24.0. `virt_fn` is marked as virtual, so the child method is invoked.
  call_a_virt_fn(b)

Args:
  unsafe_override: Overrides existing registered extension types.

Returns:
  A new class that serves as a factory for the extension type.</code></pre>

### `kd.extension_types.get_annotations(cls: type[Any]) -> dict[str, Any]` {#kd.extension_types.get_annotations}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the annotations for the provided extension type class.</code></pre>

### `kd.extension_types.get_attr(ext: Any, attr: str | QValue, qtype: QType) -> Any` {#kd.extension_types.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attribute of `ext` with name `attr` and type `qtype`.</code></pre>

### `kd.extension_types.get_attr_qtype(ext, attr)` {#kd.extension_types.get_attr_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the qtype of the `attr`, or NOTHING if the `attr` is missing.</code></pre>

### `kd.extension_types.get_extension_cls(qtype: QType) -> type[Any]` {#kd.extension_types.get_extension_cls}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the extension type class for the given QType.</code></pre>

### `kd.extension_types.get_extension_qtype(cls: type[Any]) -> QType` {#kd.extension_types.get_extension_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the QType for the given extension type class.</code></pre>

### `kd.extension_types.has_attr(ext, attr)` {#kd.extension_types.has_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `attr` is an attribute of `ext`.</code></pre>

### `kd.extension_types.is_koda_extension(x: Any) -> bool` {#kd.extension_types.is_koda_extension}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff the given object is an instance of a Koda extension type.</code></pre>

### `kd.extension_types.is_koda_extension_type(cls: type[Any]) -> bool` {#kd.extension_types.is_koda_extension_type}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff the given type is a registered Koda extension type.</code></pre>

### `kd.extension_types.is_null(ext)` {#kd.extension_types.is_null}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `ext` is null.</code></pre>

### `kd.extension_types.make(qtype: QType, prototype: Object | None = None, /, **attrs: Any)` {#kd.extension_types.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an extension type of the given `qtype` with the given `attrs`.

Args:
  qtype: the output qtype of the extension type.
  prototype: parent object (arolla.Object).
  **attrs: attributes of the extension type.</code></pre>

### `kd.extension_types.make_null(qtype: QType) -> Any` {#kd.extension_types.make_null}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a null instance of an extension type.</code></pre>

### `kd.extension_types.override()` {#kd.extension_types.override}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Marks the method as overriding a virtual method.</code></pre>

### `kd.extension_types.unwrap(ext)` {#kd.extension_types.unwrap}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unwraps the extension type `ext` into an arolla::Object.</code></pre>

### `kd.extension_types.virtual()` {#kd.extension_types.virtual}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Marks the method as virtual, allowing it to be overridden.</code></pre>

### `kd.extension_types.with_attrs(ext, /, **attrs)` {#kd.extension_types.with_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ext` containing the given `attrs`.</code></pre>

### `kd.extension_types.wrap(x: Any, qtype: QType) -> Any` {#kd.extension_types.wrap}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wraps `x` into an instance of the given extension type.</code></pre>

</section>

### kd.functor {#kd.functor}

Operators to create and call functors.

<section class="zippy closed">

**Operators**

### `kd.functor.FunctorFactory(*args, **kwargs)` {#kd.functor.FunctorFactory}

<pre class="no-copy"><code class="lang-text no-auto-prettify">`functor_factory` argument protocol for `trace_as_fn`.

Implements:
  (py_types.FunctionType, return_type_as: arolla.QValue) -&gt; DataItem</code></pre>

### `kd.functor.allow_arbitrary_unused_inputs(fn_def: DataSlice) -> DataSlice` {#kd.functor.allow_arbitrary_unused_inputs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a functor that allows unused inputs but otherwise behaves the same.

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
  there is no existing variadic keyword argument.</code></pre>

### `kd.functor.bind(fn_def: DataSlice, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataSlice` {#kd.functor.bind}
Aliases:

- [kd.bind](#kd.bind)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor that partially binds a function.

This function is intended to work the same as functools.partial in Python.
The bound positional arguments are prepended to the arguments provided at call
time. For keyword arguments, for every &#34;k=something&#34; argument that you pass
to this function, whenever the resulting functor is called, if the user did
not provide &#34;k=something_else&#34; at call time, we will add &#34;k=something&#34;.

Moreover, if the user provides a value for a positional-or-keyword argument
positionally, and it was previously bound using bind(..., k=v), an exception
will occur.

You can pass expressions with their own inputs as values in `args` and
`kwargs`. Those inputs will become inputs of the resulting functor, will be
used to compute those expressions, _and_ they will also be passed to the
underying functor.

Example:
  f = kd.bind(kd.fn(I.x + I.y), x=0)
  kd.call(f, y=1)  # 1
  g = kd.bind(kd.fn(S + 7), 5)
  kd.call(g) # 12

Args:
  fn_def: A Koda functor.
  *args: Positional arguments to bind. The values may be Koda expressions or
    DataItems.
  return_type_as: The return type of the functor is expected to be the same as
    the type of this value. This needs to be specified if the functor does not
    return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
    passed here.
  **kwargs: Keyword arguments to bind. The values in this map may be Koda
    expressions or DataItems. When they are expressions, they must evaluate to
    a DataSlice/DataItem or a primitive that will be automatically wrapped
    into a DataItem. This function creates auxiliary variables with names
    starting with &#39;_aux_fn&#39;, so it is not recommended to pass variables with
    such names.

Returns:
  A new Koda functor with some parameters bound.</code></pre>

### `kd.functor.call(fn, *args, return_type_as=None, **kwargs)` {#kd.functor.call}
Aliases:

- [kd.call](#kd.call)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor.

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
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The result of the call.</code></pre>

### `kd.functor.call_and_update_namedtuple(fn, *args, namedtuple_to_update, **kwargs)` {#kd.functor.call_and_update_namedtuple}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor which returns a namedtuple and applies it as an update.

This operator exists to avoid the need to specify return_type_as for the inner
call (since the returned namedtuple may have a subset of fields of the
original namedtuple, potentially in a different order).

Example:
  kd.functor.call_and_update_namedtuple(
      kd.fn(lambda x: kd.namedtuple(x=x * 2)),
      x=2,
      namedtuple_to_update=kd.namedtuple(x=1, y=2))
  # returns kd.namedtuple(x=4, y=2)

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.
  namedtuple_to_update: The namedtuple to be updated with the result of the
    call. The returned namedtuple must have a subset (possibly empty or full)
    of fields of this namedtuple, with the same types.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The updated namedtuple.</code></pre>

### `kd.functor.call_fn_normally_when_parallel(fn, *args, return_type_as=None, **kwargs)` {#kd.functor.call_fn_normally_when_parallel}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Special call that will invoke the functor normally in parallel mode.

Normally, nested functor calls are also parallelized in parallel mode. This
operator can be used to disable the nested parallelization for a specific
call. Instead, the parallel evaluation will first wait for all inputs of
the call to be ready, and then call the functor normally on them. Note that
the functor must accept and return non-parallel types (DataSlices, DataBags)
and not futures. The functor may return an iterable, which will be converted
to a stream in parallel mode, or another non-parallel value, which will
be converted to a future in parallel mode. The functor must not accept
iterables as inputs, which will result in an error.

Outside of the parallel mode, this operator behaves exactly like
`functor.call`.

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword arguments to pass to the call.

Returns:
  The result of the call.</code></pre>

### `kd.functor.call_fn_returning_stream_when_parallel(fn, *args, return_type_as=ITERABLE[DATA_SLICE]{sequence(value_qtype=DATA_SLICE)}, **kwargs)` {#kd.functor.call_fn_returning_stream_when_parallel}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Special call that will be transformed to expect fn to return a stream.

It should be used only if functor is provided externally in production
enviroment. Prefer `functor.call` for functors fully implemented in Python.

Args:
  fn: function to be called. Should return Iterable in interactive mode and
    Stream in parallel mode.
  *args: positional args to pass to the function.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a Iterable[DataSlice].
  **kwargs: The keyword arguments to pass to the call.</code></pre>

### `kd.functor.expr_fn(returns: Any, *, signature: DataSlice | None = None, auto_variables: bool = False, **variables: Any) -> DataSlice` {#kd.functor.expr_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a functor.

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
    based on the provided expressions for &#39;returns&#39; and user-provided
    variables. All non-scalar-primitive DataSlice literals become their own
    variables, and all named subexpressions become their own variables. This
    helps readability and manipulation of the resulting functor.
  **variables: The variables of the functor. Each variable can either be an
    expression to be evaluated, or a DataItem, or a primitive that will be
    wrapped as a DataItem. The result of evaluating the variable can be
    accessed as V.smth in other expressions.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.flat_map_chain(iterable, fn, value_type_as=None)` {#kd.functor.flat_map_chain}
Aliases:

- [kd.flat_map_chain](#kd.flat_map_chain)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an iterable.
The resulting iterable is then chained to get the final result.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))`.

Example:
  ```
  kd.functor.flat_map_chain(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 2, 3, 10, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as chained output of `fn`.</code></pre>

### `kd.functor.flat_map_interleaved(iterable, fn, value_type_as=None)` {#kd.functor.flat_map_interleaved}
Aliases:

- [kd.flat_map_interleaved](#kd.flat_map_interleaved)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given iterable.

`fn` is called for each item in the iterable, and must return an
iterable. The resulting iterables are then interleaved to get the final
result. Please note that the order of the items in each functor output
iterable is preserved, while the order of these iterables is not preserved.

If `fn=lambda x: kd.iterables.make(f(x), g(x))` and
`iterable=kd.iterables.make(x1, x2)`, the resulting iterable will be
`kd.iterables.make(f(x1), g(x1), f(x2), g(x2))` or `kd.iterables.make(f(x1),
f(x2), g(x1), g(x2))` or `kd.iterables.make(g(x1), f(x1), f(x2), g(x2))` or
`kd.iterables.make(g(x1), g(x2), f(x1), f(x2))`.

Example:
  ```
  kd.functor.flat_map_interleaved(
      kd.iterables.make(1, 10),
      lambda x: kd.iterables.make(x, x * 2, x * 3),
  )
  ```
  result: `kd.iterables.make(1, 10, 2, 3, 20, 30)`.

Args:
  iterable: The iterable to iterate over.
  fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument and must return an
    iterable.
  value_type_as: The type to use as element type of the resulting iterable.

Returns:
  The resulting iterable as interleaved output of `fn`.</code></pre>

### `kd.functor.fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataSlice` {#kd.functor.fn}
Aliases:

- [kd.fn](#kd.fn)

- [kd_ext.Fn](#kd_ext.Fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor representing `f`.

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
  A Koda functor representing `f`.</code></pre>

### `kd.functor.for_(iterable, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.functor.for_}
Aliases:

- [kd.for_](#kd.for_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes a loop over the given iterable.

Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
and that dictates what this operator returns.

When `returns` is specified, it is one more variable added to `initial_state`,
and the value of that variable at the end of the loop is returned.

When `yields` is specified, it must be an iterable, and the value
passed there, as well as the values set to this variable in each
iteration of the loop, are chained to get the resulting iterable.

When `yields_interleaved` is specified, the behavior is the same as `yields`,
but the values are interleaved instead of chained.

The behavior of the loop is equivalent to the following pseudocode:

  state = initial_state  # Also add `returns` to it if specified.
  while condition_fn(state):
    item = next(iterable)
    if item == &lt;end-of-iterable&gt;:
      upd = finalize_fn(**state)
    else:
      upd = body_fn(item, **state)
    if yields/yields_interleaved is specified:
      yield the corresponding data from upd, and remove it from upd.
    state.update(upd)
    if item == &lt;end-of-iterable&gt;:
      break
  if returns is specified:
    return state[&#39;returns&#39;]

Args:
  iterable: The iterable to iterate over.
  body_fn: The function to be executed for each item in the iterable. It will
    receive the iterable item as the positional argument, and the loop
    variables as keyword arguments (excluding `yields`/`yields_interleaved` if
    those are specified), and must return a namedtuple with the new values for
    some or all loop variables (including `yields`/`yields_interleaved` if
    those are specified).
  finalize_fn: The function to be executed when the iterable is exhausted. It
    will receive the same arguments as `body_fn` except the positional
    argument, and must return the same namedtuple. If not specified, the state
    at the end will be the same as the state after processing the last item.
    Note that finalize_fn is not called if condition_fn ever returns a missing
    mask.
  condition_fn: The function to be executed to determine whether to continue
    the loop. It will receive the loop variables as keyword arguments, and
    must return a MASK scalar. Can be used to terminate the loop early without
    processing all items in the iterable. If not specified, the loop will
    continue until the iterable is exhausted.
  returns: The loop variable that holds the return value of the loop.
  yields: The loop variables that holds the values to yield at each iteration,
    to be chained together.
  yields_interleaved: The loop variables that holds the values to yield at
    each iteration, to be interleaved.
  **initial_state: The initial state of the loop variables.

Returns:
  Either the return value or the iterable of yielded values.</code></pre>

### `kd.functor.fstr_fn(returns: str, **kwargs) -> DataSlice` {#kd.functor.fstr_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor from format string.

Format-string must be created via Python f-string syntax. It must contain at
least one formatted expression.

kwargs are used to assign values to the functor variables and can be used in
the formatted expression using V. syntax.

Each formatted expression must have custom format specification,
e.g. `{I.x:s}` or `{V.y:.2f}`.

Examples:
  kd.call(fstr_fn(f&#39;{I.x:s} {I.y:s}&#39;), x=1, y=2)  # kd.slice(&#39;1 2&#39;)
  kd.call(fstr_fn(f&#39;{V.x:s} {I.y:s}&#39;, x=1), y=2)  # kd.slice(&#39;1 2&#39;)
  kd.call(fstr_fn(f&#39;{(I.x + I.y):s}&#39;), x=1, y=2)  # kd.slice(&#39;3&#39;)
  kd.call(fstr_fn(&#39;abc&#39;))  # error - no substitutions
  kd.call(fstr_fn(&#39;{I.x}&#39;), x=1)  # error - format should be f-string

Args:
  returns: A format string.
  **kwargs: variable assignments.</code></pre>

### `kd.functor.get_signature(fn_def: DataSlice) -> DataSlice` {#kd.functor.get_signature}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Retrieves the signature attached to the given functor.

Args:
  fn_def: The functor to retrieve the signature for, or a slice thereof.

Returns:
  The signature(s) attached to the functor(s).</code></pre>

### `kd.functor.has_fn(x)` {#kd.functor.has_fn}
Aliases:

- [kd.has_fn](#kd.has_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` for each item in `x` that is a functor.

Note that this is a pointwise operator. See `kd.functor.is_fn` for the
corresponding scalar version.

Args:
  x: DataSlice to check.</code></pre>

### `kd.functor.if_(cond, yes_fn, no_fn, *args, return_type_as=None, **kwargs)` {#kd.functor.if_}
Aliases:

- [kd.if_](#kd.if_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls either `yes_fn` or `no_fn` depending on the value of `cond`.

This a short-circuit sibling of kd.cond which executes only one of the two
functors, and therefore requires that `cond` is a MASK scalar.

Example:
  x = kd.item(5)
  kd.if_(x &gt; 3, lambda x: x + 1, lambda x: x - 1, x)
  # returns 6, note that both lambdas were traced into functors.

Args:
  cond: The condition to branch on. Must be a MASK scalar.
  yes_fn: The functor to be called if `cond` is present.
  no_fn: The functor to be called if `cond` is missing.
  *args: The positional argument(s) to pass to the functor.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: The keyword argument(s) to pass to the functor.

Returns:
  The result of the call of either `yes_fn` or `no_fn`.</code></pre>

### `kd.functor.is_fn(obj: Any) -> DataSlice` {#kd.functor.is_fn}
Aliases:

- [kd.is_fn](#kd.is_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Checks if `obj` represents a functor.

Args:
  obj: The value to check.

Returns:
  kd.present if `obj` is a DataSlice representing a functor, kd.missing
  otherwise (for example if obj has wrong type).</code></pre>

### `kd.functor.map(fn, *args, include_missing=False, **kwargs)` {#kd.functor.map}
Aliases:

- [kd.map](#kd.map)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Aligns fn and args/kwargs and calls corresponding fn on corresponding arg.

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
  The evaluation result.</code></pre>

### `kd.functor.map_py_fn(f: Union[Callable[..., Any], PyObject], *, schema: Any = None, max_threads: Any = 1, ndim: Any = 0, include_missing: Any = None, **defaults: Any) -> DataSlice` {#kd.functor.map_py_fn}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a python function for kd.map_py.

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
    docstring for py_fn for more details.</code></pre>

### `kd.functor.py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataSlice` {#kd.functor.py_fn}
Aliases:

- [kd.py_fn](#kd.py_fn)

- [kd_ext.PyFn](#kd_ext.PyFn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a python function.

This is the most flexible way to wrap a python function for large, complex
code that doesn&#39;t require serialization. Note that functions wrapped with
py_fn are not serializable. See register_py_fn for an alternative that is
serializable.

Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
functor will have exactly the same signature as the original function. In
particular, if the original function does not accept variadic keyword
arguments and and unknown argument is passed when calling the functor, an
exception will occur.

Args:
  f: Python function. It is required that this function returns a
    DataSlice/DataItem or a primitive that will be automatically wrapped into
    a DataItem.
  return_type_as: The return type of the function is expected to be the same
    as the type of this value. This needs to be specified if the function does
    not return a DataSlice/DataItem or a primitive that would be auto-boxed
    into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **defaults: Keyword defaults to bind to the function. The values in this map
    may be Koda expressions or DataItems (see docstring for kd.bind for more
    details). Defaults can be overridden through kd.call arguments. **defaults
    and inputs to kd.call will be combined and passed through to the function.
    If a parameter that is not passed does not have a default value defined by
    the function then an exception will occur.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.reduce(fn, items, initial_value)` {#kd.functor.reduce}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces an iterable using the given functor.

The result is a DataSlice that has the value: fn(fn(fn(initial_value,
items[0]), items[1]), ...), where the fn calls are done in the order of the
items in the iterable.

Args:
  fn: A binary function or functor to be applied to each item of the iterable;
    its return type must be the same as the first argument.
  items: An iterable to be reduced.
  initial_value: The initial value to be passed to the functor.

Returns:
  Result of the reduction as a single value.</code></pre>

### `kd.functor.register_py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, unsafe_override: bool = False, **defaults: Any) -> DataSlice` {#kd.functor.register_py_fn}
Aliases:

- [kd.register_py_fn](#kd.register_py_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor wrapping a function registered as an operator.

This is the recommended way to wrap a non-traceable python function into a
functor.

`f` will be wrapped into an operator and registered with the name taken from
the `__module__` and `__qualname__` attributes. This requires `f` to be named,
and to not be a locally defined function. Furthermore, attempts to call
`register_py_fn` on the same function will fail unless unsafe_override is True
(not recommended).

The resulting functor can be serialized and loaded from a different process
(unlike `py_fn`). In order for the serialized functor to be deserialized, an
equivalent call to `register_py_fn` is required to have been made in the
process that performs the deserialization. In practice, this is often
implemented through `f` being a top-level function that is traced at library
import time.

Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
functor will have exactly the same signature as the original function. In
particular, if the original function does not accept variadic keyword
arguments and and unknown argument is passed when calling the functor, an
exception will occur.

Args:
  f: Python function. It is required that this function returns a
    DataSlice/DataItem or a primitive that will be automatically wrapped into
    a DataItem. The function must also have `__qualname__` and `__module__`
    attributes set.
  return_type_as: The return type of the function is expected to be the same
    as the type of this value. This needs to be specified if the function does
    not return a DataSlice/DataItem or a primitive that would be auto-boxed
    into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  unsafe_override: Whether to override an existing operator. Not recommended
    unless you know what you are doing.
  **defaults: Keyword defaults to bind to the function. The values in this map
    may be Koda expressions or DataItems (see docstring for kd.bind for more
    details). Defaults can be overridden through kd.call arguments. **defaults
    and inputs to kd.call will be combined and passed through to the function.
    If a parameter that is not passed does not have a default value defined by
    the function then an exception will occur.

Returns:
  A DataItem representing the functor.</code></pre>

### `kd.functor.trace_as_fn(*, name: str | None = None, return_type_as: Any = None, functor_factory: FunctorFactory | None = None)` {#kd.functor.trace_as_fn}
Aliases:

- [kd.trace_as_fn](#kd.trace_as_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A decorator to customize the tracing behavior for a particular function.

A function with this decorator is converted to an internally-stored functor.
In traced expressions that call the function, that functor is invoked as a
sub-functor via by &#39;kde.call&#39;, rather than the function being re-traced.
Additionally, the functor passed to &#39;kde.call&#39; is assigned a name, so that
when auto_variables=True is used (which is the default in kd.trace_py_fn),
the functor for the decorated function will become an attribute of the
functor for the outer function being traced.
The result of &#39;kde.call&#39; is also assigned a name with a &#39;_result&#39; suffix, so
that it also becomes an separate variable in the outer function being traced.
This is useful for debugging.

This can be used to avoid excessive re-tracing and recompilation of shared
python functions, to quickly add structure to the functor produced by tracing
for complex computations, or to conveniently embed a py_fn into a traced
expression.

When using kd.parallel.call_multithreaded, using this decorator on
sub-functors can improve parallelism, since all sub-functor calls
are treated as separate tasks to be parallelized there.

This decorator is intended to be applied to standalone functions.

When applying it to a lambda, consider specifying an explicit name, otherwise
it will be called &#39;&lt;lambda&gt;&#39; or &#39;&lt;lambda&gt;_0&#39; etc, which is not very useful.

When applying it to a class method, it is likely to fail in tracing mode
because it will try to auto-box the class instance into an expr, which is
likely not supported.

When executing the resulting function in eager mode, we will evaluate the
underlying function directly instead of evaluating the functor, to have
nicer stack traces in case of an exception. However, we will still apply
the boxing rules on the returned value (for example, convert Python primitives
to DataItems) to better emulate what will happen in tracing mode.</code></pre>

### `kd.functor.trace_py_fn(f: Callable[..., Any], *, auto_variables: bool = True, **defaults: Any) -> DataSlice` {#kd.functor.trace_py_fn}
Aliases:

- [kd.trace_py_fn](#kd.trace_py_fn)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor created by tracing a given Python function.

When &#39;f&#39; has variadic positional (*args) or variadic keyword
(**kwargs) arguments, their name must start with &#39;unused&#39;, and they
must actually be unused inside &#39;f&#39;.
&#39;f&#39; must not use Python control flow operations such as if or for.

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
  A DataItem representing the functor.</code></pre>

### `kd.functor.while_(condition_fn, body_fn, *, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.functor.while_}
Aliases:

- [kd.while_](#kd.while_)

<pre class="no-copy"><code class="lang-text no-auto-prettify">While a condition functor returns present, runs a body functor repeatedly.

The items in `initial_state` (and `returns`, if specified) are used to
initialize a dict of state variables, which are passed as keyword arguments
to `condition_fn` and `body_fn` on each loop iteration, and updated from the
namedtuple (see kd.namedtuple) return value of `body_fn`.

Exactly one of `returns`, `yields`, or `yields_interleaved` must be specified.
The return value of this operator depends on which one is present:
- `returns`: the value of `returns` when the loop ends. The initial value of
  `returns` must have the same qtype (e.g. DataSlice, DataBag) as the final
  return value.
- `yields`: a single iterable chained (using `kd.iterables.chain`) from the
  value of `yields` returned from each invocation of `body_fn`, The value of
  `yields` must always be an iterable, including initially.
- `yields_interleaved`: the same as for `yields`, but the iterables are
  interleaved (using `kd.iterables.iterleave`) instead of being chained.

Args:
  condition_fn: A functor with keyword argument names matching the state
    variable names and returning a MASK DataItem.
  body_fn: A functor with argument names matching the state variable names and
    returning a namedtuple (see kd.namedtuple) with a subset of the keys
    of `initial_state`.
  returns: If present, the initial value of the &#39;returns&#39; state variable.
  yields: If present, the initial value of the &#39;yields&#39; state variable.
  yields_interleaved: If present, the initial value of the
    `yields_interleaved` state variable.
  **initial_state: A dict of the initial values for state variables.

Returns:
  If `returns` is a state variable, the value of `returns` when the loop
  ended. Otherwise, an iterable combining the values of `yields` or
  `yields_interleaved` from each body invocation.</code></pre>

</section>

### kd.ids {#kd.ids}

Operators that work with ItemIds.

<section class="zippy closed">

**Operators**

### `kd.ids.agg_uuid(x, ndim=unspecified)` {#kd.ids.agg_uuid}
Aliases:

- [kd.agg_uuid](#kd.agg_uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes aggregated uuid of elements over the last `ndim` dimensions.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 &lt;= ndim &lt;=
    get_ndim(x).

Returns:
  DataSlice with that has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.</code></pre>

### `kd.ids.decode_itemid(ds)` {#kd.ids.decode_itemid}
Aliases:

- [kd.decode_itemid](#kd.decode_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns ItemIds decoded from the base62 strings.</code></pre>

### `kd.ids.deep_uuid(x, /, schema=unspecified, *, seed='')` {#kd.ids.deep_uuid}
Aliases:

- [kd.deep_uuid](#kd.deep_uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve &#39;*&#39; and &#39;**&#39; tokens. If not specified,
    will use the schema of the &#39;x&#39; DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.</code></pre>

### `kd.ids.encode_itemid(ds)` {#kd.ids.encode_itemid}
Aliases:

- [kd.encode_itemid](#kd.encode_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the base62 encoded ItemIds in `ds` as strings.</code></pre>

### `kd.ids.has_uuid(x)` {#kd.ids.has_uuid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that has an UUID.

Also see `kd.ids.is_uuid` for checking if `x` is a UUIDs DataSlice. But note
that `kd.all(kd.has_uuid(x))` is not always equivalent to `kd.is_uuid(x)`. For
example,

  kd.ids.is_uuid(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.ids.is_uuid(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.ids.hash_itemid(x)` {#kd.ids.hash_itemid}
Aliases:

- [kd.hash_itemid](#kd.hash_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a INT64 DataSlice of hash values of `x`.

The hash values are in the range of [0, 2**63-1].

The hash algorithm is subject to change. It is not guaranteed to be stable in
future releases.

Args:
  x: DataSlice of ItemIds.

Returns:
  A DataSlice of INT64 hash values.</code></pre>

### `kd.ids.is_uuid(x)` {#kd.ids.is_uuid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is an UUID DataSlice.

Note that the operator returns `kd.present` even for missing values, as long
as their schema does not prevent containing UUIDs.

Also see `kd.ids.has_uuid` for a pointwise version. But note that
`kd.all(kd.ids.has_uuid(x))` is not always equivalent to `kd.is_uuid(x)`. For
example,

  kd.ids.is_uuid(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.ids.is_uuid(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.ids.has_uuid(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `kd.ids.uuid(seed='', **kwargs)` {#kd.ids.uuid}
Aliases:

- [kd.uuid](#kd.uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuid_for_dict(seed='', **kwargs)` {#kd.ids.uuid_for_dict}
Aliases:

- [kd.uuid_for_dict](#kd.uuid_for_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying dict items.

e.g.

kd.dict([&#39;a&#39;, &#39;b&#39;], [1, 2], itemid=kd.uuid_for_dict(seed=&#39;seed&#39;, a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuid_for_list(seed='', **kwargs)` {#kd.ids.uuid_for_list}
Aliases:

- [kd.uuid_for_list](#kd.uuid_for_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying list items.

e.g.

kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed=&#39;seed&#39;, a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.</code></pre>

### `kd.ids.uuids_with_allocation_size(seed='', *, size)` {#kd.ids.uuids_with_allocation_size}
Aliases:

- [kd.uuids_with_allocation_size](#kd.uuids_with_allocation_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice whose items are uuids.

The uuids are allocated in a single allocation. They are all distinct.
You can think of the result as a DataSlice created with:
[fingerprint(seed, size, i) for i in range(size)]

Args:
  seed: text seed for the uuid computation.
  size: the size of the allocation. It will also be used for the uuid
    computation.

Returns:
  A 1-dimensional DataSlice with `size` distinct uuids.</code></pre>

</section>

### kd.iterables {#kd.iterables}

Operators that work with iterables. These APIs are in active development and might change often.

<section class="zippy closed">

**Operators**

### `kd.iterables.chain(*iterables, value_type_as=unspecified)` {#kd.iterables.chain}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable that chains the given iterables, in the given order.

The iterables must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the iterables, if any.

Args:
  *iterables: A list of iterables to be chained (concatenated).
  value_type_as: A value that has the same type as the iterables. It is useful
    to specify this explicitly if the list of iterables may be empty. If this
    is not specified and the list of iterables is empty, the iterable will
    have DataSlice as the value type.

Returns:
  An iterable that chains the given iterables, in the given order.</code></pre>

### `kd.iterables.from_1d_slice(slice_)` {#kd.iterables.from_1d_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a 1D DataSlice to a Koda iterable of DataItems.

Args:
  slice_: A 1D DataSlice to be converted to an iterable.

Returns:
  A Koda iterable of DataItems, in the order of the slice. All returned
  DataItems point to the same DataBag as the input DataSlice.</code></pre>

### `kd.iterables.interleave(*iterables, value_type_as=unspecified)` {#kd.iterables.interleave}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable that interleaves the given iterables.

The resulting iterable has all items from all input iterables, and the order
within each iterable is preserved. But the order of interleaving of different
iterables can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

The iterables must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the iterables, if any.

Args:
  *iterables: A list of iterables to be interleaved.
  value_type_as: A value that has the same type as the iterables. It is useful
    to specify this explicitly if the list of iterables may be empty. If this
    is not specified and the list of iterables is empty, the iterable will
    have DataSlice as the value type.

Returns:
  An iterable that interleaves the given iterables, in arbitrary order.</code></pre>

### `kd.iterables.make(*items, value_type_as=unspecified)` {#kd.iterables.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable from the provided items, in the given order.

The items must all have the same type (for example data slice, or data bag).
However, in case of data slices, the items can have different shapes or
schemas.

Args:
  *items: Items to be put into the iterable.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  An iterable with the given items.</code></pre>

### `kd.iterables.make_unordered(*items, value_type_as=unspecified)` {#kd.iterables.make_unordered}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates an iterable from the provided items, in an arbitrary order.

Having unspecified order allows the parallel execution to put the items into
the iterable in the order they are computed, potentially increasing the amount
of parallel processing done.

When used with the non-parallel evaluation, we intentionally randomize the
order to prevent user code from depending on the order, and avoid
discrepancies when switching to parallel evaluation.

Args:
  *items: Items to be put into the iterable.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  An iterable with the given items, in an arbitrary order.</code></pre>

### `kd.iterables.reduce_concat(items, initial_value, ndim=1)` {#kd.iterables.reduce_concat}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates the values of the given iterable.

This operator is a concrete case of the more general kd.functor.reduce, which
exists to speed up such concatenation from O(N^2) that the general reduce
would provide to O(N). See the docstring of kd.concat for more details about
the concatenation semantics.

Args:
  items: An iterable of data slices to be concatenated.
  initial_value: The initial value to be concatenated before items.
  ndim: The number of last dimensions to concatenate.

Returns:
  The concatenated data slice.</code></pre>

### `kd.iterables.reduce_updated_bag(items, initial_value)` {#kd.iterables.reduce_updated_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Merges the bags from the given iterable into one.

This operator is a concrete case of the more general kd.functor.reduce, which
exists to speed up such merging from O(N^2) that the general reduce
would provide to O(N). See the docstring of kd.updated_bag for more details
about the merging semantics.

Args:
  items: An iterable of data bags to be merged.
  initial_value: The data bag to be merged with the items. Note that the items
    will be merged as updates to this bag, meaning that they will take
    precedence over the initial_value on conflicts.

Returns:
  The merged data bag.</code></pre>

</section>

### kd.json {#kd.json}

JSON serialization operators.

<section class="zippy closed">

**Operators**

### `kd.json.from_json(x, /, schema=OBJECT, default_number_schema=OBJECT, *, on_invalid=DataSlice([], schema: NONE), keys_attr='json_object_keys', values_attr='json_object_values')` {#kd.json.from_json}
Aliases:

- [kd.from_json](#kd.from_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of JSON strings.

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

  kd.from_json(None) -&gt; kd.obj(None)
  kd.from_json(&#39;null&#39;) -&gt; kd.obj(None)
  kd.from_json(&#39;true&#39;) -&gt; kd.obj(True)
  kd.from_json(&#39;[true, false, null]&#39;) -&gt; kd.obj([True, False, None])
  kd.from_json(&#39;[1, 2.0]&#39;) -&gt; kd.obj([1, 2.0])
  kd.from_json(&#39;[1, 2.0]&#39;, kd.OBJECT, kd.FLOAT64)
    -&gt; kd.obj([kd.float64(1.0), kd.float64(2.0)])

JSON objects parsed using an OBJECT schema will record the object key order on
the attribute specified by `keys_attr` as a LIST[STRING], and also redundantly
record a copy of the object values as a parallel LIST on the attribute
specified by `values_attr`. If there are duplicate keys, the last value is the
one stored on the Koda object attribute. If a key conflicts with `keys_attr`
or `values_attr`, it is only available in the `values_attr` list. These
behaviors can be disabled by setting `keys_attr` and/or `values_attr` to None.

For example:

  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None,
             json_object_keys=kd.list([&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]),
             json_object_values=kd.list([1.0, &#39;y&#39;, None]))
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;,
               keys_attr=None, values_attr=None) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None)
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;y&#34;, &#34;c&#34;: null}&#39;,
               keys_attr=&#39;my_keys&#39;, values_attr=&#39;my_values&#39;) -&gt;
      kd.obj(a=1.0, b=&#39;y&#39;, c=None,
             my_keys=kd.list([&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]),
             my_values=kd.list([1.0, &#39;y&#39;, None]))
  kd.from_json(&#39;{&#34;a&#34;: 1, &#34;a&#34;: 2&#34;, &#34;a&#34;: 3}&#39;) -&gt;
      kd.obj(a=3.0,
             json_object_keys=kd.list([&#39;a&#39;, &#39;a&#39;, &#39;a&#39;]),
             json_object_values=kd.list([1.0, 2.0, 3.0]))
  kd.from_json(&#39;{&#34;json_object_keys&#34;: [&#34;x&#34;, &#34;y&#34;]}&#39;) -&gt;
      kd.obj(json_object_keys=kd.list([&#39;json_object_keys&#39;]),
             json_object_values=kd.list([[&#34;x&#34;, &#34;y&#34;]]))

If `schema` is explicitly specified, it is used to validate the JSON data,
and the result DataSlice will have `schema` as its schema.

OBJECT schemas inside subtrees of `schema` are allowed, and will use the
inference behavior described above.

Primitive schemas in `schema` will attempt to cast any JSON primitives using
normal Koda explicit casting rules, and if those fail, using the following
additional rules:
- BYTES will accept JSON strings containing base64 (RFC 4648 section 4)

If entity schemas in `schema` have attributes matching `keys_attr` and/or
`values_attr`, then the object key and/or value order (respectively) will be
recorded as lists on those attributes, similar to the behavior for OBJECT
described above. These attributes must have schemas LIST[STRING] and
LIST[T] (for a T compatible with the contained values) if present.

For example:

  kd.from_json(&#39;null&#39;, kd.MASK) -&gt; kd.missing
  kd.from_json(&#39;null&#39;, kd.STRING) -&gt; kd.str(None)
  kd.from_json(&#39;123&#39;, kd.INT32) -&gt; kd.int32(123)
  kd.from_json(&#39;123&#39;, kd.FLOAT32) -&gt; kd.int32(123.0)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.STRING) -&gt; kd.string(&#39;123&#39;)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.INT32) -&gt; kd.int32(123)
  kd.from_json(&#39;&#34;123&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(123.0)
  kd.from_json(&#39;&#34;MTIz&#34;&#39;, kd.BYTES) -&gt; kd.bytes(b&#39;123&#39;)
  kd.from_json(&#39;&#34;inf&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(float(&#39;inf&#39;))
  kd.from_json(&#39;&#34;1e100&#34;&#39;, kd.FLOAT32) -&gt; kd.float32(float(&#39;inf&#39;))
  kd.from_json(&#39;[1, 2, 3]&#39;, kd.list_schema(kd.INT32)) -&gt; kd.list([1, 2, 3])
  kd.from_json(&#39;{&#34;a&#34;: 1}&#39;, kd.schema.new_schema(a=kd.INT32)) -&gt; kd.new(a=1)
  kd.from_json(&#39;{&#34;a&#34;: 1}&#39;, kd.dict_schema(kd.STRING, kd.INT32)
    -&gt; kd.dict({&#34;a&#34;: 1})

  kd.from_json(&#39;{&#34;b&#34;: 1, &#34;a&#34;: 2}&#39;,
               kd.new_schema(
                   a=kd.INT32, json_object_keys=kd.list_schema(kd.STRING))) -&gt;
    kd.new(a=1, json_object_keys=kd.list([&#39;b&#39;, &#39;a&#39;, &#39;c&#39;]))
  kd.from_json(&#39;{&#34;b&#34;: 1, &#34;a&#34;: 2, &#34;c&#34;: 3}&#39;,
               kd.new_schema(a=kd.INT32,
                             json_object_keys=kd.list_schema(kd.STRING),
                             json_object_values=kd.list_schema(kd.OBJECT))) -&gt;
    kd.new(a=1, c=3.0,
           json_object_keys=kd.list([&#39;b&#39;, &#39;a&#39;, &#39;c&#39;]),
           json_object_values=kd.list([1, 2.0, 3.0]))

In general:

  `kd.to_json(kd.from_json(x))` is equivalent to `x`, ignoring differences in
  JSON number formatting and padding.

  `kd.from_json(kd.to_json(x), kd.get_schema(x))` is equivalent to `x` if `x`
  has a concrete (no OBJECT) schema, ignoring differences in Koda itemids.
  In other words, `to_json` doesn&#39;t capture the full information of `x`, but
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
  A DataSlice with the same shape as `x` and schema `schema`.</code></pre>

### `kd.json.to_json(x, /, *, indent=None, ensure_ascii=True, keys_attr='json_object_keys', values_attr='json_object_values', include_missing_values=True)` {#kd.json.to_json}
Aliases:

- [kd.to_json](#kd.to_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts `x` to a DataSlice of JSON strings.

The following schemas are allowed:
- STRING, BYTES, INT32, INT64, FLOAT32, FLOAT64, MASK, BOOLEAN
- LIST[T] where T is an allowed schema
- DICT{K, V} where K is one of {STRING, BYTES, INT32, INT64}, and V is an
  allowed schema
- Entity schemas where all attribute values have allowed schemas
- OBJECT schemas resolving to allowed schemas

Itemid cycles are not allowed.

Missing DataSlice items in the input are missing in the result. Missing values
inside of lists/entities/etc. are encoded as JSON `null` (or `false` for
`kd.missing`). If `include_missing_values` is `False`, entity attributes with
missing values are omitted from the JSON output.

For example:

  kd.to_json(None) -&gt; kd.str(None)
  kd.to_json(kd.missing) -&gt; kd.str(None)
  kd.to_json(kd.present) -&gt; &#39;true&#39;
  kd.to_json(True) -&gt; &#39;true&#39;
  kd.to_json(kd.slice([1, None, 3])) -&gt; [&#39;1&#39;, None, &#39;3&#39;]
  kd.to_json(kd.list([1, None, 3])) -&gt; &#39;[1, null, 3]&#39;
  kd.to_json(kd.dict({&#39;a&#39;: 1, &#39;b&#39;:&#39;2&#39;}) -&gt; &#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;2&#34;}&#39;
  kd.to_json(kd.new(a=1, b=&#39;2&#39;)) -&gt; &#39;{&#34;a&#34;: 1, &#34;b&#34;: &#34;2&#34;}&#39;
  kd.to_json(kd.new(x=None)) -&gt; &#39;{&#34;x&#34;: null}&#39;
  kd.to_json(kd.new(x=kd.missing)) -&gt; &#39;{&#34;x&#34;: false}&#39;
  kd.to_json(kd.new(a=1, b=None), include_missing_values=False)
    -&gt; &#39;{&#34;a&#34;: 1}&#39;

Koda BYTES values are converted to base64 strings (RFC 4648 section 4).

Integers are always stored exactly in decimal. Finite floating point values
are formatted similar to python format string `%.17g`, except that a decimal
point and at least one decimal digit are always present if the format doesn&#39;t
use scientific notation. This appears to match the behavior of python json.

Non-finite floating point values are stored as the strings &#34;inf&#34;, &#34;-inf&#34; and
&#34;nan&#34;. This differs from python json, which emits non-standard JSON tokens
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

  kd.to_json(kd.new(x=1, y=2)) -&gt; &#39;{&#34;x&#34;: 2, &#34;y&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, json_object_keys=kd.list([&#39;y&#39;, &#39;x&#39;])))
    -&gt; &#39;{&#34;y&#34;: 2, &#34;x&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, foo=kd.list([&#39;y&#39;, &#39;x&#39;])), keys_attr=&#39;foo&#39;)
    -&gt; &#39;{&#34;y&#34;: 2, &#34;x&#34;: 1}&#39;
  kd.to_json(kd.new(x=1, y=2, z=3, json_object_keys=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;])))
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 3, &#34;x&#34;: 1}&#39;

  kd.to_json(kd.new(json_object_keys=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;]),
                    json_object_values=kd.list([1, 2, 3])))
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 2, &#34;x&#34;: 3}&#39;
  kd.to_json(kd.new(a=kd.list([&#39;x&#39;, &#39;z&#39;, &#39;x&#39;]), b=kd.list([1, 2, 3])),
             keys_attr=&#39;a&#39;, values_attr=&#39;b&#39;)
    -&gt; &#39;{&#34;x&#34;: 1, &#34;z&#34;: 2, &#34;x&#34;: 3}&#39;


The `indent` and `ensure_ascii` arguments control JSON formatting:
- If `indent` is negative, then the JSON is formatted without any whitespace.
- If `indent` is None (the default), the JSON is formatted with a single
  padding space only after &#39;,&#39; and &#39;:&#39; and no other whitespace.
- If `indent` is zero or positive, the JSON is pretty-printed, with that
  number of spaces used for indenting each level.
- If `ensure_ascii` is True (the default) then all non-ASCII code points in
  strings will be escaped, and the result strings will be ASCII-only.
  Otherwise, they will be left as-is.

For example:

  kd.to_json(kd.list([1, 2, 3]), indent=-1) -&gt; &#39;[1,2,3]&#39;
  kd.to_json(kd.list([1, 2, 3]), indent=2) -&gt; &#39;[\n  1,\n  2,\n  3\n]&#39;

  kd.to_json(&#39;✨&#39;, ensure_ascii=True) -&gt; &#39;&#34;\\u2728&#34;&#39;
  kd.to_json(&#39;✨&#39;, ensure_ascii=False) -&gt; &#39;&#34;✨&#34;&#39;

Args:
  x: The DataSlice to convert.
  indent: An INT32 DataItem that describes how the result should be indented.
  ensure_ascii: A BOOLEAN DataItem that controls non-ASCII escaping.
  keys_attr: A STRING DataItem that controls which entity attribute controls
    json object key order, or None to always use sorted order. Defaults to
    `json_object_keys`.
  values_attr: A STRING DataItem that can be used with `keys_attr` to give
    full control over json object contents. Defaults to
    `json_object_values`.
  include_missing_values: A BOOLEAN DataItem. If `False`, attributes with
    missing values will be omitted from entity JSON objects. Defaults to
    `True`.</code></pre>

</section>

### kd.lists {#kd.lists}

Operators working with lists.

<section class="zippy closed">

**Operators**

### `kd.lists.appended_list(x, append)` {#kd.lists.appended_list}
Aliases:

- [kd.appended_list](#kd.appended_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Appends items in `append` to the end of each list in `x`.

`x` and `append` must have compatible shapes.

The resulting lists have different ItemIds from the original lists.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  DataSlice of lists with new itemd ids in a new immutable DataBag.</code></pre>

### `kd.lists.concat(*lists: DataSlice) -> DataSlice` {#kd.lists.concat}
Aliases:

- [kd.concat_lists](#kd.concat_lists)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of Lists concatenated from the List items of `lists`.

Returned lists are immutable.

Each input DataSlice must contain only present List items, and the item
schemas of each input must be compatible. Input DataSlices are aligned (see
`kd.align`) automatically before concatenation.

If `lists` is empty, this returns a single empty list with OBJECT item schema.

Args:
  *lists: the DataSlices of Lists to concatenate

Returns:
  DataSlice of concatenated Lists</code></pre>

### `kd.lists.explode(x, ndim=1)` {#kd.lists.explode}
Aliases:

- [kd.explode](#kd.explode)

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

### `kd.lists.get_item(x, key_or_index)` {#kd.lists.get_item}

Alias for [kd.core.get_item](#kd.core.get_item) operator.

### `kd.lists.has_list(x)` {#kd.lists.has_list}
Aliases:

- [kd.has_list](#kd.has_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present for each item in `x` that is List.

Note that this is a pointwise operation.

Also see `kd.is_list` for checking if `x` is a List DataSlice. But note that
`kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
example,

  kd.is_list(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_list(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataSlice with the same shape as `x`.</code></pre>

### `kd.lists.implode(x: DataSlice, /, ndim: int | DataSlice = 1, itemid: DataSlice | None = None) -> DataSlice` {#kd.lists.implode}
Aliases:

- [kd.implode](#kd.implode)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Implodes a Dataslice `x` a specified number of times.

Returned lists are immutable.

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
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  DataSlice of nested Lists</code></pre>

### `kd.lists.is_list(x)` {#kd.lists.is_list}
Aliases:

- [kd.is_list](#kd.is_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether x is a List DataSlice.

`x` is a List DataSlice if it meets one of the following conditions:
  1) it has a List schema
  2) it has OBJECT schema and only has List items

Also see `kd.has_list` for a pointwise version. But note that
`kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
example,

  kd.is_list(kd.item(None, kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -&gt; invalid for kd.all
  kd.is_list(kd.item([None], kd.OBJECT)) -&gt; kd.present
  kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -&gt; kd.missing

Args:
  x: DataSlice to check.

Returns:
  A MASK DataItem.</code></pre>

### `kd.lists.like(shape_and_mask_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.lists.like}
Aliases:

- [kd.list_like](#kd.list_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

Returns immutable lists.

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

Returns:
  A DataSlice with the lists.</code></pre>

### `kd.lists.list_append_update(x, append)` {#kd.lists.list_append_update}
Aliases:

- [kd.list_append_update](#kd.list_append_update)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataBag containing an update to a DataSlice of lists.

The updated lists are the lists in `x` with the specified items appended at
the end.

`x` and `append` must have compatible shapes.

The resulting lists maintain the same ItemIds. Also see kd.appended_list()
which works similarly but resulting lists have new ItemIds.

Args:
  x: DataSlice of lists.
  append: DataSlice of values to append to each list in `x`.

Returns:
  A new immutable DataBag containing the list with the appended items.</code></pre>

### `kd.lists.new(items=unspecified, *, item_schema=unspecified, schema=unspecified, itemid=unspecified)` {#kd.lists.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates list(s) by collapsing `items` into an immutable list.

If there is no argument, returns an empty Koda List.
If the argument is a Python list, creates a nested Koda List.

Examples:
kd.list() -&gt; a single empty Koda List
kd.list([1, 2, 3]) -&gt; Koda List with items 1, 2, 3
kd.list([[1, 2, 3], [4, 5]]) -&gt; nested Koda List [[1, 2, 3], [4, 5]]
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

### `kd.lists.select_items(ds, fltr)` {#kd.lists.select_items}
Aliases:

- [kd.select_items](#kd.select_items)

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

### `kd.lists.shaped(shape: JaggedShape, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.lists.shaped}
Aliases:

- [kd.list_shaped](#kd.list_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda lists with the given shape.

Returns immutable lists.

Args:
  shape: the desired shape.
  items: optional items to assign to the newly created lists. If not
    given, the function returns empty lists.
  item_schema: the schema of the list items. If not specified, it will be
    deduced from `items` or defaulted to OBJECT.
  schema: The schema to use for the list. If specified, then item_schema must
    not be specified.
  itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

Returns:
  A DataSlice with the lists.</code></pre>

### `kd.lists.shaped_as(shape_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.lists.shaped_as}
Aliases:

- [kd.list_shaped_as](#kd.list_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Koda lists with shape of the given DataSlice.

Returns immutable lists.

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

Returns:
  A DataSlice with the lists.</code></pre>

### `kd.lists.size(list_slice)` {#kd.lists.size}
Aliases:

- [kd.list_size](#kd.list_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a List.</code></pre>

### `kd.lists.with_list_append_update(x, append)` {#kd.lists.with_list_append_update}
Aliases:

- [kd.with_list_append_update](#kd.with_list_append_update)

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

</section>

### kd.masking {#kd.masking}

Masking operators.

<section class="zippy closed">

**Operators**

### `kd.masking.agg_all(x, ndim=unspecified)` {#kd.masking.agg_all}
Aliases:

- [kd.agg_all](#kd.agg_all)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present if all elements are present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.agg_any(x, ndim=unspecified)` {#kd.masking.agg_any}
Aliases:

- [kd.agg_any](#kd.agg_any)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present if any element is present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.agg_has(x, ndim=unspecified)` {#kd.masking.agg_has}
Aliases:

- [kd.agg_has](#kd.agg_has)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff any element is present along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

It is equivalent to `kd.agg_any(kd.has(x))`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.masking.all(x)` {#kd.masking.all}
Aliases:

- [kd.all](#kd.all)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff all elements are present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.</code></pre>

### `kd.masking.any(x)` {#kd.masking.any}
Aliases:

- [kd.any](#kd.any)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff any element is present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.</code></pre>

### `kd.masking.apply_mask(x, y)` {#kd.masking.apply_mask}
Aliases:

- [kd.apply_mask](#kd.apply_mask)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Filters `x` to items where `y` is present.

Pointwise masking operator that replaces items in DataSlice `x` by None
if corresponding items in DataSlice `y` of MASK dtype is `kd.missing`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  Masked DataSlice.</code></pre>

### `kd.masking.coalesce(x, y)` {#kd.masking.coalesce}
Aliases:

- [kd.coalesce](#kd.coalesce)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Fills in missing values of `x` with values of `y`.

Pointwise masking operator that replaces missing items (i.e. None) in
DataSlice `x` by corresponding items in DataSlice y`.
`x` and `y` do not need to have the same type.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.</code></pre>

### `kd.masking.cond(condition, yes, no=None)` {#kd.masking.cond}
Aliases:

- [kd.cond](#kd.cond)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `yes` where `condition` is present, otherwise `no`.

Pointwise operator selects items in `yes` if corresponding items are
`kd.present` or items in `no` otherwise. `condition` must have MASK dtype.

If `no` is unspecified corresponding items in result are missing.

Note that there is _no_ short-circuiting based on the `condition` - both `yes`
and `no` branches will be evaluated irrespective of its value. See `kd.if_`
for a short-circuiting version of this operator.

Args:
  condition: DataSlice.
  yes: DataSlice.
  no: DataSlice or unspecified.

Returns:
  DataSlice of items from `yes` and `no` based on `condition`.</code></pre>

### `kd.masking.disjoint_coalesce(x, y)` {#kd.masking.disjoint_coalesce}
Aliases:

- [kd.disjoint_coalesce](#kd.disjoint_coalesce)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Fills in missing values of `x` with values of `y`.

Raises if `x` and `y` intersect. It is equivalent to `x | y` with additional
assertion that `x` and `y` are disjoint.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.</code></pre>

### `kd.masking.has(x)` {#kd.masking.has}
Aliases:

- [kd.has](#kd.has)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns presence of `x`.

Pointwise operator which take a DataSlice and return a MASK indicating the
presence of each item in `x`. Returns `kd.present` for present items and
`kd.missing` for missing items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the presence of `x`.</code></pre>

### `kd.masking.has_not(x)` {#kd.masking.has_not}
Aliases:

- [kd.has_not](#kd.has_not)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `x` is missing element-wise.

Pointwise operator which take a DataSlice and return a MASK indicating
iff `x` is missing element-wise. Returns `kd.present` for missing
items and `kd.missing` for present items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the non-presence of `x`.</code></pre>

### `kd.masking.mask_and(x, y)` {#kd.masking.mask_and}
Aliases:

- [kd.mask_and](#kd.mask_and)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_AND operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_AND operation is defined as:
  kd.mask_and(kd.present, kd.present) -&gt; kd.present
  kd.mask_and(kd.present, kd.missing) -&gt; kd.missing
  kd.mask_and(kd.missing, kd.present) -&gt; kd.missing
  kd.mask_and(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x &amp; y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_equal(x, y)` {#kd.masking.mask_equal}
Aliases:

- [kd.mask_equal](#kd.mask_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_EQUAL operation is defined as:
  kd.mask_equal(kd.present, kd.present) -&gt; kd.present
  kd.mask_equal(kd.present, kd.missing) -&gt; kd.missing
  kd.mask_equal(kd.missing, kd.present) -&gt; kd.missing
  kd.mask_equal(kd.missing, kd.missing) -&gt; kd.present

Note that this is different from `x == y`. For example,
  kd.missing == kd.missing -&gt; kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_not_equal(x, y)` {#kd.masking.mask_not_equal}
Aliases:

- [kd.mask_not_equal](#kd.mask_not_equal)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_NOT_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_NOT_EQUAL operation is defined as:
  kd.mask_not_equal(kd.present, kd.present) -&gt; kd.missing
  kd.mask_not_equal(kd.present, kd.missing) -&gt; kd.present
  kd.mask_not_equal(kd.missing, kd.present) -&gt; kd.present
  kd.mask_not_equal(kd.missing, kd.missing) -&gt; kd.missing

Note that this is different from `x != y`. For example,
  kd.present != kd.missing -&gt; kd.missing
  kd.missing != kd.present -&gt; kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.mask_or(x, y)` {#kd.masking.mask_or}
Aliases:

- [kd.mask_or](#kd.mask_or)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise MASK_OR operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_OR operation is defined as:
  kd.mask_or(kd.present, kd.present) -&gt; kd.present
  kd.mask_or(kd.present, kd.missing) -&gt; kd.present
  kd.mask_or(kd.missing, kd.present) -&gt; kd.present
  kd.mask_or(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x | y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

### `kd.masking.present_like(x)` {#kd.masking.present_like}
Aliases:

- [kd.present_like](#kd.present_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the shape and sparsity of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.present_like(x) -&gt; kd.slice([[present], [present, None]])

Args:
  x: DataSlice to match the shape and sparsity of.

Returns:
  A DataSlice with the same shape and sparsity as `x`.</code></pre>

### `kd.masking.present_shaped(shape)` {#kd.masking.present_shaped}
Aliases:

- [kd.present_shaped](#kd.present_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the given shape.

Example:
  shape = kd.shapes.new([2], [1, 2])
  kd.masking.present_shaped(shape) -&gt; kd.slice([[present], [present,
  present]])

Args:
  shape: shape to expand to.

Returns:
  A DataSlice with the same shape as `shape`.</code></pre>

### `kd.masking.present_shaped_as(x)` {#kd.masking.present_shaped_as}
Aliases:

- [kd.present_shaped_as](#kd.present_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice of present masks with the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.masking.present_shaped_as(x) -&gt; kd.slice([[present], [present, present]])

Args:
  x: DataSlice to match the shape of.

Returns:
  A DataSlice with the same shape as `x`.</code></pre>

### `kd.masking.xor(x, y)` {#kd.masking.xor}
Aliases:

- [kd.xor](#kd.xor)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies pointwise XOR operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. XOR operation is defined as:
  kd.xor(kd.present, kd.present) -&gt; kd.missing
  kd.xor(kd.present, kd.missing) -&gt; kd.present
  kd.xor(kd.missing, kd.present) -&gt; kd.present
  kd.xor(kd.missing, kd.missing) -&gt; kd.missing

It is equivalent to `x ^ y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.</code></pre>

</section>

### kd.math {#kd.math}

Arithmetic operators.

<section class="zippy closed">

**Operators**

### `kd.math.abs(x)` {#kd.math.abs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise absolute value of the input.</code></pre>

### `kd.math.add(x, y)` {#kd.math.add}
Aliases:

- [kd.add](#kd.add)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x + y.</code></pre>

### `kd.math.agg_inverse_cdf(x, cdf_arg, ndim=unspecified)` {#kd.math.agg_inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value with CDF (in [0, 1]) approximately equal to the input.

The value is computed along the last ndim dimensions.

The return value will have an offset of floor((cdf - 1e-6) * size()) in the
(ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.
  ndim: The number of dimensions to compute inverse CDF over. Requires 0 &lt;=
    ndim &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_max(x, ndim=unspecified)` {#kd.math.agg_max}
Aliases:

- [kd.agg_max](#kd.agg_max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the maximum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_max(ds)  # -&gt; kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=1)  # -&gt; kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=2)  # -&gt; kd.slice(4)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_mean(x, ndim=unspecified)` {#kd.math.agg_mean}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the means along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, None], [3, 4], [None, None]])
  kd.agg_mean(ds)  # -&gt; kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=1)  # -&gt; kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=2)  # -&gt; kd.slice(2.6666666666666) # (1 + 3 + 4) / 3)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_median(x, ndim=unspecified)` {#kd.math.agg_median}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the medians along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_min(x, ndim=unspecified)` {#kd.math.agg_min}
Aliases:

- [kd.agg_min](#kd.agg_min)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the minimum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_min(ds)  # -&gt; kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=1)  # -&gt; kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=2)  # -&gt; kd.slice(1)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_std(x, unbiased=True, ndim=unspecified)` {#kd.math.agg_std}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the standard deviation along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_std(ds)  # -&gt; kd.slice(1.0)
  kd.agg_std(ds, unbiased=False)  # -&gt; kd.slice(0.8164966)

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_sum(x, ndim=unspecified)` {#kd.math.agg_sum}
Aliases:

- [kd.agg_sum](#kd.agg_sum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the sums along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4], [None, None]])
  kd.agg_sum(ds)  # -&gt; kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=1)  # -&gt; kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=2)  # -&gt; kd.slice(9)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.agg_var(x, unbiased=True, ndim=unspecified)` {#kd.math.agg_var}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the variance along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_var(ds)  # -&gt; kd.slice(1.0)
  kd.agg_var(ds, unbiased=False)  # -&gt; kd.slice([0.6666667])

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.argmax(x, ndim=unspecified)` {#kd.math.argmax}
Aliases:

- [kd.argmax](#kd.argmax)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns indices of the maximum of items along the last ndim dimensions.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Returns the index of NaN in case there is a NaN present.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None], [2, NaN, 1]])
  kd.argmax(ds)  # -&gt; kd.slice([0, 1, None, 1])
  kd.argmax(ds, ndim=1)  # -&gt; kd.slice([0, 1, None, 1])
  kd.argmax(ds, ndim=2)  # -&gt; kd.slice(8) # index of NaN

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.argmin(x, ndim=unspecified)` {#kd.math.argmin}
Aliases:

- [kd.argmin](#kd.argmin)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns indices of the minimum of items along the last ndim dimensions.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Returns the index of NaN in case there is a NaN present.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None], [2, NaN, 1]])
  kd.argmin(ds)  # -&gt; kd.slice([2, 0, None, 1])
  kd.argmin(ds, ndim=1)  # -&gt; kd.slice([2, 0, None, 1])
  kd.argmin(ds, ndim=2)  # -&gt; kd.slice(8) # index of NaN

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.math.cdf(x, weights=unspecified, ndim=unspecified)` {#kd.math.cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the CDF of x in the last ndim dimensions of x element-wise.

The CDF is an array of floating-point values of the same shape as x and
weights, where each element represents which percentile the corresponding
element in x is situated at in its sorted group, i.e. the percentage of values
in the group that are smaller than or equal to it.

Args:
  x: a DataSlice of numbers.
  weights: if provided, will compute weighted CDF: each output value will
    correspond to the weight percentage of values smaller than or equal to x.
  ndim: The number of dimensions to compute CDF over.</code></pre>

### `kd.math.ceil(x)` {#kd.math.ceil}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise ceiling of the input, e.g.

rounding up: returns the smallest integer value that is not less than the
input.</code></pre>

### `kd.math.cum_max(x, ndim=unspecified)` {#kd.math.cum_max}
Aliases:

- [kd.cum_max](#kd.cum_max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative max of items along the last ndim dimensions.</code></pre>

### `kd.math.cum_min(x, ndim=unspecified)` {#kd.math.cum_min}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative minimum of items along the last ndim dimensions.</code></pre>

### `kd.math.cum_sum(x, ndim=unspecified)` {#kd.math.cum_sum}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the cumulative sum of items along the last ndim dimensions.</code></pre>

### `kd.math.divide(x, y)` {#kd.math.divide}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x / y.</code></pre>

### `kd.math.exp(x)` {#kd.math.exp}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise exponential of the input.</code></pre>

### `kd.math.floor(x)` {#kd.math.floor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise floor of the input, e.g.

rounding down: returns the largest integer value that is not greater than the
input.</code></pre>

### `kd.math.floordiv(x, y)` {#kd.math.floordiv}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x // y.</code></pre>

### `kd.math.inverse_cdf(x, cdf_arg)` {#kd.math.inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value with CDF (in [0, 1]) approximately equal to the input.

The return value is computed over all dimensions. It will have an offset of
floor((cdf - 1e-6) * size()) in the (ascendingly) sorted array.

Args:
  x: a DataSlice of numbers.
  cdf_arg: (float) CDF value.</code></pre>

### `kd.math.is_nan(x)` {#kd.math.is_nan}
Aliases:

- [kd.is_nan](#kd.is_nan)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns pointwise `kd.present|missing` if the input is NaN or not.</code></pre>

### `kd.math.log(x)` {#kd.math.log}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise natural logarithm of the input.</code></pre>

### `kd.math.log10(x)` {#kd.math.log10}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise logarithm in base 10 of the input.</code></pre>

### `kd.math.max(x)` {#kd.math.max}
Aliases:

- [kd.max](#kd.max)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the maximum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.maximum(x, y)` {#kd.math.maximum}
Aliases:

- [kd.maximum](#kd.maximum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise max(x, y).</code></pre>

### `kd.math.mean(x)` {#kd.math.mean}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the mean of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.median(x)` {#kd.math.median}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the median of elements over all dimensions.

The result is a zero-dimensional DataItem.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.min(x)` {#kd.math.min}
Aliases:

- [kd.min](#kd.min)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the minimum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.minimum(x, y)` {#kd.math.minimum}
Aliases:

- [kd.minimum](#kd.minimum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise min(x, y).</code></pre>

### `kd.math.mod(x, y)` {#kd.math.mod}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x % y.</code></pre>

### `kd.math.multiply(x, y)` {#kd.math.multiply}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x * y.</code></pre>

### `kd.math.neg(x)` {#kd.math.neg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise negation of the input, i.e. -x.</code></pre>

### `kd.math.pos(x)` {#kd.math.pos}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise positive of the input, i.e. +x.</code></pre>

### `kd.math.pow(x, y)` {#kd.math.pow}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x ** y.</code></pre>

### `kd.math.round(x)` {#kd.math.round}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise rounding of the input.

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
round(-2.5) == -3.0 # not -2.0</code></pre>

### `kd.math.sigmoid(x, half=0.0, slope=1.0)` {#kd.math.sigmoid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes sigmoid of the input.

sigmoid(x) = 1 / (1 + exp(-slope * (x - half)))

Args:
  x: A DataSlice of numbers.
  half: A DataSlice of numbers.
  slope: A DataSlice of numbers.

Return:
  sigmoid(x) computed with the formula above.</code></pre>

### `kd.math.sign(x)` {#kd.math.sign}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes the sign of the input.

Args:
  x: A DataSlice of numbers.

Returns:
  A dataslice of with {-1, 0, 1} of the same shape and type as the input.</code></pre>

### `kd.math.softmax(x, beta=1.0, ndim=unspecified)` {#kd.math.softmax}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the softmax of x alon the last ndim dimensions.

The softmax represents Exp(x * beta) / Sum(Exp(x * beta)) over last ndim
dimensions of x.

Args:
  x: An array of numbers.
  beta: A floating point scalar number that controls the smooth of the
    softmax.
  ndim: The number of last dimensions to compute softmax over.</code></pre>

### `kd.math.sqrt(x)` {#kd.math.sqrt}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise sqrt of the input.</code></pre>

### `kd.math.subtract(x, y)` {#kd.math.subtract}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Computes pointwise x - y.</code></pre>

### `kd.math.sum(x)` {#kd.math.sum}
Aliases:

- [kd.sum](#kd.sum)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the sum of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.math.t_distribution_inverse_cdf(x, degrees_of_freedom)` {#kd.math.t_distribution_inverse_cdf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Student&#39;s t-distribution inverse CDF.

Args:
  x: A DataSlice of numbers.
  degrees_of_freedom: A DataSlice of numbers.

Return:
  t_distribution_inverse_cdf(x).</code></pre>

</section>

### kd.objs {#kd.objs}

Operators that work solely with objects.

<section class="zippy closed">

**Operators**

### `kd.objs.like(shape_and_mask_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.objs.like}
Aliases:

- [kd.obj_like](#kd.obj_like)

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

- [kd.obj](#kd.obj)

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

- [kd.obj_shaped](#kd.obj_shaped)

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

- [kd.obj_shaped_as](#kd.obj_shaped_as)

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

- [kd.uuobj](#kd.uuobj)

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

</section>

### kd.optools {#kd.optools}

Operator definition and registration tooling.

<section class="zippy closed">

**Operators**

### `kd.optools.add_alias(name: str, alias: str)` {#kd.optools.add_alias}
*No description*

### `kd.optools.add_to_registry(name: str | None = None, *, aliases: Collection[str] = (), unsafe_override: bool = False, view: type[ExprView] | None = <class 'koladata.expr.view.KodaView'>, repr_fn: Union[Callable[[Expr, NodeTokenView], ReprToken], None] = None)` {#kd.optools.add_to_registry}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wrapper around Arolla&#39;s add_to_registry with Koda functionality.

Args:
  name: Optional name of the operator. Otherwise, inferred from the op.
  aliases: Optional aliases for the operator.
  unsafe_override: Whether to override an existing operator.
  view: Optional view to use for the operator. If None, the default arolla
    ExprView will be used.
  repr_fn: Optional repr function to use for the operator and its aliases. In
    case of None, a default repr function will be used.

Returns:
  Registered operator.</code></pre>

### `kd.optools.add_to_registry_as_overload(name: str | None = None, *, overload_condition_expr: Any, unsafe_override: bool = False)` {#kd.optools.add_to_registry_as_overload}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda wrapper around Arolla&#39;s add_to_registry_as_overload.

Note that for e.g. `name = &#34;foo.bar.baz&#34;`, the wrapped operator will
be registered as an overload `&#34;baz&#34;` of the overloadable operator `&#34;foo.bar&#34;`.

Performs no additional Koda-specific registration.

Args:
  name: Optional name of the operator. Otherwise, inferred from the op.
  overload_condition_expr: Condition for the overload.
  unsafe_override: Whether to override an existing operator.

Returns:
  A decorator that registers an overload for the operator with the
  corresponding name. Returns the original operator (unlinke the arolla
  equivalent).</code></pre>

### `kd.optools.add_to_registry_as_overloadable(name: str, *, unsafe_override: bool = False, view: type[ExprView] | None = <class 'koladata.expr.view.KodaView'>, repr_fn: Union[Callable[[Expr, NodeTokenView], ReprToken], None] = None, aux_policy: str = 'koladata_default_boxing')` {#kd.optools.add_to_registry_as_overloadable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Koda wrapper around Arolla&#39;s add_to_registry_as_overloadable.

Performs additional Koda-specific registration, such as setting the view and
repr function.

Args:
  name: Name of the operator.
  unsafe_override: Whether to override an existing operator.
  view: Optional view to use for the operator.
  repr_fn: Optional repr function to use for the operator and its aliases. In
    case of None, a default repr function will be used.
  aux_policy: Aux policy for the operator.

Returns:
  An overloadable registered operator.</code></pre>

### `kd.optools.as_backend_operator(name: str, *, qtype_inference_expr: Expr | QType = DATA_SLICE, qtype_constraints: Iterable[tuple[Expr, str]] = (), deterministic: bool = True, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None) -> Callable[[function], BackendOperator]` {#kd.optools.as_backend_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator for Koladata backend operators with a unified binding policy.

Args:
  name: The name of the operator.
  qtype_inference_expr: Expression that computes operator&#39;s output type.
    Argument types can be referenced using `arolla.P.arg_name`.
  qtype_constraints: List of `(predicate_expr, error_message)` pairs.
    `predicate_expr` may refer to the argument QType using
    `arolla.P.arg_name`. If a type constraint is not fulfilled, the
    corresponding `error_message` is used. Placeholders, like `{arg_name}`,
    get replaced with the actual type names during the error message
    formatting.
  deterministic: If set to False, a hidden parameter (with the name
    `optools.UNIFIED_NON_DETERMINISTIC_PARAM_NAME`) is added to the end of the
    signature. This parameter receives special handling by the binding policy
    implementation.
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).

Returns:
  A decorator that constructs a backend operator based on the provided Python
  function signature.</code></pre>

### `kd.optools.as_lambda_operator(name: str, *, qtype_constraints: Iterable[tuple[Expr, str]] = (), deterministic: bool | None = None, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None, suppress_unused_parameter_warning: bool = False) -> Callable[[function], LambdaOperator | RestrictedLambdaOperator]` {#kd.optools.as_lambda_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator for Koladata lambda operators with a unified binding policy.

Args:
  name: The name of the operator.
  qtype_constraints: List of `(predicate_expr, error_message)` pairs.
    `predicate_expr` may refer to the argument QType using
    `arolla.P.arg_name`. If a type constraint is not fulfilled, the
    corresponding `error_message` is used. Placeholders, like `{arg_name}`,
    get replaced with the actual type names during the error message
    formatting.
  deterministic: If True, the resulting operator will be deterministic and may
    only use deterministic operators. If False, the operator will be declared
    non-deterministic. By default, the decorator attempts to detect the
    operator&#39;s determinism.
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
  suppress_unused_parameter_warning: If True, unused parameters will not cause
    a warning.

Returns:
  A decorator that constructs a lambda operator by tracing a Python function.</code></pre>

### `kd.optools.as_py_function_operator(name: str, *, qtype_inference_expr: Expr | QType = DATA_SLICE, qtype_constraints: Iterable[tuple[Expr, str]] = (), codec: bytes | None = None, deterministic: bool = True, custom_boxing_fn_name_per_parameter: dict[str, str] | None = None) -> Callable[[function], Operator]` {#kd.optools.as_py_function_operator}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a decorator for defining Koladata-specific py-function operators.

The decorated function should accept QValues as input and returns a single
QValue. Variadic positional and keyword arguments are passed as tuples and
dictionaries of QValues, respectively.

Importantly, it is recommended that the function on which the operator is
based be pure -- that is, deterministic and without side effects.
If the function is not pure, please specify deterministic=False.

Args:
  name: The name of the operator.
  qtype_inference_expr: expression that computes operator&#39;s output qtype; an
    argument qtype can be referenced as P.arg_name.
  qtype_constraints: QType constraints for the operator.
  codec: A PyObject serialization codec for the wrapped function, compatible
    with `arolla.types.encode_py_object`. The resulting operator is
    serializable only if the codec is specified.
  deterministic: Set this to `False` if the wrapped function is not pure
    (i.e., non-deterministic or has side effects).
  custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
    function per parameter (constants with the boxing functions look like:
    `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).</code></pre>

### `kd.optools.as_qvalue(arg: Any) -> QValue` {#kd.optools.as_qvalue}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into QValues.</code></pre>

### `kd.optools.as_qvalue_or_expr(arg: Any) -> Expr | QValue` {#kd.optools.as_qvalue_or_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts Python values into QValues or Exprs.</code></pre>

### `kd.optools.equiv_to_op(this_op: Operator | str, that_op: Operator | str) -> bool` {#kd.optools.equiv_to_op}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff the impl of `this_op` equals the impl of `that_op`.</code></pre>

### `kd.optools.make_operators_container(*namespaces: str) -> OperatorsContainer` {#kd.optools.make_operators_container}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an OperatorsContainer for the given namespaces.

Note that the returned container accesses the global namespace. A common
pattern is therefore:
  foo = make_operators_container(&#39;foo&#39;, &#39;foo.bar&#39;, &#39;foo.baz&#39;).foo

Args:
  *namespaces: Namespaces to make available in the returned container.</code></pre>

### `kd.optools.unified_non_deterministic_arg() -> Expr` {#kd.optools.unified_non_deterministic_arg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a non-deterministic token for use with `bind_op(..., arg)`.</code></pre>

### `kd.optools.unified_non_deterministic_kwarg() -> dict[str, Expr]` {#kd.optools.unified_non_deterministic_kwarg}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a non-deterministic token for use with `bind_op(..., **kwarg)`.</code></pre>

</section>

### kd.proto {#kd.proto}

Protocol buffer serialization operators.

<section class="zippy closed">

**Operators**

### `kd.proto.from_proto_bytes(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.proto.from_proto_bytes}
Aliases:

- [kd.from_proto_bytes](#kd.from_proto_bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of binary proto messages.

This is equivalent to parsing `x.to_py()` as a binary proto message in Python,
and then converting the parsed message to a DataSlice using `kd.from_proto`,
but bypasses Python, is traceable, and supports any shape and sparsity, and
can handle parse errors.

`x` must be a DataSlice of BYTES. Missing elements of `x` will be missing in
the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
and `schema` arguments.

If `on_invalid` is unset, this operator will throw an error if any input
fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
will be used in place of the result wherever the input fails to parse.

Args:
  x: DataSlice of BYTES
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING
  itemids: DataSlice of ITEMID with the same shape as `x` (optional)
  schema: DataItem containing SCHEMA (optional)
  on_invalid: DataSlice broacastable to the result (optional)

Returns:
  A DataSlice representing the proto data.</code></pre>

### `kd.proto.from_proto_json(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.proto.from_proto_json}
Aliases:

- [kd.from_proto_json](#kd.from_proto_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Parses a DataSlice `x` of proto JSON-format strings.

This is equivalent to parsing `x.to_py()` as a JSON-format proto message in
Python, and then converting the parsed message to a DataSlice using
`kd.from_proto`, but bypasses Python, is traceable, supports any shape and
sparsity, and can handle parse errors.

`x` must be a DataSlice of STRING. Missing elements of `x` will be missing in
the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
and `schema` arguments.

If `on_invalid` is unset, this operator will throw an error if any input
fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
will be used in place of the result wherever the input fails to parse.

Args:
  x: DataSlice of STRING
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING
  itemids: DataSlice of ITEMID with the same shape as `x` (optional)
  schema: DataItem containing SCHEMA (optional)
  on_invalid: DataSlice broacastable to the result (optional)

Returns:
  A DataSlice representing the proto data.</code></pre>

### `kd.proto.get_proto_attr(x, field_name)` {#kd.proto.get_proto_attr}
Aliases:

- [kd.get_proto_attr](#kd.get_proto_attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a field value on proto message DataSlice `x`.

This is nearly the same as `kd.get_attr(x, field_name)`, but has two changes
that make working with proto fields easier:
1. Missing primitive values are replaced with proto field default values if
  a custom default value is configured for that field.
2. BOOLEAN values are simplified to MASK using `== kd.bool(True)` (after
  applying default values).

This generally expects `x` to have a schema derived from a proto (i.e. using
`kd.from_proto` or `kd.schema_from_proto`), but it will also work on other
Koda objects, treating entities like messages with no custom field defaults.
It will not work on primitives.

Args:
  x: DataSlice
  field_name: DataItem containing STRING

Returns:
  A DataSlice</code></pre>

### `kd.proto.get_proto_field_custom_default(x, field_name)` {#kd.proto.get_proto_field_custom_default}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the default value on proto message or schema DataSlice `x`.

The result will have the same shape as `x`, because the schemas in `x` could
vary per item.

When a proto message is converted to a Koda DataSlice using `kd.from_proto` or
`kd.schema_from_proto`, any custom default values on message fields are
recorded in the schema metadata. This operator allows us to access that
metadata more easily.

If `x` was not converted from a proto message, or no custom field default
value was defined for this field, returns None.

Args:
  x: DataSlice
  field_name: DataItem containing STRING

Returns:
  A DataSlice</code></pre>

### `kd.proto.get_proto_full_name(x)` {#kd.proto.get_proto_full_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the proto full name of a proto message or schema DataSlice `x`.

The result will have the same shape as `x`, because the schemas in `x` could
vary per item.

When a proto message is converted to a Koda DataSlice using `kd.from_proto` or
`kd.schema_from_proto`, its full name is recorded in the schema metadata. This
operator allows us to access that metadata more easily.

The proto full name of non-entity schemas and any entity schemas not
converted from protos is None.

Args:
  x: DataSlice

Returns:
  A STRING DataSlice</code></pre>

### `kd.proto.schema_from_proto_path(proto_path, /, *, extensions=DataItem(Entity:#5ikYYvXepp19g47QDLnJR2, schema: ITEMID))` {#kd.proto.schema_from_proto_path}
Aliases:

- [kd.schema_from_proto_path](#kd.schema_from_proto_path)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda schema representing a proto message class.

This is equivalent to `kd.schema_from_proto(message_cls)` if `message_cls` is
the Python proto class with full name `proto_path`, but bypasses Python and
is traceable.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

See `kd.schema_from_proto` for a detailed explanation of the `extensions`
argument.

Args:
  proto_path: DataItem containing STRING
  extensions: 1D DataSlice of STRING</code></pre>

### `kd.proto.to_proto_bytes(x, proto_path, /)` {#kd.proto.to_proto_bytes}
Aliases:

- [kd.to_proto_bytes](#kd.to_proto_bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes a DataSlice `x` as binary proto messages.

This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
in Python, then serializing that message into a binary proto, but bypasses
Python, is traceable, and supports any shape and sparsity.

`x` must be serializable as the proto message with full name `proto_path`.
Missing elements of `x` will be missing in the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

Args:
  x: DataSlice
  proto_path: DataItem containing STRING

Returns:
  A DataSlice of BYTES with the same shape and sparsity as `x`.</code></pre>

### `kd.proto.to_proto_json(x, proto_path, /)` {#kd.proto.to_proto_json}
Aliases:

- [kd.to_proto_json](#kd.to_proto_json)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes a DataSlice `x` as JSON-format proto messages.

This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
in Python, then serializing that message into a JSON-format proto, but
bypasses Python, is traceable, and supports any shape and sparsity.

`x` must be serializable as the proto message with full name `proto_path`.
Missing elements of `x` will be missing in the result.

`proto_path` must be a DataItem containing a STRING fully-qualified proto
message name, which will be used to look up the message descriptor in the C++
generated descriptor pool. For this to work, the C++ proto message needs to
be compiled into the binary that executes this operator, which is not the
same as the proto message being available in Python.

Args:
  x: DataSlice
  proto_path: DataItem containing STRING

Returns:
  A DataSlice of STRING with the same shape and sparsity as `x`.</code></pre>

</section>

### kd.parallel {#kd.parallel}

Operators for parallel computation.

<section class="zippy closed">

**Operators**

### `kd.parallel.call_multithreaded(fn: DataItem, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, max_threads: int | None = None, timeout: float | None = None, **kwargs: Any) -> Any` {#kd.parallel.call_multithreaded}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor with the given arguments.

Variables of the functor or of its sub-functors will be computed in parallel
when they don&#39;t depend on each other. If the internal computation involves
iterables, the corresponding computations will be done in a streaming fashion.

Note that you should not use this function inside another functor (via py_fn),
as it will block the thread executing it, which can lead to deadlock when we
don&#39;t have enough threads in the thread pool. Instead, please compose all
functors first into one functor and then use one call to call_multithreaded to
execute them all in parallel.

Args:
  fn: The functor to call.
  *args: The positional arguments to pass to the functor.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  max_threads: The maximum number of threads to use. None means to use the
    default executor.
  timeout: The maximum time to wait for the call to finish. None means to wait
    indefinitely.
  **kwargs: The keyword arguments to pass to the functor.

Returns:
  The result of the call. Iterables and tuples/namedtuples of iterables are
  not yet supported for the result, since that would mean that the result
  is/has a stream, and this method needs to return multiple values at
  different times instead of one value at the end.</code></pre>

### `kd.parallel.transform(fn: DataItem | function | partial[Any], *, allow_runtime_transforms: bool = False) -> DataItem` {#kd.parallel.transform}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Transforms a functor to run in parallel.

The resulting functor will take and return parallel versions of the arguments
and return values of `fn`. Currently there is no public API to create a
parallel version (DataSlice -&gt; future[DataSlice]), this is work in progress.

Args:
  fn: The functor to transform.
  allow_runtime_transforms: Whether to allow sub-functors to be not literals,
    but computed expressions, which will therefore have to be transformed at
    runtime. This can be slow.

Returns:
  The transformed functor.</code></pre>

### `kd.parallel.yield_multithreaded(fn: DataItem, /, *args: Any, value_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, max_threads: int | None = None, timeout: float | None = None, **kwargs: Any) -> Iterator[Any]` {#kd.parallel.yield_multithreaded}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor returning an iterable, and yields the results as they go.

Variables of the functor or of its sub-functors will be computed in parallel
when they don&#39;t depend on each other. If the internal computation involves
iterables, the corresponding computations will be done in a streaming fashion.
The functor must return an iterable.

Note that you should not use this function inside another functor (via py_fn),
as it will block the thread executing it, which can lead to deadlock when we
don&#39;t have enough threads in the thread pool. Instead, please compose all
functors first into one functor and then use
one call to call_multithreaded/yield_multithreaded to execute them all in
parallel.

Args:
  fn: The functor to call.
  *args: The positional arguments to pass to the functor.
  value_type_as: The return type of the call is expected to be an iterable of
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return an iterable of DataSlice. kd.types.DataSlice, kd.types.DataBag
    and kd.types.JaggedShape can also be passed here.
  max_threads: The maximum number of threads to use. None means to use the
    default executor.
  timeout: The maximum time to wait for the computation of all items of the
    output iterable to finish. None means to wait indefinitely.
  **kwargs: The keyword arguments to pass to the functor.

Returns:
  Yields the items of the output iterable as soon as they are available.</code></pre>

</section>

### kd.py {#kd.py}

Operators that call Python functions.

<section class="zippy closed">

**Operators**

### `kd.py.apply_py(fn, *args, return_type_as=unspecified, **kwargs)` {#kd.py.apply_py}
Aliases:

- [kd.apply_py](#kd.apply_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies Python function `fn` on args.

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
    auto-boxed into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: keyword arguments to pass to `fn`.

Returns:
  Result of fn applied on the arguments.</code></pre>

### `kd.py.map_py(fn, *args, schema=None, max_threads=1, ndim=0, include_missing=None, item_completed_callback=None, **kwargs)` {#kd.py.map_py}
Aliases:

- [kd.map_py](#kd.map_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply the python function `fn` on provided `args` and `kwargs`.

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
    return db.obj(x=1, y=2) if x.z &gt; 3 else db.obj(x=2, y=1)

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

It&#39;s also possible to set custom serialization for the fn (i.e. if you want to
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
  Result DataSlice.</code></pre>

### `kd.py.map_py_on_cond(true_fn, false_fn, cond, *args, schema=None, max_threads=1, item_completed_callback=None, **kwargs)` {#kd.py.map_py_on_cond}
Aliases:

- [kd.map_py_on_cond](#kd.map_py_on_cond)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply python functions on `args` and `kwargs` based on `cond`.

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
  Result DataSlice.</code></pre>

### `kd.py.map_py_on_selected(fn, cond, *args, schema=None, max_threads=1, item_completed_callback=None, **kwargs)` {#kd.py.map_py_on_selected}
Aliases:

- [kd.map_py_on_selected](#kd.map_py_on_selected)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply python function `fn` on `args` and `kwargs` based on `cond`.

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
  Result DataSlice.</code></pre>

</section>

### kd.random {#kd.random}

Random and sampling operators.

<section class="zippy closed">

**Operators**

### `kd.random.cityhash(x, seed)` {#kd.random.cityhash}
Aliases:

- [kd.cityhash](#kd.cityhash)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a hash value of &#39;x&#39; for given seed.

The hash value is generated using CityHash library. The result will have the
same shape and sparsity as `x`. The output values are INT64.

Args:
  x: DataSlice for hash.
  seed: seed for hash, must be a scalar.

Returns:
  The hash values as INT64 DataSlice.</code></pre>

### `kd.random.mask(x, ratio, seed, key=unspecified)` {#kd.random.mask}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a mask with near size(x) * ratio present values at random indices.

The sampling of indices is performed on flatten `x` rather than on the last
dimension.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) &lt; ratio * 2^63
Therefore, exact sampled count is not guaranteed. E.g. result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.random.mask(ds, 0.5, 123)
    -&gt; kd.slice([
           [None, None, kd.present, None],
           [kd.present, None, None, kd.present]
       ])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.random.mask(ds_1, 0.5, 123, key_1)
    -&gt; kd.slice([
           [None, None, None, kd.present],
           [None, None, None, kd.present],
       ])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;d&#39;, &#39;b&#39;, &#39;a&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.random.mask(ds_2, 0.5, 123, key_2)
    -&gt; kd.slice([
           [None, kd.present, None, None],
           [None, None, None, kd.present],
       ])

Args:
  x: DataSlice whose shape is used for sampling.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.</code></pre>

### `kd.random.randint_like(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_like}
Aliases:

- [kd.randint_like](#kd.randint_like)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the same sparsity as `x`.

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
  A DataSlice of random numbers.</code></pre>

### `kd.random.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped}
Aliases:

- [kd.randint_shaped](#kd.randint_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the given shape.

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
  A DataSlice of random numbers.</code></pre>

### `kd.random.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.random.randint_shaped_as}
Aliases:

- [kd.randint_shaped_as](#kd.randint_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of random INT64 numbers with the same shape as `x`.

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
  A DataSlice of random numbers.</code></pre>

### `kd.random.sample(x, ratio, seed, key=unspecified)` {#kd.random.sample}
Aliases:

- [kd.sample](#kd.sample)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly sample items in `x` based on ratio.

The sampling is performed on flatten `x` rather than on the last dimension.

All items including missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) &lt; ratio * 2^63
Therefore, exact sampled count is not guaranteed. E.g. result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample(ds, 0.5, 123) -&gt; kd.slice([[None, 4], [None, 8]])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample(ds_1, 0.5, 123, key_1) -&gt; kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;a&#39;, &#39;b&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample(ds_2, 0.5, 123, key_2) -&gt; kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.</code></pre>

### `kd.random.sample_n(x, n, seed, key=unspecified)` {#kd.random.sample_n}
Aliases:

- [kd.sample_n](#kd.sample_n)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly sample n items in `x` from the last dimension.

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
  kd.sample_n(ds, 2, 123) -&gt; kd.slice([[2, 4], [None, 8]])

  # Select 1 item from the first and 2 items from the second.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, [1, 2], 123) -&gt; kd.slice([[4], [None, 5]])

  # Use &#39;key&#39; for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([[&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample_n(ds_1, 2, 123, key_1) -&gt; kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([[&#39;c&#39;, &#39;a&#39;, &#39;b&#39;, &#39;d&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;, &#39;d&#39;]])
  kd.sample_n(ds_2, 2, 123, key_2) -&gt; kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  n: number of items to sample. Either an integer or a DataSlice.
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.</code></pre>

### `kd.random.shuffle(x, /, ndim=unspecified, seed=unspecified)` {#kd.random.shuffle}
Aliases:

- [kd.shuffle](#kd.shuffle)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Randomly shuffles a DataSlice along a single dimension (last by default).

If `ndim` is not specified, items are shuffled in the last dimension.
If `ndim` is specified, then the dimension `ndim` from the last is shuffled,
equivalent to `kd.explode(kd.shuffle(kd.implode(x, ndim)), ndim)`.

When `seed` is not specified, the results are different across multiple
invocations given the same input.

For example:

  kd.shuffle(kd.slice([[1, 2, 3], [4, 5], [6]]))
  -&gt; kd.slice([[3, 1, 2], [5, 4], [6]]) (possible output)

  kd.shuffle(kd.slice([[1, 2, 3], [4, 5]]), ndim=1)
  -&gt; kd.slice([[4, 5], [6], [1, 2, 3]]) (possible output)

Args:
  x: DataSlice to shuffle.
  ndim: The index of the dimension to shuffle, from the end (0 = last dim).
    The last dimension is shuffled if this is unspecified.
  seed: Seed for the random number generator. The same input with the same
    seed generates the same random numbers.

Returns:
  Shuffled DataSlice.</code></pre>

</section>

### kd.schema {#kd.schema}

Schema-related operators.

<section class="zippy closed">

**Operators**

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

- [kd.cast_to](#kd.cast_to)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema` using explicit casting rules.

Dispatches to the relevant `kd.to_...` operator. Performs permissive casting,
e.g. allowing FLOAT32 -&gt; INT32 casting through `kd.cast_to(slice, INT32)`.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.</code></pre>

### `kd.schema.cast_to_implicit(x, schema)` {#kd.schema.cast_to_implicit}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema` using implicit casting rules.

Note that `schema` must be the common schema of `schema` and `x.get_schema()`
according to go/koda-type-promotion.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.</code></pre>

### `kd.schema.cast_to_narrow(x, schema)` {#kd.schema.cast_to_narrow}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` casted to the provided `schema`.

Allows for schema narrowing, where OBJECT types can be casted to primitive
schemas as long as the data is implicitly castable to the schema. Follows the
casting rules of `kd.cast_to_implicit` for the narrowed schema.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.</code></pre>

### `kd.schema.common_schema(x)` {#kd.schema.common_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the common schema as a scalar DataItem of `x`.

The &#34;common schema&#34; is defined according to go/koda-type-promotion.

Args:
  x: DataSlice of schemas.</code></pre>

### `kd.schema.dict_schema(key_schema, value_schema)` {#kd.schema.dict_schema}
Aliases:

- [kd.dict_schema](#kd.dict_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Dict schema with the provided `key_schema` and `value_schema`.</code></pre>

### `kd.schema.get_dtype(ds)` {#kd.schema.get_dtype}
Aliases:

- [kd.schema.get_primitive_schema](#kd.schema.get_primitive_schema)

- [kd.get_dtype](#kd.get_dtype)

- [kd.get_primitive_schema](#kd.get_primitive_schema)

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

- [kd.get_item_schema](#kd.get_item_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the item schema of a List schema`.</code></pre>

### `kd.schema.get_itemid(x)` {#kd.schema.get_itemid}
Aliases:

- [kd.schema.to_itemid](#kd.schema.to_itemid)

- [kd.get_itemid](#kd.get_itemid)

- [kd.to_itemid](#kd.to_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to ITEMID using explicit (permissive) casting rules.</code></pre>

### `kd.schema.get_key_schema(dict_schema)` {#kd.schema.get_key_schema}
Aliases:

- [kd.get_key_schema](#kd.get_key_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the key schema of a Dict schema`.</code></pre>

### `kd.schema.get_nofollowed_schema(schema)` {#kd.schema.get_nofollowed_schema}
Aliases:

- [kd.get_nofollowed_schema](#kd.get_nofollowed_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original schema from nofollow schema.

Requires `nofollow_schema` to be a nofollow schema, i.e. that it wraps some
other schema.

Args:
  schema: nofollow schema DataSlice.</code></pre>

### `kd.schema.get_obj_schema(x)` {#kd.schema.get_obj_schema}
Aliases:

- [kd.get_obj_schema](#kd.get_obj_schema)

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

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype) operator.

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

- [kd.get_schema](#kd.get_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of `x`.</code></pre>

### `kd.schema.get_value_schema(dict_schema)` {#kd.schema.get_value_schema}
Aliases:

- [kd.get_value_schema](#kd.get_value_schema)

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

- [kd.list_schema](#kd.list_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a List schema with the provided `item_schema`.</code></pre>

### `kd.schema.named_schema(name, /, **kwargs)` {#kd.schema.named_schema}
Aliases:

- [kd.named_schema](#kd.named_schema)

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

- [kd.nofollow_schema](#kd.nofollow_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a NoFollow schema of the provided schema.

`nofollow_schema` is reversible with `get_actual_schema`.

`nofollow_schema` can only be called on implicit and explicit schemas and
OBJECT. It raises an Error if called on primitive schemas, ITEMID, etc.

Args:
  schema: Schema DataSlice to wrap.</code></pre>

### `kd.schema.schema_from_py(tpe: type[Any]) -> SchemaItem` {#kd.schema.schema_from_py}
Aliases:

- [kd.schema_from_py](#kd.schema_from_py)

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

- [kd.to_expr](#kd.to_expr)

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

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid) operator.

### `kd.schema.to_mask(x)` {#kd.schema.to_mask}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to MASK using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_none(x)` {#kd.schema.to_none}
Aliases:

- [kd.to_none](#kd.to_none)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to NONE using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_object(x)` {#kd.schema.to_object}
Aliases:

- [kd.to_object](#kd.to_object)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to OBJECT using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_schema(x)` {#kd.schema.to_schema}
Aliases:

- [kd.to_schema](#kd.to_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to SCHEMA using explicit (permissive) casting rules.</code></pre>

### `kd.schema.to_str(x)` {#kd.schema.to_str}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to STRING using explicit (permissive) casting rules.</code></pre>

### `kd.schema.uu_schema(seed='', **kwargs)` {#kd.schema.uu_schema}
Aliases:

- [kd.uu_schema](#kd.uu_schema)

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

- [kd.with_schema](#kd.with_schema)

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

- [kd.with_schema_from_obj](#kd.with_schema_from_obj)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.</code></pre>

</section>

### kd.shapes {#kd.shapes}

Operators that work on shapes

<section class="zippy closed">

**Operators**

### `kd.shapes.dim_mapping(shape, dim)` {#kd.shapes.dim_mapping}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the parent-to-child mapping of the dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [3, 2], [1, 2, 0, 2, 1])
  kd.shapes.dim_mapping(shape, 0) # -&gt; kd.slice([0, 0])
  kd.shapes.dim_mapping(shape, 1) # -&gt; kd.slice([0, 0, 0, 1, 1])
  kd.shapes.dim_mapping(shape, 2) # -&gt; kd.slice([0, 1, 1, 3, 3, 4])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the parent-to-child mapping for.</code></pre>

### `kd.shapes.dim_sizes(shape, dim)` {#kd.shapes.dim_sizes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the row sizes at the provided dimension in the given shape.

Example:
  shape = kd.shapes.new([2], [2, 1])
  kd.shapes.dim_sizes(shape, 0)  # -&gt; kd.slice([2])
  kd.shapes.dim_sizes(shape, 1)  # -&gt; kd.slice([2, 1])

Args:
  shape: a JaggedShape.
  dim: the dimension to get the sizes for.</code></pre>

### `kd.shapes.expand_to_shape(x, shape, ndim=unspecified)` {#kd.shapes.expand_to_shape}
Aliases:

- [kd.expand_to_shape](#kd.expand_to_shape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands `x` based on the provided `shape`.

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
  Expanded DataSlice</code></pre>

### `kd.shapes.flatten(x, from_dim=0, to_dim=unspecified)` {#kd.shapes.flatten}
Aliases:

- [kd.flatten](#kd.flatten)

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

### `kd.shapes.flatten_end(x, n_times=1)` {#kd.shapes.flatten_end}
Aliases:

- [kd.flatten_end](#kd.flatten_end)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with a shape flattened `n_times` from the end.

The new shape has x.get_ndim() - n_times dimensions.

Given that flattening happens from the end, only positive integers are
allowed. For more control over flattening, please use `kd.flatten`, instead.

Args:
  x: a DataSlice.
  n_times: number of dimensions to flatten from the end
    (0 &lt;= n_times &lt;= rank).</code></pre>

### `kd.shapes.get_shape(x)` {#kd.shapes.get_shape}
Aliases:

- [kd.get_shape](#kd.get_shape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the shape of `x`.</code></pre>

### `kd.shapes.get_sizes(x)` {#kd.shapes.get_sizes}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of sizes of a given shape.

Example:
  kd.shapes.get_sizes(kd.shapes.new([2], [2, 1])) -&gt; kd.slice([[2], [2, 1]])
  kd.shapes.get_sizes(kd.slice([[&#39;a&#39;, &#39;b&#39;], [&#39;c&#39;]])) -&gt; kd.slice([[2], [2,
  1]])

Args:
  x: a shape or a DataSlice from which the shape will be taken.

Returns:
  A 2-dimensional DataSlice where the first dimension&#39;s size corresponds to
  the shape&#39;s rank and the n-th subslice corresponds to the sizes of the n-th
  dimension of the original shape.</code></pre>

### `kd.shapes.is_expandable_to_shape(x, target_shape, ndim=unspecified)` {#kd.shapes.is_expandable_to_shape}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true if `x` is expandable to `target_shape`.

See `expand_to_shape` for a detailed description of expansion.

Args:
  x: DataSlice that would be expanded.
  target_shape: JaggedShape that would be expanded to.
  ndim: The number of dimensions to implode before expansion. If unset,
    defaults to 0.</code></pre>

### `kd.shapes.ndim(shape)` {#kd.shapes.ndim}
Aliases:

- [kd.shapes.rank](#kd.shapes.rank)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the rank of the jagged shape.</code></pre>

### `kd.shapes.new(*dimensions)` {#kd.shapes.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a JaggedShape from the provided dimensions.

Example:
  # Creates a scalar shape (i.e. no dimension).
  kd.shapes.new()  # -&gt; JaggedShape()

  # Creates a 3-dimensional shape with all uniform dimensions.
  kd.shapes.new(2, 3, 1)  # -&gt; JaggedShape(2, 3, 1)

  # Creates a 3-dimensional shape with 2 sub-values in the first dimension.
  #
  # The second dimension is jagged with 2 values. The first value in the
  # second dimension has 2 sub-values, and the second value has 1 sub-value.
  #
  # The third dimension is jagged with 3 values. The first value in the third
  # dimension has 1 sub-value, the second has 2 sub-values, and the third has
  # 3 sub-values.
  kd.shapes.new(2, [2, 1], [1, 2, 3])
      # -&gt; JaggedShape(2, [2, 1], [1, 2, 3])

Args:
  *dimensions: A combination of Edges and DataSlices representing the
    dimensions of the JaggedShape. Edges are used as is, while DataSlices are
    treated as sizes. DataItems (of ints) are interpreted as uniform
    dimensions which have the same child size for all parent elements.
    DataSlices (of ints) are interpreted as a list of sizes, where `ds[i]` is
    the child size of parent `i`. Only rank-0 or rank-1 int DataSlices are
    supported.</code></pre>

### `kd.shapes.rank(shape)` {#kd.shapes.rank}

Alias for [kd.shapes.ndim](#kd.shapes.ndim) operator.

### `kd.shapes.reshape(x, shape)` {#kd.shapes.reshape}
Aliases:

- [kd.reshape](#kd.reshape)

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

### `kd.shapes.reshape_as(x, shape_from)` {#kd.shapes.reshape_as}
Aliases:

- [kd.reshape_as](#kd.reshape_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice x reshaped to the shape of DataSlice shape_from.</code></pre>

### `kd.shapes.size(shape)` {#kd.shapes.size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the total number of elements the jagged shape represents.</code></pre>

</section>

### kd.slices {#kd.slices}

Operators that perform DataSlice transformations.

<section class="zippy closed">

**Operators**

### `kd.slices.agg_count(x, ndim=unspecified)` {#kd.slices.agg_count}
Aliases:

- [kd.agg_count](#kd.agg_count)

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

- [kd.agg_size](#kd.agg_size)

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

- [kd.align](#kd.align)

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

- [kd.at](#kd.at)

- [kd.take](#kd.take)

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

- [kd.bool](#kd.bool)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.BOOLEAN).</code></pre>

### `kd.slices.bytes(x: Any) -> DataSlice` {#kd.slices.bytes}
Aliases:

- [kd.bytes](#kd.bytes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.BYTES).</code></pre>

### `kd.slices.collapse(x, ndim=unspecified)` {#kd.slices.collapse}
Aliases:

- [kd.collapse](#kd.collapse)

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

- [kd.concat](#kd.concat)

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

- [kd.count](#kd.count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `kd.slices.cum_count(x, ndim=unspecified)` {#kd.slices.cum_count}
Aliases:

- [kd.cum_count](#kd.cum_count)

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

- [kd.dense_rank](#kd.dense_rank)

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

- [kd.empty_shaped](#kd.empty_shaped)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of missing items with the given shape.

If `schema` is a Struct schema, an empty Databag is created and attached to
the resulting DataSlice and `schema` is adopted into that DataBag.

Args:
  shape: Shape of the resulting DataSlice.
  schema: optional schema of the resulting DataSlice.</code></pre>

### `kd.slices.empty_shaped_as(shape_from, schema=MASK)` {#kd.slices.empty_shaped_as}
Aliases:

- [kd.empty_shaped_as](#kd.empty_shaped_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of missing items with the shape of `shape_from`.

If `schema` is a Struct schema, an empty Databag is created and attached to
the resulting DataSlice and `schema` is adopted into that DataBag.

Args:
  shape_from: used for the shape of the resulting DataSlice.
  schema: optional schema of the resulting DataSlice.</code></pre>

### `kd.slices.expand_to(x, target, ndim=unspecified)` {#kd.slices.expand_to}
Aliases:

- [kd.expand_to](#kd.expand_to)

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

- [kd.expr_quote](#kd.expr_quote)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.EXPR).</code></pre>

### `kd.slices.float32(x: Any) -> DataSlice` {#kd.slices.float32}
Aliases:

- [kd.float32](#kd.float32)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.FLOAT32).</code></pre>

### `kd.slices.float64(x: Any) -> DataSlice` {#kd.slices.float64}
Aliases:

- [kd.float64](#kd.float64)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.FLOAT64).</code></pre>

### `kd.slices.get_ndim(x)` {#kd.slices.get_ndim}
Aliases:

- [kd.get_ndim](#kd.get_ndim)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of dimensions of DataSlice `x`.</code></pre>

### `kd.slices.get_repr(x, /, *, depth=25, item_limit=200, item_limit_per_dimension=25, format_html=False, max_str_len=100, max_expr_quote_len=10000, show_attributes=True, show_databag_id=False, show_shape=False, show_schema=False, show_item_id=False)` {#kd.slices.get_repr}
Aliases:

- [kd.get_repr](#kd.get_repr)

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

Returns:
  A string representation of the DataSlice `x`.</code></pre>

### `kd.slices.group_by(x, *keys, sort=False)` {#kd.slices.group_by}
Aliases:

- [kd.group_by](#kd.group_by)

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

- [kd.group_by_indices](#kd.group_by_indices)

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

- [kd.index](#kd.index)

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

- [kd.int32](#kd.int32)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.INT32).</code></pre>

### `kd.slices.int64(x: Any) -> DataSlice` {#kd.slices.int64}
Aliases:

- [kd.int64](#kd.int64)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.INT64).</code></pre>

### `kd.slices.internal_is_compliant_attr_name(attr_name, /)` {#kd.slices.internal_is_compliant_attr_name}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true iff `attr_name` can be accessed through `getattr(slice, attr_name)`.</code></pre>

### `kd.slices.internal_select_by_slice(ds, fltr, expand_filter=True)` {#kd.slices.internal_select_by_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A version of kd.select that does not support lambdas/functors.</code></pre>

### `kd.slices.inverse_mapping(x, ndim=unspecified)` {#kd.slices.inverse_mapping}
Aliases:

- [kd.inverse_mapping](#kd.inverse_mapping)

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

- [kd.inverse_select](#kd.inverse_select)

- [kd.reverse_select](#kd.reverse_select)

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

- [kd.is_empty](#kd.is_empty)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if all items in the DataSlice are missing.</code></pre>

### `kd.slices.is_expandable_to(x, target, ndim=unspecified)` {#kd.slices.is_expandable_to}
Aliases:

- [kd.is_expandable_to](#kd.is_expandable_to)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns true if `x` is expandable to `target`.

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode before expansion.

See `expand_to` for a detailed description of expansion.</code></pre>

### `kd.slices.is_shape_compatible(x, y)` {#kd.slices.is_shape_compatible}
Aliases:

- [kd.is_shape_compatible](#kd.is_shape_compatible)

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

- [kd.isin](#kd.isin)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataItem indicating whether DataItem x is present in y.</code></pre>

### `kd.slices.item(x, /, schema=None)` {#kd.slices.item}
Aliases:

- [kd.item](#kd.item)

- [DataItem.from_vals](#DataItem.from_vals)

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

- [kd.mask](#kd.mask)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.MASK).</code></pre>

### `kd.slices.ordinal_rank(x, tie_breaker=unspecified, descending=False, ndim=unspecified)` {#kd.slices.ordinal_rank}
Aliases:

- [kd.ordinal_rank](#kd.ordinal_rank)

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

- [kd.range](#kd.range)

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

- [kd.repeat](#kd.repeat)

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

- [kd.repeat_present](#kd.repeat_present)

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

- [kd.reverse](#kd.reverse)

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

Alias for [kd.slices.inverse_select](#kd.slices.inverse_select) operator.

### `kd.slices.select(ds, fltr, expand_filter=True)` {#kd.slices.select}
Aliases:

- [kd.select](#kd.select)

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

- [kd.select_present](#kd.select_present)

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

- [kd.size](#kd.size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.</code></pre>

### `kd.slices.slice(x, /, schema=None)` {#kd.slices.slice}
Aliases:

- [kd.slice](#kd.slice)

- [DataSlice.from_vals](#DataSlice.from_vals)

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

- [kd.sort](#kd.sort)

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

- [kd.stack](#kd.stack)

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

- [kd.str](#kd.str)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.slice(x, kd.STRING).</code></pre>

### `kd.slices.subslice(x, *slices)` {#kd.slices.subslice}
Aliases:

- [kd.subslice](#kd.subslice)

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

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.slices.tile(x, shape)` {#kd.slices.tile}
Aliases:

- [kd.tile](#kd.tile)

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

- [kd.translate](#kd.translate)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Translates `keys_to` based on `keys_from`-&gt;`values_from` mapping.

The translation is done by matching keys from `keys_from` to `keys_to` over
the last dimension of `keys_to`. `keys_from` cannot have duplicate keys within
each group of the last dimension. Also see kd.translate_group.

`values_from` is first broadcasted to `keys_from` and the first N-1 dimensions
of `keys_from` and `keys_to` must be the same. The resulting DataSlice has the
same shape as `keys_to` and the same DataBag as `values_from`.

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

- [kd.translate_group](#kd.translate_group)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Translates `keys_to` based on `keys_from`-&gt;`values_from` mapping.

The translation is done by creating an additional dimension under `keys_to`
and putting items in `values_from` to this dimension by matching keys from
`keys_from` to `keys_to` over the last dimension of `keys_to`.
`keys_to` can have duplicate keys within each group of the last
dimension.

`values_from` and `keys_from` must have the same shape and the first N-1
dimensions of `keys_from` and `keys_to` must be the same. The shape of
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

- [kd.unique](#kd.unique)

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

- [kd.val_like](#kd.val_like)

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

- [kd.val_shaped](#kd.val_shaped)

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

- [kd.val_shaped_as](#kd.val_shaped_as)

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

- [kd.zip](#kd.zip)

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

</section>

### kd.strings {#kd.strings}

Operators that work with strings data.

<section class="zippy closed">

**Operators**

### `kd.strings.agg_join(x, sep=None, ndim=unspecified)` {#kd.strings.agg_join}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of strings joined on last ndim dimensions.

Example:
  ds = kd.slice([[&#39;el&#39;, &#39;psy&#39;, &#39;congroo&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]))
  kd.agg_join(ds, &#39; &#39;)  # -&gt; kd.slice([&#39;el psy congroo&#39;, &#39;a b c&#39;])
  kd.agg_join(ds, &#39; &#39;, ndim=2)  # -&gt; kd.slice(&#39;el psy congroo a b c&#39;)

Args:
  x: String or bytes DataSlice
  sep: If specified, will join by the specified string, otherwise will be
    empty string.
  ndim: The number of dimensions to compute indices over. Requires 0 &lt;= ndim
    &lt;= get_ndim(x).</code></pre>

### `kd.strings.contains(s, substr)` {#kd.strings.contains}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff `s` contains `substr`.

Examples:
  kd.strings.constains(kd.slice([&#39;Hello&#39;, &#39;Goodbye&#39;]), &#39;lo&#39;)
    # -&gt; kd.slice([kd.present, kd.missing])
  kd.strings.contains(
    kd.slice([b&#39;Hello&#39;, b&#39;Goodbye&#39;]),
    kd.slice([b&#39;lo&#39;, b&#39;Go&#39;]))
    # -&gt; kd.slice([kd.present, kd.present])

Args:
  s: The strings to consider. Must have schema STRING or BYTES.
  substr: The substrings to look for in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of present/missing values with schema MASK.</code></pre>

### `kd.strings.count(s, substr)` {#kd.strings.count}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Counts the number of occurrences of `substr` in `s`.

Examples:
  kd.strings.count(kd.slice([&#39;Hello&#39;, &#39;Goodbye&#39;]), &#39;l&#39;)
    # -&gt; kd.slice([2, 0])
  kd.strings.count(
    kd.slice([b&#39;Hello&#39;, b&#39;Goodbye&#39;]),
    kd.slice([b&#39;Hell&#39;, b&#39;o&#39;]))
    # -&gt; kd.slice([1, 2])

Args:
  s: The strings to consider.
  substr: The substrings to count in `s`. Must have the same schema as `s`.

Returns:
  The DataSlice of INT32 counts.</code></pre>

### `kd.strings.decode(x)` {#kd.strings.decode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decodes `x` as STRING using UTF-8 decoding.</code></pre>

### `kd.strings.decode_base64(x, /, *, on_invalid=unspecified)` {#kd.strings.decode_base64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decodes BYTES from `x` using base64 encoding (RFC 4648 section 4).

The input strings may either have no padding, or must have the correct amount
of padding. ASCII whitespace characters anywhere in the string are ignored.

Args:
  x: DataSlice of STRING or BYTES containing base64-encoded strings.
  on_invalid: If unspecified (the default), any invalid base64 strings in `x`
    will cause an error. Otherwise, this must be a DataSlice broadcastable to
    `x` with a schema compatible with BYTES, and will be used in the result
    wherever the input string was not valid base64.

Returns:
  DataSlice of BYTES.</code></pre>

### `kd.strings.encode(x)` {#kd.strings.encode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Encodes `x` as BYTES using UTF-8 encoding.</code></pre>

### `kd.strings.encode_base64(x)` {#kd.strings.encode_base64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Encodes BYTES `x` using base64 encoding (RFC 4648 section 4), with padding.

Args:
  x: DataSlice of BYTES to encode.

Returns:
  DataSlice of STRING.</code></pre>

### `kd.strings.find(s, substr, start=0, end=None)` {#kd.strings.find}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the offset of the first occurrence of `substr` in `s`.

The units of `start`, `end`, and the return value are all byte offsets if `s`
is `BYTES` and codepoint offsets if `s` is `STRING`.

Args:
 s: (STRING or BYTES) Strings to search in.
 substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
   dtype as `s`.
 start: (optional int) Offset to start the search, defaults to 0.
 end: (optional int) Offset to stop the search, defaults to end of the string.

Returns:
  The offset of the first occurrence of `substr` in `s`, or missing if there
  are no occurrences.</code></pre>

### `kd.strings.format(fmt, /, **kwargs)` {#kd.strings.format}
Aliases:

- [kd.format](#kd.format)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Formats strings according to python str.format style.

Format support is slightly different from Python:
1. {x:v} is equivalent to {x} and supported for all types as default string
   format.
2. Only float and integers support other format specifiers.
  E.g., {x:.1f} and {x:04d}.
3. If format is missing type specifier `f` or `d` at the end, we are
   adding it automatically based on the type of the argument.

Note: only keyword arguments are supported.

Examples:
  kd.strings.format(kd.slice([&#39;Hello {n}!&#39;, &#39;Goodbye {n}!&#39;]), n=&#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World!&#39;, &#39;Goodbye World!&#39;])
  kd.strings.format(&#39;{a} + {b} = {c}&#39;, a=1, b=2, c=3)
    # -&gt; kd.slice(&#39;1 + 2 = 3&#39;)
  kd.strings.format(
      &#39;{a} + {b} = {c}&#39;,
      a=kd.slice([1, 2]),
      b=kd.slice([2, 3]),
      c=kd.slice([3, 5]))
    # -&gt; kd.slice([&#39;1 + 2 = 3&#39;, &#39;2 + 3 = 5&#39;])
  kd.strings.format(
      &#39;({a:03} + {b:e}) * {c:.2f} =&#39;
      &#39; {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}&#39;
      a=5, b=5.7, c=75)
    # -&gt; kd.slice(
    #        &#39;(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000&#39;)

Args:
  fmt: Format string (String or Bytes).
  **kwargs: Arguments to format.

Returns:
  The formatted string.</code></pre>

### `kd.strings.fstr(x)` {#kd.strings.fstr}
Aliases:

- [kd.fstr](#kd.fstr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Evaluates Koladata f-string into DataSlice.

f-string must be created via Python f-string syntax. It must contain at least
one formatted DataSlice.
Each DataSlice must have custom format specification,
e.g. `{ds:s}` or `{ds:.2f}`.
Find more about format specification in kd.strings.format docs.

NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

Examples:
  countries = kd.slice([&#39;USA&#39;, &#39;Schweiz&#39;])
  kd.fstr(f&#39;Hello, {countries:s}!&#39;)
    # -&gt; kd.slice([&#39;Hello, USA!&#39;, &#39;Hello, Schweiz!&#39;])

  greetings = kd.slice([&#39;Hello&#39;, &#39;Gruezi&#39;])
  kd.fstr(f&#39;{greetings:s}, {countries:s}!&#39;)
    # -&gt; kd.slice([&#39;Hello, USA!&#39;, &#39;Gruezi, Schweiz!&#39;])

  states = kd.slice([[&#39;California&#39;, &#39;Arizona&#39;, &#39;Nevada&#39;], [&#39;Zurich&#39;, &#39;Bern&#39;]])
  kd.fstr(f&#39;{greetings:s}, {states:s} in {countries:s}!&#39;)
    # -&gt; kd.slice([
             [&#39;Hello, California in USA!&#39;,
              &#39;Hello, Arizona in USA!&#39;,
              &#39;Hello, Nevada in USA!&#39;],
             [&#39;Gruezi, Zurich in Schweiz!&#39;,
              &#39;Gruezi, Bern in Schweiz!&#39;]]),

  prices = kd.slice([35.5, 49.2])
  currencies = kd.slice([&#39;USD&#39;, &#39;CHF&#39;])
  kd.fstr(f&#39;Lunch price in {countries:s} is {prices:.2f} {currencies:s}.&#39;)
    # -&gt; kd.slice([&#39;Lunch price in USA is 35.50 USD.&#39;,
                   &#39;Lunch price in Schweiz is 49.20 CHF.&#39;])

Args:
  s: f-string to evaluate.
Returns:
  DataSlice with evaluated f-string.</code></pre>

### `kd.strings.join(*args)` {#kd.strings.join}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates the given strings.

Examples:
  kd.strings.join(kd.slice([&#39;Hello &#39;, &#39;Goodbye &#39;]), &#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World&#39;, &#39;Goodbye World&#39;])
  kd.strings.join(kd.slice([b&#39;foo&#39;]), kd.slice([b&#39; &#39;]), kd.slice([b&#39;bar&#39;]))
    # -&gt; kd.slice([b&#39;foo bar&#39;])

Args:
  *args: The inputs to concatenate in the given order.

Returns:
  The string concatenation of all the inputs.</code></pre>

### `kd.strings.length(x)` {#kd.strings.length}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of lengths in bytes for Byte or codepoints for String.

For example,
  kd.strings.length(kd.slice([&#39;abc&#39;, None, &#39;&#39;])) -&gt; kd.slice([3, None, 0])
  kd.strings.length(kd.slice([b&#39;abc&#39;, None, b&#39;&#39;])) -&gt; kd.slice([3, None, 0])
  kd.strings.length(kd.item(&#39;你好&#39;)) -&gt; kd.item(2)
  kd.strings.length(kd.item(&#39;你好&#39;.encode())) -&gt; kd.item(6)

Note that the result DataSlice always has INT32 schema.

Args:
  x: String or Bytes DataSlice.

Returns:
  A DataSlice of lengths.</code></pre>

### `kd.strings.lower(x)` {#kd.strings.lower}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the lowercase version of each string in the input.

For example,
  kd.strings.lower(kd.slice([&#39;AbC&#39;, None, &#39;&#39;])) -&gt; kd.slice([&#39;abc&#39;, None, &#39;&#39;])
  kd.strings.lower(kd.item(&#39;FOO&#39;)) -&gt; kd.item(&#39;foo&#39;)

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of lowercase strings.</code></pre>

### `kd.strings.lstrip(s, chars=None)` {#kd.strings.lstrip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from the left side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading characters from `s`
that are present in the `chars` set.

Examples:
  kd.strings.lstrip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;spacious   &#39;, &#39;text \n&#39;])
  kd.strings.lstrip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;example.com&#39;])
  kd.strings.lstrip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;Section 3.1 Issue #32 ...&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars: (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.printf(fmt, *args)` {#kd.strings.printf}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Formats strings according to printf-style (C++) format strings.

See absl::StrFormat documentation for the format string details.

Example:
  kd.strings.printf(kd.slice([&#39;Hello %s!&#39;, &#39;Goodbye %s!&#39;]), &#39;World&#39;)
    # -&gt; kd.slice([&#39;Hello World!&#39;, &#39;Goodbye World!&#39;])
  kd.strings.printf(&#39;%v + %v = %v&#39;, 1, 2, 3)  # -&gt; kd.slice(&#39;1 + 2 = 3&#39;)

Args:
  fmt: Format string (String or Bytes).
  *args: Arguments to format (primitive types compatible with `fmt`).

Returns:
  The formatted string.</code></pre>

### `kd.strings.regex_extract(text, regex)` {#kd.strings.regex_extract}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts a substring from `text` with the capturing group of `regex`.

Regular expression matches are partial, which means `regex` is matched against
a substring of `text`.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.
The pattern must contain exactly one capturing group.

Examples:
  kd.strings.regex_extract(kd.item(&#39;foo&#39;), kd.item(&#39;f(.)&#39;))
    # kd.item(&#39;o&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;o(..)&#39;))
    # kd.item(&#39;ob&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;^o(..)$&#39;))
    # kd.item(None).with_schema(kd.STRING)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;^.o(..)a.$&#39;))
    # kd.item(&#39;ob&#39;)
  kd.strings.regex_extract(kd.item(&#39;foobar&#39;), kd.item(&#39;.*(b.*r)$&#39;))
    # kd.item(&#39;bar&#39;)
  kd.strings.regex_extract(kd.slice([&#39;abcd&#39;, None, &#39;&#39;]), kd.slice(&#39;b(.*)&#39;))
    # -&gt; kd.slice([&#39;cd&#39;, None, None])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax) with exactly one capturing group.

Returns:
  For the first partial match of `regex` and `text`, returns the substring of
  `text` that matches the capturing group of `regex`.</code></pre>

### `kd.strings.regex_find_all(text, regex)` {#kd.strings.regex_find_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the captured groups of all matches of `regex` in `text`.

The strings in `text` are scanned left-to-right to find all non-overlapping
matches of `regex`. The order of the matches is preserved in the result. For
each match, the substring matched by each capturing group of `regex` is
recorded. For each item of `text`, the result contains a 2-dimensional value,
where the first dimension captures the number of matches, and the second
dimension captures the captured groups.

Examples:
  # No capturing groups, but two matches:
  kd.strings.regex_find_all(kd.item(&#39;foo&#39;), kd.item(&#39;o&#39;))
    # -&gt; kd.slice([[], []])
  # One capturing group, three matches:
  kd.strings.regex_find_all(kd.item(&#39;foo&#39;), kd.item(&#39;(.)&#39;))
    # -&gt; kd.slice([[&#39;f&#39;], [&#39;o&#39;], [&#39;o&#39;]])
  # Two capturing groups:
  kd.strings.regex_find_all(
      kd.slice([&#39;fooz&#39;, &#39;bar&#39;, &#39;&#39;, None]),
      kd.item(&#39;(.)(.)&#39;)
  )
    # -&gt; kd.slice([[[&#39;f&#39;, &#39;o&#39;], [&#39;o&#39;, &#39;z&#39;]], [[&#39;b&#39;, &#39;a&#39;]], [], []])
  # Get information about the entire substring of each non-overlapping match
  # by enclosing the pattern in additional parentheses:
  kd.strings.regex_find_all(
      kd.slice([[&#39;fool&#39;, &#39;solo&#39;], [&#39;bar&#39;, &#39;boat&#39;]]),
      kd.item(&#39;((.*)o)&#39;)
  )
    # -&gt; kd.slice([[[[&#39;foo&#39;, &#39;fo&#39;]], [[&#39;solo&#39;, &#39;sol&#39;]]], [[], [[&#39;bo&#39;, &#39;b&#39;]]]])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).

Returns:
  A DataSlice where each item of `text` is associated with a 2-dimensional
  representation of its matches&#39; captured groups.</code></pre>

### `kd.strings.regex_match(text, regex)` {#kd.strings.regex_match}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if `text` matches the regular expression `regex`.

Matches are partial, which means a substring of `text` matches the pattern.
For full matches, where the whole string must match a pattern, please enclose
the pattern in `^` and `$` characters.

Examples:
  kd.strings.regex_match(kd.item(&#39;foo&#39;), kd.item(&#39;oo&#39;))
    # -&gt; kd.present
  kd.strings.regex_match(kd.item(&#39;foo&#39;), &#39;^oo$&#39;)
    # -&gt; kd.missing
  kd.strings.regex_match(kd.item(&#39;foo), &#39;^foo$&#39;)
    # -&gt; kd.present
  kd.strings.regex_match(kd.slice([&#39;abc&#39;, None, &#39;&#39;]), &#39;b&#39;)
    # -&gt; kd.slice([kd.present, kd.missing, kd.missing])
  kd.strings.regex_match(kd.slice([&#39;abcd&#39;, None, &#39;&#39;]), kd.slice(&#39;b.d&#39;))
    # -&gt; kd.slice([kd.present, kd.missing, kd.missing])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).

Returns:
  `present` if `text` matches `regex`.</code></pre>

### `kd.strings.regex_replace_all(text, regex, replacement)` {#kd.strings.regex_replace_all}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Replaces all non-overlapping matches of `regex` in `text`.

Examples:
  # Basic with match:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;ana&#39;),
      kd.item(&#39;ono&#39;)
  )  # -&gt; kd.item(&#39;bonona&#39;)
  # Basic with no match:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;x&#39;),
      kd.item(&#39;a&#39;)
  )  # -&gt; kd.item(&#39;banana&#39;)
  # Reference the first capturing group in the replacement:
  kd.strings.regex_replace_all(
      kd.item(&#39;banana&#39;),
      kd.item(&#39;a(.)a&#39;),
      kd.item(r&#39;o\1\1o&#39;)
  )  # -&gt; kd.item(&#39;bonnona&#39;)
  # Reference the whole match in the replacement with \0:
  kd.strings.regex_replace_all(
     kd.item(&#39;abcd&#39;),
     kd.item(&#39;(.)(.)&#39;),
     kd.item(r&#39;\2\1\0&#39;)
  )  # -&gt; kd.item(&#39;baabdccd&#39;)
  # With broadcasting:
  kd.strings.regex_replace_all(
      kd.item(&#39;foopo&#39;),
      kd.item(&#39;o&#39;),
      kd.slice([&#39;a&#39;, &#39;e&#39;]),
  )  # -&gt; kd.slice([&#39;faapa&#39;, &#39;feepe&#39;])
  # With missing values:
  kd.strings.regex_replace_all(
      kd.slice([&#39;foobor&#39;, &#39;foo&#39;, None, &#39;bar&#39;]),
      kd.item(&#39;o(.)&#39;),
      kd.slice([r&#39;\0x\1&#39;, &#39;ly&#39;, &#39;a&#39;, &#39;o&#39;]),
  )  # -&gt; kd.slice([&#39;fooxoborxr&#39;, &#39;fly&#39;, None, &#39;bar&#39;])

Args:
  text: (STRING) A string.
  regex: (STRING) A scalar string that represents a regular expression (RE2
    syntax).
  replacement: (STRING) A string that should replace each match.
    Backslash-escaped digits (\1 to \9) can be used to reference the text that
    matched the corresponding capturing group from the pattern, while \0
    refers to the entire match. Replacements are not subject to re-matching.
    Since it only replaces non-overlapping matches, replacing &#34;ana&#34; within
    &#34;banana&#34; makes only one replacement, not two.

Returns:
  The text string where the replacements have been made.</code></pre>

### `kd.strings.replace(s, old, new, max_subs=None)` {#kd.strings.replace}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Replaces up to `max_subs` occurrences of `old` within `s` with `new`.

If `max_subs` is missing or negative, then there is no limit on the number of
substitutions. If it is zero, then `s` is returned unchanged.

If the search string is empty, the original string is fenced with the
replacement string, for example: replace(&#34;ab&#34;, &#34;&#34;, &#34;-&#34;) returns &#34;-a-b-&#34;. That
behavior is similar to Python&#39;s string replace.

Args:
 s: (STRING or BYTES) Original string.
 old: (STRING or BYTES, the same as `s`) String to replace.
 new: (STRING or BYTES, the same as `s`) Replacement string.
 max_subs: (optional INT32) Max number of substitutions. If unspecified or
   negative, then there is no limit on the number of substitutions.

Returns:
  String with applied substitutions.</code></pre>

### `kd.strings.rfind(s, substr, start=0, end=None)` {#kd.strings.rfind}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the offset of the last occurrence of `substr` in `s`.

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
  are no occurrences.</code></pre>

### `kd.strings.rstrip(s, chars=None)` {#kd.strings.rstrip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from the right side of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all tailing characters from `s` that
are present in the `chars` set.

Examples:
  kd.strings.rstrip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;   spacious&#39;, &#39;\t text&#39;])
  kd.strings.rstrip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;www.example&#39;])
  kd.strings.rstrip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;#... Section 3.1 Issue #32&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.split(x, sep=None)` {#kd.strings.split}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns x split by the provided separator.

Example:
  ds = kd.slice([&#39;Hello world!&#39;, &#39;Goodbye world!&#39;])
  kd.split(ds)  # -&gt; kd.slice([[&#39;Hello&#39;, &#39;world!&#39;], [&#39;Goodbye&#39;, &#39;world!&#39;]])

Args:
  x: DataSlice: (can be text or bytes)
  sep: If specified, will split by the specified string not omitting empty
    strings, otherwise will split by whitespaces while omitting empty strings.</code></pre>

### `kd.strings.strip(s, chars=None)` {#kd.strings.strip}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Strips whitespaces or the specified characters from both sides of `s`.

If `chars` is missing, then whitespaces are removed.
If `chars` is present, then it will strip all leading and tailing characters
from `s` that are present in the `chars` set.

Examples:
  kd.strings.strip(kd.slice([&#39;   spacious   &#39;, &#39;\t text \n&#39;]))
    # -&gt; kd.slice([&#39;spacious&#39;, &#39;text&#39;])
  kd.strings.strip(kd.slice([&#39;www.example.com&#39;]), kd.slice([&#39;cmowz.&#39;]))
    # -&gt; kd.slice([&#39;example&#39;])
  kd.strings.strip(kd.slice([[&#39;#... Section 3.1 Issue #32 ...&#39;], [&#39;# ...&#39;]]),
      kd.slice(&#39;.#! &#39;))
    # -&gt; kd.slice([[&#39;Section 3.1 Issue #32&#39;], [&#39;&#39;]])

Args:
  s: (STRING or BYTES) Original string.
  chars (Optional STRING or BYTES, the same as `s`): The set of chars to
    remove.

Returns:
  Stripped string.</code></pre>

### `kd.strings.substr(x, start=0, end=None)` {#kd.strings.substr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of substrings with indices [start, end).

The usual Python rules apply:
  * A negative index is computed from the end of the string.
  * An empty range yields an empty string, for example when start &gt;= end and
    both are positive.

The result is broadcasted to the common shape of all inputs.

Examples:
  ds = kd.slice([[&#39;Hello World!&#39;, &#39;Ciao bella&#39;], [&#39;Dolly!&#39;]])
  kd.substr(ds)         # -&gt; kd.slice([[&#39;Hello World!&#39;, &#39;Ciao bella&#39;],
                                       [&#39;Dolly!&#39;]])
  kd.substr(ds, 5)      # -&gt; kd.slice([[&#39; World!&#39;, &#39;bella&#39;], [&#39;!&#39;]])
  kd.substr(ds, -2)     # -&gt; kd.slice([[&#39;d!&#39;, &#39;la&#39;], [&#39;y!&#39;]])
  kd.substr(ds, 1, 5)   # -&gt; kd.slice([[&#39;ello&#39;, &#39;iao &#39;], [&#39;olly&#39;]])
  kd.substr(ds, 5, -1)  # -&gt; kd.slice([[&#39; World&#39;, &#39;bell&#39;], [&#39;&#39;]])
  kd.substr(ds, 4, 100) # -&gt; kd.slice([[&#39;o World!&#39;, &#39; bella&#39;], [&#39;y!&#39;]])
  kd.substr(ds, -1, -2) # -&gt; kd.slice([[&#39;&#39;, &#39;&#39;], [&#39;&#39;]])
  kd.substr(ds, -2, -1) # -&gt; kd.slice([[&#39;d&#39;, &#39;l&#39;], [&#39;y&#39;]])

  # Start and end may also be multidimensional.
  ds = kd.slice(&#39;Hello World!&#39;)
  start = kd.slice([1, 2])
  end = kd.slice([[2, 3], [4]])
  kd.substr(ds, start, end) # -&gt; kd.slice([[&#39;e&#39;, &#39;el&#39;], [&#39;ll&#39;]])

Args:
  x: Text or Bytes DataSlice. If text, then `start` and `end` are codepoint
    offsets. If bytes, then `start` and `end` are byte offsets.
  start: The start index of the substring. Inclusive. Assumed to be 0 if
    unspecified.
  end: The end index of the substring. Exclusive. Assumed to be the length of
    the string if unspecified.</code></pre>

### `kd.strings.upper(x)` {#kd.strings.upper}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with the uppercase version of each string in the input.

For example,
  kd.strings.upper(kd.slice([&#39;abc&#39;, None, &#39;&#39;])) -&gt; kd.slice([&#39;ABC&#39;, None, &#39;&#39;])
  kd.strings.upper(kd.item(&#39;foo&#39;)) -&gt; kd.item(&#39;FOO&#39;)

Note that the result DataSlice always has STRING schema.

Args:
  x: String DataSlice.

Returns:
  A String DataSlice of uppercase strings.</code></pre>

</section>

### kd.streams {#kd.streams}

Operators that work with streams of items. These APIs are in active development and might change often (b/424742492).

<section class="zippy closed">

**Operators**

### `kd.streams.await_(arg)` {#kd.streams.await_}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Indicates to kd.streams.call that the argument should be awaited.

This operator acts as a marker. When the returned value is passed to
`kd.streams.call`, it signals that `kd.streams.call` should await
the underlying stream to yield a single item. This single item is then
passed to the functor.

Importantly, `stream_await` itself does not perform any awaiting or blocking.

If the input `arg` is not a stream, this operators returns `arg` unchanged.

Note: `kd.streams.call` expects an awaited stream to yield exactly one item.
Producing zero or more than one item from an awaited stream will result in
an error during the `kd.streams.call` evaluation.

Args:
  arg: The input argument (the operator has effect only if `arg` is a stream).

Returns:
  If `arg` was a stream, it gets labeled with &#39;AWAIT&#39;. If `arg` was not
  a stream, `arg` is returned without modification.</code></pre>

### `kd.streams.call(fn, *args, executor=unspecified, return_type_as=None, **kwargs)` {#kd.streams.call}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Calls a functor on the given executor and yields the result(s) as a stream.

For stream arguments tagged with `kd.streams.await_`, `kd.streams.call` first
awaits the corresponding input streams. Each of these streams is expected to
yield exactly one item, which is then passed as the argument to the functor
`fn`. If a labeled stream is empty or yields more than one item, it is
considered an error.

The `return_type_as` parameter specifies the return type of the functor `fn`.
Unless the return type is already a stream, the result of `kd.streams.call` is
a `STREAM[return_type]` storing a single value returned by the functor.
However, if `return_type_as` is a stream, the result of `kd.streams.call` is
of the same stream type, holding the same items as the stream returned by
the functor.

It&#39;s recommended to specify the same `return_type_as` for `kd.streams.call`
calls as it would be for regular `kd.call`.

Importantly, `kd.streams.call` supports the case when `return_type_as` is
non-stream while the functor actually returns `STREAM[return_type]`. This
enables nested `kd.streams.call` calls.

Args:
  fn: The functor to be called, typically created via kd.fn().
  *args: The positional arguments to pass to the call. The stream arguments
    tagged with `kd.streams.await_` will be awaited before the call, and
    expected to yield exactly one item.
  executor: The executor to use for computations.
  return_type_as: The return type of the functor `fn` call.
  **kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  If the return type of the functor (as specified by `return_type_as`) is
  a non-stream type, the result of `kd.streams.call` is a single-item stream
  with the functor&#39;s return value. Otherwise, the result is a stream of
  the same type as `return_type_as`, containing the same items as the stream
  returned by the functor.</code></pre>

### `kd.streams.chain(*streams, value_type_as=unspecified)` {#kd.streams.chain}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that chains the given streams, in the given order.

The streams must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the streams, if any.

Args:
  *streams: A list of streams to be chained (concatenated).
  value_type_as: A value that has the same type as the items in the streams.
    It is useful to specify this explicitly if the list of streams may be
    empty. If this is not specified and the list of streams is empty, the
    stream will have DATA_SLICE as the value type.

Returns:
  A stream that chains the given streams in the given order.</code></pre>

### `kd.streams.chain_from_stream(stream_of_streams)` {#kd.streams.chain_from_stream}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that chains the given streams.

The resulting stream has all items from the first sub-stream, then all items
from the second sub-stream, and so on.

Example:
    ```
    kd.streams.chain_from_stream(
        kd.streams.make(
            kd.streams.make(1, 2, 3),
            kd.streams.make(4),
            kd.streams.make(5, 6),
        )
    )
    ```
    result: A stream with items [1, 2, 3, 4, 5, 6].

Args:
  stream_of_streams: A stream of input streams.

Returns:
  A stream that chains the input streams.</code></pre>

### `kd.streams.current_executor()` {#kd.streams.current_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the current executor.

If the current computation is running on an executor, this operator
returns it. If no executor is set for the current context, this operator
returns an error.

Note: For the convenience, in Python environments, the default executor
(see `get_default_executor`) is implicitly set as the current executor.
However, this might not be not the case for other environments.</code></pre>

### `kd.streams.flat_map_chained(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.flat_map_chained}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given stream.

`fn` is called for each item in the input stream, and it must return a new
stream. The streams returned by `fn` are then chained to produce the final
result.

Example:
    ```
    kd.streams.flat_map_interleaved(
        kd.streams.make(1, 10),
        lambda x: kd.streams.make(x, x * 2, x * 3),
    )
    ```
    result: A stream with items [1, 2, 3, 10, 20, 30].

Args:
  stream: The stream to iterate over.
  fn: The function to be executed for each item in the stream. It will receive
    the stream item as the positional argument and must return a stream of
    values compatible with value_type_as.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as element type of the resulting stream.

Returns:
  The resulting interleaved results of `fn` calls.</code></pre>

### `kd.streams.flat_map_interleaved(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.flat_map_interleaved}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes flat maps over the given stream.

`fn` is called for each item in the input stream, and it must return a new
stream. The streams returned by `fn` are then interleaved to produce the final
result. Note that while the internal order of items within each stream
returned by `fn` is preserved, the overall order of items from different
streams is not guaranteed.

Example:
    ```
    kd.streams.flat_map_interleaved(
        kd.streams.make(1, 10),
        lambda x: kd.streams.make(x, x * 2, x * 3),
    )
    ```
    result: A stream with items {1, 2, 3, 10, 20, 30}. While the relative
      order within {1, 2, 3} and {10, 20, 30} is guaranteed, the overall order
      of items is unspecified. For instance, the following orderings are both
      possible:
       * [1, 10, 2, 20, 3, 30]
       * [10, 20, 30, 1, 2, 3]

Args:
  stream: The stream to iterate over.
  fn: The function to be executed for each item in the stream. It will receive
    the stream item as the positional argument and must return a stream of
    values compatible with value_type_as.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as element type of the resulting stream.

Returns:
  The resulting interleaved results of `fn` calls.</code></pre>

### `kd.streams.foreach(stream, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, executor=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.streams.foreach}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Executes a loop over the given stream.

Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
and that dictates what this operator returns.

When `returns` is specified, it is one more variable added to `initial_state`,
and the value of that variable at the end of the loop is returned in a single-
item stream.

When `yields` is specified, it must be an stream, and the value
passed there, as well as the values set to this variable in each
stream of the loop, are chained to get the resulting stream.

When `yields_interleaved` is specified, the behavior is the same as `yields`,
but the values are interleaved instead of chained.

The behavior of the loop is equivalent to the following pseudocode (with
a simplification that `stream` is an `iterable`):

  state = initial_state  # Also add `returns` to it if specified.
  while condition_fn(state):
    item = next(iterable)
    if item == &lt;end-of-iterable&gt;:
      upd = finalize_fn(**state)
    else:
      upd = body_fn(item, **state)
    if yields/yields_interleaved is specified:
      yield the corresponding data from upd, and remove it from upd.
    state.update(upd)
    if item == &lt;end-of-iterable&gt;:
      break
  if returns is specified:
    yield state[&#39;returns&#39;]

Args:
  stream: The stream to iterate over.
  body_fn: The function to be executed for each item in the stream. It will
    receive the stream item as the positional argument, and the loop variables
    as keyword arguments (excluding `yields`/`yields_interleaved` if those are
    specified), and must return a namedtuple with the new values for some or
    all loop variables (including `yields`/`yields_interleaved` if those are
    specified).
  finalize_fn: The function to be executed when the stream is exhausted. It
    will receive the same arguments as `body_fn` except the positional
    argument, and must return the same namedtuple. If not specified, the state
    at the end will be the same as the state after processing the last item.
    Note that finalize_fn is not called if condition_fn ever returns false.
  condition_fn: The function to be executed to determine whether to continue
    the loop. It will receive the loop variables as keyword arguments, and
    must return a MASK scalar. Can be used to terminate the loop early without
    processing all items in the stream. If not specified, the loop will
    continue until the stream is exhausted.
  executor: The executor to use for computations.
  returns: The loop variable that holds the return value of the loop.
  yields: The loop variables that holds the values to yield at each iteration,
    to be chained together.
  yields_interleaved: The loop variables that holds the values to yield at
    each iteration, to be interleaved.
  **initial_state: The initial state of the loop variables.

Returns:
  Either a stream with a single returns value or a stream of yielded values.</code></pre>

### `kd.streams.from_1d_slice(slice_)` {#kd.streams.from_1d_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a 1D DataSlice to a stream of DataItems.

Args:
  slice_: A 1D DataSlice to be converted to a stream.

Returns:
  A stream of DataItems, in the order of the slice. All returned
  DataItems point to the same DataBag as the input DataSlice.</code></pre>

### `kd.streams.get_default_executor()` {#kd.streams.get_default_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the default executor.</code></pre>

### `kd.streams.get_eager_executor()` {#kd.streams.get_eager_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an executor that runs tasks right away on the same thread.</code></pre>

### `kd.streams.get_stream_qtype(value_qtype)` {#kd.streams.get_stream_qtype}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the stream qtype for the given value qtype.</code></pre>

### `kd.streams.interleave(*streams, value_type_as=unspecified)` {#kd.streams.interleave}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that interleaves the given streams.

The resulting stream has all items from all input streams, and the order of
items from each stream is preserved. But the order of interleaving of
different streams can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

The input streams must all have the same value type. If value_type_as is
specified, it must be the same as the value type of the streams, if any.

Args:
  *streams: Input streams.
  value_type_as: A value that has the same type as the items in the streams.
    It is useful to specify this explicitly if the list of streams may be
    empty. If this is not specified and the list of streams is empty, the
    resulting stream will have DATA_SLICE as the value type.

Returns:
  A stream that interleaves the input streams in an unspecified order.</code></pre>

### `kd.streams.interleave_from_stream(stream_of_streams)` {#kd.streams.interleave_from_stream}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream that interleaves the given streams.

The resulting stream has all items from all input streams, and the order of
items from each stream is preserved. But the order of interleaving of
different streams can be arbitrary.

Having unspecified order allows the parallel execution to put the items into
the result in the order they are computed, potentially increasing the amount
of parallel processing done.

Args:
  stream_of_streams: A stream of input streams.

Returns:
  A stream that interleaves the input streams in an unspecified order.</code></pre>

### `kd.streams.make(*items, value_type_as=unspecified)` {#kd.streams.make}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a stream from the given items, in the given order.

The items must all have the same type (for example data slice, or data bag).
However, in case of data slices, the items can have different shapes or
schemas.

Args:
  *items: Items to be put into the stream.
  value_type_as: A value that has the same type as the items. It is useful to
    specify this explicitly if the list of items may be empty. If this is not
    specified and the list of items is empty, the iterable will have data
    slice as the value type.

Returns:
  A stream with the given items.</code></pre>

### `kd.streams.make_executor(*, thread_limit=0)` {#kd.streams.make_executor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new executor.

Note: The `thread_limit` limits the concurrency; however, the executor may
have no dedicated threads, and the actual concurrency limit might be lower.

Args:
  thread_limit: The number of threads to use. Must be non-negative; 0 means
    that the number of threads is selected automatically.</code></pre>

### `kd.streams.map(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new stream by applying `fn` to each item in the input stream.

For each item of the input `stream`, the `fn` is called. The single
resulting item from each call is then written into the new output stream.

Args:
  stream: The input stream.
  fn: The function to be executed for each item of the input stream. It will
    receive an item as the positional argument and its result must be of the
    same type as `value_type_as`.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as value type of the resulting stream.

Returns:
  The resulting stream.</code></pre>

### `kd.streams.map_unordered(stream, fn, *, executor=unspecified, value_type_as=None)` {#kd.streams.map_unordered}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new stream by applying `fn` to each item in the input `stream`.

For each item of the input `stream`, the `fn` is called. The single
resulting item from each call is then written into the new output stream.

IMPORTANT: The order of the items in the resulting stream is not guaranteed.

Args:
  stream: The input stream.
  fn: The function to be executed for each item of the input stream. It will
    receive an item as the positional argument and its result must be of the
    same type as `value_type_as`.
  executor: An executor for scheduling asynchronous operations.
  value_type_as: The type to use as value type of the resulting stream.

Returns:
  The resulting stream.</code></pre>

### `kd.streams.reduce(fn, stream, initial_value, *, executor=unspecified)` {#kd.streams.reduce}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces a stream by iteratively applying a functor `fn`.

This operator applies `fn` sequentially to an accumulating value and each
item of the `stream`. The process begins with `initial_value`, then follows
this pattern:

         value_0 = initial_value
         value_1 = fn(value_0, stream[0])
         value_2 = fn(value_1, stream[1])
                ...

The result of the reduction is the final computed value.

Args:
  fn: A binary function that takes two positional arguments -- the current
    accumulating value and the next item from the stream -- and returns a new
    value. It&#39;s expected to return a value of the same type as
    `initial_value`.
  stream: The input stream.
  initial_value: The initial value.
  executor: The executor to use for computations.

Returns:
  A stream with a single item containing the final result of the reduction.</code></pre>

### `kd.streams.reduce_concat(stream, initial_value, *, ndim=1, executor=unspecified)` {#kd.streams.reduce_concat}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Concatenates data slices from the stream.

A specialized version of kd.streams.reduce() designed to speed up
the concatenation of data slices.

Using a standard kd.streams.reduce() with kd.concat() would result in
an O(N**2) computational complexity. This implementation, however, achieves
an O(N) complexity.

See the docstring for `kd.concat` for more details about the concatenation
semantics.

Args:
  stream: A stream of data slices to be concatenated.
  initial_value: The initial value to be concatenated before items.
  ndim: The number of last dimensions to concatenate.
  executor: The executor to use for computations.

Returns:
  A single-item stream with the concatenated data slice.</code></pre>

### `kd.streams.reduce_stack(stream, initial_value, *, ndim=0, executor=unspecified)` {#kd.streams.reduce_stack}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Stacks data slices from the stream.

A specialized version of kd.streams.reduce() designed to speed up
the concatenation of data slices.

Using a standard kd.streams.reduce() with kd.stack() would result in
an O(N**2) computational complexity. This implementation, however,
achieves an O(N) complexity.

See the docstring for `kd.stack` for more details about the stacking
semantics.

Args:
  stream: A stream of data slices to be stacked.
  initial_value: The initial value to be stacked before items.
  ndim: The number of last dimensions to stack (default 0).
  executor: The executor to use for computations.

Returns:
  A single-item stream with the stacked data slice.</code></pre>

### `kd.streams.sync_wait(stream)` {#kd.streams.sync_wait}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Blocks until the given stream yields a single item.

NOTE: This operator cannot be used from an asynchronous task running on
an executor (even if it&#39;s an eager executor).

Args:
  stream: A single-item input stream.

Returns:
  The single item from the stream.</code></pre>

### `kd.streams.unsafe_blocking_wait(stream)` {#kd.streams.unsafe_blocking_wait}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Blocks until the given stream yields a single item.

Unlike `kd.streams.sync_wait`, this operator can be used from an asynchronous
task running on an executor, but that makes it inherently unsafe.

IMPORTANT: This operator is inherently unsafe and should be used with extreme
caution. It&#39;s primarily intended for transitional periods when migrating
a complex, synchronous computation to a concurrent model, enabling incremental
changes instead of a complete migration in one step.

The main danger stems from its blocking nature: it blocks the calling thread
until the stream is ready. However, if the task responsible for filling
the stream is also scheduled on the same executor, and all executor workers
become blocked, that task may never execute, leading to a deadlock.

While seemingly acceptable initially, prolonged or widespread use of this
operator will eventually cause deadlocks, requiring a non-trivial refactoring
of your computation.

BEGIN-GOOGLE-INTERNAL
Note: While this operator is relatively safe to use with fibers, it&#39;s still
NOT recommended for permanent use.
END-GOOGLE-INTERNAL

Args:
  stream: A single-item input stream.

Returns:
  The single item from the stream.</code></pre>

### `kd.streams.while_(condition_fn, body_fn, *, executor=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.streams.while_}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Repeatedly applies a body functor while a condition is met.

Each iteration, the operator passes current state variables (including
`returns`, if specified) as keyword arguments to `condition_fn` and `body_fn`.
The loop continues if `condition_fn` returns `present`. State variables are
then updated from `body_fn`&#39;s namedtuple return value.

This operator always returns a stream, with the concrete behaviour
depending on whether `returns`, `yields`, or `yields_interleaved` was
specified (exactly one of them must be specified):

- `returns`: a single-item stream with the final value of the `returns` state
  variable;

- `yields`: a stream created by chaining the initial `yields` stream with any
  subsequent streams produced by the `body_fn`;

- `yields_interleaved`: the same as for `yields`, but instead of being chained
  the streams are interleaved.

Args:
  condition_fn: A functor that accepts state variables (including `returns`,
    if specified) as keyword arguments and returns a MASK data-item, either
    directly or as a single-item stream. A `present` value indicates the loop
    should continue; `missing` indicates it should stop.
  body_fn: A functor that accepts state variables *including `returns`, if
    specified) as keyword arguments and returns a namedtuple (see
    `kd.make_namedtuple`) containing updated values for a subset of the state
    variables. These updated values must retain their original types.
  executor: The executor to use for computations.
  returns: If present, the initial value of the &#39;returns&#39; state variable.
  yields: If present, the initial value of the &#39;yields&#39; state variable.
  yields_interleaved: If present, the initial value of the
    `yields_interleaved` state variable.
  **initial_state: Initial values for state variables.

Returns:
  If `returns` is a state variable, the value of `returns` when the loop
  ended. Otherwise, a stream combining the values of `yields` or
  `yields_interleaved` from each body invocation.</code></pre>

</section>

### kd.tuples {#kd.tuples}

Operators to create tuples.

<section class="zippy closed">

**Operators**

### `kd.tuples.get_namedtuple_field(namedtuple: NamedTuple, field_name: str | DataItem) -> Any` {#kd.tuples.get_namedtuple_field}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value of the specified `field_name` from the `namedtuple`.

Args:
  namedtuple: a namedtuple.
  field_name: the name of the field to return.</code></pre>

### `kd.tuples.get_nth(x: Any, n: SupportsIndex) -> Any` {#kd.tuples.get_nth}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the nth element of the tuple `x`.

Args:
  x: a tuple.
  n: the index of the element to return. Must be in the range [0, len(x)).</code></pre>

### `kd.tuples.namedtuple(**kwargs)` {#kd.tuples.namedtuple}
Aliases:

- [kd.namedtuple](#kd.namedtuple)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a namedtuple-like object containing the given `**kwargs`.</code></pre>

### `kd.tuples.slice(start=unspecified, stop=unspecified, step=unspecified)` {#kd.tuples.slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a slice for the Python indexing syntax foo[start:stop:step].

Args:
  start: (optional) Indexing start.
  stop: (optional) Indexing stop.
  step: (optional) Indexing step size.</code></pre>

### `kd.tuples.tuple(*args)` {#kd.tuples.tuple}
Aliases:

- [kd.tuple](#kd.tuple)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a tuple-like object containing the given `*args`.</code></pre>

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

### `kd.argmax(x, ndim=unspecified)` {#kd.argmax}

Alias for [kd.math.argmax](#kd.math.argmax) operator.

### `kd.argmin(x, ndim=unspecified)` {#kd.argmin}

Alias for [kd.math.argmin](#kd.math.argmin) operator.

### `kd.at(x, indices)` {#kd.at}

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.attr(x, attr_name, value, overwrite_schema=False)` {#kd.attr}

Alias for [kd.core.attr](#kd.core.attr) operator.

### `kd.attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.attrs}

Alias for [kd.core.attrs](#kd.core.attrs) operator.

### `kd.bag()` {#kd.bag}

Alias for [kd.bags.new](#kd.bags.new) operator.

### `kd.bind(fn_def: DataSlice, /, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataSlice` {#kd.bind}

Alias for [kd.functor.bind](#kd.functor.bind) operator.

### `kd.bitwise_and(x, y)` {#kd.bitwise_and}

Alias for [kd.bitwise.bitwise_and](#kd.bitwise.bitwise_and) operator.

### `kd.bitwise_count(x)` {#kd.bitwise_count}

Alias for [kd.bitwise.count](#kd.bitwise.count) operator.

### `kd.bitwise_invert(x)` {#kd.bitwise_invert}

Alias for [kd.bitwise.invert](#kd.bitwise.invert) operator.

### `kd.bitwise_or(x, y)` {#kd.bitwise_or}

Alias for [kd.bitwise.bitwise_or](#kd.bitwise.bitwise_or) operator.

### `kd.bitwise_xor(x, y)` {#kd.bitwise_xor}

Alias for [kd.bitwise.bitwise_xor](#kd.bitwise.bitwise_xor) operator.

### `kd.bool(x: Any) -> DataSlice` {#kd.bool}

Alias for [kd.slices.bool](#kd.slices.bool) operator.

### `kd.bytes(x: Any) -> DataSlice` {#kd.bytes}

Alias for [kd.slices.bytes](#kd.slices.bytes) operator.

### `kd.call(fn, *args, return_type_as=None, **kwargs)` {#kd.call}

Alias for [kd.functor.call](#kd.functor.call) operator.

### `kd.cast_to(x, schema)` {#kd.cast_to}

Alias for [kd.schema.cast_to](#kd.schema.cast_to) operator.

### `kd.check_inputs(**kw_constraints: SchemaItem | _DuckType | _StaticWhenTraced)` {#kd.check_inputs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator factory for adding runtime input type checking to Koda functions.

Resulting decorators will check the schemas of DataSlice inputs of
a function at runtime, and raise TypeError in case of mismatch.

Decorated functions will preserve the original function&#39;s signature and
docstring.

Decorated functions can be traced using `kd.fn` and the inputs to the
resulting functor will be wrapped in kd.assertion.with_assertion nodes that
match the assertions of the eager version.

Example for primitive schemas:

  @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
  @kd.check_output(kd.STRING)
  def timestamp(hours, minutes):
    return kd.str(hours) + &#39;:&#39; + kd.str(minutes)

  timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
  timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

Example for complex schemas:

  Doc = kd.schema.named_schema(&#39;Doc&#39;, doc_id=kd.INT64, score=kd.FLOAT32)

  Query = kd.schema.named_schema(
      &#39;Query&#39;,
      query_text=kd.STRING,
      query_id=kd.INT32,
      docs=kd.list_schema(Doc),
  )

  @kd.check_inputs(query=Query)
  @kd.check_output(Doc)
  def get_docs(query):
    return query.docs[:]

Example for an argument that should not be an Expr at tracing time:
  @kd.check_inputs(x=kd.constant_when_tracing(kd.INT32))
  def f(x):
    return x


Args:
  **kw_constraints: mapping of parameter names to type constraints. Names must
    match parameter names in the decorated function. Arguments for the given
    parameters must be DataSlices/DataItems that match the given type
    constraint(in particular, for SchemaItems, they must have the
    corresponding schema).

Returns:
  A decorator that can be used to type annotate a function that accepts
  DataSlices/DataItem inputs.</code></pre>

### `kd.check_output(constraint: SchemaItem | _DuckType | _StaticWhenTraced)` {#kd.check_output}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator factory for adding runtime output type checking to Koda functions.

Resulting decorators will check the schema of the DataSlice output of
a function at runtime, and raise TypeError in case of mismatch.

Decorated functions will preserve the original function&#39;s signature and
docstring.

Decorated functions can be traced using `kd.fn` and the output of the
resulting functor will be wrapped in a kd.assertion.with_assertion node that
match the assertion of the eager version.

Example for primitive schemas:

  @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
  @kd.check_output(kd.STRING)
  def timestamp(hours, minutes):
    return kd.to_str(hours) + &#39;:&#39; + kd.to_str(minutes)

  timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
  timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

Example for complex schemas:

  Doc = kd.schema.named_schema(&#39;Doc&#39;, doc_id=kd.INT64, score=kd.FLOAT32)

  Query = kd.schema.named_schema(
      &#39;Query&#39;,
      query_text=kd.STRING,
      query_id=kd.INT32,
      docs=kd.list_schema(Doc),
  )

  @kd.check_inputs(query=Query)
  @kd.check_output(Doc)
  def get_docs(query):
    return query.docs[:]

Args:
  constraint: A type constraint for the output. Output of the decorated
    function must be a DataSlice/DataItem that matches the constraint(in
    particular, for SchemaItems, they must have the corresponding schema).

Returns:
  A decorator that can be used to annotate a function returning a
  DataSlice/DataItem.</code></pre>

### `kd.cityhash(x, seed)` {#kd.cityhash}

Alias for [kd.random.cityhash](#kd.random.cityhash) operator.

### `kd.clear_eval_cache()` {#kd.clear_eval_cache}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Clears Koda specific eval caches.</code></pre>

### `kd.clone(x, /, *, itemid=unspecified, schema=unspecified, **overrides)` {#kd.clone}

Alias for [kd.core.clone](#kd.core.clone) operator.

### `kd.coalesce(x, y)` {#kd.coalesce}

Alias for [kd.masking.coalesce](#kd.masking.coalesce) operator.

### `kd.collapse(x, ndim=unspecified)` {#kd.collapse}

Alias for [kd.slices.collapse](#kd.slices.collapse) operator.

### `kd.concat(*args, ndim=1)` {#kd.concat}

Alias for [kd.slices.concat](#kd.slices.concat) operator.

### `kd.concat_lists(*lists: DataSlice) -> DataSlice` {#kd.concat_lists}

Alias for [kd.lists.concat](#kd.lists.concat) operator.

### `kd.cond(condition, yes, no=None)` {#kd.cond}

Alias for [kd.masking.cond](#kd.masking.cond) operator.

### `kd.container(**attrs: Any) -> DataSlice` {#kd.container}

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

### `kd.deep_uuid(x, /, schema=unspecified, *, seed='')` {#kd.deep_uuid}

Alias for [kd.ids.deep_uuid](#kd.ids.deep_uuid) operator.

### `kd.del_attr(x: DataSlice, attr_name: str)` {#kd.del_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Deletes an attribute `attr_name` from `x`.</code></pre>

### `kd.dense_rank(x, descending=False, ndim=unspecified)` {#kd.dense_rank}

Alias for [kd.slices.dense_rank](#kd.slices.dense_rank) operator.

### `kd.dict(items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict}

Alias for [kd.dicts.new](#kd.dicts.new) operator.

### `kd.dict_like(shape_and_mask_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, *, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_like}

Alias for [kd.dicts.like](#kd.dicts.like) operator.

### `kd.dict_schema(key_schema, value_schema)` {#kd.dict_schema}

Alias for [kd.schema.dict_schema](#kd.schema.dict_schema) operator.

### `kd.dict_shaped(shape: JaggedShape, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_shaped}

Alias for [kd.dicts.shaped](#kd.dicts.shaped) operator.

### `kd.dict_shaped_as(shape_from: DataSlice, /, items_or_keys: Any | None = None, values: Any | None = None, key_schema: DataSlice | None = None, value_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.dict_shaped_as}

Alias for [kd.dicts.shaped_as](#kd.dicts.shaped_as) operator.

### `kd.dict_size(dict_slice)` {#kd.dict_size}

Alias for [kd.dicts.size](#kd.dicts.size) operator.

### `kd.dict_update(x, keys, values=unspecified)` {#kd.dict_update}

Alias for [kd.dicts.dict_update](#kd.dicts.dict_update) operator.

### `kd.dir(x: DataSlice) -> list[str]` {#kd.dir}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a sorted list of unique attribute names of the given DataSlice.

This is equivalent to `kd.get_attr_names(ds, intersection=True)`. For more
finegrained control, use `kd.get_attr_names` directly instead.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  x: A DataSlice.

Returns:
  A list of unique attributes sorted by alphabetical order.</code></pre>

### `kd.disjoint_coalesce(x, y)` {#kd.disjoint_coalesce}

Alias for [kd.masking.disjoint_coalesce](#kd.masking.disjoint_coalesce) operator.

### `kd.duck_dict(key_constraint: SchemaItem | _DuckType | _StaticWhenTraced, value_constraint: SchemaItem | _DuckType | _StaticWhenTraced)` {#kd.duck_dict}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck dict constraint to be used in kd.check_inputs/output.

A duck_dict constraint will assert a DataSlice is a dict, checking the
key_constraint on the keys and value_constraint on the values. Use it if you
need to nest duck type constraints in dict constraints.

Example:
  @kd.check_inputs(mapping=kd.duck_dict(kd.STRING,
      kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)))
  def f(query):
    pass

Args:
  key_constraint:  DuckType or SchemaItem representing the constraint to be
    checked on the keys of the dict.
  value_constraint:  DuckType or SchemaItem representing the constraint to be
    checked on the values of the dict.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.duck_list(item_constraint: SchemaItem | _DuckType | _StaticWhenTraced)` {#kd.duck_list}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck list constraint to be used in kd.check_inputs/output.

A duck_list constraint will assert a DataSlice is a list, checking the
item_constraint on the items. Use it if you need to nest
duck type constraints in list constraints.

Example:
  @kd.check_inputs(query=kd.duck_type(docs=kd.duck_list(
      kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)
  )))
  def f(query):
    pass

Args:
  item_constraint:  DuckType or SchemaItem representing the constraint to be
    checked on the items of the list.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.duck_type(**kwargs: SchemaItem | _DuckType | _StaticWhenTraced)` {#kd.duck_type}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck type constraint to be used in kd.check_inputs/output.

A duck type constraint will assert that the DataSlice input/output of a
function has (at least) a certain set of attributes, as well as to specify
recursive constraints for those attributes.

Example:
  @kd.check_inputs(query=kd.duck_type(query_text=kd.STRING,
     docs=kd.duck_type()))
  def f(query):
    pass

  Checks that the DataSlice input parameter `query` has a STRING attribute
  `query_text`, and an attribute `docs` of any schema. `query` may also have
  additional unspecified attributes.

Args:
  **kwargs: mapping of attribute names to constraints. The constraints must be
    either DuckTypes or SchemaItems. To assert only the presence of an
    attribute, without specifying additional constraints on that attribute,
    pass an empty duck type for that attribute.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.dumps(x: DataSlice | DataBag, /, *, riegeli_options: str | None = None) -> bytes` {#kd.dumps}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes a DataSlice or a DataBag.

In case of a DataSlice, we try to use `x.extract()` to avoid serializing
unnecessary DataBag data. If this is undesirable, consider serializing the
DataBag directly.

Due to current limitations of the underlying implementation, this can
only serialize data slices with up to roughly 10**8 items.

Args:
  x: DataSlice or DataBag to serialize.
  riegeli_options: A string with riegeli/records writer options. See
    https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
      for details. If not provided, &#39;snappy&#39; will be used.

Returns:
  Serialized data.</code></pre>

### `kd.embed_schema(x: DataSlice) -> DataSlice` {#kd.embed_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as &#39;__schema__&#39; attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.

Args:
  x: (DataSlice) whose schema is embedded.</code></pre>

### `kd.empty_shaped(shape, schema=MASK)` {#kd.empty_shaped}

Alias for [kd.slices.empty_shaped](#kd.slices.empty_shaped) operator.

### `kd.empty_shaped_as(shape_from, schema=MASK)` {#kd.empty_shaped_as}

Alias for [kd.slices.empty_shaped_as](#kd.slices.empty_shaped_as) operator.

### `kd.encode_itemid(ds)` {#kd.encode_itemid}

Alias for [kd.ids.encode_itemid](#kd.ids.encode_itemid) operator.

### `kd.enriched(ds, *bag)` {#kd.enriched}

Alias for [kd.core.enriched](#kd.core.enriched) operator.

### `kd.enriched_bag(*bags)` {#kd.enriched_bag}

Alias for [kd.bags.enriched](#kd.bags.enriched) operator.

### `kd.equal(x, y)` {#kd.equal}

Alias for [kd.comparison.equal](#kd.comparison.equal) operator.

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

Alias for [kd.slices.expand_to](#kd.slices.expand_to) operator.

### `kd.expand_to_shape(x, shape, ndim=unspecified)` {#kd.expand_to_shape}

Alias for [kd.shapes.expand_to_shape](#kd.shapes.expand_to_shape) operator.

### `kd.explode(x, ndim=1)` {#kd.explode}

Alias for [kd.lists.explode](#kd.lists.explode) operator.

### `kd.expr_quote(x: Any) -> DataSlice` {#kd.expr_quote}

Alias for [kd.slices.expr_quote](#kd.slices.expr_quote) operator.

### `kd.extension_type(unsafe_override=False) -> Callable[[type[Any]], type[Any]]` {#kd.extension_type}

Alias for [kd.extension_types.extension_type](#kd.extension_types.extension_type) operator.

### `kd.extract(ds, schema=unspecified)` {#kd.extract}

Alias for [kd.core.extract](#kd.core.extract) operator.

### `kd.extract_bag(ds, schema=unspecified)` {#kd.extract_bag}

Alias for [kd.core.extract_bag](#kd.core.extract_bag) operator.

### `kd.flat_map_chain(iterable, fn, value_type_as=None)` {#kd.flat_map_chain}

Alias for [kd.functor.flat_map_chain](#kd.functor.flat_map_chain) operator.

### `kd.flat_map_interleaved(iterable, fn, value_type_as=None)` {#kd.flat_map_interleaved}

Alias for [kd.functor.flat_map_interleaved](#kd.functor.flat_map_interleaved) operator.

### `kd.flatten(x, from_dim=0, to_dim=unspecified)` {#kd.flatten}

Alias for [kd.shapes.flatten](#kd.shapes.flatten) operator.

### `kd.flatten_end(x, n_times=1)` {#kd.flatten_end}

Alias for [kd.shapes.flatten_end](#kd.shapes.flatten_end) operator.

### `kd.float32(x: Any) -> DataSlice` {#kd.float32}

Alias for [kd.slices.float32](#kd.slices.float32) operator.

### `kd.float64(x: Any) -> DataSlice` {#kd.float64}

Alias for [kd.slices.float64](#kd.slices.float64) operator.

### `kd.fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataSlice` {#kd.fn}

Alias for [kd.functor.fn](#kd.functor.fn) operator.

### `kd.follow(x)` {#kd.follow}

Alias for [kd.core.follow](#kd.core.follow) operator.

### `kd.for_(iterable, body_fn, *, finalize_fn=unspecified, condition_fn=unspecified, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.for_}

Alias for [kd.functor.for_](#kd.functor.for_) operator.

### `kd.format(fmt, /, **kwargs)` {#kd.format}

Alias for [kd.strings.format](#kd.strings.format) operator.

### `kd.freeze(x)` {#kd.freeze}

Alias for [kd.core.freeze](#kd.core.freeze) operator.

### `kd.freeze_bag(x)` {#kd.freeze_bag}

Alias for [kd.core.freeze_bag](#kd.core.freeze_bag) operator.

### `kd.from_json(x, /, schema=OBJECT, default_number_schema=OBJECT, *, on_invalid=DataSlice([], schema: NONE), keys_attr='json_object_keys', values_attr='json_object_values')` {#kd.from_json}

Alias for [kd.json.from_json](#kd.json.from_json) operator.

### `kd.from_proto(messages: Message | list[_NestedMessageList] | None, /, *, extensions: list[str] | None = None, itemid: DataSlice | None = None, schema: DataSlice | None = None) -> DataSlice` {#kd.from_proto}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice representing proto data.

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

  &#34;path.to.field.(package_name.some_extension)&#34;
  &#34;path.to.repeated_field.(package_name.some_extension)&#34;
  &#34;path.to.map_field.values.(package_name.some_extension)&#34;
  &#34;path.(package_name.some_extension).(package_name2.nested_extension)&#34;

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
list of proto Messages, the result is an 1D DataSlice.

Args:
  messages: Message or nested list of Message of the same type. Any of the
    messages may be None, which will produce missing items in the result.
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

### `kd.from_proto_bytes(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.from_proto_bytes}

Alias for [kd.proto.from_proto_bytes](#kd.proto.from_proto_bytes) operator.

### `kd.from_proto_json(x, proto_path, /, *, extensions=unspecified, itemids=unspecified, schema=unspecified, on_invalid=unspecified)` {#kd.from_proto_json}

Alias for [kd.proto.from_proto_json](#kd.proto.from_proto_json) operator.

### `kd.from_py(py_obj: Any, *, dict_as_obj: bool = False, itemid: DataSlice | None = None, schema: DataSlice | None = None, from_dim: int = 0) -> DataSlice` {#kd.from_py}
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

### `kd.from_pytree(py_obj: Any, *, dict_as_obj: bool = False, itemid: DataSlice | None = None, schema: DataSlice | None = None, from_dim: int = 0) -> DataSlice` {#kd.from_pytree}

Alias for [kd.from_py](#kd.from_py) operator.

### `kd.fstr(x)` {#kd.fstr}

Alias for [kd.strings.fstr](#kd.strings.fstr) operator.

### `kd.full_equal(x, y)` {#kd.full_equal}

Alias for [kd.comparison.full_equal](#kd.comparison.full_equal) operator.

### `kd.get_attr(x, attr_name, default=unspecified)` {#kd.get_attr}

Alias for [kd.core.get_attr](#kd.core.get_attr) operator.

### `kd.get_attr_names(x: DataSlice, *, intersection: bool) -> list[str]` {#kd.get_attr_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a sorted list of unique attribute names of the given DataSlice.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  x: A DataSlice.
  intersection: If True, the intersection of all object attributes is
    returned. Otherwise, the union is returned.

Returns:
  A list of unique attributes sorted by alphabetical order.</code></pre>

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

### `kd.get_metadata(x)` {#kd.get_metadata}

Alias for [kd.core.get_metadata](#kd.core.get_metadata) operator.

### `kd.get_ndim(x)` {#kd.get_ndim}

Alias for [kd.slices.get_ndim](#kd.slices.get_ndim) operator.

### `kd.get_nofollowed_schema(schema)` {#kd.get_nofollowed_schema}

Alias for [kd.schema.get_nofollowed_schema](#kd.schema.get_nofollowed_schema) operator.

### `kd.get_obj_schema(x)` {#kd.get_obj_schema}

Alias for [kd.schema.get_obj_schema](#kd.schema.get_obj_schema) operator.

### `kd.get_primitive_schema(ds)` {#kd.get_primitive_schema}

Alias for [kd.schema.get_dtype](#kd.schema.get_dtype) operator.

### `kd.get_proto_attr(x, field_name)` {#kd.get_proto_attr}

Alias for [kd.proto.get_proto_attr](#kd.proto.get_proto_attr) operator.

### `kd.get_repr(x, /, *, depth=25, item_limit=200, item_limit_per_dimension=25, format_html=False, max_str_len=100, max_expr_quote_len=10000, show_attributes=True, show_databag_id=False, show_shape=False, show_schema=False, show_item_id=False)` {#kd.get_repr}

Alias for [kd.slices.get_repr](#kd.slices.get_repr) operator.

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

### `kd.group_by(x, *keys, sort=False)` {#kd.group_by}

Alias for [kd.slices.group_by](#kd.slices.group_by) operator.

### `kd.group_by_indices(*keys, sort=False)` {#kd.group_by_indices}

Alias for [kd.slices.group_by_indices](#kd.slices.group_by_indices) operator.

### `kd.has(x)` {#kd.has}

Alias for [kd.masking.has](#kd.masking.has) operator.

### `kd.has_attr(x, attr_name)` {#kd.has_attr}

Alias for [kd.core.has_attr](#kd.core.has_attr) operator.

### `kd.has_bag(ds)` {#kd.has_bag}

Alias for [kd.core.has_bag](#kd.core.has_bag) operator.

### `kd.has_dict(x)` {#kd.has_dict}

Alias for [kd.dicts.has_dict](#kd.dicts.has_dict) operator.

### `kd.has_entity(x)` {#kd.has_entity}

Alias for [kd.core.has_entity](#kd.core.has_entity) operator.

### `kd.has_fn(x)` {#kd.has_fn}

Alias for [kd.functor.has_fn](#kd.functor.has_fn) operator.

### `kd.has_list(x)` {#kd.has_list}

Alias for [kd.lists.has_list](#kd.lists.has_list) operator.

### `kd.has_not(x)` {#kd.has_not}

Alias for [kd.masking.has_not](#kd.masking.has_not) operator.

### `kd.has_primitive(x)` {#kd.has_primitive}

Alias for [kd.core.has_primitive](#kd.core.has_primitive) operator.

### `kd.hash_itemid(x)` {#kd.hash_itemid}

Alias for [kd.ids.hash_itemid](#kd.ids.hash_itemid) operator.

### `kd.if_(cond, yes_fn, no_fn, *args, return_type_as=None, **kwargs)` {#kd.if_}

Alias for [kd.functor.if_](#kd.functor.if_) operator.

### `kd.implode(x: DataSlice, /, ndim: int | DataSlice = 1, itemid: DataSlice | None = None) -> DataSlice` {#kd.implode}

Alias for [kd.lists.implode](#kd.lists.implode) operator.

### `kd.index(x, dim=-1)` {#kd.index}

Alias for [kd.slices.index](#kd.slices.index) operator.

### `kd.int32(x: Any) -> DataSlice` {#kd.int32}

Alias for [kd.slices.int32](#kd.slices.int32) operator.

### `kd.int64(x: Any) -> DataSlice` {#kd.int64}

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

### `kd.is_expr(obj: Any) -> DataSlice` {#kd.is_expr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is an Expr and kd.missing otherwise.</code></pre>

### `kd.is_fn(obj: Any) -> DataSlice` {#kd.is_fn}

Alias for [kd.functor.is_fn](#kd.functor.is_fn) operator.

### `kd.is_item(obj: Any) -> DataSlice` {#kd.is_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is a scalar DataItem and kd.missing otherwise.</code></pre>

### `kd.is_list(x)` {#kd.is_list}

Alias for [kd.lists.is_list](#kd.lists.is_list) operator.

### `kd.is_nan(x)` {#kd.is_nan}

Alias for [kd.math.is_nan](#kd.math.is_nan) operator.

### `kd.is_null_bag(bag)` {#kd.is_null_bag}

Alias for [kd.bags.is_null_bag](#kd.bags.is_null_bag) operator.

### `kd.is_primitive(x)` {#kd.is_primitive}

Alias for [kd.core.is_primitive](#kd.core.is_primitive) operator.

### `kd.is_shape_compatible(x, y)` {#kd.is_shape_compatible}

Alias for [kd.slices.is_shape_compatible](#kd.slices.is_shape_compatible) operator.

### `kd.is_slice(obj: Any) -> DataSlice` {#kd.is_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns kd.present if the given object is a DataSlice and kd.missing otherwise.</code></pre>

### `kd.isin(x, y)` {#kd.isin}

Alias for [kd.slices.isin](#kd.slices.isin) operator.

### `kd.item(x, /, schema=None)` {#kd.item}

Alias for [kd.slices.item](#kd.slices.item) operator.

### `kd.less(x, y)` {#kd.less}

Alias for [kd.comparison.less](#kd.comparison.less) operator.

### `kd.less_equal(x, y)` {#kd.less_equal}

Alias for [kd.comparison.less_equal](#kd.comparison.less_equal) operator.

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

Alias for [kd.lists.list_append_update](#kd.lists.list_append_update) operator.

### `kd.list_like(shape_and_mask_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_like}

Alias for [kd.lists.like](#kd.lists.like) operator.

### `kd.list_schema(item_schema)` {#kd.list_schema}

Alias for [kd.schema.list_schema](#kd.schema.list_schema) operator.

### `kd.list_shaped(shape: JaggedShape, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_shaped}

Alias for [kd.lists.shaped](#kd.lists.shaped) operator.

### `kd.list_shaped_as(shape_from: DataSlice, /, items: Any | None = None, *, item_schema: DataSlice | None = None, schema: DataSlice | None = None, itemid: DataSlice | None = None) -> DataSlice` {#kd.list_shaped_as}

Alias for [kd.lists.shaped_as](#kd.lists.shaped_as) operator.

### `kd.list_size(list_slice)` {#kd.list_size}

Alias for [kd.lists.size](#kd.lists.size) operator.

### `kd.loads(x: bytes) -> Any` {#kd.loads}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Deserializes a DataSlice or a DataBag.</code></pre>

### `kd.map(fn, *args, include_missing=False, **kwargs)` {#kd.map}

Alias for [kd.functor.map](#kd.functor.map) operator.

### `kd.map_py(fn, *args, schema=None, max_threads=1, ndim=0, include_missing=None, item_completed_callback=None, **kwargs)` {#kd.map_py}

Alias for [kd.py.map_py](#kd.py.map_py) operator.

### `kd.map_py_on_cond(true_fn, false_fn, cond, *args, schema=None, max_threads=1, item_completed_callback=None, **kwargs)` {#kd.map_py_on_cond}

Alias for [kd.py.map_py_on_cond](#kd.py.map_py_on_cond) operator.

### `kd.map_py_on_selected(fn, cond, *args, schema=None, max_threads=1, item_completed_callback=None, **kwargs)` {#kd.map_py_on_selected}

Alias for [kd.py.map_py_on_selected](#kd.py.map_py_on_selected) operator.

### `kd.mask(x: Any) -> DataSlice` {#kd.mask}

Alias for [kd.slices.mask](#kd.slices.mask) operator.

### `kd.mask_and(x, y)` {#kd.mask_and}

Alias for [kd.masking.mask_and](#kd.masking.mask_and) operator.

### `kd.mask_equal(x, y)` {#kd.mask_equal}

Alias for [kd.masking.mask_equal](#kd.masking.mask_equal) operator.

### `kd.mask_not_equal(x, y)` {#kd.mask_not_equal}

Alias for [kd.masking.mask_not_equal](#kd.masking.mask_not_equal) operator.

### `kd.mask_or(x, y)` {#kd.mask_or}

Alias for [kd.masking.mask_or](#kd.masking.mask_or) operator.

### `kd.max(x)` {#kd.max}

Alias for [kd.math.max](#kd.math.max) operator.

### `kd.maximum(x, y)` {#kd.maximum}

Alias for [kd.math.maximum](#kd.math.maximum) operator.

### `kd.maybe(x, attr_name)` {#kd.maybe}

Alias for [kd.core.maybe](#kd.core.maybe) operator.

### `kd.metadata(x, /, **attrs)` {#kd.metadata}

Alias for [kd.core.metadata](#kd.core.metadata) operator.

### `kd.min(x)` {#kd.min}

Alias for [kd.math.min](#kd.math.min) operator.

### `kd.minimum(x, y)` {#kd.minimum}

Alias for [kd.math.minimum](#kd.math.minimum) operator.

### `kd.mutable_bag()` {#kd.mutable_bag}
Aliases:

- [DataBag.empty_mutable](#DataBag.empty_mutable)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an empty mutable DataBag. Only works in eager mode.</code></pre>

### `kd.named_container()` {#kd.named_container}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Container that automatically names Exprs.

For non-expr inputs, in tracing mode it will be converted to an Expr,
while in non-tracing mode it will be stored as is. This allows to use
NamedContainer eager code that will later be traced.

For example:
  c = kd.ext.expr_container.NamedContainer()
  c.x_plus_y = I.x + I.y
  c.x_plus_y  # Returns (I.x + I.y).with_name(&#39;x_plus_y&#39;)
  c.foo = 5
  c.foo  # Returns 5

Functions and lambdas are automatically traced in tracing mode.

For example:
  def foo(x):
    c = kd.ext.expr_container.NamedContainer()
    c.x = x
    c.update = lambda x: x + 1
    return c.update(c.x)

  fn = kd.fn(foo)
  fn(x=5)  # Returns 6</code></pre>

### `kd.named_schema(name, /, **kwargs)` {#kd.named_schema}

Alias for [kd.schema.named_schema](#kd.schema.named_schema) operator.

### `kd.namedtuple(**kwargs)` {#kd.namedtuple}

Alias for [kd.tuples.namedtuple](#kd.tuples.namedtuple) operator.

### `kd.new(arg: Any = unspecified, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new}

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

### `kd.new_like(shape_and_mask_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_like}

Alias for [kd.entities.like](#kd.entities.like) operator.

### `kd.new_listid()` {#kd.new_listid}

Alias for [kd.allocation.new_listid](#kd.allocation.new_listid) operator.

### `kd.new_listid_like(shape_and_mask_from)` {#kd.new_listid_like}

Alias for [kd.allocation.new_listid_like](#kd.allocation.new_listid_like) operator.

### `kd.new_listid_shaped(shape)` {#kd.new_listid_shaped}

Alias for [kd.allocation.new_listid_shaped](#kd.allocation.new_listid_shaped) operator.

### `kd.new_listid_shaped_as(shape_from)` {#kd.new_listid_shaped_as}

Alias for [kd.allocation.new_listid_shaped_as](#kd.allocation.new_listid_shaped_as) operator.

### `kd.new_shaped(shape: JaggedShape, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_shaped}

Alias for [kd.entities.shaped](#kd.entities.shaped) operator.

### `kd.new_shaped_as(shape_from: DataSlice, /, *, schema: DataSlice | str | None = None, overwrite_schema: bool = False, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.new_shaped_as}

Alias for [kd.entities.shaped_as](#kd.entities.shaped_as) operator.

### `kd.no_bag(ds)` {#kd.no_bag}

Alias for [kd.core.no_bag](#kd.core.no_bag) operator.

### `kd.nofollow(x)` {#kd.nofollow}

Alias for [kd.core.nofollow](#kd.core.nofollow) operator.

### `kd.nofollow_schema(schema)` {#kd.nofollow_schema}

Alias for [kd.schema.nofollow_schema](#kd.schema.nofollow_schema) operator.

### `kd.not_equal(x, y)` {#kd.not_equal}

Alias for [kd.comparison.not_equal](#kd.comparison.not_equal) operator.

### `kd.obj(arg: Any = unspecified, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj}

Alias for [kd.objs.new](#kd.objs.new) operator.

### `kd.obj_like(shape_and_mask_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_like}

Alias for [kd.objs.like](#kd.objs.like) operator.

### `kd.obj_shaped(shape: JaggedShape, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_shaped}

Alias for [kd.objs.shaped](#kd.objs.shaped) operator.

### `kd.obj_shaped_as(shape_from: DataSlice, /, *, itemid: DataSlice | None = None, **attrs: Any) -> DataSlice` {#kd.obj_shaped_as}

Alias for [kd.objs.shaped_as](#kd.objs.shaped_as) operator.

### `kd.ordinal_rank(x, tie_breaker=unspecified, descending=False, ndim=unspecified)` {#kd.ordinal_rank}

Alias for [kd.slices.ordinal_rank](#kd.slices.ordinal_rank) operator.

### `kd.present_like(x)` {#kd.present_like}

Alias for [kd.masking.present_like](#kd.masking.present_like) operator.

### `kd.present_shaped(shape)` {#kd.present_shaped}

Alias for [kd.masking.present_shaped](#kd.masking.present_shaped) operator.

### `kd.present_shaped_as(x)` {#kd.present_shaped_as}

Alias for [kd.masking.present_shaped_as](#kd.masking.present_shaped_as) operator.

### `kd.pwl_curve(p, adjustments)` {#kd.pwl_curve}

Alias for [kd.curves.pwl_curve](#kd.curves.pwl_curve) operator.

### `kd.py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataSlice` {#kd.py_fn}

Alias for [kd.functor.py_fn](#kd.functor.py_fn) operator.

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

Alias for [kd.random.randint_like](#kd.random.randint_like) operator.

### `kd.randint_shaped(shape, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped}

Alias for [kd.random.randint_shaped](#kd.random.randint_shaped) operator.

### `kd.randint_shaped_as(x, low=unspecified, high=unspecified, seed=unspecified)` {#kd.randint_shaped_as}

Alias for [kd.random.randint_shaped_as](#kd.random.randint_shaped_as) operator.

### `kd.range(start, end=unspecified)` {#kd.range}

Alias for [kd.slices.range](#kd.slices.range) operator.

### `kd.ref(ds)` {#kd.ref}

Alias for [kd.core.ref](#kd.core.ref) operator.

### `kd.register_py_fn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, unsafe_override: bool = False, **defaults: Any) -> DataSlice` {#kd.register_py_fn}

Alias for [kd.functor.register_py_fn](#kd.functor.register_py_fn) operator.

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

### `kd.schema_from_proto_path(proto_path, /, *, extensions=DataItem(Entity:#5ikYYvXepp19g47QDLnJR2, schema: ITEMID))` {#kd.schema_from_proto_path}

Alias for [kd.proto.schema_from_proto_path](#kd.proto.schema_from_proto_path) operator.

### `kd.schema_from_py(tpe: type[Any]) -> SchemaItem` {#kd.schema_from_py}

Alias for [kd.schema.schema_from_py](#kd.schema.schema_from_py) operator.

### `kd.select(ds, fltr, expand_filter=True)` {#kd.select}

Alias for [kd.slices.select](#kd.slices.select) operator.

### `kd.select_items(ds, fltr)` {#kd.select_items}

Alias for [kd.lists.select_items](#kd.lists.select_items) operator.

### `kd.select_keys(ds, fltr)` {#kd.select_keys}

Alias for [kd.dicts.select_keys](#kd.dicts.select_keys) operator.

### `kd.select_present(ds)` {#kd.select_present}

Alias for [kd.slices.select_present](#kd.slices.select_present) operator.

### `kd.select_values(ds, fltr)` {#kd.select_values}

Alias for [kd.dicts.select_values](#kd.dicts.select_values) operator.

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

Alias for [kd.core.shallow_clone](#kd.core.shallow_clone) operator.

### `kd.shuffle(x, /, ndim=unspecified, seed=unspecified)` {#kd.shuffle}

Alias for [kd.random.shuffle](#kd.random.shuffle) operator.

### `kd.size(x)` {#kd.size}

Alias for [kd.slices.size](#kd.slices.size) operator.

### `kd.slice(x, /, schema=None)` {#kd.slice}

Alias for [kd.slices.slice](#kd.slices.slice) operator.

### `kd.sort(x, sort_by=unspecified, descending=False)` {#kd.sort}

Alias for [kd.slices.sort](#kd.slices.sort) operator.

### `kd.stack(*args, ndim=0)` {#kd.stack}

Alias for [kd.slices.stack](#kd.slices.stack) operator.

### `kd.static_when_tracing(base_type: SchemaItem | None = None) -> _StaticWhenTraced` {#kd.static_when_tracing}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A constraint that the argument is static when tracing.

It is used to check that the argument is not an expression during tracing to
prevent a common mistake.

Examples:
- combined with checking the type:
  @type_checking.check_inputs(value=kd.static_when_tracing(kd.INT32))
- without checking the type:
  @type_checking.check_inputs(pick_a=kd.static_when_tracing())

Args:
  base_type: (optional)The base type to check against. If not specified, only
    checks that the argument is a static when tracing.

Returns:
  A constraint that the argument is a static when tracing.</code></pre>

### `kd.str(x: Any) -> DataSlice` {#kd.str}

Alias for [kd.slices.str](#kd.slices.str) operator.

### `kd.strict_attrs(x, /, **attrs)` {#kd.strict_attrs}

Alias for [kd.core.strict_attrs](#kd.core.strict_attrs) operator.

### `kd.strict_with_attrs(x, /, **attrs)` {#kd.strict_with_attrs}

Alias for [kd.core.strict_with_attrs](#kd.core.strict_with_attrs) operator.

### `kd.stub(x, attrs=DataSlice([], schema: NONE))` {#kd.stub}

Alias for [kd.core.stub](#kd.core.stub) operator.

### `kd.subslice(x, *slices)` {#kd.subslice}

Alias for [kd.slices.subslice](#kd.slices.subslice) operator.

### `kd.sum(x)` {#kd.sum}

Alias for [kd.math.sum](#kd.math.sum) operator.

### `kd.take(x, indices)` {#kd.take}

Alias for [kd.slices.at](#kd.slices.at) operator.

### `kd.tile(x, shape)` {#kd.tile}

Alias for [kd.slices.tile](#kd.slices.tile) operator.

### `kd.to_expr(x)` {#kd.to_expr}

Alias for [kd.schema.to_expr](#kd.schema.to_expr) operator.

### `kd.to_itemid(x)` {#kd.to_itemid}

Alias for [kd.schema.get_itemid](#kd.schema.get_itemid) operator.

### `kd.to_json(x, /, *, indent=None, ensure_ascii=True, keys_attr='json_object_keys', values_attr='json_object_values', include_missing_values=True)` {#kd.to_json}

Alias for [kd.json.to_json](#kd.json.to_json) operator.

### `kd.to_none(x)` {#kd.to_none}

Alias for [kd.schema.to_none](#kd.schema.to_none) operator.

### `kd.to_object(x)` {#kd.to_object}

Alias for [kd.schema.to_object](#kd.schema.to_object) operator.

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

### `kd.to_proto_bytes(x, proto_path, /)` {#kd.to_proto_bytes}

Alias for [kd.proto.to_proto_bytes](#kd.proto.to_proto_bytes) operator.

### `kd.to_proto_json(x, proto_path, /)` {#kd.to_proto_json}

Alias for [kd.proto.to_proto_json](#kd.proto.to_proto_json) operator.

### `kd.to_py(ds: DataSlice, max_depth: int = 2, obj_as_dict: bool = False, include_missing_attrs: bool = True) -> Any` {#kd.to_py}

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
    objects.</code></pre>

### `kd.to_pylist(x: DataSlice) -> list[Any]` {#kd.to_pylist}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands the outermost DataSlice dimension into a list of DataSlices.</code></pre>

### `kd.to_pytree(ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True) -> Any` {#kd.to_pytree}
*No description*

### `kd.to_schema(x)` {#kd.to_schema}

Alias for [kd.schema.to_schema](#kd.schema.to_schema) operator.

### `kd.trace_as_fn(*, name: str | None = None, return_type_as: Any = None, functor_factory: FunctorFactory | None = None)` {#kd.trace_as_fn}

Alias for [kd.functor.trace_as_fn](#kd.functor.trace_as_fn) operator.

### `kd.trace_py_fn(f: Callable[..., Any], *, auto_variables: bool = True, **defaults: Any) -> DataSlice` {#kd.trace_py_fn}

Alias for [kd.functor.trace_py_fn](#kd.functor.trace_py_fn) operator.

### `kd.translate(keys_to, keys_from, values_from)` {#kd.translate}

Alias for [kd.slices.translate](#kd.slices.translate) operator.

### `kd.translate_group(keys_to, keys_from, values_from)` {#kd.translate_group}

Alias for [kd.slices.translate_group](#kd.slices.translate_group) operator.

### `kd.tuple(*args)` {#kd.tuple}

Alias for [kd.tuples.tuple](#kd.tuples.tuple) operator.

### `kd.unique(x, sort=False)` {#kd.unique}

Alias for [kd.slices.unique](#kd.slices.unique) operator.

### `kd.update_schema(obj: DataSlice, **attr_schemas: Any) -> DataSlice` {#kd.update_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Updates the schema of `obj` DataSlice using given schemas for attrs.</code></pre>

### `kd.updated(ds, *bag)` {#kd.updated}

Alias for [kd.core.updated](#kd.core.updated) operator.

### `kd.updated_bag(*bags)` {#kd.updated_bag}

Alias for [kd.bags.updated](#kd.bags.updated) operator.

### `kd.uu(seed: str | None = None, *, schema: DataSlice | None = None, overwrite_schema: bool = False, **attrs: Any) -> DataSlice` {#kd.uu}

Alias for [kd.entities.uu](#kd.entities.uu) operator.

### `kd.uu_schema(seed='', **kwargs)` {#kd.uu_schema}

Alias for [kd.schema.uu_schema](#kd.schema.uu_schema) operator.

### `kd.uuid(seed='', **kwargs)` {#kd.uuid}

Alias for [kd.ids.uuid](#kd.ids.uuid) operator.

### `kd.uuid_for_dict(seed='', **kwargs)` {#kd.uuid_for_dict}

Alias for [kd.ids.uuid_for_dict](#kd.ids.uuid_for_dict) operator.

### `kd.uuid_for_list(seed='', **kwargs)` {#kd.uuid_for_list}

Alias for [kd.ids.uuid_for_list](#kd.ids.uuid_for_list) operator.

### `kd.uuids_with_allocation_size(seed='', *, size)` {#kd.uuids_with_allocation_size}

Alias for [kd.ids.uuids_with_allocation_size](#kd.ids.uuids_with_allocation_size) operator.

### `kd.uuobj(seed: str | None = None, **attrs: Any) -> DataSlice` {#kd.uuobj}

Alias for [kd.objs.uu](#kd.objs.uu) operator.

### `kd.val_like(x, val)` {#kd.val_like}

Alias for [kd.slices.val_like](#kd.slices.val_like) operator.

### `kd.val_shaped(shape, val)` {#kd.val_shaped}

Alias for [kd.slices.val_shaped](#kd.slices.val_shaped) operator.

### `kd.val_shaped_as(x, val)` {#kd.val_shaped_as}

Alias for [kd.slices.val_shaped_as](#kd.slices.val_shaped_as) operator.

### `kd.while_(condition_fn, body_fn, *, returns=unspecified, yields=unspecified, yields_interleaved=unspecified, **initial_state)` {#kd.while_}

Alias for [kd.functor.while_](#kd.functor.while_) operator.

### `kd.with_attr(x, attr_name, value, overwrite_schema=False)` {#kd.with_attr}

Alias for [kd.core.with_attr](#kd.core.with_attr) operator.

### `kd.with_attrs(x, /, *, overwrite_schema=False, **attrs)` {#kd.with_attrs}

Alias for [kd.core.with_attrs](#kd.core.with_attrs) operator.

### `kd.with_bag(ds, bag)` {#kd.with_bag}

Alias for [kd.core.with_bag](#kd.core.with_bag) operator.

### `kd.with_dict_update(x, keys, values=unspecified)` {#kd.with_dict_update}

Alias for [kd.dicts.with_dict_update](#kd.dicts.with_dict_update) operator.

### `kd.with_list_append_update(x, append)` {#kd.with_list_append_update}

Alias for [kd.lists.with_list_append_update](#kd.lists.with_list_append_update) operator.

### `kd.with_merged_bag(ds)` {#kd.with_merged_bag}

Alias for [kd.core.with_merged_bag](#kd.core.with_merged_bag) operator.

### `kd.with_metadata(x, /, **attrs)` {#kd.with_metadata}

Alias for [kd.core.with_metadata](#kd.core.with_metadata) operator.

### `kd.with_name(obj: Any, name: str | Text) -> Any` {#kd.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `kd.with_print(x, *args, sep=' ', end='\n')` {#kd.with_print}

Alias for [kd.core.with_print](#kd.core.with_print) operator.

### `kd.with_schema(x, schema)` {#kd.with_schema}

Alias for [kd.schema.with_schema](#kd.schema.with_schema) operator.

### `kd.with_schema_from_obj(x)` {#kd.with_schema_from_obj}

Alias for [kd.schema.with_schema_from_obj](#kd.schema.with_schema_from_obj) operator.

### `kd.xor(x, y)` {#kd.xor}

Alias for [kd.masking.xor](#kd.masking.xor) operator.

### `kd.zip(*args)` {#kd.zip}

Alias for [kd.slices.zip](#kd.slices.zip) operator.

</section>

## kd_ext {#kd_ext}

Operators under the `kd_ext.xxx` modules for extension utilities. Importing from
the following module is needed:
`from koladata import kd_ext`


<section class="zippy open">

**Namespaces**

### kd_ext.contrib {#kd_ext.contrib}

External contributions not necessarily endorsed by Koda.

<section class="zippy closed">

**Operators**

### `kd_ext.contrib.value_counts(x)` {#kd_ext.contrib.value_counts}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns Dicts mapping entries in `x` to their count over the last dim.

Similar to Pandas&#39; `value_counts`.

The output is a `x.get_ndim() - 1`-dimensional DataSlice containing one
Dict per aggregated row in `x`. Each Dict maps the values to the number of
occurrences (as an INT64) in the final dimension.

Example:
  x = kd.slice([[4, 3, 4], [None, 2], [2, 1, 4, 1], [None]])
  kd_ext.contrib.value_counts(x)
    # -&gt; [Dict{4: 2, 3: 1}, Dict{2: 1}, Dict{2: 1, 1: 2, 4: 1}, Dict{}]

Args:
  x: the non-scalar DataSlice to compute occurrences for.</code></pre>

</section>

### kd_ext.nested_data {#kd_ext.nested_data}

Utilities for manipulating nested data.

<section class="zippy closed">

**Operators**

### `kd_ext.nested_data.selected_path_update(root_ds: DataSlice, selection_ds_path: list[str], selection_ds: DataSlice | function) -> DataBag` {#kd_ext.nested_data.selected_path_update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataBag where only the selected items are present in child lists.

The selection_ds_path must contain at least one list attribute. In general,
all lists must use an explicit list schema; this function does not work for
lists stored as kd.OBJECT.

Example:
  ```
  selection_ds = root_ds.a[:].b.c[:].x &gt; 1
  ds = root_ds.updated(selected_path(root_ds, [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;], selection_ds))
  assert not kd.any(ds.a[:].b.c[:].x &lt;= 1)
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
  @root_ds.updated(selected_path(....)).</code></pre>

</section>

### kd_ext.npkd {#kd_ext.npkd}

Tools for Numpy <-> Koda interoperability.

<section class="zippy closed">

**Operators**

### `kd_ext.npkd.from_array(arr: ndarray) -> DataSlice` {#kd_ext.npkd.from_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a numpy array to a DataSlice.</code></pre>

### `kd_ext.npkd.get_elements_indices_from_ds(ds: DataSlice) -> list[ndarray]` {#kd_ext.npkd.get_elements_indices_from_ds}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list of np arrays representing the DataSlice&#39;s indices.

You can consider this as a n-dimensional coordinates of the items, p.ex. for a
two-dimensional DataSlice:

[[a, b],
 [],
 [c, d]] -&gt; [[0, 0, 2, 2], [0, 1, 0, 1]]

 Let&#39;s explain this:
 - &#39;a&#39; is in the first row and first column, its coordinates are (0, 0)
 - &#39;b&#39; is in the first row and second column, its coordinates are (0, 1)
 - &#39;c&#39; is in the third row and first column, its coordinates are (2, 0)
 - &#39;d&#39; is in the third row and second column, its coordinates are (2, 1)

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
  list of np arrays representing the DataSlice&#39;s elements indices.</code></pre>

### `kd_ext.npkd.reshape_based_on_indices(ds: DataSlice, indices: list[ndarray]) -> DataSlice` {#kd_ext.npkd.reshape_based_on_indices}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reshapes a DataSlice corresponding to the given indices.

Inverse operation to get_elements_indices_from_ds.

Let&#39;s explain this based on the following example:

ds: [a, b, c, d]
indices: [[0, 0, 2, 2], [0, 1, 0, 1]]
result: [[a, b], [], [c, d]]

Indices represent y- and x-coordinates of the items in the DataSlice.
- &#39;a&#39;: according to the indices, its coordinates are (0, 0) (first element
from the first and second row of indices conrrespondingly);
it will be placed in the first row and first column of the result;
- &#39;b&#39;: its coordinates are (0, 1); it will be placed in the first row and
second column of the result;
- &#39;c&#39;: its coordinates are (2, 0); it will be placed in the third row and
first column of the result;
- &#39;d&#39;: its coordinates are (2, 1); it will be placed in the third row and
second column of the result.

The result DataSlice will have the same number of items as the original
DataSlice. Its dimensionality will be equal to the number of rows in the
indices.

Args:
  ds: DataSlice to reshape; can only be 1D.
  indices: list of np arrays representing the DataSlice&#39;s indices; it has to
    be a list of one-dimensional arrays where each row has equal number of
    elements corresponding to the number of items in the DataSlice.

Returns:
  DataSlice reshaped based on the given indices.</code></pre>

### `kd_ext.npkd.to_array(ds: DataSlice) -> ndarray` {#kd_ext.npkd.to_array}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts a DataSlice to a numpy array.</code></pre>

</section>

### kd_ext.pdkd {#kd_ext.pdkd}

Tools for Pandas <-> Koda interoperability.

<section class="zippy closed">

**Operators**

### `kd_ext.pdkd.df(ds: DataSlice, cols: list[str | Expr] | None = None, include_self: bool = False) -> DataFrame` {#kd_ext.pdkd.df}
Aliases:

- [kd_ext.pdkd.to_dataframe](#kd_ext.pdkd.to_dataframe)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a pandas DataFrame from the given DataSlice.

If `ds` has no dimension, it will be converted to a single row DataFrame. If
it has one dimension, it willbe converted an 1D DataFrame. If it has more than
one dimension, it will be converted to a MultiIndex DataFrame with index
columns corresponding to each dimension.

When `cols` is not specified, DataFrame columns are inferred from `ds`.
  1) If `ds` has primitives, lists, dicts or ITEMID schema, a single
     column named &#39;self_&#39; is used and items themselves are extracted.
  2) If `ds` has entity schema, all attributes from `ds` are extracted as
     columns.
  3) If `ds` has OBJECT schema, the union of attributes from all objects in
     `ds` are used as columns. Missing values are filled if objects do not
     have corresponding attributes.

For example,

  ds = kd.slice([1, 2, 3])
  to_dataframe(ds) -&gt; extract &#39;self_&#39;

  ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
  to_dataframe(ds) -&gt; extract &#39;x&#39; and &#39;y&#39;

  ds = kd.slice([kd.obj(x=1, y=&#39;a&#39;), kd.obj(x=2), kd.obj(x=3, y=&#39;c&#39;)])
  to_dataframe(ds) -&gt; extract &#39;x&#39;, &#39;y&#39;

`cols` can be used to specify which data from the DataSlice should be
extracted as DataFrame columns. It can contain either the string names of
attributes or Exprs which can be evaluated on the DataSlice. If `ds` has
OBJECT schema, specified attributes must present in all objects in `ds`. To
ignore objects which do not have specific attributes, one can use
`S.maybe(attr)` in `cols`. For example,

  ds = kd.slice([1, 2, 3])
  to_dataframe(ds) -&gt; extract &#39;self_&#39;

  ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
  to_dataframe(ds, [&#39;x&#39;]) -&gt; extract &#39;x&#39;
  to_dataframe(ds, [S.x, S.x + S.y]) -&gt; extract &#39;S.x&#39; and &#39;S.x + S.y&#39;

  ds = kd.slice([kd.obj(x=1, y=&#39;a&#39;), kd.obj(x=2), kd.obj(x=3, y=&#39;c&#39;)])
  to_dataframe(ds, [&#39;x&#39;]) -&gt; extract &#39;x&#39;
  to_dataframe(ds, [S.y]) -&gt; raise an exception as &#39;y&#39; does not exist in
      kd.obj(x=2)
  to_dataframe(ds, [S.maybe(&#39;y&#39;)]) -&gt; extract &#39;y&#39; but ignore items which
      do not have &#39;x&#39; attribute.

If extracted column DataSlices have different shapes, they will be aligned to
the same dimensions. For example,

  ds = kd.new(
      x = kd.slice([1, 2, 3]),
      y=kd.list(kd.new(z=kd.slice([[4], [5], [6]]))),
      z=kd.list(kd.new(z=kd.slice([[4, 5], [], [6]]))),
  )
  to_dataframe(ds, cols=[S.x, S.y[:].z]) -&gt; extract &#39;S.x&#39; and &#39;S.y[:].z&#39;:
         &#39;x&#39; &#39;y[:].z&#39;
    0 0   1     4
      1   1     5
    2 0   3     6
  to_dataframe(ds, cols=[S.y[:].z, S.z[:].z]) -&gt; error: shapes mismatch

The conversion adheres to:
  * All output data will be of nullable types (e.g. `Int64Dtype()` rather than
    `np.int64`)
  * `pd.NA` is used for missing values.
  * Numeric dtypes, booleans and strings will use corresponding pandas dtypes.
  * MASK will be converted to pd.BooleanDtype(), with `kd.present =&gt; True` and
    `kd.missing =&gt; pd.NA`.
  * All other dtypes (including a mixed DataSlice) will use the `object` dtype
    holding python data, with missing values represented through `pd.NA`.
    `kd.present` is converted to True.

Args:
  ds: DataSlice to convert.
  cols: list of columns to extract from DataSlice. If None all attributes will
    be extracted.
  include_self: whether to include the &#39;self_&#39; column. &#39;self_&#39; column is
    always included if `cols` is None and `ds` contains primitives/lists/dicts
    or it has ITEMID schema.

Returns:
  DataFrame with columns from DataSlice fields.</code></pre>

### `kd_ext.pdkd.from_dataframe(df_: DataFrame, as_obj: bool = False) -> DataSlice` {#kd_ext.pdkd.from_dataframe}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice from the given pandas DataFrame.

The DataFrame must have at least one column. It will be converted to a
DataSlice of entities/objects with attributes corresponding to the DataFrame
columns. Supported column dtypes include all primitive dtypes and ItemId.

If the DataFrame has MultiIndex, it will be converted to a DataSlice with
the shape derived from the MultiIndex.

When `as_obj` is set, the resulting DataSlice will be a DataSlice of objects
instead of entities.

The conversion adheres to:
* All missing values (according to `pd.isna`) become missing values in the
  resulting DataSlice.
* Data with `object` dtype is converted to an OBJECT DataSlice.
* Data with other dtypes is converted to a DataSlice with corresponding
  schema.

Args:
 df_: pandas DataFrame to convert.
 as_obj: whether to convert the resulting DataSlice to Objects.

Returns:
  DataSlice of items with attributes from DataFrame columns.</code></pre>

### `kd_ext.pdkd.from_series(series: Series) -> DataSlice` {#kd_ext.pdkd.from_series}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice from the given pandas Series.

The Series is first converted to a DataFrame with a single column named
&#39;self_&#39;, and then `from_dataframe` is used to convert it to a DataSlice.

Args:
  series: pandas Series to convert.

Returns:
  DataSlice representing the content of the Series.</code></pre>

### `kd_ext.pdkd.to_dataframe(ds: DataSlice, cols: list[str | Expr] | None = None, include_self: bool = False) -> DataFrame` {#kd_ext.pdkd.to_dataframe}

Alias for [kd_ext.pdkd.df](#kd_ext.pdkd.df) operator.

</section>

### kd_ext.persisted_data {#kd_ext.persisted_data}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Tools for persisted incremental data.</code></pre>


<section class="zippy open">

**Namespaces**

#### kd_ext.persisted_data.PersistedIncrementalDataBagManager {#kd_ext.persisted_data.PersistedIncrementalDataBagManager}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Manager of a DataBag that is assembled from multiple smaller bags.

Short version of the contract:
* Instances are not thread-safe.
* Multiple instances can be created for the same persistence directory:
  * Multiple readers are allowed.
  * The effects of write operations (calls to add_bags()) are not propagated
    to other instances that already exist.
  * Concurrent writers are not allowed. A write operation will fail if the
    state of the persistence directory was modified in the meantime by another
    instance.

It is often convenient to create a DataBag by incrementally adding smaller
bags, where each of the smaller bags is an update to the large DataBag.

This also provides the opportunity to persist the smaller bags separately,
along with the stated dependencies among the smaller bags.

Then at a later point, usually in a different process, one can reassemble the
large DataBag. But instead of loading the entire DataBag, one can load only
the smaller bags that are needed, thereby saving loading time and memory. In
fact the smaller bags can be loaded incrementally, so that decisions about
which bags to load can be made on the fly instead of up-front. In that way,
the incremental creation of the large DataBag is mirrored by its incremental
consumption.

To streamline the consumption, you have to specify dependencies between the
smaller bags when they are added. It is trivial to specify a linear chain of
dependencies, but setting up a dependency DAG is easy and can significantly
improve the loading time and memory usage of data consumers. In fact this
class will always manage a rooted DAG of small bags, and a chain of bags is
just a special case.

This class manages the smaller bags, which are named, and their
interdependencies. It also handles the persistence of the smaller bags along
with some metadata to facilitate the later consumption of the data and also
its further augmentation. The persistence uses a filesystem directory, which
is hermetic in the sense that it can be moved or copied (although doing so
will break branches if any exist - see the docstring of create_branch()). The
persistence directory is consistent after each public operation of this class,
provided that it is not modified externally and that there is sufficient space
to accommodate the writes.

This class is not thread-safe. When an instance is created for a persistence
directory that is already populated, then the instance is initialized with
the current state found in the persistence directory at that point in time.
Write operations (calls to add_bags()) by other instances for the same
persistence directory are not propagated to this instance. A write operation
will fail if the state of the persistence directory was modified in the
meantime by another instance.</code></pre>

<section class="zippy closed">

**Members**

### `PersistedIncrementalDataBagManager.__init__(self, persistence_dir: str, *, fs: FileSystemInterface | None = None)` {#PersistedIncrementalDataBagManager.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initializes the manager.

Args:
  persistence_dir: The directory where the small bags and metadata will be
    persisted. If it does not exist, or it is empty, then it will be
    populated with an empty bag named &#39;&#39;. Otherwise, the manager will be
    initialized from the existing artifacts in the directory.
  fs: All interactions with the file system will go through this instance.
    If None, then the default interaction with the file system is used.</code></pre>

### `PersistedIncrementalDataBagManager.add_bags(self, bags_to_add: list[BagToAdd])` {#PersistedIncrementalDataBagManager.add_bags}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Adds the given bags to the manager, which will persist them.

Conceptually, the items of bags_to_add are added one by one in the order
specified by the list. Each item of bags_to_add is a BagToAdd object with:
  - bag_name: The name of the bag to add. This must be a name that is not
    already present in get_available_bag_names() or any preceding item of
    bags_to_add.
  - bag: The DataBag to add.
  - dependencies: A non-empty collection of the names of the bags that `bag`
    depends on. It should include all the direct dependencies. There is no
    need to include transitive dependencies. All the names mentioned here
    must already be present in get_available_bag_names() or must be the name
    of some preceding item in bags_to_add.

The implementation does not simply add the bags one by one - internally it
persists them in parallel.

After this function returns, the bags and all their transitive dependencies
will be loaded and will hence be present in get_loaded_bag_names().

Args:
  bags_to_add: A list of bags to add. They are added in the order given by
    the list.</code></pre>

### `PersistedIncrementalDataBagManager.clear_cache(self)` {#PersistedIncrementalDataBagManager.clear_cache}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Clears the cache of loaded bags.

After this method returns, get_loaded_bag_names() will return only {&#39;&#39;},
i.e. only the initial empty bag with name &#39;&#39; will still be loaded.</code></pre>

### `PersistedIncrementalDataBagManager.create_branch(self, bag_names: Collection[str], *, with_all_dependents: bool = False, output_dir: str, fs: FileSystemInterface | None = None)` {#PersistedIncrementalDataBagManager.create_branch}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a branch of the current manager in a new persistence directory.

This function is very similar to extract_bags(), but it does not copy any of
the bags. Instead, it will simply point to the original files. A manager
for output_dir will therefore depend on the persistence directory of the
current manager, which should not be moved or deleted as long as output_dir
is used. After this function returns, the current manager and its branch
are independent: adding bags to the current manager will not affect the
branch, and similarly the branch won&#39;t affect the current manager.

To create a branch with all the bags managed by this manager, you can call
this function with the arguments bag_names=[&#39;&#39;], with_all_dependents=True.

Args:
  bag_names: The names of the bags that must be included in the branch. They
    must be a non-empty subset of get_available_bag_names(). The branch will
    also include their transitive dependencies.
  with_all_dependents: If True, then the branch will also include all the
    dependents of bag_names. The dependents are computed transitively. All
    the transitive dependencies of the dependents will also be included in
    the branch.
  output_dir: The directory in which the branch will be created. It must
    either be empty or not exist yet.
  fs: All interactions with the file system for output_dir will happen via
    this instance.</code></pre>

### `PersistedIncrementalDataBagManager.extract_bags(self, bag_names: Collection[str], *, with_all_dependents: bool = False, output_dir: str, fs: FileSystemInterface | None = None)` {#PersistedIncrementalDataBagManager.extract_bags}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Extracts the requested bags to the given output directory.

To extract all the bags managed by this manager, you can call this function
with the arguments bag_names=[&#39;&#39;], with_all_dependents=True.

Args:
  bag_names: The names of the bags that will be extracted. They must be a
    non-empty subset of get_available_bag_names(). The extraction will also
    include their transitive dependencies.
  with_all_dependents: If True, then the extracted bags will also include
    all dependents of bag_names. The dependents are computed transitively.
    All transitive dependencies of the dependents will also be included in
    the extraction.
  output_dir: The directory to which the bags will be extracted. It must
    either be empty or not exist yet.
  fs: All interactions with the file system for output_dir will happen via
    this instance.</code></pre>

### `PersistedIncrementalDataBagManager.get_available_bag_names(self) -> Set[str]` {#PersistedIncrementalDataBagManager.get_available_bag_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the names of all bags that are managed by this manager.

They include the initial empty bag (named &#39;&#39;), all bags that have been added
to this manager instance, and all bags that were already persisted in the
persistence directory before this manager instance was created.</code></pre>

### `PersistedIncrementalDataBagManager.get_loaded_bag(self) -> DataBag` {#PersistedIncrementalDataBagManager.get_loaded_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a bag consisting of all the small bags that are currently loaded.</code></pre>

### `PersistedIncrementalDataBagManager.get_loaded_bag_names(self) -> Set[str]` {#PersistedIncrementalDataBagManager.get_loaded_bag_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the names of all bags that are currently loaded in this manager.

The initial empty bag (with name &#39;&#39;) is always loaded, and the bags that
have been added to this manager instance or loaded by previous calls to
load_bags() and their transitive dependencies are also considered loaded.

Some methods, such as get_minimal_bag() or extract_bags(), may load bags as
a side effect when they are needed but not loaded yet.</code></pre>

### `PersistedIncrementalDataBagManager.get_minimal_bag(self, bag_names: Collection[str], *, with_all_dependents: bool = False) -> DataBag` {#PersistedIncrementalDataBagManager.get_minimal_bag}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a minimal bag that includes bag_names and all their dependencies.

Args:
  bag_names: The name of the bags whose data must be included in the result.
    It must be a non-empty subset of get_available_bag_names(). These bags
    and their transitive dependencies will be loaded if they are not loaded
    yet.
  with_all_dependents: If True, then the returned bag will also include all
    the dependents of bag_names. The dependents are computed transitively.
    All transitive dependencies of the dependents are then also included in
    the result.

Returns:
  A minimal bag that has the data of the requested small bags. It will not
  include any unrelated bags that are already loaded.</code></pre>

### `PersistedIncrementalDataBagManager.load_bags(self, bag_names: Collection[str], *, with_all_dependents: bool = False)` {#PersistedIncrementalDataBagManager.load_bags}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Loads the requested bags and their transitive dependencies.

Args:
  bag_names: The names of the bags that should be loaded. They must be a
    subset of get_available_bag_names(). All their transitive dependencies
    will be loaded as well.
  with_all_dependents: If True, then all the dependents of bag_names will
    also be loaded. The dependents are computed transitively. All transitive
    dependencies of the dependents will also be loaded.</code></pre>

</section>

#### kd_ext.persisted_data.fs_interface {#kd_ext.persisted_data.fs_interface}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface to interact with the file system.</code></pre>

<section class="zippy closed">

**Operators**

### `kd_ext.persisted_data.fs_interface.Collection(*args, **kwargs)` {#kd_ext.persisted_data.fs_interface.Collection}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A generic version of collections.abc.Collection.</code></pre>

### `kd_ext.persisted_data.fs_interface.FileSystemInterface()` {#kd_ext.persisted_data.fs_interface.FileSystemInterface}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface to interact with the file system.</code></pre>

### `kd_ext.persisted_data.fs_interface.IO()` {#kd_ext.persisted_data.fs_interface.IO}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Generic base class for TextIO and BinaryIO.

This is an abstract, generic version of the return of open().

NOTE: This does not distinguish between the different possible
classes (text vs. binary, read vs. write vs. read/write,
append-only, unbuffered).  The TextIO and BinaryIO subclasses
below capture the distinctions between text vs. binary, which is
pervasive in the interface; however we currently do not offer a
way to track the other distinctions in the type system.</code></pre>

</section>
</section>
<section class="zippy closed">

**Operators**

### `kd_ext.persisted_data.BagToAdd(bag_name: str, bag: DataBag, dependencies: tuple[str, ...])` {#kd_ext.persisted_data.BagToAdd}

<pre class="no-copy"><code class="lang-text no-auto-prettify">BagToAdd(bag_name: str, bag: koladata.types.data_bag.DataBag, dependencies: tuple[str, ...])</code></pre>

### `kd_ext.persisted_data.DataSliceManagerInterface()` {#kd_ext.persisted_data.DataSliceManagerInterface}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface for data slice managers.</code></pre>

### `kd_ext.persisted_data.DataSliceManagerView(manager: data_slice_manager_interface.DataSliceManagerInterface, path_from_root: data_slice_path_lib.DataSlicePath = DataSlicePath(''))` {#kd_ext.persisted_data.DataSliceManagerView}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A view of a DataSliceManager from a particular DataSlicePath.

This is a thin wrapper around a DataSliceManager and a DataSlicePath.

The DataSlicePath is called the &#34;view path&#34;. It is a full path, i.e. it is
relative to the root of the DataSliceManager.

The underlying DataSliceManager&#39;s state can be updated. As a result, a view
path can become invalid for the underlying manager. While the state of an
invalid view can still be extracted, e.g. via get_manager() and
get_path_from_root(), some of the methods, such as get_schema() and
get_data_slice(), will fail when is_view_valid() is False.

Implementation note: The method names should ideally contain verbs. That
should reduce the possible confusion between methods of the view and
attributes in the data, which are usually accessed as attributes, i.e. via
__getattr__(). For example, writing doc_view.title is more natural than
doc_view.get_attr(&#39;title&#39;), but if we define a method named &#39;title&#39; in this
class, then users cannot write doc_view.title anymore and will be forced to
write doc_view.get_attr(&#39;title&#39;). To avoid this, most of the methods start
with a verb, usually &#39;get&#39;, so we can use e.g. get_title() for the method.
Reducing the possibility for conflicts with data attributes is also the reason
why the is_view_valid() method is not simply called is_valid() or valid().</code></pre>

### `kd_ext.persisted_data.DataSlicePath(actions: tuple[DataSliceAction, ...])` {#kd_ext.persisted_data.DataSlicePath}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A data slice path.</code></pre>

### `kd_ext.persisted_data.FileSystemInteraction(options: Options | None = None)` {#kd_ext.persisted_data.FileSystemInteraction}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interactions with Google-internal file systems such as CNS.</code></pre>

### `kd_ext.persisted_data.PersistedIncrementalDataBagManager(persistence_dir: str, *, fs: FileSystemInterface | None = None)` {#kd_ext.persisted_data.PersistedIncrementalDataBagManager}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Manager of a DataBag that is assembled from multiple smaller bags.

Short version of the contract:
* Instances are not thread-safe.
* Multiple instances can be created for the same persistence directory:
  * Multiple readers are allowed.
  * The effects of write operations (calls to add_bags()) are not propagated
    to other instances that already exist.
  * Concurrent writers are not allowed. A write operation will fail if the
    state of the persistence directory was modified in the meantime by another
    instance.

It is often convenient to create a DataBag by incrementally adding smaller
bags, where each of the smaller bags is an update to the large DataBag.

This also provides the opportunity to persist the smaller bags separately,
along with the stated dependencies among the smaller bags.

Then at a later point, usually in a different process, one can reassemble the
large DataBag. But instead of loading the entire DataBag, one can load only
the smaller bags that are needed, thereby saving loading time and memory. In
fact the smaller bags can be loaded incrementally, so that decisions about
which bags to load can be made on the fly instead of up-front. In that way,
the incremental creation of the large DataBag is mirrored by its incremental
consumption.

To streamline the consumption, you have to specify dependencies between the
smaller bags when they are added. It is trivial to specify a linear chain of
dependencies, but setting up a dependency DAG is easy and can significantly
improve the loading time and memory usage of data consumers. In fact this
class will always manage a rooted DAG of small bags, and a chain of bags is
just a special case.

This class manages the smaller bags, which are named, and their
interdependencies. It also handles the persistence of the smaller bags along
with some metadata to facilitate the later consumption of the data and also
its further augmentation. The persistence uses a filesystem directory, which
is hermetic in the sense that it can be moved or copied (although doing so
will break branches if any exist - see the docstring of create_branch()). The
persistence directory is consistent after each public operation of this class,
provided that it is not modified externally and that there is sufficient space
to accommodate the writes.

This class is not thread-safe. When an instance is created for a persistence
directory that is already populated, then the instance is initialized with
the current state found in the persistence directory at that point in time.
Write operations (calls to add_bags()) by other instances for the same
persistence directory are not propagated to this instance. A write operation
will fail if the state of the persistence directory was modified in the
meantime by another instance.</code></pre>

### `kd_ext.persisted_data.PersistedIncrementalDataSliceManager(*, internal_call: object, persistence_dir: str, fs: fs_interface.FileSystemInterface, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface, data_bag_manager: dbm.PersistedIncrementalDataBagManager, schema_bag_manager: dbm.PersistedIncrementalDataBagManager, schema_helper: schema_helper_lib.SchemaHelper, initial_schema_node_name_to_data_bag_names: kd.types.DataSlice, schema_node_name_to_data_bags_updates_manager: dbm.PersistedIncrementalDataBagManager, metadata: metadata_pb2.PersistedIncrementalDataSliceManagerMetadata)` {#kd_ext.persisted_data.PersistedIncrementalDataSliceManager}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Manager of a DataSlice that is assembled from multiple smaller data slices.

Short version of the contract:
* Instances are not thread-safe.
* Multiple instances can be created for the same persistence directory:
  * Multiple readers are allowed.
  * The effects of write operations (calls to update()) are not propagated
    to other instances that already exist.
  * Concurrent writers are not allowed. A write operation will fail if the
    state of the persistence directory was modified in the meantime by another
    instance.

It is often convenient to create a DataSlice by incrementally adding smaller
slices, where each of the smaller slices is an update to the large DataSlice.
This also provides the opportunity to persist the updates separately.
Then at a later point, usually in a different process, one can reassemble the
large DataSlice. But instead of loading the entire DataSlice, one can load
only the updates (parts) that are needed, thereby saving loading time and
memory. In fact the updates can be loaded incrementally, so that decisions
about which ones to load can be made on the fly instead of up-front. In that
way, the incremental creation of the large DataSlice is mirrored by the
incremental consumption of its subslices.

This class manages the DataSlice and its incremental updates. It also handles
the persistence of the updates along with some metadata to facilitate the
later consumption of the data and also its further augmentation. The
persistence uses a filesystem directory, which is hermetic in the sense that
it can be moved or copied (although doing so will break branches if any
exist - see the docstring of branch()). The persistence directory is
consistent after each public operation of this class, provided that it is not
modified externally and that there is sufficient space to accommodate the
writes.

This class is not thread-safe. When an instance is created for a persistence
directory that is already populated, then the instance is initialized with
the current state found in the persistence directory at that point in time.
Write operations (calls to update()) by other instances for the same
persistence directory are not propagated to this instance. A write operation
will fail if the state of the persistence directory was modified in the
meantime by another instance. Multiple instances can be created for the same
persistence directory and concurrently read from it. So creating multiple
instances and calling get_schema() or get_data_slice() concurrently is fine.

Implementation details:

The manager indexes each update DataBag with the schema node names for which
the update can possibly provide data. When a user requests a subslice,
the manager consults the index and asks the bag manager to load all the needed
updates (data bags).</code></pre>

</section>

### kd_ext.vis {#kd_ext.vis}

Koda visualization functionality.

<section class="zippy closed">

**Operators**

### `kd_ext.vis.AccessType(*values)` {#kd_ext.vis.AccessType}
Aliases:

- [kd_g3_ext.vis.AccessType](#kd_g3_ext.vis.AccessType)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Types of accesses that can appear in an access path.</code></pre>

### `kd_ext.vis.DataSliceVisOptions(num_items: int = 48, unbounded_type_max_len: int = 256, detail_width: int | str | None = None, detail_height: int | str | None = 300, attr_limit: int | None = 20, item_limit: int | None = 20)` {#kd_ext.vis.DataSliceVisOptions}
Aliases:

- [kd_g3_ext.vis.DataSliceVisOptions](#kd_g3_ext.vis.DataSliceVisOptions)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Options for visualizing a DataSlice.</code></pre>

### `kd_ext.vis.DescendMode(*values)` {#kd_ext.vis.DescendMode}
Aliases:

- [kd_g3_ext.vis.DescendMode](#kd_g3_ext.vis.DescendMode)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Create a collection of name/value pairs.

Example enumeration:

&gt;&gt;&gt; class Color(Enum):
...     RED = 1
...     BLUE = 2
...     GREEN = 3

Access them by:

- attribute access:

  &gt;&gt;&gt; Color.RED
  &lt;Color.RED: 1&gt;

- value lookup:

  &gt;&gt;&gt; Color(1)
  &lt;Color.RED: 1&gt;

- name lookup:

  &gt;&gt;&gt; Color[&#39;RED&#39;]
  &lt;Color.RED: 1&gt;

Enumerations can be iterated over, and know how many members they have:

&gt;&gt;&gt; len(Color)
3

&gt;&gt;&gt; list(Color)
[&lt;Color.RED: 1&gt;, &lt;Color.BLUE: 2&gt;, &lt;Color.GREEN: 3&gt;]

Methods can be added to enumerations, and members can have their own
attributes -- see the documentation for details.</code></pre>

### `kd_ext.vis.register_formatters()` {#kd_ext.vis.register_formatters}
Aliases:

- [kd_g3_ext.vis.register_formatters](#kd_g3_ext.vis.register_formatters)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Register DataSlice visualization in IPython.</code></pre>

### `kd_ext.vis.visualize_slice(ds: DataSlice, options: DataSliceVisOptions | None = None) -> _DataSliceViewState` {#kd_ext.vis.visualize_slice}
Aliases:

- [kd_g3_ext.vis.visualize_slice](#kd_g3_ext.vis.visualize_slice)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Visualizes a DataSlice as a html widget.</code></pre>

</section>

### kd_ext.kv {#kd_ext.kv}

Experimental Koda View API.


<section class="zippy open">

**Namespaces**

#### kd_ext.kv.View {#kd_ext.kv.View}

`View` class

<section class="zippy closed">

**Members**

### `View.__init__(self, obj: Any, depth: int, internal_call: object, /)` {#View.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Internal constructor. Please use kv.view() instead.</code></pre>

### `View.append(self, value: ViewOrAutoBoxType)` {#View.append}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Appends an item or items to all containers in the view.</code></pre>

### `View.collapse(self, ndim: int = 1) -> View` {#View.collapse}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Collapses equal items along the specified number dimensions of the view.</code></pre>

### `View.expand_to(self, other: ViewOrAutoBoxType, ndim: int = 0) -> View` {#View.expand_to}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Expands the view to the shape of other view.</code></pre>

### `View.explode(self, ndim: int = 1) -> View` {#View.explode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unnests iterable elements, increasing rank by `ndim`.</code></pre>

### `View.flatten(self, from_dim: int = 0, to_dim: int | None = None) -> View` {#View.flatten}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Flattens the specified dimensions of the view.</code></pre>

### `View.get(self) -> Any` {#View.get}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an object represented by the view.

Example:
  view(&#39;foo&#39;).get()
  # &#39;foo&#39;
  view([[1,2],[3]])[:].get()
  # ([1,2],[3]).
  view([[1,2],[3]])[:][:].get()
  # ((1,2),(3,)).</code></pre>

### `View.get_attr(self, attr_name: str, default: Any = NO_DEFAULT) -> View` {#View.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new view with the given attribute of each item.</code></pre>

### `View.get_depth(self) -> int` {#View.get_depth}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the depth of the view.</code></pre>

### `View.get_item(self, key_or_index: ViewOrAutoBoxType | slice) -> View` {#View.get_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an item or items from the given view containing containers.</code></pre>

### `View.group_by(self, *args: ViewOrAutoBoxType, sort: bool = False) -> View` {#View.group_by}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Groups items by the values of the given args.</code></pre>

### `View.implode(self, ndim: int = 1) -> View` {#View.implode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces view dimension by grouping items into tuples.</code></pre>

### `View.inverse_select(self, fltr: ViewOrAutoBoxType) -> View` {#View.inverse_select}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Restores the original shape that was reduced by select.</code></pre>

### `View.map(self, f: Callable[[Any], Any], *, ndim: int = 0, include_missing: bool | None = None) -> View` {#View.map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies a function to every item in the view.

If `ndim=0`, then the function is applied to the items of the view.
If `ndim=1`, then the function is applied to tuples of items of the
view corresponding to the last dimension. If `ndim=2`, then the function
is applied to tuples of tuples, and so on. The depth of the result is
therefore decreased by `ndim` compared to the depth of `self`.

Example:
  view([1, None, 2])[:].map(lambda x: x * 2).get()
  # (2, None, 4)
  view([1, None, 2]).map(lambda x: x * 2).get()
  # [1, None, 2, 1, None, 2]
  # We have used &#34;*&#34; operator on the list [1, None, 2] and the integer 2.
  view([1, None, 2])[:].map(lambda x: x * 2, ndim=1).get()
  # (1, None, 2, 1, None, 2)
  # Here we have used &#34;*&#34; operator on the tuple (1, None, 2) and the integer
  # 2.

Args:
  f: The function to apply.
  ndim: Dimensionality of items to pass to `f`, must be less or equal to the
    depth of the view.
  include_missing: Specifies whether `f` applies to all items (`=True`) or
    only to present items (`=False`, valid only when `ndim=0`); defaults to
    `False` when `ndim=0`.

Returns:
  A new view with the function applied to every item.</code></pre>

### `View.select(self, fltr: ViewOrAutoBoxType, expand_filter: bool = True) -> View` {#View.select}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Keeps only items in the view where the filter is present.</code></pre>

### `View.set_attr(self, attr_name: str, value: ViewOrAutoBoxType)` {#View.set_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets the given attribute of each item.</code></pre>

### `View.set_item(self, key_or_index: ViewOrAutoBoxType, value: ViewOrAutoBoxType)` {#View.set_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets an item or items for all containers in the view.</code></pre>

### `View.take(self, index: ViewOrAutoBoxType) -> View` {#View.take}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view with the given index in the last dimension.</code></pre>

</section>
</section>
<section class="zippy closed">

**Operators**

### `kd_ext.kv.align(*args: ViewOrAutoBoxType) -> tuple[View, ...]` {#kd_ext.kv.align}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Aligns the views to a common shape.

We will also apply auto-boxing if some inputs are not views but can be
automatically boxed into one.

Args:
  *args: The views to align, or values that can be automatically boxed into
    views.

Returns:
  A tuple of aligned views, of size len(others) + 1.</code></pre>

### `kd_ext.kv.append(v: View | int | float | str | bytes | bool | _Present | None, value: View | int | float | str | bytes | bool | _Present | None)` {#kd_ext.kv.append}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Appends an item or items to all containers in the view.

This essentially calls `x.append(y) for x, y in `zip(v, value)`, but with
additions:
- when `value` is a view or auto-boxable into a view, we first align all
  arguments.
- if `x` is None, we skip appending the item.

If the same list object appears multiple times in `v`, or `v` has lower depth
than `value`, we will append all corresponding values in order. Where in
Python one would call `list1.extend(list2)`, we can achieve the same effect
with `kv.append(kv.view(list1), kv.view(list2)[:])`.

Example:
  x = [[], [1]]
  kv.append(kv.view(x)[:], kv.view([10, 20])[:])
  # x is now [[10], [1, 20]]
  kv.append(kv.view(x)[:], kv.view([[30, 40], [50]])[:][:])
  # x is now [[10, 30, 40], [1, 20, 50]]
  kv.append(kv.view(x)[:], kv.view(None))
  # x is now [[10, 30, 40, None], [1, 20, 50, None]]

Args:
  v: The view containing the lists to append items to.
  value: The value to append.</code></pre>

### `kd_ext.kv.apply_mask(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.apply_mask}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a &amp; b`.

Returns the values from `a` where `b` is present, and None otherwise.

Args:
  a: The view to apply the mask to.
  b: The mask to apply. Must only have `kv.present` and `None` values.

Returns:
  A new view with only the requested values from `a`.</code></pre>

### `kd_ext.kv.coalesce(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.coalesce}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a | b`.

Returns the values from `a` where they are present, or the values from `b`
otherwise.

Args:
  a: The view to coalesce.
  b: The view to coalesce with.

Returns:
  A new view with the values from `a` and `b` combined.</code></pre>

### `kd_ext.kv.collapse(v: View | int | float | str | bytes | bool | _Present | None, ndim: int = 1) -> View` {#kd_ext.kv.collapse}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Collapses equal items along the specified number dimensions of the view.

Example:
  x = kv.view([[1, 1, None, 1], [2, 3], []])[:]
  kv.collapse(x).get()
  # (1, None, None)
  kv.collapse(x, ndim=2).get()
  # None

Args:
  v: The view to collapse.
  ndim: The number of dimensions to collapse.

Returns:
  A new view with `ndim` fewer dimensions. The value of each item is equal
  to the value of its uncollapsed items if they are the same, or None
  otherwise.</code></pre>

### `kd_ext.kv.equal(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a == b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are equal and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.expand_to(v: View | int | float | str | bytes | bool | _Present | None, other: View | int | float | str | bytes | bool | _Present | None, ndim: int = 0) -> View` {#kd_ext.kv.expand_to}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the view expanded to the shape of other view.

The view must have dimensions that match a prefix of the other view&#39;s
dimensions. The corresponding items then will be repeated among the additional
dimensions.

When `ndim` is set, the expansion is performed in 3 steps:
1) the last N dimensions of `v` are first imploded into tuples
2) the expansion operation is performed on the View of those tuples
3) the tuples in the expanded View are exploded

Example:
  x = kv.view([1, None, 2])[:]
  y = kv.view([[], [1, None], [3, 4, 5]])[:][:]
  kv.expand_to(x, y).get()
  # ((), (None, None), (2, 2, 2))
  kv.expand_to(x, y, ndim=1).get()
  # ((), ((1, None, 2), (1, None, 2)), ((1, None, 2), (1, None, 2), (1, None,
  # 2)))

Args:
  v: The view to expand.
  other: The view to expand to.
  ndim: the number of dimensions to implode before expansion and explode back
    afterwards.</code></pre>

### `kd_ext.kv.explode(v: View | int | float | str | bytes | bool | _Present | None, ndim: int = 1) -> View` {#kd_ext.kv.explode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unnests iterable elements, increasing rank by `ndim`.

If a view contains iterable elements, `explode` with `ndim=1` creates a new
view containing elements from those iterables, and increases view rank by 1.
This is useful for &#34;diving&#34; into lists within your data structure.
Usually used via `[:]`.

`ndim=2` applies the same transformation twice, and so on.

It is user&#39;s responsibility to ensure that all items are iterable and
have `len`.

If one of the items is None, it will be treated as an empty iterable,
instead of raising an error that len() would raise.

Example:
  x = kv.view(types.SimpleNamespace(a=[1, 2]))
  kv.explode(x).map(lambda i: i + 1).get()
  # (2, 3)

Args:
  v: The view to explode.
  ndim: The number of dimensions to explode. Must be non-negative.

Returns:
  A new view with `ndim` more dimensions.</code></pre>

### `kd_ext.kv.flatten(v: View | int | float | str | bytes | bool | _Present | None, from_dim: int = 0, to_dim: int | None = None) -> View` {#kd_ext.kv.flatten}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Flattens the specified dimensions of the view.

Indexing works as in Python:
* If `to_dim` is unspecified, `to_dim = get_depth()` is used.
* If `to_dim &lt; from_dim`, `to_dim = from_dim` is used.
* If `to_dim &lt; 0`, `max(0, to_dim + get_depth())` is used. The same goes for
  `from_dim`.
* If `to_dim &gt; get_depth()`, `get_depth()` is used. The same goes for
`from_dim`.

The above-mentioned adjustments places both `from_dim` and `to_dim` in the
range `[0, get_depth()]`. After adjustments, the new View has `get_depth() ==
old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
&#34;unit&#34; dimension is inserted at `from_dim`.

Note that this does not look into the objects stored at the leaf level,
so even if they are tuples or lists themselves, they will not be flattened.

Example:
  x = kv.view([[1, 2], [3]])
  kv.flatten(x[:][:]).get()
  # (1, 2, 3)
  kv.flatten(x[:]).get()
  # ([1, 2], [3])
  kv.flatten(x).get()
  # ([[1, 2], [3]],)
  kv.flatten(x[:][:], 1).get()
  # ((1, 2), (3,))
  kv.flatten(x[:][:], -1).get()
  # ((1, 2), (3,))
  kv.flatten(x[:][:], 2).get()
  # (((1,), (2,)), ((3,),))
  kv.flatten(x[:][:], 1, 1).get()
  # (((1,), (2,)), ((3,),))

Args:
  v: The view to flatten. Can also be a Python primitive, which will be
    automatically boxed into a view.
  from_dim: The dimension to start flattening from.
  to_dim: The dimension to end flattening at, or None to flatten until the
    last dimension.

Returns:
  A new view with the specified dimensions flattened.</code></pre>

### `kd_ext.kv.get_attr(v: View | int | float | str | bytes | bool | _Present | None, attr_name: str, default: Any = NO_DEFAULT) -> View` {#kd_ext.kv.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new view with the given attribute of each item.

If one of the items is None, the corresponding value will be None as well,
instead of raising an error that Python&#39;s built-in getattr() would raise.

Example:
  x = kv.view(types.SimpleNamespace(_b=6))
  kv.get_attr(x, &#39;_b&#39;).get()
  # 6

Args:
  v: The view to get the attribute from.
  attr_name: The name of the attribute to get.
  default: When specified, if the attribute value is None or getting the
    attribute raises AttributeError, this value will be used instead.</code></pre>

### `kd_ext.kv.get_item(v: View | int | float | str | bytes | bool | _Present | None, key_or_index: View | int | float | str | bytes | bool | _Present | None | slice) -> View` {#kd_ext.kv.get_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns an item or items from the given view containing containers.

This essentially calls `[x[y] for x, y in zip(v, key_or_index)]`, but
with some additions:
- when `key_or_index` is a slice (`v[a:b]` syntax), we add a new
  dimension to the resulting view that corresponds to iterating over the
  requested range of indices.
- when `key_or_index` is a view or auto-boxable into a view, we first align
  it with `v`. See the examples below for more details.
- if x[y] raises IndexError or KeyError, we catch it and return None for
  that item instead.

Example:
  x = [
      types.SimpleNamespace(
        a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
      ),
      types.SimpleNamespace(
        a=[types.SimpleNamespace(b=3)]
      ),
  ]
  kv.get_item(kv.get_item(kv.view(x), slice(None)).a, slice(None)).b.get()
  # ((1, 2), (3,))
  # Shorter syntax for the same result:
  kv.view(x)[:].a[:].b.get()
  # ((1, 2), (3,))
  kv.view(x)[:].a[:-1].b.get()
  # ((1,), ())
  # Get the second element from each list (`key_or_index` is expanded to `v`):
  kv.view(x)[:].a[2].b.get()
  # (2, None)

  y = [{&#39;a&#39;: 1, &#39;b&#39;: 2}, {&#39;a&#39;: 3, &#39;c&#39;: 4}]
  # Get the value for &#39;a&#39; from each dict (`key_or_index` is expanded to `v`):
  kv.get_item(kv.view(y)[:], &#39;a&#39;).get()
  # (1, 3)
  kv.get_item(kv.view(y)[:], &#39;c&#39;).get()
  # (None, 4)
  # Get the value for the corresponding key from each dict (`key_or_index` has
  # same shape as `v`):
  kv.get_item(kv.view(y)[:], kv.view([&#39;b&#39;, &#39;c&#39;])[:]).get()
  # (2, 4)
  # Get the value for multiple keys from each dict (`v` is expanded to
  # `key_or_index`):
  kv.get_item(kv.view(y)[:],
              kv.view([[&#39;b&#39;, &#39;a&#39;], [&#39;a&#39;, &#39;b&#39;, &#39;c&#39;]])[:][:]).get()
  # ((2, 1), (3, None, 4))

Args:
  v: The view containing the collections to get items from.
  key_or_index: The key or index or a slice or indices to get.</code></pre>

### `kd_ext.kv.greater(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.greater}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a &gt; b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are greater and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.greater_equal(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.greater_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a &gt;= b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are greater or equal and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.group_by(v: View | int | float | str | bytes | bool | _Present | None, *keys: View | int | float | str | bytes | bool | _Present | None, sort: bool = False) -> View` {#kd_ext.kv.group_by}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `v` with values in last dimension grouped using a new dimension.

The resulting View has depth increased by 1. The first `v.get_depth() - 1`
dimensions are unchanged. The last two dimensions correspond to the groups
and the items within the groups. Elements within the same group are ordered by
the appearance order in `v`.

`keys` are used for the grouping keys. If length of `keys` is greater than 1,
the key is a tuple. If `keys` is empty, the key is `v`.

If sort=True groups are ordered by the grouping key, otherwise groups are
ordered by the appearance of the first object in the group.

Example 1:
  v: kv.view([1, 3, 2, 1, 2, 3, 1, 3])[:]
  result: kv.view([[1, 1, 1], [3, 3, 3], [2, 2]])[:][:]

Example 2:
  v: kv.view([1, 3, 2, 1, 2, 3, 1, 3])[:], sort=True
  result: kv.view([[1, 1, 1], [2, 2], [3, 3, 3]])[:][:]

Example 3:
  v: kv.view([[1, 2, 1, 3, 1, 3], [1, 3, 1]])[:][:]
  result: kv.view([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])[:][:][:]

Example 4:
  v: kv.view([1, 3, 2, 1, None, 3, 1, None])[:]
  result: kv.view([[1, 1, 1], [3, 3], [2]])[:][:]

  Missing values are not listed in the result.

Example 5:
  v:    kv.view([1, 2, 3, 4, 5, 6, 7, 8])[:],
  key1: kv.view([7, 4, 0, 9, 4, 0, 7, 0])[:],
  result: kv.view([[1, 7], [2, 5], [3, 6, 8], [4]])[:][:]

  When *keys is present, `v` is not used for the key.

Example 6:
  v:    kv.view([1, 2, 3, 4, None, 6, 7, 8])[:],
  key1: kv.view([7, 4, 0, 9, 4,    0, 7, None])[:],
  result: kv.view([[1, 7], [2, None], [3, 6], [4]])[:][:]

  Items with missing key are not listed in the result.
  Missing `v` values are missing in the result.

Example 7:
  v:    kv.view([ 1,   2,   3,   4,   5,   6,   7,   8])[:],
  key1: kv.view([ 7,   4,   0,   9,   4,   0,   7,   0])[:],
  key2: kv.view([&#39;A&#39;, &#39;D&#39;, &#39;B&#39;, &#39;A&#39;, &#39;D&#39;, &#39;C&#39;, &#39;A&#39;, &#39;B&#39;])[:],
  result: kv.view([[1, 7], [2, 5], [3, 8], [4], [6]])[:][:]

  When *keys has two or more values, the key is a tuple.
  In this example we have the following groups:
  (7, &#39;A&#39;), (4, &#39;D&#39;), (0, &#39;B&#39;), (9, &#39;A&#39;), (0, &#39;C&#39;)

Args:
  v: the view to group.
  *keys: the keys to group by. All views must have the same shape as `v`.
    Scalar views are not supported. If not present, `v` is used as the key.
  sort: Whether groups in the result should be ordered by the grouping key.

Returns:
  A view with items within the last dimension reordered into groups and
  injected grouped by dimension.</code></pre>

### `kd_ext.kv.implode(v: View | int | float | str | bytes | bool | _Present | None, ndim: int = 1) -> View` {#kd_ext.kv.implode}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reduces view dimension by grouping items into tuples.

This is an inverse operation to `explode`. It groups items into tuples
according to the shape of topmost `ndim` dimensions. If `ndim` is negative,
will implode all the way to a scalar.

Example:
  view_2d = kv.view([[1,2],[3]])[:][:]
  kv.implode(view_2d)
  # The same structure as view([(1,2),(3,)])[:].
  kv.implode(view_2d, ndim=2)
  kd.implode(view_2d, ndim=-1)
  # The same structure as view(((1,2),(3,))).

Args:
  v: The view to implode.
  ndim: The number of dimensions to implode.

Returns:
  A new view with `ndim` fewer dimensions.</code></pre>

### `kd_ext.kv.inverse_select(v: View | int | float | str | bytes | bool | _Present | None, fltr: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.inverse_select}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a view by putting items in `v` to present positions in `fltr`.

The depth of `v` and `fltr` must be the same.
The number of items in `v` must be equal to the number of present items in
`fltr`.

Example:
  v = kv.view([[1, None], [2]])[:][:]
  fltr = kv.view([[None, kv.present, kv.present], [kv.present, None]])[:][:]
  kv.inverse_select(v, fltr).get()
  # ((None, 1, None), (2, None))

The most common use case of inverse_select is to restore the shape of the
original view after applying select and performing some operations on
the subset of items in the original view. E.g.
  a = kv.view(...)
  fltr = a &gt; 0
  filtered_v = kv.select(a, fltr)
  # do something on filtered_v
  a = kv.inverse_select(filtered_v, fltr) | a

Args:
  v: view to be inverse filtered.
  fltr: filter view with values kv.present or None.

Returns:
  Inverse filtered view.</code></pre>

### `kd_ext.kv.less(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.less}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a &lt; b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are less and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.less_equal(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.less_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a &lt;= b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are less or equal and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.map(f: Callable[..., Any], *args: ViewOrAutoBoxType, ndim: int = 0, include_missing: bool | None = None, **kwargs: ViewOrAutoBoxType) -> View` {#kd_ext.kv.map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies a function to corresponding items in the args/kwargs view.

Arguments will be broadcasted to a common shape. There must be at least one
argument or keyword argument.

The `ndim` argument controls how many dimensions should be passed to `f` in
each call. If `ndim = 0` then the items of the corresponding view will be
passed, if `ndim = 1` then python tuples of items corresponding
to the last dimension will be passed, if `ndim = 2` then tuples of tuples,
and so on.

Example:
  x = types.SimpleNamespace(
      a=[types.SimpleNamespace(b=1), types.SimpleNamespace(b=2)]
  )
  kv.map(lambda i: i + 1, kv.view(x).a[:].b).get()
  # (2, 3)
  kv.map(lambda x: x + y, kv.view(x).a[:].b, kv.view(1)).get()
  # (2, 3)
  kv.map(lambda i: i + i, kv.view(x).a[:].b, ndim=1).get()
  # (1, 2, 1, 2)

Args:
  f: The function to apply.
  *args: The positional arguments to pass to the function. They must all be
    views or auto-boxable into views.
  ndim: Dimensionality of items to pass to `f`.
  include_missing: Specifies whether `f` applies to all items (`=True`) or
    only to items present in all `args` and `kwargs` (`=False`, valid only
    when `ndim=0`); defaults to `False` when `ndim=0`.
  **kwargs: The keyword arguments to pass to the function. They must all be
    views or auto-boxable into views.

Returns:
  A new view with the function applied to the corresponding items.</code></pre>

### `kd_ext.kv.not_equal(a: View | int | float | str | bytes | bool | _Present | None, b: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.not_equal}

<pre class="no-copy"><code class="lang-text no-auto-prettify">An alias for `a != b`.

Compares the items of `a` and `b` and returns a view with `kv.present` if the
corresponding items are not equal and `None` otherwise.

Args:
  a: The view to compare.
  b: The view to compare with.

Returns:
  A new view with the comparison result.</code></pre>

### `kd_ext.kv.select(v: View | int | float | str | bytes | bool | _Present | None, fltr: View | int | float | str | bytes | bool | _Present | None, expand_filter: bool = True) -> View` {#kd_ext.kv.select}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new view by filtering out items where filter is not present.

The dimensions of `fltr` needs to be compatible with the dimensions of `v`.
By default, `fltr` is expanded to &#39;v&#39; and items in `v` corresponding to
missing items in `fltr` are removed. The last dimension of the resulting
view is changed while the first N-1 dimensions are the same as those in
`v`.

Example:
  val = kv.view([[1, None, 4], [None], [2, 8]])[:][:]
  kv.select(val, val &gt; 3).get()
  # ((4,), (), (8,))
  fltr = kv.view(
      [[None, kv.present, kv.present], [kv.present], [kv.present, None]]
  )[:][:]
  kv.select(val, fltr).get()
  # ((None, 4), (None,), (2))

  fltr = kv.view([kv.present, kv.present, None])[:]
  kv.select(val, fltr)
  # ((1, None, 4), (None,), ())
  kv.select(val, fltr, expand_filter=False)
  # ((1, None, 4), (None,))

Args:
  v: View with depth &gt; 0 to be filtered.
  fltr: filter view with values kv.present or None.
  expand_filter: flag indicating if the &#39;fltr&#39; should be expanded to &#39;v&#39;. When
    False, we will remove items at the level of `fltr`.

Returns:
  Filtered view.</code></pre>

### `kd_ext.kv.set_attr(v: View | int | float | str | bytes | bool | _Present | None, attr_name: str, value: View | int | float | str | bytes | bool | _Present | None)` {#kd_ext.kv.set_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets the given attribute of each item.

If one of the items in `v` is None, the corresponding value will be ignored.
If one of the items in `value` is None, the attribute of the corresponding
item will be set to None.

If the same object has multiple references in `v`, we will process the
set_attr in order, so the attribute will have the last assigned value.

Example:
  o = kv.view([types.SimpleNamespace(), types.SimpleNamespace()])[:]
  kv.set_attr(o, &#39;a&#39;, 1)
  kv.set_attr(o, &#39;_b&#39;, kv.view([None, 2])[:])
  o.get()
  # (namespace(a=1, _b=None), namespace(a=1, _b=2))

Args:
  v: The view to set the attribute for.
  attr_name: The name of the attribute to set.
  value: The value to set the attribute to. Can also be a Python primitive,
    which will be automatically boxed into a view.</code></pre>

### `kd_ext.kv.set_item(v: View | int | float | str | bytes | bool | _Present | None, key_or_index: View | int | float | str | bytes | bool | _Present | None, value: View | int | float | str | bytes | bool | _Present | None)` {#kd_ext.kv.set_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets an item or items for all containers in the view.

This essentially calls `(x[y] = z) for x, y, z in zip(v, key_or_index,
value)`, but with additions:
- when `key_or_index` or `value` are views or auto-boxable into a view, we
  first align all arguments.
- if `x` is None or `y` is None, we skip setting the item.
- if `x[y] = z` raises IndexError, we catch it and ignore it.

If the same (object, key) pair appears multiple times, we will process the
assignments in order, so the attribute will have the last assigned value.

Example:
  x = [{}, {&#39;a&#39;: 1}]
  kv.set_item(kv.view(x)[:], &#39;a&#39;, kv.view([10, 20])[:])
  # x is now [{&#39;a&#39;: 10}, {&#39;a&#39;: 20}]

Args:
  v: The view containing the collections to set items in.
  key_or_index: The key or index to set.
  value: The value to set.</code></pre>

### `kd_ext.kv.take(v: View | int | float | str | bytes | bool | _Present | None, index: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.take}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view with the items at the given index in the last dimension.

This is a shortcut for `kv.get_item(kv.implode(v), index)`. This also implies
the broadcasting behavior, for example `index` must have compatible shape with
`kv.implode(v)`.

Example:
  x = kv.view([1, 2, 3])[:]
  kv.take(x, 1).get()
  # 2
  kv.take(x, -1).get()
  # 3
  kv.take(x, kv.view([1, 2, 3, 4])[:]).get()
  # (2, 3, None, None)

Args:
  v: The view to take the index from. It must have at least one dimension.
  index: The index in the last dimension of `v` to take the item from.</code></pre>

### `kd_ext.kv.view(obj: Any) -> View` {#kd_ext.kv.view}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a view on an object that can be used for vectorized access.

A view represents traversing a particular path in a tree represented
by the object, with the leaves of that path being the items in the view,
and the structure of that path being the shape of the view.

Note that when we traverse a path, sometimes we branch when there are several
uniform edges, such as when using the `view[:]` API. So in formal terms what
we call a `path` is actually a subtree of the tree.

For example, consider the following set of objects:

x = Obj(d=3)
y = Obj(d=4)
z = [x, y]
w = Obj(b=1, c=z)

Object w can be represented as the following tree:

w --b--&gt; 1
  --c--&gt; z --item0--&gt; x --d--&gt; 3
           --item1--&gt; y --d--&gt; 4

Now view(w) corresponds to just the root of this tree. view(w).c corresponds
to traversing edge labeled with c to z. view(w).c[:] corresponds to traversing
the edges labeled with item0 and item1 to x and y respectively. view(w).c[:].d
corresponds to traversing the edges labeled with d to 3 and 4.

We call the leaf nodes of this path traversal the items of the view, and
the number of branches used to get to them the depth of the view.

For example, for view(w).c[:].d, its items are 3 and 4, and its depth is 1.

Example:
  view([1, 2])[:].map(lambda x: x + 1).get()
  # (2, 3)
  view([[1, 2], [3]])[:].map(lambda x: len(x)).get()
  # (2, 1)

Args:
  obj: An arbitrary object to create a view for.

Returns:
  A scalar view on the object.</code></pre>

</section>
</section>
<section class="zippy closed">

**Operators**

### `kd_ext.Fn(f: Any, *, use_tracing: bool = True, **kwargs: Any) -> DataSlice` {#kd_ext.Fn}

Alias for [kd.functor.fn](#kd.functor.fn) operator.

### `kd_ext.PyFn(f: Callable[..., Any], *, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **defaults: Any) -> DataSlice` {#kd_ext.PyFn}

Alias for [kd.functor.py_fn](#kd.functor.py_fn) operator.

### `kd_ext.py_cloudpickle(obj: Any) -> PyObject` {#kd_ext.py_cloudpickle}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Wraps into a Arolla QValue using cloudpickle for serialization.</code></pre>

</section>

## DataSlice {#DataSlice}

`DataSlice` represents a jagged array of items (i.e. primitive values, ItemIds).

<section class="zippy closed">

**Members**

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
Aliases:

- [DataItem.append](#DataItem.append)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Append a value to each list in this DataSlice</code></pre>

### `DataSlice.clear()` {#DataSlice.clear}
Aliases:

- [DataItem.clear](#DataItem.clear)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Clears all dicts or lists in this DataSlice</code></pre>

### `DataSlice.clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.clone}
Aliases:

- [DataItem.clone](#DataItem.clone)

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

### `DataSlice.deep_clone(self, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.deep_clone}
Aliases:

- [DataItem.deep_clone](#DataItem.deep_clone)

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
Aliases:

- [DataItem.deep_uuid](#DataItem.deep_uuid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Recursively computes uuid for x.

Args:
  x: The slice to take uuid on.
  schema: The schema to use to resolve &#39;*&#39; and &#39;**&#39; tokens. If not specified,
    will use the schema of the &#39;x&#39; DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application `x`.</code></pre>

### `DataSlice.dict_size(self) -> DataSlice` {#DataSlice.dict_size}
Aliases:

- [DataItem.dict_size](#DataItem.dict_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a Dict.</code></pre>

### `DataSlice.display(self: DataSlice, options: Any | None = None) -> None` {#DataSlice.display}
Aliases:

- [DataItem.display](#DataItem.display)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Visualizes a DataSlice as an html widget.

Args:
  self: The DataSlice to visualize.
  options: This should be a `koladata.ext.vis.DataSliceVisOptions`.</code></pre>

### `DataSlice.embed_schema()` {#DataSlice.embed_schema}
Aliases:

- [DataItem.embed_schema](#DataItem.embed_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as &#39;__schema__&#39; attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.</code></pre>

### `DataSlice.enriched(self, *bag: DataBag) -> DataSlice` {#DataSlice.enriched}
Aliases:

- [DataItem.enriched](#DataItem.enriched)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with a additional fallback DataBag(s).

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
  DataSlice with additional fallbacks.</code></pre>

### `DataSlice.expand_to(self, target: Any, ndim: Any = unspecified) -> DataSlice` {#DataSlice.expand_to}
Aliases:

- [DataItem.expand_to](#DataItem.expand_to)

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
Aliases:

- [DataItem.explode](#DataItem.explode)

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
Aliases:

- [DataItem.extract](#DataItem.extract)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice with a new DataBag containing only reachable attrs.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A DataSlice with a new immutable DataBag attached.</code></pre>

### `DataSlice.extract_bag(self, schema: Any = unspecified) -> DataBag` {#DataSlice.extract_bag}
Aliases:

- [DataItem.extract_bag](#DataItem.extract_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new DataBag containing only reachable attrs from &#39;ds&#39;.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted DataSlice.

Returns:
  A new immutable DataBag with only the reachable attrs from &#39;ds&#39;.</code></pre>

### `DataSlice.flatten(self, from_dim: int | DataSlice = 0, to_dim: Any = unspecified) -> DataSlice` {#DataSlice.flatten}
Aliases:

- [DataItem.flatten](#DataItem.flatten)

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
Aliases:

- [DataItem.flatten_end](#DataItem.flatten_end)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with a shape flattened `n_times` from the end.

The new shape has x.get_ndim() - n_times dimensions.

Given that flattening happens from the end, only positive integers are
allowed. For more control over flattening, please use `kd.flatten`, instead.

Args:
  x: a DataSlice.
  n_times: number of dimensions to flatten from the end
    (0 &lt;= n_times &lt;= rank).</code></pre>

### `DataSlice.follow(self) -> DataSlice` {#DataSlice.follow}
Aliases:

- [DataItem.follow](#DataItem.follow)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it&#39;s attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.</code></pre>

### `DataSlice.fork_bag(self) -> DataSlice` {#DataSlice.fork_bag}
Aliases:

- [DataItem.fork_bag](#DataItem.fork_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of the DataSlice with a forked mutable DataBag.</code></pre>

### `DataSlice.freeze_bag()` {#DataSlice.freeze_bag}
Aliases:

- [DataItem.freeze_bag](#DataItem.freeze_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen DataSlice equivalent to `self`.</code></pre>

### `DataSlice.from_vals(x, /, schema=None)` {#DataSlice.from_vals}

Alias for [kd.slices.slice](#kd.slices.slice) operator.

### `DataSlice.get_attr(attr_name, /, default=None)` {#DataSlice.get_attr}
Aliases:

- [DataItem.get_attr](#DataItem.get_attr)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Gets attribute `attr_name` where missing items are filled from `default`.

Args:
  attr_name: name of the attribute to get.
  default: optional default value to fill missing items.
           Note that this value can be fully omitted.</code></pre>

### `DataSlice.get_attr_names(*, intersection)` {#DataSlice.get_attr_names}
Aliases:

- [DataItem.get_attr_names](#DataItem.get_attr_names)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a sorted list of unique attribute names of this DataSlice.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  intersection: If True, the intersection of all object attributes is returned.
    Otherwise, the union is returned.

Returns:
  A list of unique attributes sorted by alphabetical order.</code></pre>

### `DataSlice.get_bag()` {#DataSlice.get_bag}
Aliases:

- [DataItem.get_bag](#DataItem.get_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the attached DataBag.</code></pre>

### `DataSlice.get_dtype(self) -> DataSlice` {#DataSlice.get_dtype}
Aliases:

- [DataItem.get_dtype](#DataItem.get_dtype)

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

### `DataSlice.get_item_schema(self) -> DataSlice` {#DataSlice.get_item_schema}
Aliases:

- [DataItem.get_item_schema](#DataItem.get_item_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the item schema of a List schema`.</code></pre>

### `DataSlice.get_itemid(self) -> DataSlice` {#DataSlice.get_itemid}
Aliases:

- [DataItem.get_itemid](#DataItem.get_itemid)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Casts `x` to ITEMID using explicit (permissive) casting rules.</code></pre>

### `DataSlice.get_key_schema(self) -> DataSlice` {#DataSlice.get_key_schema}
Aliases:

- [DataItem.get_key_schema](#DataItem.get_key_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the key schema of a Dict schema`.</code></pre>

### `DataSlice.get_keys()` {#DataSlice.get_keys}
Aliases:

- [DataItem.get_keys](#DataItem.get_keys)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns keys of all dicts in this DataSlice.</code></pre>

### `DataSlice.get_ndim(self) -> DataSlice` {#DataSlice.get_ndim}
Aliases:

- [DataItem.get_ndim](#DataItem.get_ndim)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of dimensions of DataSlice `x`.</code></pre>

### `DataSlice.get_obj_schema(self) -> DataSlice` {#DataSlice.get_obj_schema}
Aliases:

- [DataItem.get_obj_schema](#DataItem.get_obj_schema)

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
Aliases:

- [DataItem.get_present_count](#DataItem.get_present_count)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.</code></pre>

### `DataSlice.get_schema()` {#DataSlice.get_schema}
Aliases:

- [DataItem.get_schema](#DataItem.get_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a schema DataItem with type information about this DataSlice.</code></pre>

### `DataSlice.get_shape()` {#DataSlice.get_shape}
Aliases:

- [DataItem.get_shape](#DataItem.get_shape)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the shape of the DataSlice.</code></pre>

### `DataSlice.get_size(self) -> DataSlice` {#DataSlice.get_size}
Aliases:

- [DataItem.get_size](#DataItem.get_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.</code></pre>

### `DataSlice.get_sizes(self) -> DataSlice` {#DataSlice.get_sizes}
Aliases:

- [DataItem.get_sizes](#DataItem.get_sizes)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice of sizes of the DataSlice&#39;s shape.</code></pre>

### `DataSlice.get_value_schema(self) -> DataSlice` {#DataSlice.get_value_schema}
Aliases:

- [DataItem.get_value_schema](#DataItem.get_value_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the value schema of a Dict schema`.</code></pre>

### `DataSlice.get_values(self, key_ds: Any = unspecified) -> DataSlice` {#DataSlice.get_values}
Aliases:

- [DataItem.get_values](#DataItem.get_values)

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
Aliases:

- [DataItem.has_attr](#DataItem.has_attr)

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
Aliases:

- [DataItem.has_bag](#DataItem.has_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `present` if DataSlice `ds` has a DataBag attached.</code></pre>

### `DataSlice.implode(self, ndim: int | DataSlice = 1, itemid: Any = unspecified) -> DataSlice` {#DataSlice.implode}
Aliases:

- [DataItem.implode](#DataItem.implode)

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
Aliases:

- [DataItem.internal_as_arolla_value](#DataItem.internal_as_arolla_value)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts primitive DataSlice / DataItem into an equivalent Arolla value.</code></pre>

### `DataSlice.internal_as_dense_array()` {#DataSlice.internal_as_dense_array}
Aliases:

- [DataItem.internal_as_dense_array](#DataItem.internal_as_dense_array)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Converts primitive DataSlice to an Arolla DenseArray with appropriate qtype.</code></pre>

### `DataSlice.internal_as_py()` {#DataSlice.internal_as_py}
Aliases:

- [DataItem.internal_as_py](#DataItem.internal_as_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Python object equivalent to this DataSlice.

If the values in this DataSlice represent objects, then the returned python
structure will contain DataItems.</code></pre>

### `DataSlice.internal_is_itemid_schema()` {#DataSlice.internal_is_itemid_schema}
Aliases:

- [DataItem.internal_is_itemid_schema](#DataItem.internal_is_itemid_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is ITEMID Schema.</code></pre>

### `DataSlice.is_dict()` {#DataSlice.is_dict}
Aliases:

- [DataItem.is_dict](#DataItem.is_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has Dict schema or contains only dicts.</code></pre>

### `DataSlice.is_dict_schema()` {#DataSlice.is_dict_schema}
Aliases:

- [DataItem.is_dict_schema](#DataItem.is_dict_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a Dict Schema.</code></pre>

### `DataSlice.is_empty()` {#DataSlice.is_empty}
Aliases:

- [DataItem.is_empty](#DataItem.is_empty)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is empty.</code></pre>

### `DataSlice.is_entity()` {#DataSlice.is_entity}
Aliases:

- [DataItem.is_entity](#DataItem.is_entity)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has Entity schema or contains only entities.</code></pre>

### `DataSlice.is_entity_schema()` {#DataSlice.is_entity_schema}
Aliases:

- [DataItem.is_entity_schema](#DataItem.is_entity_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice represents an Entity Schema.</code></pre>

### `DataSlice.is_list()` {#DataSlice.is_list}
Aliases:

- [DataItem.is_list](#DataItem.is_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice has List schema or contains only lists.</code></pre>

### `DataSlice.is_list_schema()` {#DataSlice.is_list_schema}
Aliases:

- [DataItem.is_list_schema](#DataItem.is_list_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a List Schema.</code></pre>

### `DataSlice.is_mutable()` {#DataSlice.is_mutable}
Aliases:

- [DataItem.is_mutable](#DataItem.is_mutable)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff the attached DataBag is mutable.</code></pre>

### `DataSlice.is_primitive(self) -> DataSlice` {#DataSlice.is_primitive}
Aliases:

- [DataItem.is_primitive](#DataItem.is_primitive)

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
Aliases:

- [DataItem.is_primitive_schema](#DataItem.is_primitive_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice is a primitive (scalar) Schema.</code></pre>

### `DataSlice.is_struct_schema()` {#DataSlice.is_struct_schema}
Aliases:

- [DataItem.is_struct_schema](#DataItem.is_struct_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataSlice represents a Struct Schema.</code></pre>

### `DataSlice.list_size(self) -> DataSlice` {#DataSlice.list_size}
Aliases:

- [DataItem.list_size](#DataItem.list_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a List.</code></pre>

### `DataSlice.maybe(self, attr_name: str) -> DataSlice` {#DataSlice.maybe}
Aliases:

- [DataItem.maybe](#DataItem.maybe)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A shortcut for kd.get_attr(x, attr_name, default=None).</code></pre>

### `DataSlice.new(self, **attrs)` {#DataSlice.new}
Aliases:

- [DataItem.new](#DataItem.new)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new Entity with this Schema.</code></pre>

### `DataSlice.no_bag()` {#DataSlice.no_bag}
Aliases:

- [DataItem.no_bag](#DataItem.no_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice without DataBag.</code></pre>

### `DataSlice.pop(index, /)` {#DataSlice.pop}
Aliases:

- [DataItem.pop](#DataItem.pop)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Pop a value from each list in this DataSlice</code></pre>

### `DataSlice.ref(self) -> DataSlice` {#DataSlice.ref}
Aliases:

- [DataItem.ref](#DataItem.ref)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.</code></pre>

### `DataSlice.repeat(self, sizes: Any) -> DataSlice` {#DataSlice.repeat}
Aliases:

- [DataItem.repeat](#DataItem.repeat)

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
Aliases:

- [DataItem.reshape](#DataItem.reshape)

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
Aliases:

- [DataItem.reshape_as](#DataItem.reshape_as)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice x reshaped to the shape of DataSlice shape_from.</code></pre>

### `DataSlice.select(self, fltr: Any, expand_filter: bool | DataSlice = True) -> DataSlice` {#DataSlice.select}
Aliases:

- [DataItem.select](#DataItem.select)

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
Aliases:

- [DataItem.select_items](#DataItem.select_items)

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
Aliases:

- [DataItem.select_keys](#DataItem.select_keys)

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
Aliases:

- [DataItem.select_present](#DataItem.select_present)

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
Aliases:

- [DataItem.select_values](#DataItem.select_values)

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
Aliases:

- [DataItem.set_attr](#DataItem.set_attr)

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
Aliases:

- [DataItem.set_attrs](#DataItem.set_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets multiple attributes on an object / entity.

Args:
  overwrite_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.</code></pre>

### `DataSlice.set_schema(schema, /)` {#DataSlice.set_schema}
Aliases:

- [DataItem.set_schema](#DataItem.set_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.</code></pre>

### `DataSlice.shallow_clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataSlice.shallow_clone}
Aliases:

- [DataItem.shallow_clone](#DataItem.shallow_clone)

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
Aliases:

- [DataItem.strict_with_attrs](#DataItem.strict_with_attrs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataSlice with a new DataBag containing updated attrs in `x`.

Strict version of kd.attrs disallowing adding new attributes.

Args:
  x: Entity for which the attributes update is being created.
  **attrs: attrs to set in the update.</code></pre>

### `DataSlice.stub(self, attrs: DataSlice = DataSlice([], schema: NONE)) -> DataSlice` {#DataSlice.stub}
Aliases:

- [DataItem.stub](#DataItem.stub)

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
Aliases:

- [DataItem.take](#DataItem.take)

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

### `DataSlice.to_py(ds: DataSlice, max_depth: int = 2, obj_as_dict: bool = False, include_missing_attrs: bool = True) -> Any` {#DataSlice.to_py}
Aliases:

- [DataItem.to_py](#DataItem.to_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a readable python object from a DataSlice.

Attributes, lists, and dicts are recursively converted to Python objects.

Args:
  ds: A DataSlice
  max_depth: Maximum depth for recursive printing. Each attribute, list, and
    dict increments the depth by 1. Use -1 for unlimited depth.
  obj_as_dict: Whether to convert objects to python dicts. By default objects
    are converted to automatically constructed &#39;Obj&#39; dataclass instances.
  include_missing_attrs: whether to include attributes with None value in
    objects.</code></pre>

### `DataSlice.to_pytree(ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True) -> Any` {#DataSlice.to_pytree}
Aliases:

- [DataItem.to_pytree](#DataItem.to_pytree)

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
Aliases:

- [DataItem.updated](#DataItem.updated)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of a DataSlice with DataBag(s) of updates applied.

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
  DataSlice with additional fallbacks.</code></pre>

### `DataSlice.with_attr(self, attr_name: str | DataSlice, value: Any, overwrite_schema: bool | DataSlice = False) -> DataSlice` {#DataSlice.with_attr}
Aliases:

- [DataItem.with_attr](#DataItem.with_attr)

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
Aliases:

- [DataItem.with_attrs](#DataItem.with_attrs)

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
Aliases:

- [DataItem.with_bag](#DataItem.with_bag)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with DataBag `db`.</code></pre>

### `DataSlice.with_dict_update(self, keys: Any, values: Any = unspecified) -> DataSlice` {#DataSlice.with_dict_update}
Aliases:

- [DataItem.with_dict_update](#DataItem.with_dict_update)

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
Aliases:

- [DataItem.with_list_append_update](#DataItem.with_list_append_update)

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
Aliases:

- [DataItem.with_merged_bag](#DataItem.with_merged_bag)

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

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `DataSlice.with_schema(schema, /)` {#DataSlice.with_schema}
Aliases:

- [DataItem.with_schema](#DataItem.with_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.</code></pre>

### `DataSlice.with_schema_from_obj(self) -> DataSlice` {#DataSlice.with_schema_from_obj}
Aliases:

- [DataItem.with_schema_from_obj](#DataItem.with_schema_from_obj)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns `x` with its embedded common schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have a common schema.
* If `x` is empty, the schema is set to NONE.
* If `x` contains mixed primitives without a common primitive type, the output
  will have OBJECT schema.

Args:
  x: An OBJECT DataSlice.</code></pre>

</section>

## DataBag {#DataBag}

`DataBag` is a set of triples (Entity.Attribute => Value).

<section class="zippy closed">

**Members**

### `DataBag.adopt(slice, /)` {#DataBag.adopt}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Adopts all data reachable from the given slice into this DataBag.

Args:
  slice: DataSlice to adopt data from.

Returns:
  The DataSlice with this DataBag (including adopted data) attached.</code></pre>

### `DataBag.adopt_stub(slice, /)` {#DataBag.adopt_stub}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Copies the given DataSlice&#39;s schema stub into this DataBag.

The &#34;schema stub&#34; of a DataSlice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that DataSlice. See kd.stub() for more details.

Args:
  slice: DataSlice to extract the schema stub from.

Returns:
  The &#34;stub&#34; with this DataBag attached.</code></pre>

### `DataBag.concat_lists(self: DataBag, /, *lists: _DataSlice) -> _DataSlice` {#DataBag.concat_lists}

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

### `DataBag.contents_repr(self: DataBag, /, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#DataBag.contents_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of the DataBag contents.</code></pre>

### `DataBag.data_triples_repr(self: DataBag, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#DataBag.data_triples_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of the DataBag contents, omitting schema triples.</code></pre>

### `DataBag.dict(self: DataBag, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.dict}

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

### `DataBag.dict_like(self: DataBag, shape_and_mask_from: _DataSlice, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.dict_like}

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

### `DataBag.dict_schema(key_schema, value_schema)` {#DataBag.dict_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a dict schema from the schemas of the keys and values</code></pre>

### `DataBag.dict_shaped(self: DataBag, shape: _jagged_shape.JaggedShape, /, items_or_keys: dict[Any, Any] | _DataSlice | None = None, values: _DataSlice | None = None, *, key_schema: _DataSlice | None = None, value_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.dict_shaped}

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

### `DataBag.empty()` {#DataBag.empty}

Alias for [kd.bags.new](#kd.bags.new) operator.

### `DataBag.empty_mutable()` {#DataBag.empty_mutable}

Alias for [kd.mutable_bag](#kd.mutable_bag) operator.

### `DataBag.fork(mutable=True)` {#DataBag.fork}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a newly created DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.

Args:
  mutable: If true (default), returns a mutable DataBag. If false, the DataBag
    will be immutable.
Returns:
  data_bag.DataBag</code></pre>

### `DataBag.freeze(self: DataBag) -> DataBag` {#DataBag.freeze}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a frozen DataBag equivalent to `self`.</code></pre>

### `DataBag.get_approx_size()` {#DataBag.get_approx_size}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns approximate size of the DataBag.</code></pre>

### `DataBag.implode(self: DataBag, x: _DataSlice, /, ndim: int | _DataSlice = 1, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.implode}

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

### `DataBag.is_mutable()` {#DataBag.is_mutable}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns present iff this DataBag is mutable.</code></pre>

### `DataBag.list(self: DataBag, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.list}

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

### `DataBag.list_like(self: DataBag, shape_and_mask_from: _DataSlice, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.list_like}

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

### `DataBag.list_schema(item_schema)` {#DataBag.list_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list schema from the schema of the items</code></pre>

### `DataBag.list_shaped(self: DataBag, shape: _jagged_shape.JaggedShape, /, items: list[Any] | _DataSlice | None = None, *, item_schema: _DataSlice | None = None, schema: _DataSlice | None = None, itemid: _DataSlice | None = None) -> _DataSlice` {#DataBag.list_shaped}

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

### `DataBag.merge_fallbacks()` {#DataBag.merge_fallbacks}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a new DataBag with all the fallbacks merged.</code></pre>

### `DataBag.merge_inplace(self: DataBag, other_bags: DataBag | Iterable[DataBag], /, *, overwrite: bool = True, allow_data_conflicts: bool = True, allow_schema_conflicts: bool = False) -> DataBag` {#DataBag.merge_inplace}

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

### `DataBag.named_schema(name, /, **attrs)` {#DataBag.named_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a named schema with ItemId derived only from its name.</code></pre>

### `DataBag.new(arg, *, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#DataBag.new}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Entities with given attrs.

Args:
  arg: optional Python object to be converted to an Entity.
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

### `DataBag.new_like(shape_and_mask_from, *, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#DataBag.new_like}

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

### `DataBag.new_schema(**attrs)` {#DataBag.new_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new schema object with given types of attrs.</code></pre>

### `DataBag.new_shaped(shape, *, schema=None, overwrite_schema=False, itemid=None, **attrs)` {#DataBag.new_shaped}

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

### `DataBag.obj(arg, *, itemid=None, **attrs)` {#DataBag.obj}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Python object to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    itemid will only be set when the args is not a primitive or primitive slice
    if args presents.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.</code></pre>

### `DataBag.obj_like(shape_and_mask_from, *, itemid=None, **attrs)` {#DataBag.obj_like}

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

### `DataBag.obj_shaped(shape, *, itemid=None, **attrs)` {#DataBag.obj_shaped}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.</code></pre>

### `DataBag.schema_triples_repr(self: DataBag, *, triple_limit: int = 1000) -> ContentsReprWrapper` {#DataBag.schema_triples_repr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a representation of schema triples in the DataBag.</code></pre>

### `DataBag.uu(seed='', *, schema=None, overwrite_schema=False, **kwargs)` {#DataBag.uu}

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

### `DataBag.uu_schema(seed='', **attrs)` {#DataBag.uu_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates new uuschema from given types of attrs.</code></pre>

### `DataBag.uuobj(seed='', **kwargs)` {#DataBag.uuobj}

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

### `DataBag.with_name(obj: Any, name: str | Text) -> Any` {#DataBag.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

</section>

## DataItem {#DataItem}

`DataItem` represents a single item (i.e. primitive value, ItemId).

<section class="zippy closed">

**Members**

### `DataItem.L` {#DataItem.L}

<pre class="no-copy"><code class="lang-text no-auto-prettify">ListSlicing helper for DataSlice.

x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
of DataSlice x as a a list.</code></pre>

### `DataItem.S` {#DataItem.S}

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

### `DataItem.append(value, /)` {#DataItem.append}

Alias for [DataSlice.append](#DataSlice.append) operator.

### `DataItem.bind(self, *args: Any, return_type_as: Any = <class 'koladata.types.data_slice.DataSlice'>, **kwargs: Any) -> DataSlice` {#DataItem.bind}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a Koda functor that partially binds a function to `args` and `kwargs`.

This function is intended to work the same as functools.partial in Python.
The bound positional arguments are prepended to the arguments provided at call
time. For keyword arguments, for every &#34;k=something&#34; argument that you pass
to this function, whenever the resulting functor is called, if the user did
not provide &#34;k=something_else&#34; at call time, we will add &#34;k=something&#34;.

Moreover, if the user provides a value for a positional-or-keyword argument
positionally, and it was previously bound using bind(..., k=v), an exception
will occur.

You can pass expressions with their own inputs as values in `args` and
`kwargs`. Those inputs will become inputs of the resulting functor, will be
used to compute those expressions, _and_ they will also be passed to the
underying functor.

Example:
  f = kd.fn(I.x + I.y).bind(x=0)
  kd.call(f, y=1)  # 1
  g = kd.fn(lambda x, y: x + y).bind(5)
  kd.call(g, y=6) # 11

Args:
  self: A Koda functor.
  *args: Positional arguments to bind. The values may be Koda expressions or
    DataItems.
  return_type_as: The return type of the functor is expected to be the same as
    the type of this value. This needs to be specified if the functor does not
    return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: Keyword arguments to bind. The values in this map may be Koda
    expressions or DataItems. When they are expressions, they must evaluate to
    a DataSlice/DataItem or a primitive that will be automatically wrapped
    into a DataItem. This function creates auxiliary variables with names
    starting with &#39;_aux_fn&#39;, so it is not recommended to pass variables with
    such names.

Returns:
  A new Koda functor with some parameters bound.</code></pre>

### `DataItem.clear()` {#DataItem.clear}

Alias for [DataSlice.clear](#DataSlice.clear) operator.

### `DataItem.clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataItem.clone}

Alias for [DataSlice.clone](#DataSlice.clone) operator.

### `DataItem.deep_clone(self, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataItem.deep_clone}

Alias for [DataSlice.deep_clone](#DataSlice.deep_clone) operator.

### `DataItem.deep_uuid(self, schema: Any = unspecified, *, seed: str | DataSlice = '') -> DataSlice` {#DataItem.deep_uuid}

Alias for [DataSlice.deep_uuid](#DataSlice.deep_uuid) operator.

### `DataItem.dict_size(self) -> DataSlice` {#DataItem.dict_size}

Alias for [DataSlice.dict_size](#DataSlice.dict_size) operator.

### `DataItem.display(self: DataSlice, options: Any | None = None) -> None` {#DataItem.display}

Alias for [DataSlice.display](#DataSlice.display) operator.

### `DataItem.embed_schema()` {#DataItem.embed_schema}

Alias for [DataSlice.embed_schema](#DataSlice.embed_schema) operator.

### `DataItem.enriched(self, *bag: DataBag) -> DataSlice` {#DataItem.enriched}

Alias for [DataSlice.enriched](#DataSlice.enriched) operator.

### `DataItem.expand_to(self, target: Any, ndim: Any = unspecified) -> DataSlice` {#DataItem.expand_to}

Alias for [DataSlice.expand_to](#DataSlice.expand_to) operator.

### `DataItem.explode(self, ndim: int | DataSlice = 1) -> DataSlice` {#DataItem.explode}

Alias for [DataSlice.explode](#DataSlice.explode) operator.

### `DataItem.extract(self, schema: Any = unspecified) -> DataSlice` {#DataItem.extract}

Alias for [DataSlice.extract](#DataSlice.extract) operator.

### `DataItem.extract_bag(self, schema: Any = unspecified) -> DataBag` {#DataItem.extract_bag}

Alias for [DataSlice.extract_bag](#DataSlice.extract_bag) operator.

### `DataItem.flatten(self, from_dim: int | DataSlice = 0, to_dim: Any = unspecified) -> DataSlice` {#DataItem.flatten}

Alias for [DataSlice.flatten](#DataSlice.flatten) operator.

### `DataItem.flatten_end(self, n_times: int | DataSlice = 1) -> DataSlice` {#DataItem.flatten_end}

Alias for [DataSlice.flatten_end](#DataSlice.flatten_end) operator.

### `DataItem.follow(self) -> DataSlice` {#DataItem.follow}

Alias for [DataSlice.follow](#DataSlice.follow) operator.

### `DataItem.fork_bag(self) -> DataSlice` {#DataItem.fork_bag}

Alias for [DataSlice.fork_bag](#DataSlice.fork_bag) operator.

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

### `DataItem.get_dtype(self) -> DataSlice` {#DataItem.get_dtype}

Alias for [DataSlice.get_dtype](#DataSlice.get_dtype) operator.

### `DataItem.get_item_schema(self) -> DataSlice` {#DataItem.get_item_schema}

Alias for [DataSlice.get_item_schema](#DataSlice.get_item_schema) operator.

### `DataItem.get_itemid(self) -> DataSlice` {#DataItem.get_itemid}

Alias for [DataSlice.get_itemid](#DataSlice.get_itemid) operator.

### `DataItem.get_key_schema(self) -> DataSlice` {#DataItem.get_key_schema}

Alias for [DataSlice.get_key_schema](#DataSlice.get_key_schema) operator.

### `DataItem.get_keys()` {#DataItem.get_keys}

Alias for [DataSlice.get_keys](#DataSlice.get_keys) operator.

### `DataItem.get_ndim(self) -> DataSlice` {#DataItem.get_ndim}

Alias for [DataSlice.get_ndim](#DataSlice.get_ndim) operator.

### `DataItem.get_obj_schema(self) -> DataSlice` {#DataItem.get_obj_schema}

Alias for [DataSlice.get_obj_schema](#DataSlice.get_obj_schema) operator.

### `DataItem.get_present_count(self) -> DataSlice` {#DataItem.get_present_count}

Alias for [DataSlice.get_present_count](#DataSlice.get_present_count) operator.

### `DataItem.get_schema()` {#DataItem.get_schema}

Alias for [DataSlice.get_schema](#DataSlice.get_schema) operator.

### `DataItem.get_shape()` {#DataItem.get_shape}

Alias for [DataSlice.get_shape](#DataSlice.get_shape) operator.

### `DataItem.get_size(self) -> DataSlice` {#DataItem.get_size}

Alias for [DataSlice.get_size](#DataSlice.get_size) operator.

### `DataItem.get_sizes(self) -> DataSlice` {#DataItem.get_sizes}

Alias for [DataSlice.get_sizes](#DataSlice.get_sizes) operator.

### `DataItem.get_value_schema(self) -> DataSlice` {#DataItem.get_value_schema}

Alias for [DataSlice.get_value_schema](#DataSlice.get_value_schema) operator.

### `DataItem.get_values(self, key_ds: Any = unspecified) -> DataSlice` {#DataItem.get_values}

Alias for [DataSlice.get_values](#DataSlice.get_values) operator.

### `DataItem.has_attr(self, attr_name: str) -> DataSlice` {#DataItem.has_attr}

Alias for [DataSlice.has_attr](#DataSlice.has_attr) operator.

### `DataItem.has_bag()` {#DataItem.has_bag}

Alias for [DataSlice.has_bag](#DataSlice.has_bag) operator.

### `DataItem.implode(self, ndim: int | DataSlice = 1, itemid: Any = unspecified) -> DataSlice` {#DataItem.implode}

Alias for [DataSlice.implode](#DataSlice.implode) operator.

### `DataItem.internal_as_arolla_value()` {#DataItem.internal_as_arolla_value}

Alias for [DataSlice.internal_as_arolla_value](#DataSlice.internal_as_arolla_value) operator.

### `DataItem.internal_as_dense_array()` {#DataItem.internal_as_dense_array}

Alias for [DataSlice.internal_as_dense_array](#DataSlice.internal_as_dense_array) operator.

### `DataItem.internal_as_py()` {#DataItem.internal_as_py}

Alias for [DataSlice.internal_as_py](#DataSlice.internal_as_py) operator.

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

### `DataItem.is_primitive(self) -> DataSlice` {#DataItem.is_primitive}

Alias for [DataSlice.is_primitive](#DataSlice.is_primitive) operator.

### `DataItem.is_primitive_schema()` {#DataItem.is_primitive_schema}

Alias for [DataSlice.is_primitive_schema](#DataSlice.is_primitive_schema) operator.

### `DataItem.is_struct_schema()` {#DataItem.is_struct_schema}

Alias for [DataSlice.is_struct_schema](#DataSlice.is_struct_schema) operator.

### `DataItem.list_size(self) -> DataSlice` {#DataItem.list_size}

Alias for [DataSlice.list_size](#DataSlice.list_size) operator.

### `DataItem.maybe(self, attr_name: str) -> DataSlice` {#DataItem.maybe}

Alias for [DataSlice.maybe](#DataSlice.maybe) operator.

### `DataItem.new(self, **attrs)` {#DataItem.new}

Alias for [DataSlice.new](#DataSlice.new) operator.

### `DataItem.no_bag()` {#DataItem.no_bag}

Alias for [DataSlice.no_bag](#DataSlice.no_bag) operator.

### `DataItem.pop(index, /)` {#DataItem.pop}

Alias for [DataSlice.pop](#DataSlice.pop) operator.

### `DataItem.ref(self) -> DataSlice` {#DataItem.ref}

Alias for [DataSlice.ref](#DataSlice.ref) operator.

### `DataItem.repeat(self, sizes: Any) -> DataSlice` {#DataItem.repeat}

Alias for [DataSlice.repeat](#DataSlice.repeat) operator.

### `DataItem.reshape(self, shape: JaggedShape) -> DataSlice` {#DataItem.reshape}

Alias for [DataSlice.reshape](#DataSlice.reshape) operator.

### `DataItem.reshape_as(self, shape_from: DataSlice) -> DataSlice` {#DataItem.reshape_as}

Alias for [DataSlice.reshape_as](#DataSlice.reshape_as) operator.

### `DataItem.select(self, fltr: Any, expand_filter: bool | DataSlice = True) -> DataSlice` {#DataItem.select}

Alias for [DataSlice.select](#DataSlice.select) operator.

### `DataItem.select_items(self, fltr: Any) -> DataSlice` {#DataItem.select_items}

Alias for [DataSlice.select_items](#DataSlice.select_items) operator.

### `DataItem.select_keys(self, fltr: Any) -> DataSlice` {#DataItem.select_keys}

Alias for [DataSlice.select_keys](#DataSlice.select_keys) operator.

### `DataItem.select_present(self) -> DataSlice` {#DataItem.select_present}

Alias for [DataSlice.select_present](#DataSlice.select_present) operator.

### `DataItem.select_values(self, fltr: Any) -> DataSlice` {#DataItem.select_values}

Alias for [DataSlice.select_values](#DataSlice.select_values) operator.

### `DataItem.set_attr(attr_name, value, /, overwrite_schema=False)` {#DataItem.set_attr}

Alias for [DataSlice.set_attr](#DataSlice.set_attr) operator.

### `DataItem.set_attrs(*, overwrite_schema=False, **attrs)` {#DataItem.set_attrs}

Alias for [DataSlice.set_attrs](#DataSlice.set_attrs) operator.

### `DataItem.set_schema(schema, /)` {#DataItem.set_schema}

Alias for [DataSlice.set_schema](#DataSlice.set_schema) operator.

### `DataItem.shallow_clone(self, *, itemid: Any = unspecified, schema: Any = unspecified, **overrides: Any) -> DataSlice` {#DataItem.shallow_clone}

Alias for [DataSlice.shallow_clone](#DataSlice.shallow_clone) operator.

### `DataItem.strict_with_attrs(self, **attrs) -> DataSlice` {#DataItem.strict_with_attrs}

Alias for [DataSlice.strict_with_attrs](#DataSlice.strict_with_attrs) operator.

### `DataItem.stub(self, attrs: DataSlice = DataSlice([], schema: NONE)) -> DataSlice` {#DataItem.stub}

Alias for [DataSlice.stub](#DataSlice.stub) operator.

### `DataItem.take(self, indices: Any) -> DataSlice` {#DataItem.take}

Alias for [DataSlice.take](#DataSlice.take) operator.

### `DataItem.to_py(ds: DataSlice, max_depth: int = 2, obj_as_dict: bool = False, include_missing_attrs: bool = True) -> Any` {#DataItem.to_py}

Alias for [DataSlice.to_py](#DataSlice.to_py) operator.

### `DataItem.to_pytree(ds: DataSlice, max_depth: int = 2, include_missing_attrs: bool = True) -> Any` {#DataItem.to_pytree}

Alias for [DataSlice.to_pytree](#DataSlice.to_pytree) operator.

### `DataItem.updated(self, *bag: DataBag) -> DataSlice` {#DataItem.updated}

Alias for [DataSlice.updated](#DataSlice.updated) operator.

### `DataItem.with_attr(self, attr_name: str | DataSlice, value: Any, overwrite_schema: bool | DataSlice = False) -> DataSlice` {#DataItem.with_attr}

Alias for [DataSlice.with_attr](#DataSlice.with_attr) operator.

### `DataItem.with_attrs(self, *, overwrite_schema: bool | DataSlice = False, **attrs) -> DataSlice` {#DataItem.with_attrs}

Alias for [DataSlice.with_attrs](#DataSlice.with_attrs) operator.

### `DataItem.with_bag(bag, /)` {#DataItem.with_bag}

Alias for [DataSlice.with_bag](#DataSlice.with_bag) operator.

### `DataItem.with_dict_update(self, keys: Any, values: Any = unspecified) -> DataSlice` {#DataItem.with_dict_update}

Alias for [DataSlice.with_dict_update](#DataSlice.with_dict_update) operator.

### `DataItem.with_list_append_update(self, append: Any) -> DataSlice` {#DataItem.with_list_append_update}

Alias for [DataSlice.with_list_append_update](#DataSlice.with_list_append_update) operator.

### `DataItem.with_merged_bag(self) -> DataSlice` {#DataItem.with_merged_bag}

Alias for [DataSlice.with_merged_bag](#DataSlice.with_merged_bag) operator.

### `DataItem.with_name(obj: Any, name: str | Text) -> Any` {#DataItem.with_name}

Alias for [kd.annotation.with_name](#kd.annotation.with_name) operator.

### `DataItem.with_schema(schema, /)` {#DataItem.with_schema}

Alias for [DataSlice.with_schema](#DataSlice.with_schema) operator.

### `DataItem.with_schema_from_obj(self) -> DataSlice` {#DataItem.with_schema_from_obj}

Alias for [DataSlice.with_schema_from_obj](#DataSlice.with_schema_from_obj) operator.

</section>
