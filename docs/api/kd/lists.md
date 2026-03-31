<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.lists API

Operators working with lists.





### `kd.lists.appended_list(x, append)` {#kd.lists.appended_list}
Aliases:

- [kd.appended_list](../kd.md#kd.appended_list)

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

- [kd.concat_lists](../kd.md#kd.concat_lists)

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

- [kd.explode](../kd.md#kd.explode)

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

Alias for [kd.core.get_item](core.md#kd.core.get_item)

### `kd.lists.has_list(x)` {#kd.lists.has_list}
Aliases:

- [kd.has_list](../kd.md#kd.has_list)

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

- [kd.implode](../kd.md#kd.implode)

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

- [kd.is_list](../kd.md#kd.is_list)

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

- [kd.list_like](../kd.md#kd.list_like)

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

- [kd.list_append_update](../kd.md#kd.list_append_update)

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

- [kd.select_items](../kd.md#kd.select_items)

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

- [kd.list_shaped](../kd.md#kd.list_shaped)

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

- [kd.list_shaped_as](../kd.md#kd.list_shaped_as)

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

- [kd.list_size](../kd.md#kd.list_size)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns size of a List.</code></pre>

### `kd.lists.uu(items=unspecified, *, item_schema=unspecified, schema=unspecified, seed='')` {#kd.lists.uu}
Aliases:

- [kd.uulist](../kd.md#kd.uulist)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a list with itemid determined deterministically.

The list&#39;s ItemId is computed deterministically from the seed and items
using uuid_for_list.

Examples:
uulist([1, 2, 3]) -&gt; returns a deterministic list ([1, 2, 3])
uulist(kd.slice([1, 2, 3])) -&gt; returns a deterministic list ([1, 2, 3])
uulist(kd.slice([1, 2, 3]), seed=&#39;my_seed&#39;)
  -&gt; returns a deterministic list with a different id than above

Args:
  items: a Python list, or a DataSlice with items.
  item_schema: the schema of the list items. If not specified, it will be
    deduced from items or defaulted to OBJECT.
  schema: the schema to use for the newly created List. If specified, then
    item_schema must not be specified.
  seed: text seed for the uuid computation.

Returns:
  A DataSlice with the list.</code></pre>

### `kd.lists.with_list_append_update(x, append)` {#kd.lists.with_list_append_update}
Aliases:

- [kd.with_list_append_update](../kd.md#kd.with_list_append_update)

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

