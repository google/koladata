<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.kv API

Experimental Koda View API.


Subcategory | Description
----------- | ------------
[types](kv/types.md) | Types for the Koda View API.




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

### `kd_ext.kv.deep_clone(v: View | int | float | str | bytes | bool | _Present | None) -> View` {#kd_ext.kv.deep_clone}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a deep copy of the given view.

Utilizes `copy.deepcopy` in the implementation. See its documentation for how
to customize the copying behavior.

Args:
  v: The view to deep copy.</code></pre>

### `kd_ext.kv.deep_map(f: Callable[..., Any], *args: View | int | float | str | bytes | bool | _Present | None, include_missing: bool = False, namespace: str = '') -> View` {#kd_ext.kv.deep_map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies a function to every nested primitive value in the args views.

All arguments will be broadcasted to a common shape based on the depth of the
views. There must be at least one argument.

Unlike `map`, which applies the function to each value at the current depth,
`deep_map` traverses nested structures indiscriminately using
`optree.tree_map` keeping structures intact. See https://optree.readthedocs.io
for more details on how to register handlers for custom types.

Example:
  kv.deep_map(lambda x: x * 2, kv.view([1, None, 2])).get()
  # [2, None, 4]
  kv.deep_map(lambda x: x * 2, kv.view([{&#39;x&#39;: 1, &#39;y&#39;: 2, &#39;z&#39;: None}])).get()
  # [{&#39;x&#39;: 2, &#39;y&#39;: 4, &#39;z&#39;: None}]
  kv.deep_map(
      lambda x, y: x + y,
      kv.view([[1, 2], [3, 4]])[:],
      kv.view([5, 6])
  ).get()
  # [[6, 8], [8, 10]]
  # `[5, 6]` is broadcasted to ([5, 6], [5, 6]) before mapped.

Args:
  f: The function to apply.
  *args: The views to apply the function to.
  include_missing: Specifies whether `f` applies to all items (`=True`) or
    only to present items (`=False`).
  namespace: The namespace to use for the custom type handler.

Returns:
  A new view with the function applied to every nested primitive value.</code></pre>

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

### `kd_ext.kv.set_attrs(v: View | int | float | str | bytes | bool | _Present | None, /, **attrs: View | int | float | str | bytes | bool | _Present | None)` {#kd_ext.kv.set_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets the given attributes of each item.

If one of the items in `v` is None, the corresponding value will be ignored.
If one of the items in `attrs` is None, the attribute of the corresponding
item will be set to None.

If the same object has multiple references in `v`, we will process the
set_attrs in order, so the attribute will have the last assigned value.

Example:
  o = kv.view([types.SimpleNamespace(), types.SimpleNamespace()])[:]
  kv.set_attrs(o, a=1, _b=kv.view([None, 2])[:])
  o.get()
  # (namespace(a=1, _b=None), namespace(a=1, _b=2))

Args:
  v: The view to set the attribute for.
  **attrs: The values to set the attributes to. Can also be a Python
    primitive, which will be automatically boxed into a view.</code></pre>

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

