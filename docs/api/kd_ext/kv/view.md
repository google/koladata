<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.kv.View API

`View` class





### `View.__init__(self, obj: Any, depth: int, internal_call: object, /)` {#View.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Internal constructor. Please use kv.view() instead.</code></pre>

### `View.append(self, value: ViewOrAutoBoxType)` {#View.append}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Appends an item or items to all containers in the view.</code></pre>

### `View.collapse(self, ndim: int = 1) -> View` {#View.collapse}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Collapses equal items along the specified number dimensions of the view.</code></pre>

### `View.deep_clone(self) -> View` {#View.deep_clone}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a deep copy of the view.</code></pre>

### `View.deep_map(self, f: Callable[[Any], Any], *, include_missing: bool = False, namespace: str = '') -> View` {#View.deep_map}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies a function to every nested primitive value in the view.

Unlike `map`, which applies the function to each value at the current depth,
`deep_map` traverses nested structures indiscriminately using
`optree.tree_map` while keeping structures intact. See
https://optree.readthedocs.io for more details on how to register handlers
for custom types.

Example:
  view([1, None, 2]).deep_map(lambda x: x * 2).get()
  # [2, None, 4]
  view([1, None, 2])[:].deep_map(lambda x: x * 2).get()
  # (2, None, 4)
  view([{&#39;x&#39;: 1, &#39;y&#39;: 2, &#39;z&#39;: None}]).deep_map(lambda x: x * 2).get()
  # [{&#39;x&#39;: 2, &#39;y&#39;: 4, &#39;z&#39;: None}]

Args:
  f: The function to apply.
  include_missing: Specifies whether `f` applies to all items (`=True`) or
    only to present items (`=False`).
  namespace: The namespace to use for the custom type handler.

Returns:
  A new view with the function applied to every nested primitive value.</code></pre>

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

### `View.set_attrs(self, /, **attrs: ViewOrAutoBoxType) -> None` {#View.set_attrs}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets the given attributes of each item.</code></pre>

### `View.set_item(self, key_or_index: ViewOrAutoBoxType, value: ViewOrAutoBoxType)` {#View.set_item}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets an item or items for all containers in the view.</code></pre>

### `View.take(self, index: ViewOrAutoBoxType) -> View` {#View.take}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view with the given index in the last dimension.</code></pre>

