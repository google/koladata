<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.vis API

Koda visualization functionality.





### `kd_ext.vis.AccessType(*values)` {#kd_ext.vis.AccessType}
Aliases:

- [kd_g3_ext.vis.AccessType](../kd_g3_ext/vis.md#kd_g3_ext.vis.AccessType)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Types of accesses that can appear in an access path.</code></pre>

### `kd_ext.vis.DataSliceVisOptions(num_items: int = 48, unbounded_type_max_len: int = 256, detail_width: int | str | None = None, detail_height: int | str | None = 300, attr_limit: int | None = 20, item_limit: int | None = 20, repr_depth: int = 2, max_folds: int = 2)` {#kd_ext.vis.DataSliceVisOptions}
Aliases:

- [kd_g3_ext.vis.DataSliceVisOptions](../kd_g3_ext/vis.md#kd_g3_ext.vis.DataSliceVisOptions)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Options for visualizing a DataSlice.</code></pre>

### `kd_ext.vis.DescendMode(*values)` {#kd_ext.vis.DescendMode}
Aliases:

- [kd_g3_ext.vis.DescendMode](../kd_g3_ext/vis.md#kd_g3_ext.vis.DescendMode)

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

### `kd_ext.vis.register_formatters() -> bool` {#kd_ext.vis.register_formatters}
Aliases:

- [kd_g3_ext.vis.register_formatters](../kd_g3_ext/vis.md#kd_g3_ext.vis.register_formatters)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Register DataSlice visualization in IPython.

Returns:
  True if the formatters were registered, False if they were already
  registered.</code></pre>

### `kd_ext.vis.unregister_formatters() -> bool` {#kd_ext.vis.unregister_formatters}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Unregister DataSlice visualization in IPython.

Returns:
  True if the formatters were unregistered, False if they were not registered.</code></pre>

### `kd_ext.vis.visualize_slice(ds: DataSlice, options: DataSliceVisOptions | None = None) -> _DataSliceViewState` {#kd_ext.vis.visualize_slice}
Aliases:

- [kd_g3_ext.vis.visualize_slice](../kd_g3_ext/vis.md#kd_g3_ext.vis.visualize_slice)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Visualizes a DataSlice as a html widget.</code></pre>

