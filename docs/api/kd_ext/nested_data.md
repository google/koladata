<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.nested_data API

Utilities for manipulating nested data.





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

