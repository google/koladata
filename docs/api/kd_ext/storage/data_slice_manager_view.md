<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.DataSliceManagerView API

<pre class="no-copy"><code class="lang-text no-auto-prettify">A view of a DataSliceManager from a particular DataSlicePath.

This is a thin wrapper around a DataSliceManager and a DataSlicePath.

The DataSlicePath is called the "view path". It is a full path, i.e. it is
relative to the root of the DataSliceManager.

The underlying DataSliceManager's state can be updated. As a result, a view
path can become invalid for the underlying manager. While the state of an
invalid view can still be extracted, e.g. via get_manager() and
get_path_from_root(), some of the methods, such as get_schema() and
get_data_slice(), will fail when is_view_valid() is False.

Implementation note: The method names should ideally contain verbs. That
should reduce the possible confusion between methods of the view and
attributes in the data, which are usually accessed as attributes, i.e. via
__getattr__(). For example, writing doc_view.title is more natural than
doc_view.get_attr('title'), but if we define a method named 'title' in this
class, then users cannot write doc_view.title anymore and will be forced to
write doc_view.get_attr('title'). To avoid this, most of the methods start
with a verb, usually 'get', so we can use e.g. get_title() for the method.
Reducing the possibility for conflicts with data attributes is also the reason
why the is_view_valid() method is not simply called is_valid() or valid().
</code></pre>





### `DataSliceManagerView.__init__(self, manager: data_slice_manager_interface.DataSliceManagerInterface, path_from_root: data_slice_path_lib.DataSlicePath = DataSlicePath(''))` {#kd_ext.storage.DataSliceManagerView.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initializes the view with the given manager and view path.

Args:
  manager: The underlying DataSliceManager.
  path_from_root: The view path. It must be a valid path for the manager,
    i.e. manager.exists(path_from_root) must be True.</code></pre>

### `DataSliceManagerView.branch(self, *, description: str | None = None) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.branch}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Branches the underlying DataSliceManager and returns a view of it.

It is cheap to create branches. The branch will rely on the data of the
DataSliceManager of `self`, so deleting or moving its storage might break
the branch. A branch can be branched in turn, which means that a branch
manager relies on the data of all the managers in its branching history.

Future updates to this view&#39;s manager and the branch manager are completely
independent:
* New calls to `update` this view will not affect the branch.
* New calls to `update` the branch will not affect this view.

Args:
  description: A description of the branch. Optional. If provided, it will
    be stored in the history metadata of the branched DataSliceManager.

Returns:
  A view of the branched manager that uses the same path from the root as
  this view.</code></pre>

### `DataSliceManagerView.filter(self, selection_mask: kd.types.DataSlice, *, description: str | None = None)` {#kd_ext.storage.DataSliceManagerView.filter}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Filters the DataSlice at the view path.

Filtering is only supported for valid non-root paths. It is a no-op if the
selection mask contains only kd.present. Otherwise, this method mutates the
underlying DataSliceManager. When all items at a container such as a list or
a dict are removed, their container is also removed up to the root
attribute. The root itself is never removed.

Args:
  selection_mask: A MASK DataSlice with the same shape as the DataSlice at
    the view path, or a shape that can be expanded to the shape of the
    DataSlice at the view path. It indicates which items to keep.
  description: A description of the filtering operation. Optional. If
    provided, it will be stored in the history metadata of the underlying
    DataSliceManager.</code></pre>

### `DataSliceManagerView.find_descendants(self, view_predicate: Callable[[DataSliceManagerView], bool], *, max_delta_depth: int = -1) -> Generator[DataSliceManagerView, None, None]` {#kd_ext.storage.DataSliceManagerView.find_descendants}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Generates all descendants of this view that satisfy the predicate.

Views with recursive schemas may have an infinite number of descendants. To
limit the search, one can specify max_delta_depth. Alternatively, the caller
can decide when to abandon the generation (e.g. after a certain number of
views has been generated).

Args:
  view_predicate: A predicate that takes a DataSliceManagerView and returns
    True iff the view should be included in the result.
  max_delta_depth: The maximum depth of the descendants to consider. It is a
    delta relative to the depth of the current view. For example, if the
    current view is at depth 3 from the root and max_delta_depth is 2, then
    only descendants with depth 4 or 5 from the root will be considered. Use
    -1 to consider all descendants.

Yields:
  The descendant views that satisfy the predicate.</code></pre>

### `DataSliceManagerView.get(self, *, populate: Collection[DataSliceManagerView] | None = None, populate_including_descendants: Collection[DataSliceManagerView] | None = None) -> kd.types.DataSlice` {#kd_ext.storage.DataSliceManagerView.get}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the DataSlice at the view path. Sugar for get_data_slice().

The view path must be valid, i.e. self.is_view_valid() must be True.

Args:
  populate: Additional views whose DataSlicePaths will be populated. Their
    paths must be descendants of this view&#39;s path for you to see their data
    in the returned DataSlice.
  populate_including_descendants: Additional views whose DataSlicePaths and
    all their descendants will be populated.</code></pre>

### `DataSliceManagerView.get_ancestor(self, num_levels_up: int) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_ancestor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view from an ancestor of the view path.

The view path does not need to be valid, but the ancestor path at
num_levels_up must be valid for this method to succeed.

Args:
  num_levels_up: The number of levels to go up. Must satisfy 0 &lt;=
    num_levels_up &lt;= len(self.get_path_from_root().actions).</code></pre>

### `DataSliceManagerView.get_attr(self, attr_name: str) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_attr}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the attribute with the given name.

The view path must be valid and associated with an entity schema, i.e.
both self.is_view_valid() and self.get_schema().is_entity_schema() must be
True.

Args:
  attr_name: The name of the attribute whose value should be viewed.

Returns:
  A view of the value of the attribute with the given name.</code></pre>

### `DataSliceManagerView.get_children(self) -> list[DataSliceManagerView]` {#kd_ext.storage.DataSliceManagerView.get_children}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a list of views of the children of the view path.

The view path must be valid, i.e. self.is_view_valid() must be True.
If the view path has no children, then it returns an empty list.

The order of the children is fixed:
- If the DataSlice at the view path is a list, then there is a single child
  that views the exploded list.
- If the DataSlice at the view path is a dict, then there are two children,
  namely views for its keys and its values, in that order.
- If the DataSlice at the view path is an entity, then the children are the
  views of its attributes in sorted order, i.e. the order agrees with that
  of kd.dir(self.get_schema()).</code></pre>

### `DataSliceManagerView.get_data_slice(self, *, populate: Collection[DataSliceManagerView] | None = None, populate_including_descendants: Collection[DataSliceManagerView] | None = None) -> kd.types.DataSlice` {#kd_ext.storage.DataSliceManagerView.get_data_slice}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the DataSlice at the view path.

The view path must be valid, i.e. self.is_view_valid() must be True.

Args:
  populate: Additional views whose DataSlicePaths will be populated. Their
    paths must be descendants of this view&#39;s path for you to see their data
    in the returned DataSlice.
  populate_including_descendants: Additional views whose DataSlicePaths and
    all their descendants will be populated.</code></pre>

### `DataSliceManagerView.get_dict_keys(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_dict_keys}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the dict keys.

The view path must be valid and associated with a dict schema, i.e. both
self.is_view_valid() and self.get_schema().is_dict_schema() must be True.

Returns:
  A view of the dict keys.</code></pre>

### `DataSliceManagerView.get_dict_values(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_dict_values}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the dict values.

The view path must be valid and associated with a dict schema, i.e. both
self.is_view_valid() and self.get_schema().is_dict_schema() must be True.

Returns:
  A view of the dict values.</code></pre>

### `DataSliceManagerView.get_grandparent(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_grandparent}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the grandparent of the view path.

If there is no grandparent, i.e. if the view path is the root or a child of
the root, then it raises ValueError.

The view path does not need to be valid, but the grandparent path must be
valid for this method to succeed.</code></pre>

### `DataSliceManagerView.get_list_items(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_list_items}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the list items.

The view path must be valid and associated with a list schema, i.e. both
self.is_view_valid() and self.get_schema().is_list_schema() must be True.

Returns:
  A view of the exploded list.</code></pre>

### `DataSliceManagerView.get_manager(self) -> data_slice_manager_interface.DataSliceManagerInterface` {#kd_ext.storage.DataSliceManagerView.get_manager}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the underlying DataSliceManager.

Always succeeds, even if the view is currently invalid, i.e. even if
self.is_view_valid() is False.</code></pre>

### `DataSliceManagerView.get_parent(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_parent}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the parent of the view path.

If there is no parent, i.e. if the view path is the root, then it raises
ValueError.

The view path does not need to be valid, but the parent path must be valid
for this method to succeed.</code></pre>

### `DataSliceManagerView.get_path_from_root(self) -> data_slice_path_lib.DataSlicePath` {#kd_ext.storage.DataSliceManagerView.get_path_from_root}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the path of this view. The &#34;view path&#34;.

It is a full path, i.e. it starts from the root of the underlying
DataSliceManager. This method always returns the path, even if the view is
currently invalid, i.e. even if self.is_view_valid() is False.</code></pre>

### `DataSliceManagerView.get_root(self) -> DataSliceManagerView` {#kd_ext.storage.DataSliceManagerView.get_root}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a view of the root of the DataSliceManager.

Always succeeds, even if the view is currently invalid, i.e. even if
self.is_view_valid() is False.</code></pre>

### `DataSliceManagerView.get_schema(self) -> kd.types.SchemaItem` {#kd_ext.storage.DataSliceManagerView.get_schema}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of the DataSlice at the view path.

The view path must be valid, i.e. self.is_view_valid() must be True.</code></pre>

### `DataSliceManagerView.grep_descendants(self, path_from_root_regex: str, *, max_delta_depth: int = -1) -> Generator[DataSliceManagerView, None, None]` {#kd_ext.storage.DataSliceManagerView.grep_descendants}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Generates all descendants of this view whose paths match the given regex.

Views with recursive schemas may have an infinite number of descendants. To
limit the search, one can specify max_delta_depth. Alternatively, the caller
can decide when to abandon the generation (e.g. after a certain number of
views has been generated).

Args:
  path_from_root_regex: A regex in RE2 syntax that must match
    descendant_view.path_from_root().to_string() for the descendant_view to
    be included in the result.
  max_delta_depth: The maximum depth of the descendants to consider. It is a
    delta relative to the depth of the current view. For example, if the
    current view is at depth 3 from the root and max_delta_depth is 2, then
    only descendants with depth 4 or 5 from the root will be considered. Use
    -1 to consider all descendants.

Yields:
  The descendant views whose paths match the given regex.</code></pre>

### `DataSliceManagerView.is_view_valid(self) -> bool` {#kd_ext.storage.DataSliceManagerView.is_view_valid}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns True iff the view path is valid. Never raises an error.</code></pre>

### `DataSliceManagerView.update(self, attr_name: str, attr_value: kd.types.DataSlice, description: str | None = None)` {#kd_ext.storage.DataSliceManagerView.update}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Updates the given attribute at the view path.

The view path must be valid and associated with an entity schema, i.e.
both self.is_view_valid() and self.get_schema().is_entity_schema() must be
True.

Args:
  attr_name: The name of the attribute to update.
  attr_value: The value of the attribute to update. Restrictions imposed by
    the underlying DataSliceManager must be respected.
  description: A description of the update. Optional. If provided, it will
    be stored in the history metadata of the underlying DataSliceManager.</code></pre>

