<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage API

Koda data storage utilities.


Subcategory | Description
----------- | ------------
[CompositeInitialDataManager](storage/composite_initial_data_manager.md) | Initial data that composes several DataSliceManagers.
[DataSliceManager](storage/data_slice_manager.md) | Manager of a DataSlice that is assembled from multiple smaller data slices.
[DataSliceManagerInterface](storage/data_slice_manager_interface.md) | Interface for data slice managers.
[DataSliceManagerView](storage/data_slice_manager_view.md) | A view of a DataSliceManager from a particular DataSlicePath.
[DataSlicePath](storage/data_slice_path.md) | A data slice path.
[data_slice_manager](storage/data_slice_manager_1.md) | Management of a DataSlice that is assembled from smaller slices.
[data_slice_path](storage/data_slice_path_1.md) | The definition of data slice paths and utilities for working with them.




### `kd_ext.storage.CompositeInitialDataManager(*, internal_call: object, managers: list[data_slice_manager.DataSliceManager])` {#kd_ext.storage.CompositeInitialDataManager}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initial data that composes several DataSliceManagers.

The state of the component managers are pinned at the time of the creation of
the composite manager.

Conceptually, this class serves a DataSlice that is
equivalent to:
ds[0].enriched(*[ds[i].get_bag() for i in range(1, len(ds))]).
where ds[i] is the full DataSlice of the i-th component manager, i.e.
ds[i] = manager[i].get_data_slice(populate_including_descendants=[&#39;&#39;])

In particular, it means that the list of component managers will form a chain
of fallbacks, where the data of the earlier managers in the list will have
a higher priority than the data of the later managers in the list. That is
the standard semantics of Koda&#39;s enrichment mechanism.</code></pre>

### `kd_ext.storage.DataSliceManager(*, internal_call: object, persistence_dir: str, read_only: bool, fs: kd.file_io.FileSystemInterface, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface, data_bag_manager: dbm.DataBagManager, schema_bag_manager: dbm.DataBagManager, schema_helper: schema_helper_lib.SchemaHelper, initial_schema_node_name_to_data_bag_names: kd.types.DictItem, schema_node_name_to_data_bags_updates_manager: dbm.DataBagManager, metadata: metadata_pb2.DataSliceManagerMetadata)` {#kd_ext.storage.DataSliceManager}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager](storage/data_slice_manager_1.md#kd_ext.storage.data_slice_manager.DataSliceManager)

### `kd_ext.storage.DataSliceManagerInterface()` {#kd_ext.storage.DataSliceManagerInterface}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Interface for data slice managers.</code></pre>

### `kd_ext.storage.DataSliceManagerView(manager: data_slice_manager_interface.DataSliceManagerInterface, path_from_root: data_slice_path_lib.DataSlicePath = DataSlicePath(''))` {#kd_ext.storage.DataSliceManagerView}

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

### `kd_ext.storage.DataSlicePath(actions: tuple[DataSliceAction, ...])` {#kd_ext.storage.DataSlicePath}

Alias for [kd_ext.storage.data_slice_path.DataSlicePath](storage/data_slice_path_1.md#kd_ext.storage.data_slice_path.DataSlicePath)

### `kd_ext.storage.get_internal_global_cache() -> LruSizeTrackingCache[str, DataBag | DataSlice]` {#kd_ext.storage.get_internal_global_cache}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the internal global cache.

Normal clients should not call this method. It is intended for use by low
level code that needs to interact with the global cache, for example for
adjusting the maximum size of the cache or for clearing the cache in OOM
prevention.</code></pre>

