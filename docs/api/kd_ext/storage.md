<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Tools for persisted incremental data.</code></pre>





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

<pre class="no-copy"><code class="lang-text no-auto-prettify">A data slice path.</code></pre>

### `kd_ext.storage.get_internal_global_cache() -> LruSizeTrackingCache[str, DataBag | DataSlice]` {#kd_ext.storage.get_internal_global_cache}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the internal global cache.

Normal clients should not call this method. It is intended for use by low
level code that needs to interact with the global cache, for example for
adjusting the maximum size of the cache or for clearing the cache in OOM
prevention.</code></pre>

