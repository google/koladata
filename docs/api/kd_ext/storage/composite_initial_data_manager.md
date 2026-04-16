<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.CompositeInitialDataManager API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initial data that composes several DataSliceManagers.

The state of the component managers are pinned at the time of the creation of
the composite manager.

Conceptually, this class serves a DataSlice that is
equivalent to:
ds[0].enriched(*[ds[i].get_bag() for i in range(1, len(ds))]).
where ds[i] is the full DataSlice of the i-th component manager, i.e.
ds[i] = manager[i].get_data_slice(populate_including_descendants=[''])

In particular, it means that the list of component managers will form a chain
of fallbacks, where the data of the earlier managers in the list will have
a higher priority than the data of the later managers in the list. That is
the standard semantics of Koda's enrichment mechanism.
</code></pre>





### `CompositeInitialDataManager.__init__(self, *, internal_call: object, managers: list[data_slice_manager.DataSliceManager])` {#kd_ext.storage.CompositeInitialDataManager.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initialize self.  See help(type(self)) for accurate signature.</code></pre>

### `CompositeInitialDataManager.copy(self) -> CompositeInitialDataManager` {#kd_ext.storage.CompositeInitialDataManager.copy}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a copy of this manager.

It should be faster to create a copy than to serialize+deserialize this
manager.

It should be possible to use the copy in another thread. If there is
internal mutable state, such as objects to access data sources, that are
shared between this instance and the copy, then that state must be properly
synchronized.</code></pre>

### `CompositeInitialDataManager.create_new(managers: list[data_slice_manager.DataSliceManager]) -> Self` {#kd_ext.storage.CompositeInitialDataManager.create_new}
*No description*

### `CompositeInitialDataManager.deserialize(persistence_dir: str, *, fs: kd.file_io.FileSystemInterface) -> CompositeInitialDataManager` {#kd_ext.storage.CompositeInitialDataManager.deserialize}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Deserializes the initial data and metadata from the given directory.</code></pre>

### `CompositeInitialDataManager.get_data_bag_for_schema_node_names(self, schema_node_names: Collection[str]) -> kd.types.DataBag` {#kd_ext.storage.CompositeInitialDataManager.get_data_bag_for_schema_node_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataBag with the data for the given schema node names.</code></pre>

### `CompositeInitialDataManager.get_data_slice_for_schema_node_names(self, schema_node_names: Collection[str]) -> kd.types.DataSlice` {#kd_ext.storage.CompositeInitialDataManager.get_data_slice_for_schema_node_names}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the root data slice populated only with the requested data.

The root data slice must be a scalar with an entity schema. It is always
present, i.e. never missing. All itemids in the returned data slice must be
stable, in the sense that they are always the same for repeated calls, even
after serialization and deserialization roundtrips.

Args:
  schema_node_names: The names of the schema nodes whose data should be
    included in the returned data slice. Must be a subset of
    self.get_all_schema_node_names(). Care should be taken that the result
    contains only the root and the requested data, and nothing more than
    that. The reason is that all the data returned by DataSliceManager
    should be up-to-date. If this manager returns more data than requested,
    and the extra data has been updated in the meantime, then the extra data
    will be outdated because DataSliceManager will not apply the updates
    before returning the data to the user.</code></pre>

### `CompositeInitialDataManager.get_description(self) -> str` {#kd_ext.storage.CompositeInitialDataManager.get_description}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a brief description of the initial data.

If the user does not provide a description when creating a DataSliceManager,
then the manager will craft a description of the form
f&#39;Initial state with {initial_data_manager.get_description()}&#39;.</code></pre>

### `CompositeInitialDataManager.get_id() -> str` {#kd_ext.storage.CompositeInitialDataManager.get_id}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a unique id for this kind of initial data manager.</code></pre>

### `CompositeInitialDataManager.serialize(self, persistence_dir: str, *, fs: kd.file_io.FileSystemInterface)` {#kd_ext.storage.CompositeInitialDataManager.serialize}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Serializes the initial data and metadata to the given directory.</code></pre>

