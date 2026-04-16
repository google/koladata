<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.DataSliceManager API

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
updates (data bags).
</code></pre>





### `DataSliceManager.__init__(self, *, internal_call: object, persistence_dir: str, read_only: bool, fs: kd.file_io.FileSystemInterface, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface, data_bag_manager: dbm.DataBagManager, schema_bag_manager: dbm.DataBagManager, schema_helper: schema_helper_lib.SchemaHelper, initial_schema_node_name_to_data_bag_names: kd.types.DictItem, schema_node_name_to_data_bags_updates_manager: dbm.DataBagManager, metadata: metadata_pb2.DataSliceManagerMetadata)` {#kd_ext.storage.DataSliceManager.__init__}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Private constructor.

Clients should use the factory methods create_new() or create_from_dir() to
create instances of this class.

Args:
  internal_call: A private sentinel object to make sure that this
    constructor is called only internally. An error is raised if it is
    called externally. Clients should use the factory methods create_new()
    or create_from_dir() to create instances of this class.
  persistence_dir: The directory that holds the artifacts of the manager.
  read_only: If True, then the manager is created in read-only mode, in
    which case update operations will raise an error.
  fs: All interactions with the file system will go through this instance.
  initial_data_manager: The initial data of the DataSlice.
  data_bag_manager: The manager for the data bags.
  schema_bag_manager: The manager for the schema bags.
  schema_helper: The helper for the schema.
  initial_schema_node_name_to_data_bag_names: A Koda DICT that maps schema
    node names to LISTs of data bag names. It is populated with empty LISTs
    for all the schema node names of the initial data manager.
  schema_node_name_to_data_bags_updates_manager: The manager for updates to
    the Koda DICT of initial_schema_node_name_to_data_bag_names.
  metadata: The metadata of the manager.</code></pre>

### `DataSliceManager.branch(self, output_dir: str | None = None, *, revision_history_index: int = -1, fs: kd.file_io.FileSystemInterface | None = None, description: str | None = None) -> DataSliceManager` {#kd_ext.storage.DataSliceManager.branch}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.branch](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.branch)

### `DataSliceManager.create_from_dir(persistence_dir: str, *, at_revision_history_index: int | None = None, read_only: bool = False, fs: kd.file_io.FileSystemInterface | None = None) -> DataSliceManager` {#kd_ext.storage.DataSliceManager.create_from_dir}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.create_from_dir](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.create_from_dir)

### `DataSliceManager.create_new(persistence_dir: str, *, fs: kd.file_io.FileSystemInterface | None = None, description: str | None = None, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface | None = None) -> DataSliceManager` {#kd_ext.storage.DataSliceManager.create_new}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.create_new](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.create_new)

### `DataSliceManager.exists(self, path: data_slice_path_lib.DataSlicePath) -> bool` {#kd_ext.storage.DataSliceManager.exists}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.exists](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.exists)

### `DataSliceManager.generate_paths(self, *, max_depth: int) -> Generator[data_slice_path_lib.DataSlicePath, None, None]` {#kd_ext.storage.DataSliceManager.generate_paths}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.generate_paths](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.generate_paths)

### `DataSliceManager.get_data_slice(self, populate: Collection[data_slice_path_lib.DataSlicePath] | None = None, populate_including_descendants: Collection[data_slice_path_lib.DataSlicePath] | None = None) -> kd.types.DataSlice` {#kd_ext.storage.DataSliceManager.get_data_slice}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice)

### `DataSliceManager.get_data_slice_at(self, path: data_slice_path_lib.DataSlicePath, with_all_descendants: bool = False) -> kd.types.DataSlice` {#kd_ext.storage.DataSliceManager.get_data_slice_at}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice_at](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice_at)

### `DataSliceManager.get_persistence_directory(self) -> str` {#kd_ext.storage.DataSliceManager.get_persistence_directory}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_persistence_directory](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_persistence_directory)

### `DataSliceManager.get_readonly_copy(self) -> DataSliceManager` {#kd_ext.storage.DataSliceManager.get_readonly_copy}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_readonly_copy](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_readonly_copy)

### `DataSliceManager.get_revision_history(self, tz: datetime.tzinfo | None = None, timestamp_format: str = '%Y-%m-%d %H:%M:%S %Z') -> list[RevisionMetadata]` {#kd_ext.storage.DataSliceManager.get_revision_history}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_revision_history](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_revision_history)

### `DataSliceManager.get_schema(self) -> kd.types.SchemaItem` {#kd_ext.storage.DataSliceManager.get_schema}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.get_schema](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.get_schema)

### `DataSliceManager.internal_get_data_bag_for_schema_node_names(self, schema_node_names: Collection[str]) -> kd.types.DataBag` {#kd_ext.storage.DataSliceManager.internal_get_data_bag_for_schema_node_names}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.internal_get_data_bag_for_schema_node_names](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.internal_get_data_bag_for_schema_node_names)

### `DataSliceManager.is_read_only` {#kd_ext.storage.DataSliceManager.is_read_only}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether this manager is in read-only mode.</code></pre>

### `DataSliceManager.set_read_only(self)` {#kd_ext.storage.DataSliceManager.set_read_only}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.set_read_only](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.set_read_only)

### `DataSliceManager.update(self, *, at_path: data_slice_path_lib.DataSlicePath, attr_name: str, attr_value: kd.types.DataSlice, description: str | None = None)` {#kd_ext.storage.DataSliceManager.update}

Alias for [kd_ext.storage.data_slice_manager.DataSliceManager.update](data_slice_manager/data_slice_manager.md#kd_ext.storage.data_slice_manager.DataSliceManager.update)

