<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_manager API

<pre class="no-copy"><code class="lang-text no-auto-prettify">Management of a DataSlice that is assembled from smaller slices.

The main user-facing abstraction in this module is the class DataSliceManager.
</code></pre>


Subcategory | Description
----------- | ------------
[AttributeUpdateMetadata](data_slice_manager/attribute_update_metadata.md) | Metadata about an attribute update operation.
[BranchMetadata](data_slice_manager/branch_metadata.md) | Metadata about a branch creation operation.
[CreationMetadata](data_slice_manager/creation_metadata.md) | Metadata about the creation of a DataSliceManager.
[DataSliceManager](data_slice_manager/data_slice_manager.md) | Manager of a DataSlice that is assembled from multiple smaller data slices.
[RevisionMetadata](data_slice_manager/revision_metadata.md) | Metadata about a revision of a DataSliceManager.




### `kd_ext.storage.data_slice_manager.AttributeUpdateMetadata(description: str, timestamp: str, at_path: str, attr_name: str)` {#kd_ext.storage.data_slice_manager.AttributeUpdateMetadata}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Metadata about an attribute update operation.</code></pre>

### `kd_ext.storage.data_slice_manager.BranchMetadata(description: str, timestamp: str, parent_persistence_directory: str, parent_revision_history_index: int)` {#kd_ext.storage.data_slice_manager.BranchMetadata}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Metadata about a branch creation operation.</code></pre>

### `kd_ext.storage.data_slice_manager.CreationMetadata(description: str, timestamp: str)` {#kd_ext.storage.data_slice_manager.CreationMetadata}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Metadata about the creation of a DataSliceManager.</code></pre>

### `kd_ext.storage.data_slice_manager.DataSliceManager(*, internal_call: object, persistence_dir: str, read_only: bool, fs: kd.file_io.FileSystemInterface, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface, data_bag_manager: dbm.DataBagManager, schema_bag_manager: dbm.DataBagManager, schema_helper: schema_helper_lib.SchemaHelper, initial_schema_node_name_to_data_bag_names: kd.types.DictItem, schema_node_name_to_data_bags_updates_manager: dbm.DataBagManager, metadata: metadata_pb2.DataSliceManagerMetadata)` {#kd_ext.storage.data_slice_manager.DataSliceManager}
Aliases:

- [kd_ext.storage.DataSliceManager](../storage.md#kd_ext.storage.DataSliceManager)

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

### `kd_ext.storage.data_slice_manager.RevisionMetadata(description: str, timestamp: str)` {#kd_ext.storage.data_slice_manager.RevisionMetadata}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Metadata about a revision of a DataSliceManager.

A revision gets created for each successful write operation, i.e. an operation
that can mutate the data and/or schema managed by the manager.

Revision metadata should ideally be represented in a format that can render in
a human-readable way out of the box. The reason is that the primary use case
of the metadata is to surface history information to users in interactive
sessions or during debugging. So as a rule of thumb, the data should be
organized in a way that is easy to consume. E.g. it should be flattened, not
(deeply) nested, and timestamps and DataSlicePath are presented as
human-readable strings.</code></pre>

