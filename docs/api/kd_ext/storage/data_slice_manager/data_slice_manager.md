<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.storage.data_slice_manager.DataSliceManager API

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





### `DataSliceManager.__init__(self, *, internal_call: object, persistence_dir: str, read_only: bool, fs: kd.file_io.FileSystemInterface, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface, data_bag_manager: dbm.DataBagManager, schema_bag_manager: dbm.DataBagManager, schema_helper: schema_helper_lib.SchemaHelper, initial_schema_node_name_to_data_bag_names: kd.types.DictItem, schema_node_name_to_data_bags_updates_manager: dbm.DataBagManager, metadata: metadata_pb2.DataSliceManagerMetadata)` {#kd_ext.storage.data_slice_manager.DataSliceManager.__init__}

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

### `DataSliceManager.branch(self, output_dir: str | None = None, *, revision_history_index: int = -1, fs: kd.file_io.FileSystemInterface | None = None, description: str | None = None) -> DataSliceManager` {#kd_ext.storage.data_slice_manager.DataSliceManager.branch}
Aliases:

- [kd_ext.storage.DataSliceManager.branch](../data_slice_manager.md#kd_ext.storage.DataSliceManager.branch)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a branch of the state of this manager.

It is cheap to create branches. The branch will rely on the data in the
persistence directory of `self`, so deleting or moving the persistence
directory of `self` will break the branch. A branch can be branched in turn,
which means that a manager relies on the persistence directories of all the
managers in its branching history.

Future updates to this manager and its branch are completely independent:
* New calls to `update` this manager will not affect the branch.
* New calls to `update` the branch will not affect this manager.

Use the revision_history_index argument to use a previous revision of this
manager as the basis for the branch. That is useful for rolling back to a
previous state without modifying/updating this manager.

Args:
  output_dir: the new persistence directory to use for the branch. It must
    not exist yet or it must be empty. If not provided, then a new directory
    will be created and used for the branch.
  revision_history_index: The index of the revision in the revision history
    of this manager that should be used as the basis for the branch. The
    initial state of the branch manager&#39;s DataSlice will be the same as it
    was in this manager right after the revision at the given index was
    created. The value of revision_history_index must be a valid index of
    self.get_revision_history(). By default, the branch is created on top of
    the latest revision, i.e. the state that is current when branch() is
    called.
  fs: All interactions with the file system for output_dir will happen via
    this instance. If None, then the interaction object of `self` is used.
  description: A description of the branch. Optional. If provided, it will
    be stored in the history metadata of the branch.

Returns:
  A new branch of this manager.</code></pre>

### `DataSliceManager.create_from_dir(persistence_dir: str, *, at_revision_history_index: int | None = None, read_only: bool = False, fs: kd.file_io.FileSystemInterface | None = None) -> DataSliceManager` {#kd_ext.storage.data_slice_manager.DataSliceManager.create_from_dir}
Aliases:

- [kd_ext.storage.DataSliceManager.create_from_dir](../data_slice_manager.md#kd_ext.storage.DataSliceManager.create_from_dir)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Initializes a manager from an existing persistence directory.

Args:
  persistence_dir: The directory that holds the artifacts, persisted
    previously by a DataSliceManager, from which the new manager should be
    initialized. Updates to the data and metadata will be persisted to this
    directory; call returned_manager.branch(...) if you want to persist
    updates to a different directory.
  at_revision_history_index: The index of the revision in the revision
    history that should be used as the basis for the resulting manager. The
    initial state of the manager&#39;s DataSlice will be the same as it was
    right after the revision at the given index was created. The value of
    at_revision_history_index must be a valid non-negative index number. By
    default, the manager is created on top of the latest revision found in
    the persistence directory. If you have a directory and want to see which
    revisions are available, you can simply call
    create_from_dir(persistence_dir).get_revision_history().
  read_only: If True, then the manager is created in read-only mode, in
    which case update operations will raise an error.
  fs: All interactions with the file system will go through this instance.
    If None, then the default interaction with the file system is used.</code></pre>

### `DataSliceManager.create_new(persistence_dir: str, *, fs: kd.file_io.FileSystemInterface | None = None, description: str | None = None, initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface | None = None) -> DataSliceManager` {#kd_ext.storage.data_slice_manager.DataSliceManager.create_new}
Aliases:

- [kd_ext.storage.DataSliceManager.create_new](../data_slice_manager.md#kd_ext.storage.DataSliceManager.create_new)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a new manager with the given initial state.

Args:
  persistence_dir: The directory that should be initialized to hold the
    metadata, the initial data and the updates to the data. It must be empty
    or not exist.
  fs: All interactions with the file system will go through this instance.
    If None, then the default interaction with the file system is used.
  description: A description of the initial state of the DataSlice. If
    provided, this description is stored in the history metadata.
  initial_data_manager: The initial data of the DataSlice. By default, an
    empty root obtained by kd.new() is used.

Returns:
  A new manager.</code></pre>

### `DataSliceManager.exists(self, path: data_slice_path_lib.DataSlicePath) -> bool` {#kd_ext.storage.data_slice_manager.DataSliceManager.exists}
Aliases:

- [kd_ext.storage.DataSliceManager.exists](../data_slice_manager.md#kd_ext.storage.DataSliceManager.exists)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether the given data slice path exists for this manager.</code></pre>

### `DataSliceManager.generate_paths(self, *, max_depth: int) -> Generator[data_slice_path_lib.DataSlicePath, None, None]` {#kd_ext.storage.data_slice_manager.DataSliceManager.generate_paths}
Aliases:

- [kd_ext.storage.DataSliceManager.generate_paths](../data_slice_manager.md#kd_ext.storage.DataSliceManager.generate_paths)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Yields all data slice paths induced by self.get_schema().

This is a generator because the number of data slice paths can be very
large, or even infinite in the case of recursive schemas. The maximum depth
value is used to limit the data slice paths that are generated;
alternatively, the caller can decide when to stop the generation with custom
logic.

Args:
  max_depth: The maximum depth of the paths to yield. If -1, then all paths
    are yielded. If negative but not -1, then no paths are yielded. If zero,
    then only the root path is yielded. If positive, then the root path and
    all its descendants up to the maximum depth are yielded.

Yields:
  All data slice paths that exist and satisfy the max_depth condition.</code></pre>

### `DataSliceManager.get_data_slice(self, populate: Collection[data_slice_path_lib.DataSlicePath] | None = None, populate_including_descendants: Collection[data_slice_path_lib.DataSlicePath] | None = None) -> kd.types.DataSlice` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice}
Aliases:

- [kd_ext.storage.DataSliceManager.get_data_slice](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_data_slice)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the dataslice with data for the requested data slice paths.

If this method is called multiple times without intervening calls to
update(), then the DataBags of the returned DataSlices are guaranteed to
be compatible with each other. For example,
manager.get_data_slice({p1}).updated(manager.get_data_slice({p2}).get_bag())
will be a DataSlice populated with data for paths p1 and p2, and will be
equivalent to manager.get_data_slice({p1, p2}).

The result might contain more data than requested. All the data in the
result is guaranteed to be valid and up-to-date.

Args:
  populate: The set of paths whose data must be populated in the result.
    Each path must be valid, i.e. self.exists(path) must be True.
  populate_including_descendants: A set of paths whose data must be
    populated in the result; the data of all their descendant paths must
    also be populated. Descendants are computed with respect to the schema,
    i.e. self.get_schema(). Each path must be valid, i.e. self.exists(path)
    must be True.

Returns:
  The root dataslice populated with data for the requested data slice paths.</code></pre>

### `DataSliceManager.get_data_slice_at(self, path: data_slice_path_lib.DataSlicePath, with_all_descendants: bool = False) -> kd.types.DataSlice` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_data_slice_at}
Aliases:

- [kd_ext.storage.DataSliceManager.get_data_slice_at](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_data_slice_at)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the data slice managed by this manager at the given path.

Args:
  path: The path for which the data slice is requested. It must be valid:
    self.exists(path) must be True.
  with_all_descendants: If True, then the result will also include the data
    of all the descendant paths of `path`.

Returns:
  The data slice managed by this manager at the given path.</code></pre>

### `DataSliceManager.get_persistence_directory(self) -> str` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_persistence_directory}
Aliases:

- [kd_ext.storage.DataSliceManager.get_persistence_directory](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_persistence_directory)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the persistence directory of this manager.</code></pre>

### `DataSliceManager.get_readonly_copy(self) -> DataSliceManager` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_readonly_copy}
Aliases:

- [kd_ext.storage.DataSliceManager.get_readonly_copy](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_readonly_copy)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a read-only copy of this manager at its current revision.

The copy will use the same persistence directory as this manager. It will
use the same revision as this manager at the time of copying.

Conceptually, the result is equivalent to calling
DataSliceManager.create_from_dir(
    self.get_persistence_dir(),
    at_revision_history_index=len(self.get_revision_history()) - 1,
    read_only=True,
    fs=self._fs,
)
However, the copy implementation should be faster than that, as there is no
real need to read anything from disk. The implementation is free to read
cheap parts from disk if that turns out to be convenient.

It should be possible to use the copy in another thread. If there is
internal mutable state, such as caches, that are shared between this
instance and the copy, then that state must be properly synchronized.</code></pre>

### `DataSliceManager.get_revision_history(self, tz: datetime.tzinfo | None = None, timestamp_format: str = '%Y-%m-%d %H:%M:%S %Z') -> list[RevisionMetadata]` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_revision_history}
Aliases:

- [kd_ext.storage.DataSliceManager.get_revision_history](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_revision_history)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the history of the revisions of this manager.

A revision gets created for each successful write operation, i.e. an
operation that can mutate the data and/or schema managed by the manager.

Args:
  tz: The timezone to use for the timestamps. If None, then the local
    timezone is used.
  timestamp_format: The format of the timestamps in the returned metadata.

Returns:
  The history of revisions of this manager in the order they were created.</code></pre>

### `DataSliceManager.get_schema(self) -> kd.types.SchemaItem` {#kd_ext.storage.data_slice_manager.DataSliceManager.get_schema}
Aliases:

- [kd_ext.storage.DataSliceManager.get_schema](../data_slice_manager.md#kd_ext.storage.DataSliceManager.get_schema)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns the schema of the entire DataSlice managed by this manager.</code></pre>

### `DataSliceManager.internal_get_data_bag_for_schema_node_names(self, schema_node_names: Collection[str]) -> kd.types.DataBag` {#kd_ext.storage.data_slice_manager.DataSliceManager.internal_get_data_bag_for_schema_node_names}
Aliases:

- [kd_ext.storage.DataSliceManager.internal_get_data_bag_for_schema_node_names](../data_slice_manager.md#kd_ext.storage.DataSliceManager.internal_get_data_bag_for_schema_node_names)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns a DataBag with the data for the given schema node names.</code></pre>

### `DataSliceManager.is_read_only` {#kd_ext.storage.data_slice_manager.DataSliceManager.is_read_only}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns whether this manager is in read-only mode.</code></pre>

### `DataSliceManager.set_read_only(self)` {#kd_ext.storage.data_slice_manager.DataSliceManager.set_read_only}
Aliases:

- [kd_ext.storage.DataSliceManager.set_read_only](../data_slice_manager.md#kd_ext.storage.DataSliceManager.set_read_only)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Sets this manager instance to read-only mode.

Update operations will henceforth raise a ValueError.</code></pre>

### `DataSliceManager.update(self, *, at_path: data_slice_path_lib.DataSlicePath, attr_name: str, attr_value: kd.types.DataSlice, description: str | None = None)` {#kd_ext.storage.data_slice_manager.DataSliceManager.update}
Aliases:

- [kd_ext.storage.DataSliceManager.update](../data_slice_manager.md#kd_ext.storage.DataSliceManager.update)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Updates the data and schema at the given data slice path.

Raises a ValueError if self.is_readonly is True.
Otherwise, the given attribute name is updated with the given value.
An update can provide new data and new schema, or it can provide updated
data only or updated data+schema.

Some restrictions apply to attr_value:
* attr_value.get_schema() must not use kd.SCHEMA anywhere.
* attr_value.get_schema() must not use kd.OBJECT anywhere, apart from its
  implicit use as the schema of schema metadata objects.
* The attributes of schema metadata objects must have only primitive Koda
  values.
* Each itemid in attr_value must be associated with at most one schema other
  than ITEMID. In particular, that implies that:
  1) The behavior is undetermined if an itemid is associated with two or
     more structured schemas. Here is an example of how that could happen:

     # AVOID: an attr_value like this leads to undetermined behavior!
     e_foo = kd.new(a=1, schema=&#39;foo&#39;)
     e_bar = e_foo.with_schema(kd.named_schema(&#39;bar&#39;, a=kd.INT32))
     attr_value = kd.new(foo=e_foo, bar=e_bar)
     assert attr_value.foo.get_itemid() == attr_value.bar.get_itemid()
     assert attr_value.foo.get_schema() != attr_value.bar.get_schema()

  2) The behavior is undetermined if the itemid of a schema metadata object
     is used with some non-ITEMID schema in attr_value. Schema metadata
     objects are not explicitly mentioned in the schema, but their itemids
     are associated with kd.OBJECT. The restriction says that attr_value
     should not associate such an itemid with an explicit schema that is not
     ITEMID. Here is a contrived example of how that could happen:

     # AVOID: an attr_value like this leads to undetermined behavior!
     foo_schema = kd.named_schema(&#39;foo&#39;, a=kd.INT32)
     foo_schema = kd.with_metadata(
         foo_schema, proto_name=&#39;my.proto.Message&#39;
     )
     schema_metadata_object = kd.get_metadata(foo_schema)
     explicit_metadata_schema = kd.named_schema(
         &#39;my_metadata&#39;, proto_name=kd.STRING
     )
     schema_metadata_entity = schema_metadata_object.with_schema(
         explicit_metadata_schema
     )
     attr_value = kd.new(
         # This line associates the itemid of schema_metadata_object with
         # the schema kd.OBJECT:
         foo=foo_schema.new(a=1),
         # This line associates the itemid of schema_metadata_object with
         # explicit_metadata_schema:
         metadata=schema_metadata_entity
     )
     assert (
         kd.get_metadata(attr_value.foo.get_schema()).get_itemid()
         == attr_value.metadata.get_itemid()
     )
     assert (
         kd.get_metadata(attr_value.foo.get_schema()).get_schema()
         != attr_value.metadata.get_schema()
     )

  Moreover, if an itemid is already present in the overall slice, i.e. in
  self.get_data_slice(populate_including_descendants={root_path}), where
  root_path is DataSlicePath.from_actions([]), and already associated
  with a non-ITEMID schema, then attr_value should not introduce a new
  non-ITEMID schema for that itemid. These restrictions mean that
  &#34;re-interpreting&#34; an itemid with two different non-ITEMID schemas is not
  allowed, but there are no restrictions when itemids are added with a
  schema ITEMID.

Args:
  at_path: The data slice path at which the update is made. It must be a
    valid data slice path, i.e. self.exists(at_path) must be True. It must
    be associated with an entity schema.
  attr_name: The name of the attribute to update.
  attr_value: The value to assign to the attribute. The restrictions
    mentioned above apply.
  description: A description of the update. Optional. If provided, it will
    be stored in the history metadata of this manager.</code></pre>

