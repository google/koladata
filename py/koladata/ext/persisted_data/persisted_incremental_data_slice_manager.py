# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Management of a DataSlice that is assembled from smaller slices.

The main user-facing abstraction in this module is the class
PersistedIncrementalDataSliceManager.
"""

from __future__ import annotations

import dataclasses
import datetime
import os
from typing import Collection, Generator, cast
import uuid

from google.protobuf import timestamp
from koladata import kd
from koladata.ext.persisted_data import bare_root_initial_data_manager
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import initial_data_manager_interface
from koladata.ext.persisted_data import initial_data_manager_registry
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as dbm
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager_metadata_pb2 as metadata_pb2
from koladata.ext.persisted_data import schema_helper as schema_helper_lib
from koladata.ext.persisted_data import stubs_and_minimal_bags_lib


class _BagIdManager:
  """Manages bag ids, an internal concept."""

  def __init__(self):
    self._next_bag_id = 0

  def get_next_bag_id(self) -> int:
    """Returns the next bag id and increments the internal counter."""
    bag_id = self._next_bag_id
    self._next_bag_id += 1
    return bag_id


@dataclasses.dataclass(frozen=True)
class RevisionMetadata:
  """Metadata about a revision of a PersistedIncrementalDataSliceManager.

  A revision gets created for each successful write operation, i.e. an operation
  that can mutate the data and/or schema managed by the manager.

  Revision metadata should ideally be represented in a format that can render in
  a human-readable way out of the box. The reason is that the primary use case
  of the metadata is to surface history information to users in interactive
  sessions or during debugging. So as a rule of thumb, the data should be
  organized in a way that is easy to consume. E.g. it should be flattened, not
  (deeply) nested, and timestamps and DataSlicePath are presented as
  human-readable strings.
  """

  # A description of the revision. Might be empty. It is usually provided by the
  # user, although some operations may have descriptions that are
  # programmatically generated.
  description: str
  # The timestamp of the revision. The format and timezone are specified in the
  # arguments of PersistedIncrementalDataSliceManager.get_revision_history().
  timestamp: str


@dataclasses.dataclass(frozen=True)
class CreationMetadata(RevisionMetadata):
  """Metadata about the creation of a PersistedIncrementalDataSliceManager."""

  pass


@dataclasses.dataclass(frozen=True)
class AttributeUpdateMetadata(RevisionMetadata):
  """Metadata about an attribute update operation."""

  # The data slice path at which the attribute was updated.
  at_path: str
  # The name of the attribute that was updated.
  attr_name: str


@dataclasses.dataclass(frozen=True)
class BranchMetadata(RevisionMetadata):
  """Metadata about a branch creation operation."""

  # The persistence directory from which the branch was created.
  parent_persistence_directory: str
  # The index of the revision in the parent manager's revision_history on top of
  # which the branch was created.
  parent_revision_history_index: int


_INTERNAL_CALL = object()


class PersistedIncrementalDataSliceManager(
    data_slice_manager_interface.DataSliceManagerInterface
):
  """Manager of a DataSlice that is assembled from multiple smaller data slices.

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
  """

  @classmethod
  def create_new(
      cls,
      persistence_dir: str,
      *,
      fs: fs_interface.FileSystemInterface | None = None,
      description: str | None = None,
      initial_data_manager: (
          initial_data_manager_interface.InitialDataManagerInterface | None
      ) = None,
  ) -> PersistedIncrementalDataSliceManager:
    """Creates a new manager with the given initial state.

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
      A new manager.
    """
    fs = fs or fs_util.get_default_file_system_interaction()
    initial_data_manager = (
        initial_data_manager
        or bare_root_initial_data_manager.BareRootInitialDataManager()
    )
    if description is None:
      description = (
          f'Initial state with {initial_data_manager.get_description()}'
      )

    if fs.exists(persistence_dir):
      if not fs.is_dir(persistence_dir):
        raise ValueError(
            f'persistence_dir should be a directory. Got: {persistence_dir}'
        )
      if fs.glob(os.path.join(persistence_dir, '*')):
        raise ValueError(
            f'persistence_dir should be empty. Got: {persistence_dir}'
        )
    # If persistence_dir does not exist yet, then it will be created by the
    # initial_data_manager.serialize() call or at the latest by the creation of
    # the data_bag_manager below.

    # Fail fast if the initial data manager is not registered. If a user is able
    # to create an instance, then it should be registered. Otherwise it would
    # typically be impossible later on for create_from_dir() to find the id in
    # the registry when it wants to deserialize the initial data manager.
    initial_data_manager_registry.get_initial_data_manager_class(
        initial_data_manager.get_id()
    )
    initial_data_manager.serialize(
        _get_initial_data_dir(persistence_dir), fs=fs
    )
    data_bag_manager = dbm.PersistedIncrementalDataBagManager(
        _get_data_bags_dir(persistence_dir), fs=fs
    )
    schema_bag_manager = dbm.PersistedIncrementalDataBagManager(
        _get_schema_bags_dir(persistence_dir), fs=fs
    )
    schema_helper = schema_helper_lib.SchemaHelper(
        initial_data_manager.get_schema()
    )
    initial_schema_node_name_to_data_bag_names = kd.dict({
        snn: kd.list([], item_schema=kd.STRING)
        for snn in schema_helper.get_all_schema_node_names()
    })
    fs_util.write_slice_to_file(
        fs,
        initial_schema_node_name_to_data_bag_names,
        filepath=_get_initial_schema_node_name_to_bag_names_filepath(
            persistence_dir
        ),
    )
    schema_node_name_to_data_bags_updates_manager = (
        dbm.PersistedIncrementalDataBagManager(
            _get_schema_node_name_to_data_bags_updates_dir(persistence_dir),
            fs=fs,
        )
    )
    metadata = metadata_pb2.PersistedIncrementalDataSliceManagerMetadata(
        version='1.0.0',
        initial_data_manager_id=initial_data_manager.get_id(),
        metadata_update_number=0,
        revision_history=[
            metadata_pb2.RevisionMetadata(
                timestamp=timestamp.from_current_time(),
                description=description,
                added_data_bag_names=sorted(
                    data_bag_manager.get_available_bag_names()
                ),
                added_schema_bag_names=sorted(
                    schema_bag_manager.get_available_bag_names()
                ),
                added_snn_to_data_bags_update_bag_names=sorted(
                    schema_node_name_to_data_bags_updates_manager.get_available_bag_names()
                ),
                creation=metadata_pb2.CreationMetadata(),
            )
        ],
    )
    _persist_metadata(fs, persistence_dir, metadata)
    return cls(
        internal_call=_INTERNAL_CALL,
        persistence_dir=persistence_dir,
        fs=fs,
        initial_data_manager=initial_data_manager,
        data_bag_manager=data_bag_manager,
        schema_bag_manager=schema_bag_manager,
        schema_helper=schema_helper,
        initial_schema_node_name_to_data_bag_names=initial_schema_node_name_to_data_bag_names,
        schema_node_name_to_data_bags_updates_manager=schema_node_name_to_data_bags_updates_manager,
        metadata=metadata,
    )

  @classmethod
  def create_from_dir(
      cls,
      persistence_dir: str,
      *,
      fs: fs_interface.FileSystemInterface | None = None,
  ) -> PersistedIncrementalDataSliceManager:
    """Initializes a manager from an existing persistence directory.

    Args:
      persistence_dir: The directory that holds the artifacts, persisted
        previously by a PersistedIncrementalDataSliceManager, from which the new
        manager should be initialized. Updates to the data and metadata will be
        persisted to this directory; call returned_manager.branch(...) if you
        want to persist updates to a different directory.
      fs: All interactions with the file system will go through this instance.
        If None, then the default interaction with the file system is used.
    """
    fs = fs or fs_util.get_default_file_system_interaction()

    if not fs.glob(os.path.join(persistence_dir, '*')):
      raise ValueError(
          f'persistence_dir {persistence_dir} should contain artifacts'
          ' persisted previously by a PersistedIncrementalDataSliceManager'
      )

    metadata = _read_latest_metadata(fs, persistence_dir)
    initial_data_manager_class = (
        initial_data_manager_registry.get_initial_data_manager_class(
            metadata.initial_data_manager_id
        )
    )
    initial_data_manager = initial_data_manager_class.deserialize(
        _get_initial_data_dir(persistence_dir), fs=fs
    )
    data_bag_manager = dbm.PersistedIncrementalDataBagManager(
        _get_data_bags_dir(persistence_dir), fs=fs
    )
    schema_bag_manager = dbm.PersistedIncrementalDataBagManager(
        _get_schema_bags_dir(persistence_dir), fs=fs
    )
    schema_bag_names = set()
    for revision in metadata.revision_history:
      schema_bag_names.update(revision.added_schema_bag_names)
    schema_helper = schema_helper_lib.SchemaHelper(
        initial_data_manager.get_schema().updated(
            schema_bag_manager.get_minimal_bag(schema_bag_names)
        )
    )
    initial_schema_node_name_to_data_bag_names = cast(
        kd.types.DictItem,
        fs_util.read_slice_from_file(
            fs,
            _get_initial_schema_node_name_to_bag_names_filepath(
                persistence_dir
            ),
        ),
    )
    schema_node_name_to_data_bags_updates_manager = (
        dbm.PersistedIncrementalDataBagManager(
            _get_schema_node_name_to_data_bags_updates_dir(persistence_dir),
            fs=fs,
        )
    )
    update_bags_to_load = set()
    for revision in metadata.revision_history:
      update_bags_to_load.update(
          revision.added_snn_to_data_bags_update_bag_names
      )
    schema_node_name_to_data_bags_updates_manager.load_bags(update_bags_to_load)
    return cls(
        internal_call=_INTERNAL_CALL,
        persistence_dir=persistence_dir,
        fs=fs,
        initial_data_manager=initial_data_manager,
        data_bag_manager=data_bag_manager,
        schema_bag_manager=schema_bag_manager,
        schema_helper=schema_helper,
        initial_schema_node_name_to_data_bag_names=initial_schema_node_name_to_data_bag_names,
        schema_node_name_to_data_bags_updates_manager=schema_node_name_to_data_bags_updates_manager,
        metadata=metadata,
    )

  def __init__(
      self,
      *,
      internal_call: object,
      persistence_dir: str,
      fs: fs_interface.FileSystemInterface,
      initial_data_manager: initial_data_manager_interface.InitialDataManagerInterface,
      data_bag_manager: dbm.PersistedIncrementalDataBagManager,
      schema_bag_manager: dbm.PersistedIncrementalDataBagManager,
      schema_helper: schema_helper_lib.SchemaHelper,
      initial_schema_node_name_to_data_bag_names: kd.types.DictItem,
      schema_node_name_to_data_bags_updates_manager: (
          dbm.PersistedIncrementalDataBagManager
      ),
      metadata: metadata_pb2.PersistedIncrementalDataSliceManagerMetadata,
  ):
    """Private constructor.

    Clients should use the factory methods create_new() or create_from_dir() to
    create instances of this class.

    Args:
      internal_call: A private sentinel object to make sure that this
        constructor is called only internally. An error is raised if it is
        called externally. Clients should use the factory methods create_new()
        or create_from_dir() to create instances of this class.
      persistence_dir: The directory that holds the artifacts of the manager.
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
      metadata: The metadata of the manager.
    """
    if internal_call is not _INTERNAL_CALL:
      raise ValueError(
          'please do not call the PersistedIncrementalDataSliceManager'
          ' constructor directly; use the class factory methods create_new() or'
          ' create_from_dir() instead'
      )

    self._persistence_dir = persistence_dir
    self._fs = fs
    self._initial_data_manager = initial_data_manager
    self._data_bag_manager = data_bag_manager
    self._schema_bag_manager = schema_bag_manager
    self._schema_helper = schema_helper
    self._initial_schema_node_name_to_data_bag_names = (
        initial_schema_node_name_to_data_bag_names
    )
    self._schema_node_name_to_data_bags_updates_manager = (
        schema_node_name_to_data_bags_updates_manager
    )
    self._metadata = metadata

  def get_schema(self) -> kd.types.SchemaItem:
    """Returns the schema of the entire DataSlice managed by this manager."""
    schema_bag_names = set()
    for revision in self._metadata.revision_history:
      schema_bag_names.update(revision.added_schema_bag_names)
    return self._initial_data_manager.get_schema().updated(
        self._schema_bag_manager.get_minimal_bag(schema_bag_names)
    )

  def generate_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    """Yields all data slice paths induced by self.get_schema().

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
      All data slice paths that exist and satisfy the max_depth condition.
    """
    yield from self._schema_helper.generate_available_data_slice_paths(
        max_depth=max_depth
    )

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    """Returns whether the given data slice path exists for this manager."""
    return self._schema_helper.is_valid_data_slice_path(path)

  def get_data_slice(
      self,
      populate: Collection[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          Collection[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    """Returns the dataslice with data for the requested data slice paths.

    If this method is called muliple times without intervening calls to
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
      The root dataslice populated with data for the requested data slice paths.
    """
    populate = set(populate or [])
    populate_including_descendants = set(populate_including_descendants or [])
    for path in populate:
      if not self.exists(path):
        raise ValueError(
            f"data slice path '{path}' passed in argument 'populate' is invalid"
        )
    for path in populate_including_descendants:
      if not self.exists(path):
        raise ValueError(
            f"data slice path '{path}' passed in argument"
            " 'populate_including_descendants' is invalid"
        )

    needed_schema_node_names = {
        self._schema_helper.get_schema_node_name_for_data_slice_path(path)
        for path in populate_including_descendants
    }
    needed_schema_node_names.update(
        self._schema_helper.get_descendant_schema_node_names(
            needed_schema_node_names
        )
    )
    needed_schema_node_names.update({
        self._schema_helper.get_schema_node_name_for_data_slice_path(path)
        for path in populate
    })
    needed_schema_node_names.update(
        self._schema_helper.get_ancestor_schema_node_names(
            needed_schema_node_names
        )
    )

    needed_bag_names = set()
    for snn in needed_schema_node_names:
      names_list = self._get_data_bag_names(snn)
      assert names_list is not None, (
          f'invariant violation: schema node name {snn} must have a list of'
          ' data bag names'
      )
      needed_bag_names.update(names_list.to_py())
    data_bag = self._data_bag_manager.get_minimal_bag(needed_bag_names)

    schema_bag = self._schema_helper.get_schema_bag(
        needed_schema_node_names,
    )

    return (
        self._initial_data_manager.get_data_slice(
            needed_schema_node_names
            & self._initial_data_manager.get_all_schema_node_names()
        )
        .updated(data_bag)
        .updated(schema_bag)
    )

  def get_data_slice_at(
      self,
      path: data_slice_path_lib.DataSlicePath,
      with_all_descendants: bool = False,
  ) -> kd.types.DataSlice:
    """Returns the data slice managed by this manager at the given path.

    Args:
      path: The path for which the data slice is requested. It must be valid:
        self.exists(path) must be True.
      with_all_descendants: If True, then the result will also include the data
        of all the descendant paths of `path`.

    Returns:
      The data slice managed by this manager at the given path.
    """
    if with_all_descendants:
      populate = None
      populate_including_descendants = {path}
    else:
      populate = {path}
      populate_including_descendants = None
    ds = self.get_data_slice(
        populate=populate,
        populate_including_descendants=populate_including_descendants,
    )
    return path.evaluate(ds)

  def get_revision_history(
      self,
      tz: datetime.tzinfo | None = None,
      timestamp_format: str = '%Y-%m-%d %H:%M:%S %Z',
  ) -> list[RevisionMetadata]:
    """Returns the history of the revisions of this manager.

    A revision gets created for each successful write operation, i.e. an
    operation that can mutate the data and/or schema managed by the manager.

    Args:
      tz: The timezone to use for the timestamps. If None, then the local
        timezone is used.
      timestamp_format: The format of the timestamps in the returned metadata.

    Returns:
      The history of revisions of this manager in the order they were created.
    """

    def _format_timestamp(
        revision_metadata: metadata_pb2.RevisionMetadata,
    ) -> str:
      dt = timestamp.to_datetime(
          revision_metadata.timestamp, tz=datetime.timezone.utc
      )
      return dt.astimezone(tz).strftime(timestamp_format)

    result = []
    for revision_metadata in self._metadata.revision_history:
      revision = revision_metadata.WhichOneof('revision_kind')
      if revision == 'creation':
        result.append(
            CreationMetadata(
                description=revision_metadata.description,
                timestamp=_format_timestamp(revision_metadata),
            )
        )
        continue
      if revision == 'attribute_update':
        result.append(
            AttributeUpdateMetadata(
                description=revision_metadata.description,
                timestamp=_format_timestamp(revision_metadata),
                at_path=revision_metadata.attribute_update.at_path,
                attr_name=revision_metadata.attribute_update.attr_name,
            )
        )
        continue
      if revision == 'branch':
        result.append(
            BranchMetadata(
                description=revision_metadata.description,
                timestamp=_format_timestamp(revision_metadata),
                parent_persistence_directory=revision_metadata.branch.parent_persistence_directory,
                parent_revision_history_index=revision_metadata.branch.parent_revision_history_index,
            )
        )
        continue
      raise ValueError(f'unknown revision kind: {revision}')
    return result

  def get_persistence_directory(self) -> str:
    """Returns the persistence directory of this manager."""
    return self._persistence_dir

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
      description: str | None = None,
  ):
    """Updates the data and schema at the given data slice path.

    In particular, the given attribute name is updated with the given value.
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
         e_foo = kd.new(a=1, schema='foo')
         e_bar = e_foo.with_schema(kd.named_schema('bar', a=kd.INT32))
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
         foo_schema = kd.named_schema('foo', a=kd.INT32)
         foo_schema = kd.with_metadata(
             foo_schema, proto_name='my.proto.Message'
         )
         schema_metadata_object = kd.get_metadata(foo_schema)
         explicit_metadata_schema = kd.named_schema(
             'my_metadata', proto_name=kd.STRING
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
      "re-interpreting" an itemid with two different non-ITEMID schemas is not
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
        be stored in the history metadata of this manager.
    """
    # The implementation below creates many small DataBags by traversing a
    # DataSlice. Conceptually, it would be much simpler and faster to traverse
    # the DataBag to get data for each schema node name directly. However, there
    # is currently no convenient API in Python for doing that. So for the time
    # being, we stick to the indirect traversal of the DataBag via a traversal
    # of the DataSlice.
    try:
      extracted_attr_value = attr_value.extract()
    except ValueError:
      # Some slices don't have DataBags. In that case, we just pass the slice
      # through, since it is already minimal without superfluous data.
      extracted_attr_value = attr_value
    del attr_value  # To avoid accidental misuse.
    at_schema_node_name = (
        self._schema_helper.get_schema_node_name_for_data_slice_path(at_path)
    )
    at_subschema = self._schema_helper.get_subschema_at(at_schema_node_name)
    if not kd.schema.is_entity_schema(at_subschema):
      raise ValueError(
          f"the schema at data slice path '{at_path}' is {at_subschema}, which"
          ' does not support updates. Please pass a data slice path that is'
          ' associated with an entity schema'
      )
    affected_schema_node_names = (
        self._schema_helper.get_affected_schema_node_names(
            at_schema_node_name=at_schema_node_name,
            attr_name=attr_name,
            attr_value_schema=extracted_attr_value.get_schema(),
        )
    )

    new_schema_bag = kd.attrs(
        at_subschema,
        **{attr_name: extracted_attr_value.get_schema()},
    )
    new_full_schema = self.get_schema().updated(new_schema_bag)

    stub_with_update = (
        # Loading the data slice at the given path has the effect of loading all
        # the bags of the affected schema node names that exist so far, as well
        # as their ancestors.
        self.get_data_slice_at(at_path, with_all_descendants=False)
        .stub()
        .with_attrs(**{attr_name: extracted_attr_value})
    )
    # This also validates the schema of attr_value, e.g. by rejecting uses of
    # kd.OBJECT immediately:
    stub_with_update_schema_helper = schema_helper_lib.SchemaHelper(
        stub_with_update.get_schema()
    )

    bag_id_manager = _BagIdManager()
    new_bag_id_to_new_bag: dict[int, kd.types.DataBag] = dict()
    schema_node_name_to_new_bag_ids: dict[str, list[int]] = {
        snn: [] for snn in affected_schema_node_names
    }
    seen_items = kd.mutable_bag().dict(
        key_schema=kd.ITEMID, value_schema=kd.MASK
    )

    def process(
        stub_with_update_data_slice_path: data_slice_path_lib.DataSlicePath,
        stub_with_update_subslice: kd.types.DataSlice,
    ):
      """stub_with_update_subslice is a sub-slice of stub_with_update."""

      def get_schema_node_name_and_path(
          action: data_slice_path_lib.DataSliceAction,
      ) -> tuple[str, data_slice_path_lib.DataSlicePath]:
        path = stub_with_update_data_slice_path.extended_with_action(action)
        schema_node_name = stub_with_update_schema_helper.get_schema_node_name_for_data_slice_path(
            path
        )
        return schema_node_name, path

      itemids = stub_with_update_subslice.get_itemid()
      seen_mask = seen_items[itemids]
      stub_with_update_subslice = stub_with_update_subslice & ~seen_mask
      if stub_with_update_subslice.is_empty():
        # Return early. The minimal bags below will all be None anyway.
        return
      seen_items[itemids] = kd.present
      del itemids, seen_mask

      schema = stub_with_update_subslice.get_schema()
      assert schema.is_struct_schema()

      if schema.is_list_schema():
        minimal_bag = stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
            stub_with_update_subslice
        )
        if minimal_bag is None:
          return
        bag_id = bag_id_manager.get_next_bag_id()
        new_bag_id_to_new_bag[bag_id] = minimal_bag
        action = data_slice_path_lib.ListExplode()
        items_schema_node_name, items_path = get_schema_node_name_and_path(
            action
        )
        schema_node_name_to_new_bag_ids[items_schema_node_name].append(bag_id)
        if stub_with_update_schema_helper.is_non_leaf_schema_node_name(
            items_schema_node_name
        ):
          process(items_path, action.evaluate(stub_with_update_subslice))
        return

      if schema.is_dict_schema():
        minimal_bag = stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
            stub_with_update_subslice
        )
        if minimal_bag is None:
          return
        bag_id = bag_id_manager.get_next_bag_id()
        new_bag_id_to_new_bag[bag_id] = minimal_bag
        get_keys_action = data_slice_path_lib.DictGetKeys()
        get_values_action = data_slice_path_lib.DictGetValues()
        keys_schema_node_name, keys_path = get_schema_node_name_and_path(
            get_keys_action
        )
        values_schema_node_name, values_path = get_schema_node_name_and_path(
            get_values_action
        )
        schema_node_name_to_new_bag_ids[keys_schema_node_name].append(bag_id)
        schema_node_name_to_new_bag_ids[values_schema_node_name].append(bag_id)
        if stub_with_update_schema_helper.is_non_leaf_schema_node_name(
            keys_schema_node_name
        ):
          process(
              keys_path, get_keys_action.evaluate(stub_with_update_subslice)
          )
        if stub_with_update_schema_helper.is_non_leaf_schema_node_name(
            values_schema_node_name
        ):
          process(
              values_path, get_values_action.evaluate(stub_with_update_subslice)
          )
        return

      assert schema.is_entity_schema()
      for attr_name in kd.dir(schema):
        minimal_bag = stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
            stub_with_update_subslice, attr_name
        )
        if minimal_bag is None:
          continue
        bag_id = bag_id_manager.get_next_bag_id()
        new_bag_id_to_new_bag[bag_id] = minimal_bag
        action = data_slice_path_lib.GetAttr(attr_name)
        value_schema_node_name, value_path = get_schema_node_name_and_path(
            action
        )
        schema_node_name_to_new_bag_ids[value_schema_node_name].append(bag_id)
        if stub_with_update_schema_helper.is_non_leaf_schema_node_name(
            value_schema_node_name
        ):
          process(value_path, action.evaluate(stub_with_update_subslice))

    process(
        stub_with_update_data_slice_path=data_slice_path_lib.DataSlicePath.from_actions(
            []
        ),
        stub_with_update_subslice=stub_with_update,
    )

    new_bag_ids_to_merged_bag_name: dict[tuple[int, ...], str] = dict()
    merged_bag_name_to_merged_bag: dict[str, kd.types.DataBag] = dict()
    # The body of the next loop creates a new merged bag name and associates it
    # with the merged bag. It processes the bag_ids in lexicographic order. A
    # fixed ordering is not required for correctness, but it can make debugging
    # easier. `merged_bag_names` records the order in which the merged bag names
    # are created.
    merged_bag_names: list[str] = []
    for bag_ids in sorted(schema_node_name_to_new_bag_ids.values()):
      if not bag_ids:
        continue
      bag_ids = tuple(bag_ids)
      if bag_ids in new_bag_ids_to_merged_bag_name:
        continue
      merged_bag_name = self._get_fresh_bag_name()
      new_bag_ids_to_merged_bag_name[bag_ids] = merged_bag_name
      merged_bag_name_to_merged_bag[merged_bag_name] = kd.bags.updated(
          *[new_bag_id_to_new_bag[bn] for bn in bag_ids]
      ).merge_fallbacks()
      merged_bag_names.append(merged_bag_name)

    # The values in this dict are always empty or singleton lists.
    snn_to_merged_bag_name: dict[str, list[str]] = {
        snn: (
            [new_bag_ids_to_merged_bag_name[tuple(bag_names)]]
            if bag_names
            else []
        )
        for snn, bag_names in schema_node_name_to_new_bag_ids.items()
    }

    # The processing so far has no errors, so the user inputs are valid and the
    # update can be performed. We proceed to update the attributes of "self". We
    # do so in a way that is transactional: the state of "self" must either be
    # the same as before in case something fails (e.g. the disk storage is out
    # of space), or the update succeeds. The code is only truly transactional
    # if self._fs supports atomic file-to-file renames. Concurrent writers are
    # then also properly detected and prevented from overwriting each other's
    # changes.

    # Add bags to the various bag managers. It is okay if there is an exception
    # somewhere and not all the bag managers commit their changes to disk,
    # because `self` will always interact with bag managers using explicit sets
    # of bag names. I.e. `self` will never rely on what happens to be committed
    # to disk or loaded in a bag manager. Instead, it will only ask for bags
    # mentioned in self._metadata, so if self._metadata is not updated, then it
    # does not matter if a bag manager has some extra bags.
    data_bags_to_add = [
        # Depend only on the bag manager's root bag - the trivial set of
        # dependencies.
        # A fixed set of non-trivial dependencies here would be too inflexible
        # for our use case, so instead we compute the set of dependencies
        # dynamically right before we load bags. In particular, two features
        # make it difficult to use a fixed set of non-trivial dependencies:
        # 1. Aliasing. Suppose we add ".x" and then ".x.a". Adding ".y" as an
        #    alias of ".x" means that ".x.a" now depends on ".y" too, because if
        #    we call get_data_slice_at(".y", with_all_descendants=True), then we
        #    want to load ".x.a" as well. So this conceptually requires adding a
        #    dependency of ".x.a" on ".y", but the bag+deps of the former was
        #    added a long time ago already.
        # 2. Schema overwrites. The schema of an attribute might be overwritten
        #    by an update. For example, we might want to change ".x" so that it
        #    now contains an INT32 instead of a complex entity. When calling
        #    get_data_slice(populate_including_descendants={root_path}), there
        #    is no need to load the complex entity anymore (provided of course
        #    that it is not aliased elsewhere in the overall slice). So this
        #    conceptually requires removing the dependency of the complex entity
        #    on the root entity's bag, and this dep was added a long time ago.
        dbm.BagToAdd(
            merged_bag_name,
            merged_bag_name_to_merged_bag[merged_bag_name],
            dependencies=(dbm._INITIAL_BAG_NAME,),  # pylint: disable=protected-access
        )
        # We add bags in the order of `merged_bag_names`. The code is also
        # functionally correct without a fixed order: all the bags are sub-bags
        # of stub_with_update.get_bag(), so all overlaps are consistent and it
        # does not matter which order we use. The bag manager remembers the
        # total order in which the bags are added, and will always compose a
        # selection of bags in the fixed order that is consistent with that
        # total order. Using a fixed order here means that the total order of
        # the bags is always the same for the same update and even for the same
        # sequence of updates, which is useful for debugging.
        for merged_bag_name in merged_bag_names
    ]
    self._data_bag_manager.add_bags(data_bags_to_add)
    new_schema_bag_name = self._get_fresh_bag_name()
    schema_bags_to_add = [
        dbm.BagToAdd(
            new_schema_bag_name,
            new_schema_bag,
            # We don't specify fine-grained dependencies for the time being.
            # Instead, we always load+use all the schema bags, and we rely on
            # the manager's bookkeeping of the total order in which the bags
            # were added.
            dependencies=(dbm._INITIAL_BAG_NAME,),  # pylint: disable=protected-access
        )
    ]
    self._schema_bag_manager.add_bags(schema_bags_to_add)
    new_schema_helper = schema_helper_lib.SchemaHelper(new_full_schema)
    snn_to_data_bags_updates = []
    for snn, new_bag_names in snn_to_merged_bag_name.items():
      existing_bag_names = self._get_data_bag_names(snn)
      if existing_bag_names is None:
        update_bag = kd.dict_update(
            self._initial_schema_node_name_to_data_bag_names,
            kd.dict({snn: kd.list(new_bag_names, item_schema=kd.STRING)}),
        )
      else:
        update_bag = kd.list_append_update(
            existing_bag_names, kd.slice(new_bag_names, schema=kd.STRING)
        )
      snn_to_data_bags_updates.append(update_bag)
    map_update_bag_name = self._get_fresh_bag_name()
    self._schema_node_name_to_data_bags_updates_manager.add_bags([
        dbm.BagToAdd(
            map_update_bag_name,
            kd.bags.updated(*snn_to_data_bags_updates).merge_fallbacks(),
            # We don't specify fine-grained dependencies. Instead, we always
            # load+use all the update bags, and we rely on the manager's
            # bookkeeping of the total order in which the bags were added.
            dependencies=(dbm._INITIAL_BAG_NAME,),  # pylint: disable=protected-access
        )
    ])
    new_metadata = metadata_pb2.PersistedIncrementalDataSliceManagerMetadata()
    new_metadata.CopyFrom(self._metadata)
    new_metadata.metadata_update_number += 1
    new_metadata.revision_history.append(
        metadata_pb2.RevisionMetadata(
            timestamp=timestamp.from_current_time(),
            description=description,
            added_data_bag_names=sorted(
                [added.bag_name for added in data_bags_to_add]
            ),
            added_schema_bag_names=[new_schema_bag_name],
            added_snn_to_data_bags_update_bag_names=[map_update_bag_name],
            attribute_update=metadata_pb2.AttributeUpdateMetadata(
                at_path=at_path.to_string(),
                attr_name=attr_name,
            ),
        )
    )
    # Persist the new metadata in a transactional way. Concurrent writers are
    # detected and prevented from overwriting each other's changes when self._fs
    # supports atomic file-to-file renames.
    _persist_metadata(self._fs, self._persistence_dir, new_metadata)
    # The new metadata has be committed to disk. The persistence directory is
    # now at a new revision, but `self` is still at the revision where it was
    # when update() was called. Next, we want to update the state of "self" in a
    # transactional way to the new revision. That is difficult to do in Python,
    # so we detect potential issues and surface them to the user.
    try:
      self._metadata = new_metadata
      self._schema_helper = new_schema_helper
    except BaseException as e:
      # Some exception occurred, e.g. a KeyboardInterrupt.
      # Each of the assignments in the try block is atomic. Just in case the
      # exception or interrupt happened right between the two assignments.
      e.add_note(
          'The manager is in an inconsistent state. Please use a new manager'
          ' instance to avoid data corruption.'
      )
      raise

  def branch(
      self,
      output_dir: str,
      *,
      revision_history_index: int = -1,
      fs: fs_interface.FileSystemInterface | None = None,
      description: str | None = None,
  ) -> PersistedIncrementalDataSliceManager:
    """Creates a branch of the state of this manager.

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
        not exist yet or it must be empty.
      revision_history_index: The index of the revision in the revision history
        of this manager that should be used as the basis for the branch. The
        initial state of the branch manager's DataSlice will be the same as it
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
      A new branch of this manager.
    """
    if not (
        -len(self._metadata.revision_history)
        <= revision_history_index
        < len(self._metadata.revision_history)
    ):
      raise ValueError(
          f'revision_history_index {revision_history_index} is out of bounds.'
          ' Valid values are in the range'
          f' [{-len(self._metadata.revision_history)},'
          f' {len(self._metadata.revision_history)})'
      )
    # We normalize the revision history index to be in the range
    # [0, len(self._metadata.revision_history)). That way it is ready for the
    # BranchMetadata proto, which uses absolute indexes (and not indexes
    # relative to the last revision, because new revisions of this manager can
    # be created after this method returns).
    if revision_history_index < 0:
      revision_history_index = (
          len(self._metadata.revision_history) + revision_history_index
      )

    branch_fs = fs or self._fs
    del fs  # Use branch_fs henceforth.

    if not branch_fs.exists(output_dir):
      branch_fs.make_dirs(output_dir)
    else:
      if not branch_fs.is_dir(output_dir):
        raise ValueError(
            f'the output_dir {output_dir} exists but it is not a directory'
        )
      if branch_fs.glob(os.path.join(output_dir, '*')):
        raise ValueError(f'the output_dir {output_dir} is not empty')

    self._initial_data_manager.serialize(
        os.path.join(output_dir, 'initial_data'), fs=branch_fs
    )
    fs_util.write_slice_to_file(
        branch_fs,
        self._initial_schema_node_name_to_data_bag_names,
        filepath=_get_initial_schema_node_name_to_bag_names_filepath(
            output_dir
        ),
    )

    data_bag_names = set()
    schema_bag_names = set()
    snn_to_data_bags_update_bag_names = set()
    for index in range(revision_history_index + 1):
      revision = self._metadata.revision_history[index]
      data_bag_names.update(revision.added_data_bag_names)
      schema_bag_names.update(revision.added_schema_bag_names)
      snn_to_data_bags_update_bag_names.update(
          revision.added_snn_to_data_bags_update_bag_names
      )

    self._data_bag_manager.create_branch(
        data_bag_names,
        output_dir=os.path.join(output_dir, 'data_bags'),
        fs=branch_fs,
    )
    self._schema_bag_manager.create_branch(
        schema_bag_names,
        output_dir=os.path.join(output_dir, 'schema_bags'),
        fs=branch_fs,
    )
    self._schema_node_name_to_data_bags_updates_manager.create_branch(
        snn_to_data_bags_update_bag_names,
        output_dir=os.path.join(output_dir, 'snn_to_data_bags_updates'),
        fs=branch_fs,
    )

    branch_metadata = metadata_pb2.PersistedIncrementalDataSliceManagerMetadata(
        version='1.0.0',
        initial_data_manager_id=self._initial_data_manager.get_id(),
        metadata_update_number=0,
        revision_history=[
            metadata_pb2.RevisionMetadata(
                timestamp=timestamp.from_current_time(),
                description=description,
                added_data_bag_names=sorted(data_bag_names),
                added_schema_bag_names=sorted(schema_bag_names),
                added_snn_to_data_bags_update_bag_names=sorted(
                    snn_to_data_bags_update_bag_names
                ),
                branch=metadata_pb2.BranchMetadata(
                    parent_persistence_directory=self._persistence_dir,
                    parent_revision_history_index=revision_history_index,
                ),
            )
        ],
    )
    _persist_metadata(branch_fs, output_dir, branch_metadata)

    return PersistedIncrementalDataSliceManager.create_from_dir(
        output_dir, fs=branch_fs
    )

  def clear_cache(self):
    """Clears the internal cache with loaded data of the managed DataSlice.

    Calling this method will typically reduce the memory footprint, unless the
    data is still referenced in your code (for example, when the result of an
    earlier call to get_data_slice(...) is still stored in a variable
    somewhere).

    Calling this method will not affect the functional behavior of this manager.
    For example, the result of get_schema() will remain unchanged, and calling
    get_data_slice(...) will simply load the data again and return the same
    result as before.
    """
    self._initial_data_manager.clear_cache()
    self._data_bag_manager.clear_cache()

  def _get_schema_node_name_to_data_bag_names(self) -> kd.types.DataItem:
    """Returns the full Koda DICT that maps schema node names to data bag names."""
    update_bag_names = set()
    for revision in self._metadata.revision_history:
      update_bag_names.update(revision.added_snn_to_data_bags_update_bag_names)
    return self._initial_schema_node_name_to_data_bag_names.updated(
        self._schema_node_name_to_data_bags_updates_manager.get_minimal_bag(
            update_bag_names
        )
    )

  def _get_data_bag_names(
      self, schema_node_name: str
  ) -> kd.types.DataItem | None:
    """Returns a Koda LIST of data bag names for the given schema node name.

    Returns None if the schema node name is not known, i.e. if it is not a key
    in self._get_schema_node_name_to_data_bag_names(). Otherwise, it returns the
    corresponding value, which is a (possibly empty) Koda LIST of data bag
    names.

    Args:
      schema_node_name: The schema node name for which to get the LIST of data
        bag names.

    Returns:
      A Koda LIST of data bag names, or None if the schema node name is not a
      known schema node name.
    """
    snn_to_data_bag_names = self._get_schema_node_name_to_data_bag_names()
    bag_names = snn_to_data_bag_names[schema_node_name]
    if not kd.has(bag_names):
      return None
    return bag_names

  def _get_fresh_bag_name(self) -> str:
    return _get_uuid()


def _get_initial_data_dir(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'initial_data')


def _get_data_bags_dir(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'data_bags')


def _get_schema_bags_dir(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'schema_bags')


def _get_schema_node_name_to_data_bags_updates_dir(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'snn_to_data_bags_updates')


def _get_initial_schema_node_name_to_bag_names_filepath(
    persistence_dir: str,
) -> str:
  return os.path.join(
      persistence_dir, 'initial_schema_node_name_to_bag_names.kd'
  )


def _get_metadata_filepath(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'metadata.pb')


def _get_uuid() -> str:
  return str(uuid.uuid1())


# The update number is padded to 12 digits in filenames.
_UPDATE_NUMBER_NUM_DIGITS = 12


def _persist_metadata(
    fs: fs_interface.FileSystemInterface,
    persistence_dir: str,
    metadata: metadata_pb2.PersistedIncrementalDataSliceManagerMetadata,
):
  """Persists the given metadata to disk.

  The writing of the metadata is atomic if `fs.rename(src_file, dst_file,
  overwrite=False)` is atomic, i.e. if the given `fs` object to interact with
  the file system supports atomic file-to-file renaming. In that case,
  concurrent writers are detected and prevented from overwriting each other's
  changes.

  Args:
    fs: The filesystem interface to use.
    persistence_dir: The persistence directory of the manager.
    metadata: The metadata to persist.
  """
  update_number = str(metadata.metadata_update_number).zfill(
      _UPDATE_NUMBER_NUM_DIGITS
  )
  final_filepath = os.path.join(persistence_dir, f'metadata-{update_number}.pb')
  if fs.exists(final_filepath):
    # Fail fast if the metadata already exists. No point in writing a temporary
    # file that will end up not being used because of this error.
    raise ValueError(
        f'While attemping to commit update {metadata.metadata_update_number},'
        ' we detected that it is already present in the destination directory'
        f' {persistence_dir}. Some other process must have updated the'
        ' directory in parallel. Updates via this manager are not possible'
        ' anymore. Consider calling branch() with a new directory to'
        ' create a new manager instance with the same state as this one but'
        ' that can perform updates, or create a new manager instance for'
        f' {persistence_dir} and attempt the update again.'
    )
  temp_filepath = os.path.join(
      persistence_dir,
      f'metadata-{update_number}-{_get_uuid()}.pb',
  )
  with fs.open(temp_filepath, 'wb') as f:
    f.write(metadata.SerializeToString())
  fs.rename(temp_filepath, final_filepath, overwrite=False)


def _read_latest_metadata(
    fs: fs_interface.FileSystemInterface,
    persistence_dir: str,
) -> metadata_pb2.PersistedIncrementalDataSliceManagerMetadata:
  update_number_pattern = '[0-9]' * _UPDATE_NUMBER_NUM_DIGITS
  committed_metadata_filepaths = fs.glob(
      os.path.join(persistence_dir, f'metadata-{update_number_pattern}.pb')
  )
  latest_metadata_filepath = max(committed_metadata_filepaths)
  with fs.open(latest_metadata_filepath, 'rb') as f:
    return metadata_pb2.PersistedIncrementalDataSliceManagerMetadata.FromString(
        f.read()
    )
