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

import datetime
import os
from typing import AbstractSet, Generator

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as dbm
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager_metadata_pb2 as metadata_pb2
from koladata.ext.persisted_data import schema_helper


class PersistedIncrementalDataSliceManager:
  """Manager of a DataSlice that is assembled from multiple smaller data slices.

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
  it can be moved or copied. The persistence directory is consistent after each
  public operation of this class, provided that it is not modified externally
  and that there is sufficient space to accommodate the writes.

  This class is not thread-safe. However, multiple instances of this class can
  concurrently read from the same persistence directory. When an instance
  modifies the persistence directory, which happens when update() is called,
  then there should be no other instances where read/write operations straddle
  the modification. I.e. all the read/write operations of another instance must
  happen completely before or completely after the modification.

  Implementation details:

  The manager indexes each update DataBag with the schema node names for which
  the update can possibly provide data. When a user requests a subslice,
  the manager consults the index and asks the bag manager to load all the needed
  updates (data bags).
  """

  def __init__(
      self,
      persistence_dir: str,
      *,
      fs: fs_interface.FileSystemInterface | None = None,
  ):
    """Initializes the manager.

    Args:
      persistence_dir: The directory where the updates and metadata will be
        persisted. If it does not exist, or it is empty, then it will be
        populated with an empty Koda entity item. Otherwise, the manager will be
        initialized from the existing artifacts in the directory.
      fs: All interactions with the file system will go through this instance.
        If None, then the default interaction with the file system is used.
    """
    self._persistence_dir = persistence_dir
    self._fs = fs or fs_util.get_default_file_system_interaction()
    del persistence_dir, fs  # Forces the use of the attributes henceforth.
    bags_dir = os.path.join(self._persistence_dir, 'bags')
    if not self._fs.exists(self._persistence_dir):
      self._fs.make_dirs(self._persistence_dir)
    if not self._fs.glob(os.path.join(self._persistence_dir, '*')):  # Empty dir
      self._root_dataslice = kd.new()
      fs_util.write_slice_to_file(
          self._fs,
          self._root_dataslice,
          self._get_root_filepath(),
      )
      self._bag_manager = dbm.PersistedIncrementalDataBagManager(
          bags_dir, fs=self._fs
      )
      self._schema = self._root_dataslice.get_schema()
      fs_util.write_slice_to_file(
          self._fs,
          self._schema,
          self._get_schema_filepath(),
      )
      self._schema_helper = schema_helper.SchemaHelper(self._schema)
      self._schema_node_name_to_bag_names = {
          snn: [dbm._INITIAL_BAG_NAME]
          for snn in self._schema_helper.get_all_schema_node_names()
      }
      self._persist_schema_node_name_to_bag_names()
      self._metadata = (
          metadata_pb2.PersistedIncrementalDataSliceManagerMetadata(
              version='1.0.0'
          )
      )
      self._persist_metadata()
    else:
      self._root_dataslice = fs_util.read_slice_from_file(
          self._fs,
          self._get_root_filepath(),
      )
      self._bag_manager = dbm.PersistedIncrementalDataBagManager(
          bags_dir, fs=self._fs
      )
      self._schema = fs_util.read_slice_from_file(
          self._fs,
          self._get_schema_filepath(),
      )
      self._schema_helper = schema_helper.SchemaHelper(self._schema)
      self._schema_node_name_to_bag_names = (
          self._read_schema_node_name_to_bag_names()
      )
      self._metadata = self._read_metadata()

  def get_schema(self) -> kd.types.DataSlice:
    """Returns the schema of the entire DataSlice managed by this manager."""
    return self._schema

  def generate_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    """Yields all data slice paths induced by self.get_schema().

    Args:
      max_depth: The maximum depth of the paths to yield. If negative, then no
        paths are yielded. If zero, then only the root path is yielded. If
        positive, then the root path and all its descendants up to the maximum
        depth are yielded. Recursive schemas typically have an infinite number
        of paths, so it is necessary to impose a limit on the depth.

    Yields:
      All data slice paths that exist up to the given max_depth.
    """
    yield from self._schema_helper.generate_available_data_slice_paths(
        max_depth=max_depth
    )

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    """Returns whether the given data slice path exists for this manager."""
    return self._schema_helper.is_valid_data_slice_path(path)

  def get_data_slice(
      self,
      at_path: data_slice_path_lib.DataSlicePath | None = None,
      with_all_descendants: bool = False,
  ) -> kd.types.DataSlice:
    """Returns the dataslice at the given data slice path.

    If this method is called muliple times without intervening calls to
    update(), then the DataBags of the returned DataSlices are guaranteed to
    be compatible with each other. For example,
    manager.get_data_slice(dsp1).updated(manager.get_data_slice(dsp2).get_bag())
    will be a DataSlice that is populated with subslices for dsp1 and dsp2.

    Args:
      at_path: The data slice path for which the subslice is requested. If None,
        then the empty path is used and the root dataslice is returned.
        Otherwise, self.exists(at_path) must be True.
      with_all_descendants: If True, the returned DataSlice will have all the
        descendants of at_path populated. Descendants are computed with respect
        to the schema, i.e. self.get_schema(). If False, then the returned
        DataSlice might still have some descendants populated.

    Returns:
      The requested subslice of the root dataslice.
    """
    if at_path is None:
      at_path = data_slice_path_lib.DataSlicePath.from_actions([])

    needed_schema_node_names = {
        self._schema_helper.get_schema_node_name_for_data_slice_path(at_path)
    }
    if with_all_descendants:
      needed_schema_node_names.update(
          self._schema_helper.get_descendant_schema_node_names(
              needed_schema_node_names
          )
      )
    needed_schema_node_names.update(
        self._schema_helper.get_ancestor_schema_node_names(
            needed_schema_node_names
        )
    )

    needed_bag_names = set()
    for snn in needed_schema_node_names:
      needed_bag_names.update(self._schema_node_name_to_bag_names[snn])

    bag = self._bag_manager.get_minimal_bag(needed_bag_names)
    loaded_slice = self._root_dataslice.updated(bag)
    return data_slice_path_lib.get_subslice(loaded_slice, at_path)

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
      overwrite_schema: bool = False,
  ):
    """Updates the data and schema at the given data slice path.

    In particular, the given attribute name is updated with the given value.
    An update can provide new data and new schema, or it can provide updated
    data only or updated data+schema.

    Some restrictions apply to attr_value:
    * attr_value.get_schema() must not use kd.OBJECT anywhere.
    * Each itemid in attr_value must be associated with at most one structured
      schema. The behavior is undefined if an itemid is associated with two or
      more structured schemas. Here is an example of how that could happen:

      # AVOID: an attr_value like this leads to undefined behavior!
      e_foo = kd.new(a=1, schema='foo')
      e_bar = e_foo.with_schema(kd.named_schema('bar', a=kd.INT32))
      attr_value = kd.new(foo=e_foo, bar=e_bar)
      assert attr_value.foo.get_itemid() == attr_value.bar.get_itemid()
      assert attr_value.foo.get_schema() != attr_value.bar.get_schema()

      Moreover, if an itemid is already present in the overall slice, i.e. in
      self.get_data_slice(with_all_descendants=True), and already associated
      with a structured schema, then attr_value should not introduce a new
      structured schema for that itemid. These restrictions mean that
      "re-interpreting" an itemid with a different structured schema is not
      allowed, but there are no restrictions when itemids are added with a
      schema ITEMID, because ITEMID is not a structured schema.

    Args:
      at_path: The data slice path at which the update is made. It must be a
        valid data slice path, i.e. self.exists(at_path) must be True. It must
        be associated with an entity schema.
      attr_name: The name of the attribute to update.
      attr_value: The value to assign to the attribute. The restrictions
        mentioned above apply.
      overwrite_schema: If True, then the schema of the attribute will be
        overwritten. Otherwise, the schema will be augmented. The value of this
        argument is forwarded to `kd.attrs()`, which provides more information
        about the semantics.
    """
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
    subschema = self._schema_helper.get_subschema_at(at_schema_node_name)
    if not kd.schema.is_entity_schema(subschema):
      raise ValueError(
          f"the schema at data slice path '{at_path}' is {subschema}, which"
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

    # Loading the data slice at the given path has the effect of loading all
    # the bags of the affected schema node names that exist so far, as well as
    # their ancestors, which is required by _add_update().
    ds = self.get_data_slice(at_path, with_all_descendants=False)
    data_update = kd.attrs(
        ds,
        **{attr_name: extracted_attr_value},
        overwrite_schema=overwrite_schema,
    )
    schema_update = kd.attrs(
        subschema, **{attr_name: extracted_attr_value.get_schema()}
    )
    self._add_update(
        data_update=data_update,
        schema_update=schema_update,
        affected_schema_node_names=affected_schema_node_names,
    )

  def _add_update(
      self,
      *,
      data_update: kd.types.DataBag,
      schema_update: kd.types.DataBag,
      affected_schema_node_names: AbstractSet[str],
  ):
    """Internally adds the update to the manager.

    Callers must already have loaded the DataBags for the subset of
    affected_schema_node_names already exist in the schema graph before the
    update, as well as the DataBags of all their ancestors in the schema graph.

    Args:
      data_update: The data update to add.
      schema_update: The schema update to apply. It is assumed to be included in
        data_update, so callers must ensure that.
      affected_schema_node_names: The schema node names that are affected by the
        update.

    Implementation note: It might perhaps be feasible in the future to have a
      public method that takes only a data_update DataBag. Such a method would
      rely on lower level facilities to compute the schema update and the
      affected schema node names. For the time being, we keep the current
      interface for simplicity.
    """
    # Before changing any attributes of "self", we make sure that there are no
    # errors. I.e. all attributes must be updated successfully or none must be
    # updated.

    new_schema = self._schema.updated(schema_update)
    new_schema_helper = schema_helper.SchemaHelper(new_schema)

    added_bag_name = self._get_timestamped_bag_name()
    self._bag_manager.add_bag(
        bag_name=added_bag_name,
        # Here we do not use kd.bags.updated(data_update, schema_update) because
        # the schema_update is assumed to be included in the data_update.
        bag=data_update,
        # Depend only on the bag manager's root bag - the trivial set of
        # dependencies.
        # A fixed set of non-trivial dependencies here would be too
        # inflexible for our use case, so instead we compute the set of
        # dependencies dynamically right before we load bags. In particular, two
        # features make it difficult to use a fixed set of non-trivial
        # dependencies:
        # 1. Aliasing. Suppose we add ".x" and then ".x.a". Adding ".y" as an
        #    alias of ".x" means that ".x.a" now depends on ".y" too, because if
        #    we call get_data_slice(".y", with_all_descendants=True), then we
        #    want to load ".x.a" as well. So this conceptually requires adding
        #    a dependency of ".x.a" on ".y", but the bag+deps of the former was
        #    added a long time ago already.
        # 2. Schema overwrites. The schema of an attribute might be overwritten
        #    by an update. For example, we might want to change ".x" so that it
        #    now contains an INT32 instead of a complex entity. When calling
        #    get_data_slice(with_all_descendants=True), there is no need to load
        #    the complex entity anymore. So this conceptually requires removing
        #    the dependency of the complex entity on the root entity's bag, and
        #    this dep was added a long time ago.
        dependencies={''},
    )

    new_schema_node_name_to_bag_names = dict(
        self._schema_node_name_to_bag_names
    )
    for snn in affected_schema_node_names:
      if snn not in new_schema_node_name_to_bag_names:
        new_schema_node_name_to_bag_names[snn] = [added_bag_name]
      else:
        new_schema_node_name_to_bag_names[snn].append(added_bag_name)

    self._schema = new_schema
    fs_util.write_slice_to_file(
        self._fs,
        self._schema,
        self._get_schema_filepath(),
        overwrite=True,
    )
    self._schema_helper = new_schema_helper
    self._schema_node_name_to_bag_names = new_schema_node_name_to_bag_names
    self._persist_schema_node_name_to_bag_names()

  def _get_root_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'root.kd')

  def _get_schema_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'schema.kd')

  def _get_schema_node_name_to_bag_names_filepath(self) -> str:
    return os.path.join(
        self._persistence_dir, 'schema_node_name_to_bag_names.kd'
    )

  def _get_metadata_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'metadata.pb')

  def _persist_schema_node_name_to_bag_names(self):
    item = kd.dict(self._schema_node_name_to_bag_names)
    fs_util.write_slice_to_file(
        self._fs,
        item,
        filepath=self._get_schema_node_name_to_bag_names_filepath(),
        overwrite=True,
    )

  def _read_schema_node_name_to_bag_names(self) -> dict[str, list[str]]:
    item = fs_util.read_slice_from_file(
        self._fs,
        self._get_schema_node_name_to_bag_names_filepath(),
    )
    return item.to_py()

  def _persist_metadata(self):
    with self._fs.open(self._get_metadata_filepath(), 'wb') as f:
      f.write(self._metadata.SerializeToString())

  def _read_metadata(
      self,
  ) -> metadata_pb2.PersistedIncrementalDataSliceManagerMetadata:
    with self._fs.open(self._get_metadata_filepath(), 'rb') as f:
      return (
          metadata_pb2.PersistedIncrementalDataSliceManagerMetadata.FromString(
              f.read()
          )
      )

  def _get_timestamped_bag_name(self) -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        '%Y-%m-%d-%H-%M-%S-%f'
    )
