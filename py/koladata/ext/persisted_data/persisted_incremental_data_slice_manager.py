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
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as dbm
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager_metadata_pb2 as metadata_pb2
from koladata.ext.persisted_data import schema_helper
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


class PersistedIncrementalDataSliceManager(
    data_slice_manager_interface.DataSliceManagerInterface
):
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
      populate: AbstractSet[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          AbstractSet[data_slice_path_lib.DataSlicePath] | None
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
    populate = populate or set()
    populate_including_descendants = populate_including_descendants or set()
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
      needed_bag_names.update(self._schema_node_name_to_bag_names[snn])
    data_bag = self._bag_manager.get_minimal_bag(needed_bag_names)

    schema_bag = self._schema_helper.get_schema_bag(
        needed_schema_node_names,
    )

    return self._root_dataslice.updated(data_bag).updated(schema_bag)

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
    return data_slice_path_lib.get_subslice(ds, path)

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
  ):
    """Updates the data and schema at the given data slice path.

    In particular, the given attribute name is updated with the given value.
    An update can provide new data and new schema, or it can provide updated
    data only or updated data+schema.

    Some restrictions apply to attr_value:
    * attr_value.get_schema() must not use kd.OBJECT or kd.SCHEMA anywhere.
    * attr_value.get_schema() can use only Koda primitives in schema metadata
      attributes.
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
      self.get_data_slice(populate_including_descendants={root_path}), where
      root_path is DataSlicePath.from_actions([]), and already associated with a
      structured schema, then attr_value should not introduce a new structured
      schema for that itemid. These restrictions mean that "re-interpreting" an
      itemid with a different structured schema is not allowed, but there are no
      restrictions when itemids are added with a schema ITEMID, because ITEMID
      is not a structured schema.

    Args:
      at_path: The data slice path at which the update is made. It must be a
        valid data slice path, i.e. self.exists(at_path) must be True. It must
        be associated with an entity schema.
      attr_name: The name of the attribute to update.
      attr_value: The value to assign to the attribute. The restrictions
        mentioned above apply.
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

    new_full_schema = self._schema.updated(
        kd.attrs(
            at_subschema,
            **{attr_name: extracted_attr_value.get_schema()},
        )
    )

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
    stub_with_update_schema_helper = schema_helper.SchemaHelper(
        stub_with_update.get_schema()
    )

    bag_id_manager = _BagIdManager()
    new_bag_id_to_new_bag: dict[int, kd.types.DataBag] = dict()
    schema_node_name_to_new_bag_ids: dict[str, list[int]] = {
        snn: [] for snn in affected_schema_node_names
    }
    seen_items = kd.bag().dict(key_schema=kd.ITEMID, value_schema=kd.MASK)

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
    # with the merged bag. It processes the bag_ids in lexicographic order. Each
    # newly generated bag name is increasing lexicographically, so as a result,
    # iterating over the new merged bag names in lexicographic order will follow
    # the same order as this loop. A fixed ordering is not required for
    # correctness, but it can make debugging easier.
    for bag_ids in sorted(schema_node_name_to_new_bag_ids.values()):
      if not bag_ids:
        continue
      bag_ids = tuple(bag_ids)
      if bag_ids in new_bag_ids_to_merged_bag_name:
        continue
      merged_bag_name = self._get_timestamped_bag_name()
      new_bag_ids_to_merged_bag_name[bag_ids] = merged_bag_name
      merged_bag_name_to_merged_bag[merged_bag_name] = kd.bags.updated(
          *[new_bag_id_to_new_bag[bn] for bn in bag_ids]
      ).merge_fallbacks()

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
    # update can be performed. We proceed to update the attributes of "self".
    bags_to_add = [
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
        dbm.BagToAdd(bag_name, bag, dependencies=('',))
        # We sort based on the bag name to make the order deterministic. The
        # code is also functionally correct without the sorting. The bag manager
        # remembers the total order in which the bags are added, and will always
        # compose a selection of bags in the fixed order that is consistent with
        # that total order. Adding the bags here in a deterministic order can be
        # useful for debugging purposes, because then the total order of the
        # bags is always the same for the same update and even for the same
        # sequence of updates.
        for bag_name, bag in sorted(merged_bag_name_to_merged_bag.items())
    ]
    self._bag_manager.add_bags(bags_to_add)
    self._schema = new_full_schema
    fs_util.write_slice_to_file(
        self._fs,
        self._schema,
        self._get_schema_filepath(),
        overwrite=True,
    )
    self._schema_helper = schema_helper.SchemaHelper(self._schema)
    for snn, new_bag_names in snn_to_merged_bag_name.items():
      existing_bag_names = self._schema_node_name_to_bag_names.get(snn, None)
      if existing_bag_names is None:
        self._schema_node_name_to_bag_names[snn] = new_bag_names
      else:
        existing_bag_names.extend(new_bag_names)
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
    # Some values of self._schema_node_name_to_bag_names may be empty lists,
    # so doing item = kd.dict(self._schema_node_name_to_bag_names) might fail
    # due to limitations of Koda. In particular, the empty lists will by
    # default get schema LIST[NONE], which isn't compatible with LIST[STRING].
    # We overcome that by explicitly specifying the item schema as STRING in a
    # temporary dict d that is then turned into a Koda item.
    d = {
        snn: kd.list(v, item_schema=kd.STRING)
        for snn, v in self._schema_node_name_to_bag_names.items()
    }
    item = kd.dict(d)
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
