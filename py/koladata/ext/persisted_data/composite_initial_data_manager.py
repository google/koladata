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

"""Initial data that composes several PersistedIncrementalDataSliceManagers."""

from __future__ import annotations

import os
from typing import Collection, Self

from koladata import kd
from koladata.ext.persisted_data import composite_initial_data_manager_metadata_pb2 as metadata_pb2
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import initial_data_manager_registry
from koladata.ext.persisted_data import initial_data_manager_with_schema_helper
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager
from koladata.ext.persisted_data import schema_helper as schema_helper_lib


_INTERNAL_CALL = object()


class CompositeInitialDataManager(
    initial_data_manager_with_schema_helper.InitialDataManagerWithSchemaHelper
):
  """Initial data that composes several PersistedIncrementalDataSliceManagers.

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
  """

  _managers: list[
      persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
  ]
  _schema_helper: schema_helper_lib.SchemaHelper

  @classmethod
  def get_id(cls) -> str:
    return 'CompositeInitialDataManager'

  @classmethod
  def create_new(
      cls,
      managers: list[
          persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
      ],
  ) -> Self:
    if not managers:
      raise ValueError('at least one manager is required')

    # The glued managers are all mutable, so we create immutable copies that are
    # pinned to the current revision.
    managers = _new_manager_instances_with_pinned_revisions(managers)

    return CompositeInitialDataManager(
        internal_call=_INTERNAL_CALL,
        managers=managers,
    )

  def __init__(
      self,
      *,
      internal_call: object,
      managers: list[
          persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
      ],
  ):
    if internal_call is not _INTERNAL_CALL:
      raise ValueError(
          'please do not call the CompositeInitialDataManager constructor'
          ' directly; use the class factory methods create_new() or'
          ' deserialize() instead'
      )

    self._managers = managers
    self._schema_helper = schema_helper_lib.SchemaHelper(
        self._managers[0]
        .get_schema()
        .enriched(*[m.get_schema().get_bag() for m in self._managers[1:]])
    )

  def _get_schema_helper(self) -> schema_helper_lib.SchemaHelper:
    return self._schema_helper

  def serialize(
      self, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ):
    if not fs.exists(persistence_dir):
      fs.make_dirs(persistence_dir)
    if fs.glob(os.path.join(persistence_dir, '*')):
      raise ValueError(
          f'the given persistence_dir {persistence_dir} is not empty'
      )

    metadata = metadata_pb2.CompositeInitialDataManagerMetadata(
        version='1.0.0',
        component_manager_metadata=[
            metadata_pb2.ComponentManagerMetadata(
                persistence_dir=m.get_persistence_directory(),
                revision_history_index=len(m.get_revision_history()) - 1,
            )
            for m in self._managers
        ],
    )
    with fs.open(_get_metadata_filepath(persistence_dir), 'wb') as f:
      f.write(metadata.SerializeToString())

  @classmethod
  def deserialize(
      cls, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ) -> CompositeInitialDataManager:
    with fs.open(_get_metadata_filepath(persistence_dir), 'rb') as f:
      metadata = metadata_pb2.CompositeInitialDataManagerMetadata.FromString(
          f.read()
      )

    managers = [
        persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager.create_from_dir(
            component_manager_metadata.persistence_dir,
            at_revision_history_index=component_manager_metadata.revision_history_index,
            fs=fs,
        )
        for component_manager_metadata in metadata.component_manager_metadata
    ]

    return CompositeInitialDataManager(
        internal_call=_INTERNAL_CALL, managers=managers
    )

  def get_data_slice_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataSlice:
    root = self._managers[0].get_data_slice()  # root is a stub-like DataItem.
    root_snn = (
        schema_helper_lib.get_schema_node_name_from_schema_having_an_item_id(
            root.get_schema()
        )
    )
    bag = self.get_data_bag_for_schema_node_names(
        {root_snn} | set(schema_node_names)
    )
    return root.enriched(bag)

  def get_data_bag_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataBag:
    """Returns a DataBag with the data for the given schema node names."""
    schema_node_names = frozenset(schema_node_names)
    invalid_schema_node_names = (
        schema_node_names - self.get_all_schema_node_names()
    )
    if invalid_schema_node_names:
      raise ValueError(
          'schema_node_names contains invalid entries:'
          f' {invalid_schema_node_names}'
      )

    return kd.bags.enriched(*[
        m.internal_get_data_bag_for_schema_node_names(
            schema_node_names & m.get_all_schema_node_names()
        )
        for m in self._managers
    ])

  def clear_cache(self):
    for manager in self._managers:
      manager.clear_cache()

  def get_description(self) -> str:
    return (
        'a composition of the managers'
        f' [{", ".join(m.get_persistence_directory() for m in self._managers)}]'
    )

  def copy(self) -> CompositeInitialDataManager:
    return CompositeInitialDataManager(
        internal_call=_INTERNAL_CALL,
        # Use new manager instances. An instance is not thread-safe, creating
        # new instances is cheap, and no synchronization/locking is needed in
        # or around the new instances.
        # TODO: Once all DataSliceManager and InitialDataManager
        # implementations are thread-safe, we can completely remove the copy()
        # method from InitialDataManagerInterface and simply share references to
        # the InitialDataManager instances. The method
        # PersistedIncrementalDataSliceManager.internal_copy() must be retained,
        # because it is used to create instances that have pinned revisions
        # which is still needed by CompositeInitialDataManager.create_new()
        # above. Said otherwise, the InitialDataManager abstraction is
        # conceptually immutable, and the DataSliceManager abstraction is
        # conceptually mutable, so even if both are thread-safe we need some way
        # to make immutable snapshots of the mutable instances for use in
        # CompositeInitialDataManager.
        managers=_new_manager_instances_with_pinned_revisions(self._managers),
    )


def _new_manager_instances_with_pinned_revisions(
    managers: list[
        persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
    ],
) -> list[
    persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
]:
  return [m.internal_copy() for m in managers]


def _get_metadata_filepath(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'metadata.pb')


initial_data_manager_registry.register_initial_data_manager(
    CompositeInitialDataManager
)
