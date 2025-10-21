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

import re
from unittest import mock

from absl.testing import absltest
from koladata.ext.persisted_data import initial_data_manager_interface
from koladata.ext.persisted_data import initial_data_manager_registry


class FakeBareRootInitialDataManager(
    initial_data_manager_interface.InitialDataManagerInterface
):

  @classmethod
  def get_id(cls) -> str:
    return 'FakeBareRootInitialDataManager'


class InitialDataManagerRegistryTest(absltest.TestCase):

  def test_registration_and_lookup(self):
    initial_data_manager_registry.register_initial_data_manager(
        FakeBareRootInitialDataManager
    )
    self.assertIs(
        initial_data_manager_registry.get_initial_data_manager_class(
            'FakeBareRootInitialDataManager'
        ),
        FakeBareRootInitialDataManager,
    )

  def test_multiple_registrations_of_same_manager_are_allowed(self):
    initial_data_manager_registry.register_initial_data_manager(
        FakeBareRootInitialDataManager
    )
    initial_registry = (
        initial_data_manager_registry._INITIAL_DATA_MANAGER_REGISTRY
    )
    self.assertLen(initial_registry, 1)
    for _ in range(3):
      initial_data_manager_registry.register_initial_data_manager(
          FakeBareRootInitialDataManager
      )
    self.assertEqual(
        initial_data_manager_registry._INITIAL_DATA_MANAGER_REGISTRY,
        initial_registry,
    )

  def test_registrations_with_id_conflicts(self):

    class AnotherFakeBareRootInitialDataManager(
        initial_data_manager_interface.InitialDataManagerInterface
    ):

      @classmethod
      def get_id(cls) -> str:
        return 'FakeBareRootInitialDataManager'

    self.assertEqual(
        FakeBareRootInitialDataManager.get_id(),
        AnotherFakeBareRootInitialDataManager.get_id(),
    )

    registry_dict = {}
    with mock.patch.object(
        initial_data_manager_registry,
        '_INITIAL_DATA_MANAGER_REGISTRY',
        new=registry_dict,
    ):

      initial_data_manager_registry.register_initial_data_manager(
          FakeBareRootInitialDataManager
      )
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "initial data manager with id 'FakeBareRootInitialDataManager' is"
              ' already registered: '
          ),
      ):
        initial_data_manager_registry.register_initial_data_manager(
            AnotherFakeBareRootInitialDataManager
        )

      initial_data_manager_registry.register_initial_data_manager(
          AnotherFakeBareRootInitialDataManager, override=True
      )
      self.assertLen(registry_dict, 1)
      self.assertIs(
          initial_data_manager_registry.get_initial_data_manager_class(
              'FakeBareRootInitialDataManager'
          ),
          AnotherFakeBareRootInitialDataManager,
      )

  def test_lookup_of_non_registered_manager_raises_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "initial data manager with id 'UNSPECIFIED' is not registered."
            ' Please import the module that contains the call'
            ' register_initial_data_manager(manager_class) where'
            " manager_class.get_id() == 'UNSPECIFIED'"
        ),
    ):
      initial_data_manager_registry.get_initial_data_manager_class(
          'UNSPECIFIED'
      )


if __name__ == '__main__':
  absltest.main()
