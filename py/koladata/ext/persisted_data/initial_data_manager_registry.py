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

"""Registry of initial data manager classes."""

from koladata.ext.persisted_data import initial_data_manager_interface

_INITIAL_DATA_MANAGER_REGISTRY: dict[
    str,
    type[initial_data_manager_interface.InitialDataManagerInterface],
] = {}


def register_initial_data_manager(
    initial_data_manager_class: type[
        initial_data_manager_interface.InitialDataManagerInterface
    ],
    *,
    override: bool = False,
):
  """Registers an initial data manager class.

  Args:
    initial_data_manager_class: The initial data manager class to register.
    override: Whether to override an existing registration of
      initial_data_manager_class.get_id(). By default, it raises an error if
      another class is already registered with the same id, and it is a noop if
      the same class is already registered.
  """
  manager_class_id = initial_data_manager_class.get_id()
  if override or manager_class_id not in _INITIAL_DATA_MANAGER_REGISTRY:
    _INITIAL_DATA_MANAGER_REGISTRY[manager_class_id] = (
        initial_data_manager_class
    )
    return
  existing_class = _INITIAL_DATA_MANAGER_REGISTRY[manager_class_id]
  if existing_class != initial_data_manager_class:
    raise ValueError(
        f"initial data manager with id '{manager_class_id}' is already"
        f" registered: {existing_class}"
    )


def get_initial_data_manager_class(
    manager_class_id: str,
) -> type[initial_data_manager_interface.InitialDataManagerInterface]:
  """Returns the initial data manager class with the given id."""
  manager_class = _INITIAL_DATA_MANAGER_REGISTRY.get(manager_class_id)
  if manager_class is None:
    raise ValueError(
        f"initial data manager with id '{manager_class_id}' is not registered."
        " Please import the module that contains the call"
        " register_initial_data_manager(manager_class) where"
        f" manager_class.get_id() == '{manager_class_id}'"
    )
  return manager_class
