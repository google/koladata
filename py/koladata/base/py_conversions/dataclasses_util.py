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

"""Module for creating dataclasses from the list of attribute names."""

import dataclasses
from typing import Any


def _eq(x, y):
  """Checks whether two dataclasses are equal ignoring types."""
  return dataclasses.is_dataclass(y) and dataclasses.asdict(
      x
  ) == dataclasses.asdict(y)


def make_dataclass(attr_names):
  obj_class = dataclasses.make_dataclass(
      'Obj',
      [
          (attr_name, Any, dataclasses.field(default=None))
          for attr_name in attr_names
      ],
      eq=False,
  )
  obj_class.__eq__ = _eq
  return obj_class


def fields_names_and_values(py_obj):
  """Returns list of (attribute name, value) for a dataclass."""
  if not dataclasses.is_dataclass(py_obj):
    return None
  return [
      (field.name, getattr(py_obj, field.name))
      for field in dataclasses.fields(py_obj)
  ]
