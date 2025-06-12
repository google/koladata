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

"""Python functions used in serving.bzl."""

import importlib
from typing import Any

from arolla import arolla
from koladata import kd


def _import_object(path: str) -> Any:
  """Imports an object from a path."""
  module_name, _, object_name = path.rpartition('.')
  try:
    module = importlib.import_module(module_name)
  except ImportError as e:
    raise ValueError(f'Failed to import module {module_name}') from e
  return getattr(module, object_name)


def trace_py_fn(function_path: str) -> kd.types.DataSlice:
  """Traces a Python function into a Koda functor."""
  fn = _import_object(function_path)
  return kd.fn(fn)


def serialize_slices(slices: dict[str, kd.types.DataSlice]):
  """Serializes Koda slices into bytes."""
  names = kd.slice(list(slices.keys()))
  return arolla.s11n.riegeli_dumps_many([names, *slices.values()], [])
