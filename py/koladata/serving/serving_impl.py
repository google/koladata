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
from koladata.type_checking import type_checking


def _import_object(path: str) -> Any:
  """Imports an object from a path."""
  module_name, _, object_name = path.rpartition('.')
  try:
    with type_checking.disable_traced_type_checking():
      module = importlib.import_module(module_name)
  except ImportError as e:
    raise ValueError(f'Failed to import module {module_name}') from e
  return getattr(module, object_name)


def trace_py_fn(function_path: str) -> kd.types.DataSlice:
  """Traces a Python function into a Koda functor."""
  with type_checking.disable_traced_type_checking():
    fn = _import_object(function_path)
    fn = kd.fn(fn)
  return fn


def serialize_slices(slices: dict[str, kd.types.DataSlice]):
  """Serializes Koda slices into bytes."""
  names = kd.slice(list(slices.keys()))
  return arolla.s11n.riegeli_dumps_many([names, *slices.values()], [])


def serialize_slices_into(
    slices: dict[str, kd.types.DataSlice], output_paths: list[str]
):
  """Serializes Koda slices into the given files."""
  if len(slices) != len(output_paths):
    raise ValueError(
        'Number of slices must match number of output paths, but got'
        f' {len(slices)} and {len(output_paths)}'
    )
  for ds, output_path in zip(slices.values(), output_paths):
    serialized = arolla.s11n.riegeli_dumps_many([ds], [])
    with open(output_path, 'wb') as f:
      f.write(serialized)
