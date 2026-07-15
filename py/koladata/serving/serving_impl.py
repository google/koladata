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

from collections.abc import Mapping
import importlib
from typing import Any

from arolla import arolla
from koladata import kd
from koladata.serving import determinism
from koladata.serving import serving_impl_clib
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


def trace_py_fn(
    function_path: str, experimental_deterministic_mode: bool = False
) -> kd.types.DataSlice:
  """Traces a Python function into a Koda functor."""
  with type_checking.disable_traced_type_checking():
    fn = _import_object(function_path)
    fn = kd.fn(fn)

    if experimental_deterministic_mode:
      fn = determinism.Determinizer(
          seed=function_path, strip_source_locations=True
      ).make_deterministic(fn)
  return fn


def parallel_transform(fn: kd.types.DataItem) -> kd.types.DataItem:
  """Applies kd.parallel.transform to the given functor."""
  return kd.parallel.transform(fn)


def serialize_slices(slices: Mapping[str, kd.types.DataSlice]):
  """Serializes Koda slices into bytes."""
  names = kd.slice(list(slices.keys()))
  data = arolla.s11n.riegeli_dumps_many([names, *slices.values()], [])
  return serving_impl_clib.cleanup_bag_id(data)


def serialize_slices_into(
    path_to_slice: Mapping[str, kd.types.DataSlice]
):
  """Serializes Koda slices into the given files."""
  for path, ds in path_to_slice.items():
    serialized = arolla.s11n.riegeli_dumps_many([ds], [])
    with open(path, 'wb') as f:
      f.write(serving_impl_clib.cleanup_bag_id(serialized))
