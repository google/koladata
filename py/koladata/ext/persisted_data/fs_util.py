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

"""Utilities for interacting with the file system."""

from koladata import kd
from koladata.ext.persisted_data import fs_implementation
from koladata.ext.persisted_data import fs_interface


def get_default_file_system_interaction():
  return fs_implementation.FileSystemInteraction()


def write_slice_to_file(
    fs: fs_interface.FileSystemInterface,
    ds: kd.types.DataSlice,
    filepath: str,
    *,
    overwrite: bool = False,
    riegeli_options: str | None = None,
):
  """Writes the given DataSlice to a file; overwrites the file if requested."""
  if fs.exists(filepath):
    if overwrite:
      fs.remove(filepath)
    else:
      raise ValueError(f'File {filepath} already exists.')
  with fs.open(filepath, 'wb') as f:
    f.write(kd.dumps(ds, riegeli_options=riegeli_options))


def read_slice_from_file(
    fs: fs_interface.FileSystemInterface,
    filepath: str,
) -> kd.types.DataSlice:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())


def write_bag_to_file(
    fs: fs_interface.FileSystemInterface,
    ds: kd.types.DataBag,
    filepath: str,
    *,
    overwrite: bool = False,
    riegeli_options: str | None = None,
):
  """Writes the given DataBag to a file; overwrites the file if requested."""
  if fs.exists(filepath):
    if overwrite:
      fs.remove(filepath)
    else:
      raise ValueError(f'File {filepath} already exists.')
  with fs.open(filepath, 'wb') as f:
    f.write(kd.dumps(ds, riegeli_options=riegeli_options))


def read_bag_from_file(
    fs: fs_interface.FileSystemInterface, filepath: str
) -> kd.types.DataBag:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())
