# Copyright 2024 Google LLC
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

"""Utilities to interact with the file system."""

import glob
import os
from typing import Collection, IO

from koladata import kd


class FileSystemInterface:
  """Interface to interact with the file system."""

  def exists(self, filepath: str) -> bool:
    raise NotImplementedError

  def remove(self, filepath: str):
    raise NotImplementedError

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    raise NotImplementedError

  def make_dirs(self, dirpath: str):
    raise NotImplementedError

  def glob(self, pattern: str) -> Collection[str]:
    raise NotImplementedError


class FileSystemInteraction(FileSystemInterface):
  """Interacts with the file system."""

  def exists(self, filepath: str) -> bool:
    return os.path.exists(filepath)

  def remove(self, filepath: str):
    os.remove(filepath)

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    return open(filepath, mode)

  def make_dirs(self, dirpath: str):
    os.makedirs(dirpath, exist_ok=True)

  def glob(self, pattern: str) -> Collection[str]:
    return glob.glob(pattern)


def write_slice_to_file(
    ds: kd.types.DataSlice,
    filepath: str,
    *,
    overwrite: bool = False,
    riegeli_options: str = 'snappy',
    fs: FileSystemInterface = FileSystemInteraction(),
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
    filepath: str, *, fs: FileSystemInterface = FileSystemInteraction()
) -> kd.types.DataSlice:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())


def write_bag_to_file(
    ds: kd.types.DataBag,
    filepath: str,
    *,
    overwrite: bool = False,
    riegeli_options: str = 'snappy',
    fs: FileSystemInterface = FileSystemInteraction(),
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
    filepath: str, *, fs: FileSystemInterface = FileSystemInteraction()
) -> kd.types.DataBag:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())
