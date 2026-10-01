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

"""Implementation for interacting with the file system."""

from concurrent import futures
import glob
import os
import stat
from typing import Collection, IO, Iterator, Mapping

from koladata.file_io import fs_interface

_MAX_WORKERS = 256


def _path_depth(path: str) -> int:
  return os.path.normpath(path).count(os.sep)


def _group_by_depth(paths: Collection[str]) -> dict[int, list[str]]:
  by_depth: dict[int, list[str]] = {}
  for p in set(paths):
    by_depth.setdefault(_path_depth(p), []).append(p)
  return by_depth


class FileSystemInteraction(fs_interface.FileSystemInterface):
  """Interacts with the file system."""

  def exists(self, filepath: str) -> bool:
    return os.path.exists(filepath)

  def remove(self, filepath: str):
    if os.path.isdir(filepath) and not os.path.islink(filepath):
      os.rmdir(filepath)
    else:
      os.remove(filepath)

  def bulk_remove(self, filepaths: Collection[str]):
    by_depth = _group_by_depth(filepaths)
    for depth in sorted(by_depth.keys(), reverse=True):
      for path in by_depth[depth]:
        self.remove(path)

  def bulk_make_dirs(self, dirpaths: Collection[str]):
    for dirpath in dirpaths:
      self.make_dirs(dirpath)

  def bulk_write(self, files: Mapping[str, bytes]):
    for filepath, data in files.items():
      with open(filepath, 'wb') as f:
        f.write(data)

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    return open(filepath, mode)

  def make_dirs(self, dirpath: str):
    os.makedirs(dirpath, exist_ok=True)

  def is_dir(self, filepath: str) -> bool:
    return os.path.isdir(filepath)

  def glob(self, pattern: str) -> Collection[str]:
    return glob.glob(pattern)

  def bulk_stat(
      self, filepaths: Collection[str]
  ) -> dict[str, fs_interface.StatResult]:
    res = {}
    for p in filepaths:
      st = os.stat(p)
      res[p] = fs_interface.StatResult(
          is_dir=stat.S_ISDIR(st.st_mode),
          mtime_ns=st.st_mtime_ns,
          size=st.st_size,
      )
    return res

  def walk(self, top: str) -> Iterator[tuple[str, list[str], list[str]]]:
    if not os.path.exists(top):
      return
    yield from os.walk(top)

  def rename(self, oldpath: str, newpath: str, overwrite: bool = False):
    if not overwrite:
      # On Unix systems, the `os.rename` call below will not raise an error if
      # the destination already exists. We have to do the check ourselves.
      # Since another process could write to `newpath` after our check and
      # before the rename, file-to-file renaming is unfortunately not atomic on
      # Unix systems with the current API of the os module.
      if self.exists(newpath):
        raise ValueError(f'Destination {newpath} already exists.')
      os.rename(oldpath, newpath)
      return

    os.replace(oldpath, newpath)
