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

  def bulk_remove(self, filepaths: Collection[str]) -> dict[str, str]:
    errors: dict[str, str] = {}
    by_depth = _group_by_depth(filepaths)
    for depth in sorted(by_depth.keys(), reverse=True):
      for path in by_depth[depth]:
        try:
          self.remove(path)
        except OSError as e:
          errors[path] = str(e)
    return errors

  def bulk_make_dirs(self, dirpaths: Collection[str]) -> dict[str, str]:
    errors: dict[str, str] = {}
    for dirpath in dirpaths:
      try:
        self.make_dirs(dirpath)
      except OSError as e:
        errors[dirpath] = str(e)
    return errors

  def bulk_write(self, files: Mapping[str, bytes]) -> dict[str, str]:
    errors: dict[str, str] = {}
    for filepath, data in files.items():
      try:
        with open(filepath, 'wb') as f:
          f.write(data)
      except OSError as e:
        errors[filepath] = str(e)
    return errors

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
    res: dict[str, fs_interface.StatResult] = {}
    for p in filepaths:
      try:
        st = os.stat(p)
        res[p] = fs_interface.StatResult(
            is_dir=stat.S_ISDIR(st.st_mode),
            mtime_ns=st.st_mtime_ns,
            size=st.st_size,
        )
      except OSError as e:
        res[p] = fs_interface.StatResult(error=str(e))
    return res

  def walk(self, top: str) -> Iterator[tuple[str, list[str], list[str]]]:
    if not os.path.exists(top):
      return
    for dirpath, dirnames, filenames in os.walk(top):
      yield (
          dirpath,
          dirnames,
          [f for f in filenames if os.path.isfile(os.path.join(dirpath, f))],
      )

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
