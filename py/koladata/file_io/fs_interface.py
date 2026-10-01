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

"""Interface to interact with the file system."""

import dataclasses
from typing import Collection, IO, Iterator, Mapping


@dataclasses.dataclass(frozen=True)
class StatResult:
  """Metadata for a file or directory."""

  is_dir: bool
  mtime_ns: int
  size: int


class FileSystemInterface:
  """Interface to interact with the file system."""

  def exists(self, filepath: str) -> bool:
    """Returns True if the file or directory exists."""
    raise NotImplementedError

  def remove(self, filepath: str):
    """Removes a file or an empty directory."""
    raise NotImplementedError

  def bulk_remove(self, filepaths: Collection[str]):
    """Removes multiple files or directories.

    Raises an exception on error; it is not specified whether some operations
    may be partially completed.

    Args:
      filepaths: Collection of file or directory paths to remove.
    """
    raise NotImplementedError

  def bulk_write(self, files: Mapping[str, bytes]):
    """Writes multiple files with their respective byte contents.

    Raises an exception on error; it is not specified whether some operations
    may be partially completed.

    Args:
      files: Mapping from file paths to their byte contents.
    """
    raise NotImplementedError

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    """Opens a file with the given mode."""
    raise NotImplementedError

  def make_dirs(self, dirpath: str):
    """Creates a directory path (and intermediate directories) if not exists."""
    raise NotImplementedError

  def bulk_make_dirs(self, dirpaths: Collection[str]):
    """Creates multiple directory paths (and intermediate directories).

    Raises an exception on error; it is not specified whether some operations
    may be partially completed.

    Args:
      dirpaths: Collection of directory paths to create.
    """
    raise NotImplementedError

  def is_dir(self, filepath: str) -> bool:
    """Returns True if the path is an existing directory."""
    raise NotImplementedError

  def glob(self, pattern: str) -> Collection[str]:
    """Returns a list of paths matching a pathname pattern."""
    raise NotImplementedError

  def bulk_stat(self, filepaths: Collection[str]) -> dict[str, StatResult]:
    """Returns metadata for multiple files or directories.

    Raises an exception on error, for example if at least one file or directory
    does not exist.

    Args:
      filepaths: Collection of file or directory paths to inspect.
    """
    raise NotImplementedError

  def walk(self, top: str) -> Iterator[tuple[str, list[str], list[str]]]:
    """Directory tree generator yielding (dirpath, dirnames, filenames).

    Can be called on non-existing directories, in which case the returned
    generator is empty.

    Args:
      top: Root directory path to walk.
    """
    raise NotImplementedError

  def rename(self, oldpath: str, newpath: str, overwrite: bool = False):
    """Rename or move a file or a directory.

    Atomicity should be an aspirational goal for implementations, especially for
    file-to-file renaming. It is unfortunately not an API guarantee at present.

    Args:
      oldpath: the file or directory to be moved.
      newpath: the new name of the file or directory.
      overwrite: boolean; if False, it is an error for newpath to be occupied by
        an existing file or directory.
    """
    raise NotImplementedError
