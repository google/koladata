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

from typing import Collection, IO


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

  def is_dir(self, filepath: str) -> bool:
    raise NotImplementedError

  def glob(self, pattern: str) -> Collection[str]:
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
