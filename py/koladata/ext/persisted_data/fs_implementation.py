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

import glob
import os
from typing import Collection, IO

from koladata.ext.persisted_data import fs_interface


class FileSystemInteraction(fs_interface.FileSystemInterface):
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
