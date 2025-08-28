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

import os

from absl.testing import absltest
from absl.testing import parameterized
from koladata.ext.persisted_data import fs_implementation
from koladata.ext.persisted_data import fs_interface


class FsImplementationTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('FileSystemInteraction', fs_implementation.FileSystemInteraction()),
  )
  def test_interactions_with_file_system(
      self, fs: fs_interface.FileSystemInterface
  ):
    test_dir = self.create_tempdir().full_path

    # Test existence.
    self.assertTrue(fs.exists(test_dir))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'file.txt')))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'subdir')))

    # Open and write/read a file.
    with fs.open(os.path.join(test_dir, 'file.txt'), 'w') as f:
      f.write('test_content')
    self.assertTrue(fs.exists(os.path.join(test_dir, 'file.txt')))
    with fs.open(os.path.join(test_dir, 'file.txt'), 'r') as f:
      self.assertEqual(f.read(), 'test_content')

    # Create a directory.
    fs.make_dirs(os.path.join(test_dir, 'subdir'))
    self.assertTrue(fs.exists(os.path.join(test_dir, 'subdir')))

    # Globbing.
    self.assertEqual(
        set(fs.glob(os.path.join(test_dir, '*'))),
        {os.path.join(test_dir, 'file.txt'), os.path.join(test_dir, 'subdir')},
    )
    self.assertEqual(set(fs.glob(os.path.join(test_dir, 'subdir', '*'))), set())

    # Remove a file.
    fs.remove(os.path.join(test_dir, 'file.txt'))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'file.txt')))


if __name__ == '__main__':
  absltest.main()
