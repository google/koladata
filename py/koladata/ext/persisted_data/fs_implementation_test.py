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

    # Test is_dir.
    self.assertTrue(fs.is_dir(test_dir))
    self.assertTrue(fs.is_dir(os.path.join(test_dir, 'subdir')))
    self.assertFalse(fs.is_dir(os.path.join(test_dir, 'file.txt')))
    self.assertFalse(fs.is_dir(os.path.join(test_dir, 'non_existent_dir')))

    # Globbing.
    self.assertEqual(
        set(fs.glob(os.path.join(test_dir, '*'))),
        {os.path.join(test_dir, 'file.txt'), os.path.join(test_dir, 'subdir')},
    )
    self.assertEqual(set(fs.glob(os.path.join(test_dir, 'subdir', '*'))), set())

    # Remove a file.
    fs.remove(os.path.join(test_dir, 'file.txt'))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'file.txt')))

    # Renaming: file-to-file.
    test_dir = self.create_tempdir().full_path
    with fs.open(os.path.join(test_dir, 'file.txt'), 'w') as f:
      f.write('test_content')
    # Case 1: overwrite=False, destination does not exist.
    fs.rename(
        os.path.join(test_dir, 'file.txt'),
        os.path.join(test_dir, 'renamed_file.txt'),
    )
    self.assertTrue(fs.exists(os.path.join(test_dir, 'renamed_file.txt')))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'file.txt')))
    with fs.open(os.path.join(test_dir, 'file.txt'), 'w') as f:
      f.write('new_content')
    # Case 2: overwrite=False, destination already exists.
    with self.assertRaises(Exception):
      fs.rename(
          os.path.join(test_dir, 'file.txt'),
          os.path.join(test_dir, 'renamed_file.txt'),
          # Overwrite is False by default.
      )
    # Case 3: overwrite=True, destination already exists.
    fs.rename(
        os.path.join(test_dir, 'file.txt'),
        os.path.join(test_dir, 'renamed_file.txt'),
        overwrite=True,
    )
    with fs.open(os.path.join(test_dir, 'renamed_file.txt'), 'r') as f:
      self.assertEqual(f.read(), 'new_content')
    self.assertFalse(fs.exists(os.path.join(test_dir, 'file.txt')))
    # Case 4: overwrite=True, destination does not exist.
    fs.rename(
        os.path.join(test_dir, 'renamed_file.txt'),
        os.path.join(test_dir, 'file.txt'),
        overwrite=True,
    )
    with fs.open(os.path.join(test_dir, 'file.txt'), 'r') as f:
      self.assertEqual(f.read(), 'new_content')
    self.assertFalse(fs.exists(os.path.join(test_dir, 'renamed_file.txt')))

    # Renaming: directory-to-directory.
    fs.make_dirs(os.path.join(test_dir, 'subdir'))
    # Case 1: overwrite=False, destination does not exist.
    fs.rename(
        os.path.join(test_dir, 'subdir'),
        os.path.join(test_dir, 'renamed_subdir'),
    )
    self.assertTrue(fs.exists(os.path.join(test_dir, 'renamed_subdir')))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'subdir')))
    fs.make_dirs(os.path.join(test_dir, 'subdir'))
    # Case 2: overwrite=False, destination already exists.
    with self.assertRaises(Exception):
      fs.rename(
          os.path.join(test_dir, 'subdir'),
          os.path.join(test_dir, 'renamed_subdir'),
          # Overwrite is False by default.
      )
    # Case 3: overwrite=True, destination already exists.
    fs.rename(
        os.path.join(test_dir, 'subdir'),
        os.path.join(test_dir, 'renamed_subdir'),
        overwrite=True,
    )
    self.assertTrue(fs.exists(os.path.join(test_dir, 'renamed_subdir')))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'subdir')))
    # Case 4: overwrite=True, destination does not exist.
    fs.rename(
        os.path.join(test_dir, 'renamed_subdir'),
        os.path.join(test_dir, 'subdir'),
        overwrite=True,
    )
    self.assertTrue(fs.exists(os.path.join(test_dir, 'subdir')))
    self.assertFalse(fs.exists(os.path.join(test_dir, 'renamed_subdir')))


if __name__ == '__main__':
  absltest.main()
