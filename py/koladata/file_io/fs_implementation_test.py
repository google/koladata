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
from koladata.file_io import fs_implementation
from koladata.file_io import fs_interface


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

  @parameterized.named_parameters(
      ('FileSystemInteraction', fs_implementation.FileSystemInteraction()),
  )
  def test_make_dirs_permissions(self, fs: fs_interface.FileSystemInterface):
    test_dir = self.create_tempdir().full_path

    # Not group-writable (0o750).
    old_umask = os.umask(0o027)
    try:
      subdir_no_group = os.path.join(test_dir, 'no_group')
      fs.make_dirs(subdir_no_group)
      self.assertEqual(os.stat(subdir_no_group).st_mode & 0o777, 0o750)

      # Group-writable (0o770).
      os.umask(0o007)
      subdir_group = os.path.join(test_dir, 'group')
      fs.make_dirs(subdir_group)
      self.assertEqual(os.stat(subdir_group).st_mode & 0o777, 0o770)
    finally:
      os.umask(old_umask)

  @parameterized.named_parameters(
      ('FileSystemInteraction', fs_implementation.FileSystemInteraction()),
  )
  def test_stat_walk_and_bulk_operations(
      self, fs: fs_interface.FileSystemInterface
  ):
    test_dir = self.create_tempdir().full_path
    sub_dir = os.path.join(test_dir, 'sub')
    nested_child = os.path.join(test_dir, 'x', 'y', 'z')
    nested_parent = os.path.join(test_dir, 'x', 'y')
    fs.bulk_make_dirs([sub_dir, nested_child, nested_parent])
    self.assertTrue(fs.exists(sub_dir))
    self.assertTrue(fs.exists(nested_child))
    fs.bulk_remove([nested_child, nested_parent, os.path.join(test_dir, 'x')])

    f1 = os.path.join(test_dir, 'a.txt')
    f2 = os.path.join(sub_dir, 'b.txt')
    missing = os.path.join(test_dir, 'missing.txt')
    fs.bulk_write({f1: b'hello', f2: b'world'})

    bulk_stats = fs.bulk_stat([f1, sub_dir])
    self.assertCountEqual(bulk_stats.keys(), [f1, sub_dir])
    self.assertFalse(bulk_stats[f1].is_dir)
    self.assertGreater(bulk_stats[f1].mtime_ns, 0)
    self.assertEqual(bulk_stats[f1].size, 5)
    self.assertTrue(bulk_stats[sub_dir].is_dir)

    with self.assertRaises(Exception):
      fs.bulk_stat([f1, missing])

    walked = {
        dirpath: (sorted(dirnames), sorted(filenames))
        for dirpath, dirnames, filenames in fs.walk(test_dir)
    }
    self.assertEqual(
        walked,
        {
            test_dir: (['sub'], ['a.txt']),
            sub_dir: ([], ['b.txt']),
        },
    )
    self.assertEmpty(list(fs.walk(os.path.join(test_dir, 'non_existent'))))

    # Pass parent `sub_dir` before child `f2` to verify child-before-parent
    # deletion ordering.
    fs.bulk_remove([f1, sub_dir, f2])
    self.assertFalse(fs.exists(f1))
    self.assertFalse(fs.exists(f2))
    self.assertFalse(fs.exists(sub_dir))

    with self.assertRaises(Exception):
      fs.bulk_remove([missing])


if __name__ == '__main__':
  absltest.main()
