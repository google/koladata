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
    self.assertEmpty(
        fs.bulk_make_dirs([sub_dir, nested_child, nested_parent])
    )
    self.assertTrue(fs.exists(sub_dir))
    self.assertTrue(fs.exists(nested_child))
    self.assertEmpty(
        fs.bulk_remove(
            [nested_child, nested_parent, os.path.join(test_dir, 'x')]
        )
    )

    f1 = os.path.join(test_dir, 'a.txt')
    f2 = os.path.join(sub_dir, 'b.txt')
    missing = os.path.join(test_dir, 'missing.txt')
    self.assertEmpty(fs.bulk_write({f1: b'hello', f2: b'world'}))

    # Per-path errors in bulk_make_dirs and bulk_write (e.g., file vs dir
    # conflict or missing parent directory).
    make_dir_errors = fs.bulk_make_dirs([f1, os.path.join(test_dir, 'ok_dir')])
    self.assertEqual(set(make_dir_errors.keys()), {f1})
    self.assertIsInstance(make_dir_errors[f1], str)
    self.assertNotEmpty(make_dir_errors[f1])
    self.assertTrue(fs.is_dir(os.path.join(test_dir, 'ok_dir')))
    fs.remove(os.path.join(test_dir, 'ok_dir'))

    bad_write_path = os.path.join(test_dir, 'no_such_dir', 'c.txt')
    write_errors = fs.bulk_write({f1: b'hello', bad_write_path: b'fail'})
    self.assertEqual(set(write_errors.keys()), {bad_write_path})
    self.assertIsInstance(write_errors[bad_write_path], str)
    self.assertNotEmpty(write_errors[bad_write_path])

    bulk_stats = fs.bulk_stat([f1, sub_dir, missing])
    self.assertCountEqual(bulk_stats.keys(), [f1, sub_dir, missing])
    self.assertIsNone(bulk_stats[f1].error)
    self.assertFalse(bulk_stats[f1].is_dir)
    self.assertGreater(bulk_stats[f1].mtime_ns, 0)
    self.assertEqual(bulk_stats[f1].size, 5)
    self.assertIsNone(bulk_stats[sub_dir].error)
    self.assertTrue(bulk_stats[sub_dir].is_dir)
    self.assertIsInstance(bulk_stats[missing].error, str)
    self.assertNotEmpty(bulk_stats[missing].error)
    self.assertFalse(bulk_stats[missing].is_dir)
    self.assertEqual(bulk_stats[missing].mtime_ns, 0)
    self.assertEqual(bulk_stats[missing].size, 0)

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
    # deletion ordering, and include `missing` to verify per-path errors.
    remove_errors = fs.bulk_remove([f1, sub_dir, f2, missing])
    self.assertEqual(set(remove_errors.keys()), {missing})
    self.assertIsInstance(remove_errors[missing], str)
    self.assertNotEmpty(remove_errors[missing])
    self.assertFalse(fs.exists(f1))
    self.assertFalse(fs.exists(f2))
    self.assertFalse(fs.exists(sub_dir))

  def test_file_system_interaction_walk_excludes_non_regular_files(self):
    fs = fs_implementation.FileSystemInteraction()
    test_dir = self.create_tempdir().full_path
    regular_file = os.path.join(test_dir, 'regular.txt')
    self.assertEmpty(fs.bulk_write({regular_file: b'ok'}))
    os.mkfifo(os.path.join(test_dir, 'pipe.fifo'))
    os.symlink(
        os.path.join(test_dir, 'nonexistent_target'),
        os.path.join(test_dir, 'broken_link'),
    )

    walked = {
        dirpath: (sorted(dirnames), sorted(filenames))
        for dirpath, dirnames, filenames in fs.walk(test_dir)
    }
    self.assertEqual(walked, {test_dir: ([], ['regular.txt'])})


if __name__ == '__main__':
  absltest.main()
