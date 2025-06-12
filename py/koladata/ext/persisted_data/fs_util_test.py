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
from koladata import kd
from koladata.ext.persisted_data import fs_util


class DataSliceAndBagPersistenceTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._fs = fs_util.get_default_file_system_interaction()

  def test_dataslice_write_read(self):
    ds = kd.slice([1, 2, 3])
    filepath = os.path.join(self.create_tempdir().full_path, 'test_slice.kd')
    fs_util.write_slice_to_file(self._fs, ds, filepath)
    kd.testing.assert_equal(
        fs_util.read_slice_from_file(self._fs, filepath), ds
    )

    new_ds = kd.slice([4, 5, 6])
    with self.assertRaisesRegex(ValueError, 'already exists'):
      fs_util.write_slice_to_file(self._fs, new_ds, filepath)
    fs_util.write_slice_to_file(self._fs, new_ds, filepath, overwrite=True)
    kd.testing.assert_equal(
        fs_util.read_slice_from_file(self._fs, filepath), new_ds
    )

  def test_databag_write_read(self):
    db = kd.new(x=1, y=2).get_bag()
    filepath = os.path.join(self.create_tempdir().full_path, 'test_bag.kd')
    fs_util.write_bag_to_file(self._fs, db, filepath)
    kd.testing.assert_equivalent(
        fs_util.read_bag_from_file(self._fs, filepath), db
    )

    new_db = kd.new(x=3, y=4).get_bag()
    with self.assertRaisesRegex(ValueError, 'already exists'):
      fs_util.write_bag_to_file(self._fs, new_db, filepath)
    fs_util.write_bag_to_file(self._fs, new_db, filepath, overwrite=True)
    kd.testing.assert_equivalent(
        fs_util.read_bag_from_file(self._fs, filepath), new_db
    )


if __name__ == '__main__':
  absltest.main()
