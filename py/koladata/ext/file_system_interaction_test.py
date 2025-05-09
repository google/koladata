# Copyright 2024 Google LLC
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
from koladata.ext import file_system_interaction as fsi


class DataSliceAndBagPersistenceTest(absltest.TestCase):

  def test_dataslice_write_read(self):
    ds = kd.slice([1, 2, 3])
    fsilepath = os.path.join(self.create_tempdir().full_path, 'test_slice.kd')
    fsi.write_slice_to_file(ds, fsilepath)
    kd.testing.assert_equal(fsi.read_slice_from_file(fsilepath), ds)

    new_ds = kd.slice([4, 5, 6])
    with self.assertRaisesRegex(ValueError, 'already exists'):
      fsi.write_slice_to_file(new_ds, fsilepath)
    fsi.write_slice_to_file(new_ds, fsilepath, overwrite=True)
    kd.testing.assert_equal(fsi.read_slice_from_file(fsilepath), new_ds)

  def test_databag_write_read(self):
    db = kd.new(x=1, y=2).get_bag()
    fsilepath = os.path.join(self.create_tempdir().full_path, 'test_bag.kd')
    fsi.write_bag_to_file(db, fsilepath)
    kd.testing.assert_equivalent(fsi.read_bag_from_file(fsilepath), db)

    new_db = kd.new(x=3, y=4).get_bag()
    with self.assertRaisesRegex(ValueError, 'already exists'):
      fsi.write_bag_to_file(new_db, fsilepath)
    fsi.write_bag_to_file(new_db, fsilepath, overwrite=True)
    kd.testing.assert_equivalent(fsi.read_bag_from_file(fsilepath), new_db)


if __name__ == '__main__':
  absltest.main()
