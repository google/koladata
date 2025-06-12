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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


class DirTest(absltest.TestCase):

  def test_entity(self):
    db = fns.bag()
    x = db.new(a=1, b='abc')
    self.assertEqual(fns.dir(x), ['a', 'b'])
    self.assertEqual(fns.dir(ds([x])), ['a', 'b'])

  def test_object(self):
    db = fns.bag()
    x = db.obj(a=1, b='abc')
    self.assertEqual(fns.dir(x), ['a', 'b'])
    self.assertEqual(fns.dir(ds([x])), ['a', 'b'])
    # Returns the intersection of attributes.
    self.assertEqual(fns.dir(ds([x, db.obj(a='def', c=123)])), ['a'])

  def test_primitives(self):
    x = ds([1, 2, 3]).with_bag(fns.bag())
    self.assertEqual(fns.dir(x), [])

  def test_no_bag_error(self):
    db = fns.bag()
    x = db.obj(a=1, b='abc')
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      fns.dir(x.no_bag())


if __name__ == '__main__':
  absltest.main()
