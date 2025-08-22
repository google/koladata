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
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import qtypes


class MutableBagTest(absltest.TestCase):

  def test_docstring(self):
    self.assertEqual(
        fns.mutable_bag.__doc__,
        'Returns an empty mutable DataBag. Only works in eager mode.',
    )

  def test_qtype(self):
    self.assertEqual(fns.mutable_bag().qtype, qtypes.DATA_BAG)

  def test_create(self):
    self.assertIsInstance(fns.mutable_bag(), data_bag.DataBag)
    self.assertTrue(fns.mutable_bag().is_mutable())

  def test_repr(self):
    self.assertIn(
        '[1, 2]', repr(fns.mutable_bag().list([1, 2]).get_bag().contents_repr())
    )

  def test_equality(self):
    db = fns.mutable_bag()
    testing.assert_equal(db, db)
    with self.assertRaises(AssertionError):
      testing.assert_equal(db, fns.mutable_bag())
    db.new(a=1, b='text')
    testing.assert_equal(db, db)

  def test_equivalence(self):
    testing.assert_equivalent(fns.mutable_bag(), fns.mutable_bag())
    db1 = fns.mutable_bag()
    db2 = fns.mutable_bag()
    entity = db1.new(a=1, b='text')
    with self.assertRaises(AssertionError):
      testing.assert_equivalent(db1, db2)
    entity.with_bag(db2).set_attr('a', 1)
    entity.with_bag(db2).set_attr('b', 'text')
    testing.assert_equivalent(db1, db2)


if __name__ == '__main__':
  absltest.main()
