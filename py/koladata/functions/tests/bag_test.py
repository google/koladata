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


class BagTest(absltest.TestCase):

  def test_docstring(self):
    self.assertEqual(fns.bag.__doc__, 'Returns an empty immutable DataBag.')

  def test_qtype(self):
    self.assertEqual(fns.bag().qtype, qtypes.DATA_BAG)

  def test_create(self):
    self.assertIsInstance(fns.bag(), data_bag.DataBag)
    self.assertFalse(fns.bag().is_mutable())

  def test_equality(self):
    db = fns.bag()
    testing.assert_equal(db, db)
    testing.assert_not_equal(db, fns.bag())

  def test_equivalence(self):
    testing.assert_equivalent(fns.bag(), fns.bag())

  def test_alias(self):
    self.assertIs(fns.bag, fns.bags.new)


if __name__ == '__main__':
  absltest.main()
