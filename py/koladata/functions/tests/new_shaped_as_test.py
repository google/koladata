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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class NewShapedAsTest(absltest.TestCase):

  def test_item(self):
    x = fns.new_shaped_as(
        ds(1),
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.TEXT),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b, schema_constants.TEXT.with_bag(x.get_bag())
    )

  def test_slice(self):
    x = fns.new_shaped_as(
        ds([['a', 'b'], ['c']]),
        a=ds([[1, 2], [3]]),
        b=fns.new(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b.bb, schema_constants.TEXT.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().c, schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as._eval(ds([[1, 1], [1]]))  # pylint: disable=protected-access
    x = fns.new_shaped_as(itemid, a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.new_shaped_as(ds(1), a=1, b='a', db=db)
    testing.assert_equal(db, x.get_bag())

  def test_schema_arg(self):
    schema = fns.new_schema(a=schema_constants.FLOAT32, b=schema_constants.TEXT)
    x = fns.new_shaped_as(ds([1, 2]), a=42, b='xyz', schema=schema)
    self.assertEqual(dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42.0, 42.0]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.get_schema().a.with_bag(None), schema_constants.FLOAT32
    )
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.with_bag(None), schema_constants.TEXT)

  def test_update_schema_arg(self):
    schema = fns.new_schema(a=schema_constants.INT32)
    x = fns.new_shaped_as(
        ds([1, 2]),
        a=42,
        b='xyz',
        schema=schema,
        update_schema=True,
    )
    self.assertEqual(dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.get_schema().a.with_bag(None), schema_constants.INT32
    )
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.with_bag(None), schema_constants.TEXT)


if __name__ == '__main__':
  absltest.main()
