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
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ObjLikeTest(absltest.TestCase):

  def test_item(self):
    x = fns.obj_like(
        ds(1),
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.b.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )

    x = fns.obj_like(ds(None), a=42)
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds(None, schema_constants.MASK)
    )
    testing.assert_equal(
        x.a, ds(None, schema_constants.OBJECT).with_bag(x.get_bag())
    )

  def test_slice(self):
    x = fns.obj_like(
        ds([[1, None], [1]]),
        a=ds([[1, 2], [3]]),
        b=fns.obj(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.a, ds([[1, None], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', None], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', None], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(x.b.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        x.b.bb.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.c.get_schema(), schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_broadcast_attrs(self):
    x = fns.obj_like(ds([1, 1]), a=42, b='xyz')
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))

  def test_broadcast_error(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'cannot be expanded'):
      fns.obj_like(ds([1, 1]), a=ds([42]))

  def test_all_empty_slice(self):
    x = fns.obj_like(ds([None, None]), a=42)
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds([None, None], schema_constants.MASK)
    )
    testing.assert_equal(
        x.a, ds([None, None], schema_constants.OBJECT).with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = fns.obj_like(ds(1))
    x.a = 'abc'
    y = fns.obj_like(x, x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    )
    x = fns.obj_like(ds([[1, None], [1]]), a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, None], [42]]))
    testing.assert_equal(
        x.no_bag().get_itemid(), itemid & expr_eval.eval(kde.has(x))
    )

  def test_itemid_from_different_bag(self):
    itemid = fns.obj(non_existent=ds([[42, 42], [42]])).get_itemid()
    assert itemid.get_bag() is not None
    # Successful.
    x = fns.obj_like(ds([[1, None], [1]]), a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = x.non_existent

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.obj_like(ds(1), a=1, b='a', db=db)
    testing.assert_equal(db, x.get_bag())

  def test_schema_arg(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'please use new'):
      fns.obj_like(ds(1), a=1, b='a', schema=schema_constants.INT32)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.obj_like(ds(1), x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.obj_like(ds(1), x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.obj_like(ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.obj_like(ds([1, 2, 3]), dct={'a': 42})

  def test_alias(self):
    self.assertIs(fns.obj_like, fns.core.obj_like)


if __name__ == '__main__':
  absltest.main()
