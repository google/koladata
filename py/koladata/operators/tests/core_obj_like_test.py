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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


def generate_qtypes():
  for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for attrs_type in [
        arolla.make_namedtuple_qtype(),
        arolla.make_namedtuple_qtype(a=DATA_SLICE),
        arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
    ]:
      yield DATA_SLICE, itemid_arg_type, attrs_type, arolla.types.INT64, DATA_SLICE


QTYPES = list(generate_qtypes())


class CoreObjLikeTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape_and_mask_from = ds([6, 7, 8], schema_constants.INT32)
    x = kde.core.obj_like(shape_and_mask_from).eval()
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape_and_mask_from = ds(0)
    x = kde.core.obj_like(shape_and_mask_from).eval()
    self.assertIsNotNone(x.db)
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape_and_mask_from = ds([[6, 7, 8], [6, 7, 8]])
    x = kde.core.obj_like(
        shape_and_mask_from, x=2, a=1, b='p', c=fns.list([5, 6])
    ).eval()
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    testing.assert_equal(x.x.no_bag(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_bag(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_bag(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    testing.assert_equal(x.x.no_bag(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_bag(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_bag(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(
        x.c[:].no_bag(),
        ds([[[5, 6], [5, 6], [5, 6]], [[5, 6], [5, 6], [5, 6]]]),
    )
    self.assertFalse(x.is_mutable())

  def test_sparsity_item_with_empty_attr(self):
    x = kde.core.obj_like(ds(None), a=42).eval()
    testing.assert_equal(
        kde.has._eval(x).no_db(), ds(None, schema_constants.MASK)
    )

  def test_sparsity_all_empty_slice(self):
    x = kde.core.obj_like(ds([None, None]), a=42).eval()
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        kde.has._eval(x).no_db(), ds([None, None], schema_constants.MASK)
    )
    testing.assert_equal(
        x.a, ds([None, None], schema_constants.OBJECT).with_db(x.db)
    )

  def test_adopt_bag(self):
    x = kde.core.obj_like(ds(1), a='abc').eval()
    y = kde.core.obj_like(x, x=x).eval()
    # y.db is merged with x.db, so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_db(y.db))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_db(x.db))
    testing.assert_equal(y.x.a.no_db().get_schema(), schema_constants.TEXT)

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_like._eval(ds([[1, 1], [1]]))
    x = kde.core.obj_like(itemid, a=42, itemid=itemid).eval()
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.a.no_db(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_db().as_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).as_itemid()
    assert itemid.db is not None
    x = kde.core.obj_like(itemid, a=42, itemid=itemid).eval()
    with self.assertRaisesRegex(
        ValueError, "attribute 'non_existent' is missing"
    ):
      _ = x.non_existent

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError, "missing required positional argument: 'shape_and_mask_from'"
    ):
      _ = kde.core.obj_like().eval()

  def test_fails_with_shape_input(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_SLICE'):
      _ = kde.core.obj_like(ds(0).get_shape()).eval()

  def test_non_determinism(self):
    x = ds([1, None, 3])
    res_1 = expr_eval.eval(kde.core.obj_like(x, a=5))
    res_2 = expr_eval.eval(kde.core.obj_like(x, a=5))
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1.a.no_db(), res_2.a.no_db())

    expr = kde.core.obj_like(x, a=5)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1.a.no_db(), res_2.a.no_db())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.obj_like,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.obj_like(I.x)))
    self.assertTrue(view.has_data_slice_view(kde.core.obj_like(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.obj_like, kde.obj_like))

  def test_repr(self):
    self.assertIn(
        "kde.core.obj_like(I.x, unspecified, M.namedtuple.make('a', I.y),"
        ' L._koladata_hidden_seed_leaf + int64{',
        repr(kde.core.obj_like(I.x, a=I.y)),
    )


if __name__ == '__main__':
  absltest.main()