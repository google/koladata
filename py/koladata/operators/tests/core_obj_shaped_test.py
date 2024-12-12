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
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


def generate_qtypes():
  for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for attrs_type in [
        arolla.make_namedtuple_qtype(),
        arolla.make_namedtuple_qtype(a=DATA_SLICE),
        arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
    ]:
      yield JAGGED_SHAPE, itemid_arg_type, attrs_type, NON_DETERMINISTIC_TOKEN, DATA_SLICE


QTYPES = list(generate_qtypes())


class CoreObjShapedTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape = jagged_shape.create_shape(2, 3)
    x = kde.core.obj_shaped(shape).eval()
    testing.assert_equal(x.get_shape(), shape)
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape = jagged_shape.create_shape()
    x = kde.core.obj_shaped(shape).eval()
    self.assertIsNotNone(x.get_bag())
    testing.assert_equal(x.get_shape(), shape)
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape = jagged_shape.create_shape(2, 3)
    x = kde.core.obj_shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6])).eval()
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(x.x.no_bag(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_bag(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_bag(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(
        x.c[:].no_bag(),
        ds([[[5, 6], [5, 6], [5, 6]], [[5, 6], [5, 6], [5, 6]]]),
    )
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)
    self.assertFalse(x.is_mutable())

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    )
    x = kde.core.obj_shaped(itemid.get_shape(), a=42, itemid=itemid).eval()
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).get_itemid()
    assert itemid.get_bag() is not None
    x = kde.core.obj_shaped(itemid.get_shape(), a=42, itemid=itemid).eval()
    with self.assertRaisesRegex(
        ValueError, "attribute 'non_existent' is missing"
    ):
      _ = x.non_existent

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError, "missing 1 required positional argument: 'shape'"
    ):
      _ = kde.core.obj_shaped().eval()

  def test_fails_with_dataslice_input(self):
    with self.assertRaisesRegex(ValueError, 'expected JAGGED_SHAPE'):
      _ = kde.core.obj_shaped(ds(0)).eval()

  def test_non_determinism(self):
    shape = jagged_shape.create_shape(2, 3)
    res_1 = expr_eval.eval(
        kde.core.obj_shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    res_2 = expr_eval.eval(
        kde.core.obj_shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1.a.no_db(), res_2.a.no_db())

    expr = kde.core.obj_shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1.a.no_db(), res_2.a.no_db())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.obj_shaped,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.obj_shaped(I.x)))
    self.assertTrue(view.has_koda_view(kde.core.obj_shaped(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.obj_shaped, kde.obj_shaped))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.obj_shaped(I.x, a=I.y)),
        'kde.core.obj_shaped(I.x, itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
