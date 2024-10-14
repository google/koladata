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
      yield DATA_SLICE, itemid_arg_type, attrs_type, DATA_SLICE


QTYPES = list(generate_qtypes())


class CoreObjShapedAsTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape_from = ds([6, 7, 8])
    x = kde.core.obj_shaped_as(shape_from).eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape_from = ds(0)
    x = kde.core.obj_shaped_as(shape_from).eval()
    self.assertIsNotNone(x.db)
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape_from = ds([[6, 7, 8], [6, 7, 8]])
    x = kde.core.obj_shaped_as(
        shape_from, x=2, a=1, b='p', c=fns.list([5, 6])
    ).eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(x.x.no_db(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_db(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_db(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(x.x.no_db(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_db(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_db(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(
        x.c[:].no_db(),
        ds([[[5, 6], [5, 6], [5, 6]], [[5, 6], [5, 6], [5, 6]]]),
    )
    self.assertFalse(x.is_mutable())

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as._eval(ds([[1, 1], [1]]))
    x = kde.core.obj_shaped_as(itemid, a=42, itemid=itemid).eval()
    testing.assert_equal(x.a.no_db(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_db().as_itemid(), itemid)

  def test_itemid_from_different_db(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).as_itemid()
    assert itemid.db is not None
    x = kde.core.obj_shaped_as(itemid, a=42, itemid=itemid).eval()
    with self.assertRaisesRegex(
        ValueError, "attribute 'non_existent' is missing"
    ):
      _ = x.non_existent

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError, "missing required positional argument: 'shape_from'"
    ):
      _ = kde.core.obj_shaped_as().eval()

  def test_fails_with_shape_input(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_SLICE'):
      _ = kde.core.obj_shaped_as(ds(0).get_shape()).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.obj_shaped_as,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.obj_shaped_as(I.x)))
    self.assertTrue(
        view.has_data_slice_view(kde.core.obj_shaped_as(I.x, a=I.y))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.obj_shaped_as, kde.obj_shaped_as)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.obj_shaped_as(I.x, a=I.y)),
        'kde.core.obj_shaped_as(I.x, itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()