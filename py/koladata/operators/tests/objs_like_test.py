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
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


def generate_qtypes():
  for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for attrs_type in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES:
      yield DATA_SLICE, itemid_arg_type, attrs_type, NON_DETERMINISTIC_TOKEN, DATA_SLICE


QTYPES = list(generate_qtypes())


class ObjsLikeTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape_and_mask_from = ds([6, 7, 8], schema_constants.INT32)
    x = kde.objs.like(shape_and_mask_from).eval()
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape_and_mask_from = ds(0)
    x = kde.objs.like(shape_and_mask_from).eval()
    self.assertIsNotNone(x.get_bag())
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape_and_mask_from = ds([[6, 7, 8], [6, 7, 8]])
    x = kde.objs.like(
        shape_and_mask_from, x=2, a=1, b='p', c=fns.list([5, 6])
    ).eval()
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
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
    x = kde.objs.like(ds(None), a=42).eval()
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds(None, schema_constants.MASK)
    )

  def test_sparsity_all_empty_slice(self):
    x = kde.objs.like(ds([None, None]), a=42).eval()
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds([None, None], schema_constants.MASK)
    )
    testing.assert_equal(x.a, ds([None, None]).with_bag(x.get_bag()))

  def test_adopt_bag(self):
    x = kde.objs.like(ds(1), a='abc').eval()
    y = kde.objs.like(x, x=x).eval()
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_itemid())

    with self.subTest('present DataItem and present itemid'):
      x = expr_eval.eval(kde.objs.like(ds(1), a=42, itemid=itemid))
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = expr_eval.eval(kde.objs.like(ds(None), a=42, itemid=(itemid & None)))
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = expr_eval.eval(kde.objs.like(ds(None), a=42, itemid=itemid))
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesRegex(
          ValueError,
          'kd.objs.like: `itemid` only has 0 present items but 1 are required',
      ):
        _ = expr_eval.eval(kde.objs.like(ds(1), a=42, itemid=(itemid & None)))

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_itemid())
    id2 = expr_eval.eval(kde.allocation.new_itemid())
    id3 = expr_eval.eval(kde.allocation.new_itemid())

    with self.subTest('full DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.objs.like(ds([1, 1, 1]), a=42, itemid=ds([id1, id2, id3]))
      )
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesRegex(
          ValueError,
          'kd.objs.like: `itemid` only has 2 present items but 3 are required',
      ):
        _ = expr_eval.eval(
            kde.objs.like(ds([1, 1, 1]), a=42, itemid=ds([id1, None, id3]))
        )

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          'kd.objs.like: `itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.objs.like(ds([1, 1, 1]), a=42, itemid=ds([id1, id2, id1]))
        )

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = expr_eval.eval(
          kde.objs.like(ds([1, None, 1]), a=42, itemid=ds([id1, None, id3]))
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest(
        'sparse DataSlice and sparse itemid with sparsity mismatch'
    ):
      with self.assertRaisesRegex(
          ValueError,
          'kd.objs.like: `itemid` and `shape_and_mask_from` must have the same'
          ' sparsity',
      ):
        _ = expr_eval.eval(
            kde.objs.like(ds([1, None, 1]), a=42, itemid=ds([id1, id2, None]))
        )

    with self.subTest('sparse DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.objs.like(ds([1, None, 1]), a=42, itemid=ds([id1, id2, id3]))
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest('sparse DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          'kd.objs.like: `itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.objs.like(ds([1, None, 1]), a=42, itemid=ds([id1, id1, id1]))
        )

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = expr_eval.eval(
          kde.objs.like(ds([1, None, 1]), a=42, itemid=ds([id1, id1, id3]))
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    itemid = fns.obj(non_existent=ds([[42, 42], [42]])).get_itemid()
    assert itemid.get_bag() is not None
    # Successful.
    x = expr_eval.eval(kde.objs.like(ds([[1, None], [1]]), a=42, itemid=itemid))
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = x.non_existent

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError,
        "missing 1 required positional argument: 'shape_and_mask_from'",
    ):
      _ = kde.objs.like().eval()

  def test_fails_with_shape_input(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_SLICE'):
      _ = kde.objs.like(ds(0).get_shape()).eval()

  def test_non_determinism(self):
    x = ds([1, None, 3])
    res_1 = expr_eval.eval(kde.objs.like(x, a=5))
    res_2 = expr_eval.eval(kde.objs.like(x, a=5))
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

    expr = kde.objs.like(x, a=5)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.objs.like,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.objs.like(I.x)))
    self.assertTrue(view.has_koda_view(kde.objs.like(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.objs.like, kde.obj_like))

  def test_repr(self):
    self.assertEqual(
        repr(kde.objs.like(I.x, a=I.y)),
        'kd.objs.like(I.x, itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
