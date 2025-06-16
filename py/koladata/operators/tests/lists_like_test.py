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

import itertools

from absl.testing import absltest
from absl.testing import parameterized
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
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty


QTYPE_SIGNATURES = list(
    (
        qtypes.DATA_SLICE, *args, qtypes.NON_DETERMINISTIC_TOKEN,
        qtypes.DATA_SLICE
    )
    for args in itertools.product(
        [qtypes.DATA_SLICE, arolla.UNSPECIFIED], repeat=4
    )
)


class ListLikeTest(parameterized.TestCase):

  @parameterized.parameters(
      ([ds(None)], dict()),
      ([ds(1)], dict()),
      ([ds(1)], dict(items=ds([2]))),
      ([ds([[1, None], [3]])], dict()),
      ([ds([[1, None], [3]])], dict(item_schema=schema_constants.OBJECT)),
      ([ds([[1, None], [3]])], dict(item_schema=schema_constants.INT64)),
      ([ds([[1, None], [3]])], dict(items=ds([[1, 2], [3]]))),
      ([ds([[1, None], [3]])], dict(items=ds([[1, 'foo'], [3]]))),
      (
          [ds([[1, None], [3]])],
          dict(
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([[1, None], [3]])],
          dict(
              items=ds([[1, 2], [3]]),
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([[1, None], [3]])],
          dict(itemid=bag().list_like(ds([[0, 0], [0]])).get_itemid()),
      ),
  )
  def test_value(self, args, kwargs):
    actual = expr_eval.eval(kde.lists.like(*args, **kwargs))
    expected = bag().list_like(*args, **kwargs)
    testing.assert_equal(actual[:].no_bag(), expected[:].no_bag())

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = expr_eval.eval(
        kde.lists.like(
            ds([1, None]), items=ds([['a', 'b'], ['c']]), itemid=itemid
        )
    )
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], []]))
    testing.assert_equal(
        x.no_bag().get_itemid(), itemid & expr_eval.eval(kde.has(x))
    )

  def test_db_is_immutable(self):
    lst = expr_eval.eval(kde.lists.like(ds([[1, None], [3]])))
    self.assertFalse(lst.is_mutable())

  def test_adopt_values(self):
    lst = kde.lists.implode(ds([[1, 2], [3]])).eval()
    lst2 = kde.lists.like(ds([None, 0]), lst).eval()

    testing.assert_equal(
        lst2[:][:],
        ds([[], [[3]]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    list_schema = kde.schema.list_schema(
        kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    lst = kde.lists.like(ds([None, 0]), schema=list_schema).eval()

    testing.assert_equal(
        lst[:].a.no_bag(), ds([[], []], schema_constants.INT32)
    )

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_listid())
    input_arg = ds(['a'])

    with self.subTest('present DataItem and present itemid'):
      x = expr_eval.eval(kde.lists.like(ds(1), input_arg, itemid=itemid))
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = expr_eval.eval(
          kde.lists.like(ds(None), input_arg, itemid=(itemid & None))
      )
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = expr_eval.eval(kde.lists.like(ds(None), input_arg, itemid=itemid))
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 0 present items but 1 are required',
      ):
        _ = expr_eval.eval(
            kde.lists.like(ds(1), input_arg, itemid=(itemid & None))
        )

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_listid())
    id2 = expr_eval.eval(kde.allocation.new_listid())
    id3 = expr_eval.eval(kde.allocation.new_listid())
    input_arg = ds([['a'], ['b'], ['c']])

    with self.subTest('full DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.lists.like(
              ds([1, 1, 1]), input_arg, itemid=ds([id1, id2, id3])
          )
      )
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 2 present items but 3 are required',
      ):
        _ = expr_eval.eval(
            kde.lists.like(
                ds([1, 1, 1]),
                input_arg,
                itemid=ds([id1, None, id3]),
            )
        )

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.lists.like(
                ds([1, 1, 1]), input_arg, itemid=ds([id1, id2, id1])
            )
        )

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = expr_eval.eval(
          kde.lists.like(
              ds([1, None, 1]),
              input_arg,
              itemid=ds([id1, None, id3]),
          )
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
          '`itemid` and `shape_and_mask_from` must have the same sparsity',
      ):
        _ = expr_eval.eval(
            kde.lists.like(
                ds([1, None, 1]), input_arg, itemid=ds([id1, id2, None])
            )
        )

    with self.subTest('sparse DataSlice and full itemid'):
      x = expr_eval.eval(
          kde.lists.like(
              ds([1, None, 1]),
              input_arg,
              itemid=ds([id1, id2, id3]),
          )
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
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = expr_eval.eval(
            kde.lists.like(
                ds([1, None, 1]),
                input_arg,
                itemid=ds([id1, id1, id1]),
            )
        )

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = expr_eval.eval(
          kde.lists.like(
              ds([1, None, 1]),
              input_arg,
              itemid=ds([id1, id1, id3]),
          )
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.implode(ds([[[triple], []], [[]]]))

    # Successful.
    x = expr_eval.eval(
        kde.lists.like(ds([[1, None], [1]]), itemid=itemid.get_itemid())
    )
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got shape_and_mask_from: DATA_BAG',
    ):
      expr_eval.eval(kde.lists.like(fns.bag()))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.lists.like(ds([[1, None], [2]]), items=ds([1, 2, 3]))
      )

  def test_schema_arg_error(self):
    mask_and_shape = ds([[1, None], [3]])
    list_schema = kde.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      expr_eval.eval(kde.lists.like(
          mask_and_shape,
          item_schema=schema_constants.INT64,
          schema=list_schema,
      ))

  def test_wrong_arg_types(self):
    mask_and_shape = ds([[1, None], [3]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.lists.like(mask_and_shape, item_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.lists.like(mask_and_shape, schema=42))

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for list items is incompatible.

Expected schema for list items: BYTES
Assigned schema for list items: INT32""",
    ):
      expr_eval.eval(kde.lists.like(
          ds([[1, None], [3]]),
          items=ds([[1, 2], [3]]),
          item_schema=schema_constants.BYTES,
      ))

  def test_non_determinism(self):
    x = ds([[1, None], [3]])
    items = ds([[1, 2], [3]])
    res_1 = expr_eval.eval(kde.lists.like(x, items=items))
    res_2 = expr_eval.eval(kde.lists.like(x, items=items))
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

    expr = kde.lists.like(x, items=items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.like,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.like(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.like, kde.lists.like))

  def test_repr(self):
    self.assertEqual(
        repr(kde.lists.like(I.x, schema=I.y)),
        'kd.lists.like(I.x, unspecified, item_schema=unspecified,'
        ' schema=I.y, itemid=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
