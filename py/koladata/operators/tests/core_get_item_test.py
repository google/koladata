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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eager = eager_op_utils.operators_container('kd')
eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE
db = data_bag.DataBag.empty()
ds = data_slice.DataSlice.from_vals

list_item = db.list([1, 2, 3])
list_item2 = db.list([4, 5, 6, 7])
list_slice = ds([list_item, list_item2])
nested_list_item = db.list([list_item, list_item2])
dict_item = db.dict({1: 2, 3: 4})
dict_item2 = db.dict({3: 5})
dict_slice = ds([dict_item, dict_item2]).with_bag(db)


class CoreGetItemTest(parameterized.TestCase):

  @parameterized.parameters(
      # List DataItem
      (list_item, slice(None), ds([1, 2, 3])),
      (list_item, slice(None, 2), ds([1, 2])),
      (list_item, slice(1, None), ds([2, 3])),
      (list_item, slice(1, -1), ds([2])),
      (list_item, 1, ds(2)),
      (list_item, -1, ds(3)),
      (list_item, ds(None), ds(None, schema_constants.INT32)),
      (list_item, ds([1, -2, None]), ds([2, 2, None])),
      (nested_list_item, slice(None), ds([list_item, list_item2])),
      (nested_list_item, slice(None, 2), ds([list_item, list_item2])),
      (nested_list_item, slice(1, None), ds([list_item2])),
      (nested_list_item, slice(0, -1), ds([list_item])),
      (nested_list_item, 1, list_item2),
      (nested_list_item, -1, list_item2),
      (nested_list_item, ds(None), list_item2 & ds(None)),
      (nested_list_item, ds([1, -1, None]), ds([list_item2, list_item2, None])),
      # List DataSlice
      (list_slice, slice(None), ds([[1, 2, 3], [4, 5, 6, 7]])),
      (list_slice, slice(None, 2), ds([[1, 2], [4, 5]])),
      (list_slice, slice(1, None), ds([[2, 3], [5, 6, 7]])),
      (list_slice, slice(1, -1), ds([[2], [5, 6]])),
      (list_slice, 1, ds([2, 5])),
      (list_slice, -1, ds([3, 7])),
      (
          list_slice,
          ds(None),
          ds([None, None], schema_constants.INT32),
      ),
      (
          list_slice,
          ds([[1], [-2, None]]),
          ds([[2], [6, None]]),
      ),
      # Missing List
      (list_item & ds(None), 0, ds(None, schema_constants.INT32)),
      (
          list_slice & ds(None),
          ds([None, None]),
          ds([None, None], schema_constants.INT32),
      ),
      # OBJECT List
      (list_item.embed_schema(), 1, ds(2)),
      # Dict DataItem
      (dict_item, 3, ds(4)),
      (dict_item, ds(None), ds(None, schema_constants.INT32)),
      (dict_item, ds([3, 1, 5]), ds([4, 2, None])),
      # Dict DataSlice
      (dict_slice, 3, ds([4, 5])),
      (dict_slice, ds(None), ds([None, None], schema_constants.INT32)),
      (dict_slice, ds([[1], [None]]), ds([[2], [None]])),
      # Missing Dict
      (dict_item & ds(None), 3, ds(None, schema_constants.INT32)),
      (
          dict_slice & ds(None),
          ds([None, None]),
          ds([None, None], schema_constants.INT32),
      ),
      # OBJECT Dict
      (dict_item.embed_schema(), 3, ds(4)),
      # Empty and unknown
      (ds(None, schema_constants.OBJECT).with_bag(db), 1, ds(None)),
      (
          ds(None, schema_constants.OBJECT).with_bag(db),
          ds([1, 2]),
          ds([None, None]),
      ),
      (ds([]).with_bag(db), 0, ds([])),
      (ds(None).with_bag(db), 0, ds(None)),
  )
  def test_slice_eval(self, x, keys_or_indices, expected):
    result = eager.core.get_item(x, keys_or_indices)
    view_result = eval_op('koda_internal.view.get_item', x, keys_or_indices)
    testing.assert_equal(result, expected.with_bag(x.get_bag()))
    testing.assert_equal(view_result, expected.with_bag(x.get_bag()))

  def test_slice_expr(self):
    expr = kde.get_item(I.x, arolla.M.core.make_slice(I.start, I.end))
    li = fns.list([1, 2, 3])
    result = expr_eval.eval(expr, x=li, start=0, end=-1)
    testing.assert_equal(result, ds([1, 2]).with_bag(li.get_bag()))

  def test_invalid_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'unsupported narrowing cast to INT64 for the given STRING DataSlice',
    ):
      expr_eval.eval(kde.get_item(ds([1, 2, 3]), slice('a', 3)))

    with self.assertRaisesRegex(
        # TODO: b/389032294 - Raise KodaError.
        ValueError,
        'kd.core.get_item: slice with step != 1 is not supported',
    ):
      eager.core.get_item(ds([1, 2, 3]), slice(3, 5, 2))

  def test_repr(self):
    self.assertEqual(
        repr(kde.get_item(I.x, slice(1))),
        'I.x[:DataItem(1, schema: INT32)]',
    )
    self.assertEqual(
        repr(kde.get_item(I.x, slice(1, None))),
        'I.x[DataItem(1, schema: INT32):]',
    )
    self.assertEqual(
        repr(kde.get_item(I.x, slice(1, -1))),
        'I.x[DataItem(1, schema: INT32):DataItem(-1, schema: INT32)]',
    )
    self.assertEqual(
        repr(kde.get_item(I.x, kde.tuple.make_slice(I.start, I.end))),
        'I.x[kd.tuple.make_slice(I.start, I.end, unspecified)]',
    )
    self.assertEqual(
        repr(kde.get_item(I.x, ds(1).no_bag())),
        'I.x[DataItem(1, schema: INT32)]',
    )
    self.assertEqual(repr(I.x[:1]), 'I.x[:DataItem(1, schema: INT32)]')
    self.assertEqual(repr(I.x[1:]), 'I.x[DataItem(1, schema: INT32):]')
    self.assertEqual(
        repr(I.x[1:-1]),
        'I.x[DataItem(1, schema: INT32):DataItem(-1, schema: INT32)]',
    )
    self.assertEqual(repr(I.x[I.s]), 'I.x[I.s]')
    self.assertEqual(
        repr(I.x[slice(1, -1)]),
        'I.x[DataItem(1, schema: INT32):DataItem(-1, schema: INT32)]',
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.get_item,
        ((DATA_SLICE, DATA_SLICE, DATA_SLICE),),
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.get_item(I.x, I.key_or_index)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.get_item, kde.get_item))
    self.assertTrue(optools.equiv_to_op(kde.core.get_item, kde.dicts.get_item))
    self.assertTrue(optools.equiv_to_op(kde.core.get_item, kde.lists.get_item))


if __name__ == '__main__':
  absltest.main()
