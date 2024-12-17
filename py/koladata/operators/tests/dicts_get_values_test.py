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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
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
DATA_SLICE = qtypes.DATA_SLICE
UNSPECIFIED = arolla.UNSPECIFIED
db = data_bag.DataBag.empty()
ds = lambda *arg: data_slice.DataSlice.from_vals(*arg).with_bag(db)

dict_item = db.dict({1: 2, 3: 4})
dict_item2 = db.dict({3: 5})
dict_slice = ds([dict_item, None, dict_item2])


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, UNSPECIFIED, DATA_SLICE),
])


class CoreGetValuesTest(parameterized.TestCase):

  @parameterized.parameters(
      # Dict DataItem
      (dict_item, ds([2, 4])),
      (dict_item.embed_schema(), ds([2, 4])),
      (
          dict_item.with_schema(schema_constants.ANY),
          ds([2, 4], schema_constants.ANY),
      ),
      (
          ds(None).with_schema(dict_item.get_schema()),
          ds([], schema_constants.INT32),
      ),
      (
          ds(None).with_schema(schema_constants.OBJECT),
          ds([], schema_constants.OBJECT),
      ),
      (
          ds(None).with_schema(schema_constants.ANY),
          ds([], schema_constants.ANY),
      ),
      # Dict DataSlice
      (dict_slice, ds([[2, 4], [], [5]])),
      (dict_slice.embed_schema(), ds([[2, 4], [], [5]])),
      (
          dict_slice.with_schema(schema_constants.ANY),
          ds([[2, 4], [], [5]], schema_constants.ANY),
      ),
  )
  def test_eval(self, dict_ds, expected):
    result = expr_eval.eval(kde.get_values(dict_ds))
    testing.assert_unordered_equal(result, expected)
    testing.assert_equal(dict_ds[dict_ds.get_keys()], result)

  @parameterized.parameters(
      # Dict DataItem
      (dict_item, ds([3, 1]), ds([4, 2])),
      (dict_item.embed_schema(), ds([3, 1]), ds([4, 2])),
      (
          dict_item.with_schema(schema_constants.ANY),
          ds([3, 1]),
          ds([4, 2], schema_constants.ANY),
      ),
      (
          ds(None).with_schema(dict_item.get_schema()),
          ds([3, 1]),
          ds([None, None], schema_constants.INT32),
      ),
      (
          ds(None).with_schema(schema_constants.OBJECT),
          ds([3, 1]),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds(None).with_schema(schema_constants.ANY),
          ds([3, 1]),
          ds([None, None], schema_constants.ANY),
      ),
      # Dict DataSlice
      (dict_slice, ds([[3, 1], [1], [3]]), ds([[4, 2], [None], [5]])),
      (
          dict_slice.embed_schema(),
          ds([[3, 1], [1], [3]]),
          ds([[4, 2], [None], [5]]),
      ),
      (
          dict_slice.with_schema(schema_constants.ANY),
          ds([[3, 1], [1], [3]]),
          ds([[4, 2], [None], [5]], schema_constants.ANY),
      ),
  )
  def test_eval_with_key(self, dict_ds, key_ds, expected):
    result = expr_eval.eval(kde.get_values(dict_ds, key_ds))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (dict_item, arolla.unspecified(), ds([2, 4])),
      (dict_slice, arolla.unspecified(), ds([[2, 4], [], [5]])),
  )
  def test_eval_with_unspecified_key(self, dict_ds, key_ds, expected):
    result = expr_eval.eval(kde.get_values(dict_ds, key_ds))
    testing.assert_unordered_equal(result, expected)

  def test_fork(self):
    d1 = data_bag.DataBag.empty().dict({1: 2, 3: 4})
    result = expr_eval.eval(kde.get_values(d1))
    testing.assert_unordered_equal(result, ds([2, 4]).with_bag(d1.get_bag()))

    d2 = d1.freeze()
    result = expr_eval.eval(kde.get_values(d2))
    testing.assert_unordered_equal(result, ds([2, 4]).with_bag(d2.get_bag()))

    d3 = d2.fork_bag()
    del d3[1]
    result = expr_eval.eval(kde.get_values(d3))
    testing.assert_unordered_equal(result, ds([4]).with_bag(d3.get_bag()))

    d4 = d3.fork_bag()
    d4[1] = 1
    d4[5] = 6
    result = expr_eval.eval(kde.get_values(d4))
    testing.assert_unordered_equal(result, ds([1, 4, 6]).with_bag(d4.get_bag()))

  def test_fallback(self):
    d1 = data_bag.DataBag.empty().dict({1: 2, 3: 4})
    d2 = data_bag.DataBag.empty().dict(
        {1: 1, 3: 3, 5: 6}, itemid=d1.get_itemid()
    )
    del d2[1]

    d3 = d1.enriched(d2.get_bag())
    result = expr_eval.eval(kde.get_values(d3))
    testing.assert_unordered_equal(result, ds([2, 4, 6]).with_bag(d3.get_bag()))

    d4 = d2.enriched(d1.get_bag())
    result = expr_eval.eval(kde.get_values(d4))
    testing.assert_unordered_equal(result, ds([2, 3, 6]).with_bag(d4.get_bag()))

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot get dict values without a DataBag',
    ):
      expr_eval.eval(kde.get_values(ds([1, 2, 3]).no_bag()))

    with self.assertRaisesRegex(
        ValueError,
        'cannot get or set attributes',
    ):
      expr_eval.eval(kde.get_values(ds([1, 2, 3])))

    with self.assertRaisesRegex(
        ValueError,
        'getting attributes of primitives is not allowed',
    ):
      expr_eval.eval(
          kde.get_values(ds([dict_item, 1], schema_constants.OBJECT))
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape("the attribute '__values__' is missing"),
    ):
      expr_eval.eval(kde.get_values(db.list([1, 2, 3])))

    with self.assertRaisesRegex(
        ValueError,
        'the schema for dict keys is missing',
    ):
      expr_eval.eval(kde.get_values(db.list([1, 2, 3]), ds([0, 1])))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.dicts.get_values,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.get_values(I.dict_ds)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.get_values, kde.get_values))


if __name__ == '__main__':
  absltest.main()
