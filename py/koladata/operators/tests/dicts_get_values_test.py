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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
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
kd = eager_op_utils.operators_container('kd')
ds = data_slice.DataSlice.from_vals


QTYPES = frozenset([
    (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
    (qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE),
    (qtypes.DATA_SLICE, arolla.UNSPECIFIED, qtypes.DATA_SLICE),
])


class DictsGetValuesTest(parameterized.TestCase):

  @parameterized.parameters(
      # Dict DataItem
      (kd.dict({1: 2, 3: 4}), ds([2, 4])),
      (kd.obj(kd.dict({1: 2, 3: 4})), ds([2, 4])),
      (
          ds(None).with_schema(kd.dict({1: 2, 3: 4}).get_schema()),
          ds([], schema_constants.INT32),
      ),
      (ds(None).with_bag(data_bag.DataBag.empty()), ds([])),
      (kd.obj(None).with_bag(data_bag.DataBag.empty()), ds([])),
      # Dict DataSlice
      (
          ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})]),
          ds([[2, 4], [], [5]]),
      ),
      (
          kd.obj(ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})])),
          ds([[2, 4], [], [5]]),
      ),
  )
  def test_eval(self, dict_ds, expected):
    result = kd.get_values(dict_ds)
    testing.assert_unordered_equal(result.no_bag(), expected)
    testing.assert_equal(dict_ds[dict_ds.get_keys()], result)

  @parameterized.parameters(
      # Dict DataItem
      (kd.dict({1: 2, 3: 4}), ds([3, 1]), ds([4, 2])),
      (kd.obj(kd.dict({1: 2, 3: 4})), ds([3, 1]), ds([4, 2])),
      (
          ds(None).with_schema(kd.dict({1: 2, 3: 4}).get_schema()),
          ds([3, 1]),
          ds([None, None], schema_constants.INT32),
      ),
      (
          ds(None).with_schema(schema_constants.OBJECT).with_bag(kd.bag()),
          ds([3, 1]),
          ds([None, None]),
      ),
      # Dict DataSlice
      (
          ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})]),
          ds([[3, 1], [1], [3]]),
          ds([[4, 2], [None], [5]]),
      ),
      (
          kd.obj(ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})])),
          ds([[3, 1], [1], [3]]),
          ds([[4, 2], [None], [5]]),
      ),
  )
  def test_eval_with_key(self, dict_ds, key_ds, expected):
    result = kd.get_values(dict_ds, key_ds)
    testing.assert_equivalent(result, expected)

  @parameterized.parameters(
      (kd.dict({1: 2, 3: 4}), arolla.unspecified(), ds([2, 4])),
      (
          ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})]),
          arolla.unspecified(),
          ds([[2, 4], [], [5]]),
      ),
  )
  def test_eval_with_unspecified_key(self, dict_ds, key_ds, expected):
    result = kd.get_values(dict_ds, key_ds)
    testing.assert_unordered_equal(result.no_bag(), expected)

  def test_fork(self):
    d1 = data_bag.DataBag.empty_mutable().dict({1: 2, 3: 4})
    result = kd.get_values(d1)
    testing.assert_unordered_equal(result, ds([2, 4]).with_bag(d1.get_bag()))

    d2 = d1.freeze_bag()
    result = kd.get_values(d2)
    testing.assert_unordered_equal(result, ds([2, 4]).with_bag(d2.get_bag()))

    d3 = d2.fork_bag()
    del d3[1]
    result = kd.get_values(d3)
    testing.assert_unordered_equal(result, ds([None, 4]).with_bag(d3.get_bag()))

    d4 = d3.fork_bag()
    d4[1] = 1
    d4[5] = 6
    result = kd.get_values(d4)
    testing.assert_unordered_equal(result, ds([1, 4, 6]).with_bag(d4.get_bag()))

  def test_fallback(self):
    d1 = data_bag.DataBag.empty_mutable().dict({1: 2, 3: 4})
    d2 = data_bag.DataBag.empty_mutable().dict(
        {1: 1, 3: 3, 5: 6}, itemid=d1.get_itemid()
    )
    del d2[1]

    d3 = d1.freeze_bag().enriched(d2.get_bag())
    result = kd.get_values(d3)
    testing.assert_unordered_equal(result, ds([2, 4, 6]).with_bag(d3.get_bag()))

    d4 = d2.freeze_bag().enriched(d1.get_bag())
    result = kd.get_values(d4)
    testing.assert_unordered_equal(
        result, ds([None, 3, 6]).with_bag(d4.get_bag())
    )

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot get dict values without a DataBag',
    ):
      kd.get_values(ds([1, 2, 3]).no_bag())

    db = data_bag.DataBag.empty_mutable()
    with self.assertRaisesRegex(
        ValueError,
        'cannot get or set attributes',
    ):
      kd.get_values(ds([1, 2, 3]).with_bag(db))

    with self.assertRaisesRegex(
        ValueError,
        'getting attributes of primitives is not allowed',
    ):
      kd.get_values(
          ds([db.dict({1: 2}), 1], schema_constants.OBJECT).with_bag(db)
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('cannot get attribute from list'),
    ):
      kd.get_values(db.list([1, 2, 3]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('dict(s) expected, got LIST[INT32]'),
    ):
      kd.get_values(db.list([1, 2, 3]), ds([0, 1]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.dicts.get_values,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.get_values(I.dict_ds)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.get_values, kde.get_values))


if __name__ == '__main__':
  absltest.main()
