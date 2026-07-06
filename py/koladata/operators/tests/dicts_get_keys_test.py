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
])


class DictsGetKeysTest(parameterized.TestCase):

  @parameterized.parameters(
      # Dict DataItem
      (kd.dict({1: 2, 3: 4}), ds([1, 3])),
      (kd.obj(kd.dict({1: 2, 3: 4})), ds([1, 3])),
      (
          ds(None).with_schema(kd.dict({1: 2, 3: 4}).get_schema()),
          ds([], schema_constants.INT32),
      ),
      (ds(None).with_bag(data_bag.DataBag.empty()), ds([])),
      (kd.obj(None).with_bag(data_bag.DataBag.empty()), ds([])),
      # Dict DataSlice
      (
          ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})]),
          ds([[1, 3], [], [3]]),
      ),
      (
          kd.obj(ds([kd.dict({1: 2, 3: 4}), None, kd.dict({3: 5})])),
          ds([[1, 3], [], [3]]),
      ),
  )
  def test_eval(self, dict_ds, expected):
    testing.assert_unordered_equal(kd.get_keys(dict_ds).no_bag(), expected)

  def test_fork(self):
    d1 = data_bag.DataBag.empty_mutable().dict({1: 2, 3: 4})
    result = kd.get_keys(d1)
    testing.assert_unordered_equal(result, ds([1, 3]).with_bag(d1.get_bag()))

    d2 = d1.freeze_bag()
    result = kd.get_keys(d2)
    testing.assert_unordered_equal(result, ds([1, 3]).with_bag(d2.get_bag()))

    d3 = d2.fork_bag()
    del d3[1]
    result = kd.get_keys(d3)
    testing.assert_unordered_equal(result, ds([1, 3]).with_bag(d3.get_bag()))

    d4 = d3.fork_bag()
    d4[1] = 1
    d4[5] = 6
    result = kd.get_keys(d4)
    testing.assert_unordered_equal(result, ds([1, 3, 5]).with_bag(d4.get_bag()))

  def test_fallback(self):
    d1 = data_bag.DataBag.empty_mutable().dict({1: 2, 3: 4})
    d2 = data_bag.DataBag.empty_mutable().dict(
        {1: 1, 3: 3, 5: 6}, itemid=d1.get_itemid()
    )
    del d2[1]

    d3 = d1.freeze_bag().enriched(d2.get_bag())
    result = kd.get_keys(d3)
    testing.assert_unordered_equal(result, ds([1, 3, 5]).with_bag(d3.get_bag()))

    d4 = d2.freeze_bag().enriched(d1.get_bag())
    result = kd.get_keys(d4)
    testing.assert_unordered_equal(result, ds([1, 3, 5]).with_bag(d4.get_bag()))

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot get dict keys without a DataBag',
    ):
      kd.get_keys(ds([1, 2, 3]).no_bag())

    db = data_bag.DataBag.empty_mutable()
    with self.assertRaisesRegex(
        ValueError,
        'cannot get or set attributes',
    ):
      kd.get_keys(ds([1, 2, 3]).with_bag(db))

    with self.assertRaisesRegex(
        ValueError,
        'getting attributes of primitives is not allowed',
    ):
      kd.get_keys(
          ds([db.dict({1: 2}), 1], schema_constants.OBJECT).with_bag(db)
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.dicts.get_keys,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.get_keys(I.dict_ds)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.get_keys, kde.get_keys))


if __name__ == '__main__':
  absltest.main()
