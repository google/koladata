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


class DictsGetPresentValuesTest(parameterized.TestCase):

  @parameterized.parameters(
      # Dict DataItem
      (kd.dict({1: None, 3: 4}), ds([4])),
      (kd.obj(kd.dict({1: None, 3: 4})), ds([4])),
      (
          ds(None).with_schema(kd.dict({1: None, 3: 4}).get_schema()),
          ds([], schema_constants.INT32),
      ),
      (ds(None).with_bag(data_bag.DataBag.empty()), ds([])),
      (kd.obj(None).with_bag(data_bag.DataBag.empty()), ds([])),
      # Dict DataSlice
      (
          ds([kd.dict({1: None, 3: 4}), None, kd.dict({3: 5})]),
          ds([[4], [], [5]]),
      ),
      (
          kd.obj(ds([kd.dict({1: None, 3: 4}), None, kd.dict({3: 5})])),
          ds([[4], [], [5]]),
      ),
  )
  def test_eval(self, dict_ds, expected):
    testing.assert_equivalent(kd.get_present_values(dict_ds), expected)

  def test_deleted_keys(self):
    db = data_bag.DataBag.empty_mutable()
    d = ds([db.dict({1: 2, 3: 4}), db.dict({1: 3, 5: 6})]).with_bag(db)
    del d[1]
    testing.assert_unordered_equal(
        kd.get_present_values(d),
        ds([[4], [6]]).with_bag(db),
    )

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot get dict values without a DataBag',
    ):
      kd.get_present_values(ds([1, 2, 3]).no_bag())

    db = data_bag.DataBag.empty_mutable()
    with self.assertRaisesRegex(
        ValueError,
        'cannot get or set attributes',
    ):
      kd.get_present_values(ds([1, 2, 3]).with_bag(db))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.dicts.get_present_values,  # pyrefly: ignore[missing-attribute]
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        QTYPES,
    )

  def test_determinism(self):
    testing.assert_equal(
        kde.dicts.get_present_values(I.dict_ds),  # pyrefly: ignore[missing-attribute]
        kde.dicts.get_present_values(I.dict_ds),  # pyrefly: ignore[missing-attribute]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.get_present_values(I.dict_ds)))  # pyrefly: ignore[missing-attribute]

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.dicts.get_present_values, kde.get_present_values  # pyrefly: ignore[missing-attribute]
        )
    )


if __name__ == '__main__':
  absltest.main()
