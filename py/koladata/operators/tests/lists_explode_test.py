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
from koladata.operators.tests.testdata import lists_explode_testdata
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer("I")

kd = eager_op_utils.operators_container("kd")
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
ITEMID = schema_constants.ITEMID
ds = data_slice.DataSlice.from_vals


class ListsExplodeTest(parameterized.TestCase):

  @parameterized.parameters(*lists_explode_testdata.TEST_CASES)
  def test_eval(self, x, ndim, expected):
    result = kd.lists.explode(x, ndim)
    testing.assert_equivalent(result, expected)
    testing.assert_equal(result.get_bag(), x.get_bag())

    # Also check that the operator can take mutable inputs.
    db = data_bag.DataBag.empty_mutable()
    result = kd.lists.explode(db.adopt(x), ndim)
    testing.assert_equal(result, db.adopt(expected))

    # Check consistency with x[:] operator if applicable.
    if ndim == 1:
      testing.assert_equivalent(result, x[:])

  def test_out_of_bounds_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot explode 'x' to have additional 2 dimension(s), the maximum"
            " number of additional dimension(s) is 1"
        ),
    ):
      kd.lists.explode(kd.list([1, 2]), 2)

  def test_expand_fully_itemid_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("cannot fully explode 'x' with ITEMID schema")
    ):
      # DataItem(List[None], schema: ITEMID)
      kd.lists.explode(kd.list([]).with_schema(ITEMID), -1)

    with self.assertRaisesRegex(
        ValueError, re.escape("cannot fully explode 'x' with ITEMID schema")
    ):
      # DataItem(List[None], schema: LIST[ITEMID])
      kd.lists.explode(kd.list([ds(None).with_schema(ITEMID)]), -1)

  def test_expand_fully_none_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("cannot fully explode 'x' with NONE schema")
    ):
      # DataItem(List[None], schema: NONE)
      kd.lists.explode(kd.list([]), -1)

    with self.assertRaisesRegex(
        ValueError, re.escape("cannot fully explode 'x' with NONE schema")
    ):
      # DataItem(List[None], schema: LIST[NONE])
      kd.lists.explode(kd.list([ds(None)]), -1)

  def test_expand_fully_object_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[], schema: LIST[OBJECT])
      kd.lists.explode(kd.list(), -1)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous"
        ),
    ):
      # DataItem(List[None], schema: LIST[OBJECT])
      kd.lists.explode(
          kd.list([kd.obj() & ds(None, schema_constants.MASK)]), -1
      )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.explode,
        [
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.explode(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.explode, kde.explode))


if __name__ == "__main__":
  absltest.main()
