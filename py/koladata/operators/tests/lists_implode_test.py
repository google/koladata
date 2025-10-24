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

import math
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import lists_implode_testdata
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


QTYPE_SIGNATURES = [
    (
        qtypes.DATA_SLICE,
        qtypes.DATA_SLICE,
        arg,
        qtypes.NON_DETERMINISTIC_TOKEN,
        qtypes.DATA_SLICE,
    )
    for arg in [qtypes.DATA_SLICE, arolla.UNSPECIFIED]
]


class ListLikeTest(parameterized.TestCase):

  @parameterized.parameters(*lists_implode_testdata.TEST_CASES)
  def test_eval(self, x, ndim, expected):
    result = kd.lists.implode(x, ndim)
    testing.assert_equivalent(result, expected)
    self.assertFalse(result.is_mutable())

    # Check that eager ops work on mutable inputs, too.
    db = data_bag.DataBag.empty_mutable()
    result = kd.lists.implode(db.adopt(x), ndim)
    testing.assert_equivalent(result, expected)
    self.assertFalse(result.is_mutable())

    # Check behavior with DataItem ndim.
    result = kd.lists.implode(x, ds(ndim))
    testing.assert_equivalent(result, expected)
    self.assertFalse(result.is_mutable())

  def test_eval_nan(self):
    o = kd.obj(a=math.nan)
    x = ds([o])
    ndim = 1
    expected = kd.list([o])
    result = kd.lists.implode(x, ndim)
    with self.assertRaisesRegex(
        AssertionError,
        r'expected\[0\].a:\nDataItem\(nan, schema: FLOAT32\)\n'
        r'-> actual\[0\].a:\nDataItem\(nan, schema: FLOAT32\)',
    ):
      testing.assert_equivalent(result, expected)

  def test_itemid(self):
    itemid = kd.allocation.new_listid_shaped_as(ds([1, 1]))
    x = kd.lists.implode(ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid)
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

    itemid = kd.allocation.new_listid()
    x = kd.lists.implode(ds([['a', 'b'], ['c']]), ndim=2, itemid=itemid)
    testing.assert_equal(x[:][:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.implode: cannot implode 'x' to fold the last 2 dimension(s)"
            " because 'x' only has 1 dimensions"
        ),
    ):
      kd.lists.implode(ds([1, 2]), 2)

  def test_non_determinism(self):
    items = ds([1, None, 2]).freeze_bag()
    expr = kde.lists.implode(items)
    res_1 = expr.eval()
    res_2 = expr.eval()
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    self.assertNotEqual(res_1.no_bag().fingerprint, res_2.no_bag().fingerprint)
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.implode,
        QTYPE_SIGNATURES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.implode(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.implode, kde.lists.implode))


if __name__ == '__main__':
  absltest.main()
