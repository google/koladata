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

import math
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
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


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty

db = bag()
OBJ1 = db.obj()
OBJ2 = db.obj()
OBJ3 = db.obj(a=math.nan)


QTYPE_SIGNATURES = frozenset([
    (
        qtypes.DATA_SLICE, qtypes.DATA_SLICE, arg,
        qtypes.NON_DETERMINISTIC_TOKEN,
        qtypes.DATA_SLICE
    )
    for arg in [qtypes.DATA_SLICE, arolla.UNSPECIFIED]
])


class ListLikeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(0), 0, ds(0)),
      (ds(0), -1, ds(0)),
      (ds([1, None, 2]), 0, ds([1, None, 2])),
      (ds([1, None, 2]), 1, db.list([1, None, 2])),
      (ds([1, None, 2]), -1, db.list([1, None, 2])),
      (ds([[1, None, 2], [3, 4]]), 0, ds([[1, None, 2], [3, 4]])),
      (
          ds([[1, None, 2], [3, 4]]),
          1,
          ds([db.list([1, None, 2]), db.list([3, 4])]),
      ),
      (
          ds([[1, None, 2], [3, 4]]),
          2,
          db.list([[1, None, 2], [3, 4]]),
      ),
      (
          ds([[1, None, 2], [3, 4]]),
          -1,
          db.list([[1, None, 2], [3, 4]]),
      ),
      (
          ds([db.list([1, None, 2]), db.list([3, 4])]),
          0,
          ds([db.list([1, None, 2]), db.list([3, 4])]),
      ),
      (
          ds([db.list([1, None, 2]), db.list([3, 4])]),
          1,
          db.list([[1, None, 2], [3, 4]]),
      ),
      (
          ds([db.list([1, None, 2]), db.list([3, 4])]),
          -1,
          db.list([[1, None, 2], [3, 4]]),
      ),
      (
          ds([[OBJ1, None, OBJ2], [3, 4]]),
          0,
          ds([[OBJ1, None, OBJ2], [3, 4]]),
      ),
      (
          ds([[OBJ1, None, OBJ2], [3, 4]]),
          1,
          ds([db.list([OBJ1, None, OBJ2]), db.list([db.obj(3), db.obj(4)])]),
      ),
      (
          ds([[OBJ1, None, OBJ2], [3, 4]]),
          2,
          db.list([[OBJ1, None, OBJ2], [3, 4]]),
      ),
      (
          ds([[OBJ1, None, OBJ2], [3, 4]]),
          -1,
          db.list([[OBJ1, None, OBJ2], [3, 4]]),
      ),
      (ds([OBJ3]), 1, db.list([OBJ3])),
  )
  def test_eval(self, x, ndim, expected):
    # Test behavior with explicit existing DataBag.
    result = expr_eval.eval(kde.lists.implode(x, ndim))
    testing.assert_nested_lists_equal(result, expected)
    self.assertFalse(result.is_mutable())

    # Check behavior with DataItem ndim.
    result = expr_eval.eval(kde.lists.implode(x, ds(ndim)))
    testing.assert_nested_lists_equal(result, expected)
    self.assertFalse(result.is_mutable())

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = expr_eval.eval(
        kde.lists.implode(ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid)
    )
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

    itemid = expr_eval.eval(kde.allocation.new_listid())
    x = expr_eval.eval(
        kde.lists.implode(ds([['a', 'b'], ['c']]), ndim=2, itemid=itemid)
    )
    testing.assert_equal(x[:][:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_ndim_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            "kd.implode: cannot implode 'x' to fold the last 2 dimension(s)"
            " because 'x' only has 1 dimensions"
        ),
    ):
      expr_eval.eval(kde.lists.implode(ds([1, 2]), 2))

  def test_non_determinism(self):
    items = ds([1, None, 2])
    res_1 = expr_eval.eval(kde.lists.implode(items))
    res_2 = expr_eval.eval(kde.lists.implode(items))
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    self.assertNotEqual(res_1.no_bag().fingerprint, res_2.no_bag().fingerprint)
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

    expr = kde.lists.implode(items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    self.assertNotEqual(res_1.no_bag().fingerprint, res_2.no_bag().fingerprint)
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.implode,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.implode(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.implode, kde.lists.implode))


if __name__ == '__main__':
  absltest.main()
