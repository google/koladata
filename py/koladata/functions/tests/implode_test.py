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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice


kde = kde_operators.kde
db = fns.bag()
ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_bag(db)

OBJ1 = db.obj()
OBJ2 = db.obj()
OBJ3 = db.obj(a=math.nan)


class ImplodeTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.implode(ds([1, None])).is_mutable())

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
      (ds([None]), 1, db.list([None])),
  )
  def test_eval(self, x, ndim, expected):
    # Test behavior with explicit existing DataBag.
    result = db.implode(x, ndim)
    testing.assert_nested_lists_equal(result, expected)
    self.assertEqual(result.get_bag().fingerprint, x.get_bag().fingerprint)

    # Test behavior with implicit new DataBag.
    result = fns.implode(x, ndim)
    testing.assert_nested_lists_equal(result, expected)
    self.assertNotEqual(x.get_bag().fingerprint, result.get_bag().fingerprint)

    # Check behavior with DataItem ndim.
    result = fns.implode(x, ds(ndim))
    testing.assert_nested_lists_equal(result, expected)
    self.assertNotEqual(x.get_bag().fingerprint, result.get_bag().fingerprint)

  def test_itemid(self):
    itemid = kde.allocation.new_listid_shaped_as(ds([1, 1])).eval()
    x = fns.implode(ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid)
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]).no_bag())
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.implode(ds([[triple], []]))

    # Successful.
    x = fns.implode(ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_ndim_error(self):
    with self.assertRaisesRegex(TypeError, 'an integer is required'):
      fns.implode(ds([]), ds(None))

    with self.assertRaisesRegex(TypeError, 'an integer is required'):
      fns.implode(ds([]), ds([1]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.implode: cannot implode 'x' to fold the last 2 dimension(s)"
            " because 'x' only has 1 dimensions"
        ),
    ):
      fns.implode(ds([1, 2]), 2)


if __name__ == '__main__':
  absltest.main()
