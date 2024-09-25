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

"""Tests for kd.implode."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice


kde = kde_operators.kde
db = fns.bag()
ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_db(db)

OBJ1 = db.obj()
OBJ2 = db.obj()


class ImplodeTest(parameterized.TestCase):

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
  )
  def test_eval(self, x, ndim, expected):
    # Test behavior with explicit existing DataBag.
    result = db.implode(x, ndim)
    testing.assert_nested_lists_equal(result, expected)
    self.assertEqual(result.db.fingerprint, x.db.fingerprint)

    # Test behavior with implicit new DataBag.
    result = fns.implode(x, ndim)
    testing.assert_nested_lists_equal(result, expected)
    self.assertNotEqual(x.db.fingerprint, result.db.fingerprint)

    # Check behavior with explicit DataBag.
    db2 = fns.bag()
    result = fns.implode(x, ndim, db2)
    testing.assert_nested_lists_equal(result, expected)
    self.assertEqual(result.db.fingerprint, db2.fingerprint)


if __name__ == '__main__':
  absltest.main()
