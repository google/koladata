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

"""Tests for kd.concat_lists."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice


kde = kde_operators.kde
db = fns.bag()
ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_bag(db)

OBJ1 = db.obj()
OBJ2 = db.obj()


class ImplodeTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          (),
          fns.list([]),
      ),
      (
          (fns.list([1, 2, 3]),),
          fns.list([1, 2, 3]),
      ),
      (
          (fns.list([[1], [2, 3]]),),
          fns.list([[1], [2, 3]]),
      ),
      (
          (fns.list([1, 2, 3]), fns.list([4, 5, 6])),
          fns.list([1, 2, 3, 4, 5, 6]),
      ),
      (
          (fns.list([1]), fns.list([2, 3]), fns.list([4, 5, 6])),
          fns.list([1, 2, 3, 4, 5, 6]),
      ),
      (
          (
              fns.list(ds([[0], [1]])),
              fns.list(ds([[2], [3]])),
              fns.list(ds([[4, 5], [6]])),
          ),
          fns.list(ds([[0, 2, 4, 5], [1, 3, 6]])),
      ),
      (
          # Compatible primitive types follow type promotion.
          (fns.list([1, 2, 3]), fns.list([4.5, 5.5, 6.5])),
          fns.list([1.0, 2.0, 3.0, 4.5, 5.5, 6.5]),
      ),
      (
          (fns.list([1]), fns.list([None, 2.5]), fns.list(['a', OBJ1, b'b'])),
          fns.list([1, None, 2.5, 'a', OBJ1, b'b']),
      ),
      (
          (
              fns.list(ds([[0], [None]])),
              fns.list(ds([['a'], [b'b']])),
              fns.list(ds([[4.5, OBJ1], [OBJ2]])),
          ),
          fns.list(ds([[0, 'a', 4.5, OBJ1], [None, b'b', OBJ2]])),
      ),
  )
  def test_eval(self, lists, expected):
    # Test behavior with explicit existing DataBag.
    result = db.concat_lists(*lists)
    testing.assert_nested_lists_equal(result, expected)
    self.assertEqual(result.get_bag().fingerprint, db.fingerprint)

    # Test behavior with implicit new DataBag.
    result = fns.concat_lists(*lists)
    testing.assert_nested_lists_equal(result, expected)
    if lists:
      self.assertNotEqual(
          lists[0].get_bag().fingerprint, result.get_bag().fingerprint
      )

    # Check behavior with explicit DataBag.
    db2 = fns.bag()
    result = fns.concat_lists(*lists, db=db2)
    testing.assert_nested_lists_equal(result, expected)
    self.assertEqual(result.get_bag().fingerprint, db2.fingerprint)


if __name__ == '__main__':
  absltest.main()
