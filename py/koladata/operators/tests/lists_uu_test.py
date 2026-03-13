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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class ListsUuTest(parameterized.TestCase):

  def test_basics(self):
    res = kd.lists.uu([1, 2, 3])

    self.assertFalse(res.is_mutable())
    self.assertTrue(res.get_schema().is_list_schema())
    testing.assert_equivalent(res[:], ds([1, 2, 3]).with_bag(res.get_bag()))

  @parameterized.parameters(
      ([1, 2, 3], ''),
      ([1, 2, 3], 'specified_seed'),
  )
  def test_uuid_equals(self, items, seed):
    lhs = kd.lists.uu(items, seed=seed)
    rhs = kd.lists.uu(items, seed=seed)
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  @parameterized.parameters(
      ([1, 2, 3], '', [1, 2, 4], ''),
      ([1, 2, 3], 'seed1', [1, 2, 3], 'seed2'),
  )
  def test_uuid_not_equals(
      self, lhs_items, lhs_seed, rhs_items, rhs_seed
  ):
    lhs = kd.lists.uu(lhs_items, seed=lhs_seed)
    rhs = kd.lists.uu(rhs_items, seed=rhs_seed)
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_default_seed(self):
    lhs = kd.lists.uu([1, 2, 3])
    rhs = kd.lists.uu([1, 2, 3], seed='')
    testing.assert_equal(lhs.no_bag(), rhs.no_bag())

  def test_python_list(self):
    l = kd.uulist([1, 2, 3])
    testing.assert_equal(l.no_bag(), kd.uulist([1, 2, 3]).no_bag())
    testing.assert_equal(l[ds(0)], ds(1).with_bag(l.get_bag()))
    testing.assert_equal(l[ds(2)], ds(3).with_bag(l.get_bag()))

  def test_item_schema(self):
    l = kd.lists.uu(
        [1, 2, 3],
        item_schema=schema_constants.INT64,
    )
    testing.assert_equal(
        l[ds(0)],
        ds(1, schema_constants.INT64).with_bag(l.get_bag()),
    )

  def test_schema(self):
    list_schema = kd.list_schema(schema_constants.INT64)
    l = kd.lists.uu(
        [1, 2, 3],
        schema=list_schema,
    )
    testing.assert_equal(
        l[ds(0)],
        ds(1, schema_constants.INT64).with_bag(l.get_bag()),
    )

  def test_determinism(self):
    expr = kde.lists.uu([2, 3, 7])
    res_1 = expr.eval()
    res_2 = expr.eval()
    testing.assert_equal(res_1, res_2)

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.uu, kde.uulist))


if __name__ == '__main__':
  absltest.main()
