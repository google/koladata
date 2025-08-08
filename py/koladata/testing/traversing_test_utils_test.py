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
from koladata.operators import kde_operators
from koladata.testing import traversing_test_utils
from koladata.types import data_bag
from koladata.types import data_slice

kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class TraversingTestUtilsTest(absltest.TestCase):

  def test_assert_deep_equivalent(self):
    traversing_test_utils.assert_deep_equivalent(ds([1, 2, 3]), ds([1, 2, 3]))

  def test_assert_deep_equivalent_diff(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'DataSlices are not equivalent, mismatches found at:\n'
        r'.S\[2\]: DataItem\(3, schema: INT32\)'
        r' vs DataItem\(4, schema: INT32',
    ):
      traversing_test_utils.assert_deep_equivalent(ds([1, 2, 3]), ds([1, 2, 4]))

  def test_assert_deep_equivalent_diff_deep(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.mapping\[\'d\'\].x: '
        r'DataItem\(4, schema: INT32\) vs DataItem\(5, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.new(
          mapping=bag_a.dict(
              ds(['a', 'b', 'c', 'd']), bag_a.new(x=ds([1, 2, 3, 4]))
          )
      )
      ds_b = bag_b.new(
          mapping=bag_b.dict(
              ds(['a', 'c', 'b', 'd']), bag_b.new(x=ds([1, 3, 2, 5]))
          )
      )
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b)

  def test_assert_deep_equivalent_diff_lhs_only(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.x: DataItem\(1, schema: INT32\) vs missing',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new()
      traversing_test_utils.assert_deep_equivalent(a, b)

  def test_assert_deep_equivalent_diff_rhs_only(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.y: missing vs DataItem\(2, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1, y=2)
      traversing_test_utils.assert_deep_equivalent(a, b)

  def test_assert_deep_equivalent_diff_lhs_only_dict_key(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.mapping\[\'b\'\]: '
        r'DataItem\(Entity\(x=2\), schema: ENTITY\(x=INT32\)\) vs missing',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.new(
          mapping=bag_a.dict(
              ds(['a', 'b', 'c', 'd']), bag_a.new(x=ds([1, 2, 3, 4]))
          )
      )
      ds_b = bag_b.new(
          mapping=bag_b.dict(ds(['a', 'c', 'd']), bag_b.new(x=ds([1, 3, 4])))
      )
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b)

  def test_assert_deep_equivalent_diff_lists(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'\[2\]: DataItem\(3, schema: INT32\) vs missing',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.list([1, 2, 3])
      ds_b = bag_b.list([1, 2])
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b)

  def test_assert_deep_equivalent_diff_object_types(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.mapping: '
        r'DataItem\(Entity\(a=1, c=3, d=4\), schema: ENTITY\(.*\)\) vs '
        r'DataItem\(Dict{.*}, schema: DICT{STRING, INT32}\)',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.new(mapping=bag_a.new(a=1, c=3, d=4))
      ds_b = bag_b.new(mapping=bag_b.dict(ds(['a', 'c', 'd']), ds([1, 3, 4])))
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b)

  def test_assert_deep_equivalent_diff_partial_lhs_only(self):
    bag_a = bag()
    bag_b = bag()
    a = bag_a.new(x=1)
    b = bag_b.new()
    traversing_test_utils.assert_deep_equivalent(a, b, partial=True)

  def test_assert_deep_equivalent_diff_partial_rhs_only(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'.y: missing vs DataItem\(2, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1, y=2)
      traversing_test_utils.assert_deep_equivalent(a, b, partial=True)

  def test_assert_deep_equivalent_partial_diff_lists(self):
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'\[2\]: DataItem\(3, schema: INT32\) vs missing',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.list([1, 2, 3])
      ds_b = bag_b.list([1, 2])
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b, partial=True)

  def test_assert_deep_equivalent_diff_not_schemas_equality_lhs_only(self):
    # TODO: improve the error message for schema ObjectId mismatch.
    with self.assertRaisesRegex(
        AssertionError,
        'DataSlices are not equivalent, mismatches found at:\n'
        r'DataItem\(Entity\(x=1\), schema: ENTITY\(x=INT32\)\) vs '
        r'DataItem\(Entity\(x=1\), schema: ENTITY\(x=INT32\)\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1)
      traversing_test_utils.assert_deep_equivalent(a, b, schemas_equality=True)


if __name__ == '__main__':
  absltest.main()
