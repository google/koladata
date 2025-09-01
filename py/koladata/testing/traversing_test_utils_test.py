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
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class TraversingTestUtilsTest(absltest.TestCase):

  def test_assert_deep_equivalent(self):
    traversing_test_utils.assert_deep_equivalent(ds([1, 2, 3]), ds([1, 2, 3]))

  def test_assert_deep_equivalent_diff(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataSlice\(.*\)\n'
        r'Actual: DataSlice\(.*\), with difference:\n'
        r'modified:\n'
        r'expected.S\[2\]:\n'
        r'DataItem\(4, schema: INT32\)\n'
        r'-> actual.S\[2\]:\n'
        r'DataItem\(3, schema: INT32\)'
    ):
      traversing_test_utils.assert_deep_equivalent(ds([1, 2, 3]), ds([1, 2, 4]))

  def test_assert_deep_equivalent_diff_deep(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'modified:\n'
        r"expected.mapping\['d'\].x:\n"
        r'DataItem\(5, schema: INT32\)\n'
        r"-> actual.mapping\['d'\].x:\n"
        r'DataItem\(4, schema: INT32\)'
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
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'added:\n'
        r'actual.x:\n'
        r'DataItem\(1, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new()
      traversing_test_utils.assert_deep_equivalent(a, b)

  def test_assert_deep_equivalent_msg(self):
    with self.assertRaisesWithLiteralMatch(
        AssertionError,
        'provided message',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new()
      traversing_test_utils.assert_deep_equivalent(a, b, msg='provided message')

  def test_assert_deep_equivalent_diff_rhs_only(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'deleted:\n'
        r'expected.y:\n'
        r'DataItem\(2, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1, y=2)
      traversing_test_utils.assert_deep_equivalent(a, b)

  def test_assert_deep_equivalent_diff_lhs_only_dict_key(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'added:\n'
        r"key 'b' in actual.mapping:\n"
        r'DataItem\(Entity\(x=2\), schema: '
        r'ENTITY\(x=INT32\)\)',
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
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'added:\n'
        r'item at position 2 in list actual:\n'
        r'DataItem\(3, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.list([1, 2, 3])
      ds_b = bag_b.list([1, 2])
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b)

  def test_assert_deep_equivalent_diff_objs(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'modified schema:\n'
        r'expected:\n'
        r'OBJECT\n'
        r'-> actual:\n'
        r'.*',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.new(x=1)
      ds_b = bag_b.obj(x=1)
      traversing_test_utils.assert_deep_equivalent(
          ds_a, ds_b, schemas_equality=True
      )

  def test_assert_deep_equivalent_diff_object_types(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'modified:\n'
        r'expected.mapping:\n'
        r'DataItem\(Dict{.*}, schema: DICT{STRING, INT32}\)\n'
        r'-> actual.mapping:\n'
        r'DataItem\(Entity\(a=1, c=3, d=4\), schema: '
        r'ENTITY\(a=INT32, c=INT32, d=INT32\)\)'
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
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'deleted:\n'
        r'expected.y:\n'
        r'DataItem\(2, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1, y=2)
      traversing_test_utils.assert_deep_equivalent(a, b, partial=True)

  def test_assert_deep_equivalent_partial_diff_lists(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'added:\n'
        r'item at position 2 in list actual:\n'
        r'DataItem\(3, schema: INT32\)',
    ):
      bag_a = bag()
      bag_b = bag()
      ds_a = bag_a.list([1, 2, 3])
      ds_b = bag_b.list([1, 2])
      traversing_test_utils.assert_deep_equivalent(ds_a, ds_b, partial=True)

  def test_assert_deep_equivalent_diff_not_schemas_equality_deep(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'modified schema:\n'
        r'expected.x:\n'
        r'(\$[0-9a-zA-Z]{22})\n'
        r'-> actual.x:\n'
        r'(\$[0-9a-zA-Z]{22})',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(schema='foo', x=bag_a.new(y=1))
      b = bag_b.new(schema='foo', x=bag_b.new(y=1))
      traversing_test_utils.assert_deep_equivalent(a, b, schemas_equality=True)

  def test_assert_deep_equivalent_diff_not_schemas_equality_root_slice(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(.*\)\n'
        r'Actual: DataItem\(.*\), with difference:\n'
        r'modified schema:\n'
        r'expected:\n'
        r'(\$[0-9a-zA-Z]{22})\n'
        r'-> actual:\n'
        r'(\$[0-9a-zA-Z]{22})',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=bag_a.new(y=1))
      b = bag_b.new(x=bag_b.new(y=1))
      traversing_test_utils.assert_deep_equivalent(a, b, schemas_equality=True)

  def test_assert_deep_equivalent_ids_equality(self):
    traversing_test_utils.assert_deep_equivalent(
        bag().uu(x=1, y=2), bag().uu(x=1, y=2), ids_equality=True
    )

  def test_assert_deep_equivalent_diff_ids_equality(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(Entity\(x=1\), schema:'
        r' ENTITY\(x=INT32\)\)\n'
        r'Actual: DataItem\(Entity\(x=1\), schema: ENTITY\(x=INT32\)\),'
        r' with difference:\n'
        r'modified:\n'
        r'expected:\n'
        r'DataItem\(Entity\(x=1\), schema: ENTITY\(x=INT32\)\)\n'
        r'-> actual:\n'
        r'DataItem\(Entity\(x=1\), schema: ENTITY\(x=INT32\)\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.new(x=1)
      b = bag_b.new(x=1)
      traversing_test_utils.assert_deep_equivalent(a, b, ids_equality=True)

  def test_assert_deep_equivalent_diff_ids_equality_nested(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(Entity\(.*\), schema: .*\)\n'
        r'Actual: DataItem\(Entity\(.*\), schema: .*\),'
        r' with difference:\n'
        r'modified:\n'
        r'expected.b:\n'
        r'DataItem\(Entity\(c=1\), schema: ENTITY\(c=INT32\)\)\n'
        r'-> actual.b:\n'
        r'DataItem\(Entity\(c=1\), schema: ENTITY\(c=INT32\)\)',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.uu(a=1, b=bag_a.uu(c=1))
      b = bag_b.uu(a=1, b=bag_b.uu(c=1))
      b.b = bag_b.uu(c=2)
      b.b.c = 1
      traversing_test_utils.assert_deep_equivalent(a, b, ids_equality=True)

  def test_assert_deep_equivalent_diff_ids_not_raised_nested(self):
    bag_a = bag()
    bag_b = bag()
    a = bag_a.uu(a=1, b=bag_a.uu(c=1))
    b = bag_b.uu(a=1, b=bag_b.uu(c=1))
    b.b = bag_b.uu(c=2)
    b.b.c = 1
    traversing_test_utils.assert_deep_equivalent(a, b, schemas_equality=True)

  def test_assert_deep_equivalent_diff_schema_ids_equality_nested(self):
    with self.assertRaisesRegex(
        AssertionError,
        r'Expected: is equal to DataItem\(Entity\(.*\), schema: .*\)\n'
        r'Actual: DataItem\(Entity\(.*\), schema: .*\),'
        r' with difference:\n'
        r'modified schema:\n'
        r'expected:\n'
        r'(\#[0-9a-zA-Z]{22})\n'
        r'-> actual:\n'
        r'(\#[0-9a-zA-Z]{22})',
    ):
      bag_a = bag()
      bag_b = bag()
      a = bag_a.uu(a=1, b=bag_a.uu(c=1, schema=bag_a.named_schema('foo')))
      b = bag_b.uu(a=1, b=bag_b.uu(c=1, schema=bag_b.named_schema('bar')))
      traversing_test_utils.assert_deep_equivalent(
          a, b, ids_equality=True, schemas_equality=True
      )

  def test_assert_deep_equivalent_schema_nested(self):
    bag_a = bag()
    bag_b = bag()
    a = bag_a.uu(a=1, b=bag_a.uu(c=1, schema=bag_a.named_schema('foo')))
    b = bag_b.uu(a=1, b=bag_b.uu(c=1, schema=bag_b.named_schema('bar')))
    _ = traversing_test_utils.assert_deep_equivalent(a, b, ids_equality=True)


if __name__ == '__main__':
  absltest.main()
