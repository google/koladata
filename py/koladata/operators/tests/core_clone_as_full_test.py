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
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
present = arolla.present()


class CoreCloneAsFullTest(parameterized.TestCase):

  def test_empty_scalar(self):
    schema = kd.schema.new_schema(a=schema_constants.INT32)
    x = ds(None, schema=schema)
    cloned = kd.clone_as_full(x, a=ds(57))
    testing.assert_equal(kd.has(cloned), ds(present))
    testing.assert_equal(cloned.a.no_bag(), ds(57))
    testing.assert_equal(cloned.get_schema().no_bag(), schema.no_bag())

  def test_present_scalar(self):
    x = kd.new(a=1)
    cloned = kd.clone_as_full(x, b=ds(57))
    testing.assert_equal(kd.has(cloned), ds(present))
    testing.assert_equal(cloned.b.no_bag(), ds(57))
    testing.assert_equivalent(
        cloned.get_schema(),
        x.get_schema().with_attrs(b=schema_constants.INT32),
        ids_equality=True,
    )

  def test_array(self):
    x = kd.new(a=ds([1, 2]))
    x = x & ds([present, None])

    cloned = kd.clone_as_full(x, b=ds([3, 4]))
    testing.assert_equal(
        cloned.get_present_count(), ds(2, schema=schema_constants.INT64)
    )
    testing.assert_equal(cloned.a.no_bag(), ds([1, None]))
    testing.assert_equal(cloned.b.no_bag(), ds([3, 4]))
    testing.assert_equivalent(
        cloned.get_schema(),
        x.get_schema().with_attrs(b=schema_constants.INT32),
        ids_equality=True,
    )

  def test_full_array(self):
    x = kd.new(a=ds([1, 2]))
    cloned = kd.clone_as_full(x, b=ds([3, 4]))
    testing.assert_equal(
        cloned.get_present_count(), ds(2, schema=schema_constants.INT64)
    )
    testing.assert_equal(cloned.a.no_bag(), ds([1, 2]))
    testing.assert_equal(cloned.b.no_bag(), ds([3, 4]))
    testing.assert_equivalent(
        cloned.get_schema(),
        x.get_schema().with_attrs(b=schema_constants.INT32),
        ids_equality=True,
    )

  def test_all_missing_array(self):
    schema = kd.schema.new_schema(a=schema_constants.INT32)
    x = ds([None, None], schema=schema)
    cloned = kd.clone_as_full(x, a=ds([3, 4]))
    testing.assert_equal(
        cloned.get_present_count(), ds(2, schema=schema_constants.INT64)
    )
    testing.assert_equal(cloned.a.no_bag(), ds([3, 4]))
    testing.assert_equivalent(
        cloned.get_schema(),
        x.get_schema(),
        ids_equality=True,
    )

  def test_itemid(self):
    x = kd.new(a=ds([1, 2]))
    x = x & ds([present, None])

    y = kd.new(a=ds([1, 2]))

    cloned = kd.clone_as_full(x, itemid=y)
    testing.assert_equal(
        cloned.get_present_count(), ds(2, schema=schema_constants.INT64)
    )
    testing.assert_equal(cloned.a.no_bag(), ds([1, None]))
    testing.assert_equal(cloned.get_itemid().no_bag(), y.get_itemid().no_bag())

  def test_itemid_not_full(self):
    x = kd.new(a=ds([1, 2]))
    x = x & ds([present, None])

    with self.assertRaisesRegex(
        ValueError, 'itemid must have an objectId for each item present in ds'
    ):
      kd.clone_as_full(x, itemid=x)

  def test_object(self):
    x = kd.obj(a=ds([1, 2]))
    # NOTE: b/469018567 - Consider supporting OBJECT for consistency with
    # kd.clone.
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got OBJECT'
    ):
      kd.clone_as_full(x, b=ds([3, 4]))

  def test_primitive(self):
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got NONE'
    ):
      kd.clone_as_full(ds(None))
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got INT32'
    ):
      kd.clone_as_full(ds(57))
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got NONE'
    ):
      kd.clone_as_full(ds([]))
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got INT32'
    ):
      kd.clone_as_full(ds([], schema=schema_constants.INT32))
    with self.assertRaisesRegex(
        ValueError, 'expected Entity schema, got INT32'
    ):
      kd.clone_as_full(ds([1, None]))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.clone_as_full(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.clone_as_full, kde.clone_as_full)
    )


if __name__ == '__main__':
  absltest.main()
