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

"""Tests for introspection."""

from absl.testing import absltest
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import schema_constants

kde = kde_operators.kde
I = input_container.InputContainer('I')
kd = eager_op_utils.operators_container('kde')


class IntrospectionTest(absltest.TestCase):

  def test_get_name(self):
    self.assertEqual(
        introspection.get_name(kde.with_name(I.x + I.y, 'foo')), 'foo'
    )
    self.assertEqual(
        introspection.get_name(kde.annotation.with_name(I.x + I.y, 'foo')),
        'foo',
    )
    self.assertIsNone(
        introspection.get_name(kde.with_name(I.x + I.y, 'foo') + I.z)
    )
    self.assertIsNone(introspection.get_name(py_boxing.as_expr(1)))

  def test_unwrap_named(self):
    testing.assert_equal(
        introspection.unwrap_named(kde.with_name(I.x + I.y, 'foo')), I.x + I.y
    )
    testing.assert_equal(
        introspection.unwrap_named(kde.annotation.with_name(I.x + I.y, 'foo')),
        I.x + I.y,
    )
    with self.assertRaisesRegex(ValueError, 'non-named'):
      introspection.unwrap_named(kde.with_name(I.x + I.y, 'foo') + I.z)

  def test_pack_expr(self):
    ds = introspection.pack_expr(I.x + I.y)
    self.assertEqual(ds.get_schema(), schema_constants.EXPR)
    self.assertEqual(ds.get_ndim(), 0)
    testing.assert_equal(introspection.unpack_expr(ds), I.x + I.y)

  def test_unpack_expr(self):
    ds = introspection.pack_expr(I.x + I.y)
    testing.assert_equal(introspection.unpack_expr(ds), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(ds & mask_constants.missing)
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(ds.with_schema(schema_constants.ANY))
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(ds.with_schema(schema_constants.OBJECT))
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(ds.repeat(1))

  def test_is_packed_expr(self):
    ds = introspection.pack_expr(I.x + I.y)
    testing.assert_equal(
        introspection.is_packed_expr(ds), mask_constants.present
    )
    testing.assert_equal(
        introspection.is_packed_expr(ds & mask_constants.missing),
        mask_constants.missing,
    )
    testing.assert_equal(
        introspection.is_packed_expr(ds.with_schema(schema_constants.ANY)),
        mask_constants.missing,
    )
    testing.assert_equal(
        introspection.is_packed_expr(ds.with_schema(schema_constants.OBJECT)),
        mask_constants.missing,
    )
    testing.assert_equal(
        introspection.is_packed_expr(ds.repeat(1)), mask_constants.missing
    )
    testing.assert_equal(
        introspection.is_packed_expr(I.x + I.y), mask_constants.missing
    )


if __name__ == '__main__':
  absltest.main()
