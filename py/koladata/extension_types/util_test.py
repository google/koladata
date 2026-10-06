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
from arolla import arolla
from koladata.extension_types import extension_types
from koladata.extension_types import util
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import mask_constants
from koladata.types import schema_constants

M = arolla.M
ds = data_slice.DataSlice.from_vals


@extension_types.extension_type()
class A(util.NullableMixin):
  x: schema_constants.INT32


@extension_types.extension_type()
class B(A):
  y: schema_constants.FLOAT32


A_QTYPE = extension_type_registry.get_extension_qtype(A)
B_QTYPE = extension_type_registry.get_extension_qtype(B)


class NullableMixinTest(absltest.TestCase):

  def test_parent_null(self):

    with self.subTest('eager'):
      a_null = A.get_null()
      testing.assert_equal(a_null.qtype, A_QTYPE)
      testing.assert_equal(a_null.is_null(), mask_constants.present)

    with self.subTest('lazy'):
      a_null = A.get_null()
      expr = arolla.literal(a_null).is_null()
      testing.assert_equal(expr.eval(), mask_constants.present)

  def test_child_null(self):

    with self.subTest('eager'):
      b_null = B.get_null()
      testing.assert_equal(b_null.qtype, B_QTYPE)
      testing.assert_equal(b_null.is_null(), mask_constants.present)
      testing.assert_equal(
          extension_type_registry.dynamic_cast(b_null, A_QTYPE).is_null(),
          mask_constants.present,
      )

    with self.subTest('lazy'):
      b_null = B.get_null()
      expr = arolla.literal(b_null).is_null()
      testing.assert_equal(expr.eval(), mask_constants.present)

  def test_not_null(self):

    with self.subTest('eager'):
      a_not_null = A(x=1)
      testing.assert_equal(a_not_null.is_null(), mask_constants.missing)

    with self.subTest('lazy'):
      a_not_null = A(x=1)
      expr = arolla.literal(a_not_null).is_null()
      testing.assert_equal(expr.eval(), mask_constants.missing)

  def test_get_attr_on_null(self):
    a_null = A.get_null()
    with self.assertRaisesRegex(ValueError, "attribute not found: 'x'"):
      _ = a_null.x

  def test_with_attrs_on_null(self):
    a_null = A.get_null()
    with self.assertRaisesRegex(
        ValueError, 'expected a non-null extension type'
    ):
      _ = a_null.with_attrs(x=1)

  def test_repr(self):
    a_null = A.get_null()
    self.assertEqual(repr(a_null), 'A(x=<unknown>)')

  def test_positional_initialization(self):
    # Sanity check that we're not injecting values into the signature which
    # would affect initialization.
    testing.assert_equal(A(1).x, ds(1))


if __name__ == '__main__':
  absltest.main()
