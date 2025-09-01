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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import extension_types
from koladata.types import schema_constants

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


@extension_types.extension_type()
class A:
  a: schema_constants.INT32

  @extension_types.virtual()
  def get_newest(self):
    return self.a


@extension_types.extension_type()
class B(A):
  b: schema_constants.INT32

  @extension_types.override()
  def get_newest(self):
    return self.b


@extension_types.extension_type()
class C(B):
  c: schema_constants.INT32

  @extension_types.override()
  def get_newest(self):
    return self.c


@extension_types.extension_type()
class B2(A):
  b2: schema_constants.INT32

  @extension_types.override()
  def get_newest(self):
    return self.b2


A_qtype = extension_type_registry.get_extension_qtype(A)
B_qtype = extension_type_registry.get_extension_qtype(B)
C_qtype = extension_type_registry.get_extension_qtype(C)
B2_qtype = extension_type_registry.get_extension_qtype(B2)


class ExtensionTypesDynamicCastTest(parameterized.TestCase):

  def test_self_cast(self):
    ext = A(1)
    result = expr_eval.eval(
        kde.extension_types.dynamic_cast(I.ext, A_qtype), ext=ext
    )
    testing.assert_equal(result, ext)

  def test_upcast(self):
    ext = B(1, 2)
    result = expr_eval.eval(
        kde.extension_types.dynamic_cast(I.ext, A_qtype), ext=ext
    )
    testing.assert_equal(result.qtype, A_qtype)
    testing.assert_equal(result.a, ds(1))
    with self.assertRaisesRegex(AttributeError, "no attribute 'b'"):
      _ = result.b
    testing.assert_equal(result.get_newest(), ds(2))

  def test_upcast_to_grandparent(self):
    ext = C(1, 2, 3)
    result = expr_eval.eval(
        kde.extension_types.dynamic_cast(I.ext, A_qtype), ext=ext
    )
    testing.assert_equal(result.qtype, A_qtype)
    testing.assert_equal(result.a, ds(1))
    with self.assertRaisesRegex(AttributeError, "no attribute 'c'"):
      _ = result.c
    testing.assert_equal(result.get_newest(), ds(3))

  def test_downcast(self):
    ext = A(1)
    result = expr_eval.eval(
        kde.extension_types.dynamic_cast(I.ext, B_qtype), ext=ext
    )
    testing.assert_equal(result.qtype, B_qtype)
    testing.assert_equal(result.a, ds(1))
    with self.assertRaisesRegex(ValueError, "attribute not found: 'b'"):
      _ = result.b
    testing.assert_equal(result.get_newest(), ds(1))

  def test_sidecast(self):
    ext = B2(1, 2)
    result = expr_eval.eval(
        kde.extension_types.dynamic_cast(I.ext, B_qtype), ext=ext
    )
    testing.assert_equal(result.qtype, B_qtype)
    testing.assert_equal(result.a, ds(1))
    with self.assertRaisesRegex(AttributeError, "no attribute 'b2'"):
      _ = result.b2
    with self.assertRaisesRegex(ValueError, "attribute not found: 'b'"):
      _ = result.b
    testing.assert_equal(result.get_newest(), ds(2))

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(kde.extension_types.dynamic_cast(I.x, I.y))
    )
    # Has the view of the QType.
    self.assertTrue(
        hasattr(kde.extension_types.dynamic_cast(I.x, A_qtype), "a")
    )
    self.assertFalse(
        hasattr(kde.extension_types.dynamic_cast(I.x, A_qtype), "b")
    )


if __name__ == "__main__":
  absltest.main()
