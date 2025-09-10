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
from arolla.objects import objects
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.extension_types import extension_types
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import schema_constants

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty

M = arolla.M | objects.M


@extension_types.extension_type()
class A:
  x: schema_constants.INT32


A_qtype = extension_type_registry.get_extension_qtype(A)


class ExtensionTypesMakeNullTest(parameterized.TestCase):

  def test_make_null(self):
    result = expr_eval.eval(kde.extension_types.make_null(A_qtype))
    testing.assert_equal(result.qtype, A_qtype)
    unwrapped = expr_eval.eval(kde.extension_types.unwrap(result))
    testing.assert_equal(
        unwrapped, objects.Object(_is_null_marker=arolla.present())
    )

  def test_get_attr_on_null(self):
    result = expr_eval.eval(kde.extension_types.make_null(A_qtype))
    with self.assertRaisesRegex(ValueError, "attribute not found: 'x'"):
      _ = result.x

  def test_with_attrs_on_null(self):
    result = expr_eval.eval(kde.extension_types.make_null(A_qtype))
    with self.assertRaisesRegex(
        ValueError, "expected a non-null extension type"
    ):
      _ = result.with_attrs(x=1)

  def test_not_a_qtype(self):
    with self.assertRaisesRegex(
        ValueError, "expected QTYPE, got qtype: DATA_SLICE"
    ):
      kde.extension_types.make_null(ds(1))

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde.extension_types.make_null(A_qtype)))
    # Has the view of the ext QType.
    self.assertTrue(hasattr(kde.extension_types.make_null(A_qtype), "x"))
    self.assertFalse(hasattr(kde.extension_types.make_null(A_qtype), "z"))


if __name__ == "__main__":
  absltest.main()
