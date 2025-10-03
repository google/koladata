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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.extension_types import extension_types
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty

M = arolla.M


@extension_types.extension_type()
class A:
  x: schema_constants.INT32
  y: data_bag.DataBag


A_qtype = extension_type_registry.get_extension_qtype(A)


class ExtensionTypesGetAttrQTypeTest(parameterized.TestCase):

  def test_get_attr_qtype_ds(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(kde.extension_types.get_attr_qtype(a, "x"))
    testing.assert_equal(result, qtypes.DATA_SLICE)

  def test_get_attr_qtype_ds_attr_name(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(kde.extension_types.get_attr_qtype(a, ds("x")))
    testing.assert_equal(result, qtypes.DATA_SLICE)

  def test_get_attr_qtype_db(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(kde.extension_types.get_attr_qtype(a, "y"))
    testing.assert_equal(result, qtypes.DATA_BAG)

  def test_get_attr_qtype_non_existent_attr(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(kde.extension_types.get_attr_qtype(a, "z"))
    testing.assert_equal(result, arolla.NOTHING)

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(kde.extension_types.get_attr_qtype(I.x, "x"))
    )
    self.assertTrue(
        view.has_base_koda_view(kde.extension_types.get_attr_qtype(I.x, "x"))
    )


if __name__ == "__main__":
  absltest.main()
