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

import re
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


class ExtensionTypesGetAttrTest(parameterized.TestCase):

  def test_get_attr_ds(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(
        kde.extension_types.get_attr(a, "x", qtypes.DATA_SLICE)
    )
    testing.assert_equal(result, ds(1))

  def test_get_attr_ds_attr_name(self):
    a = A(ds(1), bag())
    result = expr_eval.eval(
        kde.extension_types.get_attr(a, ds("x"), qtypes.DATA_SLICE)
    )
    testing.assert_equal(result, ds(1))

  def test_get_attr_db(self):
    db = bag()
    a = A(ds(1), db)
    result = expr_eval.eval(
        kde.extension_types.get_attr(a, "y", qtypes.DATA_BAG)
    )
    testing.assert_equal(result, db)

  def test_get_attr_wrong_type(self):
    a = A(ds(1), bag())
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "looked for attribute 'x' with type DATA_BAG, but the attribute has"
            " actual type DATA_SLICE"
        ),
    ):
      _ = expr_eval.eval(kde.extension_types.get_attr(a, "x", qtypes.DATA_BAG))

  def test_get_attr_non_existent_attr(self):
    a = A(ds(1), bag())
    with self.assertRaisesRegex(ValueError, "attribute not found: 'z'"):
      _ = expr_eval.eval(
          kde.extension_types.get_attr(a, "z", qtypes.DATA_SLICE)
      )

  def test_view(self):
    # Has the view of the ext QType.
    self.assertFalse(
        view.has_koda_view(kde.extension_types.get_attr(I.x, "x", A_qtype))
    )
    self.assertTrue(
        hasattr(
            kde.extension_types.get_attr(I.x, "x", A_qtype),
            "x",
        )
    )
    self.assertTrue(
        view.has_koda_view(
            kde.extension_types.get_attr(I.x, "x", qtypes.DATA_SLICE)
        )
    )


if __name__ == "__main__":
  absltest.main()
