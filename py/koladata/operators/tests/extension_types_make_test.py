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
  y: data_bag.DataBag


A_qtype = extension_type_registry.get_extension_qtype(A)


class ExtensionTypesMakeTest(parameterized.TestCase):

  def test_make_empty(self):
    result = expr_eval.eval(kde.extension_types.make(A_qtype))
    expected = expr_eval.eval(
        kde.extension_types.wrap(objects.Object(), A_qtype)
    )
    testing.assert_equal(result, expected)

  def test_make_no_prototype(self):
    db = bag()
    result = expr_eval.eval(kde.extension_types.make(A_qtype, x=1, y=db))
    expected = expr_eval.eval(
        kde.extension_types.wrap(objects.Object(x=ds(1), y=db), A_qtype)
    )
    testing.assert_equal(result, expected)

  def test_make_with_prototype(self):
    prototype = objects.Object(a=arolla.int32(1))
    db = bag()
    result = expr_eval.eval(
        kde.extension_types.make(A_qtype, prototype, x=1, y=db)
    )
    expected = expr_eval.eval(
        kde.extension_types.wrap(
            objects.Object(prototype, x=ds(1), y=db), A_qtype
        )
    )
    testing.assert_equal(result, expected)

  def test_make_not_an_object_prototype(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("expected OBJECT, got prototype: DATA_SLICE")
    ):
      kde.extension_types.make(A_qtype, ds(1))

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde.extension_types.make(A_qtype)))
    # Has the view of the ext QType.
    self.assertTrue(hasattr(kde.extension_types.make(A_qtype), "x"))
    self.assertFalse(hasattr(kde.extension_types.make(A_qtype), "z"))
    self.assertTrue(
        hasattr(kde.extension_types.make(A_qtype, x=I.x, y=I.y), "x")
    )


if __name__ == "__main__":
  absltest.main()
