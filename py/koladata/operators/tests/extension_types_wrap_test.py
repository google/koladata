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
from arolla.objects import objects
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.extension_types import extension_types
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


@extension_types.extension_type()
class A:
  a: schema_constants.INT32


A_qtype = extension_type_registry.get_extension_qtype(A)


class ExtensionTypesWrapTest(parameterized.TestCase):

  def test_wrap(self):
    obj = objects.Object(a=ds(1))
    result = expr_eval.eval(kde.extension_types.wrap(obj, A_qtype))
    testing.assert_equal(result.qtype, A_qtype)
    testing.assert_equal(result.a, ds(1))
    testing.assert_equal(result, A(1))

  def test_wrap_not_an_object(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("expected one of [OBJECT], got obj: LABEL[A]")
    ):
      kde.extension_types.wrap(A(1), A_qtype)

  def test_wrap_not_a_qtype(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("expected QTYPE, got qtype: DATA_SLICE")
    ):
      kde.extension_types.wrap(objects.Object(a=ds(1)), ds(1))

  def test_wrap_not_a_literal(self):
    expr = kde.extension_types.wrap(objects.Object(a=ds(1)), I.qtype)
    with self.assertRaisesRegex(
        ValueError, re.escape("`derived_qtype` must be a literal")
    ):
      expr_eval.eval(expr, qtype=A_qtype)

  def test_wrap_not_a_derived_qtype(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("expected DATA_SLICE, got value: OBJECT")
    ):
      expr_eval.eval(
          kde.extension_types.wrap(objects.Object(a=ds(1)), qtypes.DATA_SLICE)
      )

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde.extension_types.wrap(I.x, A_qtype)))
    # Has the view of the QType.
    self.assertTrue(hasattr(kde.extension_types.wrap(I.x, A_qtype), "a"))
    self.assertFalse(hasattr(kde.extension_types.wrap(I.x, A_qtype), "b"))


if __name__ == "__main__":
  absltest.main()
