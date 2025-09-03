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
from arolla.derived_qtype import derived_qtype
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

M = arolla.M | derived_qtype.M | objects.M


@extension_types.extension_type()
class A:
  a: schema_constants.INT32


A_qtype = extension_type_registry.get_extension_qtype(A)


class ExtensionTypesUnwrapTest(parameterized.TestCase):

  def test_unwrap(self):
    ext = A(1)
    result = expr_eval.eval(kde.extension_types.unwrap(ext))
    testing.assert_equal(result.qtype, objects.OBJECT)
    testing.assert_equal(result.get_attr("a", qtypes.DATA_SLICE), ds(1))
    testing.assert_equal(result, objects.Object(a=ds(1)))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.extension_types.unwrap,
        ((A_qtype, objects.OBJECT),),
        possible_qtypes=arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES
        + (A_qtype, objects.OBJECT),
    )

  def test_unwrap_not_wrapped(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("expected an extension type qtype, got ext: OBJECT"),
    ):
      kde.extension_types.unwrap(objects.Object(x=ds(1)))

  def test_unwrap_an_extension(self):
    foo_qtype = arolla.eval(
        M.derived_qtype.get_labeled_qtype(qtypes.DATA_SLICE, "foo")
    )
    ext = arolla.eval(M.derived_qtype.downcast(foo_qtype, ds(1)))
    with self.assertRaisesRegex(
        ValueError,
        re.escape("expected an extension type qtype, got ext: LABEL[foo]"),
    ):
      kde.extension_types.unwrap(ext)

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde.extension_types.unwrap(I.x)))
    self.assertTrue(view.has_base_koda_view(kde.extension_types.unwrap(I.x)))


if __name__ == "__main__":
  absltest.main()
