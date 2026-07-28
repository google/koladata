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


class ExtensionTypesWithAttrsTest(parameterized.TestCase):

  def test_with_attrs(self):
    db = bag()
    a = A(ds(1), db)
    result = expr_eval.eval(kde.extension_types.with_attrs(a, x=ds(2)))
    testing.assert_equal(result.x, ds(2))
    testing.assert_equal(result.y, db)

  def test_qtype_signatures(self):
    empty_nt = arolla.make_namedtuple_qtype()
    non_empty_nt = arolla.make_namedtuple_qtype(
        x=qtypes.DATA_SLICE, y=qtypes.DATA_BAG
    )
    possible_qtypes = arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES + (
        objects.OBJECT,
        A_qtype,
        empty_nt,
        non_empty_nt,
    )
    arolla.testing.assert_qtype_signatures(
        kde.extension_types.with_attrs,
        (
            (A_qtype, A_qtype),
            (A_qtype, empty_nt, A_qtype),
            (A_qtype, non_empty_nt, A_qtype),
        ),
        possible_qtypes=possible_qtypes,
    )

  def test_with_attrs_wrong_type(self):
    a = A(ds(1), bag())
    new_bag = bag()
    res = arolla.eval(kde.extension_types.with_attrs(a, x=new_bag))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "looked for attribute 'x' with type DATA_SLICE, but the"
            " attribute has actual type DATA_BAG"
        ),
    ):
      _ = res.x
    unwrapped_res = arolla.eval(kde.extension_types.unwrap(res))
    x = unwrapped_res.get_attr("x", qtypes.DATA_BAG)
    testing.assert_equal(x, new_bag)

  def test_with_attrs_non_existent_attr(self):
    a = A(ds(1), bag())
    res = arolla.eval(kde.extension_types.with_attrs(a, z=ds(2)))
    with self.assertRaisesRegex(AttributeError, "no attribute 'z'"):
      _ = res.z
    unwrapped_res = arolla.eval(kde.extension_types.unwrap(res))
    z = unwrapped_res.get_attr("z", qtypes.DATA_SLICE)
    testing.assert_equal(z, ds(2))

  def test_with_attrs_on_null(self):
    a_null = expr_eval.eval(kde.extension_types.make_null(A_qtype))
    with self.assertRaisesRegex(
        ValueError, "expected a non-null extension type"
    ):
      _ = expr_eval.eval(kde.extension_types.with_attrs(a_null, x=ds(1)))

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde.extension_types.with_attrs(I.x)))
    # Has the view of the ext QType.
    self.assertFalse(hasattr(kde.extension_types.with_attrs(I.x), "a"))
    self.assertTrue(
        hasattr(
            kde.extension_types.with_attrs(M.annotation.qtype(I.x, A_qtype)),
            "x",
        )
    )
    self.assertFalse(
        hasattr(
            kde.extension_types.with_attrs(M.annotation.qtype(I.x, A_qtype)),
            "z",
        )
    )


if __name__ == "__main__":
  absltest.main()
