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

import re

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    *(
        (DATA_SLICE, attrs_qtype, DATA_SLICE)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreStrictWithAttrsTest(absltest.TestCase):

  def test_strict_with_attrs_success(self):
    o = kde.new(x=1, y=10).eval()
    o2 = kde.core.strict_with_attrs(
        o,
        x=3,
        y=9,
    ).eval()
    self.assertNotEqual(o.get_bag().fingerprint, o2.get_bag().fingerprint)
    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o.y.no_bag(), ds(10))
    testing.assert_equal(o2.x.no_bag(), ds(3))
    testing.assert_equal(o2.y.no_bag(), ds(9))

  def test_error(self):
    o = kde.new(x=1, y=10).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.core.strict_with_attrs: attribute 'z' not found"
            ' in schema: ENTITY(x=INT32, y=INT32)'
        ),
    ):
      _ = kde.core.strict_with_attrs(o, z=1).eval()

    o = kde.obj(x=1).eval()
    with self.assertRaisesRegex(
        ValueError,
        'kd.core.strict_with_attrs: x must have an Entity'
        ' schema, actual schema: OBJECT',
    ):
      _ = kde.core.strict_with_attrs(o, x=2).eval()

  def test_strict_with_attrs_fails(self):
    o = kde.new(x=1, y=10).eval()
    with self.assertRaisesRegex(
        ValueError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kde.core.strict_with_attrs(o, x='2').eval()

  def test_error_primitives(self):
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes, got INT32'
    ):
      _ = kde.core.strict_with_attrs(ds(0).with_bag(bag()), x=1).eval()

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'got SCHEMA DataItem with primitive INT32'
    ):
      _ = kde.core.strict_with_attrs(
          schema_constants.INT32.with_bag(bag()), x=1
      ).eval()

  def test_error_no_databag(self):
    o = bag().new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'the DataSlice is a reference without a bag',
    ):
      _ = kde.core.strict_with_attrs(o, x=1).eval()

  def test_complex_type_conflict_error_message(self):
    o = bag().new(x=bag().new(y=2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """kd.core.strict_with_attrs: the schema for attribute 'x' is incompatible.

Expected schema for 'x': ENTITY(y=INT32)
Assigned schema for 'x': ENTITY(z=INT32)"""
        ),
    ):
      _ = kde.core.strict_with_attrs(o, x=bag().new(z=3)).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.strict_with_attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.strict_with_attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.strict_with_attrs, kde.strict_with_attrs)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.strict_with_attrs(I.x, a=I.y)),
        'kd.core.strict_with_attrs(I.x, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
