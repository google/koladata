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
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG

QTYPES = frozenset([
    (DATA_SLICE, DATA_BAG),
    *(
        (DATA_SLICE, attrs_qtype, DATA_BAG)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreStrictAttrsTest(absltest.TestCase):

  def test_multi_attr_overwrite_fails(self):
    o = kd.new(x=1, y=10)
    with self.assertRaisesRegex(
        ValueError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kd.core.strict_attrs(o, x='2')

    db2 = kd.core.strict_attrs(
        o,
        x=3,
        y=9,
    )
    self.assertNotEqual(o.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o.y.no_bag(), ds(10))
    testing.assert_equal(o.updated(db2).x.no_bag(), ds(3))
    testing.assert_equal(o.updated(db2).y.no_bag(), ds(9))

  def test_error(self):
    o = kd.new(x=1, y=10)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "attribute 'z' not found in schema: ENTITY(x=INT32, y=INT32)"
        ),
    ):
      _ = kd.core.strict_attrs(o, z=1)

    o = kd.obj(x=1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('x must have an Entity schema, actual schema: OBJECT'),
    ):
      _ = kd.core.strict_attrs(o, x=2)

  def test_error_primitives(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('primitives do not have attributes, got INT32')
    ):
      _ = kd.core.strict_attrs(ds(0).with_bag(bag()), x=1)

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('got SCHEMA DataItem with primitive INT32')
    ):
      _ = kd.core.strict_attrs(schema_constants.INT32.with_bag(bag()), x=1)

  def test_error_no_databag(self):
    o = bag().new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError, re.escape('the DataSlice is a reference without a bag')
    ):
      _ = kd.core.strict_attrs(o, x=1)

  def test_complex_type_conflict_error_message(self):
    o = bag().new(x=bag().new(y=2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema for attribute 'x' is incompatible.

Expected schema for 'x': ENTITY(y=INT32)
Assigned schema for 'x': ENTITY(z=INT32)"""),
    ):
      _ = kd.core.strict_attrs(o, x=bag().new(z=3))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.strict_attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.strict_attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.strict_attrs, kde.strict_attrs)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.strict_attrs(I.x, a=I.y)),
        'kd.core.strict_attrs(I.x, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
