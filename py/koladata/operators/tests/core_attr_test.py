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
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_BAG),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_BAG),
])


class CoreAttrTest(parameterized.TestCase):

  @parameterized.parameters(
      (bag(), '~3!_3', 42),
      (bag(), ds('~3!_3'), ds(42)),
      (bag(), ds('abc'), ds([1, 2, 3])),
  )
  def test_eval(self, db, attr_name, value):
    x = db.new_shaped(ds(value).get_shape())
    res = kde.core.attr(x, attr_name, value).eval()
    if isinstance(attr_name, data_slice.DataSlice):
      attr_name = attr_name.to_py()
    x.set_attr(attr_name, value)
    self.assertNotEqual(res.fingerprint, db.fingerprint)
    testing.assert_equivalent(res, db)
    self.assertFalse(res.is_mutable())

  def test_entity_as_obj_conflict(self):
    o = kde.stack(
        bag().obj(bag().new(x='1', y=10)), bag().obj(bag().new(x=2, y=20))
    ).eval()
    with self.assertRaisesRegex(
        exceptions.KodaError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kde.core.attr(o, 'x', '2').eval()
    db2 = kde.core.attr(o, 'x', '2', update_schema=True).eval()
    testing.assert_equal(o.updated(db2).x.no_bag(), ds(['2', '2']))

  def test_invalid_attr_name(self):
    o = bag().new(x=1)
    with self.assertRaisesRegex(
        ValueError, 'requires `attr_name` to be DataItem holding string'
    ):
      kde.core.attr(o, 42, 42).eval()
    with self.assertRaisesRegex(
        ValueError, 'requires `attr_name` to be DataItem holding string'
    ):
      kde.core.attr(o, ds(['a']), 42).eval()

  def test_invalid_update_schema(self):
    o = bag().new(x=1)
    with self.assertRaisesRegex(
        ValueError,
        'argument `update_schema` must be an item holding boolean, got an'
        ' item of INT32',
    ):
      kde.core.attr(o, 'x', 2, update_schema=1).eval()
    with self.assertRaisesRegex(
        ValueError,
        'argument `update_schema` must be an item holding boolean, got a slice'
        ' of rank 1 > 0',
    ):
      kde.core.attr(o, 'x', 2, update_schema=ds([True])).eval()

  def test_complex_type_conflict_error_message(self):
    o = bag().new(x=bag().new(y=2))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            "Expected schema for 'x': SCHEMA(y=INT32)\nAssigned schema for 'x':"
            ' SCHEMA(z=INT32)'
        ),
    ):
      _ = kde.core.attr(o, 'x', bag().new(z=3)).eval()

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'cannot get or set attributes on schema: INT32'
    ):
      _ = kde.core.attr(ds(0).with_bag(bag()), 'x', 1).eval()

  def test_error_no_databag(self):
    o = bag().new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'cannot set attributes on a DataSlice without a DataBag',
    ):
      _ = kde.core.attr(o, 'x', 1).eval()

  def test_any_works(self):
    o = bag().new().as_any()
    db = kde.core.attr(o, 'x', 1).eval()
    self.assertEqual(o.with_bag(db).x.no_bag(), ds(1))
    db = kde.core.attr(o, 'x', 1, update_schema=True).eval()
    self.assertEqual(o.with_bag(db).x.no_bag(), ds(1))

  def test_schema_works(self):
    o = bag().new_schema()
    db = kde.core.attr(o, 'x', schema_constants.INT32).eval()
    self.assertEqual(o.with_bag(db).x.no_bag(), schema_constants.INT32)
    db = kde.core.attr(
        o, 'x', schema_constants.INT32, update_schema=True
    ).eval()
    self.assertEqual(o.with_bag(db).x.no_bag(), schema_constants.INT32)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.attr(I.x, I.a, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.attr, kde.attr))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.attr(I.x, I.a, I.y)),
        'kde.core.attr(I.x, I.a, I.y, DataItem(False, schema: BOOLEAN))',
    )


if __name__ == '__main__':
  absltest.main()
