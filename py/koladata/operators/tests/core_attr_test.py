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
    res = kd.core.attr(x, attr_name, value)
    if isinstance(attr_name, data_slice.DataSlice):
      attr_name = attr_name.to_py()
    x.set_attr(attr_name, value)
    self.assertNotEqual(res.fingerprint, db.fingerprint)
    testing.assert_equivalent(res, db)
    self.assertFalse(res.is_mutable())

  def test_entity_as_obj_conflict(self):
    o = kd.stack(
        bag().obj(bag().new(x='1', y=10)), bag().obj(bag().new(x=2, y=20))
    )
    with self.assertRaisesRegex(
        ValueError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kd.core.attr(o, 'x', '2')
    db2 = kd.core.attr(o, 'x', '2', overwrite_schema=True)
    testing.assert_equal(o.updated(db2).x.no_bag(), ds(['2', '2']))

  def test_attr_update_on_objects(self):
    o = kd.obj(x=3.14)
    db = kd.core.attr(o, 'x', '2')
    testing.assert_equal(o.updated(db).x.no_bag(), ds('2'))

  def test_attr_update_implicit_casting(self):
    o = kd.new(x=3.14)
    db = kd.core.attr(o, 'x', 42)
    testing.assert_equal(o.updated(db).x.no_bag(), ds(42.0))

  def test_attr_update_with_ds_attr(self):
    with self.subTest('object'):
      ds1 = kd.stack(kd.obj(x=1), kd.obj(y=2))
      db = kd.core.attr(ds1, ds(['x', 'y']), 42)
      testing.assert_equal(ds1.updated(db).maybe('x').no_bag(), ds([42, None]))
      testing.assert_equal(ds1.updated(db).maybe('y').no_bag(), ds([None, 42]))

    with self.subTest('entity'):
      ds1 = kd.new(x=ds([1, 3]), y=ds([2, 4]))
      db = kd.core.attr(ds1, ds(['x', 'y']), 42)
      testing.assert_equal(ds1.updated(db).x.no_bag(), ds([42, 3]))
      testing.assert_equal(ds1.updated(db).y.no_bag(), ds([2, 42]))

  def test_invalid_attr_name(self):
    o = bag().new(x=1)
    with self.assertRaisesRegex(
        ValueError, 'argument `attr_name` must be an item holding STRING'
    ):
      kd.core.attr(o, 42, 42)
    with self.assertRaisesRegex(
        ValueError, 'argument `attr_name` must be a slice of STRING'
    ):
      kd.core.attr(o, ds([1]), 42)

  def test_invalid_overwrite_schema(self):
    o = bag().new(x=1)
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got an '
        'item of INT32',
    ):
      kd.core.attr(o, 'x', 2, overwrite_schema=1)
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got a '
        'slice of rank 1 > 0',
    ):
      kd.core.attr(o, 'x', 2, overwrite_schema=ds([True]))

  def test_complex_type_conflict_error_message(self):
    o = bag().new(x=bag().new(y=2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "Expected schema for 'x': ENTITY(y=INT32)\nAssigned schema for 'x':"
            ' ENTITY(z=INT32)'
        ),
    ):
      _ = kd.core.attr(o, 'x', bag().new(z=3))

  def test_error_primitives(self):
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes, got INT32'
    ):
      _ = kd.core.attr(ds(0).with_bag(bag()), 'x', 1)

  def test_error_no_databag(self):
    o = bag().new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError, 'the DataSlice is a reference without a bag'
    ):
      _ = kd.core.attr(o, 'x', 1)

  def test_schema_works(self):
    o = bag().new_schema()
    db = kd.core.attr(o, 'x', schema_constants.INT32)
    self.assertEqual(o.with_bag(db).x.no_bag(), schema_constants.INT32)
    db = kd.core.attr(o, 'x', schema_constants.INT32, overwrite_schema=True)
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
        'kd.core.attr(I.x, I.a, I.y, DataItem(False, schema: BOOLEAN))',
    )


if __name__ == '__main__':
  absltest.main()
