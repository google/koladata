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
    (DATA_SLICE, DATA_SLICE, DATA_BAG),
    *(
        (DATA_SLICE, DATA_SLICE, attrs_qtype, DATA_BAG)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreAttrsTest(absltest.TestCase):

  def test_multi_attr_overwrite(self):
    o = kd.new(x=1, y=10)
    with self.assertRaisesRegex(
        ValueError, "the schema for attribute 'x' is incompatible."
    ):
      _ = kd.core.attrs(o, x='2')
    db2 = kd.core.attrs(
        o, x='2', a=1, b='p', c=bag().list([1, 2]), overwrite_schema=True
    )
    self.assertNotEqual(o.get_bag().fingerprint, db2.fingerprint)

    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o.y.no_bag(), ds(10))
    testing.assert_equal(o.updated(db2).x.no_bag(), ds('2'))
    testing.assert_equal(o.updated(db2).y.no_bag(), ds(10))
    testing.assert_equal(o.updated(db2).a.no_bag(), ds(1))
    testing.assert_equal(o.updated(db2).b.no_bag(), ds('p'))
    testing.assert_equal(o.updated(db2).c[:].no_bag(), ds([1, 2]))

    self.assertSameElements(
        o.get_schema().get_attr_names(intersection=True), ['x', 'y']
    )
    self.assertSameElements(
        o.updated(db2).get_schema().get_attr_names(intersection=True),
        ['x', 'y', 'a', 'b', 'c'],
    )
    self.assertSameElements(
        o.with_bag(db2).get_schema().get_attr_names(intersection=True),
        ['x', 'a', 'b', 'c'],
    )

  def test_multi_attr_overwrite_object_schema(self):
    o = kd.obj(x=1, y=10)
    db2 = kd.core.attrs(o, x='2', a=1, b='p', c=bag().list([1, 2]))
    self.assertNotEqual(o.get_bag().fingerprint, db2.fingerprint)

    testing.assert_equal(o.x.no_bag(), ds(1))
    testing.assert_equal(o.y.no_bag(), ds(10))
    testing.assert_equal(o.updated(db2).x.no_bag(), ds('2'))
    testing.assert_equal(o.updated(db2).y.no_bag(), ds(10))
    testing.assert_equal(o.updated(db2).a.no_bag(), ds(1))
    testing.assert_equal(o.updated(db2).b.no_bag(), ds('p'))
    testing.assert_equal(o.updated(db2).c[:].no_bag(), ds([1, 2]))

    self.assertSameElements(
        o.get_obj_schema().get_attr_names(intersection=True), ['x', 'y']
    )
    self.assertSameElements(
        o.updated(db2).get_obj_schema().get_attr_names(intersection=True),
        ['x', 'y', 'a', 'b', 'c'],
    )
    self.assertSameElements(
        o.with_bag(db2).get_obj_schema().get_attr_names(intersection=True),
        ['x', 'a', 'b', 'c'],
    )

  def test_entity_as_obj_conflict(self):
    o = kd.stack(
        bag().obj(bag().new(x='1', y=10)), bag().obj(bag().new(x=2, y=20))
    )
    with self.assertRaisesRegex(
        ValueError, re.escape("the schema for attribute 'x' is incompatible")
    ):
      _ = kd.core.attrs(o, x='2')
    db2 = kd.core.attrs(o, x='2', overwrite_schema=True)
    testing.assert_equal(o.updated(db2).x.no_bag(), ds(['2', '2']))

  def test_attr_update_implicit_casting(self):
    o = kd.new(x=3.14)
    db = kd.core.attrs(o, x=42)
    testing.assert_equal(o.updated(db).x.no_bag(), ds(42.0))

  def test_empty_slice(self):
    entity = kd.new_like(ds([]))
    db = kd.core.attrs(entity, x=ds([], schema_constants.OBJECT))
    expected_db = bag()
    expected_db[entity.get_schema()].x = schema_constants.OBJECT
    testing.assert_equivalent(db, expected_db)

    db = kd.core.attrs(kd.obj_like(ds([])), x=ds([]))
    testing.assert_equivalent(db, bag())

  def test_empty_item(self):
    entity = kd.new_like(ds(None))
    db = kd.core.attrs(entity, x=42)
    expected_db = bag()
    expected_db[entity.get_schema()].x = schema_constants.INT32
    testing.assert_equivalent(db, expected_db)

    db = kd.core.attrs(kd.obj_like(ds(None)), x=42)
    testing.assert_equivalent(db, bag())

  def test_non_bool_overwrite_schema(self):
    o = bag().new(x=1)
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got an'
        ' item of INT32',
    ):
      kd.core.attrs(o, x=2, overwrite_schema=1)
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got a'
        ' slice of rank 1 > 0',
    ):
      kd.core.attrs(o, x=2, overwrite_schema=ds([True]))

  def test_complex_type_conflict_error_message(self):
    o = bag().new(x=bag().new(y=2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': ENTITY(y=INT32)
Assigned schema for 'x': ENTITY(z=INT32)

To fix this, explicitly override schema of 'x' in the original schema by passing overwrite_schema=True."""
        ),
    ):
      _ = kd.core.attrs(o, x=bag().new(z=3))

  def test_error_primitives(self):
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes, got INT32'
    ):
      _ = kd.core.attrs(ds(0).with_bag(bag()), x=1)

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'got SCHEMA DataItem with primitive INT32'
    ):
      _ = kd.core.attrs(schema_constants.INT32.with_bag(bag()), x=1)

  def test_error_no_databag(self):
    o = bag().new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError, 'the DataSlice is a reference without a bag'
    ):
      _ = kd.core.attrs(o, x=1)

  def test_schema_works(self):
    o = bag().new_schema()
    db = kd.core.attrs(o, x=schema_constants.INT32)
    self.assertEqual(o.with_bag(db).x.no_bag(), schema_constants.INT32)
    db = kd.core.attrs(o, x=schema_constants.INT32, overwrite_schema=True)
    self.assertEqual(o.with_bag(db).x.no_bag(), schema_constants.INT32)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.attrs,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.attrs(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.attrs, kde.attrs))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.attrs(I.x, a=I.y)),
        'kd.core.attrs(I.x, overwrite_schema=DataItem(False, schema: BOOLEAN),'
        ' a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
