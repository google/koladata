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
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


def generate_qtypes():
  for schema_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
      for attrs_type in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES:
        yield arolla.UNSPECIFIED, schema_arg_type, DATA_SLICE, itemid_arg_type, attrs_type, NON_DETERMINISTIC_TOKEN, DATA_SLICE


QTYPES = list(generate_qtypes())


class EntitiesNewTest(absltest.TestCase):

  def test_item(self):
    x = kd.entities.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b, schema_constants.STRING.with_bag(x.get_bag())
    )

  def test_slice(self):
    x = kd.entities.new(
        a=ds([[1, 2], [3]]),
        b=kd.entities.new(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b.bb, schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().c, schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = kd.entities.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    y = kd.entities.new(x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_bag(y.get_bag())
    )
    testing.assert_equal(y.x.b, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.get_schema().x.with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = kd.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    x = kd.entities.new(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_schema_arg_simple(self):
    schema = bag().new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = kd.entities.new(a=42, b='xyz', schema=schema)
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_deep(self):
    nested_schema = bag().new_schema(p=schema_constants.BYTES)
    schema = bag().new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
        nested=nested_schema,
    )
    x = kd.entities.new(
        a=42,
        b='xyz',
        nested=kd.entities.new(p=b'0123', schema=nested_schema),
        schema=schema,
    )
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b', 'nested'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)
    testing.assert_equal(x.nested.p, ds(b'0123').with_bag(x.get_bag()))
    testing.assert_equal(
        x.nested.get_schema().p.no_bag(), schema_constants.BYTES
    )

  def test_schema_arg_schema_with_fallback(self):
    schema = bag().new_schema(a=schema_constants.INT32)
    fallback_bag = bag()
    schema.with_bag(fallback_bag).set_attr('b', schema_constants.STRING)
    schema = schema.enriched(fallback_bag)
    x = kd.entities.new(a=42, b='xyz', schema=schema)
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = bag().new_schema(a=schema_constants.FLOAT32)
    x = kd.entities.new(a=42, schema=schema)
    self.assertEqual(x.get_attr_names(intersection=True), ['a'])
    testing.assert_equal(
        x.a, ds(42, schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = bag().new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError, re.escape("schema for attribute 'a' is incompatible")
    ):
      kd.entities.new(a='xyz', schema=schema)

  def test_schema_arg_overwrite_schema(self):
    schema = bag().new_schema(a=schema_constants.FLOAT32)
    x = kd.entities.new(a=42, b='xyz', schema=schema, overwrite_schema=True)
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_embed_schema(self):
    schema = bag().new_schema(a=schema_constants.OBJECT)
    x = kd.entities.new(a=kd.entities.new(p=42, q='xyz'), schema=schema)
    self.assertEqual(x.get_attr_names(intersection=True), ['a'])
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        x.a.get_attr('__schema__').p.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        x.a.get_attr('__schema__').q.no_bag(), schema_constants.STRING
    )

  def test_str_as_schema_arg(self):
    x = kd.entities.new(schema='name', a=42)
    expected_schema = kd.named_schema('name')
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    x = kd.entities.new(schema=ds('name'), a=42)
    expected_schema = kd.named_schema('name')
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      kd.entities.new(a=1, schema=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      kd.entities.new(a=1, schema=ds(['name']))
    with self.assertRaisesRegex(ValueError, 'schema can only be 0-rank'):
      kd.entities.new(
          a=1, schema=ds([schema_constants.INT32, schema_constants.STRING])
      )
    with self.assertRaisesRegex(
        ValueError, 'kd.entities.new: expected Entity schema, got INT32'
    ):
      kd.entities.new(a=1, schema=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError, 'kd.entities.new: expected Entity schema, got OBJECT'
    ):
      kd.entities.new(a=1, schema=schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.entities.new: expected Entity schema, got LIST[INT32]'),
    ):
      kd.entities.new(a=1, schema=kd.list_schema(schema_constants.INT32))

  def test_converter_not_supported(self):
    with self.assertRaisesRegex(
        ValueError, 'please use eager only kd.eager.new'
    ):
      _ = kd.entities.new(ds(42))

  def test_non_determinism(self):
    schema = bag().new_schema(a=schema_constants.INT32).freeze_bag()
    expr = kde.entities.new(schema=schema, a=42)
    res_1 = expr.eval()
    res_2 = expr.eval()
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.entities.new,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.entities.new(I.x)))
    self.assertTrue(
        view.has_koda_view(kde.entities.new(itemid=I.itemid, a=I.y))
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.entities.new, kde.new))

  def test_repr(self):
    self.assertEqual(
        repr(kde.entities.new(a=I.y)),
        'kd.entities.new(unspecified, schema=unspecified, '
        'overwrite_schema=DataItem(False, schema: BOOLEAN), '
        'itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
