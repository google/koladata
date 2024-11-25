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

from absl.testing import absltest
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
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
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


def generate_qtypes():
  for schema_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
      for attrs_type in [
          arolla.make_namedtuple_qtype(),
          arolla.make_namedtuple_qtype(a=DATA_SLICE),
          arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
      ]:
        yield arolla.UNSPECIFIED, schema_arg_type, DATA_SLICE, itemid_arg_type, attrs_type, arolla.types.INT64, DATA_SLICE


QTYPES = list(generate_qtypes())


class CoreNewTest(absltest.TestCase):

  def test_item(self):
    x = kde.core.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    ).eval()
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
    x = kde.core.new(
        a=ds([[1, 2], [3]]),
        b=kde.core.new(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    ).eval()
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
    x = kde.core.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    ).eval()
    y = kde.core.new(x=x).eval()
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_bag(y.get_bag())
    )
    testing.assert_equal(y.x.b, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.get_schema().x.with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    )
    x = kde.core.new(a=42, itemid=itemid).eval()
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_schema_arg_simple(self):
    schema = bag().new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = kde.core.new(a=42, b='xyz', schema=schema).eval()
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
    x = kde.core.new(
        a=42,
        b='xyz',
        nested=kde.core.new(p=b'0123', schema=nested_schema),
        schema=schema,
    ).eval()
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
    x = kde.core.new(a=42, b='xyz', schema=schema).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = bag().new_schema(a=schema_constants.FLOAT32)
    x = kde.core.new(a=42, schema=schema).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a'])
    testing.assert_equal(
        x.a, ds(42, schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = bag().new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError, r'schema for attribute \'a\' is incompatible'
    ):
      kde.core.new(a='xyz', schema=schema).eval()

  def test_schema_arg_update_schema(self):
    schema = bag().new_schema(a=schema_constants.FLOAT32)
    x = kde.core.new(a=42, b='xyz', schema=schema, update_schema=True).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_any(self):
    x = kde.core.new(a=1, b='a', schema=schema_constants.ANY).eval()
    self.assertEqual(x.get_attr_names(intersection=True), [])
    testing.assert_equal(x.get_schema().no_bag(), schema_constants.ANY)
    testing.assert_equal(x.a, ds(1).as_any().with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('a').as_any().with_bag(x.get_bag()))

  def test_schema_contains_any(self):
    schema = bag().new_schema(x=schema_constants.ANY)
    entity = kde.core.new().eval()
    x = kde.core.new(x=entity, schema=schema).eval()
    testing.assert_equal(x.x.no_bag(), entity.no_bag().as_any())

  def test_schema_arg_embed_schema(self):
    schema = bag().new_schema(a=schema_constants.OBJECT)
    x = kde.core.new(a=kde.core.new(p=42, q='xyz'), schema=schema).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a'])
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        x.a.get_attr('__schema__').p.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        x.a.get_attr('__schema__').q.no_bag(), schema_constants.STRING
    )

  def test_str_as_schema_arg(self):
    x = kde.core.new(schema='name', a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    x = kde.core.new(schema=ds('name'), a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'schema must be SCHEMA, got: INT32'
    ):
      kde.core.new(a=1, schema=ds([1, 2, 3])).eval()
    with self.assertRaisesRegex(
        ValueError, 'schema must be SCHEMA, got: STRING'
    ):
      kde.core.new(a=1, schema=ds(['name'])).eval()
    with self.assertRaisesRegex(ValueError, 'schema can only be 0-rank'):
      kde.core.new(
          a=1, schema=ds([schema_constants.INT32, schema_constants.STRING])
      ).eval()
    with self.assertRaisesRegex(
        ValueError, 'requires Entity schema, got INT32'
    ):
      kde.core.new(a=1, schema=schema_constants.INT32).eval()
    with self.assertRaisesRegex(
        ValueError, 'requires Entity schema, got OBJECT'
    ):
      kde.core.new(a=1, schema=schema_constants.OBJECT).eval()

  def test_converter_not_supported(self):
    with self.assertRaisesRegex(ValueError, 'please use eager only kd.kdi.new'):
      _ = kde.core.new(ds(42)).eval()

  def test_non_determinism(self):
    schema = bag().new_schema(a=schema_constants.INT32)
    res_1 = expr_eval.eval(kde.core.new(schema=schema, a=I.a), a=42)
    res_2 = expr_eval.eval(kde.core.new(schema=schema, a=I.a), a=42)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

    expr = kde.core.new(schema=schema, a=42)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.new,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_namedtuple_qtype(),
            arolla.make_namedtuple_qtype(a=DATA_SLICE),
            arolla.make_namedtuple_qtype(a=DATA_SLICE, b=DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.new(I.x)))
    self.assertTrue(view.has_koda_view(kde.core.new(itemid=I.itemid, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.new, kde.new))

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.new(a=I.y)),
        'kde.core.new(unspecified, schema=unspecified, '
        'update_schema=DataItem(False, schema: BOOLEAN), itemid=unspecified, '
        'a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
