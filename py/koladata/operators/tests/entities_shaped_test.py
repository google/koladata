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

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


def generate_qtypes():
  for schema_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
    for itemid_arg_type in [DATA_SLICE, arolla.UNSPECIFIED]:
      for attrs_type in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES:
        yield JAGGED_SHAPE, schema_arg_type, DATA_SLICE, itemid_arg_type, attrs_type, NON_DETERMINISTIC_TOKEN, DATA_SLICE


QTYPES = list(generate_qtypes())


class EntitiesShapedTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape = jagged_shape.create_shape(2, 3)
    x = kde.entities.shaped(shape).eval()
    testing.assert_equal(x.get_shape(), shape)
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape = jagged_shape.create_shape()
    x = kde.entities.shaped(shape).eval()
    self.assertIsNotNone(x.get_bag())
    testing.assert_equal(x.get_shape(), shape)
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape = jagged_shape.create_shape(2, 3)
    x = kde.entities.shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6])).eval()
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(x.x.no_bag(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_bag(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_bag(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(
        x.c[:].no_bag(),
        ds([[[5, 6], [5, 6], [5, 6]], [[5, 6], [5, 6], [5, 6]]]),
    )
    self.assertFalse(x.is_mutable())

  def test_schema_arg_simple(self):
    shape = jagged_shape.create_shape(2, 3)
    schema = kde.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = kde.entities.shaped(shape, schema=schema).eval()
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_deep(self):
    nested_schema = kde.schema.new_schema(p=schema_constants.BYTES)
    schema = kde.schema.new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
        nested=nested_schema,
    )
    x = kde.entities.shaped(
        jagged_shape.create_shape(),
        a=42,
        b='xyz',
        nested=kde.new_shaped(
            jagged_shape.create_shape(), p=b'0123', schema=nested_schema
        ),
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

  def test_schema_arg_implicit_casting(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32)
    x = kde.entities.shaped(
        jagged_shape.create_shape([2]), a=42, schema=schema
    ).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a'])
    testing.assert_equal(
        x.a, ds([42, 42], schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        "kd.entities.shaped: the schema for attribute 'a' is incompatible",
    ):
      kde.entities.shaped(
          jagged_shape.create_shape([2]), a='xyz', schema=schema
      ).eval()

  def test_str_as_schema_arg(self):
    shape = jagged_shape.create_shape([2])
    x = kde.entities.shaped(shape, schema='name', a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    shape = jagged_shape.create_shape([2])
    x = kde.entities.shaped(shape, schema=ds('name'), a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    shape = jagged_shape.create_shape([2])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      _ = kde.entities.shaped(shape, schema=ds(['name']), a=42).eval()
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      _ = kde.entities.shaped(shape, schema=42, a=42).eval()

  def test_schema_arg_overwrite_schema(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32)
    x = kde.entities.shaped(
        jagged_shape.create_shape([2]),
        a=42,
        b='xyz',
        schema=schema,
        overwrite_schema=True,
    ).eval()
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_overwrite_schema_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got an '
        'item of INT32',
    ):
      kde.entities.shaped(
          jagged_shape.create_shape(),
          schema=schema_constants.INT32,
          overwrite_schema=42,
      ).eval()

  def test_schema_arg_overwrite_schema_overwriting(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32)
    x = kde.entities.shaped(
        jagged_shape.create_shape(),
        a='xyz',
        schema=schema,
        overwrite_schema=True,
    ).eval()
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    )
    x = kde.entities.shaped(itemid.get_shape(), a=42, itemid=itemid).eval()
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).get_itemid()
    assert itemid.get_bag() is not None
    x = kde.entities.shaped(itemid.get_shape(), a=42, itemid=itemid).eval()
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = x.non_existent

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError, "missing 1 required positional argument: 'shape'"
    ):
      _ = kde.entities.shaped().eval()

  def test_fails_with_dataslice_input(self):
    with self.assertRaisesRegex(ValueError, 'expected JAGGED_SHAPE'):
      _ = kde.entities.shaped(ds(0)).eval()

  def test_non_determinism(self):
    shape = jagged_shape.create_shape(2, 3)
    res_1 = expr_eval.eval(
        kde.entities.shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    res_2 = expr_eval.eval(
        kde.entities.shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

    expr = kde.entities.shaped(shape, x=2, a=1, b='p', c=fns.list([5, 6]))
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.entities.shaped,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.entities.shaped(I.x)))
    self.assertTrue(view.has_koda_view(kde.entities.shaped(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.entities.shaped, kde.new_shaped))

  def test_repr(self):
    self.assertEqual(
        repr(kde.entities.shaped(I.x, a=I.y)),
        'kd.entities.shaped(I.x, schema=unspecified, overwrite_schema=DataItem'
        '(False, schema: BOOLEAN), itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
