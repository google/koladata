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
from koladata import kd
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


class EntitiesShapedAsTest(absltest.TestCase):

  def test_slice_no_attrs(self):
    shape_from = ds([6, 7, 8])
    x = kde.entities.shaped_as(shape_from).eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_item_no_attrs(self):
    shape_from = ds(0)
    x = kde.entities.shaped_as(shape_from).eval()
    self.assertIsNotNone(x.get_bag())
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    self.assertFalse(x.is_mutable())

  def test_with_attrs(self):
    shape_from = ds([[6, 7, 8], [6, 7, 8]])
    x = kde.entities.shaped_as(
        shape_from, x=2, a=1, b='p', c=fns.list([5, 6])
    ).eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(x.x.no_bag(), ds([[2, 2, 2], [2, 2, 2]]))
    testing.assert_equal(x.a.no_bag(), ds([[1, 1, 1], [1, 1, 1]]))
    testing.assert_equal(x.b.no_bag(), ds([['p', 'p', 'p'], ['p', 'p', 'p']]))
    testing.assert_equal(
        x.c[:].no_bag(),
        ds([[[5, 6], [5, 6], [5, 6]], [[5, 6], [5, 6], [5, 6]]]),
    )
    self.assertFalse(x.is_mutable())

  def test_no_sparsity_item_with_empty_attr(self):
    x = kde.entities.shaped_as(ds(None), a=42).eval()
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds(kd.present, schema_constants.MASK)
    )

  def test_no_sparsity_all_empty_slice(self):
    shape_from = ds([None, None])
    x = kde.entities.shaped_as(shape_from, a=42).eval()
    testing.assert_equal(
        kde.has(x).eval().no_bag(),
        ds([kd.present, kd.present], schema_constants.MASK),
    )
    testing.assert_equal(
        x.a, ds([42, 42], schema_constants.INT32).with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = kde.entities.shaped_as(ds(1), a='abc').eval()
    y = kde.entities.shaped_as(x, x=x).eval()
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.STRING)

  def test_schema_arg(self):
    shape_from = ds([])
    schema = kde.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = kde.entities.shaped_as(shape_from, schema=schema).eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_str_as_schema_arg(self):
    shape_from = ds([[6, 7], [8]])
    x = kde.entities.shaped_as(shape_from, schema='name', a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    shape_from = ds([[6, 7], [8]])
    x = kde.entities.shaped_as(shape_from, schema=ds('name'), a=42).eval()
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape_from.get_shape())
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    shape_from = ds([[6, 7], [8]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      _ = kde.entities.shaped_as(shape_from, schema=ds(['name']), a=42).eval()
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      _ = kde.entities.shaped_as(shape_from, schema=42, a=42).eval()

  def test_itemid_arg(self):
    shape_from = ds([[6, 7], [8]])
    itemid = expr_eval.eval(kde.allocation.new_itemid_shaped_as(shape_from))
    x = kde.entities.shaped_as(itemid, a=42, itemid=itemid).eval()
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_overwrite_schema_arg(self):
    shape_from = ds([6, 7])
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32)
    x = kde.entities.shaped_as(
        shape_from,
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

  def test_overwrite_schema_arg_error(self):
    shape_from = ds([6, 7])
    with self.assertRaisesRegex(
        ValueError,
        'argument `overwrite_schema` must be an item holding BOOLEAN, got an '
        'item of INT32',
    ):
      kde.entities.shaped_as(
          shape_from,
          schema=schema_constants.INT32,
          overwrite_schema=42,
      ).eval()

  def test_fails_without_shape(self):
    with self.assertRaisesRegex(
        TypeError, "missing 1 required positional argument: 'shape_from'"
    ):
      _ = kde.entities.shaped_as().eval()

  def test_fails_with_shape_input(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_SLICE'):
      _ = kde.entities.shaped_as(ds(0).get_shape()).eval()

  def test_non_determinism(self):
    shape_from = ds([[6, 7, 8], [6, 7, 8]])
    res_1 = expr_eval.eval(
        kde.entities.shaped_as(shape_from, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    res_2 = expr_eval.eval(
        kde.entities.shaped_as(shape_from, x=2, a=1, b='p', c=fns.list([5, 6]))
    )
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1.a.no_bag(), res_2.a.no_bag())

    expr = kde.entities.shaped_as(
        shape_from, x=2, a=1, b='p', c=fns.list([5, 6])
    )
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
    self.assertTrue(view.has_koda_view(kde.entities.shaped_as(I.x)))
    self.assertTrue(view.has_koda_view(kde.entities.shaped_as(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.entities.shaped_as, kde.new_shaped_as)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.entities.shaped_as(I.x, a=I.y)),
        'kd.entities.shaped_as(I.x, schema=unspecified, '
        'overwrite_schema=DataItem(False, schema: BOOLEAN), '
        'itemid=unspecified, a=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
