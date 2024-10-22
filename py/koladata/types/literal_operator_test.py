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

"""Tests for literal_operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


kd = eager_op_utils.operators_container('kde')
ds = data_slice.DataSlice.from_vals


class LiteralOperatorTest(parameterized.TestCase):

  def test_type(self):
    expr = literal_operator.literal(ds([1, 2, 3]))
    self.assertIsInstance(expr, arolla.Expr)
    self.assertIsInstance(expr.op, literal_operator.LiteralOperator)

  def test_name(self):
    x = ds([1, 2, 3])
    expr = literal_operator.literal(x)
    self.assertEqual(repr(expr), repr(x))

  def test_repr(self):
    x = ds([1, 2, 3])
    expr = literal_operator.literal(x)
    self.assertEqual(repr(expr), repr(x))

  def test_fingerprint(self):
    x = ds([1, 2, 3])
    expr1 = literal_operator.literal(x)
    expr2 = literal_operator.literal(x)
    self.assertEqual(expr1.fingerprint, expr2.fingerprint)

    y = ds([3, 2, 1])
    expr3 = literal_operator.literal(y)
    self.assertNotEqual(expr1.fingerprint, expr3.fingerprint)

  def test_lowering(self):
    expr = literal_operator.literal(ds([1, 2, 3]))
    testing.assert_equal(arolla.abc.to_lowest(expr), expr)

  def test_eval(self):
    x = ds([1, 2, 3])
    expr = literal_operator.literal(x)
    testing.assert_equal(arolla.eval(expr), x)

  def test_qtype(self):
    self.assertEqual(
        literal_operator.literal(ds([1, 2, 3])).qtype, qtypes.DATA_SLICE
    )
    self.assertEqual(
        literal_operator.literal(data_bag.DataBag.empty()).qtype,
        qtypes.DATA_BAG,
    )

  def test_qvalue(self):
    x = ds([1, 2, 3])
    expr = literal_operator.literal(x)
    arolla.testing.assert_qvalue_equal_by_fingerprint(expr.qvalue, x)

  def test_koda_boxing(self):
    l1 = data_bag.DataBag.empty().list(['a', 'b'])
    l2 = data_bag.DataBag.empty().list(['x', 'y'])
    literal_value = ds([l1, l2])
    expr = literal_operator.literal(literal_value)
    testing.assert_equal(
        expr.qvalue[:],
        ds([['a', 'b'], ['x', 'y']]).with_bag(literal_value.get_bag()),
    )

  def test_qvalue_passthrough(self):
    expr = literal_operator.literal(arolla.int32(1))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        expr.qvalue, arolla.int32(1)
    )

  def test_doc(self):
    self.assertEqual(
        literal_operator.literal.__doc__,
        'Constructs an expr with a LiteralOperator wrapping the provided'
        ' QValue.',
    )

  def test_view(self):
    # Arbitrary literal.
    x = literal_operator.literal(arolla.int32(1))
    self.assertTrue(view.has_basic_koda_view(x))

    # DataSlice.
    x = literal_operator.literal(ds(1))
    self.assertTrue(view.has_data_slice_view(x))

    # DataBag.
    x = literal_operator.literal(data_bag.DataBag.empty())
    self.assertTrue(view.has_data_bag_view(x))

  @parameterized.parameters(
      (arolla.L.x,),
      (1,),
      ([1, 2, 3],),
      (slice(0, 5),),
      (...,),
  )
  def test_literal_expr_non_qvalue_error(self, non_qvalue):
    with self.assertRaisesRegex(
        TypeError,
        '`value` must be a QValue to be wrapped into a LiteralOperator,'
        rf' got: .*{type(non_qvalue).__name__}',
    ):
      literal_operator.literal(non_qvalue)

  def test_literal_operator_init(self):
    op = literal_operator.LiteralOperator(arolla.int32(1))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(op()), arolla.int32(1)
    )

  @parameterized.parameters(
      (arolla.L.x,),
      (1,),
      ([1, 2, 3],),
      (slice(0, 5),),
      (...,),
  )
  def test_literal_operator_non_qvalue_error(self, non_qvalue):
    with self.assertRaisesRegex(
        TypeError,
        '`value` must be a QValue to be wrapped into a LiteralOperator,'
        rf' got: .*{type(non_qvalue).__name__}',
    ):
      literal_operator.LiteralOperator(non_qvalue)  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
