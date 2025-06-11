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
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import schema_constants

kde = kde_operators.kde
I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
kd = eager_op_utils.operators_container('kd')

ds = data_slice.DataSlice.from_vals


class IntrospectionTest(absltest.TestCase):

  def test_get_name(self):
    self.assertEqual(
        introspection.get_name(kde.with_name(I.x + I.y, 'foo')), 'foo'
    )
    self.assertEqual(
        introspection.get_name(kde.annotation.with_name(I.x + I.y, 'foo')),
        'foo',
    )
    self.assertIsNone(
        introspection.get_name(kde.with_name(I.x + I.y, 'foo') + I.z)
    )
    self.assertIsNone(introspection.get_name(py_boxing.as_expr(1)))

  def test_unwrap_named(self):
    testing.assert_equal(
        introspection.unwrap_named(kde.with_name(I.x + I.y, 'foo')), I.x + I.y
    )
    testing.assert_equal(
        introspection.unwrap_named(kde.annotation.with_name(I.x + I.y, 'foo')),
        I.x + I.y,
    )
    with self.assertRaisesRegex(ValueError, 'non-named'):
      introspection.unwrap_named(kde.with_name(I.x + I.y, 'foo') + I.z)

  def test_pack_expr(self):
    x = introspection.pack_expr(I.x + I.y)
    self.assertEqual(x.get_schema(), schema_constants.EXPR)
    self.assertEqual(x.get_ndim(), 0)
    testing.assert_equal(introspection.unpack_expr(x), I.x + I.y)

  def test_unpack_expr(self):
    x = introspection.pack_expr(I.x + I.y)
    testing.assert_equal(introspection.unpack_expr(x), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(x & mask_constants.missing)
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(x.with_schema(schema_constants.OBJECT))
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      introspection.unpack_expr(x.repeat(1))

  def test_is_packed_expr(self):
    x = introspection.pack_expr(I.x + I.y)
    testing.assert_equal(
        introspection.is_packed_expr(x), mask_constants.present
    )
    testing.assert_equal(
        introspection.is_packed_expr(x & mask_constants.missing),
        mask_constants.missing,
    )
    testing.assert_equal(
        introspection.is_packed_expr(x.with_schema(schema_constants.OBJECT)),
        mask_constants.missing,
    )
    testing.assert_equal(
        introspection.is_packed_expr(x.repeat(1)), mask_constants.missing
    )
    testing.assert_equal(
        introspection.is_packed_expr(I.x + I.y), mask_constants.missing
    )

  def test_get_input_names(self):
    decayed_input_op = arolla.abc.decay_registered_operator(
        'koda_internal.input'
    )
    expr = I.x + I.y + I.z.val + V.w + decayed_input_op('V', 'v')
    self.assertEqual(introspection.get_input_names(expr), ['x', 'y', 'z'])
    self.assertEqual(introspection.get_input_names(expr, I), ['x', 'y', 'z'])
    # Note: decayed inputs are _not_ supported.
    self.assertEqual(introspection.get_input_names(expr, V), ['w'])

  def test_is_input(self):
    self.assertTrue(introspection.is_input(I.x))
    self.assertTrue(introspection.is_input(I.y))
    self.assertFalse(introspection.is_input(V.a))
    self.assertFalse(introspection.is_input(literal_operator.literal(ds(42))))
    self.assertFalse(introspection.is_input(I.x + I.y))

  def test_is_variable(self):
    self.assertTrue(introspection.is_variable(V.x))
    self.assertTrue(introspection.is_variable(V.y))
    self.assertFalse(introspection.is_variable(I.a))
    self.assertFalse(
        introspection.is_variable(literal_operator.literal(ds(42)))
    )
    self.assertFalse(introspection.is_variable(V.x + V.y))

  def test_is_literal(self):
    expr = literal_operator.literal(ds(42))
    self.assertTrue(introspection.is_literal(expr))
    self.assertFalse(introspection.is_literal(arolla.L.x))
    self.assertFalse(introspection.is_literal(arolla.L.x + 1))

  def test_sub_inputs(self):
    decayed_input_op = arolla.abc.decay_registered_operator(
        'koda_internal.input'
    )
    expr = I.x + I.y + I.z.val + V.x + decayed_input_op('V', 'v')
    testing.assert_equal(
        introspection.sub_inputs(expr, x=I.a, z=I.o),
        I.a + I.y + I.o.val + V.x + decayed_input_op('V', 'v'),
    )
    testing.assert_equal(
        introspection.sub_inputs(expr, I, x=I.a, z=I.o),
        I.a + I.y + I.o.val + V.x + decayed_input_op('V', 'v'),
    )
    # Note: decayed inputs are _not_ supported.
    testing.assert_equal(
        introspection.sub_inputs(expr, V, x=I.a, y=I.o, v=I.z),
        I.x + I.y + I.z.val + I.a + decayed_input_op('V', 'v'),
    )
    testing.assert_equal(
        introspection.sub_inputs(expr, x=1),
        ds(1) + I.y + I.z.val + V.x + decayed_input_op('V', 'v'),
    )
    with self.assertRaisesRegex(ValueError, 'unsupported type'):
      introspection.sub_inputs(expr, x={'x': 1})

  def test_sub_inputs_non_identifier(self):
    expr = I['123']
    testing.assert_equal(introspection.sub_inputs(expr, **{'123': I.x}), I.x)

  def test_get_input_names_in_lambda(self):
    @arolla.optools.as_lambda_operator('foo.bar')
    def foo_bar():
      return I.x

    self.assertEmpty(introspection.get_input_names(foo_bar(), I))
    self.assertEqual(
        introspection.get_input_names(arolla.abc.to_lowest(foo_bar()), I),
        ['x'],
    )

  def test_sub_inputs_in_lambda(self):
    @arolla.optools.as_lambda_operator('foo.bar')
    def foo_bar():
      return I.x

    testing.assert_equal(
        introspection.sub_inputs(foo_bar(), I, x=arolla.L.x), foo_bar()
    )
    testing.assert_equal(
        introspection.sub_inputs(
            arolla.abc.to_lowest(foo_bar()), I, x=arolla.L.x
        ),
        arolla.L.x,
    )

  def test_sub_by_name(self):
    foo = kde.with_name(I.x, 'foo')
    bar = kde.with_name(I.y, 'bar')
    expr = foo + bar
    testing.assert_equal(
        introspection.sub_by_name(expr, foo=I.z, baz=I.w), I.z + bar
    )
    testing.assert_equal(introspection.sub_by_name(expr, foo=1), ds(1) + bar)
    with self.assertRaisesRegex(ValueError, 'unsupported type'):
      introspection.sub_by_name(expr, foo={'x': 1})

  def test_sub(self):
    expr = I.x + I.y - V.z
    testing.assert_equal(introspection.sub(expr, I.x, I.z), I.z + I.y - V.z)
    testing.assert_equal(introspection.sub(expr, I.x + I.y, I.z), I.z - V.z)
    testing.assert_equal(
        introspection.sub(expr, (I.x, I.z), (V.z, V.w)), I.z + I.y - V.w
    )
    testing.assert_equal(
        introspection.sub(expr, (I.x, I.a), (I.y, I.b), (V.z, V.c)),
        I.a + I.b - V.c,
    )
    # No deep substitution.
    testing.assert_equal(
        introspection.sub(expr, (I.x, I.z), (I.z, I.w)), I.z + I.y - V.z
    )
    # boxing.
    expr = I.x + ds(1)
    testing.assert_equal(
        introspection.sub(expr, I.x, 2),
        literal_operator.literal(ds(2)) + literal_operator.literal(ds(1)),
    )
    testing.assert_equal(
        introspection.sub(expr, (I.x, 2)),
        literal_operator.literal(ds(2)) + literal_operator.literal(ds(1)),
    )

  def test_sub_errors(self):
    msg = (
        'either all subs must be two-element tuples of Expressions, or there'
        ' must be exactly two non-tuple subs representing a single substitution'
    )
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(I.x + I.y, I.x, I.y, I.z, I.t)
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(I.x + I.y, I.x)
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(ds(1) + I.x, 1, 2)
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(ds(1) + I.x, (1, 2))
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(I.x + I.y, ((I.x, I.y), (I.z, I.t)))
    with self.assertRaisesRegex(ValueError, msg):
      introspection.sub(I.x + I.y, [I.x, I.y], [I.z, I.t])
    with self.assertRaisesRegex(
        ValueError,
        'passing a Python list to a Koda operation is ambiguous',
    ):
      introspection.sub(I.x + I.y, I.x, [I.y, I.z])


if __name__ == '__main__':
  absltest.main()
