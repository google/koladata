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

"""Tests for kd."""

import inspect
import types

from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.expr import tracing_mode
from koladata.functor import signature_utils
from koladata.types import jagged_shape
from koladata.types import schema_constants

kde = kd.kde
kdi = kd.kdi
I = kd.I
V = kd.V
S = kd.S
kdf = kd.kdf


class KdTest(absltest.TestCase):

  def test_types(self):
    self.assertIsInstance(kd.types, types.ModuleType)
    self.assertIsInstance(kd.bag(), kd.types.DataBag)
    self.assertIsInstance(kd.slice([1, 2, 3]), kd.types.DataSlice)
    self.assertIsInstance(kd.item(5), kd.types.DataItem)
    self.assertIsInstance(kd.bag().list([1, 2]), kd.types.ListItem)
    self.assertIsInstance(kd.bag().dict({'a': 42}), kd.types.DictItem)
    self.assertIsInstance(kd.INT32, kd.types.SchemaItem)

  def test_type_annotations(self):
    def f(
        bag: kd.types.DataBag, item: kd.types.DataItem, sl: kd.types.DataSlice  # pylint: disable=unused-argument
    ):
      pass

    sig = inspect.signature(f)
    self.assertIs(sig.parameters['bag'].annotation, kd.types.DataBag)
    self.assertIs(sig.parameters['item'].annotation, kd.types.DataItem)
    self.assertIs(sig.parameters['sl'].annotation, kd.types.DataSlice)

  def test_bag_returns_new_instance(self):
    db1 = kd.bag()
    db2 = kd.bag()
    kd.testing.assert_equivalent(db1, db2)
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(db1, db2)

  def test_data_slice_and_data_item_magic_methods(self):
    kd.testing.assert_equal(
        kd.slice([1, 2, 3]) + kd.slice([4, 5, 6]), kd.slice([5, 7, 9])
    )
    kd.testing.assert_equal(kd.item(1) + kd.item(4), kd.item(5))

  def test_schema_constants(self):
    for const in dir(schema_constants):
      if isinstance(getattr(schema_constants, const), arolla.QValue):
        kd.testing.assert_equal(
            getattr(schema_constants, const), getattr(kd, const)
        )

  def test_mask_constants(self):
    self.assertEqual(kd.present.get_schema(), kd.MASK)
    self.assertEqual(kd.missing.get_schema(), kd.MASK)
    self.assertEqual(kd.has(kd.item(1)), kd.present)
    # NOTE: `==` on missing items returns missing and bool(missing) is False.
    kd.testing.assert_equal(kd.has(kd.item(None)), kd.missing)

  def test_ops(self):
    kd.testing.assert_equal(
        kd.add(kd.slice([1, 2]), kd.slice([3, 4])), kd.slice([4, 6])
    )
    kd.testing.assert_equal(
        kd.has(kd.slice([1, None])), kd.slice([arolla.present(), None])
    )
    kd.testing.assert_equal(
        kd.shapes.create(kd.slice([1])), jagged_shape.create_shape([1])
    )
    kd.testing.assert_equal(
        kd.strings.agg_join(kd.slice(['ab', 'd'])), kd.item('abd')
    )

  def test_entities(self):
    x = kd.new(a=1, b='abc')
    y = kd.new(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema().a, kd.INT32.with_db(x.db))
    kd.testing.assert_equal(x.get_schema().b, kd.TEXT.with_db(x.db))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.no_db(), y.a.no_db())

  def test_objects(self):
    x = kd.obj(a=1, b='abc')
    y = kd.obj(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema(), kd.OBJECT.with_db(x.db))
    kd.testing.assert_equal(y.get_schema(), kd.OBJECT.with_db(y.db))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.no_db(), y.a.no_db())

  def test_mutable_obj(self):
    x = kd.mutable_obj(x=1, y=2)
    kd.testing.assert_equal(x.x, kd.item(1).with_db(x.db))
    kd.testing.assert_equal(x.y, kd.item(2).with_db(x.db))
    x.x = 3
    kd.testing.assert_equal(x.x, kd.item(3).with_db(x.db))

  def test_mutable_obj_like(self):
    x = kd.mutable_obj_like(kd.slice([1, None, 2]))
    x.x = 1
    kd.testing.assert_equal(x.x, kd.slice([1, None, 1]).with_db(x.db))

  def test_mutable_obj_shaped(self):
    x = kd.mutable_obj_shaped(jagged_shape.create_shape(2, [1, 2]))
    x.x = 1
    kd.testing.assert_equal(x.x, kd.slice([[1], [1, 1]]).with_db(x.db))

  def test_expr(self):
    kd.testing.assert_equal(
        kd.eval(
            kde.add(S.x, I.y), kd.new(x=kd.slice([1, 2])), y=kd.slice([3, 4])
        ),
        kd.slice([4, 6]),
    )

  def test_literal(self):
    expr = kd.literal(kd.item(1))
    self.assertIsInstance(expr, arolla.Expr)
    kd.testing.assert_equal(arolla.eval(expr), kd.item(1))

  def test_dir(self):
    for api_name in dir(kd):
      self.assertFalse(api_name.startswith('_'))

  def test_docstring(self):
    self.assertIn('Koda API', kd.__doc__)

  def test_exception_type(self):
    self.assertIsInstance(kd.exceptions, types.ModuleType)

    def f(e: kd.exceptions.KodaError):  # pylint: disable=unused-argument
      pass

    sig = inspect.signature(f)
    self.assertIs(sig.parameters['e'].annotation, kd.exceptions.KodaError)

  def test_kdf(self):
    fn = kdf.fn(
        returns=I.x + V.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
        foo=I.y,
    )
    self.assertEqual(kdf.call(fn, 1, 2), 3)

  def test_with_name(self):
    x = kd.slice([1, 2, 3])
    y = kd.with_name(x, 'foo')
    self.assertIs(y, x)

  def test_get_name(self):
    expr = kde.with_name(I.x + I.y, 'foo')
    self.assertEqual(kd.get_name(expr), 'foo')
    self.assertIsNone(kd.get_name(expr + I.z))

  def test_unwrap_named(self):
    expr = kde.with_name(I.x + I.y, 'foo')
    kd.testing.assert_equal(kd.unwrap_named(expr), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'non-named'):
      _ = kd.unwrap_named(expr + I.z)

  def test_pack_unpack_expr(self):
    kd.testing.assert_equal(kd.unpack_expr(kd.pack_expr(I.x + I.y)), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      kd.unpack_expr(kd.item(1))

  def test_is_packed_expr(self):
    ds = kd.pack_expr(I.x + I.y)
    kd.testing.assert_equal(kd.is_packed_expr(ds), kd.present)
    kd.testing.assert_equal(kd.is_packed_expr(kd.slice(1)), kd.missing)
    kd.testing.assert_equal(kd.is_packed_expr(I.x + I.y), kd.missing)

  def test_as_expr(self):
    kd.testing.assert_equal(kd.as_expr(1), kd.literal(kd.slice(1)))
    kd.testing.assert_equal(kd.as_expr(I.x), I.x)

  def test_fstr(self):
    kd.testing.assert_equal(kd.fstr(f'{kd.slice(1):s}'), kd.slice('1'))

  def test_fstr_expr_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'contains expression.*eager kd.fstr call'
    ):
      kd.fstr(f'{kd.literal(kd.slice(1)):s}')

  def test_get_input_names(self):
    expr = I.x + I.y + V.z
    self.assertEqual(kd.get_input_names(expr), ['x', 'y'])
    self.assertEqual(kd.get_input_names(expr, container=V), ['z'])

  def test_sub_inputs(self):
    expr = I.x + I.y + V.x
    kd.testing.assert_equal(kd.sub_inputs(expr, x=I.w), I.w + I.y + V.x)
    kd.testing.assert_equal(kd.sub_inputs(expr, V, x=I.w), I.x + I.y + I.w)

  def test_sub_by_name(self):
    foo = kde.with_name(I.x, 'foo')
    bar = kde.with_name(I.y, 'bar')
    expr = foo + bar
    kd.testing.assert_equal(kd.sub_by_name(expr, foo=I.z, baz=I.w), I.z + bar)

  def test_sub(self):
    expr = I.x + I.y
    kd.testing.assert_equal(kd.sub(expr, I.x, I.z), I.z + I.y)
    kd.testing.assert_equal(kd.sub(expr, (I.x, I.z)), I.z + I.y)

  def test_kdi(self):
    self.assertCountEqual(kdi.__all__, dir(kdi))
    self.assertCountEqual(set(dir(kd)) - set(dir(kdi)), ['kdi'])
    self.assertCountEqual(set(dir(kdi)) - set(dir(kd)), [])
    for name in kdi.__all__:
      self.assertIs(getattr(kdi, name), getattr(kd, name))

  def test_missing_attribute_error_message(self):
    with self.assertRaisesRegex(
        AttributeError,
        "'koladata.kd' object has no attribute 'nonexisting_method'",
    ):
      _ = kd.nonexisting_method
    with self.assertRaisesRegex(
        AttributeError,
        "'koladata.kd' object has no attribute 'nonexisting_method'",
    ):
      with tracing_mode.enable_tracing():
        _ = kd.nonexisting_method

  def test_unavailable_in_tracing_error_message(self):
    with tracing_mode.enable_tracing():
      with self.assertRaisesRegex(
          AttributeError,
          "Attribute 'eval' is not available in tracing mode on 'koladata.kd'",
      ):
        _ = kd.eval

  def test_tracing_for_ops(self):
    with tracing_mode.enable_tracing():
      abs_op = kd.abs
    kd.testing.assert_equal(abs_op, kde.abs)
    with tracing_mode.enable_tracing():
      math_abs_op = kd.math.abs
    kd.testing.assert_equal(math_abs_op, kde.math.abs)

  def test_tracing_for_functions(self):
    with tracing_mode.enable_tracing():
      with self.assertRaisesRegex(
          AttributeError,
          "Attribute 'new' is not available in tracing mode on 'koladata.kd'",
      ):
        _ = kd.new

  def test_tracing_for_with_name(self):
    with tracing_mode.enable_tracing():
      with_name_expr = kd.with_name(1, 'foo')
    kd.testing.assert_equal(with_name_expr, kde.with_name(1, 'foo'))

  def test_tracing_for_constants(self):
    with tracing_mode.enable_tracing():
      int32_val = kd.INT32
      present_val = kd.present
    self.assertIs(int32_val, kd.INT32)
    self.assertIs(present_val, kd.present)

  def test_trace_as_fn(self):
    @kd.trace_as_fn()
    def f(x):
      return x + 1

    def g(x):
      return f(x) + 2

    fn = kdf.trace_py_fn(g)
    kd.testing.assert_equal(kd.unpack_expr(fn.returns), V.f(I.x) + 2)
    kd.testing.assert_equal(kd.unpack_expr(fn.f.returns), I.x + 1)


if __name__ == '__main__':
  absltest.main()
