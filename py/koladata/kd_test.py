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

import dataclasses
import inspect
import re
import types
from typing import Any, ClassVar

from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.expr import tracing_mode
from koladata.functions import functions
from koladata.types import jagged_shape
from koladata.types import schema_constants
from koladata.types import signature_utils

kdi = kd.eager
I = kd.I
V = kd.V
S = kd.S
kdf = kd.functor


class TuplePairTracingConfig(kd.functor.TypeTracingConfig):
  """A type tracing config for Pair using tuples."""

  def return_type_as(self, annotation: type['PairWithTupleTracing']) -> Any:
    return kd.types.DataSlice, kd.types.DataSlice

  def to_kd(
      self,
      annotation: type['PairWithTupleTracing'],
      value: 'PairWithTupleTracing',
  ) -> Any:
    return value.x, value.y

  def from_kd(
      self, annotation: type['PairWithTupleTracing'], value: Any
  ) -> 'PairWithTupleTracing':
    return annotation(x=value[0], y=value[1])


@dataclasses.dataclass(frozen=True)
class PairWithTupleTracing:
  x: kd.types.DataSlice | kd.types.Expr
  y: kd.types.DataSlice | kd.types.Expr

  _koladata_type_tracing_config_: ClassVar[type[TuplePairTracingConfig]] = (
      TuplePairTracingConfig
  )


class KdTest(absltest.TestCase):

  def test_types(self):
    self.assertIsInstance(kd.types, types.ModuleType)
    self.assertIsInstance(kd.bag(), kd.types.DataBag)
    self.assertIsInstance(kd.slice([1, 2, 3]), kd.types.DataSlice)
    self.assertIsInstance(kd.item(5), kd.types.DataItem)
    self.assertIsInstance(kd.bag().list([1, 2]), kd.types.ListItem)
    self.assertIsInstance(kd.bag().dict({'a': 42}), kd.types.DictItem)
    self.assertIsInstance(kd.INT32, kd.types.SchemaItem)
    self.assertIsInstance(I.x, kd.types.Expr)
    self.assertIsInstance(I.x + I.y, kd.types.Expr)

  def test_type_annotations(self):

    def f(
        bag: kd.types.DataBag,
        item: kd.types.DataItem,
        sl: kd.types.DataSlice,
        ex: kd.types.Expr,
    ):
      del bag, item, sl, ex

    sig = inspect.signature(f)
    self.assertIs(sig.parameters['bag'].annotation, kd.types.DataBag)
    self.assertIs(sig.parameters['item'].annotation, kd.types.DataItem)
    self.assertIs(sig.parameters['sl'].annotation, kd.types.DataSlice)
    self.assertIs(sig.parameters['ex'].annotation, kd.types.Expr)

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
        kd.shapes.new(kd.slice([1])), jagged_shape.create_shape([1])
    )
    kd.testing.assert_equal(
        kd.strings.agg_join(kd.slice(['ab', 'd'])), kd.item('abd')
    )

  def test_entities(self):
    x = kd.new(a=1, b='abc')
    y = kd.new(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema().a, kd.INT32.with_bag(x.get_bag()))
    kd.testing.assert_equal(x.get_schema().b, kd.STRING.with_bag(x.get_bag()))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.no_bag(), y.a.no_bag())

  def test_objects(self):
    x = kd.obj(a=1, b='abc')
    y = kd.obj(a=1, b='abc')
    kd.testing.assert_equal(x.get_schema(), kd.OBJECT.with_bag(x.get_bag()))
    kd.testing.assert_equal(y.get_schema(), kd.OBJECT.with_bag(y.get_bag()))
    with self.assertRaises(AssertionError):
      kd.testing.assert_equal(x, y)
    kd.testing.assert_equal(x.a.no_bag(), y.a.no_bag())

  def test_container(self):
    x = kd.container(x=1, y=2)
    kd.testing.assert_equal(x.x, kd.item(1).with_bag(x.get_bag()))
    kd.testing.assert_equal(x.y, kd.item(2).with_bag(x.get_bag()))
    x.x = 3
    kd.testing.assert_equal(x.x, kd.item(3).with_bag(x.get_bag()))

  def test_expr(self):
    kd.testing.assert_equal(
        kd.eval(
            kd.lazy.add(S.x, I.y),
            kd.new(x=kd.slice([1, 2])),
            y=kd.slice([3, 4]),
        ),
        kd.slice([4, 6]),
    )

  def test_literal(self):
    expr = kd.expr.literal(kd.item(1))
    self.assertIsInstance(expr, arolla.Expr)
    kd.testing.assert_equal(arolla.eval(expr), kd.item(1))

  def test_dir(self):
    for api_name in dir(kd):
      self.assertFalse(api_name.startswith('_'))

  def test_docstring(self):
    self.assertIn('Koda API', kd.__doc__)

  def test_kdf(self):
    fn = kdf.expr_fn(
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

  def test_bind(self):
    fn = kd.bind(kd.trace_py_fn(lambda x, y: x + y), y=2)
    self.assertEqual(fn(3), 5)

  def test_with_name(self):
    x = kd.slice([1, 2, 3])
    y = kd.with_name(x, 'foo')
    self.assertIs(y, x)
    y = kd.annotation.with_name(x, 'foo')
    self.assertIs(y, x)

  def test_get_name(self):
    expr = kd.lazy.with_name(I.x + I.y, 'foo')
    self.assertEqual(kd.expr.get_name(expr), 'foo')
    self.assertIsNone(kd.expr.get_name(expr + I.z))

  def test_unwrap_named(self):
    expr = kd.lazy.with_name(I.x + I.y, 'foo')
    kd.testing.assert_equal(kd.expr.unwrap_named(expr), I.x + I.y)
    with self.assertRaisesRegex(ValueError, 'non-named'):
      _ = kd.expr.unwrap_named(expr + I.z)

  def test_pack_unpack_expr(self):
    kd.testing.assert_equal(
        kd.expr.unpack_expr(kd.expr.pack_expr(I.x + I.y)), I.x + I.y
    )
    with self.assertRaisesRegex(ValueError, 'only present EXPR DataItems'):
      kd.expr.unpack_expr(kd.item(1))

  def test_is_packed_expr(self):
    ds = kd.expr.pack_expr(I.x + I.y)
    kd.testing.assert_equal(kd.expr.is_packed_expr(ds), kd.present)
    kd.testing.assert_equal(kd.expr.is_packed_expr(kd.slice(1)), kd.missing)
    kd.testing.assert_equal(kd.expr.is_packed_expr(I.x + I.y), kd.missing)

  def test_is_fn(self):
    fn = kdf.expr_fn(57, signature=signature_utils.signature([]))
    self.assertTrue(kd.is_fn(fn))
    self.assertEqual(kd.is_fn(fn).get_schema(), schema_constants.MASK)
    fn = fn.with_attrs(returns=None)
    self.assertFalse(kd.is_fn(fn))
    self.assertEqual(kd.is_fn(fn).get_schema(), schema_constants.MASK)
    self.assertFalse(kd.is_fn(57))
    self.assertFalse(kd.is_fn(I.x))

  def test_as_expr(self):
    kd.testing.assert_equal(kd.expr.as_expr(1), kd.expr.literal(kd.slice(1)))
    kd.testing.assert_equal(kd.expr.as_expr(I.x), I.x)

  def test_fstr(self):
    kd.testing.assert_equal(kd.fstr(f'{kd.slice(1):s}'), kd.slice('1'))
    kd.testing.assert_equal(kd.strings.fstr(f'{kd.slice(1):s}'), kd.slice('1'))

  def test_fstr_expr_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'contains expression.*eager kd.fstr call'
    ):
      kd.fstr(f'{kd.expr.literal(kd.slice(1)):s}')

  def test_get_input_names(self):
    expr = I.x + I.y + V.z
    self.assertEqual(kd.expr.get_input_names(expr), ['x', 'y'])
    self.assertEqual(kd.expr.get_input_names(expr, container=V), ['z'])

  def test_is_input(self):
    self.assertTrue(kd.expr.is_input(kd.I.x))
    self.assertFalse(kd.expr.is_input(kd.V.x))

  def test_is_variable(self):
    self.assertTrue(kd.expr.is_variable(kd.V.x))
    self.assertFalse(kd.expr.is_variable(kd.I.x))

  def test_is_literal(self):
    self.assertTrue(kd.expr.is_literal(kd.expr.literal(kd.item(42))))
    self.assertFalse(kd.expr.is_literal(kd.I.x))

  def test_named_container(self):
    c = kd.named_container()
    c.x = I.x
    c.y = 2
    c.z = c.x * c.y
    self.assertEqual(kd.eval(c.z, x=2), 4)

  def test_named_container_tracing_mode(self):
    def f(x):
      c = kd.named_container()
      c.x = x
      c.y = 2
      c.z = c.x * c.y
      return c.z

    self.assertEqual(f(2), 4)
    self.assertEqual(kd.fn(f)(x=2), 4)

  def test_check_inputs(self):
    @kd.check_inputs(x=kd.INT32)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.slice([1, 2]))
    with self.assertRaises(TypeError):
      _ = f(kd.slice([1.0, 2]))

  def test_check_output(self):
    @kd.check_output(kd.INT32)
    def f(x):
      return x

    # Assert does not raise.
    _ = f(kd.slice([1, 2]))
    with self.assertRaises(TypeError):
      _ = f(kd.slice([1.0, 2]))

  def test_check_inputs_is_traceable(self):
    def f(x):
      @kd.check_inputs(x=kd.INT32)
      def g(x):
        return x + 1

      return g(x) + 1

    fn = kd.fn(f)
    # Assert does not raise.
    kd.testing.assert_equal(fn(kd.slice([1, 2])), kd.slice([3, 4]))

    with self.assertRaises(ValueError):
      _ = fn(kd.slice([1.0, 2.0]))

  def test_check_output_is_traceable(self):
    def f(x):
      @kd.check_output(kd.INT32)
      def g(x):
        return x + 1

      return g(x) + 1

    fn = kd.fn(f)
    # Assert does not raise.
    kd.testing.assert_equal(fn(kd.slice([1, 2])), kd.slice([3, 4]))
    with self.assertRaises(ValueError):
      _ = fn(kd.slice([1.0, 2.0]))

  def test_duck_type(self):
    @kd.check_inputs(x=kd.duck_type(a=kd.INT32, b=kd.STRING))
    def f(x):
      return x.b

    kd.testing.assert_equal(
        f(kd.new(a=kd.slice([1, 2]), b=kd.slice(['a', 'b']))).no_bag(),
        kd.slice(['a', 'b']),
    )

    with self.assertRaises(TypeError):
      _ = f(kd.new(a=kd.slice([1, 2])))

  def test_duck_type_is_traceable(self):
    def f(x):
      @kd.check_inputs(x=kd.duck_type(a=kd.INT32, b=kd.STRING))
      def g(x):
        return x.b
      return g(x)

    fn = kd.fn(f)
    kd.testing.assert_equal(
        fn(kd.new(a=kd.slice([1, 2]), b=kd.slice(['a', 'b']))).no_bag(),
        kd.slice(['a', 'b']),
    )

    with self.assertRaises(ValueError):
      _ = fn(kd.new(a=kd.slice([1, 2])))

  def test_duck_list(self):
    @kd.check_inputs(x=kd.duck_list(kd.INT32))
    def f(x):
      return x[:]

    kd.testing.assert_equal(f(kd.list([1, 2])).no_bag(), kd.slice([1, 2]))

    with self.assertRaises(TypeError):
      _ = f(kd.list([1.0, 2.0]))

    # Tracing mode
    fn = kd.fn(f)
    kd.testing.assert_equal(fn(kd.list([1, 2])).no_bag(), kd.slice([1, 2]))

    with self.assertRaises(ValueError):
      _ = fn(kd.list([1.0, 2.0]))

  def test_duck_list_is_traceable(self):
    def f(x):
      @kd.check_inputs(x=kd.duck_list(kd.INT32))
      def g(x):
        return x[:]
      return g(x)

    fn = kd.fn(f)
    kd.testing.assert_equal(fn(kd.list([1, 2])).no_bag(), kd.slice([1, 2]))

    with self.assertRaises(ValueError):
      _ = fn(kd.list([1.0, 2.0]))

  def test_duck_dict(self):
    @kd.check_inputs(x=kd.duck_dict(kd.STRING, kd.INT32))
    def f(x):
      return kd.sort(x.get_values())

    kd.testing.assert_equal(
        f(kd.dict({'a': 1, 'b': 2})).no_bag(), kd.slice([1, 2]))

    with self.assertRaises(TypeError):
      _ = f(kd.dict({'a': '1', 'b': '2'}))

  def test_duck_dict_is_traceable(self):
    def f(x):
      @kd.check_inputs(x=kd.duck_dict(kd.STRING, kd.INT32))
      def g(x):
        return kd.sort(x.get_values())
      return g(x)

    fn = kd.fn(f)
    kd.testing.assert_equal(
        fn(kd.dict({'a': 1, 'b': 2})).no_bag(), kd.slice([1, 2])
    )

    with self.assertRaises(ValueError):
      _ = fn(kd.dict({'a': '1', 'b': '2'}))

  def test_sub_inputs(self):
    expr = I.x + I.y + V.x
    kd.testing.assert_equal(kd.expr.sub_inputs(expr, x=I.w), I.w + I.y + V.x)
    kd.testing.assert_equal(kd.expr.sub_inputs(expr, V, x=I.w), I.x + I.y + I.w)

  def test_sub_by_name(self):
    foo = kd.lazy.with_name(I.x, 'foo')
    bar = kd.lazy.with_name(I.y, 'bar')
    expr = foo + bar
    kd.testing.assert_equal(
        kd.expr.sub_by_name(expr, foo=I.z, baz=I.w), I.z + bar
    )

  def test_sub(self):
    expr = I.x + I.y
    kd.testing.assert_equal(kd.expr.sub(expr, I.x, I.z), I.z + I.y)
    kd.testing.assert_equal(kd.expr.sub(expr, (I.x, I.z)), I.z + I.y)

  def test_eager(self):
    self.assertCountEqual(kd.eager.__all__, dir(kd.eager))
    self.assertCountEqual(set(dir(kd)) - set(dir(kd.eager)), ['eager'])
    self.assertCountEqual(set(dir(kd.eager)) - set(dir(kd)), [])
    for name in kd.eager.__all__:
      self.assertIs(getattr(kd.eager, name), getattr(kd, name))
    for bad_name in ['eager']:
      with self.assertRaises(AttributeError):
        _ = getattr(kd.eager, bad_name)

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
          "attribute 'eval' is not available in tracing mode on 'koladata.kd'",
      ):
        _ = kd.eval

  def test_tracing_for_ops(self):
    with tracing_mode.enable_tracing():
      sum_expr = kd.sum(I.x)

    kd.testing.assert_equal(sum_expr.op, kd.lazy.annotation.source_location)
    sum_op = sum_expr.node_deps[0].op

    kd.testing.assert_equal(sum_op, kd.lazy.sum)
    with tracing_mode.enable_tracing():
      math_abs_expr = kd.math.abs(I.x)
    kd.testing.assert_equal(
        math_abs_expr.op, kd.lazy.annotation.source_location
    )
    kd.testing.assert_equal(math_abs_expr.node_deps[0].op, kd.lazy.math.abs)

  def test_tracing_for_functions_error(self):
    with tracing_mode.enable_tracing():
      with self.assertRaisesRegex(
          AttributeError,
          "attribute 'container' is not available in tracing mode on"
          " 'koladata.kd'",
      ):
        _ = kd.container
      with self.assertRaisesRegex(
          AttributeError,
          "attribute 'to_py' is not available in tracing mode on 'koladata.kd'",
      ):
        _ = kd.to_py

  def test_tracing_for_with_name(self):
    with tracing_mode.enable_tracing():
      with_name_expr = kd.with_name(1, 'foo')
    kd.testing.assert_traced_exprs_equal(
        with_name_expr, kd.lazy.with_name(1, 'foo')
    )

    with tracing_mode.enable_tracing():
      with_name_expr = kd.annotation.with_name(1, 'foo')
    kd.testing.assert_traced_exprs_equal(
        with_name_expr, kd.lazy.annotation.with_name(1, 'foo')
    )

  def test_tracing_for_slice_and_item(self):
    with tracing_mode.enable_tracing():
      ds = kd.slice([1, 2, 3], schema=kd.INT64).with_name('ds')
      item = kd.item(3, schema=kd.OBJECT).with_name('item')
    self.assertIsInstance(ds, arolla.abc.Expr)
    self.assertIsInstance(item, arolla.abc.Expr)
    kd.testing.assert_equal(kd.eval(ds), kd.slice([1, 2, 3], schema=kd.INT64))
    kd.testing.assert_equal(kd.eval(item), kd.item(3, schema=kd.OBJECT))
    self.assertEqual(kd.expr.get_name(ds), 'ds')
    self.assertEqual(kd.expr.get_name(item), 'item')

  def test_tracing_for_slice_of_kd_obj(self):
    with tracing_mode.enable_tracing():
      ds = kd.slice([[kd.obj(a=1), kd.obj(a=2)], [kd.obj(a=3)]])
    self.assertIsInstance(ds, arolla.abc.Expr)
    kd.testing.assert_equal(kd.eval(ds).a.no_bag(), kd.slice([[1, 2], [3]]))

  def test_tracing_for_nested_slice_with_cast(self):
    with tracing_mode.enable_tracing():
      ds = kd.slice(kd.slice([[1, 2], [3]]), schema=kd.INT64)
    self.assertIsInstance(ds, arolla.abc.Expr)
    kd.testing.assert_equal(
        kd.eval(ds), kd.slice([[1, 2], [3]], schema=kd.INT64)
    )

  def test_tracing_for_kd_item_does_not_use_float32(self):
    with tracing_mode.enable_tracing():
      ds = kd.item(1 + 1e-14, schema=kd.FLOAT64)
    self.assertIsInstance(ds, arolla.abc.Expr)
    kd.testing.assert_equal(kd.eval(ds), kd.item(1 + 1e-14, schema=kd.FLOAT64))

  def test_tracing_for_nested_item_with_cast(self):
    with tracing_mode.enable_tracing():
      ds = kd.item(kd.item(57), schema=kd.INT64)
    self.assertIsInstance(ds, arolla.abc.Expr)
    kd.testing.assert_equal(kd.eval(ds), kd.item(57, schema=kd.INT64))

  def test_tracing_for_dedicated_py_conversions(self):
    expr = kd.expr.pack_expr(kd.I.x)
    with tracing_mode.enable_tracing():
      int32 = kd.int32([1, 2])
      int64 = kd.int64(1)
      float32 = kd.float32(3.14)
      float64 = kd.float64(3)
      str_item = kd.str('abc')
      bytes_slice = kd.bytes([b'x', b'y'])
      bool_item = kd.bool(True)
      mask = kd.mask([kd.present, kd.missing])
      expr_quote = kd.expr_quote(expr)
    kd.testing.assert_equal(kd.eval(int32), kd.slice([1, 2], schema=kd.INT32))
    kd.testing.assert_equal(kd.eval(int64), kd.slice(1, schema=kd.INT64))
    kd.testing.assert_equal(kd.eval(float32), kd.slice(3.14))
    kd.testing.assert_equal(kd.eval(float64), kd.slice(3, schema=kd.FLOAT64))
    kd.testing.assert_equal(kd.eval(str_item), kd.slice('abc'))
    kd.testing.assert_equal(kd.eval(bytes_slice), kd.slice([b'x', b'y']))
    kd.testing.assert_equal(kd.eval(bool_item), kd.slice(True))
    kd.testing.assert_equal(kd.eval(mask), kd.slice([kd.present, kd.missing]))
    kd.testing.assert_equal(
        kd.eval(expr_quote), kd.slice(kd.expr.pack_expr(kd.I.x))
    )

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

    fn = kd.trace_py_fn(g)
    kd.testing.assert_traced_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(fn.returns), V._f_result + 2
    )
    kd.testing.assert_traced_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(fn.get_attr('_f_result')),
        V.f(I.x),
    )
    kd.testing.assert_traced_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(fn.f.returns), I.x + 1
    )

  def test_tracing_schema_new(self):
    with tracing_mode.enable_tracing():
      entity = kd.uu_schema(a=kd.INT32, b=kd.STRING).new(a=42, b='xyz')
    entity = kd.eval(entity)
    kd.testing.assert_equal(
        entity.get_schema().no_bag(),
        kd.uu_schema(a=kd.INT32, b=kd.STRING).no_bag()
    )
    kd.testing.assert_equal(entity.a.no_bag(), kd.item(42))
    kd.testing.assert_equal(entity.b.no_bag(), kd.item('xyz'))

  def test_type_tracing_config(self):

    @kd.trace_as_fn()
    def swap(a: PairWithTupleTracing) -> PairWithTupleTracing:
      return PairWithTupleTracing(x=a.y, y=a.x)

    def f(o):
      p = PairWithTupleTracing(x=o.x, y=o.y[:])
      p = swap(p)
      return kd.obj(x=kd.implode(p.x), y=p.y)

    res = f(
        kd.obj(x=kd.slice([1, 2, 3]), y=kd.implode(kd.slice([[4, 5], [], [6]])))
    )
    kd.testing.assert_equal(res.x[:].no_bag(), kd.slice([[4, 5], [], [6]]))
    kd.testing.assert_equal(res.y.no_bag(), kd.slice([1, 2, 3]))

    fn = kd.fn(f)
    res = fn(
        kd.obj(x=kd.slice([1, 2, 3]), y=kd.implode(kd.slice([[4, 5], [], [6]])))
    )
    kd.testing.assert_equal(res.x[:].no_bag(), kd.slice([[4, 5], [], [6]]))
    kd.testing.assert_equal(res.y.no_bag(), kd.slice([1, 2, 3]))

  def test_call_with_kd_types_return_type(self):
    obj = kd.obj(x=1)

    with self.subTest('DataBag'):
      fn = kdf.expr_fn(returns=I.x.get_bag())
      kd.testing.assert_equal(
          fn(x=obj, return_type_as=kd.types.DataBag), obj.get_bag()
      )

    with self.subTest('DataSlice'):
      fn = kdf.expr_fn(returns=I.x)
      kd.testing.assert_equal(fn(x=obj, return_type_as=kd.types.DataSlice), obj)

    with self.subTest('JaggedShape'):
      fn = kdf.expr_fn(returns=I.x.get_shape())
      kd.testing.assert_equal(
          fn(x=obj, return_type_as=kd.types.JaggedShape), obj.get_shape()
      )

  def test_eager_op_error_message(self):
    x = kd.slice(1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""cannot find a common schema

 the common schema(s) INT32
 the first conflicting schema ITEMID"""),
    ):
      kd.schema.cast_to_implicit(x, kd.ITEMID)

  def test_non_deterministic_op_derived_from_expr_op(self):
    itemid1 = kd.new_itemid()
    itemid2 = kd.new_itemid()
    self.assertNotEqual(itemid1.fingerprint, itemid2.fingerprint)

  def test_clear_eval_cache(self):
    obj = kd.obj(a=42)
    expr = kd.lazy.extract(obj)
    first_eval = expr.eval()
    cached_eval = expr.eval()  # literal-folded computation.
    kd.testing.assert_equal(first_eval, cached_eval)
    self.assertEqual(first_eval.fingerprint, cached_eval.fingerprint)
    kd.clear_eval_cache()
    eval_after_clear_cache = expr.eval()
    kd.testing.assert_equivalent(first_eval, eval_after_clear_cache)
    self.assertNotEqual(
        first_eval.fingerprint, eval_after_clear_cache.fingerprint
    )
    kd.testing.assert_equal(eval_after_clear_cache, expr.eval())

  def test_clear_all_arolla_caches_does_not_break(self):
    obj = kd.obj(a=42)
    expr = kd.lazy.get_attr(obj, 'a') - 12
    kd.testing.assert_equal(expr.eval(), kd.slice(30))
    arolla.abc.clear_caches()
    kd.testing.assert_equal(expr.eval(), kd.slice(30))

  def test_eager_op_overrides_expr_op(self):
    self.assertIs(kd.obj, functions.obj)
    self.assertIs(kd.objs.new, functions.objs.new)

  def test_functor_expr_fn(self):
    fn = kd.functor.expr_fn(returns=I.x + V.foo, foo=I.y)
    kd.testing.assert_equal(kd.call(fn, x=1, y=2), kd.item(3))
    kd.testing.assert_equal(fn(x=1, y=2), kd.item(3))
    self.assertTrue(kd.is_fn(fn))
    self.assertFalse(kd.is_fn(57))

  def test_functor_factorial(self):
    fn = kd.functor.expr_fn(
        kd.lazy.cond(I.n == 0, V.stop, V.go)(n=I.n),
        go=kd.functor.expr_fn(I.n * V.rec(n=I.n - 1)),
        stop=kd.functor.expr_fn(1),
    )
    fn = fn.updated(kd.attrs(fn.go, rec=fn))
    kd.testing.assert_equal(fn(n=5), kd.item(120))

  def test_trace_py_fn(self):
    fn = kd.trace_py_fn(lambda x, y: x + y)
    kd.testing.assert_equal(fn(x=1, y=2), kd.item(3))
    kd.testing.assert_traced_exprs_equal(
        kd.expr.unpack_expr(fn.returns), I.x + I.y
    )

  def test_py_fn(self):
    fn = kd.py_fn(lambda x, y: x + 1 if y == 2 else x + 3)
    kd.testing.assert_equal(fn(x=1, y=2), kd.item(2))
    kd.testing.assert_equal(fn(x=1, y=3), kd.item(4))

  def test_fn(self):
    fn = kd.fn(lambda x, y: x + y)
    kd.testing.assert_equal(fn(x=1, y=2), kd.item(3))
    fn = kd.fn(I.x + I.y)
    kd.testing.assert_equal(fn(x=1, y=2), kd.item(3))

  def test_operator_definition(self):

    @kd.optools.add_to_registry()
    @kd.optools.as_py_function_operator(
        'kd.core.kd_test_op',
        qtype_constraints=[
            kd.optools.constraints.expect_data_slice(arolla.P.x)
        ],
        qtype_inference_expr=kd.qtypes.DATA_SLICE,
    )
    def kd_test_op(x):
      return x + 1

    # Can access and eval op.
    expr = kd.lazy.core.kd_test_op(kd.slice([1, 2]))
    kd.testing.assert_equal(kd.eval(expr), kd.slice([2, 3]))
    kd.testing.assert_equal(
        kd.core.kd_test_op(kd.slice([1, 2])), kd.slice([2, 3])
    )
    # Only accepts DataSlices.
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: DATA_BAG'
    ):
      kd_test_op(kd.bag())

  def test_equiv_to_op(self):
    @kd.optools.add_to_registry()
    @kd.optools.as_lambda_operator('kd_test.bad_op')
    def bad_op(x):  # pylint: disable=unused-variable
      return x

    @kd.optools.add_to_registry()
    @kd.optools.as_lambda_operator('kd_test.equiv_to_op_op')
    def equiv_to_op_op(x):  # pylint: disable=unused-variable
      return x

    kd.optools.add_alias('kd_test.equiv_to_op_op', 'kd_test.equiv_to_op_alias')
    self.assertTrue(
        kd.optools.equiv_to_op(
            'kd_test.equiv_to_op_op', 'kd_test.equiv_to_op_op'
        )
    )
    self.assertTrue(
        kd.optools.equiv_to_op(
            'kd_test.equiv_to_op_op', 'kd_test.equiv_to_op_alias'
        )
    )
    self.assertFalse(
        kd.optools.equiv_to_op('kd_test.equiv_to_op_op', 'kd_test.bad_op')
    )

  def test_as_qvalue(self):
    kd.testing.assert_equal(kd.optools.as_qvalue(1), kd.item(1))

  def test_as_qvalue_or_expr(self):
    kd.testing.assert_equal(kd.optools.as_qvalue_or_expr(1), kd.item(1))
    kd.testing.assert_equal(kd.optools.as_qvalue_or_expr(I.x), I.x)

  def test_qtypes(self):
    kd.testing.assert_equal(kd.item(1).qtype, kd.qtypes.DATA_SLICE)
    kd.testing.assert_equal(kd.bag().qtype, kd.qtypes.DATA_BAG)

    @kd.trace_as_fn(return_type_as=arolla.INT32)
    def get_data_slice():
      return kd.qtypes.DATA_SLICE

    kd.testing.assert_equal(get_data_slice(), kd.qtypes.DATA_SLICE)

  def test_eager_operator(self):
    self.assertIsInstance(kd.add, kd.optools.eager.EagerOperator)
    kd.testing.assert_equal(kd.add.lazy_op, kd.lazy.add)

  def test_function_boxing(self):
    fn = kd.expr.as_expr(lambda x: x + 1)
    kd.testing.assert_equal(kd.eval(kd.lazy.call(fn, 5)), kd.item(6))

  def test_get_nth_eager_version(self):
    # This operator requires a specific eager implementation since it requires
    # literal inputs.
    kd.testing.assert_equal(kd.tuples.get_nth((1, 2, 3), 1), kd.item(2))

  def test_get_namedtuple_field_eager_version(self):
    # This operator requires a specific eager implementation since it requires
    # literal inputs.
    kd.testing.assert_equal(
        kd.tuples.get_namedtuple_field(kd.namedtuple(x=1, y='abc'), 'y'),
        kd.item('abc'),
    )


if __name__ == '__main__':
  absltest.main()
