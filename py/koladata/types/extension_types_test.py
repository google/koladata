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

import inspect
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.expr import input_container
from koladata.functions import functions as _
from koladata.functor import functor_factories
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import extension_types as ext_types
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals

_EXT_TYPE = M.derived_qtype.get_labeled_qtype(
    qtypes.DATA_SLICE, '_MyTestExtension'
).qvalue


class _MyTestExtension:
  x: schema_constants.INT32
  y: schema_constants.INT32


class _MyOtherTestExtension:
  pass


class ExtensionTypesTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # The registry is a module-level global. Clear it to ensure test isolation.
    self.addCleanup(ext_types._EXTENSION_TYPE_REGISTRY.clear)

  def test_is_extension_type(self):
    self.assertFalse(
        ext_types.is_koda_extension(data_slice.DataSlice.from_vals([1, 2, 3]))
    )
    self.assertTrue(
        ext_types.is_koda_extension(
            ext_types.wrap(data_slice.DataSlice.from_vals([1, 2, 3]), _EXT_TYPE)
        )
    )

  def test_wrap_unwrap(self):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    wrapped_x = ext_types.wrap(x, _EXT_TYPE)
    self.assertEqual(wrapped_x.qtype, _EXT_TYPE)
    unwrapped_x = ext_types.unwrap(wrapped_x)
    self.assertIsInstance(unwrapped_x, data_slice.DataSlice)
    testing.assert_equal(unwrapped_x, x)

  def test_wrap_invalid_input(self):
    with self.assertRaisesRegex(ValueError, 'expected a DataSlice'):
      ext_types.wrap(123, _EXT_TYPE)
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      ext_types.wrap(data_slice.DataSlice.from_vals([1, 2, 3]), arolla.INT32)

  def test_unwrap_invalid_input(self):
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      ext_types.unwrap(data_slice.DataSlice.from_vals([1, 2, 3]))

  def test_registry(self):
    # Before registration.
    self.assertFalse(ext_types.is_koda_extension_type(_MyTestExtension))
    with self.assertRaisesRegex(
        ValueError, 'is not a registered extension type'
    ):
      ext_types.get_extension_qtype(_MyTestExtension)

    # Successful registration.
    ext_types.register_extension_type(_MyTestExtension, _EXT_TYPE)
    self.assertTrue(ext_types.is_koda_extension_type(_MyTestExtension))
    self.assertEqual(ext_types.get_extension_qtype(_MyTestExtension), _EXT_TYPE)

    # Re-registering the same is fine.
    ext_types.register_extension_type(_MyTestExtension, _EXT_TYPE)

    # Test error cases.
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      ext_types.register_extension_type(_MyOtherTestExtension, arolla.INT32)

    other_qtype = M.derived_qtype.get_labeled_qtype(
        qtypes.DATA_SLICE, 'bar'
    ).qvalue
    with self.assertRaisesRegex(
        ValueError, 'is already registered with a different qtype'
    ):
      ext_types.register_extension_type(_MyTestExtension, other_qtype)

    # Unsafe override allows it to be registered.
    ext_types.register_extension_type(
        _MyTestExtension, other_qtype, unsafe_override=True
    )
    self.assertEqual(
        ext_types.get_extension_qtype(_MyTestExtension), other_qtype
    )

    with self.assertRaisesRegex(
        ValueError, 'is already registered with a different class'
    ):
      ext_types.register_extension_type(_MyOtherTestExtension, other_qtype)

    # Unsafe override allows it to be registered.
    ext_types.register_extension_type(
        _MyOtherTestExtension, other_qtype, unsafe_override=True
    )
    self.assertEqual(
        ext_types.get_extension_qtype(_MyOtherTestExtension), other_qtype
    )
    # It deregisters the old type.
    self.assertFalse(ext_types.is_koda_extension_type(_MyTestExtension))

  def test_extension_type(self):
    ext_type = ext_types.extension_type()(_MyTestExtension)
    self.assertTrue(ext_types.is_koda_extension_type(ext_type))
    self.assertEqual(ext_types.get_extension_qtype(ext_type), _EXT_TYPE)

    value = ext_type(x=1, y=2)
    self.assertEqual(value.x, 1)
    self.assertEqual(value.y, 2)

    expr = ext_type(
        x=arolla.M.annotation.qtype(I.x, qtypes.DATA_SLICE),
        y=arolla.M.annotation.qtype(I.y, qtypes.DATA_SLICE),
    )
    self.assertIsInstance(expr, arolla.Expr)
    # TODO: Make test_utils.assert_equal work for extension types.
    self.assertEqual(expr.eval(x=1, y=2).x, 1)
    self.assertEqual(expr.eval(x=1, y=2).y, 2)

  def test_kd_fn(self):

    @ext_types.extension_type()
    class MyExtension:
      x: schema_constants.INT32
      y: schema_constants.FLOAT32

      def fn(self, v):
        return self.x + v

    e = MyExtension(x=1, y=2)

    def fn(x: MyExtension):
      return x.fn(2)

    self.assertEqual(functor_factories.fn(fn)(e), 3)

  def test_attr_fields(self):

    @ext_types.extension_type()
    class MyExtensionWithDefaults:
      x: schema_constants.INT32
      y: schema_constants.INT32 = 2

    with self.subTest('default_value'):
      e = MyExtensionWithDefaults(x=1)
      self.assertEqual(e.x, 1)
      self.assertEqual(e.y, 2)

    with self.subTest('override_default_value'):
      e = MyExtensionWithDefaults(x=1, y=3)
      self.assertEqual(e.x, 1)
      self.assertEqual(e.y, 3)

    with self.subTest('positional_args'):
      e = MyExtensionWithDefaults(1)
      self.assertEqual(e.x, 1)
      self.assertEqual(e.y, 2)

    with self.subTest('positional_args_override_default'):
      e = MyExtensionWithDefaults(1, 3)
      self.assertEqual(e.x, 1)
      self.assertEqual(e.y, 3)

    with self.subTest('extra_field'):
      with self.assertRaisesRegex(
          TypeError, "got an unexpected keyword argument 'z'"
      ):
        _ = MyExtensionWithDefaults(x=1, z=3)

    with self.subTest('missing_field'):
      with self.assertRaisesRegex(
          TypeError, "missing a required argument: 'x'"
      ):
        _ = MyExtensionWithDefaults(y=2)

  def test_signature(self):

    @ext_types.extension_type()
    class MyExtensionWithDefaults:
      x: schema_constants.INT32
      y: schema_constants.INT32 = 2

    def fake_fn(x: schema_constants.INT32, y: schema_constants.INT32 = 2):
      del x, y

    sig = inspect.signature(MyExtensionWithDefaults)
    self.assertEqual(sig, inspect.signature(fake_fn))

  def test_implicit_casting(self):

    @ext_types.extension_type()
    class MyExtensionWithInt64Field:
      x: schema_constants.INT64

    x = MyExtensionWithInt64Field(x=data_slice.DataSlice.from_vals(1))
    self.assertEqual(x.x.get_schema(), schema_constants.INT64)

    with self.assertRaisesRegex(
        ValueError,
        r'cannot create Item\(s\) with the provided schema:'
        r' MyExtensionWithInt64Field\(x=INT64\)',
    ):
      _ = MyExtensionWithInt64Field(x=data_slice.DataSlice.from_vals(1.0))

  def test_extension_type_decorator_unsafe_override(self):
    class Cls1:
      pass

    class Cls2:
      pass

    Cls2.__name__ = 'Cls1'
    ext_types.extension_type()(Cls1)
    with self.assertRaisesRegex(
        ValueError, 'is already registered with a different class'
    ):
      ext_types.extension_type()(Cls2)
    ext_types.extension_type(unsafe_override=True)(Cls2)

  def test_lazy_methods(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32

      def foo(self, y):
        return self.x + y

    x = MyExtensionType(I.x)
    expr = x.foo(I.y)
    testing.assert_equal(expr.eval(x=1, y=2), ds(3))


if __name__ == '__main__':
  absltest.main()
