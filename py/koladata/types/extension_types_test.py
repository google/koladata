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
import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view
from koladata.functions import functions as _
from koladata.functor import functor_factories
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import extension_types as ext_types
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
I = input_container.InputContainer('I')
C = input_container.InputContainer('C')
ds = data_slice.DataSlice.from_vals

_EXT_TYPE = M.derived_qtype.get_labeled_qtype(
    extension_type_registry.BASE_QTYPE, '_MyTestExtension'
).qvalue

_DUMMY_EXT_TYPE = M.derived_qtype.get_labeled_qtype(
    extension_type_registry.BASE_QTYPE, '_MyDummyExtension'
).qvalue


@ext_types.extension_type()  # This is cleared in setUp()
class _MyTestExtension:
  x: schema_constants.INT32
  y: schema_constants.INT32


@ext_types.extension_type()  # This is cleared in setUp()
class _MyDummyExtension:
  x: schema_constants.NONE
  y: schema_constants.NONE


class _MyOtherTestExtension:
  pass


_DUMMY_VALUES = (
    (schema_constants.NONE, ds(None)),
    (data_slice.DataSlice, ds(None)),
    (data_item.DataItem, ds(None)),
    (data_bag.DataBag, data_bag.DataBag.empty().freeze()),
    (jagged_shape.JaggedShape, jagged_shape.create_shape()),
    (_MyDummyExtension, _MyDummyExtension(ds(None), ds(None))),
    (arolla.INT32, arolla.int32(0)),
)


class ExtensionTypesTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # The registry is a module-level global. Clear it to ensure test isolation.
    self.addCleanup(extension_type_registry._EXTENSION_TYPE_REGISTRY.clear)

  def test_extension_type(self):
    ext_type = ext_types.extension_type()(_MyTestExtension)
    self.assertTrue(extension_type_registry.is_koda_extension_type(ext_type))
    self.assertEqual(
        extension_type_registry.get_extension_qtype(ext_type), _EXT_TYPE
    )

    value = ext_type(x=1, y=2)
    self.assertEqual(value.x, 1)
    self.assertEqual(value.y, 2)

    expr = ext_type(
        x=arolla.M.annotation.qtype(I.x, qtypes.DATA_SLICE),
        y=arolla.M.annotation.qtype(I.y, qtypes.DATA_SLICE),
    )
    self.assertIsInstance(expr, arolla.Expr)
    testing.assert_equal(
        extension_type_registry.unwrap(expr.eval(x=1, y=2)),
        extension_type_registry.unwrap(value),
    )

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
      y: schema_constants.INT64

    # Does implicit casting _and_ narrowing.
    with self.subTest('eager'):
      x = MyExtensionWithInt64Field(x=ds(1), y=ds(2, schema_constants.OBJECT))
      self.assertEqual(x.x.get_schema(), schema_constants.INT64)
      self.assertEqual(x.y.get_schema(), schema_constants.INT64)

    with self.subTest('lazy'):
      x = MyExtensionWithInt64Field(x=I.x, y=I.y)
      self.assertEqual(
          x.x.get_schema().eval(x=ds(1), y=ds(2, schema_constants.OBJECT)),
          schema_constants.INT64,
      )
      self.assertEqual(
          x.y.get_schema().eval(x=ds(1), y=ds(2, schema_constants.OBJECT)),
          schema_constants.INT64,
      )

    with self.subTest('error'):
      # TODO: Add which field failed.
      with self.assertRaisesRegex(
          ValueError,
          'unsupported narrowing cast to INT64 for the given FLOAT32 DataSlice',
      ):
        _ = MyExtensionWithInt64Field(x=ds(1.0), y=ds(2))

  def test_boxing(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: data_slice.DataSlice
      y: data_slice.DataSlice

    with self.subTest('eager'):
      ext = MyExtensionType(1, 2)
      testing.assert_equal(ext.x, ds(1))
      testing.assert_equal(ext.y, ds(2))

    with self.subTest('lazy'):
      ext = MyExtensionType(1, I.y)
      testing.assert_equal(ext.x.eval(y=2), ds(1))
      testing.assert_equal(ext.y.eval(y=2), ds(2))

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

  def test_expr_view_tag(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32

    x = MyExtensionType(I.x)
    self.assertTrue(view.has_base_koda_view(x))
    self.assertFalse(view.has_koda_view(x))

  def test_expr_view_eval_method(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32

    testing.assert_equal(MyExtensionType(I.x).eval(x=1), MyExtensionType(1))
    testing.assert_equal(MyExtensionType(I.self).eval(1), MyExtensionType(1))

  def test_inputs(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32
      y: schema_constants.INT32
      z: schema_constants.INT32

    self.assertListEqual(MyExtensionType(I.x, C.y, I.z).inputs(), ['x', 'z'])

  def test_with_name(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32

    expr = MyExtensionType(I.x).with_name('my_ext_type')
    self.assertEqual(introspection.get_name(expr), 'my_ext_type')
    testing.assert_equal(introspection.unwrap_named(expr), MyExtensionType(I.x))
    testing.assert_equal(expr.x.eval(x=1), ds(1))

  def test_no_broadcasting_or_adoption(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32
      y: schema_constants.INT32

    # Different bags, different shape.
    x = ds(1).with_bag(data_bag.DataBag.empty_mutable())
    y = ds([1, 2, 3]).with_bag(data_bag.DataBag.empty_mutable())
    ext = MyExtensionType(x, y)
    testing.assert_equal(ext.x, x)
    testing.assert_equal(ext.y, y)

  @parameterized.parameters(*_DUMMY_VALUES)
  def test_annotations(self, annotation, value):
    extension_type_registry.register_extension_type(
        _MyDummyExtension, _DUMMY_EXT_TYPE
    )

    @ext_types.extension_type()
    class MyExtensionType:
      x: annotation  # pytype: disable=invalid-annotation

    with self.subTest('eager'):
      ext = MyExtensionType(value)
      testing.assert_equal(ext.x, value)

    with self.subTest('lazy'):
      ext = MyExtensionType(I.x)
      # NOTE: We use `expr_eval.eval` since for e.g. arolla.INT32, the output
      # doesn't have a Koda-like view with `.eval`.
      testing.assert_equal(expr_eval.eval(ext.x, x=value), value)

  def test_unsupported_annotation(self):
    class MyExtensionType:
      x: int

    with self.assertRaisesRegex(
        ValueError, 'unsupported extension type annotation:.*int'
    ):
      ext_types.extension_type()(MyExtensionType)

  def test_unsupported_annotation_unsupported_instance(self):
    class MyExtensionType:
      x: 1  # pytype: disable=invalid-annotation

    with self.assertRaisesRegex(
        ValueError, 'unsupported extension type annotation: 1'
    ):
      ext_types.extension_type()(MyExtensionType)

  def test_bad_value_for_annotation(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: data_slice.DataSlice

    ext = MyExtensionType(jagged_shape.create_shape())
    with self.assertRaisesRegex(
        ValueError,
        "looked for attribute 'x' with type DATA_SLICE, but the attribute has"
        ' actual type JAGGED_SHAPE',
    ):
      _ = ext.x

  def test_wrong_decorator_use(self):
    class MyExtensionType:
      x: data_slice.DataSlice

    with self.assertRaisesRegex(
        TypeError,
        'expected unsafe_override.*to be a bool - did you mean to write'
        + re.escape(' `@extension_type()` instead of `@extension_type`?'),
    ):
      ext_types.extension_type(MyExtensionType)

  def test_nested_extension_type_attribute_access(self):

    @ext_types.extension_type()
    class MyInnerExtensionType:
      x: schema_constants.INT64

      def inner_fn(self):
        return self.x + 1

    @ext_types.extension_type()
    class MyOuterExtensionType:
      y: MyInnerExtensionType

      def outer_fn(self):
        return self.y.inner_fn() + 1

    with self.subTest('eager'):
      ext = MyOuterExtensionType(MyInnerExtensionType(1))
      testing.assert_equal(ext.y, MyInnerExtensionType(1))
      testing.assert_equal(ext.y.x, ds(1, schema_constants.INT64))
      testing.assert_equal(ext.outer_fn(), ds(3, schema_constants.INT64))
      testing.assert_equal(ext.y.inner_fn(), ds(2, schema_constants.INT64))

    with self.subTest('lazy'):
      ext = MyOuterExtensionType(MyInnerExtensionType(I.x))
      testing.assert_equal(ext.y.eval(x=1), MyInnerExtensionType(1))
      testing.assert_equal(ext.y.x.eval(x=1), ds(1, schema_constants.INT64))
      testing.assert_equal(
          ext.outer_fn().eval(x=1), ds(3, schema_constants.INT64)
      )
      testing.assert_equal(
          ext.y.inner_fn().eval(x=1), ds(2, schema_constants.INT64)
      )

  def test_inheritance(self):
    @ext_types.extension_type()
    class MyExtension:
      x: schema_constants.INT32

      def foo(self):
        return self.x + 1

      def bar(self):
        return self.x

    @ext_types.extension_type()
    class MyChildExtension(MyExtension):
      y: schema_constants.INT32

      def bar(self):
        return self.y

    with self.subTest('eager'):
      ext = MyChildExtension(1, 3)
      testing.assert_equal(ext.x, ds(1))
      testing.assert_equal(ext.y, ds(3))
      testing.assert_equal(ext.foo(), ds(2))
      testing.assert_equal(ext.bar(), ds(3))

    with self.subTest('lazy'):
      ext = MyChildExtension(I.x, I.y)
      testing.assert_equal(ext.x.eval(x=1, y=3), ds(1))
      testing.assert_equal(ext.y.eval(x=1, y=3), ds(3))
      testing.assert_equal(ext.foo().eval(x=1, y=3), ds(2))
      testing.assert_equal(ext.bar().eval(x=1, y=3), ds(3))

  def test_redefine_magic_methods(self):
    @ext_types.extension_type()
    class MyExtensionType:
      x: schema_constants.INT32

      def __eq__(self, other):
        return self.x + 1 == other.x

      def __hash__(self):
        return hash(self.x)

    ext1 = MyExtensionType(1)
    ext2 = MyExtensionType(2)
    ext3 = MyExtensionType(1)

    # Uses redefined __eq__.
    self.assertEqual(ext1, ext2)
    self.assertNotEqual(ext1, ext3)

    # Doesn't fail despite __hash__ being prohibited for ExprView.
    expr = MyExtensionType(I.x)

    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      _ = hash(expr)

  def test_inheritance_casting(self):
    @ext_types.extension_type()
    class MyExtension:
      x: schema_constants.INT32

      def foo(self):
        return self.x + 1

    @ext_types.extension_type()
    class MyChildExtension(MyExtension):
      y: schema_constants.INT32

      def foo(self):
        return self.y

    my_extension_qtype = extension_type_registry.get_extension_qtype(
        MyExtension
    )
    my_child_extension_qtype = extension_type_registry.get_extension_qtype(
        MyChildExtension
    )
    with self.subTest('cast_to_self'):
      expr = extension_type_registry.dynamic_cast(
          MyExtension(I.x), my_extension_qtype
      )
      self.assertIsInstance(expr, arolla.Expr)
      testing.assert_equal(expr.eval(x=1), MyExtension(1))
      testing.assert_equal(
          extension_type_registry.dynamic_cast(
              MyExtension(1), my_extension_qtype
          ),
          MyExtension(1),
      )

    with self.subTest('cast_to_parent'):
      expr = extension_type_registry.dynamic_cast(
          MyChildExtension(I.x, I.y), my_extension_qtype
      )
      self.assertIsInstance(expr, arolla.Expr)
      res = expr.eval(x=1, y=3)
      testing.assert_equal(res.qtype, my_extension_qtype)
      testing.assert_equal(res.x, ds(1))
      # By default, we also call the parent implementation of functions.
      testing.assert_equal(expr.foo().eval(x=1, y=3), ds(2))
      # Eager.
      res = extension_type_registry.dynamic_cast(
          MyChildExtension(1, 3), my_extension_qtype
      )
      testing.assert_equal(res.qtype, my_extension_qtype)
      testing.assert_equal(res.x, ds(1))

    with self.subTest('cast_to_child'):
      # We can cast back to the child type again.
      expr = extension_type_registry.dynamic_cast(
          MyChildExtension(I.x, I.y), my_extension_qtype
      )
      expr = extension_type_registry.dynamic_cast(
          expr, my_child_extension_qtype
      )
      self.assertIsInstance(expr, arolla.Expr)
      testing.assert_equal(expr.eval(x=1, y=3), MyChildExtension(1, 3))
      # By default, we then also call the child impl.
      testing.assert_equal(expr.foo().eval(x=1, y=3), ds(3))

  def test_inheritance_casting_incompatible_field_redefinition(self):
    @ext_types.extension_type()
    class MyExtension:
      x: data_slice.DataSlice

    @ext_types.extension_type()
    class MyChildExtension(MyExtension):
      x: data_bag.DataBag

    my_extension_qtype = extension_type_registry.get_extension_qtype(
        MyExtension
    )
    casted = extension_type_registry.dynamic_cast(
        MyChildExtension(data_bag.DataBag.empty()), my_extension_qtype
    )
    with self.assertRaisesRegex(
        ValueError,
        "looked for attribute 'x' with type DATA_SLICE, but the attribute has"
        ' actual type DATA_BAG',
    ):
      _ = casted.x

  def test_inheritance_downcasting_incompatible_fields_error(self):
    @ext_types.extension_type()
    class MyExtension:
      x: schema_constants.INT32

    @ext_types.extension_type()
    class MyChildExtension(MyExtension):
      y: schema_constants.INT32

    my_child_extension_qtype = extension_type_registry.get_extension_qtype(
        MyChildExtension
    )
    casted = extension_type_registry.dynamic_cast(
        MyExtension(1), my_child_extension_qtype
    )
    with self.assertRaisesRegex(ValueError, "attribute not found: 'y'"):
      _ = casted.y


if __name__ == '__main__':
  absltest.main()
