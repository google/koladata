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
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.objects import objects
from koladata.expr import input_container
from koladata.functions import functions as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde

_EXT_TYPE = M.derived_qtype.get_labeled_qtype(
    extension_type_registry.BASE_QTYPE, '_MyTestExtension'
).qvalue


class _MyTestExtension:
  x: schema_constants.INT32
  y: schema_constants.INT32


extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)


class _MyOtherTestExtension:
  pass


def unwrap(x):
  return arolla.abc.aux_eval_op('kd.extension_types.unwrap', x)


class ExtensionTypeRegistryTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # The registry is a module-level global. Clear it to ensure test isolation.
    self.addCleanup(extension_type_registry._EXTENSION_TYPE_REGISTRY.clear)

  def test_is_extension_type(self):
    obj = objects.Object(x=ds(0), y=ds(1))
    ext = arolla.eval(M.derived_qtype.downcast(_EXT_TYPE, obj))
    # A raw Object is not an extension type.
    self.assertFalse(extension_type_registry.is_koda_extension(obj))
    # The extension type is not yet registered, so it's false even if it has
    # an ok form.
    self.assertFalse(extension_type_registry.is_koda_extension(ext))
    # Is an extension type after registering.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    self.assertTrue(extension_type_registry.is_koda_extension(ext))

  def test_wrap_unwrap(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    obj = objects.Object(x=ds(0), y=ds(1))
    wrapped_obj = extension_type_registry.wrap(obj, _EXT_TYPE)
    self.assertEqual(wrapped_obj.qtype, _EXT_TYPE)
    unwrapped_obj = unwrap(wrapped_obj)
    self.assertIsInstance(unwrapped_obj, objects.Object)
    testing.assert_equal(unwrapped_obj, obj)

  def test_wrap_invalid_input(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected one of [OBJECT], got obj: INT32'),
    ):
      extension_type_registry.wrap(123, _EXT_TYPE)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError,
        'there is no registered extension type corresponding to the QType'
        ' INT32',
    ):
      extension_type_registry.wrap(ds([1, 2, 3]), arolla.INT32)  # pytype: disable=wrong-arg-types

  def test_wrap_not_registered(self):
    obj = objects.Object(x=ds(1), y=ds(2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'there is no registered extension type corresponding to the QType'
            ' LABEL[_MyTestExtension]'
        ),
    ):
      extension_type_registry.wrap(obj, _EXT_TYPE)

  def test_unwrap_invalid_input(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      unwrap(ds([1, 2, 3]))

  def test_registry(self):
    # Before registration.
    self.assertFalse(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )
    with self.assertRaisesRegex(
        ValueError, 'is not a registered extension type'
    ):
      extension_type_registry.get_extension_qtype(_MyTestExtension)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'there is no registered extension type corresponding to the'
            ' QType LABEL[_MyTestExtension]'
        ),
    ):
      extension_type_registry.get_extension_cls(_EXT_TYPE)

    # Successful registration.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    self.assertTrue(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )
    self.assertEqual(
        extension_type_registry.get_extension_qtype(_MyTestExtension), _EXT_TYPE
    )
    self.assertEqual(
        extension_type_registry.get_extension_cls(_EXT_TYPE), _MyTestExtension
    )

    # Re-registering the same is fine.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)

    # Test error cases.
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      extension_type_registry.register_extension_type(
          _MyOtherTestExtension, arolla.INT32
      )

    other_qtype = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'bar'
    ).qvalue
    with self.assertRaisesRegex(
        ValueError, 'is already registered with a different qtype'
    ):
      extension_type_registry.register_extension_type(
          _MyTestExtension, other_qtype
      )

    # Unsafe override allows it to be registered.
    extension_type_registry.register_extension_type(
        _MyTestExtension, other_qtype, unsafe_override=True
    )
    self.assertEqual(
        extension_type_registry.get_extension_qtype(_MyTestExtension),
        other_qtype,
    )
    self.assertEqual(
        extension_type_registry.get_extension_cls(other_qtype),
        _MyTestExtension,
    )

    with self.assertRaisesRegex(
        ValueError, 'is already registered with a different class'
    ):
      extension_type_registry.register_extension_type(
          _MyOtherTestExtension, other_qtype
      )

    # Unsafe override allows it to be registered.
    extension_type_registry.register_extension_type(
        _MyOtherTestExtension, other_qtype, unsafe_override=True
    )
    self.assertEqual(
        extension_type_registry.get_extension_qtype(_MyOtherTestExtension),
        other_qtype,
    )
    self.assertEqual(
        extension_type_registry.get_extension_cls(other_qtype),
        _MyOtherTestExtension,
    )
    # It deregisters the old type.
    self.assertFalse(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )

  def test_get_dummy_value(self):

    class MyExtensionType:
      pass

    dummy_qtype = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'MyExtensionType'
    ).qvalue
    extension_type_registry.register_extension_type(
        MyExtensionType, dummy_qtype
    )
    res = extension_type_registry.get_dummy_value(MyExtensionType)
    testing.assert_equal(
        res,
        extension_type_registry.wrap(
            objects.Object(),
            extension_type_registry.get_extension_qtype(MyExtensionType),
        ),
    )
    # Also integrated with py_boxing, allowing it to be used for e.g. functor
    # annotations
    testing.assert_equal(
        py_boxing.get_dummy_qvalue(MyExtensionType),
        extension_type_registry.wrap(
            objects.Object(),
            extension_type_registry.get_extension_qtype(MyExtensionType),
        ),
    )

  def test_get_dummy_value_not_registered(self):
    class MyExtensionType:
      x: schema_constants.INT32

    with self.assertRaisesRegex(
        ValueError, 'MyExtensionType.*is not a registered extension type'
    ):
      extension_type_registry.get_dummy_value(MyExtensionType)

  def test_dynamic_cast(self):
    # NOTE: This is more thoroughly tested in extension_types_test.py and
    # operators/tests/extension_types_dynamic_cast_test.py.
    class A:
      pass

    class B:
      pass

    a_type = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'A'
    ).qvalue
    b_type = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'B'
    ).qvalue

    extension_type_registry.register_extension_type(A, a_type)
    extension_type_registry.register_extension_type(B, b_type)
    obj = objects.Object(x=ds(1), y=ds(2))
    a = extension_type_registry.wrap(obj, a_type)
    b = extension_type_registry.wrap(obj, b_type)

    testing.assert_equal(extension_type_registry.dynamic_cast(a, b_type), b)
    testing.assert_equal(extension_type_registry.dynamic_cast(b, a_type), a)

  def test_get_attr(self):
    # NOTE: This is more thoroughly tested in extension_types_test.py and
    # operators/tests/extension_types_get_attr_test.py.
    class A:
      pass

    a_type = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'A'
    ).qvalue
    extension_type_registry.register_extension_type(A, a_type)

    db = data_bag.DataBag.empty()
    obj = objects.Object(x=ds(1), y=db)
    a = extension_type_registry.wrap(obj, a_type)
    testing.assert_equal(
        extension_type_registry.get_attr(a, 'x', qtypes.DATA_SLICE),
        ds(1),
    )
    testing.assert_equal(
        extension_type_registry.get_attr(a, 'y', qtypes.DATA_BAG),
        db,
    )

    with self.assertRaisesRegex(ValueError, "attribute not found: 'z'"):
      _ = extension_type_registry.get_attr(a, 'z', qtypes.DATA_SLICE)

    with self.assertRaisesRegex(
        ValueError,
        "looked for attribute 'x' with type DATA_BAG, but the attribute has"
        ' actual type DATA_SLICE',
    ):
      _ = extension_type_registry.get_attr(a, 'x', qtypes.DATA_BAG)

  def test_make(self):
    # NOTE: This is more thoroughly tested in extension_types_test.py and
    # operators/tests/extension_types_make_test.py.
    class A:
      pass

    a_type = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'A'
    ).qvalue
    extension_type_registry.register_extension_type(A, a_type)

    with self.subTest('empty'):
      res = extension_type_registry.make(a_type)
      expected = extension_type_registry.wrap(objects.Object(), a_type)
      testing.assert_equal(res, expected)

    with self.subTest('attrs'):
      res = extension_type_registry.make(a_type, x=1, y=2)
      expected = extension_type_registry.wrap(
          objects.Object(x=ds(1), y=ds(2)), a_type
      )
      testing.assert_equal(res, expected)

    with self.subTest('prototype'):
      prototype = objects.Object(a=arolla.int32(1))
      res = extension_type_registry.make(a_type, prototype, x=1, y=2)
      expected = extension_type_registry.wrap(
          objects.Object(prototype, x=ds(1), y=ds(2)), a_type
      )
      testing.assert_equal(res, expected)

  def test_make_null(self):
    # NOTE: This is more thoroughly tested in extension_types_test.py and
    # operators/tests/extension_types_make_null_test.py.
    class A:
      pass

    a_type = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, 'A'
    ).qvalue
    extension_type_registry.register_extension_type(A, a_type)
    res = extension_type_registry.make_null(a_type)
    expected = extension_type_registry.wrap(
        objects.Object(_is_null_marker=arolla.present()), a_type
    )
    testing.assert_equal(res, expected)


if __name__ == '__main__':
  absltest.main()
