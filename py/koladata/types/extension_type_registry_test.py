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
from koladata.expr import input_container
from koladata.functions import functions as _
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals

_EXT_TYPE = M.derived_qtype.get_labeled_qtype(
    arolla.make_tuple_qtype(qtypes.DATA_SLICE, qtypes.DATA_SLICE),
    '_MyTestExtension',
).qvalue


class _MyTestExtension:
  x: schema_constants.INT32
  y: schema_constants.INT32


extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)


class _MyOtherTestExtension:
  pass


_DUMMY_VALUES = (
    (qtypes.DATA_SLICE, ds(None)),
    (qtypes.DATA_BAG, extension_type_registry._get_dummy_bag()),
    (qtypes.JAGGED_SHAPE, jagged_shape.create_shape()),
    (
        _EXT_TYPE,
        extension_type_registry.wrap(
            arolla.tuple(ds(None), ds(None)), _EXT_TYPE
        ),
    ),
)

# Sanity check.
assert isinstance(extension_type_registry._get_dummy_bag(), data_bag.DataBag)


class ExtensionTypeRegistryTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # The registry is a module-level global. Clear it to ensure test isolation.
    self.addCleanup(extension_type_registry._EXTENSION_TYPE_REGISTRY.clear)

  def test_is_extension_type(self):
    tpl = arolla.tuple(ds(1), ds(2))
    ext = arolla.eval(M.derived_qtype.downcast(_EXT_TYPE, tpl))
    # A raw Tuple is not an extension type.
    self.assertFalse(extension_type_registry.is_koda_extension(tpl))
    # The extension type is not yet registered, so it's false even if it has
    # an ok form.
    self.assertFalse(extension_type_registry.is_koda_extension(ext))
    # Is an extension type after registering.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    self.assertTrue(extension_type_registry.is_koda_extension(ext))

  def test_wrap_unwrap(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    tpl = arolla.tuple(ds(1), ds(2))
    wrapped_tpl = extension_type_registry.wrap(tpl, _EXT_TYPE)
    self.assertEqual(wrapped_tpl.qtype, _EXT_TYPE)
    unwrapped_tpl = extension_type_registry.unwrap(wrapped_tpl)
    self.assertIsInstance(unwrapped_tpl, arolla.types.Tuple)
    testing.assert_equal(unwrapped_tpl, tpl)

  def test_wrap_invalid_input(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected tuple<DATA_SLICE,DATA_SLICE>, got value: INT32'),
    ):
      extension_type_registry.wrap(123, _EXT_TYPE)
    with self.assertRaisesRegex(
        ValueError,
        'there is no registered extension type corresponding to the QType'
        ' INT32',
    ):
      extension_type_registry.wrap(ds([1, 2, 3]), arolla.INT32)

  def test_wrap_not_registered(self):
    nt = arolla.namedtuple(x=ds(1), y=ds(2))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'there is no registered extension type corresponding to the QType'
            ' LABEL[_MyTestExtension]'
        ),
    ):
      extension_type_registry.wrap(nt, _EXT_TYPE)

  def test_unwrap_not_registered(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    tpl = arolla.tuple(ds(1), ds(2))
    ext = extension_type_registry.wrap(tpl, _EXT_TYPE)
    extension_type_registry._EXTENSION_TYPE_REGISTRY.clear()
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      extension_type_registry.unwrap(ext)

  def test_unwrap_invalid_input(self):
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      extension_type_registry.unwrap(ds([1, 2, 3]))

  def test_registry(self):
    # Before registration.
    self.assertFalse(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )
    with self.assertRaisesRegex(
        ValueError, 'is not a registered extension type'
    ):
      extension_type_registry.get_extension_qtype(_MyTestExtension)

    # Successful registration.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)
    self.assertTrue(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )
    self.assertEqual(
        extension_type_registry.get_extension_qtype(_MyTestExtension), _EXT_TYPE
    )

    # Re-registering the same is fine.
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)

    # Test error cases.
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      extension_type_registry.register_extension_type(
          _MyOtherTestExtension, arolla.INT32
      )

    other_qtype = M.derived_qtype.get_labeled_qtype(
        arolla.make_namedtuple_qtype(x=qtypes.DATA_SLICE, y=qtypes.DATA_SLICE),
        'bar',
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
    # It deregisters the old type.
    self.assertFalse(
        extension_type_registry.is_koda_extension_type(_MyTestExtension)
    )

  def test_get_dummy_value_empty(self):

    class MyExtensionType:
      pass

    dummy_qtype = M.derived_qtype.get_labeled_qtype(
        arolla.make_tuple_qtype(),
        'MyExtensionType',
    ).qvalue
    extension_type_registry.register_extension_type(
        MyExtensionType, dummy_qtype
    )
    res = extension_type_registry.get_dummy_value(MyExtensionType)
    testing.assert_equal(
        res,
        extension_type_registry.wrap(
            arolla.tuple(),
            extension_type_registry.get_extension_qtype(MyExtensionType),
        ),
    )

  @parameterized.parameters(*_DUMMY_VALUES)
  def test_get_dummy_value_non_empty(self, x_qtype, expected_value):

    class MyExtensionType:
      pass

    dummy_qtype = M.derived_qtype.get_labeled_qtype(
        arolla.make_tuple_qtype(x_qtype, qtypes.DATA_SLICE),
        'MyExtensionType',
    ).qvalue
    extension_type_registry.register_extension_type(
        MyExtensionType, dummy_qtype
    )
    extension_type_registry.register_extension_type(_MyTestExtension, _EXT_TYPE)

    res = extension_type_registry.get_dummy_value(MyExtensionType)
    testing.assert_equal(
        res,
        extension_type_registry.wrap(
            arolla.tuple(expected_value, ds(None)),
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


if __name__ == '__main__':
  absltest.main()
