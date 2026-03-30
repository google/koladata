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
import types as _py_types
from typing import Any, Optional

from absl.testing import absltest
from koladata.base.py_conversions import testing_clib
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


@dataclasses.dataclass(frozen=True)
class Obj1:
  a: int = 123
  b: str = 'abc'


class DataclassesUtilTest(absltest.TestCase):

  def test_make_empty_dataclass(self):
    util = testing_clib.DataClassesUtil()
    obj = util.make_dataclass_instance([])
    self.assertEqual(dataclasses.asdict(obj), {})

  def test_make_dataclass_with_fields(self):
    util = testing_clib.DataClassesUtil()
    obj = util.make_dataclass_instance(['b', 'c', 'a'])
    # Attributes are sorted alphabetically.
    self.assertEqual(dataclasses.asdict(obj), {'a': None, 'b': None, 'c': None})

  def test_make_empty_dataclass_repeated_fields(self):
    util = testing_clib.DataClassesUtil()
    with self.assertRaisesRegex(ValueError, 'could not create a new dataclass'):
      _ = util.make_dataclass_instance(['a', 'a'])

  def test_make_empty_dataclass_empty_field_name(self):
    util = testing_clib.DataClassesUtil()
    with self.assertRaisesRegex(ValueError, 'could not create a new dataclass'):
      _ = util.make_dataclass_instance([''])

  def test_make_dataclass_from_different_instances(self):
    util1 = testing_clib.DataClassesUtil()
    util2 = testing_clib.DataClassesUtil()
    obj1 = util1.make_dataclass_instance(['a'])
    obj2 = util2.make_dataclass_instance(['a'])
    self.assertEqual(obj1, obj2)
    self.assertNotEqual(obj1.__class__, obj2.__class__)

  def test_make_dataclass_caches_classes(self):
    util = testing_clib.DataClassesUtil()
    obj1 = util.make_dataclass_instance(['a'])
    obj2 = util.make_dataclass_instance(['a'])
    obj3 = util.make_dataclass_instance(['b'])
    self.assertEqual(obj1, obj2)
    self.assertEqual(obj1.__class__, obj2.__class__)
    self.assertNotEqual(obj1, obj3)
    self.assertNotEqual(obj1.__class__, obj3.__class__)

  def test_get_attr_values(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class TestDataclass:
      a: int
      b: int
      c: int

    obj = TestDataclass(a=1, b=2, c=3)

    self.assertEqual(util.get_attr_values(obj, ['a', 'b', 'c']), [1, 2, 3])
    self.assertEqual(util.get_attr_values(obj, ['c', 'b', 'a']), [3, 2, 1])

  def test_get_attr_values_fails_for_non_existing_attr(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class TestDataclass:
      a: int
      b: int

    obj = TestDataclass(a=1, b=2)

    with self.assertRaisesRegex(AttributeError, 'object has no attribute'):
      _ = util.get_attr_values(obj, ['a', 'b', 'c'])

    with self.assertRaisesRegex(AttributeError, 'object has no attribute'):
      _ = util.get_attr_values(1, ['a', 'b', 'c'])

  def test_get_class_field_type(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: Obj1

    self.assertEqual(util.get_class_field_type(Obj2, 'a', False), Obj1)
    self.assertIsNone(util.get_class_field_type(Obj2, 'b', False))
    self.assertIsNone(util.get_class_field_type(Obj2, 'b', True))

  def test_get_class_field_type_optional(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: Obj1 | None
      b: Optional[Obj1]

    self.assertEqual(util.get_class_field_type(Obj2, 'a', False), Obj1)
    self.assertEqual(util.get_class_field_type(Obj2, 'b', False), Obj1)

  def test_get_class_field_list_tuple(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: list[Obj1]
      b: tuple[Obj1, ...]

    list_type = util.get_class_field_type(Obj2, 'a', False)
    tuple_type = util.get_class_field_type(Obj2, 'b', False)
    self.assertEqual(list_type, list[Obj1])
    self.assertEqual(tuple_type, tuple[Obj1, ...])
    self.assertEqual(
        util.get_class_field_type(list_type, '__items__', False), Obj1
    )
    with self.assertRaisesRegex(ValueError, 'only tuple.T, .... is supported'):
      _ = util.get_class_field_type(tuple[Obj1, Obj1], '__items__', False)
    with self.assertRaisesRegex(ValueError, 'only tuple.T, .... is supported'):
      _ = util.get_class_field_type(tuple[Obj1], '__items__', False)

  def test_get_class_field_dict(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: dict[Obj1, _py_types.SimpleNamespace]

    dict_type = util.get_class_field_type(Obj2, 'a', False)
    self.assertEqual(dict_type, dict[Obj1, _py_types.SimpleNamespace])
    self.assertEqual(
        util.get_class_field_type(dict_type, '__keys__', False), Obj1
    )
    self.assertEqual(
        util.get_class_field_type(dict_type, '__values__', False),
        _py_types.SimpleNamespace,
    )

  def test_get_class_field_type_simple_namespace(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: _py_types.SimpleNamespace

    self.assertEqual(
        util.get_class_field_type(_py_types.SimpleNamespace, 'any_attr', False),
        _py_types.SimpleNamespace,
    )
    self.assertEqual(
        util.get_class_field_type(Obj2, 'a', False), _py_types.SimpleNamespace
    )

  def test_get_class_field_typefor_primitive(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: int
      b: Obj1

    self.assertEqual(util.get_class_field_type(Obj2, 'a', True), int)
    self.assertEqual(util.get_class_field_type(Obj2, 'b', True), Obj1)

    self.assertIsNone(
        util.get_class_field_type(_py_types.SimpleNamespace, 'a', True), None
    )

  def test_get_class_field_type_errors(self):
    util = testing_clib.DataClassesUtil()
    self.assertIsNone(util.get_class_field_type(Obj1, 'x', False))

    with self.assertRaisesRegex(ValueError, "field 'a' has unsupported type"):
      _ = util.get_class_field_type(Obj1, 'a', False)
    with self.assertRaisesRegex(
        ValueError, 'only dataclasses or SimpleNamespace are supported'
    ):
      _ = util.get_class_field_type(int, 'a', False)

    with self.assertRaisesRegex(
        ValueError, 'expected dict class; got instead: list.int.'
    ):
      _ = util.get_class_field_type(list[int], '__keys__', False)

    with self.assertRaisesRegex(
        ValueError, 'expected dict class; got instead: list.int.'
    ):
      _ = util.get_class_field_type(list[int], '__values__', False)

    with self.assertRaisesRegex(
        ValueError, 'expected list class; got instead: dict.int. int.'
    ):
      _ = util.get_class_field_type(dict[int, int], '__items__', False)

  def test_has_optional_field(self):
    util = testing_clib.DataClassesUtil()

    @dataclasses.dataclass
    class Obj2:
      a: Obj1 | None
      c: Optional[Obj1]
      d: int
      e: None
      f: Any
      bad_0: int | Any
      bad_1: int | float | None
      bad_2: None | int
      g: int = 1

    self.assertTrue(util.has_optional_field(Obj2, 'a'))
    self.assertFalse(util.has_optional_field(Obj2, 'b'))
    self.assertTrue(util.has_optional_field(Obj2, 'c'))
    with self.assertRaisesRegex(
        ValueError,
        'field cannot have missing values: d',
    ):
      _ = util.has_optional_field(Obj2, 'd')
    with self.assertRaisesRegex(
        ValueError,
        'field cannot have missing values: e',
    ):
      _ = util.has_optional_field(Obj2, 'e')
    with self.assertRaisesRegex(
        ValueError,
        'field cannot have missing values: f',
    ):
      _ = util.has_optional_field(Obj2, 'f')
    with self.assertRaisesRegex(
        ValueError,
        'field cannot have missing values: g',
    ):
      _ = util.has_optional_field(Obj2, 'g')
    self.assertFalse(util.has_optional_field(int, 'non_existent_field'))
    with self.assertRaisesRegex(
        ValueError,
        'only unions `SomeType | None` are supported ; got instead: int |'
        ' typing.Any',
    ):
      _ = util.has_optional_field(Obj2, 'bad_0')

    with self.assertRaisesRegex(
        ValueError,
        'only unions `SomeType | None` are supported ; got instead: int | float'
        ' | None',
    ):
      _ = util.has_optional_field(Obj2, 'bad_1')
    with self.assertRaisesRegex(
        ValueError,
        'only unions `SomeType | None` are supported ; got instead: int | float'
        ' | None',
    ):
      _ = util.has_optional_field(Obj2, 'bad_2')

  def test_create_class_instance_kwargs(self):
    util = testing_clib.DataClassesUtil()
    obj = util.create_class_instance_kwargs(Obj1, ['a', 'b'], [123, 'abc'])
    self.assertEqual(obj, Obj1(a=123, b='abc'))

  def test_get_simple_namespace_class(self):
    util = testing_clib.DataClassesUtil()
    simple_namespace_class = util.get_simple_namespace_class()
    self.assertEqual(simple_namespace_class, _py_types.SimpleNamespace)


if __name__ == '__main__':
  absltest.main()
