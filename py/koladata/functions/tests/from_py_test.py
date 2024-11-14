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

import dataclasses
import gc
import sys
from unittest import mock

from absl.testing import absltest
from koladata.exceptions import exceptions
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


@dataclasses.dataclass
class NestedKlass:
  x: str


@dataclasses.dataclass
class TestKlass:
  a: int
  b: NestedKlass
  c: bytes


@dataclasses.dataclass
class TestKlassInternals:
  a: int
  b: float


class FromPyTest(absltest.TestCase):

  # More detailed tests for conversions to Koda OBJECT are located in
  # obj_test.py.
  def test_object(self):
    obj = fns.from_py({'a': {'b': [1, 2, 3]}})
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a']))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(values, ds(['b']))
    nested_values = values['b']
    testing.assert_equal(
        nested_values.get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        nested_values[:], ds([1, 2, 3]).with_bag(obj.get_bag())
    )

  def test_list(self):
    l = fns.from_py([1, 2, 3])
    testing.assert_equal(l[:].no_bag(), ds([1, 2, 3]))

  # More detailed tests for conversions to Koda Entities for Lists are located
  # in new_test.py.
  def test_list_with_schema(self):
    # Python list items can be various Python / Koda objects that are normalized
    # to Koda Items.
    l = fns.from_py([1, 2, 3], schema=fns.list_schema(schema_constants.FLOAT32))
    testing.assert_allclose(l[:].no_bag(), ds([1.0, 2.0, 3.0]))

    l = fns.from_py(
        [[1, 2], [ds(42, schema_constants.INT64)]],
        schema=fns.list_schema(fns.list_schema(schema_constants.FLOAT64)),
    )
    testing.assert_allclose(
        l[:][:].no_bag(), ds([[1.0, 2.0], [42.0]], schema_constants.FLOAT64)
    )

    # TODO: this should return OBJECTs, not FLOAT32s.
    l = fns.from_py([1, 3.14], schema=schema_constants.OBJECT)
    testing.assert_equal(
        l[:].no_bag(), ds([1.0, 3.14], schema_constants.FLOAT32)
    )

  # More detailed tests for conversions to Koda Entities for Dicts are located
  # in new_test.py.
  def test_dict_with_schema(self):
    # Python dictionary keys and values can be various Python / Koda objects
    # that are normalized to Koda Items.
    d = fns.from_py(
        {ds('a'): [1, 2], 'b': [42]},
        schema=fns.dict_schema(
            schema_constants.STRING, fns.list_schema(schema_constants.INT32)
        ),
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(d[ds(['a', 'b'])][:].no_bag(), ds([[1, 2], [42]]))

  def test_primitive(self):
    item = fns.from_py(42)
    testing.assert_equal(item, ds(42))
    item = fns.from_py(42, schema=schema_constants.FLOAT32)
    testing.assert_equal(item, ds(42.))
    # TODO: this should return OBJECTs, not FLOAT32s.
    item = fns.from_py(42, schema=schema_constants.OBJECT)
    testing.assert_equal(item, ds(42, schema_constants.INT32))

  def test_primitive_casting_error(self):
    with self.assertRaisesRegex(ValueError, 'cannot cast BYTES to FLOAT32'):
      fns.from_py(b'xyz', schema=schema_constants.FLOAT32)

  def test_list_from_dim(self):
    input_list = [[1, 2], [3, 4]]

    l0 = fns.from_py(input_list, from_dim=0)
    self.assertEqual(l0.get_ndim(), 0)
    testing.assert_equal(l0[:][:].no_bag(), ds([[1, 2], [3, 4]]))

    l1 = fns.from_py(input_list, from_dim=1)
    self.assertEqual(l1.get_ndim(), 1)
    testing.assert_equal(l1[:].no_bag(), ds([[1, 2], [3, 4]]))

    # TODO(b/378029690) this should not have a DataBag.
    l2 = fns.from_py(input_list, from_dim=2)
    self.assertEqual(l2.get_ndim(), 2)
    testing.assert_equal(l2.no_bag(), ds([[1, 2], [3, 4]]))

  def test_list_from_dim_with_schema(self):
    input_list = [[1, 2], [3, 4]]

    l0 = fns.from_py(
        input_list,
        schema=fns.list_schema(fns.list_schema(schema_constants.FLOAT64)),
        from_dim=0,
    )
    self.assertEqual(l0.get_ndim(), 0)
    testing.assert_equal(
        l0[:][:].no_bag(), ds([[1, 2], [3, 4]], schema_constants.FLOAT64)
    )

    l1 = fns.from_py(
        input_list, schema=fns.list_schema(schema_constants.FLOAT32), from_dim=1
    )
    self.assertEqual(l1.get_ndim(), 1)
    testing.assert_equal(
        l1[:].no_bag(), ds([[1, 2], [3, 4]], schema_constants.FLOAT32)
    )

    lst = fns.from_py([1, 3.14], from_dim=1, schema=schema_constants.INT32)
    testing.assert_equal(lst.no_bag(), ds([1, 3], schema_constants.INT32))

    # TODO(b/378029690) this should not have a DataBag.
    l2 = fns.from_py(input_list, schema=schema_constants.FLOAT64, from_dim=2)
    self.assertEqual(l2.get_ndim(), 2)
    testing.assert_equal(
        l2.no_bag(), ds([[1, 2], [3, 4]], schema_constants.FLOAT64)
    )

  def test_dict_from_dim(self):
    input_dict = [{ds('a'): [1, 2], 'b': [42]}, {ds('c'): [3, 4], 'd': [34]}]

    d0 = fns.from_py(input_dict, from_dim=0)
    inner_slice = d0[:]
    testing.assert_dicts_keys_equal(inner_slice, ds([['a', 'b'], ['c', 'd']]))
    testing.assert_equal(
        inner_slice[ds([['a', 'b'], ['c', 'd']])][:].no_bag(),
        ds([[[1, 2], [42]], [[3, 4], [34]]]),
    )

    d1 = fns.from_py(input_dict, from_dim=1)
    testing.assert_dicts_keys_equal(d1, ds([['a', 'b'], ['c', 'd']]))
    testing.assert_equal(
        d1[ds([['a', 'b'], ['c', 'd']])][:].no_bag(),
        ds([[[1, 2], [42]], [[3, 4], [34]]]),
    )

  def test_from_dim_error(self):
    input_list = [[1, 2, 3], 4]

    l0 = fns.from_py(input_list, from_dim=0)
    self.assertEqual(l0.get_ndim(), 0)

    l1 = fns.from_py(input_list, from_dim=1)
    self.assertEqual(l1.get_ndim(), 1)

    with self.assertRaisesRegex(
        ValueError,
        'input has to be a valid nested list. non-lists and lists cannot be'
        ' mixed in a level',
    ):
      _ = fns.from_py(input_list, from_dim=2)

  def test_none(self):
    item = fns.from_py(None)
    testing.assert_equal(item, ds(None))
    item = fns.from_py(None, schema=schema_constants.FLOAT32)
    testing.assert_equal(item, ds(None, schema_constants.FLOAT32))
    schema = fns.schema.new_schema(
        a=schema_constants.STRING, b=fns.list_schema(schema_constants.INT32)
    )
    item = fns.from_py(None, schema=schema)
    testing.assert_equivalent(item.get_schema(), schema)
    testing.assert_equal(item.no_bag(), ds(None).with_schema(schema.no_bag()))

  def test_obj_reference(self):
    obj = fns.obj()
    item = fns.from_py(obj.ref())
    testing.assert_equal(item, obj.no_bag())

  def test_entity_reference(self):
    entity = fns.new(x=42)
    item = fns.from_py(entity.ref())
    self.assertIsNotNone(item.get_bag())
    testing.assert_equal(
        item.get_attr('__schema__').no_bag(), entity.get_schema().no_bag()
    )
    testing.assert_equal(
        item.with_schema(entity.get_schema().no_bag()).no_bag(), entity.no_bag()
    )

    item = fns.from_py(entity.ref(), schema=entity.get_schema())
    testing.assert_equal(item.no_bag(), entity.no_bag())
    # no data triples (item.x => 42) in the DataBag.
    testing.assert_equivalent(item.get_schema(), entity.get_schema().extract())
    testing.assert_equal(item.x.no_bag(), ds(None, schema_constants.INT32))

  def test_dict_as_obj_object(self):
    obj = fns.from_py(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
    )
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    self.assertCountEqual(dir(obj), ['a', 'b', 'c'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    b = obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(obj.c.no_bag(), ds(b'xyz'))

  def test_dict_as_obj_entity_with_schema(self):
    schema = fns.schema.new_schema(
        a=schema_constants.FLOAT32,
        b=fns.schema.new_schema(x=schema_constants.STRING),
        c=schema_constants.BYTES,
    )
    entity = fns.from_py(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['a', 'b', 'c'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))
    b = entity.b
    testing.assert_equal(b.get_schema().no_bag(), schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))

  def test_dict_as_obj_entity_with_nested_object(self):
    schema = fns.schema.new_schema(
        a=schema_constants.INT64,
        b=schema_constants.OBJECT,
        c=schema_constants.BYTES,
    )
    entity = fns.from_py(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['a', 'b', 'c'])
    testing.assert_equal(entity.a.no_bag(), ds(42, schema_constants.INT64))
    obj_b = entity.b
    testing.assert_equal(obj_b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(obj_b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))

  def test_dict_as_obj_entity_incomplete_schema(self):
    schema = fns.schema.new_schema(b=schema_constants.OBJECT)
    entity = fns.from_py(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['b'])
    testing.assert_equal(
        entity.b.get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(entity.b.x.no_bag(), ds('abc'))

  def test_dict_as_obj_entity_empty_schema(self):
    schema = fns.schema.new_schema()
    entity = fns.from_py(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), [])

  def test_dict_as_obj_bag_adoption(self):
    obj_b = fns.from_py({'x': 'abc'}, dict_as_obj=True)
    obj = fns.from_py({'a': 42, 'b': obj_b}, dict_as_obj=True)
    testing.assert_equal(obj.b.x.no_bag(), ds('abc'))

  def test_dict_as_obj_entity_incompatible_schema(self):
    schema = fns.schema.new_schema(
        a=schema_constants.INT64,
        b=fns.schema.new_schema(x=schema_constants.FLOAT32),
        c=schema_constants.FLOAT32,
    )
    with self.assertRaisesRegex(
        exceptions.KodaError, 'schema for attribute \'x\' is incompatible'
    ):
      fns.from_py(
          {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')}, dict_as_obj=True,
          schema=schema,
      )

  def test_dict_as_obj_dict_key_is_data_item(self):
    # Object.
    obj = fns.from_py({ds('a'): 42}, dict_as_obj=True)
    self.assertCountEqual(dir(obj), ['a'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    # Entity - non STRING schema with STRING item.
    entity = fns.from_py(
        {ds('a').as_any(): 42},
        dict_as_obj=True,
        schema=fns.schema.new_schema(a=schema_constants.INT32),
    )
    self.assertCountEqual(dir(entity), ['a'])
    testing.assert_equal(entity.a.no_bag(), ds(42))

  def test_dict_as_obj_non_unicode_key(self):
    with self.assertRaisesRegex(
        ValueError,
        'dict_as_obj requires keys to be valid unicode objects, got bytes'
    ):
      fns.from_py({b'xyz': 42}, dict_as_obj=True)

  def test_dict_as_obj_non_text_data_item(self):
    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      fns.from_py({ds(['abc']): 42}, dict_as_obj=True)
    with self.assertRaisesRegex(
        ValueError, "dict keys cannot be non-STRING DataItems, got b'abc'"
    ):
      fns.from_py({ds(b'abc'): 42}, dict_as_obj=True)

  def test_dataclasses(self):
    obj = fns.from_py(TestKlass(42, NestedKlass('abc'), b'xyz'))
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    self.assertCountEqual(dir(obj), ['a', 'b', 'c'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    b = obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(obj.c.no_bag(), ds(b'xyz'))

  def test_dataclasses_with_schema(self):
    schema = fns.schema.new_schema(
        a=schema_constants.FLOAT32,
        b=fns.schema.new_schema(x=schema_constants.STRING),
        c=schema_constants.BYTES,
    )
    entity = fns.from_py(
        TestKlass(42, NestedKlass('abc'), b'xyz'), schema=schema
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['a', 'b', 'c'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))
    b = entity.b
    testing.assert_equal(b.get_schema().no_bag(), schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))

  def test_dataclasses_with_incomplete_schema(self):
    schema = fns.schema.new_schema(
        a=schema_constants.FLOAT32,
    )
    entity = fns.from_py(
        TestKlass(42, NestedKlass('abc'), b'xyz'), schema=schema
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['a'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))

  def test_list_of_dataclasses(self):
    obj = fns.from_py([NestedKlass('a'), NestedKlass('b')])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    nested = obj[:]
    testing.assert_equal(nested.S[0].x.no_bag(), ds('a'))
    testing.assert_equal(nested.S[1].x.no_bag(), ds('b'))

  def test_dataclass_with_list(self):
    @dataclasses.dataclass
    class Test:
      l: list[int]

    obj = fns.from_py(Test([1, 2, 3]))
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(obj.l[:].no_bag(), ds([1, 2, 3]))

  def test_dataclass_with_koda_obj(self):
    @dataclasses.dataclass
    class Test:
      koda: data_slice.DataSlice

    schema = fns.schema.new_schema(
        koda=fns.schema.new_schema(x=schema_constants.INT32)
    )
    entity = fns.from_py(Test(schema.koda(x=1)), schema=schema)
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(dir(entity), ['koda'])
    koda = entity.koda
    testing.assert_equal(koda.get_schema().no_bag(), schema.koda.no_bag())
    testing.assert_equal(koda.x.no_bag(), ds(1))

  def test_dataclasses_prevent_memory_leak(self):
    gc.collect()
    base_count = sys.getrefcount(42)
    for _ in range(10):
      fns.from_py(TestKlassInternals(42, 3.14))
    gc.collect()
    self.assertEqual(base_count, sys.getrefcount(42))

  def test_dataclasses_errors(self):
    with mock.patch.object(dataclasses, 'fields', return_value=[1, 2]):
      with self.assertRaisesRegex(ValueError, 'expected to return a tuple'):
        fns.from_py(TestKlassInternals(42, 3.14))
    with mock.patch.object(dataclasses, 'fields', raises=ValueError('fields')):
      with self.assertRaisesRegex(ValueError, 'fields'):
        fns.from_py(TestKlassInternals(42, 3.14))
    with mock.patch.object(dataclasses, 'fields', return_value=(1, 2)):
      with self.assertRaisesRegex(AttributeError, 'has no attribute \'name\''):
        fns.from_py(TestKlassInternals(42, 3.14))

  def test_dataclasses_field_attribute_error(self):
    class TestField:

      def __init__(self, val):
        self._val = val

      @property
      def name(self):
        return 'non_existent'

    with mock.patch.object(
        dataclasses, 'fields', return_value=(TestField(42), TestField(3.14))
    ):
      with self.assertRaisesRegex(
          AttributeError, 'has no attribute \'non_existent\''
      ):
        fns.from_py(TestKlassInternals(42, 3.14))

  def test_dataclasses_field_invalid_name_error(self):
    class TestField:

      def __init__(self, val):
        self._val = val

      @property
      def name(self):
        return b'non_existent'

    with mock.patch.object(
        dataclasses, 'fields', return_value=(TestField(42), TestField(3.14))
    ):
      with self.assertRaisesRegex(ValueError, 'invalid unicode object'):
        fns.from_py(TestKlassInternals(42, 3.14))

  def test_alias(self):
    obj = fns.from_pytree({'a': 42})
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a']))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_bag(), schema_constants.INT32)
    testing.assert_equal(values, ds(42).with_bag(values.get_bag()))

  def test_not_yet_implemented(self):
    with self.subTest('itemid'):
      with self.assertRaises(NotImplementedError):
        fns.from_py({'a': {'b': [1, 2, 3]}}, itemid=kde.uuid(a=1).eval())

  def test_arg_errors(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.from_py([1, 2], schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting dict_as_obj to be a bool, got int'
    ):
      fns.from_py([1, 2], dict_as_obj=42)  # pytype: disable=wrong-arg-types

  # pylint: disable=protected-access
  # TODO: Migrate these to test_arg_errors.
  def test_internal_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, '_from_py_impl accepts exactly 5 arguments, got 2'
    ):
      fns.bag()._from_py_impl([1, 2], 42)
    with self.assertRaisesRegex(
        TypeError, 'expecting itemid to be a DataSlice, got int'
    ):
      fns.bag()._from_py_impl([1, 2], False, 42, fns.schema.new_schema(), 0)
    with self.assertRaisesRegex(
        TypeError, 'expecting from_dim to be an int, got str'
    ):
      fns.bag()._from_py_impl(
          [1, 2], False, fns.new(), fns.schema.new_schema(), 'abc'
      )
    with self.assertRaisesRegex(OverflowError, 'Python int too large'):
      fns.bag()._from_py_impl(
          [1, 2], False, fns.new(), fns.schema.new_schema(), 1 << 100
      )
  # pylint: enable=protected-access


if __name__ == '__main__':
  absltest.main()
