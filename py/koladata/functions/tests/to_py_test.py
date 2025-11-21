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
import gc
import importlib
import math
import sys
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.functions import object_factories
from koladata.functions import py_conversions
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


def mutable_obj():
  return object_factories.mutable_bag().obj()


class ToPyTest(parameterized.TestCase):

  def test_empty_slice(self):
    self.assertEqual(py_conversions.to_py(ds([])), [])

  def test_multi_dimensional_slice(self):
    self.assertEqual(
        py_conversions.to_py(
            ds([
                [fns.list([1, 2]), fns.list([3, 4])],
                [fns.list([5, 6]), fns.list([7, 8])],
            ]),
        ),
        [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
    )

  def test_list(self):
    root = mutable_obj()
    root.none_list_value = []
    root.none_list_value.append(None)
    with self.assertRaisesRegex(
        ValueError, 'the schema for list items is incompatible'
    ):
      root.none_list_value.append(1)

    root.list_value = fns.list()
    root.list_value.append(1)
    root.list_value.append(2)
    nested_values = ds([[1, 2], [3, 4, 5]])
    root.nested_list = fns.implode(fns.implode(nested_values))
    root.empty_list = []

    @dataclasses.dataclass
    class Obj:
      list_value: list[int]
      nested_list: list[list[int]]
      empty_list: list[int]
      none_list_value: list[None]

    expected = Obj(
        list_value=[1, 2],
        nested_list=[[1, 2], [3, 4, 5]],
        empty_list=[],
        none_list_value=[None],
    )

    py_obj = py_conversions.to_py(root, max_depth=4)

    self.assertEqual(expected, py_obj)
    self.assertEqual(py_obj, expected)

  def test_list_object_ownership(self):
    entity = fns.new(a=1, b='x')
    lst = fns.list([entity, entity, entity]).to_py()
    gc.collect()
    # Check that list elements references are still valid.
    self.assertEqual(lst[0], lst[1])
    self.assertEqual(lst[0], lst[2])
    self.assertEqual(lst[1], lst[2])

  def test_same_list(self):
    lst = fns.list([1, 2, 3])
    obj = fns.obj(a=lst, b=lst)
    res = obj.to_py(obj_as_dict=True)
    self.assertEqual(
        res, {'a': [1, 2, 3], 'b': [1, 2, 3]}
    )
    self.assertEqual(id(res['a']), id(res['b']))
    res = py_conversions.to_py(obj)
    self.assertEqual(id(res.a), id(res.b))

  def test_same_objects_are_cached(self):
    obj = fns.obj(x=123)
    lst = fns.list([1, 2, 3])
    d = fns.dict({'a': 1, 'b': 2})
    primitive = 'abc'
    obj = fns.obj(
        o1=obj, o2=obj, l1=lst, l2=lst, d1=d, d2=d, p1=primitive, p2=primitive
    )
    res = py_conversions.to_py(obj)
    self.assertEqual(id(res.o1), id(res.o2))
    self.assertEqual(id(res.l1), id(res.l2))
    self.assertEqual(id(res.d1), id(res.d2))
    self.assertNotEqual(id(res.p1), id(res.p2))

  def test_dict(self):
    root = mutable_obj()
    root.dict_value = fns.dict()
    root.dict_value['key_1'] = 'value_1'
    root.dict_value['key_2'] = 'value_2'
    root.empty_dict = {}

    @dataclasses.dataclass
    class Obj:
      dict_value: dict[str, str]
      empty_dict: dict[str, str]

    expected = Obj(
        dict_value={'key_1': 'value_1', 'key_2': 'value_2'},
        empty_dict={},
    )

    py_obj = py_conversions.to_py(root, max_depth=-1)

    self.assertEqual(expected, py_obj)
    self.assertEqual(py_obj, expected)

  def test_dict_with_obj_keys(self):
    root = mutable_obj()
    root.dict_value = fns.dict()
    k1 = fns.obj(a=1)
    k2 = fns.list([1, 2])
    k3 = fns.new(b=2)
    k4 = fns.dict({'c': 3})
    root.dict_value[k1] = 1
    root.dict_value[k2] = 2
    root.dict_value[k3] = 3
    root.dict_value[k4] = 4
    py_obj = py_conversions.to_py(root, max_depth=4)

    root_keys = root.dict_value.get_keys()
    py_obj_keys = ds(list(py_obj.dict_value.keys())).with_bag(root.get_bag())
    testing.assert_unordered_equal(root_keys, py_obj_keys)

    self.assertEqual(py_obj.dict_value[k1.no_bag()], 1)
    self.assertEqual(
        py_obj.dict_value[k2.no_bag().with_schema(schema_constants.OBJECT)], 2
    )
    self.assertEqual(
        py_obj.dict_value[k3.no_bag().with_schema(schema_constants.OBJECT)], 3
    )
    self.assertEqual(
        py_obj.dict_value[k4.no_bag().with_schema(schema_constants.OBJECT)], 4
    )

  def test_dict_with_obj_keys_with_schema(self):
    root = mutable_obj()
    schema = root.get_bag().new_schema(a=schema_constants.INT32)

    k1 = fns.new(a=1, schema=schema)
    k2 = fns.new(a=2, schema=schema)
    root.dict_value = fns.dict(key_schema=k1.get_schema())

    root.dict_value[k1] = 1
    root.dict_value[k2] = 2
    py_obj = py_conversions.to_py(root, max_depth=4)

    root_keys = root.dict_value.get_keys()
    py_obj_keys = ds(list(py_obj.dict_value.keys())).with_bag(root.get_bag())
    testing.assert_unordered_equal(root_keys, py_obj_keys)

    self.assertEqual(py_obj.dict_value[k1.no_bag()], 1)
    self.assertEqual(py_obj.dict_value[k2.no_bag()], 2)

  def test_dict_with_zero_code_key(self):
    root = fns.obj(**{'a\0b': 1})
    py_obj = py_conversions.to_py(root, obj_as_dict=True)
    self.assertEqual(py_obj['a\0b'], 1)

    root = fns.dict({'a\0b': 1})
    py_obj = py_conversions.to_py(root, obj_as_dict=True)
    self.assertEqual(py_obj['a\0b'], 1)

  def test_list_obj(self):
    self.assertEqual(py_conversions.to_py(fns.obj([1, 2])), [1, 2])

  def test_dict_obj(self):
    self.assertEqual(py_conversions.to_py(fns.obj({1: 2})), {1: 2})

  def test_fallbacks(self):
    x = fns.new(x=1)
    fallback_bag = object_factories.mutable_bag()
    fallback_bag[x].set_attr('y', 'abc')
    x = x.enriched(fallback_bag)
    self.assertEqual(
        py_conversions.to_py(x, obj_as_dict=True), {'x': 1, 'y': 'abc'}
    )

    @dataclasses.dataclass
    class Obj:
      x: int
      y: str

    self.assertEqual(
        py_conversions.to_py(x, obj_as_dict=False), Obj(x=1, y='abc')
    )

  def test_self_reference_object(self):
    with self.subTest('in_attribute'):
      root = mutable_obj()
      root.x = fns.obj()
      root.x.y = root
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj.x.y), id(py_obj))
      py_obj = py_conversions.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['x']['y']), id(py_obj))

    with self.subTest('in_dict'):
      root = mutable_obj()
      root.x = fns.dict({'y': root})
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj.x['y']), id(py_obj))
      py_obj = py_conversions.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['x']['y']), id(py_obj))

    with self.subTest('in_list'):
      root = mutable_obj()
      root.a = fns.list([root])
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj.a[0]), id(py_obj))
      py_obj = py_conversions.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['a'][0]), id(py_obj))

  def test_self_reference_dict(self):
    with self.subTest('in_attribute'):
      root = fns.dict().fork_bag()
      root['a'] = fns.obj(b=root)
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj['a'].b), id(py_obj))
      py_obj = py_conversions.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['a']['b']), id(py_obj))

    with self.subTest('in_list'):
      root = fns.dict().fork_bag()
      root['a'] = fns.list()
      root['a'].append(root)
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj['a'][0]), id(py_obj))

    with self.subTest('in_dict'):
      root = fns.dict().fork_bag()
      root['a'] = fns.dict({'b': root})
      py_obj = py_conversions.to_py(root)
      self.assertEqual(id(py_obj['a']['b']), id(py_obj))

  def test_self_reference_list_in_dict(self):
    with self.subTest('in_attribute'):
      x = fns.list().fork_bag()
      x.append(fns.obj(a=x))
      py_obj = py_conversions.to_py(x)
      self.assertEqual(id(py_obj[0].a), id(py_obj))
      py_obj = py_conversions.to_py(x, obj_as_dict=True)
      self.assertEqual(id(py_obj[0]['a']), id(py_obj))

    with self.subTest('in_list'):
      x = fns.list().fork_bag()
      x.append(fns.list([x]))
      py_obj = py_conversions.to_py(x)
      self.assertEqual(id(py_obj[0][0]), id(py_obj))

    with self.subTest('in_dict'):
      x = fns.list().fork_bag()
      x.append(fns.dict({'a': x}))
      py_obj = py_conversions.to_py(x)
      self.assertEqual(id(py_obj[0]['a']), id(py_obj))

  def test_multiple_references(self):
    x = mutable_obj()
    x.foo = [1, 2]
    x.bar = x.foo

    @dataclasses.dataclass
    class Obj:
      foo: list[int]
      bar: list[int]

    expected = Obj(foo=[1, 2], bar=[1, 2])

    py_obj = py_conversions.to_py(x)

    self.assertEqual(py_obj, expected)
    self.assertEqual(expected, py_obj)

  def test_recursion_beyond_depth(self):
    x = fns.new().with_attr('__qualname__', '<lambda>')
    y = fns.new(x=x).with_attr('__qualname__', '<lambda>')
    py_obj = fns.list([y]).to_py(max_depth=1)

    self.assertEqual(py_obj, [y])

  def test_slice_without_bag(self):
    s = ds([[1, 2], [3, 4, 5]])
    py_obj = py_conversions.to_py(s)
    expected = [[1, 2], [3, 4, 5]]
    self.assertEqual(expected, py_obj)

  def test_does_not_pollute_bag(self):
    root = mutable_obj()
    root.foo = fns.implode(fns.implode(ds([[1, 2], [3, 4, 5]])))
    old_data_repr = repr(root.get_bag())
    py_conversions.to_py(root.foo[:][:])
    self.assertEqual(old_data_repr, repr(root.get_bag()))

  def test_max_depth(self):
    x = fns.obj(
        a=fns.obj(x=fns.obj(y=123)),
        b=fns.list([12, 13]),
        c=fns.dict({14: 15}),
        d=11,
        e=fns.list([[1, 2], [3, 4]]),
        f=fns.new(g=fns.new(h=17)),
    )

    testing.assert_equal(py_conversions.to_py(x, max_depth=0), x)

    res = py_conversions.to_py(x, max_depth=1)
    testing.assert_equal(res.a, x.a)
    testing.assert_equal(res.b, x.b)
    testing.assert_equal(res.c, x.c)
    self.assertEqual(res.d, 11)
    testing.assert_equal(res.e, x.e)
    testing.assert_equal(res.f, x.f)

    res = py_conversions.to_py(x, max_depth=2)
    testing.assert_equal(res.a.x, x.a.x)
    self.assertEqual(res.b, [12, 13])
    self.assertEqual(res.c, {14: 15})
    self.assertEqual(res.d, 11)
    testing.assert_equal(res.e[0], x.e[0])
    testing.assert_equal(res.e[1], x.e[1])
    self.assertEqual(
        res.f, dataclasses.make_dataclass('Obj', [('g', Any)])(g=x.f.g)
    )
    testing.assert_equal(res.f.g, x.f.g)

    res = py_conversions.to_py(x, max_depth=2, obj_as_dict=True)
    testing.assert_equal(res['a']['x'], x.a.x)
    self.assertEqual(res['b'], [12, 13])
    self.assertEqual(res['c'], {14: 15})
    self.assertEqual(res['d'], 11)
    testing.assert_equal(res['e'][0], x.e[0])
    testing.assert_equal(res['e'][1], x.e[1])
    testing.assert_equal(res['f']['g'], x.f.g)

    res = py_conversions.to_py(x, max_depth=3)
    self.assertEqual(
        res.f.g, dataclasses.make_dataclass('Obj', [('h', Any)])(h=17)
    )

    self.assertEqual(
        py_conversions.to_py(x, max_depth=3, obj_as_dict=True),
        {
            'a': {'x': {'y': 123}},
            'b': [12, 13],
            'c': {14: 15},
            'd': 11,
            'e': [[1, 2], [3, 4]],
            'f': {'g': {'h': 17}},
        },
    )

    x3 = py_conversions.to_py(x, max_depth=3)
    x_full = py_conversions.to_py(x, max_depth=-1)
    self.assertEqual(x3, x_full)

    x = ds([[1, 2], [3, 4]])
    self.assertEqual(py_conversions.to_py(x, max_depth=0), [[1, 2], [3, 4]])

  def test_max_depth_on_slice(self):
    @dataclasses.dataclass
    class Obj0:
      x: Any

    @dataclasses.dataclass
    class Obj1:
      y: Any

    @dataclasses.dataclass
    class Obj2:
      z: Any

    o1_level2 = fns.obj(z=1)
    o2_level2 = fns.obj(z=2)

    o1_level1 = fns.obj(y=o1_level2)
    o2_level1 = fns.obj(y=o2_level2)

    o1 = fns.obj(x=o1_level1)
    o2 = fns.obj(x=o2_level1)
    x = ds([o1, o2])

    self.assertEqual(py_conversions.to_py(x, max_depth=0), [o1, o2])

    self.assertEqual(
        py_conversions.to_py(x, max_depth=1), [Obj0(o1_level1), Obj0(o2_level1)]
    )
    self.assertEqual(
        py_conversions.to_py(x, max_depth=2),
        [Obj0(Obj1(o1_level2)), Obj0(Obj1(o2_level2))],
    )
    self.assertEqual(
        py_conversions.to_py(x, max_depth=3),
        [Obj0(Obj1(Obj2(1))), Obj0(Obj1(Obj2(2)))],
    )

  def test_include_missing_attrs(self):
    p = kde.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    ).eval()
    x = ds([fns.new(x=1, y=2, schema=p), fns.new(x=3, schema=p)])
    self.assertEqual(
        py_conversions.to_py(x, obj_as_dict=True, include_missing_attrs=False),
        [{'x': 1, 'y': 2}, {'x': 3}],
    )

    self.assertEqual(
        py_conversions.to_py(x, obj_as_dict=True, include_missing_attrs=True),
        [{'x': 1, 'y': 2}, {'x': 3, 'y': None}],
    )

    self.assertIsNone(py_conversions.to_py(x, include_missing_attrs=False)[1].y)
    self.assertIsNone(py_conversions.to_py(x, include_missing_attrs=True)[1].y)

  @parameterized.named_parameters(
      ('int', 5),
      ('int list', [1, 2]),
      ('int two-dimensional list', [[1, 2], [3, 4]]),
      ('float', 1.0),
      ('inf', float('inf')),
      ('true', True),
      ('false', False),
      ('none', None),
      ('string', 'abc'),
      ('bytes', b'abc'),
  )
  def test_primitive_data(self, expected):
    self.assertEqual(py_conversions.to_py(ds(expected)), expected)

  def test_nan(self):
    self.assertTrue(math.isnan(py_conversions.to_py(ds(float('nan')))))

    res = py_conversions.to_py(fns.obj(x=float('nan')))
    self.assertTrue(math.isnan(res.x))

    res = py_conversions.to_py(fns.new(x=float('nan')))
    self.assertTrue(math.isnan(res.x))

    res = py_conversions.to_py(fns.obj(x=float('nan')), obj_as_dict=True)
    self.assertTrue(math.isnan(res['x']))

    res = py_conversions.to_py(fns.list([float('nan')]))
    self.assertTrue(math.isnan(res[0]))

    res = py_conversions.to_py(fns.dict({1: float('nan')}))
    self.assertTrue(math.isnan(res[1]))

  def test_empty_object(self):
    x = fns.obj()
    self.assertEqual(py_conversions.to_py(x), {})
    self.assertEqual(py_conversions.to_py(x, obj_as_dict=True), {})

  def test_empty_entity(self):
    entity = fns.new()
    self.assertEqual(py_conversions.to_py(entity), {})
    self.assertEqual(py_conversions.to_py(entity, obj_as_dict=True), {})

  def test_named_schema(self):
    x = fns.new(x=123, schema='named_schema')
    self.assertEqual(
        py_conversions.to_py(x),
        dataclasses.make_dataclass('Obj', [('x', Any)])(x=123),
    )

  def test_itemid_dataslice(self):
    a = fns.new(x=1).get_itemid()
    b = fns.new(x=2).get_itemid()
    x = ds([a, b], schema_constants.ITEMID)
    self.assertEqual(
        py_conversions.to_py(x),
        [
            a,
            b,
        ],
    )

  def test_itemid_list(self):
    with self.subTest('list_of_entities'):
      a = fns.new(x=1).get_itemid()
      b = fns.new(x=2).get_itemid()
      x = fns.list([a, b], item_schema=schema_constants.ITEMID)
      self.assertEqual(py_conversions.to_py(x), [a, b])

    with self.subTest('list_of_objects'):
      a = fns.obj(x=1).get_itemid()
      b = fns.obj(x=2).get_itemid()
      x = fns.list([a, b], item_schema=schema_constants.ITEMID)
      self.assertEqual(py_conversions.to_py(x), [a, b])

    with self.subTest('list_of_lists'):
      a = fns.list([1, 2]).get_itemid()
      b = fns.list([3, 4]).get_itemid()
      x = fns.list([a, b], item_schema=schema_constants.ITEMID)
      self.assertEqual(py_conversions.to_py(x), [a, b])

    with self.subTest('list_of_dicts'):
      a = fns.dict({'a': 1}).get_itemid()
      b = fns.dict({'b': 2}).get_itemid()
      x = fns.list([a, b], item_schema=schema_constants.ITEMID)
      self.assertEqual(py_conversions.to_py(x), [a, b])

  def test_schema_itemid(self):
    a = kde.schema.new_schema(
        value=schema_constants.OBJECT,
        name=schema_constants.STRING,
        score=schema_constants.FLOAT32,
    ).get_itemid().eval()

    self.assertEqual(a.to_py(), a)

  def test_no_bag(self):
    x = fns.list([1, 2]).no_bag()
    testing.assert_equal(py_conversions.to_py(x), x)

  def test_missing(self):
    x = ds(None)
    self.assertIsNone(py_conversions.to_py(x), None)

    x = fns.new(x=1) & None
    self.assertIsNone(py_conversions.to_py(x), None)

    x = fns.obj(x=1) & None
    self.assertIsNone(py_conversions.to_py(x), None)

    x = fns.list([1, 2, 3]) & None
    self.assertIsNone(py_conversions.to_py(x), None)

    x = fns.dict({1: 2}) & None
    self.assertIsNone(py_conversions.to_py(x), None)

    x = fns.new(x=1).get_itemid() & None
    self.assertIsNone(py_conversions.to_py(x), None)

  def test_schema_not_supported(self):
    schema = kde.schema.new_schema(a=schema_constants.STRING).eval()
    with self.assertRaisesRegex(
        ValueError,
        'schema is not supported',
    ):
      _ = schema.to_py()

    with self.assertRaisesRegex(
        ValueError,
        'schema is not supported',
    ):
      _ = ds([schema]).to_py()

    with self.assertRaisesRegex(
        ValueError,
        'schema is not supported',
    ):
      _ = fns.list([schema]).to_py()

    with self.assertRaisesRegex(
        ValueError,
        'schema is not supported',
    ):
      _ = fns.dict({1: schema}).to_py()

    with self.assertRaisesRegex(
        ValueError,
        'schema is not supported',
    ):
      _ = fns.new(a=schema).to_py()

  def test_missing_import(self):
    x = fns.obj(x=1)
    del sys.modules['koladata.base.py_conversions.dataclasses_util']
    with self.assertRaisesRegex(
        ValueError,
        'could not import module koladata.base.py_conversions.dataclasses_util',
    ):
      _ = py_conversions.to_py(x)
    import koladata.base.py_conversions.dataclasses_util  # pylint: disable=g-import-not-at-top

    importlib.reload(koladata.base.py_conversions.dataclasses_util)
    _ = py_conversions.to_py(x)


class ToPytreeTest(absltest.TestCase):

  def test_simple(self):
    x = fns.obj(a=fns.obj(a=1), b=fns.list([1, 2, 3]), c=fns.dict({1: 2}))
    self.assertEqual(
        py_conversions.to_pytree(x),
        {'a': {'a': 1}, 'b': [1, 2, 3], 'c': {1: 2}},
    )
    # Check that the attribute order is alphabetical.
    self.assertEqual(list(py_conversions.to_pytree(x).keys()), ['a', 'b', 'c'])

    p = kde.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    ).eval()
    self.assertEqual(
        ds([fns.new(x=1, y=2, schema=p), fns.new(x=3, schema=p)]).to_pytree(
            include_missing_attrs=False
        ),
        [{'x': 1, 'y': 2}, {'x': 3}]
    )


if __name__ == '__main__':
  absltest.main()
