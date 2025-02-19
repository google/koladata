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
import math
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from koladata.exceptions import exceptions
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ToPyTest(parameterized.TestCase):

  def test_empty_slice(self):
    self.assertEqual(fns.to_py(ds([])), [])

  def test_multi_dimensional_slice(self):
    self.assertEqual(
        fns.to_py(
            ds([
                [fns.list([1, 2]), fns.list([3, 4])],
                [fns.list([5, 6]), fns.list([7, 8])],
            ]),
        ),
        [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
    )

  def test_list(self):
    root = fns.container()
    root.none_list_value = []
    root.none_list_value.append(None)
    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for List item is incompatible'
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

    py_obj = fns.to_py(root, max_depth=4)

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
    self.assertEqual(
        obj.to_py(obj_as_dict=True), {'a': [1, 2, 3], 'b': [1, 2, 3]}
    )

  def test_dict(self):
    root = fns.container()
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

    py_obj = fns.to_py(root, max_depth=-1)

    self.assertEqual(expected, py_obj)
    self.assertEqual(py_obj, expected)

  def test_dict_with_obj_keys(self):
    root = fns.container()
    root.dict_value = fns.dict()
    k = fns.obj(a=1)
    root.dict_value[k] = 1
    py_obj = fns.to_py(root, max_depth=4)
    self.assertEqual(py_obj.dict_value[k.no_bag()], 1)

  def test_dict_with_zero_code_key(self):
    root = fns.obj(**{'a\0b': 1})
    py_obj = fns.to_py(root, obj_as_dict=True)
    self.assertEqual(py_obj['a\0b'], 1)

    root = fns.dict({'a\0b': 1})
    py_obj = fns.to_py(root, obj_as_dict=True)
    self.assertEqual(py_obj['a\0b'], 1)

  def test_list_obj(self):
    self.assertEqual(fns.to_py(fns.obj([1, 2])), [1, 2])

  def test_dict_obj(self):
    self.assertEqual(fns.to_py(fns.obj({1: 2})), {1: 2})

  def test_fallbacks(self):
    x = fns.new(x=1)
    fallback_bag = fns.bag()
    fallback_bag[x].set_attr('y', 'abc')
    x = x.enriched(fallback_bag)
    self.assertEqual(fns.to_py(x, obj_as_dict=True), {'x': 1, 'y': 'abc'})

    @dataclasses.dataclass
    class Obj:
      x: int
      y: str

    self.assertEqual(fns.to_py(x, obj_as_dict=False), Obj(x=1, y='abc'))

  def test_self_reference_object(self):
    with self.subTest('in_attribute'):
      root = fns.container()
      root.x = fns.obj()
      root.x.y = root
      py_obj = fns.to_py(root)
      self.assertEqual(id(py_obj.x.y), id(py_obj))
      py_obj = fns.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['x']['y']), id(py_obj))

    with self.subTest('in_dict'):
      root = fns.container()
      root.x = fns.dict({'y': root})
      py_obj = fns.to_py(root)
      self.assertEqual(id(py_obj.x['y']), id(py_obj))
      py_obj = fns.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['x']['y']), id(py_obj))

    with self.subTest('in_list'):
      root = fns.container()
      root.a = fns.list([root])
      py_obj = fns.to_py(root)
      self.assertEqual(id(py_obj.a[0]), id(py_obj))
      py_obj = fns.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['a'][0]), id(py_obj))

  def test_self_reference_dict(self):
    with self.subTest('in_attribute'):
      root = fns.dict().fork_bag()
      root['a'] = fns.obj(b=root)
      py_obj = fns.to_py(root)
      self.assertEqual(id(py_obj['a'].b), id(py_obj))
      py_obj = fns.to_py(root, obj_as_dict=True)
      self.assertEqual(id(py_obj['a']['b']), id(py_obj))

    # TODO: make this test pass.
    # with self.subTest('in_list'):
    #   root = fns.dict().fork_bag()
    #   root['a'] = fns.list()
    #   root['a'].append(root)
    #   py_obj = fns.to_py(root)
    #   self.assertEqual(id(py_obj['a'][0]), id(py_obj))

    with self.subTest('in_dict'):
      root = fns.dict().fork_bag()
      root['a'] = fns.dict({'b': root})
      py_obj = fns.to_py(root)
      self.assertEqual(id(py_obj['a']['b']), id(py_obj))

  def test_self_reference_list_in_dict(self):
    with self.subTest('in_attribute'):
      x = fns.list().fork_bag()
      x.append(fns.obj(a=x))
      py_obj = fns.to_py(x)
      self.assertEqual(id(py_obj[0].a), id(py_obj))
      py_obj = fns.to_py(x, obj_as_dict=True)
      self.assertEqual(id(py_obj[0]['a']), id(py_obj))

    with self.subTest('in_list'):
      x = fns.list().fork_bag()
      x.append(fns.list([x]))
      py_obj = fns.to_py(x)
      self.assertEqual(id(py_obj[0][0]), id(py_obj))

    with self.subTest('in_dict'):
      x = fns.list().fork_bag()
      x.append(fns.dict({'a': x}))
      py_obj = fns.to_py(x)
      self.assertEqual(id(py_obj[0]['a']), id(py_obj))

  def test_multiple_references(self):
    x = fns.container()
    x.foo = [1, 2]
    x.bar = x.foo

    @dataclasses.dataclass
    class Obj:
      foo: list[int]
      bar: list[int]

    expected = Obj(foo=[1, 2], bar=[1, 2])

    py_obj = fns.to_py(x)

    self.assertEqual(py_obj, expected)
    self.assertEqual(expected, py_obj)

  def test_slice_without_bag(self):
    s = ds([[1, 2], [3, 4, 5]])
    py_obj = fns.to_py(s)
    expected = [[1, 2], [3, 4, 5]]
    self.assertEqual(expected, py_obj)

  def test_does_not_pollute_bag(self):
    root = fns.container()
    root.foo = fns.implode(fns.implode(ds([[1, 2], [3, 4, 5]])))
    old_data_repr = repr(root.get_bag())
    fns.to_py(root.foo[:][:])
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

    self.assertEqual(fns.to_py(x, max_depth=0), x)

    res = fns.to_py(x, max_depth=1)
    self.assertEqual(res.a, x.a)
    self.assertEqual(res.a.x, x.a.x)
    self.assertEqual(res.a.x.y, x.a.x.y)
    self.assertEqual(res.b, x.b)
    # TODO: check that the bag of the result is the same as of x.
    testing.assert_equal(res.b[:].no_bag(), ds([12, 13]).no_bag())
    self.assertEqual(res.c, x.c)
    testing.assert_equal(res.c.get_keys().no_bag(), ds([14]).no_bag())
    testing.assert_equal(res.c.get_values().no_bag(), ds([15]).no_bag())
    self.assertEqual(res.d, 11)
    self.assertEqual(res.e, x.e)
    self.assertEqual(res.f, x.f)
    self.assertEqual(res.f.g, x.f.g)
    self.assertEqual(res.f.g.h, x.f.g.h)

    res = fns.to_py(x, max_depth=2)
    self.assertEqual(res.a.x, x.a.x)
    self.assertEqual(res.b, [12, 13])
    self.assertEqual(res.c, {14: 15})
    self.assertEqual(res.d, 11)
    self.assertEqual(res.e, [x.e[0], x.e[1]])
    self.assertEqual(
        res.f, dataclasses.make_dataclass('Obj', [('g', Any)])(g=x.f.g)
    )
    self.assertEqual(res.f.g, x.f.g)
    self.assertEqual(res.f.g.h, x.f.g.h)

    res = fns.to_py(x, max_depth=2, obj_as_dict=True)
    self.assertEqual(res['a']['x'], x.a.x)
    self.assertEqual(res['b'], [12, 13])
    self.assertEqual(res['c'], {14: 15})
    self.assertEqual(res['d'], 11)
    self.assertEqual(res['e'][0], x.e[0])
    self.assertEqual(res['e'][1], x.e[1])
    self.assertEqual(res['f']['g'], x.f.g)

    res = fns.to_py(x, max_depth=3)
    self.assertEqual(
        res.f.g, dataclasses.make_dataclass('Obj', [('h', Any)])(h=17)
    )

    self.assertEqual(
        fns.to_py(x, max_depth=3, obj_as_dict=True),
        {
            'a': {'x': {'y': 123}},
            'b': [12, 13],
            'c': {14: 15},
            'd': 11,
            'e': [[1, 2], [3, 4]],
            'f': {'g': {'h': 17}},
        },
    )

    x3 = fns.to_py(x, max_depth=3)
    x_full = fns.to_py(x, max_depth=-1)
    self.assertEqual(x3, x_full)

    x = ds([[1, 2], [3, 4]])
    self.assertEqual(fns.to_py(x, max_depth=0), [[1, 2], [3, 4]])

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

    self.assertEqual(fns.to_py(x, max_depth=0), [o1, o2])

    self.assertEqual(
        fns.to_py(x, max_depth=1), [Obj0(o1_level1), Obj0(o2_level1)]
    )
    self.assertEqual(
        fns.to_py(x, max_depth=2),
        [Obj0(Obj1(o1_level2)), Obj0(Obj1(o2_level2))],
    )
    self.assertEqual(
        fns.to_py(x, max_depth=3), [Obj0(Obj1(Obj2(1))), Obj0(Obj1(Obj2(2)))]
    )

  def test_include_missing_attrs(self):
    p = fns.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    x = ds([p(x=1, y=2), p(x=3)])
    self.assertEqual(
        fns.to_py(x, obj_as_dict=True, include_missing_attrs=False),
        [{'x': 1, 'y': 2}, {'x': 3}])

    self.assertEqual(
        fns.to_py(x, obj_as_dict=True, include_missing_attrs=True),
        [{'x': 1, 'y': 2}, {'x': 3, 'y': None}],
    )

    self.assertIsNone(fns.to_py(x, include_missing_attrs=False)[1].y)
    self.assertIsNone(fns.to_py(x, include_missing_attrs=True)[1].y)

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
    self.assertEqual(fns.to_py(ds(expected)), expected)

  def test_nan(self):
    self.assertTrue(math.isnan(fns.to_py(ds(float('nan')))))

    res = fns.to_py(fns.obj(x=float('nan')))
    self.assertTrue(math.isnan(res.x))

    res = fns.to_py(fns.new(x=float('nan')))
    self.assertTrue(math.isnan(res.x))

    res = fns.to_py(fns.obj(x=float('nan')), obj_as_dict=True)
    self.assertTrue(math.isnan(res['x']))

    res = fns.to_py(fns.list([float('nan')]))
    self.assertTrue(math.isnan(res[0]))

    res = fns.to_py(fns.dict({1: float('nan')}))
    self.assertTrue(math.isnan(res[1]))

  def test_empty_object(self):
    x = fns.obj()
    self.assertEqual(fns.to_py(x), {})
    self.assertEqual(fns.to_py(x, obj_as_dict=True), {})

  def test_empty_entity(self):
    entity = fns.new()
    self.assertEqual(fns.to_py(entity), {})
    self.assertEqual(fns.to_py(entity, obj_as_dict=True), {})

  def test_named_schema(self):
    x = fns.new(x=123, schema='named_schema')
    self.assertEqual(
        fns.to_py(x), dataclasses.make_dataclass('Obj', [('x', Any)])(x=123)
    )

  def test_itemid_dataslice(self):
    a = fns.new(x=1).get_itemid()
    b = fns.new(x=2).get_itemid()
    x = ds([a, b], schema_constants.ITEMID)
    self.assertEqual(
        fns.to_py(x),
        [
            a,
            b,
        ],
    )

  def test_no_bag(self):
    x = fns.list([1, 2]).no_bag()
    # TODO: this should be x.get_itemid().
    self.assertEqual(fns.to_py(x), {})

  def test_missing(self):
    x = ds(None)
    self.assertIsNone(fns.to_py(x), None)

    x = fns.new(x=1) & None
    self.assertIsNone(fns.to_py(x), None)

    x = fns.obj(x=1) & None
    self.assertIsNone(fns.to_py(x), None)

    x = fns.list([1, 2, 3]) & None
    self.assertIsNone(fns.to_py(x), None)

    x = fns.dict({1: 2}) & None
    self.assertIsNone(fns.to_py(x), None)

    x = fns.new(x=1).get_itemid() & None
    self.assertIsNone(fns.to_py(x), None)


class ToPytreeTest(absltest.TestCase):

  def test_simple(self):
    x = fns.obj(a=fns.obj(a=1), b=fns.list([1, 2, 3]), c=fns.dict({1: 2}))
    self.assertEqual(
        fns.to_pytree(x),
        {'a': {'a': 1}, 'b': [1, 2, 3], 'c': {1: 2}})
    # Check that the attribute order is alphabetical.
    self.assertEqual(list(fns.to_pytree(x).keys()), ['a', 'b', 'c'])

    p = fns.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    self.assertEqual(
        ds([p(x=1, y=2), p(x=3)]).to_pytree(include_missing_attrs=False),
        [{'x': 1, 'y': 2}, {'x': 3}])


if __name__ == '__main__':
  absltest.main()
