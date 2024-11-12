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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.types import data_slice
from koladata.types import schema_constants


kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ToPyTest(absltest.TestCase):

  def test_list(self):
    root = fns.obj()
    root.list_value = []
    root.list_value.append(1)
    root.list_value.append(2)
    nested_values = ds([[1, 2], [3, 4, 5]])
    root.nested_list = fns.list(fns.list(nested_values))
    root.empty_list = []

    py_obj = dataclasses.asdict(fns.to_py(root, max_depth=4))
    expected = {
        'list_value': [1, 2],
        'nested_list': [[1, 2], [3, 4, 5]],
        'empty_list': [],
    }
    self.assertEqual(expected, py_obj)

  def test_dict(self):
    root = fns.obj()
    root.dict_value = {}
    root.dict_value['key_1'] = 'value_1'
    root.dict_value['key_2'] = 'value_2'
    root.empty_dict = {}

    py_obj = dataclasses.asdict(fns.to_py(root))
    expected = {
        'dict_value': {'key_1': 'value_1', 'key_2': 'value_2'},
        'empty_dict': {},
    }
    self.assertEqual(expected, py_obj)

  def test_dict_with_obj_keys(self):
    root = fns.obj()
    root.dict_value = {}
    k = fns.obj(a=1)
    root.dict_value[k] = 1
    py_obj = fns.to_py(root)
    self.assertEqual(py_obj.dict_value[k.no_bag()], 1)

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

  def test_self_reference(self):
    root = fns.obj()
    root.x = fns.obj()
    root.x.y = root
    py_obj = fns.to_py(root, max_depth=3)
    self.assertEqual(id(py_obj.x.y), id(py_obj))

  def test_multiple_references(self):
    x = fns.obj()
    x.foo = [1, 2]
    x.bar = x.foo
    py_obj = dataclasses.asdict(fns.to_py(x))

    self.assertEqual(py_obj, {'foo': [1, 2], 'bar': [1, 2]})

  def test_slice_without_bag(self):
    s = ds([[1, 2], [3, 4, 5]])
    py_obj = fns.to_py(s)
    expected = [[1, 2], [3, 4, 5]]
    self.assertEqual(expected, py_obj)

  def test_does_not_pollute_bag(self):
    root = fns.obj()
    root.foo = fns.list(fns.list(ds([[1, 2], [3, 4, 5]])))
    old_data_repr = repr(root.get_bag())
    fns.to_py(root.foo[:][:])
    self.assertEqual(old_data_repr, repr(root.get_bag()))

  def test_max_depth(self):
    x = fns.obj(
        a=fns.obj(x=2),
        b=fns.list([1, 2]),
        c=fns.dict({3: 4}),
        d=4,
        e=fns.list([[1, 2], [3, 4]]),
    )

    self.assertEqual(
        fns.to_py(x, max_depth=0).internal_as_py(), x.internal_as_py()
    )
    y = fns.to_py(x, max_depth=1)
    self.assertEqual(y.a.to_py(), x.a.to_py())
    self.assertEqual(y.b.to_py(), x.b.to_py())
    self.assertEqual(y.c.to_py(), x.c.to_py())
    self.assertEqual(y.d, 4)
    self.assertEqual(y.e.to_py(), x.e.to_py())

    y = fns.to_py(x, max_depth=2, obj_as_dict=True)
    self.assertEqual(y['a'], {'x': 2})
    self.assertEqual(y['b'], [1, 2])
    self.assertEqual(y['c'], {3: 4})
    self.assertEqual(y['d'], 4)
    self.assertEqual(y['e'][0].to_py(), x.e[0].to_py())
    self.assertEqual(y['e'][1].to_py(), x.e[1].to_py())

    self.assertEqual(
        fns.to_py(x, max_depth=3, obj_as_dict=True),
        {
            'a': {'x': 2},
            'b': [1, 2],
            'c': {3: 4},
            'd': 4,
            'e': [[1, 2], [3, 4]],
        },
    )
    self.assertEqual(
        fns.to_py(x, max_depth=3), fns.to_py(x, max_depth=-1)
    )

    x = ds([[1, 2], [3, 4]])
    self.assertEqual(fns.to_py(x, max_depth=0), [[1, 2], [3, 4]])

  def test_include_missing_attrs(self):
    p = fns.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    x = ds([p(x=1, y=2), p(x=3)])
    self.assertEqual(
        fns.to_py(x, obj_as_dict=True, include_missing_attrs=False),
        [{'x': 1, 'y': 2}, {'x': 3}])

    self.assertIsNone(fns.to_py(x, include_missing_attrs=False)[1].y)

  def test_any_schema(self):
    x = fns.list([1, 2, 3]).as_any()
    with self.assertRaisesRegex(
        ValueError, 'cannot convert a DataSlice with ANY schema to Python'):
      fns.to_py(x)

  def test_any_primitvies(self):
    x = fns.new(a=fns.new(x=1, y='abc'), b=ds(2).as_any())
    self.assertEqual(fns.to_py(x, obj_as_dict=True),
                     {'a': {'x': 1, 'y': 'abc'}, 'b': 2})


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
