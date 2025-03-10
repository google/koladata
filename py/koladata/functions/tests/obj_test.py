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

from absl.testing import absltest
from koladata.exceptions import exceptions
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ObjTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.obj().is_mutable())
    self.assertTrue(fns.obj(db=fns.bag()).is_mutable())

  def test_item(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(x.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.b.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )

  def test_slice(self):
    x = fns.obj(
        a=ds([[1, 2], [3]]),
        b=fns.obj(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(x.b.no_bag().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        x.b.bb.get_schema(), schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.c.get_schema(), schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    y = fns.obj(x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_bag(y.get_bag())
    )
    testing.assert_equal(y.x.b, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]])).eval()
    x = fns.obj(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.obj(non_existent=42).get_itemid()
    assert itemid.get_bag() is not None
    # Successful.
    x = fns.obj(a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = x.non_existent

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.obj(a=1, b='a', db=db)
    testing.assert_equal(db, x.get_bag())

    x = fns.obj([1, 2, 3], db=db)
    testing.assert_equal(db, x.get_bag())

  def test_schema_arg(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'please use new'):
      fns.obj(a=1, b='a', schema=schema_constants.INT32)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.obj(x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.obj(x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.obj(x=ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.obj(x=ds([1, 2, 3]), dct={'a': 42})

  def test_universal_converter_with_itemid(self):
    itemid = kde.uuid_for_list('obj').eval()
    res = fns.obj([{'a': 42, 'b': 17}, {'c': 12}], itemid=itemid)
    child_itemid = kde.uuid_for_dict(
        '__from_py_child__', parent=itemid,
        list_item_index=ds([0, 1], schema_constants.INT64)
    ).eval()
    testing.assert_equal(res.no_bag().get_itemid(), itemid)
    testing.assert_dicts_keys_equal(
        res[:], ds([['a', 'b'], ['c']], schema_constants.OBJECT)
    )
    testing.assert_equal(res[:].no_bag().get_itemid(), child_itemid)

    with self.assertRaisesRegex(
        ValueError, 'itemid argument to list creation, requires List ItemIds'
    ):
      _ = fns.obj([1, 2], itemid=kde.allocation.new_itemid().eval())

    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(2, \[2, 1\]\)'
    ):
      _ = fns.obj(ds([[1, 2], [3]]), itemid=kde.allocation.new_itemid().eval())

  def test_universal_converter_primitive(self):
    item = fns.obj(42)
    testing.assert_equal(item.no_bag(), ds(42, schema_constants.OBJECT))

  def test_universal_converter_none(self):
    item = fns.obj(None)
    testing.assert_equal(item.no_bag(), ds(None, schema_constants.OBJECT))

  def test_universal_converter_list(self):
    l = fns.obj([1, 2, 3])
    testing.assert_equal(l.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        l[:], ds([1, 2, 3], schema_constants.OBJECT).with_bag(l.get_bag())
    )

    l = fns.list([[1, 2], [3]])
    with self.assertRaises(AssertionError):
      testing.assert_equal(l.get_schema(), schema_constants.OBJECT)
    obj = fns.obj(l)
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(obj[:][:], ds([[1, 2], [3]]).with_bag(obj.get_bag()))

  def test_universal_converter_empty_list(self):
    l = fns.obj([])
    testing.assert_equal(
        l.get_obj_schema().get_attr('__items__').no_bag(),
        schema_constants.OBJECT,
    )
    testing.assert_equal(l[:].no_bag(), ds([], schema_constants.OBJECT))

  def test_universal_converter_dict(self):
    d = fns.obj({'a': 42, 'b': ds(37, schema_constants.INT64)})
    testing.assert_equal(d.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(d, ds(['a', 'b'], schema_constants.OBJECT))
    testing.assert_equal(
        d[ds(['a', 'b'])],
        ds(
            [42, ds(37, schema_constants.INT64)], schema_constants.OBJECT
        ).with_bag(d.get_bag()),
    )

    d = fns.dict({'a': 42, 'b': 37})
    with self.assertRaises(AssertionError):
      testing.assert_equal(d.get_schema(), schema_constants.OBJECT)
    obj = fns.obj(d)
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a', 'b']))
    testing.assert_equal(
        obj[ds(['a', 'b'])], ds([42, 37]).with_bag(obj.get_bag())
    )

  def test_universal_converter_empty_dict(self):
    d = fns.obj({})
    testing.assert_equal(
        d.get_obj_schema().get_attr('__keys__').no_bag(),
        schema_constants.OBJECT,
    )
    testing.assert_equal(
        d.get_obj_schema().get_attr('__values__').no_bag(),
        schema_constants.OBJECT,
    )
    testing.assert_dicts_keys_equal(d, ds([], schema_constants.OBJECT))
    testing.assert_equal(d[ds([])].no_bag(), ds([], schema_constants.OBJECT))

  def test_universal_converter_entity(self):
    with self.subTest('item'):
      entity = fns.new(a=42, b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds(42).with_bag(obj.get_bag()))
      testing.assert_equal(obj.b, ds('abc').with_bag(obj.get_bag()))
    with self.subTest('slice'):
      entity = fns.new(a=ds([1, 2]), b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds([1, 2]).with_bag(obj.get_bag()))
      testing.assert_equal(obj.b, ds(['abc', 'abc']).with_bag(obj.get_bag()))

  def test_universal_converter_object(self):
    obj = fns.obj(a=42, b='abc')
    new_obj = fns.obj(obj)
    with self.assertRaises(AssertionError):
      testing.assert_equal(obj.get_bag(), new_obj.get_bag())
    testing.assert_equivalent(new_obj, obj)

  def test_nested_universal_converter(self):
    obj = fns.obj({'a': {'b': [1, 2, 3]}})
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a'], schema_constants.OBJECT))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(values, ds(['b'], schema_constants.OBJECT))
    nested_values = values['b']
    testing.assert_equal(
        nested_values.get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        nested_values[:],
        ds([1, 2, 3], schema_constants.OBJECT).with_bag(obj.get_bag()),
    )

  def test_universal_converter_adopt_bag_data(self):
    nested = fns.obj(a=42, b='abc')
    obj = fns.obj([1, 2, nested])
    with self.assertRaises(AssertionError):
      testing.assert_equal(nested.get_bag(), obj.get_bag())
    testing.assert_equal(obj[2].a, ds(42).with_bag(obj.get_bag()))

  def test_universal_converter_list_of_complex(self):
    obj = fns.obj([
        {'a': 42},
        {'b': 57},
        42,
        [1, {b'xyz': ds(42, schema_constants.INT64)}, 3]
    ])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj[0], ds(['a'], schema_constants.OBJECT))
    testing.assert_dicts_keys_equal(obj[1], ds(['b'], schema_constants.OBJECT))
    testing.assert_equal(
        obj[2], ds(42, schema_constants.OBJECT).with_bag(obj.get_bag())
    )
    testing.assert_equal(
        obj[3][0], ds(1, schema_constants.OBJECT).with_bag(obj.get_bag())
    )
    testing.assert_dicts_keys_equal(
        obj[3][1], ds([b'xyz'], schema_constants.OBJECT)
    )
    testing.assert_equal(
        obj[3][1].get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        obj[3][1][b'xyz'].no_bag(),
        ds(ds(42, schema_constants.INT64), schema_constants.OBJECT),
    )
    testing.assert_equal(
        obj[3][-1], ds(3, schema_constants.OBJECT).with_bag(obj.get_bag())
    )

  def test_universal_converter_list_of_different_primitive_lists(self):
    obj = fns.obj([[1, 2], [3.14]])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        obj[:].get_attr('__schema__').get_attr('__items__').no_bag(),
        ds([schema_constants.OBJECT, schema_constants.OBJECT]),
    )
    testing.assert_equal(
        obj[0][:].no_bag(), ds([1, 2], schema_constants.OBJECT)
    )
    testing.assert_equal(
        obj[1][:].no_bag(), ds([3.14], schema_constants.OBJECT)
    )
    testing.assert_equal(
        obj[:][:].no_bag(), ds([[1, 2], [3.14]], schema_constants.OBJECT)
    )

    obj = fns.obj([[1, 2], ['xyz']])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        obj[:].get_attr('__schema__').get_attr('__items__').no_bag(),
        ds([schema_constants.OBJECT, schema_constants.OBJECT]),
    )
    testing.assert_equal(obj[:][:].no_bag(), ds([[1, 2], ['xyz']]))

    obj = fns.obj([[1, 2], [3]])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        obj[:][:].get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        obj[:][:].no_bag(), ds([[1, 2], [3]], schema_constants.OBJECT)
    )

  def test_universal_converter_container_contains_multi_dim_data_slice(self):
    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      fns.obj([ds([1, 2, 3]), 42])
    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      fns.obj({42: ds([1, 2, 3])})

  def test_universal_converter_tuple_as_list(self):
    l = fns.obj(tuple([1, 2, 3]))
    testing.assert_equal(l[:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT))

  def test_universal_converter_with_cross_ref(self):
    d1 = {'a': 42}
    d2 = {'b': 37}
    d1['d'] = d2
    d = {'d1': d1, 'd2': d2}
    obj = fns.obj(d)
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(
        obj, ds(['d1', 'd2'], schema_constants.OBJECT)
    )
    obj_d2 = obj['d2']
    self.assertNotEqual(obj['d1']['d'].get_itemid(), obj_d2.get_itemid())
    self.assertEqual(obj['d1']['d'].to_py(), obj_d2.to_py())
    testing.assert_equal(
        obj_d2['b'], ds(37, schema_constants.OBJECT).with_bag(obj.get_bag())
    )
    testing.assert_equal(
        obj['d1']['a'], ds(42, schema_constants.OBJECT).with_bag(obj.get_bag())
    )

  def test_universal_converter_with_reusing_dicts(self):
    d = {'abc': 123}
    for _ in range(2):
      d = {12: d, 42: d}
    obj = fns.obj(d)
    testing.assert_dicts_keys_equal(obj, ds([12, 42], schema_constants.OBJECT))
    d1 = obj[12]
    testing.assert_dicts_keys_equal(d1, ds([12, 42], schema_constants.OBJECT))
    d2 = obj[42]
    testing.assert_dicts_keys_equal(d2, ds([12, 42], schema_constants.OBJECT))
    # d1 and d2 are the same, but have different ItemId.
    self.assertNotEqual(d1.get_itemid(), d2.get_itemid())
    self.assertEqual(d1.to_py(), d2.to_py())
    d11 = d1[12]
    testing.assert_dicts_keys_equal(d11, ds(['abc'], schema_constants.OBJECT))
    d12 = d1[42]
    testing.assert_dicts_keys_equal(d12, ds(['abc'], schema_constants.OBJECT))
    d21 = d1[12]
    testing.assert_dicts_keys_equal(d21, ds(['abc'], schema_constants.OBJECT))
    d22 = d1[42]
    testing.assert_dicts_keys_equal(d22, ds(['abc'], schema_constants.OBJECT))

  def test_universal_converter_with_reusing_lists(self):
    l = [42, 57]
    for _ in range(2):
      l = [l, l]
    obj = fns.obj(l)
    testing.assert_equal(
        obj[:][:][:],
        ds(
            [[[42, 57], [42, 57]], [[42, 57], [42, 57]]],
            schema_constants.OBJECT,
        ).with_bag(obj.get_bag()),
    )

  def test_deep_universal_converter(self):
    d = {'abc': 42}
    for _ in range(3):
      d = {12: d}
    obj = fns.obj(d)
    testing.assert_dicts_keys_equal(obj, ds([12], schema_constants.OBJECT))
    d = obj[12]
    testing.assert_dicts_keys_equal(d, ds([12], schema_constants.OBJECT))
    d = d[12]
    testing.assert_dicts_keys_equal(d, ds([12], schema_constants.OBJECT))
    d = d[12]
    testing.assert_dicts_keys_equal(d, ds(['abc'], schema_constants.OBJECT))
    testing.assert_equal(
        d['abc'], ds(42, schema_constants.OBJECT).with_bag(obj.get_bag())
    )

  def test_universal_converter_recursive_object_error(self):
    d = {'a': 42}
    d['self'] = d
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.obj(d)
    # Deeper recursion:
    d2 = {'a': {'b': d}}
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.obj(d2)
    # Longer cycle:
    d = {'a': 42}
    bottom_d = d
    for i in range(5):
      d = {f'd{i}': d}
    level_1_d = d
    d = {'top': d}
    bottom_d['cycle'] = level_1_d
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.obj(d)
    # Cycle in list:
    l = [[1, 2], 3]
    l[0].append(l)
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.obj(l)

  def test_universal_converter_with_attrs(self):
    with self.assertRaisesRegex(
        TypeError, 'cannot set extra attributes when converting to object'
    ):
      fns.obj([1, 2, 3], a=42)

  def test_item_none(self):
    testing.assert_equal(
        fns.obj(ds(None)).no_bag(), ds(None, schema_constants.OBJECT)
    )
    testing.assert_equal(
        fns.obj(ds([[None, None], [None], []])).no_bag(),
        ds([[None, None], [None], []], schema_constants.OBJECT),
    )

  def test_alias(self):
    self.assertIs(fns.obj, fns.objs.new)


if __name__ == '__main__':
  absltest.main()
