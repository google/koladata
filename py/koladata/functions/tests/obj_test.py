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

"""Tests for obj."""

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

  def test_item(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.TEXT),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(x.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_db(x.db)
    )
    testing.assert_equal(
        x.a.get_schema(), schema_constants.FLOAT64.with_db(x.db)
    )
    testing.assert_equal(x.b.get_schema(), schema_constants.TEXT.with_db(x.db))

  def test_slice(self):
    x = fns.obj(
        a=ds([[1, 2], [3]]),
        b=fns.obj(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_db(x.db))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_db(x.db))
    testing.assert_equal(x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_db(x.db))
    testing.assert_equal(
        x.a.get_schema(), schema_constants.INT32.with_db(x.db)
    )
    testing.assert_equal(x.b.no_db().get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        x.b.bb.get_schema(), schema_constants.TEXT.with_db(x.db)
    )
    testing.assert_equal(x.c.get_schema(), schema_constants.BYTES.with_db(x.db))

  def test_adopt_db(self):
    x = fns.obj(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.TEXT),
    )
    y = fns.obj(x=x)
    # y.db is merged with x.db, so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_db(y.db)
    )
    testing.assert_equal(y.x.b, ds('abc').with_db(y.db))
    testing.assert_equal(x.get_schema(), y.x.get_schema().with_db(x.db))
    testing.assert_equal(y.x.a.no_db().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_db().get_schema(), schema_constants.TEXT)

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as._eval(ds([[1, 1], [1]]))  # pylint: disable=protected-access
    x = fns.obj(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_db(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_db().as_itemid(), itemid)

  def test_itemid_from_different_db(self):
    itemid = fns.obj(non_existent=42).as_itemid()
    assert itemid.db is not None
    # Successful.
    x = fns.obj(a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = x.non_existent

  def test_db_arg(self):
    db = fns.bag()
    x = fns.obj(a=1, b='a', db=db)
    testing.assert_equal(db, x.db)

    x = fns.obj([1, 2, 3], db=db)
    testing.assert_equal(db, x.db)

  def test_schema_arg(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'please use new'):
      fns.obj(a=1, b='a', schema=schema_constants.INT32)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.obj(x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_db(x.db))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_db(x.db))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.obj(x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_db(x.db))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_db(x.db))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.obj(x=ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.obj(x=ds([1, 2, 3]), dct={'a': 42})

  def test_universal_converter_into_itemid_is_not_supported(self):
    with self.assertRaisesRegex(
        NotImplementedError, 'do not support `itemid` in converter mode'
    ):
      _ = fns.obj([1, 2, 3], itemid=kde.allocation.new_itemid._eval())

  def test_universal_converter_list(self):
    l = fns.obj([1, 2, 3])
    testing.assert_equal(l.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_equal(l[:], ds([1, 2, 3]).with_db(l.db))

    l = fns.list([[1, 2], [3]])
    with self.assertRaises(AssertionError):
      testing.assert_equal(l.get_schema(), schema_constants.OBJECT)
    obj = fns.obj(l)
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_equal(obj[:][:], ds([[1, 2], [3]]).with_db(obj.db))

  def test_universal_converter_empty_list(self):
    l = fns.obj([])
    testing.assert_equal(
        l.get_obj_schema().get_attr('__items__').no_db(),
        schema_constants.OBJECT
    )
    testing.assert_equal(l[:].no_db(), ds([]))

  def test_universal_converter_dict(self):
    d = fns.obj({'a': 42, 'b': ds(37, schema_constants.INT64)})
    testing.assert_equal(d.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(
        d[['a', 'b']], ds([42, 37], schema_constants.INT64).with_db(d.db)
    )

    d = fns.dict({'a': 42, 'b': 37})
    with self.assertRaises(AssertionError):
      testing.assert_equal(d.get_schema(), schema_constants.OBJECT)
    obj = fns.obj(d)
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a', 'b']))
    testing.assert_equal(obj[['a', 'b']], ds([42, 37]).with_db(obj.db))

  def test_universal_converter_empty_dict(self):
    d = fns.obj({})
    testing.assert_equal(
        d.get_obj_schema().get_attr('__keys__').no_db(), schema_constants.OBJECT
    )
    testing.assert_equal(
        d.get_obj_schema().get_attr('__values__').no_db(),
        schema_constants.OBJECT
    )
    testing.assert_dicts_keys_equal(d, ds([]))
    testing.assert_equal(d[ds([])].no_db(), ds([]))

  def test_universal_converter_entity(self):
    with self.subTest('item'):
      entity = fns.new(a=42, b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds(42).with_db(obj.db))
      testing.assert_equal(obj.b, ds('abc').with_db(obj.db))
    with self.subTest('slice'):
      entity = fns.new(a=ds([1, 2]), b='abc')
      obj = fns.obj(entity)
      testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
      testing.assert_equal(obj.a, ds([1, 2]).with_db(obj.db))
      testing.assert_equal(obj.b, ds(['abc', 'abc']).with_db(obj.db))

  def test_universal_converter_object(self):
    obj = fns.obj(a=42, b='abc')
    new_obj = fns.obj(obj)
    with self.assertRaises(AssertionError):
      testing.assert_equal(obj.db, new_obj.db)
    testing.assert_equivalent(new_obj, obj)

  def test_nested_universal_converter(self):
    obj = fns.obj({'a': {'b': [1, 2, 3]}})
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a']))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(values, ds(['b']))
    nested_values = values['b']
    testing.assert_equal(
        nested_values.get_schema().no_db(), schema_constants.OBJECT
    )
    testing.assert_equal(nested_values[:], ds([1, 2, 3]).with_db(obj.db))

  def test_universal_converter_adopt_db_data(self):
    nested = fns.obj(a=42, b='abc')
    obj = fns.obj([1, 2, nested])
    with self.assertRaises(AssertionError):
      testing.assert_equal(nested.db, obj.db)
    testing.assert_equal(obj[2].a, ds(42).with_db(obj.db))

  def test_universal_converter_list_of_complex(self):
    obj = fns.obj([
        {'a': 42},
        {'b': 57},
        42,
        [1, {b'xyz': ds(42, schema_constants.INT64)}, 3]
    ])
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj[0], ds(['a']))
    testing.assert_dicts_keys_equal(obj[1], ds(['b']))
    testing.assert_equal(
        obj[2], ds(42, schema_constants.OBJECT).with_db(obj.db)
    )
    testing.assert_equal(
        obj[3][0], ds(1, schema_constants.OBJECT).with_db(obj.db)
    )
    testing.assert_dicts_keys_equal(obj[3][1], ds([b'xyz']))
    testing.assert_equal(
        obj[3][1].get_schema().no_db(), schema_constants.OBJECT
    )
    testing.assert_equal(
        obj[3][1][b'xyz'].no_db(), ds(42, schema_constants.INT64)
    )
    testing.assert_equal(
        obj[3][-1], ds(3, schema_constants.OBJECT).with_db(obj.db)
    )

  def test_universal_converter_list_of_different_primitive_lists(self):
    obj = fns.obj([[1, 2], [3.14]])
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_equal(
        obj[:].get_attr('__schema__').get_attr('__items__').no_db(),
        ds([schema_constants.INT32, schema_constants.FLOAT32])
    )
    # TODO: Cannot access obj[:][:] because we have a mixed slice
    # with FLOAT32 common schema.
    testing.assert_equal(obj[0][:].no_db(), ds([1, 2]))
    testing.assert_equal(obj[1][:].no_db(), ds([3.14]))

    obj = fns.obj([[1, 2], ['xyz']])
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_equal(
        obj[:].get_attr('__schema__').get_attr('__items__').no_db(),
        ds([schema_constants.INT32, schema_constants.TEXT])
    )
    testing.assert_equal(obj[:][:].no_db(), ds([[1, 2], ['xyz']]))

    obj = fns.obj([[1, 2], [3]])
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_equal(obj[:][:].get_schema().no_db(), schema_constants.INT32)
    testing.assert_equal(obj[:][:].no_db(), ds([[1, 2], [3]]))

  def test_universal_converter_container_contains_multi_dim_data_slice(self):
    with self.assertRaisesRegex(
        ValueError, 'dict / list containing multi-dim DataSlice'
    ):
      fns.obj([ds([1, 2, 3]), 42])
    with self.assertRaisesRegex(
        ValueError, 'dict / list containing multi-dim DataSlice'
    ):
      fns.obj({42: ds([1, 2, 3])})

  def test_universal_converter_tuple_as_list(self):
    l = fns.obj(tuple([1, 2, 3]))
    testing.assert_equal(l[:].no_db(), ds([1, 2, 3]))

  def test_universal_converter_with_cross_ref(self):
    d1 = {'a': 42}
    d2 = {'b': 37}
    d1['d'] = d2
    d = {'d1': d1, 'd2': d2}
    obj = fns.obj(d)
    testing.assert_equal(obj.get_schema().no_db(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['d1', 'd2']))
    obj_d2 = obj['d2']
    testing.assert_equal(obj['d1']['d'], obj_d2)
    testing.assert_equal(obj_d2['b'], ds(37).with_db(obj.db))
    testing.assert_equal(
        obj['d1']['a'], ds(42, schema_constants.OBJECT).with_db(obj.db)
    )

  def test_universal_converter_with_reusing_dicts(self):
    d = {'abc': 42}
    for _ in range(2):
      d = {12: d, 42: d}
    obj = fns.obj(d)
    testing.assert_dicts_keys_equal(obj, ds([12, 42]))
    d1 = obj[12]
    testing.assert_dicts_keys_equal(d1, ds([12, 42]))
    d2 = obj[42]
    testing.assert_dicts_keys_equal(d2, ds([12, 42]))
    # d1 and d2 share the same ItemId.
    testing.assert_equal(d1, d2)
    d11 = d1[12]
    testing.assert_dicts_keys_equal(d11, ds(['abc']))
    d12 = d1[42]
    testing.assert_dicts_keys_equal(d12, ds(['abc']))
    d21 = d1[12]
    testing.assert_dicts_keys_equal(d21, ds(['abc']))
    d22 = d1[42]
    testing.assert_dicts_keys_equal(d22, ds(['abc']))

  def test_universal_converter_with_reusing_lists(self):
    l = [42, 57]
    for _ in range(2):
      l = [l, l]
    obj = fns.obj(l)
    testing.assert_equal(
        obj[:][:][:],
        ds([[[42, 57], [42, 57]], [[42, 57], [42, 57]]]).with_db(obj.db),
    )

  def test_deep_universal_converter(self):
    d = {'abc': 42}
    for _ in range(3):
      d = {12: d}
    obj = fns.obj(d)
    testing.assert_dicts_keys_equal(obj, ds([12]))
    d = obj[12]
    testing.assert_dicts_keys_equal(d, ds([12]))
    d = d[12]
    testing.assert_dicts_keys_equal(d, ds([12]))
    d = d[12]
    testing.assert_dicts_keys_equal(d, ds(['abc']))
    testing.assert_equal(d['abc'], ds(42).with_db(obj.db))

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
      fns.obj(d2)
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


if __name__ == '__main__':
  absltest.main()
