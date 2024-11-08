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

import re

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


class NewTest(absltest.TestCase):

  def test_item(self):
    x = fns.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_allclose(
        x.a, ds(3.14, schema_constants.FLOAT64).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.FLOAT64.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b, schema_constants.STRING.with_bag(x.get_bag())
    )

  def test_slice(self):
    x = fns.new(
        a=ds([[1, 2], [3]]),
        b=fns.new(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, 2], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', 'b'], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', b'xyz'], [b'xyz']]).with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().a, schema_constants.INT32.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().b.bb, schema_constants.STRING.with_bag(x.get_bag())
    )
    testing.assert_equal(
        x.get_schema().c, schema_constants.BYTES.with_bag(x.get_bag())
    )

  def test_adopt_bag(self):
    x = fns.new(
        a=ds(3.14, schema_constants.FLOAT64),
        b=ds('abc', schema_constants.STRING),
    )
    y = fns.new(x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_allclose(
        y.x.a, ds(3.14, schema_constants.FLOAT64).with_bag(y.get_bag())
    )
    testing.assert_equal(y.x.b, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.get_schema().x.with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.FLOAT64)
    testing.assert_equal(y.x.b.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = kde.allocation.new_itemid_shaped_as._eval(ds([[1, 1], [1]]))  # pylint: disable=protected-access
    x = fns.new(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=42).get_itemid()
    assert itemid.get_bag() is not None
    # Successful.
    x = fns.new(a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = x.non_existent

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.new(a=1, b='a', db=db)
    testing.assert_equal(db, x.get_bag())

    x = fns.new([1, 2, 3], db=db)
    testing.assert_equal(db, x.get_bag())

  def test_schema_arg_simple(self):
    schema = fns.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = fns.new(a=42, b='xyz', schema=schema)
    self.assertEqual(dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_deep(self):
    nested_schema = fns.schema.new_schema(p=schema_constants.BYTES)
    schema = fns.schema.new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
        nested=nested_schema,
    )
    x = fns.new(
        a=42,
        b='xyz',
        nested=fns.new(p=b'0123', schema=nested_schema),
        schema=schema,
    )
    self.assertEqual(dir(x), ['a', 'b', 'nested'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)
    testing.assert_equal(x.nested.p, ds(b'0123').with_bag(x.get_bag()))
    testing.assert_equal(
        x.nested.get_schema().p.no_bag(), schema_constants.BYTES
    )

  def test_schema_arg_list(self):
    list_schema = fns.list_schema(item_schema=schema_constants.FLOAT32)
    l = fns.new([1, 2, 3], schema=list_schema)
    testing.assert_equal(l[:], ds([1.0, 2.0, 3.0]).with_bag(l.get_bag()))

  def test_schema_arg_dict(self):
    dict_schema = fns.dict_schema(
        key_schema=schema_constants.STRING, value_schema=schema_constants.INT64
    )
    d = fns.new({'a': 37, 'b': 42}, schema=dict_schema)
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(
        d[['a', 'b']],
        ds([37, 42], schema_constants.INT64).with_bag(d.get_bag()),
    )

  def test_schema_arg_dict_deep(self):
    dict_schema = fns.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.OBJECT
    )
    d = fns.new({42: {'a': 32}, 37: {'b': 57}}, schema=dict_schema)
    # Keys are casted.
    testing.assert_dicts_keys_equal(d, ds([42, 37], schema_constants.INT64))
    inner_d1 = d[42]
    inner_d2 = d[37]
    # Inner dict keys are not casted if they are primitives.
    testing.assert_dicts_keys_equal(inner_d1, ds(['a']))
    testing.assert_dicts_keys_equal(inner_d2, ds(['b']))

  def test_schema_arg_dict_schema_error(self):
    list_schema = fns.list_schema(item_schema=schema_constants.FLOAT32)
    with self.assertRaisesRegex(ValueError, 'expected Dict schema'):
      fns.new({'a': [1, 2, 3], 'b': [4, 5]}, schema=list_schema)

  def test_schema_arg_schema_with_fallback(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    fallback_bag = fns.bag()
    schema.with_bag(fallback_bag).set_attr('b', schema_constants.STRING)
    schema = schema.enriched(fallback_bag)
    x = fns.new(a=42, b='xyz', schema=schema)
    self.assertEqual(dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = fns.schema.new_schema(a=schema_constants.FLOAT32)
    x = fns.new(a=42, schema=schema)
    self.assertEqual(dir(x), ['a'])
    testing.assert_equal(
        x.a, ds(42, schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError, r'schema for attribute \'a\' is incompatible'
    ):
      fns.new(a='xyz', schema=schema)

  def test_schema_arg_supplement_succeeds(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    x = fns.new(a=42, b='xyz', schema=schema)
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_update_schema(self):
    schema = fns.schema.new_schema(a=schema_constants.FLOAT32)
    x = fns.new(a=42, b='xyz', schema=schema, update_schema=True)
    self.assertEqual(dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_update_schema_error(self):
    with self.assertRaisesRegex(TypeError, 'expected bool'):
      fns.new(a=42, schema=schema_constants.ANY, update_schema=42)  # pytype: disable=wrong-arg-types

  def test_schema_arg_update_schema_error_overwriting(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    x = fns.new(a='xyz', schema=schema, update_schema=True)
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_any(self):
    x = fns.new(a=1, b='a', schema=schema_constants.ANY)
    self.assertEqual(dir(x), [])
    testing.assert_equal(x.get_schema().no_bag(), schema_constants.ANY)
    testing.assert_equal(x.a, ds(1).as_any().with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('a').as_any().with_bag(x.get_bag()))

  def test_schema_contains_any(self):
    schema = fns.schema.new_schema(x=schema_constants.ANY)
    entity = fns.new()
    x = fns.new(x=entity, schema=schema)
    testing.assert_equal(x.x.no_bag(), entity.no_bag().as_any())

  def test_schema_arg_embed_schema(self):
    schema = fns.schema.new_schema(a=schema_constants.OBJECT)
    x = fns.new(a=fns.new(p=42, q='xyz'), schema=schema)
    self.assertEqual(dir(x), ['a'])
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        x.a.get_attr('__schema__').p.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        x.a.get_attr('__schema__').q.no_bag(), schema_constants.STRING
    )

  def test_str_as_schema_arg(self):
    x = fns.new(schema='name', a=42)
    expected_schema = fns.named_schema('name')
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    x = fns.new(schema=ds('name'), a=42)
    expected_schema = fns.named_schema('name')
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new(a=1, schema=5)
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new(a=1, schema=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      fns.new(a=1, schema=ds(['name']))
    with self.assertRaisesRegex(ValueError, 'schema can only be 0-rank'):
      fns.new(a=1, schema=ds([schema_constants.INT32, schema_constants.STRING]))
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got INT32'
    ):
      fns.new(a=1, schema=schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got OBJECT'
    ):
      fns.new(a=1, schema=schema_constants.OBJECT)

  def test_schema_error_message(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            r"""cannot create Item(s) with the provided schema: SCHEMA(a=INT32)

The cause is: the schema for attribute 'a' is incompatible.

Expected schema for 'a': INT32
Assigned schema for 'a': STRING

To fix this, explicitly override schema of 'a' in the original schema. For example,
schema.a = <desired_schema>"""
        ),
    ):
      fns.new(a='xyz', schema=schema)

    db1 = fns.bag()
    _ = db1.uuobj(x=1)
    db2 = fns.bag()
    b = db2.uuobj(x=1)
    b.x = 2
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot create Item\(s\)

The cause is: conflicting values for x for [0-9a-z]{32}:0: 1 vs 2""",
    ):
      db1.new(y=b)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.new(x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.new(x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.new(x=ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.new(x=ds([1, 2, 3]), dct={'a': 42})

  def test_universal_converter_into_itemid_is_not_supported(self):
    with self.assertRaisesRegex(
        NotImplementedError, 'do not support `itemid` in converter mode'
    ):
      _ = fns.new([1, 2, 3], itemid=kde.allocation.new_itemid._eval())

  def test_universal_converter_primitive(self):
    item = fns.new(42)
    testing.assert_equal(item.no_bag(), ds(42))
    item = fns.new(42, schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds(42.0))
    item = fns.new(ds([1, 2]), schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds([1.0, 2.0]))

  def test_universal_converter_primitive_casting_error(self):
    with self.assertRaisesRegex(ValueError, 'cannot cast BYTES to FLOAT32'):
      fns.new(b'xyz', schema=schema_constants.FLOAT32)

  def test_universal_converter_none(self):
    item = fns.new(None)
    testing.assert_equal(item.no_bag(), ds(None))
    item = fns.new(None, schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds(None, schema_constants.FLOAT32))
    schema = fns.schema.new_schema(
        a=schema_constants.STRING, b=fns.list_schema(schema_constants.INT32)
    )
    item = fns.new(None, schema=schema)
    testing.assert_equivalent(item.get_schema(), schema)
    testing.assert_equal(item.no_bag(), ds(None).with_schema(schema.no_bag()))

  def test_universal_converter_list(self):
    l = fns.new([1, 2, 3])
    testing.assert_equal(
        l.get_schema().get_attr('__items__').no_bag(), schema_constants.INT32
    )
    testing.assert_equal(l[:], ds([1, 2, 3]).with_bag(l.get_bag()))

    l = fns.list([[1, 2], [3]])
    entity = fns.new(l)
    testing.assert_equivalent(entity, l)

  def test_universal_converter_empty_list(self):
    l = fns.new([])
    testing.assert_equal(
        l.get_schema().get_attr('__items__').no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(l[:].no_bag(), ds([]))

  def test_universal_converter_dict(self):
    d = fns.new({'a': 42, 'b': ds(37, schema_constants.INT64)})
    testing.assert_equal(
        d.get_schema().get_attr('__keys__').no_bag(), schema_constants.STRING
    )
    testing.assert_equal(
        d.get_schema().get_attr('__values__').no_bag(), schema_constants.INT64
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(
        d[['a', 'b']],
        ds([42, 37], schema_constants.INT64).with_bag(d.get_bag()),
    )

    d = fns.dict({'a': ds(42, schema_constants.INT64), 'b': 37})
    entity = fns.new(d)
    testing.assert_equivalent(entity, d)

  def test_universal_converter_empty_dict(self):
    d = fns.new({})
    testing.assert_equal(
        d.get_schema().get_attr('__keys__').no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        d.get_schema().get_attr('__values__').no_bag(), schema_constants.OBJECT
    )
    testing.assert_dicts_keys_equal(d, ds([]))
    testing.assert_equal(d[ds([])].no_bag(), ds([]))

  def test_universal_converter_dict_keys_conflict(self):
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      fns.new({fns.new(): 42, fns.new(): 37})

  def test_universal_converter_list_of_complex(self):
    l = fns.new([{'a': 42}, {'b': 57}])
    testing.assert_equal(
        l.get_schema().no_bag(),
        fns.list_schema(
            fns.dict_schema(schema_constants.STRING, schema_constants.INT32)
        ).no_bag(),
    )
    dicts = l[:]
    testing.assert_dicts_keys_equal(dicts, ds([['a'], ['b']]))
    testing.assert_equal(
        dicts[ds(['a', 'b'])], ds([42, 57]).with_bag(l.get_bag())
    )

  def test_universal_converter_list_of_different_primitive_lists(self):
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      fns.new([[1, 2], [3.14]])
    fns.new(
        [[1, 2], [3.14]],
        schema=fns.list_schema(fns.list_schema(schema_constants.FLOAT32))
    )

  def test_universal_converter_container_contains_multi_dim_data_slice(self):
    with self.assertRaisesRegex(
        ValueError, 'dict / list containing multi-dim DataSlice'
    ):
      fns.new([ds([1, 2, 3]), 42])
    with self.assertRaisesRegex(
        ValueError, 'dict / list containing multi-dim DataSlice'
    ):
      fns.new({42: ds([1, 2, 3])})

  def test_universal_converter_tuple_as_list(self):
    l = fns.new(tuple([1, 2, 3]))
    testing.assert_equal(l[:].no_bag(), ds([1, 2, 3]))

  def test_universal_converter_deep_schema(self):
    s = fns.list_schema(
        fns.dict_schema(
            schema_constants.STRING,
            fns.list_schema(schema_constants.FLOAT32),
        )
    )
    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for Dict key is incompatible'
    ):
      fns.new([{b'x': [1, 2, 3]}], schema=s)
    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for List item is incompatible'
    ):
      fns.new([{'x': [1, 'x', 3]}], schema=s)

    l = fns.new([{'x': [1, 3.14, 3]}], schema=s)
    testing.assert_dicts_keys_equal(l[0], ds(['x']))
    testing.assert_allclose(l[0]['x'][:].no_bag(), ds([1.0, 3.14, 3.0]))

  def test_universal_converter_deep_schema_caching(self):
    t = tuple([1, 2, 3])
    t = tuple([1, 2, 3])
    s = fns.dict_schema(
        fns.list_schema(schema_constants.INT32),
        fns.list_schema(schema_constants.FLOAT32),
    )
    d = fns.new({t: t}, schema=s)
    testing.assert_equal(d.get_keys()[:].no_bag(), ds([[1, 2, 3]]))
    testing.assert_equal(d[d.get_keys()][:].no_bag(), ds([[1.0, 2.0, 3.0]]))

  def test_universal_converter_deep_schema_with_nested_object_schema(self):
    s = fns.list_schema(
        fns.dict_schema(schema_constants.STRING, schema_constants.OBJECT),
    )
    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for Dict key is incompatible'
    ):
      fns.new([{b'x': [1, 2, 3]}], schema=s)

    l = fns.new([{'x': [1, 3.14, 3]}, {'y': 42}, {'z': {'a': 42}}], schema=s)
    testing.assert_allclose(l[0]['x'][:].no_bag(), ds([1, 3.14, 3]))
    testing.assert_equal(l[1]['y'].no_bag(), ds(42, schema_constants.OBJECT))
    testing.assert_dicts_keys_equal(l[2]['z'], ds(['a']))

  def test_universal_converter_entity(self):
    with self.subTest('item'):
      entity = fns.new(a=42, b='abc')
      new_entity = fns.new(entity)
      with self.assertRaises(AssertionError):
        testing.assert_equal(entity.get_bag(), new_entity.get_bag())
      testing.assert_equivalent(new_entity, entity)
    with self.subTest('slice'):
      entity = fns.new(a=ds([1, 2]), b='abc')
      new_entity = fns.new(entity)
      with self.assertRaises(AssertionError):
        testing.assert_equal(entity.get_bag(), new_entity.get_bag())
      testing.assert_equivalent(new_entity, entity)

  def test_universal_converter_adopt_bag_data(self):
    nested = fns.obj(a=42, b='abc')
    entity = fns.new([1, 2, nested])
    with self.assertRaises(AssertionError):
      testing.assert_equal(nested.get_bag(), entity.get_bag())
    testing.assert_equal(entity[2].a, ds(42).with_bag(entity.get_bag()))

  def test_universal_converter_adopt_bag_schema(self):
    schema = fns.list_schema(schema_constants.FLOAT32)
    entity = fns.new([1, 2, 3], schema=schema)
    with self.assertRaises(AssertionError):
      testing.assert_equal(schema.get_bag(), entity.get_bag())
    testing.assert_equal(
        entity[:].get_schema().no_bag(), schema_constants.FLOAT32
    )
    testing.assert_equal(entity[:].no_bag(), ds([1.0, 2.0, 3.0]))

  def test_universal_converter_with_cross_ref_schema_conflict(self):
    d1 = {'a': 42}
    d2 = {'b': 37}
    d1['d'] = d2
    d = {'d1': d1, 'd2': d2}
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      fns.new(d)

  def test_universal_converter_recursive_object_error(self):
    d = {'a': 42}
    d['self'] = d
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.new(d)
    # Deeper recursion:
    d2 = {'a': {'b': d}}
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.new(d2)
    # Longer cycle:
    d = {'a': 42}
    bottom_d = d
    for i in range(5):
      d = {f'd{i}': d}
    level_1_d = d
    d = {'top': d}
    bottom_d['cycle'] = level_1_d
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.new(d2)
    # Cycle in list:
    l = [[1, 2], 3]
    l[0].append(l)
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.new(l)

  def test_universal_converter_with_attrs(self):
    with self.assertRaisesRegex(
        TypeError, 'cannot set extra attributes when converting to entity'
    ):
      fns.new([1, 2, 3], a=42)

  def test_alias(self):
    self.assertIs(fns.new, fns.core.new)


if __name__ == '__main__':
  absltest.main()
