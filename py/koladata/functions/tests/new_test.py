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
from arolla import arolla
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

  def test_mutability(self):
    self.assertFalse(fns.new().is_mutable())

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
    itemid = kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]])).eval()
    x = fns.new(a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=42).get_itemid()
    assert itemid.has_bag()
    # Successful.
    x = fns.new(a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = x.non_existent

  def test_schema_arg_simple(self):
    schema = kde.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    ).eval()
    x = fns.new(a=42, b='xyz', schema=schema)
    self.assertEqual(fns.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_deep(self):
    nested_schema = kde.schema.new_schema(p=schema_constants.BYTES).eval()
    schema = kde.schema.new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
        nested=nested_schema,
    ).eval()
    x = fns.new(
        a=42,
        b='xyz',
        nested=fns.new(p=b'0123', schema=nested_schema),
        schema=schema,
    )
    self.assertEqual(fns.dir(x), ['a', 'b', 'nested'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)
    testing.assert_equal(x.nested.p, ds(b'0123').with_bag(x.get_bag()))
    testing.assert_equal(
        x.nested.get_schema().p.no_bag(), schema_constants.BYTES
    )

  def test_schema_arg_list(self):
    list_schema = kde.list_schema(item_schema=schema_constants.FLOAT32).eval()
    l = fns.new([1, 2, 3], schema=list_schema)
    testing.assert_equal(l[:], ds([1.0, 2.0, 3.0]).with_bag(l.get_bag()))

  def test_schema_arg_dict(self):
    dict_schema = kde.dict_schema(
        key_schema=schema_constants.STRING, value_schema=schema_constants.INT64
    ).eval()
    d = fns.new({'a': 37, 'b': 42}, schema=dict_schema)
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(
        d[ds(['a', 'b'])],
        ds([37, 42], schema_constants.INT64).with_bag(d.get_bag()),
    )

  def test_schema_arg_dict_in_list(self):
    list_schema = kde.list_schema(item_schema=schema_constants.OBJECT).eval()
    d = fns.new([{'a': 32}, {'b': 64}], schema=list_schema)
    testing.assert_dicts_keys_equal(
        d[:], ds([['a'], ['b']], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        d[:], ds([[32], [64]], schema_constants.OBJECT)
    )

  def test_schema_arg_dict_deep(self):
    with self.subTest('object value'):
      dict_schema = kde.dict_schema(
          key_schema=schema_constants.INT64,
          value_schema=schema_constants.OBJECT,
      ).eval()
      d = fns.new({42: {'a': 32}, 37: {'b': 57}}, schema=dict_schema)
      # Keys are casted.
      testing.assert_dicts_keys_equal(d, ds([42, 37], schema_constants.INT64))
      inner_d1 = d[42]
      inner_d2 = d[37]
      testing.assert_dicts_keys_equal(
          inner_d1, ds(['a'], schema_constants.OBJECT)
      )
      testing.assert_dicts_keys_equal(
          inner_d2, ds(['b'], schema_constants.OBJECT)
      )

    with self.subTest('object key'):
      dict_schema = kde.dict_schema(
          key_schema=schema_constants.OBJECT,
          value_schema=schema_constants.INT64,
      ).eval()
      key1 = fns.obj(x=1)
      key2 = fns.obj(y=2)

      d = fns.new({key1: 123, key2: 456}, schema=dict_schema)
      testing.assert_dicts_keys_equal(
          d,
          ds([key1, key2], schema_constants.OBJECT).with_bag(d.get_bag()),
      )

  def test_schema_arg_dict_schema_error(self):
    list_schema = kde.list_schema(item_schema=schema_constants.FLOAT32).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Python Dict can be converted to either Entity or Dict, got schema:'
            ' DataItem(LIST[FLOAT32]'
        ),
    ):
      fns.new({'a': [1, 2, 3], 'b': [4, 5]}, schema=list_schema)

  def test_schema_arg_schema_with_fallback(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    fallback_bag = fns.mutable_bag()
    schema.with_bag(fallback_bag).set_attr('b', schema_constants.STRING)
    schema = schema.enriched(fallback_bag)
    x = fns.new(a=42, b='xyz', schema=schema)
    self.assertEqual(fns.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32).eval()
    x = fns.new(a=42, schema=schema)
    self.assertEqual(fns.dir(x), ['a'])
    testing.assert_equal(
        x.a, ds(42, schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "schema for attribute 'a' is incompatible"
        ),
    ):
      fns.new(a='xyz', schema=schema)

  def test_schema_arg_supplement_succeeds(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    x = fns.new(a=42, b='xyz', schema=schema)
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_overwrite_schema(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32).eval()
    x = fns.new(a=42, b='xyz', schema=schema, overwrite_schema=True)
    self.assertEqual(fns.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_overwrite_schema_error(self):
    with self.assertRaisesRegex(TypeError, 'expected bool'):
      fns.new(a=42, schema=schema_constants.INT32, overwrite_schema=42)  # pytype: disable=wrong-arg-types

  def test_schema_arg_overwrite_schema_error_overwriting(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    x = fns.new(a='xyz', schema=schema, overwrite_schema=True)
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_embed_schema(self):
    schema = kde.schema.new_schema(a=schema_constants.OBJECT).eval()
    x = fns.new(a=fns.new(p=42, q='xyz'), schema=schema)
    self.assertEqual(fns.dir(x), ['a'])
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        x.a.get_attr('__schema__').p.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        x.a.get_attr('__schema__').q.no_bag(), schema_constants.STRING
    )

  def test_str_as_schema_arg(self):
    x = fns.new(schema='name', a=42)
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    x = fns.new(schema=ds('name'), a=42)
    expected_schema = kde.named_schema('name').eval()
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
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected Entity schema, got INT32'
        ),
    ):
      fns.new(a=1, schema=schema_constants.INT32)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected Entity schema, got OBJECT'
        ),
    ):
      fns.new(a=1, schema=schema_constants.OBJECT)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected Entity schema, got DICT{STRING, INT32}')
        ),
    ):
      fns.new(
          a=1,
          schema=kde.dict_schema(
              schema_constants.STRING, schema_constants.INT32
          ).eval(),
      )

  def test_schema_error_message(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                r"""the schema for attribute 'a' is incompatible.

Expected schema for 'a': INT32
Assigned schema for 'a': STRING

To fix this, explicitly override schema of 'a' in the original schema by passing overwrite_schema=True."""
            )
        ),
    ) as cm:
      fns.new(a='xyz', schema=schema)
    self.assertRegex(
        str(cm.exception),
        re.escape(
            'cannot create Item(s) with the provided schema: ENTITY(a=INT32)'
        ),
    )

    db = fns.mutable_bag()
    nested_schema = db.new_schema(b=schema)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r"""the schema for attribute 'b' is incompatible.

Expected schema for 'b': ENTITY\(a=INT32\) with ItemId \$[0-9a-zA-Z]{22}
Assigned schema for 'b': ENTITY\(a=INT32\) with ItemId \$[0-9a-zA-Z]{22}

To fix this, explicitly override schema of 'b' in the original schema by passing overwrite_schema=True.""",
        ),
    ) as cm:
      fns.new(b=db.new(a=123), schema=nested_schema)
    self.assertRegex(
        str(cm.exception),
        re.escape(
            'cannot create Item(s) with the provided schema:'
            ' ENTITY(b=ENTITY(a=INT32))'
        ),
    )

    db1 = fns.mutable_bag()
    _ = db1.uuobj(x=1)
    db2 = fns.mutable_bag()
    b = db2.uuobj(x=1)
    b.x = 2
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r"""cannot merge DataBags due to an exception encountered when merging entities.

The conflicting entities in the both DataBags: Entity\(\):\#[0-9a-zA-Z]{22}

The cause is the values of attribute 'x' are different: 1 vs 2""",
        ),
    ) as cm:
      db1.new(y=b)
    self.assertRegex(
        str(cm.exception),
        re.escape('cannot create Item(s)'),
    )

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

  def test_universal_converter_primitive(self):
    item = fns.new(42)
    testing.assert_equal(item.no_bag(), ds(42))
    item = fns.new(42, schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds(42.0))
    item = fns.new(ds([1, 2]), schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds([1.0, 2.0]))

  def test_universal_converter_primitive_casting_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: BYTES
assigned schema: MASK"""),
    ):
      fns.new(b'xyz', schema=schema_constants.MASK)

  def test_universal_converter_none(self):
    item = fns.new(None)
    testing.assert_equal(item.no_bag(), ds(None))
    item = fns.new(None, schema=schema_constants.FLOAT32)
    testing.assert_equal(item.no_bag(), ds(None, schema_constants.FLOAT32))
    schema = kde.schema.new_schema(
        a=schema_constants.STRING, b=kde.list_schema(schema_constants.INT32)
    ).eval()
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
        l.get_schema().get_attr('__items__').no_bag(), schema_constants.NONE
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
        d[ds(['a', 'b'])],
        ds([42, 37], schema_constants.INT64).with_bag(d.get_bag()),
    )

    d = fns.dict({'a': ds(42, schema_constants.INT64), 'b': 37})
    entity = fns.new(d)
    testing.assert_equivalent(entity, d)

  def test_universal_converter_empty_dict(self):
    d = fns.new({})
    testing.assert_equal(
        d.get_schema().get_attr('__keys__').no_bag(), schema_constants.NONE
    )
    testing.assert_equal(
        d.get_schema().get_attr('__values__').no_bag(), schema_constants.NONE
    )
    testing.assert_dicts_keys_equal(d, ds([]))
    testing.assert_equal(d[ds([])].no_bag(), ds([]))

  def test_universal_converter_dict_keys_conflict(self):
    with self.assertRaisesRegex(ValueError, 'cannot find a common schema'):
      fns.new({fns.new(): 42, fns.new(): 37})

  def test_universal_converter_list_of_complex(self):
    l = fns.new([{'a': 42}, {'b': 57}])
    testing.assert_equal(
        l.get_schema().no_bag(),
        kde.list_schema(
            kde.dict_schema(schema_constants.STRING, schema_constants.INT32)
        ).eval().no_bag(),
    )
    dicts = l[:]
    testing.assert_dicts_keys_equal(dicts, ds([['a'], ['b']]))
    testing.assert_equal(
        dicts[ds(['a', 'b'])], ds([42, 57]).with_bag(l.get_bag())
    )

  def test_universal_converter_list_of_different_primitive_lists(self):
    with self.assertRaisesRegex(ValueError, 'cannot find a common schema'):
      fns.new([[1, 2], [3.14]])
    fns.new(
        [[1, 2], [3.14]],
        schema=kde.list_schema(kde.list_schema(schema_constants.FLOAT32)).eval()
    )

  def test_universal_converter_container_contains_multi_dim_data_slice(self):
    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      fns.new([ds([1, 2, 3]), 42])
    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      fns.new({42: ds([1, 2, 3])})

  def test_universal_converter_tuple_as_list(self):
    l = fns.new(tuple([1, 2, 3]))
    testing.assert_equal(l[:].no_bag(), ds([1, 2, 3]))

  def test_universal_converter_deep_schema(self):
    s = kde.list_schema(
        kde.dict_schema(
            schema_constants.STRING,
            kde.list_schema(schema_constants.FLOAT32),
        )
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: BYTES
assigned schema: STRING"""),
    ):
      fns.new([{b'x': [1, 2, 3]}], schema=s)
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: OBJECT
assigned schema: FLOAT32"""),
    ):
      fns.new([{'x': [1, 'x', 3]}], schema=s)

    l = fns.new([{'x': [1, 3.14, 3]}], schema=s)
    testing.assert_dicts_keys_equal(l[0], ds(['x']))
    testing.assert_allclose(l[0]['x'][:].no_bag(), ds([1.0, 3.14, 3.0]))

  def test_universal_converter_deep_schema_caching(self):
    t = tuple([1, 2, 3])
    s = kde.dict_schema(
        kde.list_schema(schema_constants.INT32),
        kde.list_schema(schema_constants.FLOAT32),
    ).eval()
    d = fns.new({t: t}, schema=s)
    testing.assert_equal(d.get_keys()[:].no_bag(), ds([[1, 2, 3]]))
    testing.assert_equal(d[d.get_keys()][:].no_bag(), ds([[1.0, 2.0, 3.0]]))

  def test_universal_converter_deep_schema_with_nested_object_schema(self):
    s = kde.list_schema(
        kde.dict_schema(schema_constants.STRING, schema_constants.OBJECT),
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('''the schema is incompatible:
expected schema: BYTES
assigned schema: STRING'''),
    ):
      fns.new([{b'x': [1, 2, 3]}], schema=s)

    l = fns.new([{'x': [1, 3.14, 3]}, {'y': 42}, {'z': {'a': 42}}], schema=s)
    testing.assert_equal(
        l[0]['x'][:].no_bag(), ds([1, 3.14, 3], schema_constants.OBJECT)
    )
    testing.assert_equal(l[1]['y'].no_bag(), ds(42, schema_constants.OBJECT))
    testing.assert_dicts_keys_equal(
        l[2]['z'], ds(['a'], schema_constants.OBJECT)
    )

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
    schema = kde.list_schema(schema_constants.FLOAT32).eval()
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
    with self.assertRaisesRegex(ValueError, 'cannot find a common schema'):
      fns.new(d)

  def test_universal_converter_with_itemid(self):
    itemid = kde.uuid_for_list('new').eval()
    res = fns.new([{'a': 42, 'b': 17}, {'c': 12}], itemid=itemid)
    child_itemid = kde.uuid_for_dict(
        '__from_py_child__', parent=itemid,
        list_item_index=ds([0, 1], schema_constants.INT64)
    ).eval()
    testing.assert_equal(res.no_bag().get_itemid(), itemid)
    testing.assert_dicts_keys_equal(res[:], ds([['a', 'b'], ['c']]))
    testing.assert_equal(res[:].no_bag().get_itemid(), child_itemid)

    with self.assertRaisesRegex(
        ValueError, 'itemid argument to list creation, requires List ItemIds'
    ):
      _ = fns.new([1, 2], itemid=kde.allocation.new_itemid().eval())

    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      _ = fns.new(ds([1, 2, 3]), itemid=kde.allocation.new_itemid().eval())

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
    self.assertIs(fns.new, fns.entities.new)


if __name__ == '__main__':
  absltest.main()
