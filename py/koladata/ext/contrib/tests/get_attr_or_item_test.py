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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata import kd_ext

I = kd.I
kde = kd_ext.lazy
ds = kd.slice
get_attr_or_item = kd_ext.contrib.get_attr_or_item  # pyrefly: ignore[missing-attribute]

_ENTITY_KEY = kd.new(a=1)
_MIXED_OBJECTS = ds(
    [
        kd.obj(x=42),
        kd.obj(y=10),
        kd.obj(kd.list([1, 2, 3])),
        kd.dict({'x': 'hello'}),
        kd.dict({'x': 100}, key_schema=kd.OBJECT),
        kd.dict({'x': 7, 1: 8}, key_schema=kd.OBJECT, value_schema=kd.OBJECT),
        kd.dict({1: 'a'}),
        kd.dict({1: 42}, key_schema=kd.INT64),
        kd.dict({1: 'obj_key'}, key_schema=kd.OBJECT),
        kd.dict(kd.int64(1), 'obj_key64', key_schema=kd.OBJECT),
        kd.dict({b'k': 'bytes'}),
        kd.dict({True: 'bool'}),
        kd.dict(kd.present, 'mask'),
        kd.dict(_ENTITY_KEY, 'entity'),
        kd.dict(_ENTITY_KEY, 'obj_entity', key_schema=kd.OBJECT),
        None,
    ],
    schema=kd.OBJECT,
)


class GetAttrOrItemTest(parameterized.TestCase):

  def test_explicit_entity(self):
    e = kd.new(x=ds([1, 2]), y=ds(['a', 'b']))
    kd.testing.assert_equivalent(get_attr_or_item(e, 'x'), ds([1, 2]))
    kd.testing.assert_equivalent(
        get_attr_or_item(e, kd.obj('y')), ds(['a', 'b'])
    )

    nested = kd.new(inner=e)
    res = get_attr_or_item(nested, 'inner')
    kd.testing.assert_equivalent(res, e, ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), nested.get_bag())

    with self.assertRaisesRegex(ValueError, "the attribute 'z' is missing"):
      get_attr_or_item(e, 'z')

    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item holding STRING'
    ):
      get_attr_or_item(e, 0)
    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item holding STRING'
    ):
      get_attr_or_item(e, ds(None))

  def test_explicit_dict(self):
    d_str = kd.dict({'a': 10, 'b': 20})
    kd.testing.assert_equivalent(get_attr_or_item(d_str, 'a'), ds(10))
    kd.testing.assert_equivalent(
        get_attr_or_item(d_str, 'missing'), ds(None, schema=kd.INT32)
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(d_str, ds(None)), ds(None, schema=kd.INT32)
    )

    d_int64 = kd.dict({1: 'x', 2: 'y'}, key_schema=kd.INT64)
    kd.testing.assert_equivalent(
        get_attr_or_item(d_int64, kd.int32(1)), ds('x')
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(d_int64, kd.int64(1)), ds('x')
    )

    d_int32 = kd.dict({1: 'x', 2: 'y'}, key_schema=kd.INT32)
    kd.testing.assert_equivalent(get_attr_or_item(d_int32, 2), ds('y'))
    with self.assertRaisesRegex(ValueError, 'incompatible'):
      get_attr_or_item(d_int32, kd.int64(2))

    with self.assertRaises(ValueError):
      get_attr_or_item(d_str, 1)

    with self.assertRaises(ValueError):
      get_attr_or_item(d_int32, 'a')

  def test_explicit_list(self):
    l = kd.list([10, 20, 30])
    kd.testing.assert_equivalent(get_attr_or_item(l, 1), ds(20))
    kd.testing.assert_equivalent(get_attr_or_item(l, -1), ds(30))
    kd.testing.assert_equivalent(get_attr_or_item(l, kd.obj(0)), ds(10))
    kd.testing.assert_equivalent(
        get_attr_or_item(l, 10), ds(None, schema=kd.INT32)
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(l, ds(None)), ds(None, schema=kd.INT32)
    )

    l_2d = ds([kd.list([1, 2]), kd.list([3])])
    kd.testing.assert_equivalent(get_attr_or_item(l_2d, 1), ds([2, None]))

    with self.assertRaisesRegex(
        ValueError,
        'cannot get items from list\\(s\\): expected indices to be integers',
    ):
      get_attr_or_item(l, 'a')

  def test_none_schema(self):
    kd.testing.assert_equal(get_attr_or_item(ds(None), 'x'), ds(None))
    kd.testing.assert_equal(
        get_attr_or_item(ds([None, None]), 0), ds([None, None])
    )

  def test_object_entity_scalar(self):
    obj_entity = kd.obj(x=42)
    kd.testing.assert_equivalent(get_attr_or_item(obj_entity, 'x'), ds(42))
    kd.testing.assert_equivalent(get_attr_or_item(obj_entity, 'y'), ds(None))
    kd.testing.assert_equivalent(get_attr_or_item(obj_entity, 0), ds(None))
    kd.testing.assert_equivalent(
        get_attr_or_item(obj_entity, ds(None)),
        ds(None),
    )

  def test_object_dict_scalar(self):
    obj_dict = kd.obj(kd.dict({'x': 99}))
    kd.testing.assert_equivalent(get_attr_or_item(obj_dict, 'x'), ds(99))
    kd.testing.assert_equivalent(get_attr_or_item(obj_dict, 'y'), ds(None))
    kd.testing.assert_equivalent(get_attr_or_item(obj_dict, 0), ds(None))

    # OBJECT value schema is kept.
    obj_values = kd.obj(kd.dict({'a': 1}, value_schema=kd.OBJECT))
    kd.testing.assert_equivalent(
        get_attr_or_item(obj_values, 'a'), ds(1, kd.OBJECT)
    )
    inner = kd.obj(b=1)
    obj_values = kd.obj(kd.dict({'a': inner}, value_schema=kd.OBJECT))
    kd.testing.assert_equivalent(
        get_attr_or_item(obj_values, 'a'), inner, ids_equality=True
    )

  def test_object_dict_entity_key(self):
    keys = kd.new(a=ds([1, 2]))
    d = ds([
        kd.obj(kd.dict(keys, ds([1, 3]), key_schema=kd.OBJECT)),
        kd.obj(kd.dict(keys, ds([2, 4]))),
    ])
    kd.testing.assert_equivalent(get_attr_or_item(d, keys.S[0]), ds([1, 2]))
    kd.testing.assert_equivalent(get_attr_or_item(d.S[0], keys.S[0]), ds(1))
    # Primitive keys are not present in the dicts.
    kd.testing.assert_equivalent(get_attr_or_item(d, 'x'), ds([None, None]))

  def test_object_dict_entity_key_schema_is_ignored(self):
    keys = kd.new(a=ds([1, 2]))
    d = ds([
        kd.obj(kd.dict(keys, ds([1, 3]), key_schema=kd.OBJECT)),
        kd.obj(kd.dict(keys, ds([2, 4]))),
    ])
    for key in [
        kd.get_itemid(keys.S[0]),
        kd.schema.unsafe_with_schema(
            keys.S[0], kd.schema.new_schema(b=kd.INT32).no_bag()
        ),
    ]:
      with self.subTest(key=key):
        kd.testing.assert_equivalent(get_attr_or_item(d, key), ds([1, 2]))
        kd.testing.assert_equivalent(get_attr_or_item(d.S[1], key), ds(2))

  def test_object_nested_entity(self):
    inner = kd.new(x=ds([1, 2]))
    o = kd.obj(inner=inner)
    res = get_attr_or_item(o, 'inner')
    kd.testing.assert_equivalent(res, inner, ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), o.get_bag())

    res = get_attr_or_item(o.S[0], 'inner')
    kd.testing.assert_equivalent(res, inner.S[0], ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), o.get_bag())

  @parameterized.parameters(
      'x',
      'y',
      1,
      -1,
      kd.int64(1),
      kd.obj(1),
      b'k',
      True,
      kd.present,
      _ENTITY_KEY,
      1.5,
      ds(None),
  )
  def test_object_item_consistent_with_slice(self, key):
    vectorized_res = get_attr_or_item(_MIXED_OBJECTS, key)
    for i in range(_MIXED_OBJECTS.get_size()):
      with self.subTest(i=i):
        scalar_res = get_attr_or_item(_MIXED_OBJECTS.S[i], key)
        # kd.obj keeps primitive dtypes, so dtype mismatches are detected.
        kd.testing.assert_equivalent(
            kd.obj(scalar_res), kd.obj(vectorized_res.S[i])
        )

  def test_object_attr_missing_in_schema(self):
    e = kd.new(x=1, y=2)
    # The entity has `y` data, but its schema has no `y` attribute.
    o = kd.obj(
        kd.schema.unsafe_with_schema(e, kd.schema.new_schema(x=kd.INT32))
    )
    kd.testing.assert_equivalent(get_attr_or_item(o, 'y'), ds(None))
    # Adoption follows the schema, so `y` data is added back.
    o = ds([o, kd.obj(y=3)]).enriched(e.get_bag())
    kd.testing.assert_equivalent(get_attr_or_item(o, 'y'), ds([None, 3]))

  def test_object_lists(self):
    l = ds([
        kd.obj(kd.list([1, 2, 3])),
        kd.obj(kd.list([4, None])),
        kd.obj(kd.list([])),
        None,
    ])
    kd.testing.assert_equivalent(get_attr_or_item(l, 0), ds([1, 4, None, None]))
    kd.testing.assert_equivalent(
        get_attr_or_item(l, 1), ds([2, None, None, None])
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(l, -1), ds([3, None, None, None])
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(l, kd.int64(1)), ds([2, None, None, None])
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(l, kd.obj(1)), ds([2, None, None, None])
    )
    kd.testing.assert_equivalent(get_attr_or_item(l, 5), ds([None] * 4))
    # Non-integer keys result in missing values.
    kd.testing.assert_equivalent(get_attr_or_item(l, 'x'), ds([None] * 4))
    kd.testing.assert_equivalent(get_attr_or_item(l, 1.0), ds([None] * 4))
    kd.testing.assert_equivalent(get_attr_or_item(l, ds(None)), ds([None] * 4))

    kd.testing.assert_equivalent(get_attr_or_item(l.S[0], -1), ds(3))
    kd.testing.assert_equivalent(get_attr_or_item(l.S[0], 5), ds(None))
    kd.testing.assert_equivalent(get_attr_or_item(l.S[0], 'x'), ds(None))

    l_2d = ds([[kd.obj(kd.list([1, 2]))], [kd.obj(kd.list([3])), None]])
    kd.testing.assert_equivalent(
        get_attr_or_item(l_2d, 1), ds([[2], [None, None]])
    )

  def test_object_list_nested_entity(self):
    inner = kd.new(x=ds([[1, 2], [3]]))
    l = kd.obj(kd.implode(inner))
    res = get_attr_or_item(l, 0)
    kd.testing.assert_equivalent(res, inner.S[:, 0], ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), l.get_bag())

    res = get_attr_or_item(l.S[0], -1)
    kd.testing.assert_equivalent(res, inner.S[0, -1], ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), l.get_bag())

  def test_object_mixed_entities_lists_and_dicts(self):
    mixed = ds([
        kd.obj(a=1),
        kd.obj(kd.list([10, 20])),
        kd.obj(kd.dict({'a': 2, 1: 3}, key_schema=kd.OBJECT)),
        None,
    ])
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, 'a'), ds([1, None, 2, None])
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, 1), ds([None, 20, 3, None])
    )
    kd.testing.assert_equivalent(get_attr_or_item(mixed, 'b'), ds([None] * 4))

  def test_object_mixed_slice_string_key(self):
    mixed = ds(
        [
            kd.obj(x=42),
            kd.dict({'x': 'hello'}),
            kd.dict({'x': 100}, key_schema=kd.OBJECT),
            kd.obj(y=10),
            kd.dict({1: 100}),
            kd.obj(kd.list([1, 2, 3])),
            None,
        ],
        schema=kd.OBJECT,
    )
    res = get_attr_or_item(mixed, 'x')
    self.assertEqual(res.get_schema(), kd.OBJECT)
    kd.testing.assert_equivalent(
        res,
        ds([42, 'hello', 100, None, None, None, None], schema=kd.OBJECT),
    )

  def test_object_mixed_slice_int_key(self):
    mixed = ds(
        [
            kd.dict({1: 'a'}),
            kd.dict({1: 42}, key_schema=kd.INT64),
            kd.dict({1: 'obj_key'}, key_schema=kd.OBJECT),
            kd.dict(kd.int64(1), 'obj_key64', key_schema=kd.OBJECT),
            kd.obj(kd.list([10, 20, 30])),
            kd.dict({'x': 10}),
            kd.obj(a=1),
            None,
        ],
        schema=kd.OBJECT,
    )
    res = get_attr_or_item(mixed, 1)
    self.assertEqual(res.get_schema(), kd.OBJECT)
    kd.testing.assert_equivalent(
        res,
        ds(
            ['a', 42, 'obj_key', 'obj_key64', 20, None, None, None],
            schema=kd.OBJECT,
        ),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, kd.obj(1)),
        ds(
            ['a', 42, 'obj_key', 'obj_key64', 20, None, None, None],
            schema=kd.OBJECT,
        ),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, kd.int64(1)),
        ds(
            [None, 42, 'obj_key', 'obj_key64', 20, None, None, None],
            schema=kd.OBJECT,
        ),
    )

  def test_object_mixed_slice_other_keys(self):
    e = kd.new(a=1)
    mixed = ds(
        [
            kd.dict({b'k': 'bytes'}),
            kd.dict({True: 'bool'}),
            kd.dict(kd.present, 'mask'),
            kd.dict(e, 'entity'),
            kd.dict(e, 'obj_entity', key_schema=kd.OBJECT),
            kd.dict(
                {b'k': 'obj_bytes', True: 'obj_bool'}, key_schema=kd.OBJECT
            ),
            kd.obj(kd.new(k=1)),
            kd.obj(kd.list([1, 2])),
        ],
        schema=kd.OBJECT,
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, b'k'),
        ds(['bytes', None, None, None, None, 'obj_bytes', None, None]),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, True),
        ds([None, 'bool', None, None, None, 'obj_bool', None, None]),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, kd.present),
        ds([None, None, 'mask', None, None, None, None, None]),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed, e),
        ds([None, None, None, 'entity', 'obj_entity', None, None, None]),
    )
    # Float keys are not supported by dicts.
    kd.testing.assert_equivalent(get_attr_or_item(mixed, 1.5), ds([None] * 8))

  def test_object_multidim_and_empty(self):
    mixed_2d = ds(
        [
            [kd.obj(a=1), kd.dict({'a': 'two'})],
            [kd.obj(kd.list([10, 20]))],
        ],
        schema=kd.OBJECT,
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed_2d, 'a'),
        ds([[1, 'two'], [None]], schema=kd.OBJECT),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed_2d, 1),
        ds([[None, None], [20]]),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(mixed_2d, ds(None)),
        ds([[None, None], [None]]),
    )

    empty_obj = ds([], schema=kd.OBJECT)
    kd.testing.assert_equal(get_attr_or_item(empty_obj, 'a'), ds([]))
    empty_item = kd.obj(None)
    kd.testing.assert_equivalent(get_attr_or_item(empty_item, 'a'), ds(None))

  def test_object_result_schema(self):
    kd.testing.assert_equivalent(
        get_attr_or_item(ds([kd.obj(a='x'), kd.obj(kd.dict({'a': 'y'}))]), 'a'),
        ds(['x', 'y']),
    )

    # Different primitive dtypes are not casted to a common schema.
    kd.testing.assert_equivalent(
        get_attr_or_item(ds([kd.obj(a=1), kd.obj(a=2.5)]), 'a'),
        ds([1, 2.5], schema=kd.OBJECT),
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(ds([kd.obj(a=1), kd.obj(a=kd.int64(2))]), 'a'),
        ds([1, kd.int64(2)], schema=kd.OBJECT),
    )

    # Different stored schemas result in OBJECT even for the same dtype.
    kd.testing.assert_equivalent(
        get_attr_or_item(
            ds(
                [kd.obj(a=1), kd.obj(kd.dict({'a': 2}, value_schema=kd.OBJECT))]
            ),
            'a',
        ),
        ds([1, 2], schema=kd.OBJECT),
    )

    # Mix of primitives and objects.
    inner = kd.obj(b=2)
    kd.testing.assert_equivalent(
        get_attr_or_item(ds([kd.obj(a=1), kd.obj(a=inner)]), 'a'),
        ds([1, inner]),
        ids_equality=True,
    )

  def test_object_struct_schema_embedding(self):
    # Slice of OBJECTs containing nested explicit entities, lists, and dicts
    # with incompatible schemas.
    e1 = kd.new(x=10)
    e2 = kd.new(y='hello')
    l = kd.list([1, 2, 3])
    target = ds([
        kd.obj(nested=e1),
        kd.obj(nested=e2),
        kd.obj(kd.dict({'nested': l})),
    ])

    kd.testing.assert_equivalent(
        get_attr_or_item(target, 'nested'),
        ds([kd.obj(e1), kd.obj(e2), kd.obj(l)]),
        ids_equality=True,
    )

  def test_object_with_mutable_input_bag(self):
    db = kd.mutable_bag()
    n1 = db.new(x=10)
    n2 = db.new(x=20)
    outer = ds([db.obj(db.new(nested=n1)), db.obj(db.new(nested=n2))])
    res = get_attr_or_item(outer, 'nested')
    self.assertTrue(db.is_mutable())
    self.assertFalse(res.get_bag().is_mutable())
    kd.testing.assert_equivalent(
        res, ds([kd.obj(n1), kd.obj(n2)]), ids_equality=True
    )

  def test_object_conflicting_embedded_schema_raises(self):
    s1 = kd.schema.new_schema(x=kd.INT32)
    s2 = kd.schema.new_schema(x=kd.INT32)
    shared = s1.new(x=1)
    o1 = kd.obj(a=shared)
    o2 = kd.obj(a=kd.schema.unsafe_with_schema(shared, s2))
    with self.assertRaisesRegex(ValueError, 'cannot embed conflicting schemas'):
      get_attr_or_item(ds([o1, o2]), 'a')

  def test_object_conflicting_existing_schema_raises(self):
    inner = kd.obj(x=1)
    other_schema = kd.schema.new_schema(x=kd.INT32)
    outer = ds([
        kd.obj(a=kd.schema.unsafe_with_schema(inner, other_schema)),
        # Forces OBJECT result schema and so schema embedding.
        kd.obj(a=1),
    ])
    # Adoption follows `other_schema`, so `inner.__schema__` is added back.
    outer = outer.enriched(inner.get_bag())
    with self.assertRaisesRegex(ValueError, 'cannot embed conflicting schemas'):
      get_attr_or_item(outer, 'a')

  def test_object_list_items_schema_embedding(self):
    e12 = kd.new(x=ds([1, 2]))
    e3 = kd.new(x=3)
    l = ds([kd.obj(kd.implode(e12)), kd.obj(kd.list([e3]))])
    kd.testing.assert_equivalent(
        get_attr_or_item(l, 0),
        ds([kd.obj(e12.S[0]), kd.obj(e3)]),
        ids_equality=True,
    )

  def test_object_nofollow_schema_embedding(self):
    x = kd.new(x=1)
    x2 = kd.new(x=2)
    outer = ds([kd.obj(a=kd.nofollow(x)), kd.obj(a=x2)])
    # Adoption does not follow NoFollow schemas, so `x` data is added back.
    outer = outer.enriched(x.get_bag())
    # The original (not NoFollow) schema of `x` is embedded.
    kd.testing.assert_equivalent(
        get_attr_or_item(outer, 'a'),
        ds([kd.obj(x), kd.obj(x2)]),
        ids_equality=True,
    )

    # Like all OBJECT values, NoFollow OBJECT values already have `__schema__`,
    # so nothing is embedded for them.
    o = kd.obj(x=1)
    outer = ds([kd.obj(a=kd.nofollow(o)), kd.obj(a=x2)])
    outer = outer.enriched(o.get_bag())
    kd.testing.assert_equivalent(
        get_attr_or_item(outer, 'a'), ds([o, kd.obj(x2)]), ids_equality=True
    )

  def test_object_shared_schema(self):
    s = kd.named_schema('S', x=kd.INT32)
    # Explicit attribute and dict value schemas.
    e1 = s.new(x=1)
    e2 = s.new(x=2)
    o = ds([kd.obj(a=e1), kd.obj(kd.dict({'a': e2})), kd.obj()])
    kd.testing.assert_equivalent(
        get_attr_or_item(o, 'a'), ds([e1, e2, None]), ids_equality=True
    )

    kd.testing.assert_equivalent(
        get_attr_or_item(kd.obj(a=e1), 'a'), e1, ids_equality=True
    )

    l1 = kd.list([1, 2])
    l2 = kd.list([3])
    kd.testing.assert_equivalent(
        get_attr_or_item(ds([kd.obj(a=l1), kd.obj(a=l2)]), 'a'),
        ds([l1, l2]),
        ids_equality=True,
    )

  def test_object_struct_schema_not_deduced_from_objects(self):
    s = kd.named_schema('S', x=kd.INT32)
    e1 = s.new(x=1)
    o2 = kd.obj(s.new(x=2))
    o = ds([
        # Explicit attribute schema.
        kd.obj(a=e1),
        # OBJECT value with `__schema__`.
        kd.obj(kd.dict({'a': o2})),
    ])
    kd.testing.assert_equivalent(
        get_attr_or_item(o, 'a'), ds([kd.obj(e1), o2]), ids_equality=True
    )

  def test_errors(self):
    # Non-scalar key.
    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item, got a slice of rank 1 > 0'
    ):
      get_attr_or_item(kd.obj(x=1), ds(['x']))

    # Primitive ds.
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to have entity, dict, list, or'
        ' OBJECT schema',
    ):
      get_attr_or_item(ds(42), 'x')

    # Schema ds.
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to have entity, dict, list, or'
        ' OBJECT schema',
    ):
      get_attr_or_item(kd.INT32, 'x')

    # OBJECT slice containing primitive.
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to contain only entities, dicts,'
        ' or lists',
    ):
      get_attr_or_item(ds([kd.obj(x=1), 42]), 'x')
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to contain only entities, dicts,'
        ' or lists',
    ):
      get_attr_or_item(kd.obj(42), 'x')

    # Missing __schema__.
    with self.assertRaisesRegex(ValueError, 'object schema is missing'):
      get_attr_or_item(kd.obj(x=1).with_bag(kd.bag()), 'x')
    with self.assertRaisesRegex(ValueError, r'object schema\(s\) are missing'):
      get_attr_or_item(ds([kd.obj(x=1), kd.obj(x=2)]).with_bag(kd.bag()), 'x')

    # Missing DataBag.
    with self.assertRaisesRegex(ValueError, 'cannot lookup without a DataBag'):
      get_attr_or_item(kd.new(x=1).no_bag(), 'x')
    with self.assertRaisesRegex(ValueError, 'cannot lookup without a DataBag'):
      get_attr_or_item(kd.obj(x=1).no_bag(), 'x')
    # Missing DataBag with all items missing.
    with self.assertRaisesRegex(ValueError, 'cannot lookup without a DataBag'):
      get_attr_or_item((kd.new(x=ds([1, 2])) & kd.missing).no_bag(), 'x')
    kd.testing.assert_equal(
        get_attr_or_item(ds([None, None], schema=kd.OBJECT), 'x'),
        ds([None, None]),
    )

  def test_tracing(self):
    def f(x, k):
      return kd_ext.contrib.get_attr_or_item(x, k)  # pyrefly: ignore[missing-attribute]

    fn = kd.trace_py_fn(f)
    mixed = ds([kd.obj(x=42), kd.obj(kd.dict({'x': 'hello'}))])
    kd.testing.assert_equivalent(
        fn(mixed, 'x'), ds([42, 'hello'], schema=kd.OBJECT)
    )

  def test_view_and_repr(self):
    expr = kde.contrib.get_attr_or_item(I.ds, I.key)
    self.assertTrue(kd.is_expr(expr))
    self.assertEqual(repr(expr), 'kd_ext.contrib.get_attr_or_item(I.ds, I.key)')

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.contrib.get_attr_or_item,
        (  # pyrefly: ignore[bad-argument-type]
            (
                kd.qtypes.DATA_SLICE,
                kd.qtypes.DATA_SLICE,
                kd.qtypes.DATA_SLICE,
            ),
        ),
        possible_qtypes=kd.testing.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )


if __name__ == '__main__':
  absltest.main()
