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

  def test_object_unimplemented(self):
    with self.assertRaisesRegex(
        ValueError,
        'lookup of values with different schemas is not supported yet',
    ):
      get_attr_or_item(ds([kd.obj(x=1), kd.obj(x='a')]), 'x')

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
