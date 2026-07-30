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
from koladata import kd
from koladata import kd_ext

I = kd.I

kde = kd_ext.lazy
bag = kd.mutable_bag
ds = kd.slice


class ContribFlattenCyclicReferencesTest(parameterized.TestCase):

  def test_entity(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=ds([None, None, None]),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_depth_3(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=3)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=db.new(
                self=db.new(
                    self=ds([None, None, None]),
                    b=db.new(a=ds([1, None, 2])),
                    c=ds(['foo', 'bar', 'baz']),
                ),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_depth_5(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=5)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=db.new(
                self=db.new(
                    self=db.new(
                        self=db.new(
                            self=ds([None, None, None]),
                            b=db.new(a=ds([1, None, 2])),
                            c=ds(['foo', 'bar', 'baz']),
                        ),
                        b=db.new(a=ds([1, None, 2])),
                        c=ds(['foo', 'bar', 'baz']),
                    ),
                    b=db.new(a=ds([1, None, 2])),
                    c=ds(['foo', 'bar', 'baz']),
                ),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_entity_unbalanced(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', ds([o.S[0], o.S[2], o.S[1]]))
    o.S[1].set_attr('self', o.S[2])
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_next = db.new(
        self=db.new(
            self=ds([None, None]),
            b=db.new(a=ds([2, None])),
            c=ds(['baz', 'bar']),
        ),
        b=db.new(a=ds([None, 2])),
        c=ds(['bar', 'baz']),
    )
    expected_ds = db.new(
        self=db.new(
            self=ds([
                None,
                expected_next.S[0],
                expected_next.S[1],
            ]),
            b=db.new(a=ds([1, 2, None])),
            c=ds(['foo', 'baz', 'bar']),
        ),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_zero_depth(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=ds([None, None, None]),
        b=db.new(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_objec(self):
    db = bag()
    b_slice = db.obj(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.obj(
        self=db.obj(
            self=ds([None, None, None]),
            b=db.obj(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        ),
        b=db.obj(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_list(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.implode(db.new(b=b_slice, c=ds(['foo', 'bar', 'baz'])))
    o[:].set_attr('self', o[:])
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.implode(
        db.new(
            self=db.new(
                self=ds([None, None, None]),
                b=db.new(a=ds([1, None, 2])),
                c=ds(['foo', 'bar', 'baz']),
            ),
            b=db.new(a=ds([1, None, 2])),
            c=ds(['foo', 'bar', 'baz']),
        )
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_dict(self):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(db.dict_like(b_slice))
    o['b'] = b_slice
    o['c'] = ds(['foo', 'bar', 'baz'])
    o['self'] = o
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_self = db.dict_like(b_slice)
    expected_self['self'] = ds([None, None, None])
    expected_self['b'] = b_slice
    expected_self['c'] = ds(['foo', 'bar', 'baz'])
    expected_ds = db.dict_like(b_slice)
    expected_ds['self'] = expected_self
    expected_ds['b'] = b_slice
    expected_ds['c'] = ds(['foo', 'bar', 'baz'])
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_empty_list(self):
    db = bag()
    o = db.new(
        a=db.implode(db.new(x=ds([], schema=kd.INT32))),
        b=ds('hello'),
    )
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=ds(None),
            a=db.implode(db.new(x=ds([], schema=kd.INT32))),
            b=ds('hello'),
        ),
        a=db.implode(db.new(x=ds([], schema=kd.INT32))),
        b=ds('hello'),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_empty_dict(self):
    db = bag()
    empty_dict = db.dict({}, key_schema=kd.STRING, value_schema=kd.INT32)
    o = db.new(a=empty_dict, b=ds('hello'))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.new(
        self=db.new(
            self=ds(None),
            a=db.dict({}, key_schema=kd.STRING, value_schema=kd.INT32),
            b=ds('hello'),
        ),
        a=db.dict({}, key_schema=kd.STRING, value_schema=kd.INT32),
        b=ds('hello'),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_list_as_object(self):
    db = bag()
    lst = db.list([1, 2, 3])
    obj_lst = db.obj(lst)
    o = db.new(l=obj_lst)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    self.assertEqual(
        result.to_pytree(max_depth=-1),
        {
            'l': [1, 2, 3],
            'self': {
                'l': [1, 2, 3],
                'self': None,
            },
        },
    )

  def test_dict_as_object(self):
    db = bag()
    dct = db.dict({'a': 1, 'b': 2})
    obj_dct = db.obj(dct)
    o = db.new(d=obj_dct)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    self.assertEqual(
        result.to_pytree(max_depth=-1),
        {
            'd': {'a': 1, 'b': 2},
            'self': {
                'd': {'a': 1, 'b': 2},
                'self': None,
            },
        },
    )

  def test_nested_dict_as_object_in_list_as_object(self):
    db = bag()
    dct = db.dict({'a': 1, 'b': 2})
    obj_dct = db.obj(dct)
    lst = db.list([obj_dct])
    obj_lst = db.obj(lst)
    o = db.new(nested=obj_lst)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    self.assertEqual(
        result.to_pytree(max_depth=-1),
        {
            'nested': [{'a': 1, 'b': 2}],
            'self': {
                'nested': [{'a': 1, 'b': 2}],
                'self': None,
            },
        },
    )

  def test_object(self):
    db = bag()
    b_slice = db.obj(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected_ds = db.obj(
        self=ds([None, None, None]),
        b=db.obj(a=ds([1, None, 2])),
        c=ds(['foo', 'bar', 'baz']),
    )
    kd.testing.assert_equivalent(result, expected_ds, schemas_equality=False)

  def test_empty_list_entity_item_schema(self):
    db = bag()
    item_schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    o = db.new(
        a=db.list([], item_schema=item_schema),
        b=ds('foo'),
    )
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    expected = kd.new(
        a=db.list([], item_schema=item_schema),
        b=ds('foo'),
        self=kd.new(
            a=db.list([], item_schema=item_schema),
            b=ds('foo'),
            schema=o.get_schema(),
        ),
        schema=o.get_schema(),
    )
    kd.testing.assert_equivalent(result, expected, schemas_equality=True)

  def test_schema_object_on_object_attribute(self):
    db = bag()
    stored_schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    o = db.obj(stored_schema=stored_schema)
    res = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]
    self.assertEqual(res.stored_schema.no_bag(), stored_schema.no_bag())

  def test_metadata_object(self):
    db = bag()
    schema = db.new_schema(x=kd.INT32, y=kd.INT32)
    schema = kd.with_metadata(schema.freeze_bag(), foo='bar')
    o = db.obj(kd.new(x=1, y=2, schema=schema))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    res_metadata = kd.get_metadata(result.get_obj_schema())
    kd.testing.assert_equal(res_metadata.foo.no_bag(), ds('bar'))

  def test_metadata_entity(self):
    db = bag()
    schema = db.new_schema(x=kd.INT32, y=kd.INT32)
    schema = kd.with_metadata(schema.freeze_bag(), foo='bar')
    o = db.new(x=1, y=2, schema=schema)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())

    expected = kd.new(
        x=1, y=2, self=kd.new(x=1, y=2, schema=schema), schema=schema
    )
    kd.testing.assert_equivalent(result, expected, schemas_equality=True)

    res_metadata = kd.get_metadata(result.get_schema())
    kd.testing.assert_equal(res_metadata.foo.no_bag(), ds('bar'))

  def test_named_schema(self):
    db = bag()
    schema = db.named_schema('MySchema', x=kd.INT32, y=kd.STRING)
    o = db.new(x=1, y='hello', schema=schema)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    # Named schema should keep its name and identity.
    kd.testing.assert_equal(
        result.get_schema().no_bag(), schema.no_bag()
    )
    kd.testing.assert_equal(
        result.get_schema().get_schema_name().no_bag(),
        ds('MySchema'),
    )
    expected = kd.new(
        x=1, y='hello',
        self=kd.new(x=1, y='hello', schema=schema),
        schema=schema,
    )
    kd.testing.assert_equivalent(result, expected, schemas_equality=True)

  def test_shared_schema(self):
    db = bag()
    schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    a = db.new(x=1, y='a', schema=schema)
    b = db.new(x=2, y='b', schema=schema)
    o = db.new(a=a, b=b)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    # Both child entities should share the same schema.
    kd.testing.assert_equal(
        result.a.get_schema().no_bag(), schema.no_bag()
    )
    kd.testing.assert_equal(
        result.b.get_schema().no_bag(), schema.no_bag()
    )
    kd.testing.assert_equal(
        result.a.get_schema().no_bag(), result.b.get_schema().no_bag()
    )

  def test_entity_schema_preserved(self):
    db = bag()
    schema = db.new_schema(x=kd.INT32)
    o = db.new(x=42, schema=schema)
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    expected = kd.new(x=42, schema=schema)
    kd.testing.assert_equivalent(result, expected, schemas_equality=True)

  def test_list_with_entity_items(self):
    db = bag()
    item_schema = db.new_schema(val=kd.INT32)
    items = db.list(
        [db.new(val=1, schema=item_schema), db.new(val=2, schema=item_schema)],
        item_schema=item_schema,
    )
    o = db.new(items=items, name=ds('test'))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    expected = db.new(items=items, name=ds('test'), schema=o.get_schema())
    kd.testing.assert_equivalent(result, expected, schemas_equality=True)

  def test_schema_as_data_attribute(self):
    db = bag()
    stored_schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    o = db.new(my_schema=stored_schema, data=ds(42))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    # The schema stored as data should be preserved and its attrs accessible.
    kd.testing.assert_equal(
        result.my_schema.no_bag(), stored_schema.no_bag()
    )
    kd.testing.assert_equal(result.my_schema.x.no_bag(), kd.INT32)
    kd.testing.assert_equal(result.my_schema.y.no_bag(), kd.STRING)

  def test_schema_as_itemid_attribute(self):
    db = bag()
    stored_schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    o = db.new(my_schema=stored_schema.get_itemid(), data=ds(42))
    o.set_attr('self', o)
    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=0)  # pyrefly: ignore[missing-attribute]

    self.assertFalse(result.get_bag().is_mutable())
    # The schema stored as data should be preserved and its attrs accessible.
    kd.testing.assert_equal(
        result.my_schema.no_bag(), stored_schema.get_itemid().no_bag()
    )
    kd.testing.assert_equivalent(
        stored_schema.with_bag(result.get_bag()), db.new_schema()
    )

  def test_object_implicit_schema_gets_new_id(self):
    """Implicit schemas should get new IDs derived from the cloned object."""
    db = bag()
    o = db.obj(x=1, y='a')
    o.set_attr('self', o)
    original_schema = o.get_obj_schema()

    result = kd_ext.contrib.flatten_cyclic_references(o, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

    # The result's obj-schema should be different from the original's because
    # implicit schemas get new IDs derived from the cloned object.
    result_schema = result.get_obj_schema()
    kd.testing.assert_not_equal(
        result_schema.no_bag(), original_schema.no_bag()
    )
    # But the schema attrs should still be correct.
    kd.testing.assert_equal(result_schema.x.no_bag(), kd.INT32)
    kd.testing.assert_equal(result_schema.y.no_bag(), kd.STRING)

  def test_schemas_slice(self):
    db = bag()
    schema = db.new_schema(x=kd.INT32, y=kd.STRING)
    x = ds([schema, schema])
    with self.assertRaisesRegex(
        ValueError,
        'cannot flatten cyclic references for a DataSlice of schemas',
    ):
      kd_ext.contrib.flatten_cyclic_references(x, max_recursion_depth=1)  # pyrefly: ignore[missing-attribute]

  def test_view(self):
    expr = kde.contrib.flatten_cyclic_references(
        I.x, max_recursion_depth=I.max_recursion_depth
    )
    self.assertTrue(kd.is_expr(expr))

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.contrib.flatten_cyclic_references(
                I.x, max_recursion_depth=I.max_recursion_depth
            )
        ),
        'kd_ext.contrib.flatten_cyclic_references(I.x,'
        ' max_recursion_depth=I.max_recursion_depth)',
    )


if __name__ == '__main__':
  absltest.main()
