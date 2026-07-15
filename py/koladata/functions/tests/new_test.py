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
from koladata.functions import attrs
from koladata.functions import functions as fns
from koladata.functions import object_factories
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
    self.assertEqual(attrs.dir(x), ['a', 'b'])
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
    self.assertEqual(attrs.dir(x), ['a', 'b', 'nested'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)
    testing.assert_equal(x.nested.p, ds(b'0123').with_bag(x.get_bag()))
    testing.assert_equal(
        x.nested.get_schema().p.no_bag(), schema_constants.BYTES
    )

  def test_schema_arg_schema_with_fallback(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    fallback_bag = object_factories.mutable_bag()
    schema.with_bag(fallback_bag).set_attr('b', schema_constants.STRING)
    schema = schema.enriched(fallback_bag)
    x = fns.new(a=42, b='xyz', schema=schema)
    self.assertEqual(attrs.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32).eval()
    x = fns.new(a=42, schema=schema)
    self.assertEqual(attrs.dir(x), ['a'])
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
    self.assertEqual(attrs.dir(x), ['a', 'b'])
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
    self.assertEqual(attrs.dir(x), ['a'])
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
    testing.assert_equal(x.get_schema().no_bag(), expected_schema.no_bag())
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    x = fns.new(schema=ds('name'), a=42)
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_schema().no_bag(), expected_schema.no_bag())
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new(a=1, schema=5)  # pytype: disable=wrong-arg-types
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

    db = object_factories.mutable_bag()
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

    db1 = object_factories.mutable_bag()
    _ = db1.uuobj(x=1)
    db2 = object_factories.mutable_bag()
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

  def test_alias(self):
    self.assertIs(fns.new, fns.entities.new)


if __name__ == '__main__':
  absltest.main()
