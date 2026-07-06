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
from koladata.expr import expr_eval
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


class NewLikeTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.new_like(ds([1, None])).is_mutable())

  def test_item(self):
    x = fns.new_like(
        ds(1),
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

    x = fns.new_like(ds(None), a=42)
    self.assertIsInstance(x, data_item.DataItem)
    testing.assert_equal(
        kde.has(x).eval().no_bag(), ds(None, schema_constants.MASK)
    )
    testing.assert_equal(
        x.a, ds(None, schema_constants.INT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_slice(self):
    x = fns.new_like(
        ds([[1, None], [1]]),
        a=ds([[1, 2], [3]]),
        b=fns.new(bb=ds([['a', 'b'], ['c']])),
        c=ds(b'xyz'),
    )
    testing.assert_equal(x.a, ds([[1, None], [3]]).with_bag(x.get_bag()))
    testing.assert_equal(x.b.bb, ds([['a', None], ['c']]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.c, ds([[b'xyz', None], [b'xyz']]).with_bag(x.get_bag())
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

  def test_broadcast_attrs(self):
    x = fns.new_like(ds([1, 1]), a=42, b='xyz')
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))

  def test_broadcast_error(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError, arolla.testing.any_cause_message_regex('cannot be expanded')
    ):
      fns.new_like(ds([1, 1]), a=ds([42]))

  def test_all_empty_slice(self):
    x = fns.new_like(ds([None, None]), a=42)
    testing.assert_equal(
        x.a, ds([None, None], schema_constants.INT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_adopt_bag(self):
    x = fns.new_like(ds(1)).fork_bag()
    x.set_attr('a', 'abc')
    y = fns.new_like(x, x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.get_schema().x.with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_itemid())

    with self.subTest('present DataItem and present itemid'):
      x = fns.new_like(ds(1), a=42, itemid=itemid)
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = fns.new_like(ds(None), a=42, itemid=(itemid & None))
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = fns.new_like(ds(None), a=42, itemid=itemid)
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              '`itemid` only has 0 present items but 1 are required'
          ),
      ):
        _ = fns.new_like(ds(1), a=42, itemid=(itemid & None))

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_itemid())
    id2 = expr_eval.eval(kde.allocation.new_itemid())
    id3 = expr_eval.eval(kde.allocation.new_itemid())

    with self.subTest('full DataSlice and full itemid'):
      x = fns.new_like(ds([1, 1, 1]), a=42, itemid=ds([id1, id2, id3]))
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              '`itemid` only has 2 present items but 3 are required'
          ),
      ):
        _ = fns.new_like(ds([1, 1, 1]), a=42, itemid=ds([id1, None, id3]))

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              '`itemid` cannot have duplicate ItemIds'
          ),
      ):
        _ = fns.new_like(ds([1, 1, 1]), a=42, itemid=ds([id1, id2, id1]))

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = fns.new_like(ds([1, None, 1]), a=42, itemid=ds([id1, None, id3]))
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest(
        'sparse DataSlice and sparse itemid with sparsity mismatch'
    ):
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              '`itemid` and `shape_and_mask_from` must have the same sparsity'
          ),
      ):
        _ = fns.new_like(ds([1, None, 1]), a=42, itemid=ds([id1, id2, None]))

    with self.subTest('sparse DataSlice and full itemid'):
      x = fns.new_like(ds([1, None, 1]), a=42, itemid=ds([id1, id2, id3]))
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest('sparse DataSlice and full itemid with duplicates'):
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              '`itemid` cannot have duplicate ItemIds'
          ),
      ):
        _ = fns.new_like(ds([1, None, 1]), a=42, itemid=ds([id1, id1, id1]))

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = fns.new_like(ds([1, None, 1]), a=42, itemid=ds([id1, id1, id3]))
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).get_itemid()
    self.assertTrue(itemid.has_bag())
    # Successful.
    x = fns.new_like(ds([[1, None], [1]]), a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = x.non_existent

  def test_schema_arg(self):
    schema = kde.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    ).eval()
    x = fns.new_like(ds([1, None]), a=42, b='xyz', schema=schema)
    self.assertEqual(attrs.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, None]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds(['xyz', None]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_implicit_casting(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32).eval()
    x = fns.new_like(ds([1, 1]), a=42, schema=schema)
    self.assertEqual(attrs.dir(x), ['a'])
    testing.assert_equal(
        x.a, ds([42, 42], schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_overwrite_schema(self):
    schema = kde.schema.new_schema(a=schema_constants.FLOAT32).eval()
    x = fns.new_like(
        ds([1, 1]), a=42, b='xyz', schema=schema, overwrite_schema=True
    )
    self.assertEqual(attrs.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_overwrite_schema_error(self):
    with self.assertRaisesRegex(TypeError, 'expected bool'):
      fns.new_like(ds(1), schema=schema_constants.INT32, overwrite_schema=42)  # pytype: disable=wrong-arg-types

  def test_schema_arg_overwrite_schema_error_overwriting(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    x = fns.new_like(ds(1), a='xyz', schema=schema, overwrite_schema=True)
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_str_as_schema_arg(self):
    shape_and_mask_from = ds([[6, 7], [8]])
    x = fns.new_like(shape_and_mask_from, schema='name', a=42)
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    testing.assert_equal(x.get_schema().no_bag(), expected_schema.no_bag())
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    shape_and_mask_from = ds([[6, 7], [8]])
    x = fns.new_like(shape_and_mask_from, schema=ds('name'), a=42)
    expected_schema = kde.named_schema('name').eval()
    testing.assert_equal(x.get_shape(), shape_and_mask_from.get_shape())
    testing.assert_equal(x.get_schema().no_bag(), expected_schema.no_bag())
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new_like(ds(1), a=1, schema=5)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new_like(ds(1), a=1, schema=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      fns.new_like(ds(1), schema=ds(['name']), a=42)
    with self.assertRaisesRegex(ValueError, 'schema can only be 0-rank'):
      fns.new_like(
          ds(1),
          a=1,
          schema=ds([schema_constants.INT32, schema_constants.STRING]),
      )
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected Entity schema, got INT32'
        ),
    ):
      fns.new_like(ds(1), a=1, schema=schema_constants.INT32)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected Entity schema, got OBJECT'
        ),
    ):
      fns.new_like(ds(1), a=1, schema=schema_constants.OBJECT)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected Entity schema, got LIST[INT32]')
        ),
    ):
      fns.new_like(
          ds(1), a=1, schema=kde.list_schema(schema_constants.INT32).eval()
      )

  def test_schema_error_message(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                """the schema for attribute 'a' is incompatible.

Expected schema for 'a': INT32
Assigned schema for 'a': STRING

To fix this, explicitly override schema of 'a' in the original schema by passing overwrite_schema=True."""
            )
        ),
    ) as cm:
      fns.new_like(ds([1, 2, 3]), a=ds(['a', 'b', 'c']), schema=schema)
    self.assertRegex(
        str(cm.exception),
        re.escape(
            'cannot create Item(s) with the provided schema: ENTITY(a=INT32)'
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

The cause is the values of attribute 'x' are different: 1 vs 2"""
        ),
    ):
      db1.new_like(ds([1, 2, 3]), y=b)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.new_like(ds(1), x=1, lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.new_like(ds(1), x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42})
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.new_like(ds([1, 2, 3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.new_like(ds([1, 2, 3]), dct={'a': 42})

  def test_alias(self):
    self.assertIs(fns.new_like, fns.entities.like)


if __name__ == '__main__':
  absltest.main()
