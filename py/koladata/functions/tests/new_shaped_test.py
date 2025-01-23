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
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import jagged_shape
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class NewShapedTest(absltest.TestCase):

  def test_mutability(self):
    shape = jagged_shape.create_shape(2, [2, 1])
    self.assertFalse(fns.new_shaped(shape).is_mutable())
    self.assertTrue(fns.new_shaped(shape, db=fns.bag()).is_mutable())

  def test_item(self):
    x = fns.new_shaped(
        jagged_shape.create_shape(),
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
    x = fns.new_shaped(
        jagged_shape.create_shape(2, [2, 1]),
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

  def test_broadcast_attrs(self):
    x = fns.new_shaped(jagged_shape.create_shape([2]), a=42, b='xyz')
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))

  def test_broadcast_error(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'cannot be expanded'):
      fns.new_shaped(jagged_shape.create_shape([2]), a=ds([42]))

  def test_adopt_bag(self):
    x = fns.new_shaped(jagged_shape.create_shape()).fork_bag()
    x.set_attr('a', 'abc')
    y = fns.new_shaped(x.get_shape(), x=x)
    # y.get_bag() is merged with x.get_bag(), so access to `a` is possible.
    testing.assert_equal(y.x.a, ds('abc').with_bag(y.get_bag()))
    testing.assert_equal(x.get_schema(), y.get_schema().x.with_bag(x.get_bag()))
    testing.assert_equal(y.x.a.no_bag().get_schema(), schema_constants.STRING)

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_itemid_shaped_as(ds([[1, 1], [1]]))
    )
    x = fns.new_shaped(itemid.get_shape(), a=42, itemid=itemid)
    testing.assert_equal(x.a.no_bag(), ds([[42, 42], [42]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    itemid = fns.new(non_existent=ds([[42, 42], [42]])).get_itemid()
    assert itemid.get_bag() is not None
    # Successful.
    x = fns.new_shaped(itemid.get_shape(), a=42, itemid=itemid)
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = x.non_existent

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.new_shaped(jagged_shape.create_shape(), a=1, b='a', db=db)
    testing.assert_equal(db, x.get_bag())

  def test_schema_arg_simple(self):
    schema = fns.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )
    x = fns.new_shaped(
        jagged_shape.create_shape([2]), a=42, b='xyz', schema=schema
    )
    self.assertEqual(fns.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_deep(self):
    nested_schema = fns.schema.new_schema(p=schema_constants.BYTES)
    schema = fns.schema.new_schema(
        a=schema_constants.INT32,
        b=schema_constants.STRING,
        nested=nested_schema,
    )
    x = fns.new_shaped(
        jagged_shape.create_shape(),
        a=42,
        b='xyz',
        nested=fns.new_shaped(
            jagged_shape.create_shape(), p=b'0123', schema=nested_schema
        ),
        schema=schema
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

  def test_schema_arg_implicit_casting(self):
    schema = fns.schema.new_schema(a=schema_constants.FLOAT32)
    x = fns.new_shaped(jagged_shape.create_shape([2]), a=42, schema=schema)
    self.assertEqual(fns.dir(x), ['a'])
    testing.assert_equal(
        x.a, ds([42, 42], schema_constants.FLOAT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.FLOAT32)

  def test_schema_arg_implicit_casting_failure(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    with self.assertRaisesRegex(
        exceptions.KodaError, r'schema for attribute \'a\' is incompatible'
    ):
      fns.new_shaped(jagged_shape.create_shape([2]), a='xyz', schema=schema)

  def test_schema_arg_supplement_succeeds(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    x = fns.new_shaped(
        jagged_shape.create_shape(), a=42, b='xyz', schema=schema
    )
    testing.assert_equal(x.a, ds(42).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_update_schema(self):
    schema = fns.schema.new_schema(a=schema_constants.FLOAT32)
    x = fns.new_shaped(
        jagged_shape.create_shape([2]),
        a=42,
        b='xyz',
        schema=schema,
        update_schema=True,
    )
    self.assertEqual(fns.dir(x), ['a', 'b'])
    testing.assert_equal(x.a, ds([42, 42]).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)
    testing.assert_equal(x.b, ds(['xyz', 'xyz']).with_bag(x.get_bag()))
    testing.assert_equal(x.get_schema().b.no_bag(), schema_constants.STRING)

  def test_schema_arg_update_schema_error(self):
    with self.assertRaisesRegex(TypeError, 'expected bool'):
      fns.new_shaped(
          jagged_shape.create_shape(), schema=schema_constants.ANY,
          update_schema=42
      )  # pytype: disable=wrong-arg-types

  def test_schema_arg_update_schema_error_overwriting(self):
    schema = fns.schema.new_schema(a=schema_constants.INT32)
    x = fns.new_shaped(
        jagged_shape.create_shape(),
        a='xyz',
        schema=schema,
        update_schema=True,
    )
    testing.assert_equal(x.a, ds('xyz').with_bag(x.get_bag()))

  def test_schema_arg_any(self):
    x = fns.new_shaped(
        jagged_shape.create_shape([2]),
        a=1, b='a',
        schema=schema_constants.ANY
    )
    self.assertEqual(fns.dir(x), [])
    testing.assert_equal(x.get_schema().no_bag(), schema_constants.ANY)
    testing.assert_equal(x.a, ds([1, 1]).as_any().with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(['a', 'a']).as_any().with_bag(x.get_bag()))

  def test_schema_arg_embed_schema(self):
    schema = fns.schema.new_schema(a=schema_constants.OBJECT)
    x = fns.new_shaped(
        jagged_shape.create_shape(),
        a=fns.new(p=42, q='xyz'),
        schema=schema
    )
    self.assertEqual(fns.dir(x), ['a'])
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        x.a.get_attr('__schema__').p.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        x.a.get_attr('__schema__').q.no_bag(), schema_constants.STRING
    )

  def test_str_as_schema_arg(self):
    shape = jagged_shape.create_shape([2])
    x = fns.new_shaped(shape, schema='name', a=42)
    expected_schema = fns.named_schema('name')
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_str_slice_as_schema_arg(self):
    shape = jagged_shape.create_shape([2])
    x = fns.new_shaped(shape, schema=ds('name'), a=42)
    expected_schema = fns.named_schema('name')
    testing.assert_equal(x.get_shape(), shape)
    testing.assert_equal(
        x.get_schema().with_bag(expected_schema.get_bag()), expected_schema
    )
    testing.assert_equal(x.get_schema().a.no_bag(), schema_constants.INT32)

  def test_schema_arg_errors(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new_shaped(jagged_shape.create_shape(), a=1, schema=5)
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      fns.new_shaped(jagged_shape.create_shape(), a=1, schema=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: STRING"
    ):
      fns.new_shaped(jagged_shape.create_shape(), schema=ds(['name']), a=1)
    with self.assertRaisesRegex(ValueError, 'schema can only be 0-rank'):
      fns.new_shaped(
          jagged_shape.create_shape(),
          a=1,
          schema=ds([schema_constants.INT32, schema_constants.STRING]),
      )
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got INT32'
    ):
      fns.new_shaped(
          jagged_shape.create_shape(), a=1, schema=schema_constants.INT32
      )
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got OBJECT'
    ):
      fns.new_shaped(
          jagged_shape.create_shape(), a=1, schema=schema_constants.OBJECT
      )

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
      fns.new_shaped(jagged_shape.create_shape(), a='a', schema=schema)

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
      schema(a='a')

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
      db1.new_shaped(jagged_shape.create_shape(), y=b)

  def test_item_assignment_rhs_no_ds_args(self):
    x = fns.new_shaped(
        jagged_shape.create_shape(), x=1, lst=[1, 2, 3], dct={'a': 42}
    )
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_item_assignment_rhs_with_ds_args(self):
    x = fns.new_shaped(
        jagged_shape.create_shape(),
        x=1, y=ds('a'), lst=[1, 2, 3], dct={'a': 42}
    )
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x.lst, list_item.ListItem)
    self.assertIsInstance(x.dct, dict_item.DictItem)
    testing.assert_equal(x.lst[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_dicts_keys_equal(x.dct, ds(['a']).with_bag(x.get_bag()))

  def test_slice_assignment_rhs(self):
    with self.assertRaisesRegex(ValueError, 'assigning a Python list/tuple'):
      fns.new_shaped(jagged_shape.create_shape([3]), lst=[1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'assigning a Python dict'):
      fns.new_shaped(jagged_shape.create_shape([3]), dct={'a': 42})

  def test_alias(self):
    self.assertIs(fns.new_shaped, fns.entities.shaped)


if __name__ == '__main__':
  absltest.main()
