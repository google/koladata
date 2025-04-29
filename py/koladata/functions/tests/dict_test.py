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
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


# TODO: Add more tests with merging, etc. Also some from
# data_bag_test.
class DictTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.dict().is_mutable())

  def test_empty(self):
    d = fns.dict().fork_bag()
    self.assertIsInstance(d, dict_item.DictItem)
    testing.assert_dicts_keys_equal(d, ds([], schema_constants.OBJECT))
    d['a'] = 1
    testing.assert_dicts_keys_equal(d, ds(['a'], schema_constants.OBJECT))
    testing.assert_equal(
        d['a'], ds(1, schema_constants.OBJECT).with_bag(d.get_bag())
    )
    keys = ds(['a', 'b', 'a'])
    testing.assert_equal(
        d[keys], ds([1, None, 1], schema_constants.OBJECT).with_bag(d.get_bag())
    )

  def test_no_input_values(self):
    d = fns.dict({})
    self.assertIsInstance(d, dict_item.DictItem)
    testing.assert_dicts_keys_equal(d, ds([]))
    testing.assert_dicts_values_equal(d, ds([]))

  def test_dict_with_uuid(self):
    testing.assert_equal(
        fns.dict(itemid=kd.uuid_for_dict(seed='seed', a=ds(1))).no_bag(),
        fns.dict(itemid=kd.uuid_for_dict(seed='seed', a=ds(1))).no_bag(),
    )

  def test_single_arg(self):
    d = fns.dict({'a': {'b': 42}})
    self.assertIsInstance(d, dict_item.DictItem)
    testing.assert_equal(d['a']['b'], ds(42).with_bag(d.get_bag()))
    with self.assertRaisesRegex(
        TypeError,
        '`items_or_keys` must be a Python dict if `values` is not provided',
    ):
      fns.dict(ds([1, 2, 3]))

  def test_two_args(self):
    d = fns.dict('a', 1).fork_bag()
    self.assertIsInstance(d, dict_item.DictItem)
    self.assertEqual(d.get_shape().rank(), 0)
    d['b'] = 1
    testing.assert_equal(d[ds(['a', 'b'])], ds([1, 1]).with_bag(d.get_bag()))

  def test_two_args_error(self):
    with self.assertRaisesRegex(
        TypeError,
        r'`items_or_keys` must be a DataSlice or DataItem \(or convertible to '
        r'DataItem\) if `values` is provided',
    ):
      fns.dict(['a', 'b'], 1)

  def test_two_args_data_slices(self):
    d = fns.dict(ds([['a', 'b'], ['c']]), 1)
    self.assertIsInstance(d, data_slice.DataSlice)
    # NOTE: Dimension of dicts is reduced by 1.
    self.assertEqual(d.get_shape().rank(), 1)

    testing.assert_dicts_keys_equal(
        d, ds([['a', 'b'], ['c']]).with_bag(d.get_bag())
    )
    testing.assert_equal(d['a'], ds([1, None]).with_bag(d.get_bag()))

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_dictid_shaped_as(ds([[1, 1], [1]]))
    )
    x = fns.dict('a', 42, itemid=itemid)
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['a']]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.dict({'a': triple})

    # Successful.
    x = fns.dict('a', 42, itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_itemid_error(self):
    itemid = expr_eval.eval(
        kde.allocation.new_dictid_shaped_as(ds([[1, 1], [1]]))
    )
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      _ = fns.dict(ds([[['a', 'a']]]), 42, itemid=itemid)

  def test_repr(self):
    self.assertRegex(
        repr(fns.dict({'a': 1, 'b': 2})),
        r'DataItem\(.*, schema: .*\)',
    )

  def test_adopt_values(self):
    dct = fns.dict('a', 7)
    dct2 = fns.dict(ds(['obj']), dct)

    testing.assert_equal(
        dct2['obj']['a'],
        ds(7, schema_constants.INT32).with_bag(dct2.get_bag()),
    )

  def test_adopt_schema(self):
    dict_schema = kde.dict_schema(
        schema_constants.STRING, kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    dct = fns.dict(schema=dict_schema)

    testing.assert_equal(
        dct[ds(None)].a.no_bag(), ds(None, schema_constants.INT32)
    )

  @parameterized.parameters(
      dict(
          keys=None,
          values=None,
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.OBJECT,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=None,
          values=None,
          key_schema=schema_constants.INT32,
          value_schema=schema_constants.OBJECT,
          schema=None,
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Deduce schema from keys and values.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 'foo'], [3]]),
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=ds([[1, 'foo'], [3]]),
          values=ds([[1, 'foo'], [3]]),
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.OBJECT,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Both schema and keys / values provided, do casting.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 2], [3]]),
          key_schema=schema_constants.INT64,
          value_schema=schema_constants.OBJECT,
          schema=None,
          expected_key_schema=schema_constants.INT64,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=None,
          values=None,
          key_schema=None,
          value_schema=None,
          schema=kde.dict_schema(
              key_schema=schema_constants.INT32,
              value_schema=schema_constants.OBJECT,
          ).eval(),
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Both schema and keys / values provided, do casting.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 2], [3]]),
          key_schema=None,
          value_schema=None,
          schema=kde.dict_schema(
              key_schema=schema_constants.INT64,
              value_schema=schema_constants.OBJECT,
          ).eval(),
          expected_key_schema=schema_constants.INT64,
          expected_value_schema=schema_constants.OBJECT,
      ),
  )
  def test_schema(
      self,
      keys,
      values,
      key_schema,
      value_schema,
      schema,
      expected_key_schema,
      expected_value_schema,
  ):
    result_schema = fns.dict(
        items_or_keys=keys,
        key_schema=key_schema,
        values=values,
        value_schema=value_schema,
        schema=schema,
    ).get_schema()
    testing.assert_equal(
        result_schema.get_attr('__keys__').no_bag(),
        expected_key_schema,
    )
    testing.assert_equal(
        result_schema.get_attr('__values__').no_bag(),
        expected_value_schema,
    )

  def test_schema_arg_error(self):
    dict_schema = kde.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.OBJECT
    ).eval()
    with self.assertRaisesRegex(
        ValueError, 'either a dict schema or key/value schemas, but not both'
    ):
      fns.dict(
          key_schema=schema_constants.INT64,
          schema=dict_schema,
      )

  def test_wrong_arg_types(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting key_schema to be a DataSlice, got int'
    ):
      fns.dict(key_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting value_schema to be a DataSlice, got int'
    ):
      fns.dict(value_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.dict(schema=42)

  def test_dict_schema_error_message(self):
    schema = kde.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.INT64
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: INT64
Assigned schema for keys: STRING"""),
    ):
      fns.dict(items_or_keys=ds(['a', 'a']), values=ds([1, 2]), schema=schema)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: INT64
Assigned schema for values: STRING"""),
    ):
      fns.dict(items_or_keys=ds([1, 2]), values=ds(['1', '2']), schema=schema)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: INT64
Assigned schema for values: STRING"""),
    ):
      fns.dict(items_or_keys={1: 'a', 2: 'b'}, schema=schema)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: INT64
Assigned schema for keys: STRING"""),
    ):
      fns.dict(items_or_keys={'a': 1, 'b': 1}, schema=schema)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: STRING
Assigned schema for values: INT32"""),
    ):
      fns.dict(
          items_or_keys={'a': 1, 'b': 1},
          key_schema=schema_constants.STRING,
          value_schema=schema_constants.STRING,
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: INT64
Assigned schema for keys: STRING"""),
    ):
      fns.dict(
          items_or_keys={'a': 1, 'b': 1},
          key_schema=schema_constants.INT64,
          value_schema=schema_constants.INT64,
      )

    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for keys is incompatible.

Expected schema for keys: SCHEMA\(x=INT32\) with ItemId \$[0-9a-zA-Z]{22}
Assigned schema for keys: SCHEMA\(x=INT32\) with ItemId \$[0-9a-zA-Z]{22}""",
    ):
      db = fns.bag()
      db.dict(
          items_or_keys={db.new(x=1): 'a'},
          key_schema=db.new_schema(x=schema_constants.INT32),
          value_schema=schema_constants.STRING,
      )

    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for values is incompatible.

Expected schema for values: SCHEMA\(x=INT32\) with ItemId \$[0-9a-zA-Z]{22}
Assigned schema for values: SCHEMA\(x=INT32\) with ItemId \$[0-9a-zA-Z]{22}""",
    ):
      db = fns.bag()
      db.dict(
          items_or_keys={'a': db.new(x=1)},
          key_schema=schema_constants.STRING,
          value_schema=db.new_schema(x=schema_constants.INT32),
      )

  def test_alias(self):
    self.assertIs(fns.dict, fns.dicts.new)


if __name__ == '__main__':
  absltest.main()
