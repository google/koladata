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

"""Tests for dict."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from koladata.exceptions import exceptions
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

  def test_empty(self):
    d = fns.dict()
    self.assertIsInstance(d, dict_item.DictItem)
    testing.assert_dicts_keys_equal(d, ds([], schema_constants.OBJECT))
    d['a'] = 1
    testing.assert_dicts_keys_equal(d, ds(['a'], schema_constants.OBJECT))
    testing.assert_equal(d['a'], ds(1, schema_constants.OBJECT).with_db(d.db))
    keys = ds(['a', 'b', 'a'])
    testing.assert_equal(
        d[keys], ds([1, None, 1], schema_constants.OBJECT).with_db(d.db)
    )

  def test_single_arg(self):
    d = fns.dict({'a': {'b': 42}})
    self.assertIsInstance(d, dict_item.DictItem)
    testing.assert_equal(d['a']['b'], ds(42).with_db(d.db))
    with self.assertRaisesRegex(
        TypeError,
        '`items_or_keys` must be a Python dict if `values` is not provided',
    ):
      fns.dict(ds([1, 2, 3]))

  def test_two_args(self):
    d = fns.dict('a', 1)
    self.assertIsInstance(d, dict_item.DictItem)
    self.assertEqual(d.get_shape().rank(), 0)
    d['b'] = 1
    testing.assert_equal(d[['a', 'b']], ds([1, 1]).with_db(d.db))

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

    testing.assert_dicts_keys_equal(d, ds([['a', 'b'], ['c']]).with_db(d.db))
    testing.assert_equal(d['a'], ds([1, None]).with_db(d.db))

  def test_itemid(self):
    itemid = kde.allocation.new_dictid_shaped_as._eval(ds([[1, 1], [1]]))  # pylint: disable=protected-access
    x = fns.dict('a', 42, itemid=itemid)
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['a']]]))
    testing.assert_equal(x.no_db().as_itemid(), itemid)

  def test_itemid_from_different_db(self):
    triple = fns.new(non_existent=42)
    itemid = fns.dict({'a': triple})

    # Successful.
    x = fns.dict('a', 42, itemid=itemid.as_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = triple.with_db(x.db).non_existent

  def test_itemid_error(self):
    itemid = kde.allocation.new_dictid_shaped_as._eval(ds([[1, 1], [1]]))  # pylint: disable=protected-access
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      _ = fns.dict(ds([[['a', 'a']]]), 42, itemid=itemid)

  def test_db_arg(self):
    db = fns.bag()
    d = fns.dict({1: 2})
    with self.assertRaises(AssertionError):
      testing.assert_equal(d.db, db)

    d = fns.dict({1: 2}, db=db)
    testing.assert_equal(d.db, db)

  def test_repr(self):
    self.assertRegex(
        repr(fns.dict({'a': 1, 'b': 2})),
        r'DataItem\(.*, schema: .*, bag_id: .*\)',
    )

  def test_merge_values(self):
    dct = fns.dict()
    dct['a'] = 7
    dct2 = fns.dict(ds(['obj']), dct)

    testing.assert_equal(
        dct2['obj']['a'], ds(7, schema_constants.OBJECT).with_db(dct2.db)
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
          value_schema=schema_constants.ANY,
          schema=None,
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.ANY,
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
          values=ds([[1, 'foo'], [3]]).as_any(),
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.OBJECT,
          expected_value_schema=schema_constants.ANY,
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
          schema=fns.dict_schema(
              key_schema=schema_constants.INT32,
              value_schema=schema_constants.ANY,
          ),
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.ANY,
      ),
      # Both schema and keys / values provided, do casting.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 2], [3]]),
          key_schema=None,
          value_schema=None,
          schema=fns.dict_schema(
              key_schema=schema_constants.INT64,
              value_schema=schema_constants.OBJECT,
          ),
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
        result_schema.get_attr('__keys__').no_db(),
        expected_key_schema,
    )
    testing.assert_equal(
        result_schema.get_attr('__values__').no_db(),
        expected_value_schema,
    )

  def test_schema_arg_error(self):
    dict_schema = fns.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.OBJECT
    )
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
    schema = fns.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.INT64
    )
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT64
Assigned schema for Dict key: TEXT"""),
    ):
      schema(items_or_keys=ds(['a', 'a']), values=ds([1, 2]))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict value is incompatible.

Expected schema for Dict value: INT64
Assigned schema for Dict value: TEXT"""),
    ):
      schema(items_or_keys=ds([1, 2]), values=ds(['1', '2']))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict value is incompatible.

Expected schema for Dict value: INT64
Assigned schema for Dict value: TEXT"""),
    ):
      fns.dict(items_or_keys={1: 'a', 2: 'b'}, schema=schema)

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT64
Assigned schema for Dict key: TEXT"""),
    ):
      fns.dict(items_or_keys={'a': 1, 'b': 1}, schema=schema)

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict value is incompatible.

Expected schema for Dict value: TEXT
Assigned schema for Dict value: INT32"""),
    ):
      fns.dict(
          items_or_keys={'a': 1, 'b': 1},
          key_schema=schema_constants.TEXT,
          value_schema=schema_constants.TEXT,
      )

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(r"""the schema for Dict key is incompatible.

Expected schema for Dict key: INT64
Assigned schema for Dict key: TEXT"""),
    ):
      fns.dict(
          items_or_keys={'a': 1, 'b': 1},
          key_schema=schema_constants.INT64,
          value_schema=schema_constants.INT64,
      )


if __name__ == '__main__':
  absltest.main()
