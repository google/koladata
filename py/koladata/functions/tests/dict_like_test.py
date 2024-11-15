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
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class DictLikeTest(parameterized.TestCase):

  def test_no_kv(self):
    x = fns.dict_like(ds([[0, None], [0]]))
    x[1] = 2
    x[2] = 3
    testing.assert_equal(
        x[ds([[1, 2], [3]])],
        ds([[2, None], [None]], schema_constants.OBJECT).with_bag(x.get_bag()),
    )

  def test_scalar_kv(self):
    x = fns.dict_like(ds([[0, None], [0]]), 'key', 42)
    testing.assert_equal(
        x['key'],
        ds([[42, None], [42]], schema_constants.INT32).with_bag(x.get_bag()),
    )

  def test_scalar_values(self):
    x = fns.dict_like(
        ds([[0, None], [0]]),
        ds(['a', 'b']),
        42,
    )
    testing.assert_equal(
        x[ds([['a', None], ['b']])],
        ds([[42, None], [42]]).with_bag(x.get_bag()),
    )

  def test_with_kv_broadcasting(self):
    x = fns.dict_like(
        ds([[None, 0], [0]]),
        ds(['a', 'b']),
        ds([1, 2]),
    )
    testing.assert_dicts_keys_equal(x, ds([[[], ['a']], [['b']]]))
    testing.assert_equal(x['a'], ds([[None, 1], [None]]).with_bag(x.get_bag()))
    testing.assert_equal(x['b'], ds([[None, None], [2]]).with_bag(x.get_bag()))

  def test_with_values_broadcasting(self):
    x = fns.dict_like(
        ds([None, 0]),
        ds([['a'], ['b', 'c']]),
        42,
    )
    testing.assert_dicts_keys_equal(x, ds([[], ['b', 'c']]))
    testing.assert_equal(
        x['a'], ds([None, None], schema_constants.INT32).with_bag(x.get_bag())
    )
    testing.assert_equal(x['b'], ds([None, 42]).with_bag(x.get_bag()))

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_dictid())

    with self.subTest('present DataItem and present itemid'):
      x = fns.dict_like(ds(1), 'a', 42, itemid=itemid)
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = fns.dict_like(ds(None), 'a', 42, itemid=(itemid & None))
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = fns.dict_like(ds(None), 'a', 42, itemid=itemid)
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 0 present items but 1 are required',
      ):
        _ = fns.dict_like(ds(1), 'a', 42, itemid=(itemid & None))

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_dictid())
    id2 = expr_eval.eval(kde.allocation.new_dictid())
    id3 = expr_eval.eval(kde.allocation.new_dictid())

    with self.subTest('full DataSlice and full itemid'):
      x = fns.dict_like(ds([1, 1, 1]), 'a', 42, itemid=ds([id1, id2, id3]))
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 2 present items but 3 are required',
      ):
        _ = fns.dict_like(
            ds([1, 1, 1]),
            'a',
            42,
            itemid=ds([id1, None, id3]),
        )

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = fns.dict_like(ds([1, 1, 1]), 'a', 42, itemid=ds([id1, id2, id1]))

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = fns.dict_like(
          ds([1, None, 1]),
          'a',
          42,
          itemid=ds([id1, None, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest(
        'sparse DataSlice and sparse itemid with sparsity mismatch'
    ):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` and `shape_and_mask_from` must have the same sparsity',
      ):
        _ = fns.dict_like(
            ds([1, None, 1]), 'a', 42, itemid=ds([id1, id2, None])
        )

    with self.subTest('sparse DataSlice and full itemid'):
      x = fns.dict_like(
          ds([1, None, 1]),
          'a',
          42,
          itemid=ds([id1, id2, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest('sparse DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = fns.dict_like(
            ds([1, None, 1]),
            'a',
            42,
            itemid=ds([id1, id1, id1]),
        )

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = fns.dict_like(
          ds([1, None, 1]),
          'a',
          42,
          itemid=ds([id1, id1, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.dict_shaped(ds([[1, 1], [1]]).get_shape(), 'a', triple)

    # Successful.
    x = fns.dict_like(ds([[1, None], [1]]), itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_bag_arg(self):
    db = fns.bag()
    x = fns.dict_like(ds([[0, None], [0]]), db=db)
    testing.assert_equal(x.get_bag(), db)

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
    mask_and_shape = ds([[1, None], [3]])
    result_schema = fns.dict_like(
        mask_and_shape,
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
    mask_and_shape = ds([[1, None], [3]])
    dict_schema = fns.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.OBJECT
    )
    with self.assertRaisesRegex(
        ValueError, 'either a dict schema or key/value schemas, but not both'
    ):
      fns.dict_like(
          mask_and_shape,
          key_schema=schema_constants.INT64,
          schema=dict_schema,
      )

  def test_wrong_arg_types(self):
    mask_and_shape = ds([[1, None], [3]])
    with self.assertRaisesRegex(
        TypeError, 'expecting key_schema to be a DataSlice, got int'
    ):
      fns.dict_like(mask_and_shape, key_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting value_schema to be a DataSlice, got int'
    ):
      fns.dict_like(mask_and_shape, value_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.dict_like(mask_and_shape, schema=42)

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got int'
    ):
      fns.dict_like(57, 'key', 'value')
    with self.assertRaisesRegex(
        TypeError,
        'expecting shape_and_mask_from to be a DataSlice, got .*DataBag',
    ):
      fns.dict_like(fns.bag(), 'key', 'value')

  def test_missing_values(self):
    with self.assertRaisesRegex(
        TypeError,
        '`items_or_keys` must be a Python dict if `values` is not provided, but'
        ' got str',
    ):
      fns.dict_like(ds([[0, None], [0]]), 'key')

  def test_dict_and_values(self):
    with self.assertRaisesRegex(
        TypeError,
        r'`items_or_keys` must be a DataSlice or DataItem \(or convertible to '
        r'DataItem\) if `values` is provided, but got dict',
    ):
      fns.dict_like(data_item.DataItem.from_vals(0), {'a': 1}, 42)

  def test_no_python_dict_broadcasting(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot create a DataSlice of dicts from a Python dictionary, only '
        'DataItem can be created directly from Python dictionary',
    ):
      fns.dict_like(ds([[0, None], [0]]), {'a': 1})

  def test_non_dataslice_keys(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            '`items_or_keys` must be a DataSlice or DataItem (or convertible '
            'to DataItem) if `values` is provided, but got list'
        ),
    ):
      fns.dict_like(ds([[None, 0], [0]]), ['a', 'b'], [1, 2])

  def test_impossible_broadcasting(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded to'):
      fns.dict_like(
          ds([[0, 0], [0]]),
          ds(['a', 'a', 'a']),
          42,
      )


if __name__ == '__main__':
  absltest.main()
