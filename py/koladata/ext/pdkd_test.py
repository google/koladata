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
from koladata.ext import pdkd
import numpy as np
import pandas as pd

S = kd.S


class PdkdTest(parameterized.TestCase):

  @parameterized.parameters(
      (pd.DataFrame({'x': [1, 2, 3]}), kd.int64([1, 2, 3])),
      (
          pd.DataFrame({'x': pd.Series([1, pd.NA, 3], dtype=pd.Int32Dtype())}),
          kd.int32([1, None, 3]),
      ),
      (
          pd.DataFrame({'x': pd.Series([1, pd.NA, 3], dtype=pd.Int64Dtype())}),
          kd.int64([1, None, 3]),
      ),
      (
          # object dtypes stay as objects.
          pd.DataFrame({'x': pd.Series([1, pd.NA, 3], dtype='object')}),
          kd.slice([1, None, 3], kd.OBJECT),
      ),
      (
          pd.DataFrame(
              {'x': pd.Series([1.0, pd.NA, 3.0], dtype=pd.Float32Dtype())}
          ),
          kd.float32([1.0, None, 3.0]),
      ),
      (
          # Numpy-style missing values are also supported.
          pd.DataFrame({'x': [1.0, float('nan'), 3.0]}),
          kd.float64([1.0, None, 3.0]),
      ),
      (
          # When it's abused for incompatible types, the output is OBJECT
          # (since the input is object).
          pd.DataFrame({'x': ['a', float('nan')]}),
          kd.slice(['a', None], kd.OBJECT),
      ),
      (
          pd.DataFrame(
              {'x': pd.Series(['a', pd.NA, 'c'], dtype=pd.StringDtype())}
          ),
          kd.str(['a', None, 'c']),
      ),
      (
          # Mixed data stays mixed.
          pd.DataFrame({'x': pd.Series([1.0, 2, pd.NA], dtype='object')}),
          kd.slice([1.0, 2, None], kd.OBJECT),
      ),
  )
  def test_from_dataframe_primitive_df(self, df, expected_ds):
    ds = pdkd.from_dataframe(df)
    kd.testing.assert_equal(ds.x.no_bag(), expected_ds)

  def test_from_dataframe_multidimensional_int_df(self):
    index = pd.MultiIndex.from_arrays([[0, 0, 1, 3, 3], [0, 1, 0, 0, 1]])
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5]}, index=index)
    ds = pdkd.from_dataframe(df)
    kd.testing.assert_equal(
        ds.x.no_bag(),
        kd.int64([[1, 2], [3], [], [4, 5]]),
    )

  def test_from_dataframe_non_primitive_df(self):
    df = pd.DataFrame({
        'self_': pd.Series(['$1', '$2', '$3'], dtype=pd.StringDtype()),
        'x': [1, 2, 3],
    })
    ds = pdkd.from_dataframe(df)
    self.assertCountEqual(kd.dir(ds), ['self_', 'x'])
    self.assertNotEqual(ds.get_schema(), kd.OBJECT)
    kd.testing.assert_equal(
        ds.get_attr('self_').no_bag(),
        kd.slice(['$1', '$2', '$3']),
    )
    kd.testing.assert_equal(ds.x.no_bag(), kd.int64([1, 2, 3]))

  def test_from_dataframe_non_primitive_df_with_as_obj(self):
    df = pd.DataFrame({
        'self_': pd.Series(['$1', '$2', '$3'], dtype=pd.StringDtype()),
        'x': [1, 2, 3],
    })
    ds = pdkd.from_dataframe(df, as_obj=True)
    self.assertCountEqual(kd.dir(ds), ['self_', 'x'])
    self.assertEqual(ds.get_schema(), kd.OBJECT)
    kd.testing.assert_equal(
        ds.get_attr('self_').no_bag(),
        kd.slice(['$1', '$2', '$3']),
    )
    kd.testing.assert_equal(ds.x.no_bag(), kd.int64([1, 2, 3]))

  def test_from_dataframe_non_primitive_df_with_as_obj_object_df(self):
    df = pd.DataFrame({'x': np.array([{1: 2}, {3: 4}], dtype=object)})
    ds = pdkd.from_dataframe(df)
    self.maxDiff = None
    kd.testing.assert_equal(
        ds.x.get_keys().no_bag(),
        kd.slice([[1], [3]], schema=kd.OBJECT),
    )
    kd.testing.assert_equal(
        ds.x.get_values().no_bag(),
        kd.slice([[2], [4]], schema=kd.OBJECT),
    )

  def test_from_dataframe_non_primitive_df_with_as_obj_empty_df(self):
    with self.assertRaisesRegex(ValueError, 'DataFrame has no columns'):
      _ = pdkd.from_dataframe(pd.DataFrame())

  @parameterized.parameters(
      (kd.slice([1, 2, 3]), [1, 2, 3], pd.Int32Dtype()),
      # OBJECTs are narrowed if possible.
      (kd.slice([1, 2, 3], kd.OBJECT), [1, 2, 3], pd.Int32Dtype()),
      # Bags are dropped / ignored for primitives.
      (kd.slice([1, 2, 3]).with_bag(kd.bag()), [1, 2, 3], pd.Int32Dtype()),
      # Missing values are represented as pd.NA.
      (kd.slice([1, 2, None]), [1, 2, pd.NA], pd.Int32Dtype()),
      # Mixed slices have dtype=object (holding python values).
      (kd.slice([1, 2.0, None], kd.OBJECT), [1, 2.0, pd.NA], 'object'),
      # Other primitives.
      (kd.slice([1, 2, None], kd.INT64), [1, 2, pd.NA], pd.Int64Dtype()),
      (kd.slice([1.0, 2.0, None]), [1.0, 2.0, pd.NA], pd.Float32Dtype()),
      (
          kd.slice([1.0, 2.0, None], kd.FLOAT64),
          [1.0, 2.0, pd.NA],
          pd.Float64Dtype(),
      ),
      (kd.slice([True, False, None]), [True, False, pd.NA], pd.BooleanDtype()),
      (kd.slice([kd.present, kd.missing]), [True, pd.NA], pd.BooleanDtype()),
      (kd.slice(['abc', None]), ['abc', pd.NA], pd.StringDtype()),
      (kd.slice([b'abc', None]), [b'abc', pd.NA], 'object'),
  )
  def test_to_dataframe_primitives(self, ds, expected_values, expected_dtype):
    df = pdkd.to_dataframe(ds)
    self.assertNotIsInstance(df.index, pd.DataFrame)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), expected_values)
    self.assertEqual(df['self_'].dtype, expected_dtype)

  def test_to_dataframe_primitive_ds_alias(self):
    self.assertIs(pdkd.df, pdkd.to_dataframe)

    ds = kd.slice([1, 2, 3])
    df = pdkd.df(ds)
    self.assertNotIsInstance(df.index, pd.DataFrame)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), [1, 2, 3])

  def test_to_dataframe_int_data_item(self):
    ds = kd.item(1)
    df = pdkd.to_dataframe(ds)
    self.assertNotIsInstance(df.index, pd.DataFrame)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), [1])

  def test_to_dataframe_multi_dimensional_int_ds(self):
    ds = kd.slice([[1, 2], [3], [], [4, 5]])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), [1, 2, 3, 4, 5])
    self.assertIsInstance(df.index, pd.MultiIndex)
    self.assertEqual(list(df.index.get_level_values(0)), [0, 0, 1, 3, 3])
    self.assertEqual(list(df.index.get_level_values(1)), [0, 1, 0, 0, 1])

  def test_to_dataframe_list_ds(self):
    l1 = kd.list()
    l2 = kd.list()
    l3 = kd.list()
    ds = kd.slice([l1, l2, l3])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), ds.internal_as_py())

  def test_to_dataframe_dict_ds(self):
    d1 = kd.dict()
    d2 = kd.dict()
    d3 = kd.dict()
    ds = kd.slice([d1, d2, d3])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), ds.internal_as_py())

  def test_to_dataframe_entity_ds(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['x', 'y'])
    self.assertEqual(list(df['x']), [1, 2, 3])
    self.assertEqual(list(df['y']), ['a', 'b', 'c'])

    df = pdkd.to_dataframe(ds, include_self=True)
    self.assertCountEqual(df.columns, ['self_', 'x', 'y'])
    self.assertEqual(list(df['self_']), ds.internal_as_py())

  def test_to_dataframe_entity_ds_with_attrs(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    df = pdkd.to_dataframe(ds, cols=['x'])
    self.assertCountEqual(df.columns, ['x'])
    self.assertEqual(list(df['x']), [1, 2, 3])

  def test_to_dataframe_obj_ds(self):
    ds = kd.obj(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['x', 'y'])
    self.assertEqual(list(df['x']), [1, 2, 3])
    self.assertEqual(list(df['y']), ['a', 'b', 'c'])

    df = pdkd.to_dataframe(ds, include_self=True)
    self.assertCountEqual(df.columns, ['self_', 'x', 'y'])
    self.assertEqual(list(df['self_']), ds.internal_as_py())

  def test_to_dataframe_obj_ds_with_different_attrs_int(self):
    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(y='c')])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['x', 'y'])
    self.assertEqual(list(df['x']), [1, 2, pd.NA])
    self.assertEqual(list(df['y']), ['a', pd.NA, 'c'])

  def test_to_dataframe_obj_ds_with_different_attrs_float(self):
    ds = kd.slice([kd.obj(x=1.0, y='a'), kd.obj(x=2.0), kd.obj(y='c')])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['x', 'y'])
    self.assertEqual(list(df['x']), [1.0, 2.0, pd.NA])
    self.assertEqual(list(df['y']), ['a', pd.NA, 'c'])

  def test_to_dataframe_mixed_obj_ds(self):
    ds = kd.slice([1, None, kd.obj(x=2), kd.obj(kd.list()), kd.obj(kd.dict())])
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['self_'])
    expected = ds.internal_as_py()
    expected[1] = pd.NA
    self.assertEqual(list(df['self_']), expected)

  def test_to_dataframe_obj_ds_with_different_attrs_and_cols(self):
    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    df = pdkd.to_dataframe(ds, cols=['x', S.get_attr('y', default=None)])
    expected_optional_column = (
        "kd.get_attr(S, DataItem('y', schema: STRING), DataItem(None,"
        ' schema: NONE))'
    )
    self.assertCountEqual(df.columns, ['x', expected_optional_column])
    self.assertEqual(list(df['x']), [1, 2, 3])
    self.assertEqual(list(df[expected_optional_column]), ['a', pd.NA, 'c'])

    ds1 = ds.fork_bag()
    ds1.y = ds1.get_attr('y', default=None)
    df = pdkd.to_dataframe(ds1, cols=['x', 'y'])
    self.assertCountEqual(df.columns, ['x', 'y'])
    self.assertEqual(list(df['x']), [1, 2, 3])
    self.assertEqual(list(df['y']), ['a', pd.NA, 'c'])

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("the attribute 'y' is missing"),
    ):
      _ = pdkd.to_dataframe(ds, cols=['y'])

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("the attribute 'z' is missing"),
    ):
      _ = pdkd.to_dataframe(ds, cols=['z'])

  def test_to_dataframe_entity_ds_without_db(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c'])).no_bag()
    df = pdkd.to_dataframe(ds)
    self.assertCountEqual(df.columns, ['self_'])
    self.assertEqual(list(df['self_']), ds.internal_as_py())

  def test_to_dataframe_use_expr_as_columns(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    df = pdkd.to_dataframe(ds, cols=[S.x, 'y', S.x + S.y])
    self.assertCountEqual(df.columns, ['S.x', 'y', 'S.x + S.y'])
    self.assertEqual(list(df['S.x']), [1, 2, 3])
    self.assertEqual(list(df['y']), [4, 5, 6])
    self.assertEqual(list(df['S.x + S.y']), [5, 7, 9])

  def test_to_dataframe_use_named_expr_as_columns(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    df = pdkd.to_dataframe(
        ds, cols=[S.x.with_name('my_x'), (S.x + S.y).with_name('my_sum')]
    )
    self.assertCountEqual(df.columns, ['my_x', 'my_sum'])
    self.assertEqual(list(df['my_x']), [1, 2, 3])
    self.assertEqual(list(df['my_sum']), [5, 7, 9])

  def test_to_dataframe_broadcast_to_common_shape(self):
    ds = kd.new(
        x=kd.slice([1, 2, 3]),
        y=kd.implode(kd.new(z=kd.slice([[4, 5], [], [6]]))),
    )
    df = pdkd.to_dataframe(ds, cols=[S.x, S.y[:].z])
    self.assertCountEqual(df.columns, ['S.x', 'S.y[:].z'])
    self.assertEqual(list(df['S.x']), [1, 1, 3])
    self.assertEqual(list(df['S.y[:].z']), [4, 5, 6])

    self.assertIsInstance(df.index, pd.MultiIndex)
    self.assertEqual(list(df.index.get_level_values(0)), [0, 0, 2])
    self.assertEqual(list(df.index.get_level_values(1)), [0, 1, 0])

  def test_to_dataframe_invalid_attr(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("the attribute 'z' is missing"),
    ):
      _ = pdkd.to_dataframe(ds, cols=['z'])

  def test_to_dataframe_invalid_column(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    with self.assertRaisesRegex(ValueError, 'Unsupported attr type'):
      _ = pdkd.to_dataframe(ds, cols=[1])

  def test_to_dataframe_invalid_expr(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    with self.assertRaisesRegex(ValueError, 'Cannot evaluate S.z on DataSlice'):
      _ = pdkd.to_dataframe(ds, cols=[S.z])

  def test_to_dataframe_broadcast_to_common_shape_error(self):
    ds = kd.new(
        x=kd.implode(kd.new(z=kd.slice([[4], [5], [6]]))),
        y=kd.implode(kd.new(z=kd.slice([[4, 5], [], [6]]))),
    )
    with self.assertRaisesRegex(
        ValueError, 'All columns must have compatible shapes'
    ):
      _ = pdkd.to_dataframe(ds, cols=[S.x[:].z, S.y[:].z])

  def test_to_dataframe_specify_columns_with_include_self(self):
    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice(['a', 'b', 'c']))
    with self.assertRaisesRegex(ValueError, 'Cannot set `include_self`'):
      _ = pdkd.to_dataframe(ds, cols=['a'], include_self=True)

  def test_object_roundtrip_preserves_type(self):
    ds = kd.slice([kd.float64(1.0), kd.int64(1)], kd.OBJECT)
    df = pdkd.to_dataframe(ds)
    res = pdkd.from_dataframe(df)
    # TODO: Make `res.self_.no_bag() == ds`.
    kd.testing.assert_equal(
        res.self_.no_bag(), kd.slice([kd.float32(1.0), kd.int32(1)], kd.OBJECT)
    )


if __name__ == '__main__':
  absltest.main()
