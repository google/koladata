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

"""Bridge between Pandas and DataSlice."""

from typing import Optional

from arolla import arolla
from koladata import kd
from koladata.ext import npkd
from koladata.types import data_slice
import pandas as pd

kdi = kd.kdi


def from_dataframe(
    df: pd.DataFrame, as_obj: bool = False
) -> data_slice.DataSlice:
  """Creates a DataSlice from the given pandas DataFrame.

  The DataFrame must have at least one column. It will be converted to a
  DataSlice of entities/objects with attributes corresponding to the DataFrame
  columns. Supported column dtypes include all primitive dtypes and ItemId.

  If the DataFrame has MultiIndex, it will be converted to a DataSlice with
  the shape derived from the MultiIndex.

  When `as_obj` is set, the resulting DataSlice will be a DataSlice of objects
  instead of entities.

  Args:
   df: pandas DataFrame to convert.
   as_obj: whether to convert the resulting DataSlice to Objects.

  Returns:
    DataSlice of items with attributes from DataFrame columns.
  """
  kwargs = {c: npkd.ds_from_np(df[c].to_numpy()) for c in df.columns}

  if not kwargs:
    raise ValueError('DataFrame has no columns.')

  res = kdi.obj(**kwargs) if as_obj else kdi.new(**kwargs)

  if isinstance(df.index, pd.MultiIndex):
    indices = [
        df.index.get_level_values(i).to_numpy() for i in range(df.index.nlevels)
    ]
    return npkd.reshape_based_on_indices(res, indices)

  return res


_SPECIAL_COLUMN_NAMES = ('__items__', '__keys__', '__values__')


def _get_column_names(ds: data_slice.DataSlice) -> set[str]:
  column_names = set(kdi.dir(ds.get_schema()))
  if column_names:
    return column_names
  for o in ds.flatten().internal_as_py():
    if isinstance(o, data_slice.DataSlice):
      column_names.update(kdi.dir(o))
  return column_names


def to_dataframe(
    ds: data_slice.DataSlice,
    cols: Optional[list[str | arolla.Expr]] = None,
) -> pd.DataFrame:
  """Creates a pandas DataFrame from the given DataSlice.

  If `ds` has no dimension, it will be converted to a single row DataFrame. If
  it has one dimension, it willbe converted an 1D DataFrame. If it has more than
  one dimension, it will be converted to a MultiIndex DataFrame with index
  columns corresponding to each dimension.

  `cols` can be used to specify which data from the DataSlice should be
  extracted as DataFrame columns. It can contain either the string names of
  attributes or Exprs which can be evaluated on the DataSlice. If `None`, all
  attributes will be extracted. If `ds` does not have attributes (e.g. it has
  only primitives, Lists, Dicts), its items will be extracted as a column named
  'self_'. For example,

    ds = kd.slice([1, 2, 3]
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds) -> extract 'x' and 'y'
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [I.x, I.x + I.y]) -> extract 'I.x' and 'I.x + I.y'

  If `ds` has OBJECT schema and `cols` is not specified, the union of attributes
  from all Object items in the `ds` will be extracted and missing values will be
  filled if Objects do not have corresponding attributes. If `cols` is
  specified, all attributes must present in all Object items. To ignore items
  which do not have specific attributes, one can set the attributes on `ds`
  using `ds.attr = ds.maybe(attr)` or use `I.self.maybe(attr)` in `cols`. For
  example,

    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    to_dataframe(ds) -> extract 'x', 'y'
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, ['y']) -> raise an exception as 'y' does not exist in
        kd.obj(x=2)
    to_dataframe(ds, [I.self.maybe('x')]) -> extract 'x' but ignore items which
        do not have 'x' attribute.

  If extracted column DataSlices have different shapes, they will be aligned to
  the same dimensions. For example,

    ds = kd.new(
        x = kd.slice([1, 2, 3]),
        y=kd.list(kd.new(z=kd.slice([[4], [5], [6]]))),
        z=kd.list(kd.new(z=kd.slice([[4, 5], [], [6]]))),
    )
    to_dataframe(ds, cols=[I.x, I.y[:].z]) -> extract 'I.x' and 'I.y[:].z':
           'x' 'y[:].z'
      0 0   1     4
        1   1     5
      2 0   3     6
    to_dataframe(ds, cols=[I.y[:].z, I.z[:].z]) -> error: shapes mismatch


  Args:
    ds: DataSlice to convert.
    cols: list of columns to extract from DataSlice. If None all attributes will
      be extracted.

  Returns:
    DataFrame with columns from DataSlice fields.
  """
  if ds.get_ndim() == 0:
    ds = ds.add_dim(1)

  get_attr_fn = kdi.get_attr
  if ds.get_bag() is None:
    if cols is not None:
      raise ValueError(
          f'Cannot specify columns {cols!r} for a DataSlice without a db.'
      )
    cols = ['self_']
  elif cols is None:
    cols = ['self_']
    cols.extend(_get_column_names(ds))
    get_attr_fn = kdi.maybe

  col_dss = []
  col_names = []
  for col in cols:
    if isinstance(col, str):
      if col in _SPECIAL_COLUMN_NAMES:
        continue
      if col == 'self_':
        col_dss.append(ds)
      else:
        col_dss.append(get_attr_fn(ds, col))
      col_names.append(col)
    elif isinstance(col, arolla.Expr):
      try:
        col_ds = kdi.eval(col, ds)
      except ValueError as e:
        raise ValueError(f'Cannot evaluate {col} on {ds!r}.') from e
      col_dss.append(col_ds)
      if (expr_name := kdi.expr.get_name(col)) is not None:
        col_names.append(expr_name)
      else:
        col_names.append(str(col))
    else:
      raise ValueError(f'Unsupported attr type: {type(col)}')

  try:
    col_dss = kdi.align(*col_dss)
  except ValueError as e:
    raise ValueError('All columns must have compatible shapes.') from e

  col_dict = {
      name: npkd.ds_to_np(col_ds.flatten())
      for col_ds, name in zip(col_dss, col_names)
  }

  index = None
  ds_for_index = col_dss[0]
  if ds_for_index.get_ndim() > 1:
    index = pd.MultiIndex.from_arrays(
        npkd.get_elements_indices_from_ds(ds_for_index)
    )

  return pd.DataFrame(col_dict, index=index)
