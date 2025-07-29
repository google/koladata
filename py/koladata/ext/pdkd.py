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

"""Bridge between Pandas and DataSlice."""

from arolla import arolla
from koladata import kd
from koladata.ext import npkd
import pandas as pd

kdi = kd.eager


def from_dataframe(
    df: pd.DataFrame, as_obj: bool = False
) -> kd.types.DataSlice:
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
  kwargs = {c: npkd.from_array(df[c].to_numpy()) for c in df.columns}

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


def to_dataframe(
    ds: kd.types.DataSlice,
    cols: list[str | arolla.Expr] | None = None,
    include_self: bool = False,
) -> pd.DataFrame:
  """Creates a pandas DataFrame from the given DataSlice.

  If `ds` has no dimension, it will be converted to a single row DataFrame. If
  it has one dimension, it willbe converted an 1D DataFrame. If it has more than
  one dimension, it will be converted to a MultiIndex DataFrame with index
  columns corresponding to each dimension.

  When `cols` is not specified, DataFrame columns are inferred from `ds`.
    1) If `ds` has primitives, lists, dicts or ITEMID schema, a single
       column named 'self_' is used and items themselves are extracted.
    2) If `ds` has entity schema, all attributes from `ds` are extracted as
       columns.
    3) If `ds` has OBJECT schema, the union of attributes from all objects in
       `ds` are used as columns. Missing values are filled if objects do not
       have corresponding attributes.

  For example,

    ds = kd.slice([1, 2, 3])
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds) -> extract 'x' and 'y'

    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    to_dataframe(ds) -> extract 'x', 'y'

  `cols` can be used to specify which data from the DataSlice should be
  extracted as DataFrame columns. It can contain either the string names of
  attributes or Exprs which can be evaluated on the DataSlice. If `ds` has
  OBJECT schema, specified attributes must present in all objects in `ds`. To
  ignore objects which do not have specific attributes, one can use
  `S.maybe(attr)` in `cols`. For example,

    ds = kd.slice([1, 2, 3])
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [S.x, S.x + S.y]) -> extract 'S.x' and 'S.x + S.y'

    ds = kd.slice([kd.obj(x=1, y='a'), kd.obj(x=2), kd.obj(x=3, y='c')])
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [S.y]) -> raise an exception as 'y' does not exist in
        kd.obj(x=2)
    to_dataframe(ds, [S.maybe('y')]) -> extract 'y' but ignore items which
        do not have 'x' attribute.

  If extracted column DataSlices have different shapes, they will be aligned to
  the same dimensions. For example,

    ds = kd.new(
        x = kd.slice([1, 2, 3]),
        y=kd.list(kd.new(z=kd.slice([[4], [5], [6]]))),
        z=kd.list(kd.new(z=kd.slice([[4, 5], [], [6]]))),
    )
    to_dataframe(ds, cols=[S.x, S.y[:].z]) -> extract 'S.x' and 'S.y[:].z':
           'x' 'y[:].z'
      0 0   1     4
        1   1     5
      2 0   3     6
    to_dataframe(ds, cols=[S.y[:].z, S.z[:].z]) -> error: shapes mismatch


  Args:
    ds: DataSlice to convert.
    cols: list of columns to extract from DataSlice. If None all attributes will
      be extracted.
    include_self: whether to include the 'self_' column. 'self_' column is
      always included if `cols` is None and `ds` contains primitives/lists/dicts
      or it has ITEMID schema.

  Returns:
    DataFrame with columns from DataSlice fields.
  """
  if ds.get_ndim() == 0:
    ds = ds.repeat(1)

  if cols is not None:
    if ds.get_bag() is None:
      raise ValueError(
          f'Cannot specify columns {cols!r} for a DataSlice without a db.'
      )
    if include_self:
      raise ValueError(
          f'Cannot set `include_self` when specifying columns {cols!r}. Add'
          " 'self_' to the list of columns instead."
      )

  get_attr_fn = kdi.get_attr
  schema = ds.get_schema()
  if cols is None:
    if ds.get_bag() is None:
      cols = ['self_']
    elif schema.is_entity_schema():
      cols = ds.get_attr_names(intersection=True)
      if include_self:
        cols.append('self_')
    elif schema == kd.OBJECT and kd.is_entity(ds):
      cols = ds.get_attr_names(intersection=False)
      get_attr_fn = kdi.maybe
      if include_self:
        cols.append('self_')
    else:
      cols = ['self_']

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
      name: npkd.to_array(col_ds.flatten())
      for col_ds, name in zip(col_dss, col_names)
  }

  index = None
  ds_for_index = col_dss[0]
  if ds_for_index.get_ndim() > 1:
    index = pd.MultiIndex.from_arrays(
        npkd.get_elements_indices_from_ds(ds_for_index)
    )

  return pd.DataFrame(col_dict, index=index)
