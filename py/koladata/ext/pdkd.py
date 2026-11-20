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

"""Tools for Pandas <-> Koda interoperability."""

from collections.abc import Iterable as _Iterable, Mapping as _Mapping

from arolla import arolla as _arolla
from arolla.experimental import numpy_conversion as _numpy_conversion
from koladata import kd as _kd
from koladata.ext import npkd as _npkd
import pandas as _pd

_kdi = _kd.eager


def _to_slice(series: _pd.Series) -> _kd.types.DataSlice:
  """Converts the `series` to a DataSlice."""
  present = ~_pd.isna(series)
  values_np = series[present].to_numpy()
  if series.dtype == 'object':
    values_ds = _kd.from_py(list(values_np), from_dim=1, schema=_kd.OBJECT)
  else:
    values_ds = _npkd.from_array(values_np)
  mask_ds = _npkd.from_array(present.to_numpy()) == _kd.bool(True)  # pyrefly: ignore[missing-attribute]
  return _kd.reverse_select(values_ds, mask_ds)


def from_dataframe(
    df_: _pd.DataFrame, as_obj: bool = False
) -> _kd.types.DataSlice:
  """Creates a DataSlice from the given pandas DataFrame.

  The DataFrame must have at least one column. It will be converted to a
  DataSlice of entities/objects with attributes corresponding to the DataFrame
  columns. Supported column dtypes include all primitive dtypes and ItemId.

  If the DataFrame has MultiIndex, it will be converted to a DataSlice with
  the shape derived from the MultiIndex.

  When `as_obj` is set, the resulting DataSlice will be a DataSlice of objects
  instead of entities.

  The conversion adheres to:
  * All missing values (according to `pd.isna`) become missing values in the
    resulting DataSlice.
  * Data with `object` dtype is converted to an OBJECT DataSlice.
  * Data with other dtypes is converted to a DataSlice with corresponding
    schema.

  Args:
   df_: pandas DataFrame to convert.
   as_obj: whether to convert the resulting DataSlice to Objects.

  Returns:
    DataSlice of items with attributes from DataFrame columns.
  """
  kwargs = {c: _to_slice(df_[c]) for c in df_.columns}

  if not kwargs:
    raise ValueError('DataFrame has no columns.')

  res = (
      _kdi.obj(**kwargs)
      if as_obj
      else _kdi.new(overwrite_schema=False, **kwargs)
  )

  if isinstance(df_.index, _pd.MultiIndex):
    indices = [
        df_.index.get_level_values(i).to_numpy()
        for i in range(df_.index.nlevels)
    ]
    return _npkd.reshape_based_on_indices(res, indices)

  return res


def from_series(series: _pd.Series) -> _kd.types.DataSlice:
  """Creates a DataSlice from the given pandas Series.

  The Series is first converted to a DataFrame with a single column named
  'self_', and then `from_dataframe` is used to convert it to a DataSlice.

  Args:
    series: pandas Series to convert.

  Returns:
    DataSlice representing the content of the Series.
  """
  res_df = _pd.DataFrame({'self_': series})
  return from_dataframe(res_df).self_


_SPECIAL_COLUMN_NAMES = ('__items__', '__keys__', '__values__')
_PD_FROM_KD = {
    _kd.INT32: _pd.Int32Dtype(),
    _kd.INT64: _pd.Int64Dtype(),
    _kd.FLOAT32: _pd.Float32Dtype(),
    _kd.FLOAT64: _pd.Float64Dtype(),
    _kd.BOOLEAN: _pd.BooleanDtype(),
    _kd.MASK: _pd.BooleanDtype(),
}


def _flat_ds_to_series(ds: _kd.types.DataSlice) -> _pd.Series:
  """Returns `ds` converted to a pd.Series.

  Note: this is _not_ a general purpose conversion tool as it expects `ds` to be
  flat. Callers must ensure that the input is flattened and is responsible for
  reindexing the result if needed.

  Args:
    ds: flat DataSlice to convert.
  """
  assert ds.get_ndim() == 1
  dtype = ds.get_dtype()
  if dtype in _PD_FROM_KD:
    # Fast path through arolla conversion for compatible types.
    if dtype == _kd.MASK:
      ds = _kd.cond(ds, True, None)
    indices, values = _numpy_conversion.as_numpy_array_with_indices(
        ds.internal_as_dense_array()
    )
    res = _pd.Series(values, index=indices, dtype=_PD_FROM_KD[dtype])
    return res.reindex(range(ds.get_size()))
  else:
    # We have a mix of types or something else that can't be converted through
    # a fast path. We do a best effort here going through python.
    #
    # TODO: Preserve numeric per-element dtypes. Right now, float64
    # will be converted to a python float that loses information about the
    # original width. When converting back to a DataSlice, this is then parsed
    # as float32.

    def _normalize(v):
      if v is None:
        return _pd.NA
      elif isinstance(v, _kd.types.DataItem) and v.get_dtype() == _kd.MASK:
        assert v == _kd.present
        return True
      else:
        return v

    pd_dtype = _pd.StringDtype() if dtype == _kd.STRING else 'object'
    ds_py = [_normalize(v) for v in ds.internal_as_py()]
    return _pd.Series(ds_py, dtype=pd_dtype)


def _prepare_cols(
    ds: _kd.types.DataSlice,
    cols: (
        _Iterable[str | _arolla.Expr] | _Mapping[str, str | _arolla.Expr] | None
    ) = None,
    include_self: bool = False,
) -> tuple[dict[str, _pd.Series], _pd.Index | None]:
  """Shared utility to prepare DataFrame columns and index."""
  if ds.get_ndim() == 0:
    ds = ds.repeat(1)

  if cols is not None:
    if not isinstance(cols, _Mapping):
      cols = list(cols)
    if not ds.has_bag():
      raise ValueError(
          f'Cannot specify columns {cols!r} for a DataSlice without a db.'
      )
    if include_self:
      raise ValueError(
          f'Cannot set `include_self` when specifying columns {cols!r}. Add'
          " 'self_' to the list of columns instead."
      )

  get_attr_fn = _kdi.get_attr
  schema = ds.get_schema()
  if cols is None:
    if not ds.has_bag():
      cols = ['self_']
    elif schema.is_entity_schema():
      cols = _kd.dir(ds)
      if include_self or not cols:
        cols.append('self_')
    elif schema == _kd.OBJECT and _kd.is_entity(ds):
      cols = _kd.dir(ds, intersection=False)
      get_attr_fn = _kdi.maybe
      if include_self or not cols:
        cols.append('self_')
    else:
      cols = ['self_']

  def _eval_col(col):
    if isinstance(col, str):
      if col == 'self_':
        return ds
      return get_attr_fn(ds, col)
    elif isinstance(col, _arolla.Expr):
      try:
        return _kdi.eval(col, ds)
      except ValueError as e:
        raise ValueError(f'Cannot evaluate {col} on {ds!r}.') from e
    else:
      raise ValueError(f'Unsupported attr type: {type(col)}')

  col_dss = []
  col_names = []
  if isinstance(cols, _Mapping):
    for col_name, col in cols.items():
      col_dss.append(_eval_col(col))
      col_names.append(col_name)
  else:
    for col in cols:
      if isinstance(col, str):
        if col in _SPECIAL_COLUMN_NAMES:
          continue
        col_names.append(col)
      elif isinstance(col, _arolla.Expr):
        if (expr_name := _kdi.expr.get_name(col)) is not None:
          col_names.append(expr_name)
        else:
          col_names.append(str(col))
      else:
        pass
      col_dss.append(_eval_col(col))

  try:
    col_dss = _kdi.align(*col_dss)
  except ValueError as e:
    raise ValueError('All columns must have compatible shapes.') from e

  col_dict = {
      name: _flat_ds_to_series(col_ds.flatten()).values
      for col_ds, name in zip(col_dss, col_names)
  }

  index = None
  ds_for_index = col_dss[0]
  if ds_for_index.get_ndim() > 1:
    index = _pd.MultiIndex.from_arrays(
        _npkd.get_elements_indices_from_ds(ds_for_index)
    )

  return col_dict, index


def to_dataframe(
    ds: _kd.types.DataSlice,
    cols: (
        _Iterable[str | _arolla.Expr] | _Mapping[str, str | _arolla.Expr] | None
    ) = None,
    include_self: bool = False,
) -> _pd.DataFrame:
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
  extracted as DataFrame columns. It can be a sequence of string names of
  attributes, sequence of Exprs, or a mapping column names to string names of
  attributes or Exprs. If `ds` has OBJECT schema, specified attributes must
  be present in all objects in `ds`. To ignore objects which do not have
  specific attributes, one can use `S.maybe(attr)` in `cols`. For example,

    ds = kd.slice([1, 2, 3])
    to_dataframe(ds) -> extract 'self_'

    ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    to_dataframe(ds, ['x']) -> extract 'x'
    to_dataframe(ds, [S.x, S.x + S.y]) -> extract 'S.x' and 'S.x + S.y'
    to_dataframe(ds, {'my_x': S.x, 'my_y': 'y'}) -> extract 'my_x' and 'my_y'

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

  The conversion adheres to:
    * All output data will be of nullable types (e.g. `Int64Dtype()` rather than
      `np.int64`)
    * `pd.NA` is used for missing values.
    * Numeric dtypes, booleans and strings will use corresponding pandas dtypes.
    * MASK will be converted to pd.BooleanDtype(), with `kd.present => True` and
      `kd.missing => pd.NA`.
    * All other dtypes (including a mixed DataSlice) will use the `object` dtype
      holding python data, with missing values represented through `pd.NA`.
      `kd.present` is converted to True.

  Args:
    ds: DataSlice to convert.
    cols: list of columns to extract or a dictionary mapping output column names
      to columns to extract from DataSlice. If None all attributes will be
      extracted.
    include_self: whether to include the 'self_' column. 'self_' column is
      always included if `cols` is None and `ds` contains primitives/lists/dicts
      or it has ITEMID schema.

  Returns:
    DataFrame with columns from DataSlice fields.
  """
  col_dict, index = _prepare_cols(ds, cols, include_self)
  return _pd.DataFrame(col_dict, index=index)


def to_series(
    ds: _kd.types.DataSlice,
    col: str | _arolla.Expr | None = None,
) -> _pd.Series:
  """Creates a pandas Series from the given DataSlice.

  If `col` is not provided, it behaves like `to_dataframe` with no columns
  specified and extracts 'self_' or raises an error if the inference would
  yield multiple columns.

  Args:
    ds: DataSlice to convert.
    col: the column to extract from the DataSlice. If None, inference is used.

  Returns:
    Series representing the extracted column.
  """
  cols = [col] if col is not None else None
  col_dict, index = _prepare_cols(ds, cols)

  if len(col_dict) != 1:
    raise ValueError(
        f'to_series requires exactly one column, got {list(col_dict.keys())}'
    )
  return _pd.Series(
      list(col_dict.values())[0],
      index=index,
      name=list(col_dict.keys())[0],
  )


df = to_dataframe
