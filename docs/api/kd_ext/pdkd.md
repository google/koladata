<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.pdkd API

Tools for Pandas <-> Koda interoperability.





### `kd_ext.pdkd.Mapping()` {#kd_ext.pdkd.Mapping}

<pre class="no-copy"><code class="lang-text no-auto-prettify">A Mapping is a generic container for associating key/value
pairs.

This class provides concrete generic implementations of all
methods except for __getitem__, __iter__, and __len__.</code></pre>

### `kd_ext.pdkd.Sequence()` {#kd_ext.pdkd.Sequence}

<pre class="no-copy"><code class="lang-text no-auto-prettify">All the operations on a read-only sequence.

Concrete subclasses must override __new__ or __init__,
__getitem__, and __len__.</code></pre>

### `kd_ext.pdkd.df(ds: DataSlice, cols: Sequence[str | Expr] | Mapping[str, str | Expr] | None = None, include_self: bool = False) -> DataFrame` {#kd_ext.pdkd.df}
Aliases:

- [kd_ext.pdkd.to_dataframe](#kd_ext.pdkd.to_dataframe)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a pandas DataFrame from the given DataSlice.

If `ds` has no dimension, it will be converted to a single row DataFrame. If
it has one dimension, it willbe converted an 1D DataFrame. If it has more than
one dimension, it will be converted to a MultiIndex DataFrame with index
columns corresponding to each dimension.

When `cols` is not specified, DataFrame columns are inferred from `ds`.
  1) If `ds` has primitives, lists, dicts or ITEMID schema, a single
     column named &#39;self_&#39; is used and items themselves are extracted.
  2) If `ds` has entity schema, all attributes from `ds` are extracted as
     columns.
  3) If `ds` has OBJECT schema, the union of attributes from all objects in
     `ds` are used as columns. Missing values are filled if objects do not
     have corresponding attributes.

For example,

  ds = kd.slice([1, 2, 3])
  to_dataframe(ds) -&gt; extract &#39;self_&#39;

  ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
  to_dataframe(ds) -&gt; extract &#39;x&#39; and &#39;y&#39;

  ds = kd.slice([kd.obj(x=1, y=&#39;a&#39;), kd.obj(x=2), kd.obj(x=3, y=&#39;c&#39;)])
  to_dataframe(ds) -&gt; extract &#39;x&#39;, &#39;y&#39;

`cols` can be used to specify which data from the DataSlice should be
extracted as DataFrame columns. It can be a sequence of string names of
attributes, sequence of Exprs, or a mapping column names to string names of
attributes or Exprs. If `ds` has OBJECT schema, specified attributes must
be present in all objects in `ds`. To ignore objects which do not have
specific attributes, one can use `S.maybe(attr)` in `cols`. For example,

  ds = kd.slice([1, 2, 3])
  to_dataframe(ds) -&gt; extract &#39;self_&#39;

  ds = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
  to_dataframe(ds, [&#39;x&#39;]) -&gt; extract &#39;x&#39;
  to_dataframe(ds, [S.x, S.x + S.y]) -&gt; extract &#39;S.x&#39; and &#39;S.x + S.y&#39;
  to_dataframe(ds, {&#39;my_x&#39;: S.x, &#39;my_y&#39;: &#39;y&#39;}) -&gt; extract &#39;my_x&#39; and &#39;my_y&#39;

  ds = kd.slice([kd.obj(x=1, y=&#39;a&#39;), kd.obj(x=2), kd.obj(x=3, y=&#39;c&#39;)])
  to_dataframe(ds, [&#39;x&#39;]) -&gt; extract &#39;x&#39;
  to_dataframe(ds, [S.y]) -&gt; raise an exception as &#39;y&#39; does not exist in
      kd.obj(x=2)
  to_dataframe(ds, [S.maybe(&#39;y&#39;)]) -&gt; extract &#39;y&#39; but ignore items which
      do not have &#39;x&#39; attribute.

If extracted column DataSlices have different shapes, they will be aligned to
the same dimensions. For example,

  ds = kd.new(
      x = kd.slice([1, 2, 3]),
      y=kd.list(kd.new(z=kd.slice([[4], [5], [6]]))),
      z=kd.list(kd.new(z=kd.slice([[4, 5], [], [6]]))),
  )
  to_dataframe(ds, cols=[S.x, S.y[:].z]) -&gt; extract &#39;S.x&#39; and &#39;S.y[:].z&#39;:
         &#39;x&#39; &#39;y[:].z&#39;
    0 0   1     4
      1   1     5
    2 0   3     6
  to_dataframe(ds, cols=[S.y[:].z, S.z[:].z]) -&gt; error: shapes mismatch

The conversion adheres to:
  * All output data will be of nullable types (e.g. `Int64Dtype()` rather than
    `np.int64`)
  * `pd.NA` is used for missing values.
  * Numeric dtypes, booleans and strings will use corresponding pandas dtypes.
  * MASK will be converted to pd.BooleanDtype(), with `kd.present =&gt; True` and
    `kd.missing =&gt; pd.NA`.
  * All other dtypes (including a mixed DataSlice) will use the `object` dtype
    holding python data, with missing values represented through `pd.NA`.
    `kd.present` is converted to True.

Args:
  ds: DataSlice to convert.
  cols: list of columns to extract or a dictionary mapping output column names
    to columns to extract from DataSlice. If None all attributes will be
    extracted.
  include_self: whether to include the &#39;self_&#39; column. &#39;self_&#39; column is
    always included if `cols` is None and `ds` contains primitives/lists/dicts
    or it has ITEMID schema.

Returns:
  DataFrame with columns from DataSlice fields.</code></pre>

### `kd_ext.pdkd.from_dataframe(df_: DataFrame, as_obj: bool = False) -> DataSlice` {#kd_ext.pdkd.from_dataframe}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice from the given pandas DataFrame.

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
  DataSlice of items with attributes from DataFrame columns.</code></pre>

### `kd_ext.pdkd.from_series(series: Series) -> DataSlice` {#kd_ext.pdkd.from_series}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a DataSlice from the given pandas Series.

The Series is first converted to a DataFrame with a single column named
&#39;self_&#39;, and then `from_dataframe` is used to convert it to a DataSlice.

Args:
  series: pandas Series to convert.

Returns:
  DataSlice representing the content of the Series.</code></pre>

### `kd_ext.pdkd.to_dataframe(ds: DataSlice, cols: Sequence[str | Expr] | Mapping[str, str | Expr] | None = None, include_self: bool = False) -> DataFrame` {#kd_ext.pdkd.to_dataframe}

Alias for [kd_ext.pdkd.df](#kd_ext.pdkd.df)

### `kd_ext.pdkd.to_series(ds: DataSlice, col: str | Expr | None = None) -> Series` {#kd_ext.pdkd.to_series}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a pandas Series from the given DataSlice.

If `col` is not provided, it behaves like `to_dataframe` with no columns
specified and extracts &#39;self_&#39; or raises an error if the inference would
yield multiple columns.

Args:
  ds: DataSlice to convert.
  col: the column to extract from the DataSlice. If None, inference is used.

Returns:
  Series representing the extracted column.</code></pre>

