<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.py API

Operators that call Python functions.





### `kd.py.apply_py(fn, *args, return_type_as=unspecified, **kwargs)` {#kd.py.apply_py}
Aliases:

- [kd.apply_py](../kd.md#kd.apply_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Applies Python function `fn` on args.

It is equivalent to fn(*args, **kwargs).

Args:
  fn: function to apply to `*args` and `**kwargs`. It is required that this
    function returns a DataSlice/DataItem or a primitive that will be
    automatically wrapped into a DataItem.
  *args: positional arguments to pass to `fn`.
  return_type_as: The return type of the function is expected to be the same
    as the return type of this expression. In most cases, this will be a
    literal of the corresponding type. This needs to be specified if the
    function does not return a DataSlice/DataItem or a primitive that would be
    auto-boxed into a DataItem. kd.types.DataSlice, kd.types.DataBag and
    kd.types.JaggedShape can also be passed here.
  **kwargs: keyword arguments to pass to `fn`.

Returns:
  Result of fn applied on the arguments.</code></pre>

### `kd.py.map_py(fn, *args, schema=None, max_threads=1, ndim=0, include_missing=None, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.py.map_py}
Aliases:

- [kd.map_py](../kd.md#kd.map_py)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply the python function `fn` on provided `args` and `kwargs`.

Example:
  def my_fn(x, y):
    if x is None or y is None:
      return None
    return x * y

  kd.map_py(my_fn, slice_1, slice_2)
  # Via keyword
  kd.map_py(my_fn, x=slice_1, y=slice_2)

All DataSlices in `args` and `kwargs` must have compatible shapes.

Lambdas also work for object inputs/outputs.
In this case, objects are wrapped as DataSlices.
For example:
  def my_fn_object_inputs(x):
    return x.y + x.z

  def my_fn_object_outputs(x):
    return db.obj(x=1, y=2) if x.z &gt; 3 else db.obj(x=2, y=1)

The `ndim` argument controls how many dimensions should be passed to `fn` in
each call. If `ndim = 0` then `0`-dimensional values will be passed, if
`ndim = 1` then python `list`s will be passed, if `ndim = 2` then lists of
python `list`s will be passed and so on.

`0`-dimensional (non-`list`) values passed to `fn` are either python
primitives (`float`, `int`, `str`, etc.) or single-valued `DataSlices`
containing `ItemId`s in the non-primitive case.

In this way, `ndim` can be used for aggregation.
For example:
  def my_agg_count(x):
    return len([i for i in x if i is not None])

  kd.map_py(my_agg_count, data_slice, ndim=1)

`fn` may return any objects that kd.from_py can handle, in other words
primitives, lists, dicts and dataslices. They will be converted to
the corresponding Koda data structures.

For example:
  def my_expansion(x):
    return [[y, y] for y in x]

  res = kd.map_py(my_expansion, data_slice, ndim=1)
  # Each item of res is a list of lists, so we can get a slice with
  # the inner items like this:
  print(res[:][:])

It&#39;s also possible to set custom serialization for the fn (i.e. if you want to
serialize the expression and later deserialize it in the different process).

For example to serialize the function using cloudpickle you can use
`kd_ext.py_cloudpickle(fn)` instead of fn.

Args:
  fn: Function.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  ndim: Dimensionality of items to pass to `fn`.
  include_missing: Specifies whether `fn` applies to all items (`=True`) or
    only to items present in all `args` and `kwargs` (`=False`, valid only
    when `ndim=0`); defaults to `False` when `ndim=0`.
  dict_as_obj: If True, will convert dicts with string keys returned by `fn`
    into Koda objects instead of Koda dicts.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called `map_py`
    in case `max_threads` is greater than 1, as we rely on this property for
    cases like progress reporting. As such, it can not be attached to the `fn`
    itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.</code></pre>

### `kd.py.map_py_on_cond(true_fn, false_fn, cond, *args, schema=None, max_threads=1, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.py.map_py_on_cond}
Aliases:

- [kd.map_py_on_cond](../kd.md#kd.map_py_on_cond)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply python functions on `args` and `kwargs` based on `cond`.

`cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
dimensions than `args` or `kwargs`.

Also see kd.map_py().

This function supports only pointwise, not aggregational, operations.
`true_fn` is applied when `cond` is kd.present. Otherwise, `false_fn` is
applied.

Args:
  true_fn: Function.
  false_fn: Function.
  cond: Conditional DataSlice.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  dict_as_obj: If True, will convert dicts with string keys returned by
    `true_fn` and `false_fn` into Koda objects instead of Koda dicts.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_cond` in case `max_threads` is greater than 1, as we rely on
    this property for cases like progress reporting. As such, it can not be
    attached to the `true_fn` and `false_fn` themselves.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.</code></pre>

### `kd.py.map_py_on_selected(fn, cond, *args, schema=None, max_threads=1, dict_as_obj=False, item_completed_callback=None, **kwargs)` {#kd.py.map_py_on_selected}
Aliases:

- [kd.map_py_on_selected](../kd.md#kd.map_py_on_selected)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Apply python function `fn` on `args` and `kwargs` based on `cond`.

`cond`, `args` and `kwargs` are first aligned. `cond` cannot have a higher
dimensions than `args` or `kwargs`.

Also see kd.map_py().

This function supports only pointwise, not aggregational, operations. `fn` is
applied when `cond` is kd.present.

Args:
  fn: Function.
  cond: Conditional DataSlice.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  dict_as_obj: If True, will convert dicts with string keys returned by `fn`
    into Koda objects instead of Koda dicts.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_selected` in case `max_threads` is greater than 1, as we rely
    on this property for cases like progress reporting. As such, it can not be
    attached to the `fn` itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.</code></pre>

