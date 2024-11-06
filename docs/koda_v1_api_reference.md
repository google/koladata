<!-- Note: This file is auto-generated, do not edit manually. -->

# Koda API Reference

<!--* freshness: {
  reviewed: '2024-11-06'
  owner: 'amik'
  owner: 'olgasilina'
} *-->

go/koda-v1-apis

This document lists public **Koda** APIs, including operators (accessible from
`kd`, `kde` and `kdf` packages) and methods of main abstractions (e.g.
DataSlice, DataBag, etc.).

[TOC]

Category  | Description
--------- | ------------
[kd-ops](#kd-ops_category) | `kd` and `kde` operators
[kdf-ops](#kdf-ops_category) | `kdf` operators
[kd-ext-ops](#kd-ext-ops_category) | `kd_ext` operators
[DataSlice](#DataSlice_category) | `DataSlice` methods
[DataBag](#DataBag_category) | `DataBag` methods

## `kd` and `kde` operators {#kd-ops_category}

`kd` and `kde` modules are containers for eager and lazy operators respectively.

While most of operators below have both eager and lazy versions (e.g.
`kd.agg_sum` vs `kde.agg_sum`), some operators (e.g. `kd.sub(expr, *subs)`) only
have eager version. Such operators often take Exprs or Functors as inputs and
does not make sense to have a lazy version.

Note that operators from extension modules (e.g. `kd_ext.npkd`) are not
included.

<section class="zippy open">

**Operators**

### `abs(x)` {#abs}

``` {.no-copy}
Computes pointwise absolute value of the input.
```

### `add(x, y)` {#add}

``` {.no-copy}
Computes pointwise x + y.
```

### `add_dim(x, sizes)` {#add_dim}

``` {.no-copy}
Returns `x` with values repeated according to `sizes`.

The resulting slice has `rank = rank + 1`. The input `sizes` are broadcasted
to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.add_dim(ds, sizes)  # -> kd.slice([[[1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.add_dim(ds, sizes)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.add_dim(ds, size)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `add_dim_to_present(x, sizes)` {#add_dim_to_present}

``` {.no-copy}
Returns `x` with present values repeated according to `sizes`.

The resulting slice has `rank = rank + 1`. The input `sizes` are broadcasted
to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.add_dim_to_present(ds, sizes)  # -> kd.slice([[[1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.add_dim(ds, sizes)  # -> kd.slice([[[1, 1], []], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.add_dim(ds, size)  # -> kd.slice([[[1, 1], []], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `agg_all(x, ndim)` {#agg_all}

``` {.no-copy}
Returns present if all elements are present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_any(x, ndim)` {#agg_any}

``` {.no-copy}
Returns present if any element is present along the last ndim dimensions.

`x` must have MASK dtype.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_count(x, ndim)` {#agg_count}

``` {.no-copy}
Returns counts of present items over the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_count(ds)  # -> kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=1)  # -> kd.slice([2, 3, 0])
  kd.agg_count(ds, ndim=2)  # -> kd.slice(5)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).
```

### `agg_has(x, ndim)` {#agg_has}

``` {.no-copy}
Returns present iff any element is present along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

It is equivalent to `kd.agg_any(kd.has(x))`.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_max(x, ndim)` {#agg_max}

``` {.no-copy}
Returns the maximum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_max(ds)  # -> kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=1)  # -> kd.slice([2, 4, None])
  kd.agg_max(ds, ndim=2)  # -> kd.slice(4)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_mean(x, ndim)` {#agg_mean}

``` {.no-copy}
Returns the means along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, None], [3, 4], [None, None]])
  kd.agg_mean(ds)  # -> kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=1)  # -> kd.slice([1, 3.5, None])
  kd.agg_mean(ds, ndim=2)  # -> kd.slice(2.6666666666666) # (1 + 3 + 4) / 3)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_median(x, ndim)` {#agg_median}

``` {.no-copy}
Returns the medians along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_min(x, ndim)` {#agg_min}

``` {.no-copy}
Returns the minimum of items along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
  kd.agg_min(ds)  # -> kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=1)  # -> kd.slice([1, 3, None])
  kd.agg_min(ds, ndim=2)  # -> kd.slice(1)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_size(x, ndim)` {#agg_size}

``` {.no-copy}
Returns number of items in `x` over the last ndim dimensions.

Note that it counts missing items, which is different from `kd.count`.

The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.agg_size(ds)  # -> kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=1)  # -> kd.slice([3, 3, 2])
  kd.agg_size(ds, ndim=2)  # -> kd.slice(8)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  A DataSlice of number of items in `x` over the last `ndim` dimensions.
```

### `agg_std(x, unbiased, ndim)` {#agg_std}

``` {.no-copy}
Returns the standard deviation along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_std(ds)  # -> kd.slice(1.0)
  kd.agg_std(ds, unbiased=False)  # -> kd.slice(0.8164966)

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_sum(x, ndim)` {#agg_sum}

``` {.no-copy}
Returns the sums along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4], [None, None]])
  kd.agg_sum(ds)  # -> kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=1)  # -> kd.slice([2, 7, None])
  kd.agg_sum(ds, ndim=2)  # -> kd.slice(9)

Args:
  x: A DataSlice of numbers.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `agg_uuid(x, ndim)` {#agg_uuid}

``` {.no-copy}
Computes aggregated uuid of elements over the last `ndim` dimensions.

Args:
  x: A DataSlice.
  ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  DataSlice with that has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.
```

### `agg_var(x, unbiased, ndim)` {#agg_var}

``` {.no-copy}
Returns the variance along the last ndim dimensions.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([10, 9, 11])
  kd.agg_var(ds)  # -> kd.slice(1.0)
  kd.agg_var(ds, unbiased=False)  # -> kd.slice([0.6666667])

Args:
  x: A DataSlice of numbers.
  unbiased: A boolean flag indicating whether to substract 1 from the number
    of elements in the denominator.
  ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
    <= get_ndim(x).
```

### `align(*args)` {#align}

``` {.no-copy}
Expands all of the DataSlices in `args` to the same common shape.

All DataSlices must be expandable to the shape of the slice with the largest
number of dimensions.

Example:
  kd.align(kd.slice([[1, 2, 3], [4, 5]]), kd.slice('a'), kd.slice([1, 2]))
  # Returns:
  # (
  #   kd.slice([[1, 2, 3], [4, 5]]),
  #   kd.slice([['a', 'a', 'a'], ['a', 'a']]),
  #   kd.slice([[1, 1, 1], [2, 2]]),
  # )

Args:
  *args: DataSlices to align.

Returns:
  A tuple of aligned DataSlices, matching `args`.
```

### `all(x)` {#all}

``` {.no-copy}
Returns present iff all elements are present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.
```

### `any(x)` {#any}

``` {.no-copy}
Returns present iff any element is present over all dimensions.

`x` must have MASK dtype.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice.
```

### `apply_mask(x, y)` {#apply_mask}

``` {.no-copy}
Filters `x` to items where `y` is present.

Pointwise masking operator that replaces items in DataSlice `x` by None
if corresponding items in DataSlice `y` of MASK dtype is `kd.missing`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  Masked DataSlice.
```

### `apply_py(fn, *args, return_type_as, **kwargs)` {#apply_py}

``` {.no-copy}
Applies Python function `fn` on args.

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
    auto-boxed into a DataItem. kd.types.DataSlice and kd.types.DataBag can
    also be passed here.
  **kwargs: keyword arguments to pass to `fn`.

Returns:
  Result of fn applied on the arguments.
```

### `apply_py_on_cond(yes_fn, no_fn, cond, *args, **kwargs)` {#apply_py_on_cond}

``` {.no-copy}
Applies Python functions on args filtered with `cond` and `~cond`.

It is equivalent to

  yes_fn(
      *( x & cond for x in args ),
      **{ k: (v & cond) for k, v in kwargs.items() },
  ) | no_fn(
      *( x & ~cond for x in args ),
      **{ k: (v & ~cond) for k, v in kwargs.items() },
  )

Args:
  yes_fn: function to apply on filtered args.
  no_fn: function to apply on inverse filtered args (this parameter can be
    None).
  cond: filter dataslice.
  *args: arguments to filter and then pass to yes_fn and no_fn.
  **kwargs: keyword arguments to filter and then pass to yes_fn and no_fn.

Returns:
  The union of results of yes_fn and no_fn applied on filtered args.
```

### `apply_py_on_selected(fn, cond, *args, **kwargs)` {#apply_py_on_selected}

``` {.no-copy}
Applies Python function `fn` on args filtered with cond.

It is equivalent to

  fn(
      *( x & cond for x in args ),
      **{ k: (v & cond) for k, v in kwargs.items() },
  )

Args:
  fn: function to apply on filtered args.
  cond: filter dataslice.
  *args: arguments to filter and then pass to fn.
  **kwargs: keyword arguments to filter and then pass to fn.

Returns:
  Result of fn applied on filtered args.
```

### `as_any(x)` {#as_any}

``` {.no-copy}
Casts `x` to ANY using explicit (permissive) casting rules.
```

### `as_itemid(x)` {#as_itemid}

``` {.no-copy}
Casts `x` to ITEMID using explicit (permissive) casting rules.

Deprecated, use `get_itemid` instead.
```

### `at(x, indices)` {#at}

``` {.no-copy}
Returns a new DataSlice with items at provided indices.

`indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
INT64 items.

Indices in the DataSlice `indices` are based on the last dimension of the
DataSlice `x`. Negative indices are supported and out-of-bound indices result
in missing items.

If ndim(x) - 1 > ndim(indices), indices are broadcasted to shape(x)[:-1].
If ndim(x) <= ndim(indices), indices are unchanged but shape(x)[:-1] must be
broadcastable to shape(indices).

Example:
  x = kd.slice([[1, None, 2], [3, 4]])
  kd.take(x, kd.item(1))  # -> kd.slice([[None, 4]])
  kd.take(x, kd.slice([0, 1]))  # -> kd.slice([1, 4])
  kd.take(x, kd.slice([[0, 1], [1]]))  # -> kd.slice([[1, None], [4]])
  kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
    # -> kd.slice([[[1, None]], []], [[4], [3]]])
  kd.take(x, kd.slice([3, -3]))  # -> kd.slice([None, None])
  kd.take(x, kd.slice([-1, -2]))  # -> kd.slice([2, 3])
  kd.take(x, kd.slice('1')) # -> dtype mismatch error
  kd.take(x, kd.slice([1, 2, 3])) -> incompatible shape

Args:
  x: DataSlice to be indexed
  indices: indices used to select items

Returns:
  A new DataSlice with items selected by indices.
```

### `attrs(x, /, *, update_schema, **attrs)` {#attrs}

``` {.no-copy}
Returns a new Databag containing attribute updates for a slice `x`.
```

### `bag` {#bag}

``` {.no-copy}
Returns an empty DataBag.
```

### `bool(x)` {#bool}

``` {.no-copy}
Returns kd.slice(x, kd.BOOLEAN).
```

### `bytes(x)` {#bytes}

``` {.no-copy}
Returns kd.slice(x, kd.BYTES).
```

### `call(fn, *args, return_type_as, **kwargs)` {#call}

``` {.no-copy}
Calls a functor.

See the docstring of `kdf.fn` on how to create a functor.

Example:
  kd.call(kdf.fn(I.x + I.y), x=2, y=3)
  # returns kd.item(5)

  kde.call(I.fn, x=2, y=3).eval(fn=kdf.fn(I.x + I.y))
  # returns kd.item(5)

Args:
  fn: The functor to be called, typically created via kdf.fn().
  args: The positional arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.
  return_type_as: The return type of the call is expected to be the same as
    the return type of this expression. In most cases, this will be a literal
    of the corresponding type. This needs to be specified if the functor does
    not return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also
    be passed here.
  kwargs: The keyword arguments to pass to the call. Scalars will be
    auto-boxed to DataItems.

Returns:
  The result of the call.
```

### `cast_to(x, schema)` {#cast_to}

``` {.no-copy}
Returns `x` casted to the provided `schema` using explicit casting rules.

Dispatches to the relevant `kd.to_...` operator. Performs permissive casting,
e.g. allowing FLOAT32 -> INT32 casting through `kd.cast_to(slice, INT32)`.

Args:
  x: DataSlice to cast.
  schema: Schema to cast to. Must be a scalar.
```

### `ceil(x)` {#ceil}

``` {.no-copy}
Computes pointwise ceiling of the input, e.g.

rounding up: returns the smallest integer value that is not less than the
input.
```

### `clone(x, schema, **overrides)` {#clone}

``` {.no-copy}
Eager version of `kde.clone` operator.
```

### `coalesce(x, y)` {#coalesce}

``` {.no-copy}
Fills in missing values of `x` with values of `y`.

Pointwise masking operator that replaces missing items (i.e. None) in
DataSlice `x` by corresponding items in DataSlice y`.
`x` and `y` do not need to have the same type.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.
```

### `collapse(x, ndim)` {#collapse}

``` {.no-copy}
Collapses the same items over the last ndim dimensions.

Missing items are ignored. For each collapse aggregation, the result is
present if and only if there is at least one present item and all present
items are the same.

The resulting slice has `rank = rank - ndim` and shape: `shape =
shape[:-ndim]`.

Example:
  ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
  kd.collapse(ds)  # -> kd.slice([1, None, None])
  kd.collapse(ds, ndim=1)  # -> kd.slice([1, None, None])
  kd.collapse(ds, ndim=2)  # -> kd.slice(None)

Args:
  x: A DataSlice.
  ndim: The number of dimensions to collapse into. Requires 0 <= ndim <=
    get_ndim(x).

Returns:
  Collapsed DataSlice.
```

### `concat(*args, ndim)` {#concat}

``` {.no-copy}
Returns the concatenation of the given DataSlices on dimension `rank-ndim`.

All given DataSlices must have the same rank, and the shapes of the first
`rank-ndim` dimensions must match. If they have incompatible shapes, consider
using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
to bring them to compatible shapes first.

The shape of the concatenated result is the following:
  1) the shape of the first `rank-ndim` dimensions remains the same
  2) the shape of the concatenation dimension is the element-wise sum of the
    shapes of the arguments' concatenation dimensions
  3) the shapes of the last `ndim-1` dimensions are interleaved within the
    groups implied by the concatenation dimension

Alteratively, if we think of each input DataSlice as a nested Python list,
this operator simultaneously iterates over the inputs at depth `rank-ndim`,
concatenating the root lists of the corresponding nested sub-lists from each
input.

For example,
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[[1], [2]], [[3], [4]]])

kd.concat(a, b, ndim=1) -> [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
kd.concat(a, b, ndim=2) -> [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
kd.concat(a, b, ndim=3) -> [[[1, 2], [3]], [[5], [7, 8]],
                            [[1], [2]], [[3], [4]]]
kd.concat(a, b, ndim=4) -> raise an exception
kd.concat(a, b) -> the same as kd.concat(a, b, ndim=1)

The reason auto-broadcasting is not supported is that such behavior can be
confusing and often not what users want. For example,

a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[1, 2], [3, 4]])
kd.concat(a, b) -> should it be which of the following?
  [[[1, 2, 1, 2], [3, 1, 2]], [[5, 3, 4], [7, 8, 3, 4]]]
  [[[1, 2, 1, 1], [3, 2]], [[5, 3], [7, 8, 4, 4]]]
  [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]

Args:
  args: The DataSlices to concatenate.
  ndim: The number of last dimensions to concatenate (default 1).

Returns:
  The contatenation of the input DataSlices on dimension `rank-ndim`. In case
  the input DataSlices come from different DataBags, this will refer to a
  new merged immutable DataBag.
```

### `concat_lists(*lists, db)` {#concat_lists}

``` {.no-copy}
Returns a DataSlice of Lists concatenated from the List items of `lists`.

  Each input DataSlice must contain only present List items, and the item
  schemas of each input must be compatible. Input DataSlices are aligned (see
  `kde.align`) automatically before concatenation.

  If `lists` is empty, this returns a single empty list with OBJECT item schema.

  The specified `db` is used to create the new concatenated lists, and is the
  DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
  is created for this purpose.

  Args:
    *lists: the DataSlices of Lists to concatenate
    db: optional DataBag to populate with the result

  Returns:
    DataSlice of concatenated Lists
```

### `cond(condition, yes, no)` {#cond}

``` {.no-copy}
Returns `yes` where `condition` is present, otherwise `no`.

Pointwise operator selects items in `yes` if corresponding items are
`kd.present` or items in `no` otherwise. `condition` must have MASK dtype.

If `no` is unspecified corresponding items in result are missing.

Args:
  condition: DataSlice.
  yes: DataSlice.
  no: DataSlice or unspecified.

Returns:
  DataSlice of items from `yes` and `no` based on `condition`.
```

### `count(x)` {#count}

``` {.no-copy}
Returns the count of present items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `cum_count(x, ndim)` {#cum_count}

``` {.no-copy}
Computes a partial count of present items over the last `ndim` dimensions.

If `ndim` isn't specified, it defaults to 1 (count over the last dimension).

Example:
  x = kd.slice([[1, None, 1, 1], [3, 4, 5]])
  kd.cum_count(x, ndim=1)  # -> kd.slice([[1, None, 2, 3], [1, 2, 3]])
  kd.cum_count(x, ndim=2)  # -> kd.slice([[1, None, 2, 3], [4, 5, 6]])

Args:
  x: A DataSlice.
  ndim: The number of trailing dimensions to count within. Requires 0 <= ndim
    <= get_ndim(x).

Returns:
  A DataSlice of INT64 with the same shape and sparsity as `x`.
```

### `cum_max(x, ndim)` {#cum_max}

``` {.no-copy}
Returns the cumulative max of items along the last ndim dimensions.
```

### `cum_min(x, ndim)` {#cum_min}

``` {.no-copy}
Returns the cumulative minimum of items along the last ndim dimensions.
```

### `cum_sum(x, ndim)` {#cum_sum}

``` {.no-copy}
Returns the cumulative sum of items along the last ndim dimensions.
```

### `decode(x)` {#decode}

``` {.no-copy}
Decodes `x` as STRING using UTF-8 decoding.
```

### `decode_itemid(ds)` {#decode_itemid}

``` {.no-copy}
Returns the base62 text decoded into item ids.
```

### `deep_clone(x, schema, **overrides)` {#deep_clone}

``` {.no-copy}
Eager version of `kde.deep_clone` operator.
```

### `deep_uuid(obj, /, schema, *, seed)` {#deep_uuid}

``` {.no-copy}
Recursively computes uuid for obj.

Args:
  obj: The slice to take uuid on.
  schema: The schema to use to resolve '*' and '**' tokens. If not specified,
    will use the schema of the 'obj' DataSlice.
  seed: The seed to use for uuid computation.

Returns:
  Result of recursive uuid application for objs/lists/dicts.
```

### `del_attr(x, attr_name)` {#del_attr}

``` {.no-copy}
Deletes an attribute `attr_name` from `x`.
```

### `dense_rank(x, descending, ndim)` {#dense_rank}

``` {.no-copy}
Returns dense ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By dense ranking ("1 2 2 3" ranking), equal items are assigned to the same
rank and the next items are assigned to that rank plus one (i.e. no gap
between the rank numbers).

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result.

Example:

  ds = kd.slice([[4, 3, None, 3], [3, None, 2, 1]])
  kd.dense_rank(x) -> kd.slice([[1, 0, None, 0], [2, None, 1, 0]])

  kd.dense_rank(x, descending=True) ->
      kd.slice([[0, 1, None, 1], [0, None, 1, 2]])

  kd.dense_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.dense_rank(x, ndim=2) -> kd.slice([[3, 2, None, 2], [2, None, 1, 0]])

Args:
  x: DataSlice to rank.
  descending: If true, items are compared in descending order.
  ndim: The number of dimensions to rank over.
    Requires 0 <= ndim <= get_ndim(x).

Returns:
  A DataSlice of dense ranks.
```

### `dict(items_or_keys, values, *, key_schema, value_schema, schema, itemid, db)` {#dict}

``` {.no-copy}
Creates a Koda dict.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) a Python dict whose keys are either primitives or DataItems and values
       are primitives, DataItems, Python list/dict which can be converted to a
       List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
       a single dict
    3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict({1: 2, 3: 4}) -> returns a single new dict
  dict({1: [1, 2]}) -> returns a single dict, mapping 1->List[1, 2]
  dict({1: kd.slice([1, 2])}) -> returns a single dict, mapping 1->List[1, 2]
  dict({db.uuobj(x=1, y=2): 3}) -> returns a single dict, mapping uuid->3
  dict(kd.slice([1, 2]), kd.slice([3, 4]))
    -> returns a dict ({1: 3, 2: 4})
  dict(kd.slice([[1], [2]]), kd.slice([3, 4]))
    -> returns a 1-D DataSlice that holds two dicts ({1: 3} and {2: 4})
  dict('key', 12) -> returns a single dict mapping 'key'->12

  Args:
    items_or_keys: a Python dict in case of items and a DataSlice in case of
      keys.
    values: a DataSlice. If provided, `items_or_keys` must be a DataSlice as
      keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where dict(s) are created.

  Returns:
    A DataSlice with the dict.
```

### `dict_like(shape_and_mask_from, items_or_keys, values, *, key_schema, value_schema, schema, itemid, db)` {#dict_like}

``` {.no-copy}
Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to shape_and_mask_from
  shape, or one dimension higher.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired dicts.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar
      shape_and_mask_from.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `dict_schema(key_schema, value_schema, db)` {#dict_schema}

``` {.no-copy}
Creates a dict schema in the given DataBag.

  Args:
    key_schema: schema of the keys in the list.
    value_schema: schema of the values in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a dict schema.
```

### `dict_shaped(shape, items_or_keys, values, key_schema, value_schema, schema, itemid, db)` {#dict_shaped}

``` {.no-copy}
Creates new Koda dicts with the given shape.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape: the desired shape.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar shape.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `dict_shaped_as(shape_from, items_or_keys, values, key_schema, value_schema, schema, itemid, db)` {#dict_shaped_as}

``` {.no-copy}
Creates new Koda dicts with shape of the given DataSlice.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape_from: mandatory DataSlice, whose shape the returned DataSlice will
      have.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar shape.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: Optional DataBag where dicts are created.

  Returns:
    A DataSlice with the dicts.
```

### `dict_size(dict_slice)` {#dict_size}

``` {.no-copy}
Returns size of a Dict.
```

### `dict_update(x, keys, values)` {#dict_update}

``` {.no-copy}
Returns DataBag containing updates to a slice of dicts.

This operator has two forms:
  kde.dict_update(x, keys, values) where keys and values are slices
  kde.dict_update(x, dict_updates) where dict_updates is a slice of dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

### `disjoint_coalesce(x, y)` {#disjoint_coalesce}

``` {.no-copy}
Fills in missing values of `x` with values of `y`.

Raises if `x` and `y` intersect. It is equivalent to `x | y` with additional
assertion that `x` and `y` are disjoint.

Args:
  x: DataSlice.
  y: DataSlice used to fill missing items in `x`.

Returns:
  Coalesced DataSlice.
```

### `divide(x, y)` {#divide}

``` {.no-copy}
Computes pointwise x / y.
```

### `dumps(x, /, *, riegeli_options)` {#dumps}

``` {.no-copy}
Serializes a DataSlice or a DataBag.

  Due to current limitations of the underlying implementation, this can
  only serialize data slices with up to roughly 10**8 items.

  Args:
    x: DataSlice or DataBag to serialize.
    riegeli_options: A string with riegeli/records writer options. See
      https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
        for details. If not provided, default options will be used.

  Returns:
    Serialized data.
```

### `embed_schema(x)` {#embed_schema}

``` {.no-copy}
Returns a DataSlice with OBJECT schema.

  * For primitives no data change is done.
  * For Entities schema is stored as '__schema__' attribute.
  * Embedding Entities requires a DataSlice to be associated with a DataBag.

  Args:
    x: (DataSlice) whose schema is embedded.
```

### `empty_shaped(shape, *, schema, db)` {#empty_shaped}

``` {.no-copy}
Creates a DataSlice of missing items with the given shape.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape: Shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the given shape.
```

### `empty_shaped_as(shape_from, *, schema, db)` {#empty_shaped_as}

``` {.no-copy}
Creates a DataSlice of missing items with the shape of `shape_from`.

  If `schema` is an Entity schema and `db` is not provided, an empty Databag is
  created and attached to the resulting DataSlice and `schema` is adopted into
  the DataBag.

  Args:
    shape_from: used for the shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
    db: optional DataBag to hold the schema if applicable.

  Returns:
    A DataSlice with the shape of the given DataSlice.
```

### `encode(x)` {#encode}

``` {.no-copy}
Encodes `x` as BYTES using UTF-8 encoding.
```

### `encode_itemid(ds)` {#encode_itemid}

``` {.no-copy}
Returns the base62 encoded item ids in `ds` as Text.
```

### `enriched(ds, *bag)` {#enriched}

``` {.no-copy}
Returns a copy of a DataSlice with a additional fallback DataBag(s).

Values in the original DataBag of `ds` take precedence over the ones in
`*bag`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`enriched(x, a, b)` is equivalent to `enriched(enriched(x, a), b)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: additional fallback DataBag(s).

Returns:
  DataSlice with additional fallbacks.
```

### `enriched_bag(*bags)` {#enriched_bag}

``` {.no-copy}
Creates a new immutable DataBag enriched by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags earlier in the list have higher priority.
 `enriched_bag(bag1, bag2, bag3)` is equivalent to
 `enriched_bag(enriched_bag(bag1, bag2), bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for enriching.

Returns:
  An immutable DataBag enriched by `bags`.
```

### `equal(x, y)` {#equal}

``` {.no-copy}
Returns present iff `x` and `y` are equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are equal. Returns `kd.present` for equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `eval(expr, self_input, /, **input_values)` {#eval}

``` {.no-copy}
Returns the expr evaluated on the given `input_values`.

  Only Koda Inputs from container `I` (e.g. `I.x`) can be evaluated. Other
  input types must be substituted before calling this function.

  Args:
    expr: Koda expression with inputs from container `I`.
    self_input: The value for I.self input. When not provided, it will still
      have a default value that can be passed to a subroutine.
    **input_values: Values to evaluate `expr` with. Note that all inputs in
      `expr` must be present in the input values. All input values should either
      be DataSlices or convertible to DataSlices.
```

### `exp(x)` {#exp}

``` {.no-copy}
Computes pointwise exponential of the input.
```

### `expand_to(x, target, ndim)` {#expand_to}

``` {.no-copy}
Expands `x` based on the shape of `target`.

When `ndim` is not set, expands `x` to the shape of
`target`. The dimensions of `x` must be the same as the first N
dimensions of `target` where N is the number of dimensions of `x`. For
example,

Example 1:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[[0], [0, 0]], [[0, 0, 0]]])
  result: kd.slice([[[1], [2, 2]], [[3, 3, 3]]])

Example 2:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[[0]], [[0, 0, 0]]])
  result: incompatible shapes

Example 3:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([0, 0])
  result: incompatible shapes

When `ndim` is set, the expansion is performed in 3 steps:
  1) the last N dimensions of `x` are first imploded into lists
  2) the expansion operation is performed on the DataSlice of lists
  3) the lists in the expanded DataSlice are exploded

The result will have M + ndim dimensions where M is the number
of dimensions of `target`.

For example,

Example 4:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[1], [2, 3]])
  ndim: 1
  result: kd.slice([[[1, 2]], [[3], [3]]])

Example 5:
  x: kd.slice([[1, 2], [3]])
  target: kd.slice([[1], [2, 3]])
  ndim: 2
  result: kd.slice([[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]])

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode during expansion.

Returns:
  Expanded DataSlice
```

### `expand_to_shape(x, shape, ndim)` {#expand_to_shape}

``` {.no-copy}
Expands `x` based on the provided `shape`.

When `ndim` is not set, expands `x` to `shape`. The dimensions
of `x` must be the same as the first N dimensions of `shape` where N is the
number of dimensions of `x`. For example,

Example 1:
  x: [[1, 2], [3]]
  shape: JaggedShape(3, [2, 1], [1, 2, 3])
  result: [[[1], [2, 2]], [[3, 3, 3]]]

Example 2:
  x: [[1, 2], [3]]
  shape: JaggedShape(3, [1, 1], [1, 3])
  result: incompatible shapes

Example 3:
  x: [[1, 2], [3]]
  shape: JaggedShape(2)
  result: incompatible shapes

When `ndim` is set, the expansion is performed in 3 steps:
  1) the last N dimensions of `x` are first imploded into lists
  2) the expansion operation is performed on the DataSlice of lists
  3) the lists in the expanded DataSlice are exploded

The result will have M + ndim dimensions where M is the number
of dimensions of `shape`.

For example,

Example 4:
  x: [[1, 2], [3]]
  shape: JaggedShape(2, [1, 2])
  ndim: 1
  result: [[[1, 2]], [[3], [3]]]

Example 5:
  x: [[1, 2], [3]]
  shape: JaggedShape(2, [1, 2])
  ndim: 2
  result: [[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]]

Args:
  x: DataSlice to expand.
  shape: JaggedShape.
  ndim: the number of dimensions to implode during expansion.

Returns:
  Expanded DataSlice
```

### `explode(x, ndim)` {#explode}

``` {.no-copy}
Explodes a List DataSlice `x` a specified number of times.

A single list "explosion" converts a rank-K DataSlice of LIST[T] to a
rank-(K+1) DataSlice of T, by unpacking the items in the Lists in the original
DataSlice as a new DataSlice dimension in the result. Missing values in the
original DataSlice are treated as empty lists.

A single list explosion can also be done with `x[:]`.

If `ndim` is set to a non-negative integer, explodes recursively `ndim` times.
An `ndim` of zero is a no-op.

If `ndim` is set to a negative integer, explodes as many times as possible,
until at least one of the items of the resulting DataSlice is not a List.

Args:
  x: DataSlice of Lists to explode
  ndim: the number of explosion operations to perform, defaults to 1

Returns:
  DataSlice
```

### `expr_quote(x)` {#expr_quote}

``` {.no-copy}
Returns kd.slice(x, kd.EXPR).
```

### `extract(ds, schema)` {#extract}

``` {.no-copy}
Creates a slice with a new DataBag containing only reachable objects.

Args:
  ds: DataSlice to extract.
  schema: schema of the extracted slice.

Returns:
  The same data slice with a new DataBag attached.
```

### `flatten(x, from_dim, to_dim)` {#flatten}

``` {.no-copy}
Returns `x` with dimensions `[from_dim:to_dim]` flattened.

Indexing works as in python:
* If `to_dim` is unspecified, `to_dim = rank()` is used.
* If `to_dim < from_dim`, `to_dim = from_dim` is used.
* If `to_dim < 0`, `max(0, to_dim + rank())` is used. The same goes for
  `from_dim`.
* If `to_dim > rank()`, `rank()` is used. The same goes for `from_dim`.

The above-mentioned adjustments places both `from_dim` and `to_dim` in the
range `[0, rank()]`. After adjustments, the new DataSlice has `rank() ==
old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
"unit" dimension is inserted at `from_dim`.

Example:
  # Flatten the last two dimensions into a single dimension, producing a
  # DataSlice with `rank = old_rank - 1`.
  kd.get_shape(x)  # -> JaggedShape(..., [2, 1], [7, 5, 3])
  flat_x = kd.flatten(x, -2)
  kd.get_shape(flat_x)  # -> JaggedShape(..., [12, 3])

  # Flatten all dimensions except the last, producing a DataSlice with
  # `rank = 2`.
  kd.get_shape(x)  # -> jaggedShape(..., [7, 5, 3])
  flat_x = kd.flatten(x, 0, -1)
  kd.get_shape(flat_x)  # -> JaggedShape([3], [7, 5, 3])

  # Flatten all dimensions.
  kd.get_shape(x)  # -> JaggedShape([3], [7, 5, 3])
  flat_x = kd.flatten(x)
  kd.get_shape(flat_x)  # -> JaggedShape([15])

Args:
  x: a DataSlice.
  from_dim: start of dimensions to flatten. Defaults to `0` if unspecified.
  to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.
```

### `float32(x)` {#float32}

``` {.no-copy}
Returns kd.slice(x, kd.FLOAT32).
```

### `float64(x)` {#float64}

``` {.no-copy}
Returns kd.slice(x, kd.FLOAT64).
```

### `floor(x)` {#floor}

``` {.no-copy}
Computes pointwise floor of the input, e.g.

rounding down: returns the largest integer value that is not greater than the
input.
```

### `floordiv(x, y)` {#floordiv}

``` {.no-copy}
Computes pointwise x // y.
```

### `follow(x)` {#follow}

``` {.no-copy}
Returns the original DataSlice from a NoFollow DataSlice.

When a DataSlice is wrapped into a NoFollow DataSlice, it's attributes
are not further traversed during extract, clone, deep_clone, etc.
`kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

Inverse of `nofollow`.

Args:
  x: DataSlice to unwrap, if nofollowed.
```

### `freeze(x)` {#freeze}

``` {.no-copy}
Returns a frozen version of `x`.
```

### `from_proto(messages, /, *, extensions, itemid, schema, db)` {#from_proto}

``` {.no-copy}
Returns a DataSlice representing proto data.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures: objects/entities, primitives, lists, and dicts,
  respectively. Enums are converted to INT32. The attribute names on the Koda
  objects match the field names in the proto definition. See below for methods
  to convert proto extensions to attributes alongside regular fields.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures. Enums are converted to ints.

  Only present values in `messages` are added. Default and missing values are
  not used.

  Proto extensions are ignored by default unless `extensions` is specified (or
  if an explicit entity schema with parenthesized attrs is used).
  The format of each extension specified in `extensions` is a dot-separated
  sequence of field names and/or extension names, where extension names are
  fully-qualified extension paths surrounded by parentheses. This sequence of
  fields and extensions is traversed during conversion, in addition to the
  default behavior of traversing all fields. For example:

    "path.to.field.(package_name.some_extension)"
    "path.to.repeated_field.(package_name.some_extension)"
    "path.to.map_field.values.(package_name.some_extension)"
    "path.(package_name.some_extension).(package_name2.nested_extension)"

  Extensions are looked up using the C++ generated descriptor pool, using
  `DescriptorPool::FindExtensionByName`, which requires that all extensions are
  compiled in as C++ protos. The Koda attribute names for the extension fields
  are parenthesized fully-qualified extension paths (e.g.
  "(package_name.some_extension)" or
  "(package_name.SomeMessage.some_extension)".) As the names contain '()' and
  '.' characters, they cannot be directly accessed using '.name' syntax but can
  be accessed using `.get_attr(name)'. For example,

    ds.get_attr('(package_name.AbcExtension.abc_extension)')
    ds.optional_field.get_attr('(package_name.DefExtension.def_extension)')

  If `messages` is a single proto Message, the result is a DataItem. If it is a
  list of proto Messages, the result is an 1D DataSlice.

  Args:
    messages: Message or list of Message of the same type.
    extensions: List of proto extension paths.
    itemid: The ItemId(s) to use for the root object(s). If not specified, will
      allocate new id(s). If specified, will also infer the ItemIds for all
      child items such as List items from this id, so that repeated calls to
      this method on the same input will produce the same id(s) for everything.
      Use this with care to avoid unexpected collisions.
    schema: The schema to use for the return value. Can be set to kd.OBJECT to
      (recursively) create an object schema. Can be set to None (default) to
      create an uuschema based on the proto descriptor. When set to an entity
      schema, some fields may be set to kd.OBJECT to create objects from that
      point.
    db: The DataBag to use for the result, or None to use a new DataBag.

  Returns:
    A DataSlice representing the proto data.
```

### `from_py(py_obj, *, dict_as_obj, itemid, schema, from_dim)` {#from_py}

``` {.no-copy}
Converts Python object into DataSlice.

  Can convert nested lists/dicts into Koda objects recursively as well.

  Args:
    py_obj: Python object to convert.
    dict_as_obj: If True, will convert dicts with string keys into Koda objects
      instead of Koda dicts.
    itemid: The ItemId to use for the root object. If not specified, will
      allocate a new id. If specified, will also infer the ItemIds for all child
      items such as list items from this id, so that repeated calls to this
      method on the same input will produce the same id for everything. Use this
      with care to avoid unexpected collisions.
    schema: The schema to use for the return value. When this schema or one of
      its attributes is OBJECT (which is also the default), recursively creates
      objects from that point on.
    from_dim: The dimension to start creating Koda objects/lists/dicts from.
      `py_obj` must be a nested list of at least from_dim depth, and the outer
      from_dim dimensions will become the returned DataSlice dimensions. When
      from_dim is 0, the return value is therefore a DataItem.

  Returns:
    A DataItem with the converted data.
```

### `from_pytree(py_obj, *, dict_as_obj, itemid, schema, from_dim)` {#from_pytree}

``` {.no-copy}
Converts Python object into DataSlice.

  Can convert nested lists/dicts into Koda objects recursively as well.

  Args:
    py_obj: Python object to convert.
    dict_as_obj: If True, will convert dicts with string keys into Koda objects
      instead of Koda dicts.
    itemid: The ItemId to use for the root object. If not specified, will
      allocate a new id. If specified, will also infer the ItemIds for all child
      items such as list items from this id, so that repeated calls to this
      method on the same input will produce the same id for everything. Use this
      with care to avoid unexpected collisions.
    schema: The schema to use for the return value. When this schema or one of
      its attributes is OBJECT (which is also the default), recursively creates
      objects from that point on.
    from_dim: The dimension to start creating Koda objects/lists/dicts from.
      `py_obj` must be a nested list of at least from_dim depth, and the outer
      from_dim dimensions will become the returned DataSlice dimensions. When
      from_dim is 0, the return value is therefore a DataItem.

  Returns:
    A DataItem with the converted data.
```

### `fstr` {#fstr}

``` {.no-copy}
Evaluates Koladata f-string into DataSlice.

  f-string must be created via Python f-string syntax. It must contain at least
  one formatted DataSlice.
  Each DataSlice must have custom format specification,
  e.g. `{ds:s}` or `{ds:.2f}`.
  Find more about format specification in kd.strings.format docs.

  NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

  Examples:
    countries = kd.slice(['USA', 'Schweiz'])
    kd.fstr(f'Hello, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Hello, Schweiz!'])

    greetings = kd.slice(['Hello', 'Gruezi'])
    kd.fstr(f'{greetings:s}, {countries:s}!')
      # -> kd.slice(['Hello, USA!', 'Gruezi, Schweiz!'])

    states = kd.slice([['California', 'Arizona', 'Nevada'], ['Zurich', 'Bern']])
    kd.fstr(f'{greetings:s}, {states:s} in {countries:s}!')
      # -> kd.slice([
               ['Hello, California in USA!',
                'Hello, Arizona in USA!',
                'Hello, Nevada in USA!'],
               ['Gruezi, Zurich in Schweiz!',
                'Gruezi, Bern in Schweiz!']]),

    prices = kd.slice([35.5, 49.2])
    currencies = kd.slice(['USD', 'CHF'])
    kd.fstr(f'Lunch price in {countries:s} is {prices:.2f} {currencies:s}.')
      # -> kd.slice(['Lunch price in USA is 35.50 USD.',
                     'Lunch price in Schweiz is 49.20 CHF.'])

  Args:
    s: f-string to evaluate.
  Returns:
    DataSlice with evaluated f-string.
```

### `full_equal(x, y)` {#full_equal}

``` {.no-copy}
Returns present iff all present items in `x` and `y` are equal.

The result is a zero-dimensional DataItem. Note that it is different from
`kd.all(x == y)`.

For example,
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, 3])) -> kd.present
  kd.full_equal(kd.slice([1, 2, 3]), kd.slice([1, 2, None])) -> kd.missing
  kd.full_equal(kd.slice([1, 2, None]), kd.slice([1, 2, None])) -> kd.present

Args:
  x: DataSlice.
  y: DataSlice.
```

### `get_attr(obj, attr_name, default)` {#get_attr}

``` {.no-copy}
Resolves (ObjectId(s), attr_name) => (Value|ObjectId)s.

In case attr points to Lists or Maps, the result is a DataSlice that
contains "pointers" to the beginning of lists/dicts.

For simple values ((obj, attr) => values), just returns
DataSlice(primitive values)

Args:
  obj: DataSlice | DataItem of object ids.
  attr_name: name of the attribute to access.
  default: DataSlice | DataItem value that should be used for objects that do
    not have this attribute. In case default is specified, this will not
    warn/raise if the attribute does not exist in the schema, so one can use
    default=None to suppress the missing attribute warning/error. When
    default=None and the attribute is missing on all objects, this will return
    an empty slices with OBJECT schema.

Returns:
  DataSlice
```

### `get_bag(ds)` {#get_bag}

``` {.no-copy}
Returns the attached DataBag.

It raises an Error if there is no DataBag attached.

Args:
  ds: DataSlice to get DataBag from.

Returns:
  The attached DataBag.
```

### `get_dtype(ds)` {#get_dtype}

``` {.no-copy}
Returns a primitive schema representing the underlying items' dtype.

If `ds` has a primitive schema, this returns that primitive schema, even if
all items in `ds` are missing. If `ds` has an OBJECT/ANY schema but contains
primitive values of a single dtype, it returns the schema for that primitive
dtype.

In case of items in `ds` have non-primitive types or mixed dtypes, returns
a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

Examples:
  kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
  kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.ANY)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
  kd.get_primitive_schema(kd.obj())) -> missing schema

Args:
  ds: DataSlice to get dtype from.

Returns:
  a primitive schema DataSlice.
```

### `get_item(x, key)` {#get_item}

``` {.no-copy}
Get items from from `x` by `key`.

`x` must be a DataSlice of Dicts or Lists, or DataBag. If `x` is a DataSlice,
`key` is used as a slice or index. If `x` is a DataBag, `key` is used as a
view into the DataBag (equivalent to `kde.with_bag(key, x))`).

Examples:
l = kd.list([1, 2, 3])
# Get List items by range slice from 1 to -1
kde.get_item(l, slice(1, -1)) -> kd.slice([2, 3])
# Get List items by indices
kde.get_item(l, kd.slice([2, 5])) -> kd.slice([3, None])

d = kd.dict({'a': 1, 'b': 2})
# Get Dict values by keys
kde.get_item(d, kd.slice(['a', 'c'])) -> kd.slice([1, None])

# db lookup.
kde.get_item(l.get_bag(), l.ref()) -> l.

Args:
  x: List or Dict DataSlice, or DataBag.
  key: DataSlice or Slice.

Returns:
  Result DataSlice.
```

### `get_item_schema(list_schema)` {#get_item_schema}

``` {.no-copy}
Returns the item schema of a List schema`.
```

### `get_itemid(x)` {#get_itemid}

``` {.no-copy}
Casts `x` to ITEMID using explicit (permissive) casting rules.
```

### `get_key_schema(dict_schema)` {#get_key_schema}

``` {.no-copy}
Returns the key schema of a Dict schema`.
```

### `get_keys(dict_ds)` {#get_keys}

``` {.no-copy}
Returns keys of all Dicts in `dict_ds`.

The result DataSlice has one more dimension used to represent keys in each
dict than `dict_ds`. While the order of keys within a dict is arbitrary, it is
the same as get_values().

Args:
  dict_ds: DataSlice of Dicts.

Returns:
  A DataSlice of keys.
```

### `get_ndim(x)` {#get_ndim}

``` {.no-copy}
Returns the number of dimensions of DataSlice `x`.
```

### `get_nofollowed_schema(schema)` {#get_nofollowed_schema}

``` {.no-copy}
Returns the original schema from nofollow schema.

Requires `nofollow_schema` to be a nofollow schema, i.e. that it wraps some
other schema.

Args:
  schema: nofollow schema DataSlice.
```

### `get_obj_schema(x)` {#get_obj_schema}

``` {.no-copy}
Returns a DataSlice of schemas for Objects and primitives in `x`.

DataSlice `x` must have OBJECT schema.

Examples:
  db = kd.bag()
  s = db.new_schema(a=kd.INT32)
  obj = s(a=1).embed_schema()
  kd.get_obj_schema(kd.slice([1, None, 2.0, obj]))
    -> kd.slice([kd.INT32, NONE, kd.FLOAT32, s])

Args:
  x: OBJECT DataSlice

Returns:
  A DataSlice of schemas.
```

### `get_primitive_schema(ds)` {#get_primitive_schema}

``` {.no-copy}
Returns a primitive schema representing the underlying items' dtype.

If `ds` has a primitive schema, this returns that primitive schema, even if
all items in `ds` are missing. If `ds` has an OBJECT/ANY schema but contains
primitive values of a single dtype, it returns the schema for that primitive
dtype.

In case of items in `ds` have non-primitive types or mixed dtypes, returns
a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

Examples:
  kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
  kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 2, 3], kd.ANY)) -> kd.INT32
  kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
  kd.get_primitive_schema(kd.obj())) -> missing schema

Args:
  ds: DataSlice to get dtype from.

Returns:
  a primitive schema DataSlice.
```

### `get_schema(x)` {#get_schema}

``` {.no-copy}
Returns the schema of `x`.
```

### `get_shape(x)` {#get_shape}

``` {.no-copy}
Returns the shape of `x`.
```

### `get_value_schema(dict_schema)` {#get_value_schema}

``` {.no-copy}
Returns the value schema of a Dict schema`.
```

### `get_values(dict_ds, key_ds)` {#get_values}

``` {.no-copy}
Returns values corresponding to `key_ds` for dicts in `dict_ds`.

When `key_ds` is specified, it is equivalent to dict_ds[key_ds].

When `key_ds` is unspecified, it returns all values in `dict_ds`. The result
DataSlice has one more dimension used to represent values in each dict than
`dict_ds`. While the order of values within a dict is arbitrary, it is the
same as get_keys().

Args:
  dict_ds: DataSlice of Dicts.
  key_ds: DataSlice of keys or unspecified.

Returns:
  A DataSlice of values.
```

### `greater(x, y)` {#greater}

``` {.no-copy}
Returns present iff `x` is greater than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than `y`. Returns `kd.present` when `x` is greater and
`kd.missing` when `x` is less than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `greater_equal(x, y)` {#greater_equal}

``` {.no-copy}
Returns present iff `x` is greater than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is greater than or equal to `y`. Returns `kd.present` when `x` is
greater than or equal to `y` and `kd.missing` when `x` is less than `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `group_by(x, *args)` {#group_by}

``` {.no-copy}
Returns permutation of `x` with injected grouped_by dimension.

The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions corresponds to the groups
and the items within the groups.

Values of the result is a permutation of `x`. `args` are used for the grouping
keys. If length of `args` is greater than 1, the key is a tuple.
If `args` is empty, the key is `x`.

Groups are ordered by the appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[1, 1, 1], [3, 3, 3], [2, 2]])

Example 2:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
  result: kd.slice([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])

Example 3:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  result: kd.slice([[1, 1, 1], [3, 3], [2]])

  Missing values are not listed in the result.

Example 4:
  x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  result: kd.slice([[1, 7], [2, 5], [3, 6, 8], [4]])

  When *args is present, `x` is not used for the key.

Example 5:
  x: kd.slice([1, 2, 3, 4, None, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4,    0, 7, None]),
  result: kd.slice([[1, 7], [2, None], [3, 6], [4]])

  Items with missing key is not listed in the result.
  Missing `x` values are missing in the result.

Example 6:
  x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  z: kd.slice([A, D, B, A, D, C, A, B]),
  result: kd.slice([[1, 7], [2, 5], [3], [4], [6]])

  When *args has two or more values, the  key is a tuple.
  In this example we have the following groups:
  (7, A), (4, D), (0, B), (9, A), (0, C)

Args:
  x: DataSlice to group.
  *args: DataSlices keys to group by. All data slices must have the same shape
    as x. Scalar DataSlices are not supported. If not present, `x` is used as
    the key.

Returns:
  DataSlice with the same shape and schema as `x` with injected grouped
  by dimension.
```

### `group_by_indices(*args)` {#group_by_indices}

``` {.no-copy}
Returns a indices DataSlice with injected grouped_by dimension.

The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
dimensions are unchanged. The last two dimensions corresponds to the groups
and the items within the groups.

Values of the data slice are the indices of the objects within the parent
dimension. `kde.take(x, kde.group_by_indices(x))` would group the objects in
`x` by their values.

Groups are ordered by the appearance of the first object in the group.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[0, 3, 6], [1, 5, 7], [2, 4]])

  We have three groups in order: 1, 3, 2. Each sublist contains the indices of
  the objects in the original DataSlice.

Example 2:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
  result: kd.slice([[[0, 2, 4], [1], [3, 5]], [[0, 2], [1]]])

  We have three groups in the first sublist in order: 1, 2, 3 and two groups
  in the second sublist in order: 1, 3.
  Each sublist contains the indices of the objects in the original sublist.

Example 3:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  result: kd.slice([[0, 3, 6], [1, 5], [2]])

  Missing values are not listed in the result.

Example 4:
  x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
  y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
  result: kd.slice([[0, 6], [1, 4], [2, 5, 7], [3]])

  With several arguments keys is a tuple.
  In this example we have the following groups: (1, 7), (2, 4), (3, 0), (1, 9)

Args:
  *args: DataSlices keys to group by. All data slices must have the same
    shape. Scalar DataSlices are not supported.

Returns:
  INT64 DataSlice with indices and injected grouped_by dimension.
```

### `group_by_indices_sorted(*args)` {#group_by_indices_sorted}

``` {.no-copy}
Similar to `group_by_indices` but groups are sorted by the value.

Each argument must contain the values of one type.

Mixed types are not supported.
ExprQuote and DType are not supported.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  result: kd.slice([[0, 3, 6], [2, 4], [1, 5, 7]])

  We have three groups in order: 1, 2, 3. Each sublist contains the indices of
  the objects in the original DataSlice.

Example 2:
  x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
  y: kd.slice([9, 4, 0, 3, 4, 0, 9, 0]),
  result: kd.slice([[3], [0, 6], [1, 4], [2, 5, 7]])

  With several arguments keys is a tuple.
  In this example we have the following groups: (1, 3), (1, 9), (2, 4), (3, 0)

Args:
  *args: DataSlices keys to group by. All data slices must have the same
    shape. Scalar DataSlices are not supported.

Returns:
  INT64 DataSlice with indices and injected grouped_by dimension.
```

### `has(x)` {#has}

``` {.no-copy}
Returns presence of `x`.

Pointwise operator which take a DataSlice and return a MASK indicating the
presence of each item in `x`. Returns `kd.present` for present items and
`kd.missing` for missing items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the presence of `x`.
```

### `has_attr(obj, attr_name)` {#has_attr}

``` {.no-copy}
Indicates whether the items in the slice have the given attribute.

This function checks for attributes based on data rather than "schema" and may
be slow in some cases.

Args:
  obj: DataSlice | DataItem instance
  attr_name: Name of the attribute to check.

Returns:
  A MASK slice with the same shape as `obj` that contains present if the
  attribute exists for the corresponding item.
```

### `has_not(x)` {#has_not}

``` {.no-copy}
Returns present iff `x` is missing element-wise.

Pointwise operator which take a DataSlice and return a MASK indicating
iff `x` is missing element-wise. Returns `kd.present` for missing
items and `kd.missing` for present items.

Args:
  x: DataSlice.

Returns:
  DataSlice representing the non-presence of `x`.
```

### `implode(x, ndim, db)` {#implode}

``` {.no-copy}
Implodes a Dataslice `x` a specified number of times.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  A single list implosion is equivalent to `kd.list(x, db)`.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  The specified `db` is used to create any new Lists, and is the DataBag of the
  result DataSlice. If `db` is not specified, a new DataBag is created for this
  purpose.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    db: optional DataBag where Lists are created from

  Returns:
    DataSlice of nested Lists
```

### `index(x, dim)` {#index}

``` {.no-copy}
Returns the indices of the elements computed over the last dim dimensions.

The resulting slice has the same shape as the input.

Example:
  ds = kd.slice([
      [
          ['a', None, 'c'],
          ['d', 'e']
      ],
      [
          [None, 'g'],
          ['h', 'i', 'j']
      ]
  ])
  kd.index(ds, dim=0)
    # -> kd.slice([[[0, None, 0], [0, 0]], [[None, 1], [1, 1, 1]]])
  kd.index(ds, dim=1)
    # -> kd.slice([[[0, None, 0], [1, 1]], [[None, 0], [1, 1, 1]]])
  kd.index(ds, dim=2)
    # -> kd.slice([[[0, None, 2], [0, 1]], [[None, 1], [0, 1, 2]]])

  kd.index(ds) -> kd.index(ds, dim=ds.get_ndim() - 1)

Args:
  x: A DataSlice.
  dim: The dimension to compute indices over. Requires 0 <= dim < get_ndim(x).
    If unspecified, it is set to the last dimension of x.
```

### `int32(x)` {#int32}

``` {.no-copy}
Returns kd.slice(x, kd.INT32).
```

### `int64(x)` {#int64}

``` {.no-copy}
Returns kd.slice(x, kd.INT64).
```

### `inverse_mapping(x, ndim)` {#inverse_mapping}

``` {.no-copy}
Returns inverse permutations of indices over the last `ndim` dimension.

It interprets `indices` over the last `ndim` dimension as a permutation and
substitute with the corresponding inverse permutation. `ndim` is set to 1 by
default if unspecified. It fails when `indices` is not a valid permutation.

Example:
  indices = kd.slice([[1, 2, 0], [1, None]])
  kd.inverse_mapping(indices)  ->  kd.slice([[2, 0, 1], [None, 0]])

  Explanation:
    indices      = [[1, 2, 0], [1, None]]
    inverse_permutation[1, 2, 0] = [2, 0, 1]
    inverse_permutation[1, None] = [None, 0]

  kd.inverse_mapping(indices, ndim=1) -> raise

  indices = kd.slice([[1, 2, 0], [3, None]])
  kd.inverse_mapping(indices, ndim=2)  ->  kd.slice([[2, 0, 1], [3, None]])

Args:
  x: A DataSlice of indices.
  ndim: The number of dimensions to compute inverse permutations over.
    Requires 0 <= ndim <= get_ndim(x).

Returns:
  An inverse permutation of indices.
```

### `inverse_select(ds, fltr)` {#inverse_select}

``` {.no-copy}
Creates a DataSlice by putting items in ds to present positions in fltr.

The shape of `ds` and the shape of `fltr` must have the same rank and the same
first N-1 dimensions. That is, only the last dimension can be different. The
shape of `ds` must be the same as the shape of the DataSlice after applying
`fltr` using kd.select. That is,
ds.get_shape() == kd.select(fltr, fltr).get_shape().

Example:
  ds = kd.slice([[1, None], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> [[None, 1, None], [2, None]]

  ds = kd.slice([1, None, 2])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different ranks

  ds = kd.slice([[1, None, 2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different N-1 dimensions

  ds = kd.slice([[1], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to incompatible shapes

Note, in most cases, kd.inverse_select is not a strict reverse operation of
kd.select as kd.select operation is lossy and does not require `ds` and `fltr`
to have the same rank. That is,
kd.inverse_select(kd.select(ds, fltr), fltr) != ds.

The most common use case of kd.inverse_select is to restore the shape of the
original DataSlice after applying kd.select and performing some operations on
the subset of items in the original DataSlice. E.g.
  filtered_ds = kd.select(ds, fltr)
  # do something on filtered_ds
  ds = kd.inverse_select(filtered_ds, fltr) | ds

Args:
  ds: DataSlice to be reverse filtered
  fltr: filter DataSlice with dtype as kd.MASK.

Returns:
  Reverse filtered DataSlice.
```

### `is_dict(ds)` {#is_dict}

``` {.no-copy}
Returns true if all present items in ds are dicts.
```

### `is_empty(obj)` {#is_empty}

``` {.no-copy}
Returns kd.present if all items in the DataSlice are missing.
```

### `is_expandable_to(x, target, ndim)` {#is_expandable_to}

``` {.no-copy}
Returns true if `x` is expandable to `target`.

Args:
  x: DataSlice to expand.
  target: target DataSlice.
  ndim: the number of dimensions to implode before expansion.

See `expand_to` for a detailed description of expansion.
```

### `is_expr(obj)` {#is_expr}

``` {.no-copy}
Returns kd.present if the given object is an Expr and kd.missing otherwise.
```

### `is_fn(obj)` {#is_fn}

``` {.no-copy}
Checks if `obj` represents a functor.

  Args:
    obj: The value to check.

  Returns:
    kd.present if `obj` is a DataSlice representing a functor, kd.missing
    otherwise (for example if fn has wrong type).
```

### `is_item(obj)` {#is_item}

``` {.no-copy}
Returns kd.present if the given object is a scalar DataItem and kd.missing otherwise.
```

### `is_list(ds)` {#is_list}

``` {.no-copy}
Returns true if all present items in ds are lists.
```

### `is_primitive(x)` {#is_primitive}

``` {.no-copy}
Returns whether x is a primitive DataSlice.
```

### `is_shape_compatible(x, y)` {#is_shape_compatible}

``` {.no-copy}
Returns present if the shapes of `x` and `y` are compatible.

Two DataSlices have compatible shapes if dimensions of one DataSlice equal or
are prefix of dimensions of another DataSlice.

Args:
  x: DataSlice to check.
  y: DataSlice to check.

Returns:
  A MASK DataItem indicating whether 'x' and 'y' are compatible.
```

### `is_slice(obj)` {#is_slice}

``` {.no-copy}
Returns kd.present if the given object is a DataSlice and kd.missing otherwise.
```

### `isin(x, y)` {#isin}

``` {.no-copy}
Returns a DataItem indicating whether DataItem x is present in y.
```

### `item` {#item}

``` {.no-copy}
Creates a DataSlice from `value`.
If `schema` is set, that schema is used,
otherwise the schema is inferred from `value`.
```

### `less(x, y)` {#less}

``` {.no-copy}
Returns present iff `x` is less than `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than `y`. Returns `kd.present` when `x` is less and
`kd.missing` when `x` is greater than or equal to `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `less_equal(x, y)` {#less_equal}

``` {.no-copy}
Returns present iff `x` is less than or equal to `y`.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` is less than or equal to `y`. Returns `kd.present` when `x` is
less than or equal to `y` and `kd.missing` when `x` is greater than `y`.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `list(items, *, item_schema, schema, itemid, db)` {#list}

``` {.no-copy}
Creates list(s) by collapsing `items`.

  If there is no argument, returns an empty Koda List.
  If the argument is a DataSlice, creates a slice of Koda Lists.
  If the argument is a Python list, creates a nested Koda List.

  Examples:
  list() -> a single empty Koda List
  list([1, 2, 3]) -> Koda List with items 1, 2, 3
  list(kd.slice([1, 2, 3])) -> (same as above) Koda List with items 1, 2, 3
  list([[1, 2, 3], [4, 5]]) -> nested Koda List [[1, 2, 3], [4, 5]]
  list(kd.slice([[1, 2, 3], [4, 5]]))
    -> 1-D DataSlice with 2 lists [1, 2, 3], [4, 5]

  Args:
    items: The items to use. If not specified, an empty list of OBJECTs will be
      created.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where list(s) are created.

  Returns:
    The slice with list/lists.
```

### `list_like(shape_and_mask_from, items, *, item_schema, schema, itemid, db)` {#list_like}

``` {.no-copy}
Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired lists.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `list_schema(item_schema, db)` {#list_schema}

``` {.no-copy}
Creates a list schema in the given DataBag.

  Args:
    item_schema: schema of the items in the list.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.

  Returns:
    data_slice.DataSlice representing a list schema.
```

### `list_shaped(shape, items, *, item_schema, schema, itemid, db)` {#list_shaped}

``` {.no-copy}
Creates new Koda lists with the given shape.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `list_shaped_as(shape_from, items, *, item_schema, schema, itemid, db)` {#list_shaped_as}

``` {.no-copy}
Creates new Koda lists with shape of the given DataSlice.

  Args:
    shape_from: mandatory DataSlice, whose shape the returned DataSlice will
      have.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.
    db: optional DataBag where lists are created.

  Returns:
    A DataSlice with the lists.
```

### `list_size(list_slice)` {#list_size}

``` {.no-copy}
Returns size of a List.
```

### `loads(x)` {#loads}

``` {.no-copy}
Deserializes a DataSlice or a DataBag.
```

### `log(x)` {#log}

``` {.no-copy}
Computes pointwise natural logarithm of the input.
```

### `log10(x)` {#log10}

``` {.no-copy}
Computes pointwise logarithm in base 10 of the input.
```

### `make_tuple(*args)` {#make_tuple}

``` {.no-copy}
Returns a tuple-like object containing the given `*args`.
```

### `map_py(fn, *args, schema, max_threads, ndim, item_completed_callback, **kwargs)` {#map_py}

``` {.no-copy}
Apply the python function `fn` on provided `args` and `kwargs`.

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
    return db.obj(x=1, y=2) if x.z > 3 else db.obj(x=2, y=1)

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


Args:
  fn: Function.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  ndim: Dimensionality of items to pass to `fn`.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called `map_py`
    in case `max_threads` is greater than 1, as we rely on this property for
    cases like progress reporting. As such, it can not be attached to the `fn`
    itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `map_py_on_cond(true_fn, false_fn, cond, *args, schema, max_threads, item_completed_callback, **kwargs)` {#map_py_on_cond}

``` {.no-copy}
Apply python functions on `args` and `kwargs` based on `cond`.

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
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_cond` in case `max_threads` is greater than 1, as we rely on
    this property for cases like progress reporting. As such, it can not be
    attached to the `true_fn` and `false_fn` themselves.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `map_py_on_present(fn, *args, schema, max_threads, item_completed_callback, **kwargs)` {#map_py_on_present}

``` {.no-copy}
Apply python function `fn` to items present in all `args` and `kwargs`.

Also see kd.map_py().

Args:
  fn: function.
  *args: Input DataSlices.
  schema: The schema to use for resulting DataSlice.
  max_threads: maximum number of threads to use.
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_present` in case `max_threads` is greater than 1, as we rely on
    this property for cases like progress reporting. As such, it can not be
    attached to the `fn` itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `map_py_on_selected(fn, cond, *args, schema, max_threads, item_completed_callback, **kwargs)` {#map_py_on_selected}

``` {.no-copy}
Apply python function `fn` on `args` and `kwargs` based on `cond`.

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
  item_completed_callback: A callback that will be called after each item is
    processed. It will be called in the original thread that called
    `map_py_on_selected` in case `max_threads` is greater than 1, as we rely
    on this property for cases like progress reporting. As such, it can not be
    attached to the `fn` itself.
  **kwargs: Input DataSlices.

Returns:
  Result DataSlice.
```

### `mask(x)` {#mask}

``` {.no-copy}
Returns kd.slice(x, kd.MASK).
```

### `mask_and(x, y)` {#mask_and}

``` {.no-copy}
Applies pointwise MASK_AND operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_AND operation is defined as:
  kd.mask_and(kd.present, kd.present) -> kd.present
  kd.mask_and(kd.present, kd.missing) -> kd.missing
  kd.mask_and(kd.missing, kd.present) -> kd.missing
  kd.mask_and(kd.missing, kd.missing) -> kd.missing

It is equivalent to `x & y`.

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `mask_equal(x, y)` {#mask_equal}

``` {.no-copy}
Applies pointwise MASK_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_EQUAL operation is defined as:
  kd.mask_equal(kd.present, kd.present) -> kd.present
  kd.mask_equal(kd.present, kd.missing) -> kd.missing
  kd.mask_equal(kd.missing, kd.present) -> kd.missing
  kd.mask_equal(kd.missing, kd.missing) -> kd.present

Note that this is different from `x == y`. For example,
  kd.missing == kd.missing -> kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `mask_not_equal(x, y)` {#mask_not_equal}

``` {.no-copy}
Applies pointwise MASK_NOT_EQUAL operation on `x` and `y`.

Both `x` and `y` must have MASK dtype. MASK_NOT_EQUAL operation is defined as:
  kd.mask_not_equal(kd.present, kd.present) -> kd.missing
  kd.mask_not_equal(kd.present, kd.missing) -> kd.present
  kd.mask_not_equal(kd.missing, kd.present) -> kd.present
  kd.mask_not_equal(kd.missing, kd.missing) -> kd.missing

Note that this is different from `x != y`. For example,
  kd.present != kd.missing -> kd.missing
  kd.missing != kd.present -> kd.missing

Args:
  x: DataSlice.
  y: DataSlice.

Returns:
  DataSlice.
```

### `max(x)` {#max}

``` {.no-copy}
Returns the maximum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `maximum(x, y)` {#maximum}

``` {.no-copy}
Computes pointwise max(x, y).
```

### `maybe(obj, attr_name)` {#maybe}

``` {.no-copy}
A shortcut for kde.get_attr(obj, attr_name, default=None).
```

### `mean(x)` {#mean}

``` {.no-copy}
Returns the mean of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `median(x)` {#median}

``` {.no-copy}
Returns the median of elements over all dimensions.

The result is a zero-dimensional DataItem.

Please note that for even number of elements, the median is the next value
down from the middle, p.ex.: median([1, 2]) == 1.
That is made by design to fulfill the following property:
1. type of median(x) == type of elements of x;
2. median(x) ∈ x.

Args:
  x: A DataSlice of numbers.
```

### `min(x)` {#min}

``` {.no-copy}
Returns the minimum of items over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `minimum(x, y)` {#minimum}

``` {.no-copy}
Computes pointwise min(x, y).
```

### `mod(x, y)` {#mod}

``` {.no-copy}
Computes pointwise x % y.
```

### `multiply(x, y)` {#multiply}

``` {.no-copy}
Computes pointwise x * y.
```

### `mutable_obj(arg, *, itemid, db, **attrs)` {#mutable_obj}

``` {.no-copy}
Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema.

  Args:
    arg: optional Python object to be converted to an Object.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
      itemid will only be set when the args is not a primitive or primitive
      slice if args presents.
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `mutable_obj_like(shape_and_mask_from, *, itemid, db, **attrs)` {#mutable_obj_like}

``` {.no-copy}
Creates Objects with shape and sparsity from shape_and_mask_from.

  Returned DataSlice has OBJECT schema.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `mutable_obj_shaped(shape, *, itemid, db, **attrs)` {#mutable_obj_shaped}

``` {.no-copy}
Creates Objects with the given shape.

  Returned DataSlice has OBJECT schema.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `neg(x)` {#neg}

``` {.no-copy}
Computes pointwise negation of the input, i.e. -x.
```

### `new(arg, *, schema, update_schema, itemid, db, **attrs)` {#new}

``` {.no-copy}
Creates Entities with given attrs.

  Args:
    arg: optional Python object to be converted to an Entity.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
      itemid will only be set when the args is not a primitive or primitive
      slice if args present.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `new_dictid()` {#new_dictid}

``` {.no-copy}
Allocates new Dict ItemId.
```

### `new_dictid_like(shape_and_mask_from)` {#new_dictid_like}

``` {.no-copy}
Allocates new Dict ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `new_dictid_shaped(shape)` {#new_dictid_shaped}

``` {.no-copy}
Allocates new Dict ItemIds of the given shape.
```

### `new_dictid_shaped_as(shape_from)` {#new_dictid_shaped_as}

``` {.no-copy}
Allocates new Dict ItemIds with the shape of shape_from.
```

### `new_itemid()` {#new_itemid}

``` {.no-copy}
Allocates new ItemId.
```

### `new_itemid_like(shape_and_mask_from)` {#new_itemid_like}

``` {.no-copy}
Allocates new ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `new_itemid_shaped(shape)` {#new_itemid_shaped}

``` {.no-copy}
Allocates new ItemIds of the given shape without any DataBag attached.
```

### `new_itemid_shaped_as(shape_from)` {#new_itemid_shaped_as}

``` {.no-copy}
Allocates new ItemIds with the shape of shape_from.
```

### `new_like(shape_and_mask_from, *, schema, update_schema, itemid, db, **attrs)` {#new_like}

``` {.no-copy}
Creates new Entities with the shape and sparsity from shape_and_mask_from.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `new_listid()` {#new_listid}

``` {.no-copy}
Allocates new List ItemId.
```

### `new_listid_like(shape_and_mask_from)` {#new_listid_like}

``` {.no-copy}
Allocates new List ItemIds with the shape and sparsity of shape_and_mask_from.
```

### `new_listid_shaped(shape)` {#new_listid_shaped}

``` {.no-copy}
Allocates new List ItemIds of the given shape.
```

### `new_listid_shaped_as(shape_from)` {#new_listid_shaped_as}

``` {.no-copy}
Allocates new List ItemIds with the shape of shape_from.
```

### `new_schema(db, **attrs)` {#new_schema}

``` {.no-copy}
Creates new schema in the given DataBag.

  Args:
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
```

### `new_shaped(shape, *, schema, update_schema, itemid, db, **attrs)` {#new_shaped}

``` {.no-copy}
Creates new Entities with the given shape.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `new_shaped_as(shape_from, *, schema, update_schema, itemid, db, **attrs)` {#new_shaped_as}

``` {.no-copy}
Creates new Koda entities with shape of the given DataSlice.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `no_bag(ds)` {#no_bag}

``` {.no-copy}
Returns DataSlice without any DataBag attached.
```

### `no_db(ds)` {#no_db}

``` {.no-copy}
Returns DataSlice without any DataBag attached.
```

### `nofollow(x)` {#nofollow}

``` {.no-copy}
Returns a nofollow DataSlice targeting the given slice.

When a slice is wrapped into a nofollow, it's attributes are not further
traversed during extract, clone, deep_clone, etc.

`nofollow` is reversible.

Args:
  x: DataSlice to wrap.
```

### `nofollow_schema(schema)` {#nofollow_schema}

``` {.no-copy}
Returns a NoFollow schema of the provided schema.

`nofollow_schema` is reversible with `get_actual_schema`.

`nofollow_schema` can only be called on implicit and explicit schemas and
OBJECT. It raises an Error if called on ANY, primitive schemas, ITEMID, etc.

Args:
  schema: Schema DataSlice to wrap.
```

### `not_equal(x, y)` {#not_equal}

``` {.no-copy}
Returns present iff `x` and `y` are not equal.

Pointwise operator which takes a DataSlice and returns a MASK indicating
iff `x` and `y` are not equal. Returns `kd.present` for not equal items and
`kd.missing` in other cases.

Args:
  x: DataSlice.
  y: DataSlice.
```

### `obj(arg, *, itemid, db, **attrs)` {#obj}

``` {.no-copy}
Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema.

  Args:
    arg: optional Python object to be converted to an Object.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
      itemid will only be set when the args is not a primitive or primitive
      slice if args presents.
    db: optional DataBag where object are created.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `obj_like(shape_and_mask_from, *, itemid, db, **attrs)` {#obj_like}

``` {.no-copy}
Creates Objects with shape and sparsity from shape_and_mask_from.

  Returned DataSlice has OBJECT schema.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `obj_shaped(shape, *, itemid, db, **attrs)` {#obj_shaped}

``` {.no-copy}
Creates Objects with the given shape.

  Returned DataSlice has OBJECT schema.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `obj_shaped_as(shape_from, *, itemid, db, **attrs)` {#obj_shaped_as}

``` {.no-copy}
Creates Objects with the shape of the given DataSlice.

  Returned DataSlice has OBJECT schema.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `ordinal_rank(x, tie_breaker, descending, ndim)` {#ordinal_rank}

``` {.no-copy}
Returns ordinal ranks of items in `x` over the last `ndim` dimensions.

Items are grouped over the last `ndim` dimensions and ranked within the group.
`ndim` is set to 1 by default if unspecified. Ranks are integers starting from
0, assigned to values in ascending order by default.

By ordinal ranking ("1 2 3 4" ranking), equal items receive distinct ranks.
Items are compared by the triple (value, tie_breaker, position) to resolve
ties. When descending=True, values are ranked in descending order but
tie_breaker and position are ranked in ascending order.

NaN values are ranked lowest regardless of the order of ranking. Ranks of
missing items are missing in the result. If `tie_breaker` is specified, it
cannot be more sparse than `x`.

Example:

  ds = kd.slice([[0, 3, None, 6], [5, None, 2, 1]])
  kd.ordinal_rank(x) -> kd.slice([[0, 1, None, 2], [2, None, 1, 0]])

  kd.ordinal_rank(x, descending=True) ->
      kd.slice([[2, 1, None, 0], [0, None, 1, 2]])

  kd.ordinal_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

  kd.ordinal_rank(x, ndim=2) -> kd.slice([[0, 3, None, 5], [4, None, 2, 1]])

Args:
  x: DataSlice to rank.
  tie_breaker: If specified, used to break ties. If `tie_breaker` does not
    fully resolve all ties, then the remaining ties are resolved by their
    positions in the DataSlice.
  descending: If true, items are compared in descending order. Does not affect
    the order of tie breaker and position in tie-breaking compairson.
  ndim: The number of dimensions to rank over.
    Requires 0 <= ndim <= get_ndim(x).

Returns:
  A DataSlice of ordinal ranks.
```

### `pos(x)` {#pos}

``` {.no-copy}
Computes pointwise positive of the input, i.e. +x.
```

### `pow(x, y)` {#pow}

``` {.no-copy}
Computes pointwise x ** y.
```

### `present_like(x)` {#present_like}

``` {.no-copy}
Creates a DataSlice of present masks with the shape and sparsity of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.present_like(x) -> kd.slice([[present], [present, None]])

Args:
  x: DataSlice to match the shape and sparsity of.

Returns:
  A DataSlice with the same shape and sparsity as `x`.
```

### `present_shaped(shape)` {#present_shaped}

``` {.no-copy}
Creates a DataSlice of present masks with the given shape.

Example:
  shape = kd.shapes.create_shape([2], [1, 2])
  kd.core.present_shaped(shape) -> kd.slice([[present], [present, present]])

Args:
  shape: shape to expand to.

Returns:
  A DataSlice with the same shape as `shape`.
```

### `present_shaped_as(x)` {#present_shaped_as}

``` {.no-copy}
Creates a DataSlice of present masks with the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.core.present_shaped_as(x) -> kd.slice([[present], [present, present]])

Args:
  x: DataSlice to match the shape of.

Returns:
  A DataSlice with the same shape as `x`.
```

### `randint_like(x, low, high, seed)` {#randint_like}

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the same sparsity as `x`.

Args:
  x: used to determine the shape and sparsity of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. Make sure to set this to new
    values to get distinct results.

Returns:
  A DataSlice of random numbers.
```

### `randint_shaped(shape, low, high, seed)` {#randint_shaped}

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the given shape.

Args:
  shape: used for the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. Make sure to set this to new
    values to get distinct results.

Returns:
  A DataSlice of random numbers.
```

### `randint_shaped_as(x, low, high, seed)` {#randint_shaped_as}

``` {.no-copy}
Returns a DataSlice of random INT64 numbers with the same shape as `x`.

Args:
  x: used to determine the shape of the resulting DataSlice.
  low: Lowest (signed) integers to be drawn (unless high=None, in which case
    this parameter is 0 and this value is used for high), inclusive.
  high: If provided, the largest integer to be drawn (see above behavior if
    high=None), exclusive.
  seed: Seed for the random number generator. Make sure to set this to new
    values to get distinct results.

Returns:
  A DataSlice of random numbers.
```

### `range(start, end)` {#range}

``` {.no-copy}
Returns a DataSlice of INT64s with range [start, end).

`start` and `end` must be broadcastable to the same shape. The resulting
DataSlice has one more dimension than the broadcasted shape.

When `end` is unspecified, `start` is used as `end` and 0 is used as `start`.
For example,

  kd.range(5) -> kd.slice([0, 1, 2, 3, 4])
  kd.range(2, 5) -> kd.slice([2, 3, 4])
  kd.range(5, 2) -> kd.slice([])  # empty range
  kd.range(kd.slice([2, 4])) -> kd.slice([[0, 1], [0, 1, 2, 3])
  kd.range(kd.slice([2, 4]), 6) -> kd.slice([[2, 3, 4, 5], [4, 5])

Args:
  start: A DataSlice for start (inclusive) of intervals (unless `end` is
    unspecified, in which case this parameter is used as `end`).
  end: A DataSlice for end (exclusive) of intervals.

Returns:
  A DataSlice of INT64s with range [start, end).
```

### `ref(ds)` {#ref}

``` {.no-copy}
Returns `ds` with the DataBag removed.

Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
allowed.

The result DataSlice still has the original schema. If the schema is an Entity
schema (including List/Dict schema), it is treated an ItemId after the DataBag
is removed.

Args:
  ds: DataSlice of ItemIds.
```

### `reify(ds, source)` {#reify}

``` {.no-copy}
Assigns a bag and schema from `source` to the slice `ds`.
```

### `remove(ds, fltr)` {#remove}

``` {.no-copy}
Creates a new DataSlice by filtering out present items in fltr.

The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
present items in `fltr` are removed. The last dimension of the resulting
DataSlice is changed while the first N-1 dimensions are the same as those in
`ds`.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.remove(val, val > 3) -> [[1, None], [None], [2]]

  fltr = kd.slice(
      [[None, None, kd.present], [kd.present], [kd.present, None]])
  kd.remove(val, fltr) -> [[1, None], [None], [8]]

Args:
  ds: DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK.

Returns:
  Filtered DataSlice.
```

### `repeat(x, sizes)` {#repeat}

``` {.no-copy}
Returns `x` with values repeated according to `sizes`.

The resulting slice has `rank = rank + 1`. The input `sizes` are broadcasted
to `x`, and each value is repeated the given number of times.

Example:
  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([[1, 2], [3]])
  kd.add_dim(ds, sizes)  # -> kd.slice([[[1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  sizes = kd.slice([2, 3])
  kd.add_dim(ds, sizes)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

  ds = kd.slice([[1, None], [3]])
  size = kd.item(2)
  kd.add_dim(ds, size)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3]]])

Args:
  x: A DataSlice of data.
  sizes: A DataSlice of sizes that each value in `x` should be repeated for.
```

### `reshape(x, shape)` {#reshape}

``` {.no-copy}
Returns a DataSlice with the provided shape.

Examples:
  x = kd.slice([1, 2, 3, 4])

  # Using a shape.
  kd.reshape(x, kd.shapes.create(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes.
  kd.reshape(x, kd.make_tuple(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of sizes and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

  # Using a tuple of slices and a placeholder dimension.
  kd.reshape(x, kd.make_tuple(-1, kd.slice([3, 1])))
      # -> kd.slice([[1, 2, 3], [4]])

  # Reshaping a scalar.
  kd.reshape(1, kd.make_tuple(1, 1))  # -> kd.slice([[1]])

  # Reshaping an empty slice.
  kd.reshape(kd.slice([]), kd.make_tuple(2, 0))  # -> kd.slice([[], []])

Args:
  x: a DataSlice.
  shape: a JaggedShape or a tuple of dimensions that forms a shape through
    `kd.shapes.create`, with additional support for a `-1` placeholder
    dimension.
```

### `reshape_as(x, y)` {#reshape_as}

``` {.no-copy}
Returns a DataSlice x reshaped to the shape of DataSlice y.
```

### `reverse(ds)` {#reverse}

``` {.no-copy}
Returns a DataSlice with items reversed on the last dimension.

Example:
  ds = kd.slice([[1, None], [2, 3, 4]])
  kd.reverse(ds) -> [[None, 1], [4, 3, 2]]

  ds = kd.slice([1, None, 2])
  kd.reverse(ds) -> [2, None, 1]

Args:
  ds: DataSlice to be reversed.

Returns:
  Reversed on the last dimension DataSlice.
```

### `reverse_select(ds, fltr)` {#reverse_select}

``` {.no-copy}
Creates a DataSlice by putting items in ds to present positions in fltr.

The shape of `ds` and the shape of `fltr` must have the same rank and the same
first N-1 dimensions. That is, only the last dimension can be different. The
shape of `ds` must be the same as the shape of the DataSlice after applying
`fltr` using kd.select. That is,
ds.get_shape() == kd.select(fltr, fltr).get_shape().

Example:
  ds = kd.slice([[1, None], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> [[None, 1, None], [2, None]]

  ds = kd.slice([1, None, 2])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different ranks

  ds = kd.slice([[1, None, 2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to different N-1 dimensions

  ds = kd.slice([[1], [2]])
  fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
  kd.inverse_select(ds, fltr) -> error due to incompatible shapes

Note, in most cases, kd.inverse_select is not a strict reverse operation of
kd.select as kd.select operation is lossy and does not require `ds` and `fltr`
to have the same rank. That is,
kd.inverse_select(kd.select(ds, fltr), fltr) != ds.

The most common use case of kd.inverse_select is to restore the shape of the
original DataSlice after applying kd.select and performing some operations on
the subset of items in the original DataSlice. E.g.
  filtered_ds = kd.select(ds, fltr)
  # do something on filtered_ds
  ds = kd.inverse_select(filtered_ds, fltr) | ds

Args:
  ds: DataSlice to be reverse filtered
  fltr: filter DataSlice with dtype as kd.MASK.

Returns:
  Reverse filtered DataSlice.
```

### `round(x)` {#round}

``` {.no-copy}
Computes pointwise rounding of the input.

Please note that this is NOT bankers rounding, unlike Python built-in or
Tensorflow round(). If the first decimal is exactly  0.5, the result is
rounded to the number with a higher absolute value:
round(1.4) == 1.0
round(1.5) == 2.0
round(1.6) == 2.0
round(2.5) == 3.0 # not 2.0
round(-1.4) == -1.0
round(-1.5) == -2.0
round(-1.6) == -2.0
round(-2.5) == -3.0 # not -2.0
```

### `sample(x, ratio, seed, key)` {#sample}

``` {.no-copy}
Randomly sample items in `x` based on ratio.

The sampling is performed on flatten `x` rather than on the last dimension.

All items including missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set and
items corresponding to empty keys are never sampled. Otherwise, the indices of
`x` is used.

Note that the sampling is performed as follows:
  hash(key, seed) < ratio * 2^63
Therefore, exact sampled count is not guaranteed. E,g, result of sampling an
array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
rather than exact 100 items. However this provides per-item stability that
the sampling result for an item is deterministic given the same key regardless
other keys are provided.

Examples:
  # Select 50% from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample(ds, 0.5, 123) -> kd.slice([[None, 4], [None, 8]])

  # Use 'key' for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample(ds_1, 0.5, 123, key_1) -> kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample(ds_2, 0.5, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  ratio: float number between [0, 1].
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.
```

### `sample_n(x, n, seed, key)` {#sample_n}

``` {.no-copy}
Randomly sample n items in `x` from the last dimension.

The sampling is performed over the last dimension rather than on flatten `x`.

`n` can either can be a scalar integer or DataSlice. If it is a DataSlice, it
must have compatible shape with `x.get_shape()[:-1]`. All items including
missing items in `x` are eligible for sampling.

The sampling is stable given the same inputs. Optional `key` can be used to
provide additional stability. That is, `key` is used for sampling if set.
Otherwise, the indices of `x` are used.

Examples:
  # Select 2 items from last dimension.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, 2, 123) -> kd.slice([[2, 4], [None, 8]])

  # Select 1 item from the first and 2 items from the second.
  ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  kd.sample_n(ds, [1, 2], 123) -> kd.slice([[4], [None, 5]])

  # Use 'key' for stability
  ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
  key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample_n(ds_1, 2, 123, key_1) -> kd.slice([[None, 2], [None, None]])

  ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
  key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
  kd.sample_n(ds_2, 2, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

Args:
  x: DataSlice to sample.
  n: number of items to sample. Either an integer or a DataSlice.
  seed: seed from random sampling.
  key: keys used to generate random numbers. The same key generates the same
    random number.

Returns:
  Sampled DataSlice.
```

### `select(ds, fltr, expand_filter)` {#select}

``` {.no-copy}
Creates a new DataSlice by filtering out missing items in fltr.

The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
missing items in `fltr` are removed. The last dimension of the resulting
DataSlice is changed while the first N-1 dimensions are the same as those in
`ds`.

Example:
  val = kd.slice([[1, None, 4], [None], [2, 8]])
  kd.select(val, val > 3) -> [[4], [], [8]]

  fltr = kd.slice(
      [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
  kd.select(val, fltr) -> [[None, 4], [None], [2]]

  fltr = kd.slice([kd.present, kd.present, None])
  kd.select(val, fltr) -> [[1, None, 4], [None], []]
  kd.select(val, fltr, expand_filter=False) -> [[1, None, 4], [None]]

Args:
  ds: DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK.
  expand_filter: flag indicating if the 'filter' should be expanded to 'ds'

Returns:
  Filtered DataSlice.
```

### `select_items(ds, fltr)` {#select_items}

``` {.no-copy}
Selects List items by filtering out missing items in fltr.

Also see kd.select.

Args:
  ds: List DataSlice to be filtered
  fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
    Functor or a Python function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `select_keys(ds, fltr)` {#select_keys}

``` {.no-copy}
Selects Dict keys by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or
    a Python function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `select_present(ds)` {#select_present}

``` {.no-copy}
Creates a new DataSlice by removing missing items.
```

### `select_values(ds, fltr)` {#select_values}

``` {.no-copy}
Selects Dict values by filtering out missing items in `fltr`.

Also see kd.select.

Args:
  ds: Dict DataSlice to be filtered
  fltr: filter DataSlice with dtype as kd.MASK or a Koda Functor or
    a Python function which can be evalauted to such DataSlice.

Returns:
  Filtered DataSlice.
```

### `set_attr(x, attr_name, value, update_schema)` {#set_attr}

``` {.no-copy}
Sets an attribute `attr_name` to `value`.

  If `update_schema` is True and `x` is either an Entity with explicit schema
  or an Object where some items are entities with explicit schema, it will get
  updated with `value`'s schema first.

  Args:
    x: a DataSlice on which to set the attribute. Must have DataBag attached.
    attr_name: attribute name
    value: a DataSlice or convertible to a DataSlice that will be assigned as an
      attribute.
    update_schema: whether to update the schema before setting an attribute.
```

### `set_attrs(x, *, update_schema, **attrs)` {#set_attrs}

``` {.no-copy}
Sets multiple attributes on an object / entity.

  Args:
    x: a DataSlice on which attributes are set. Must have DataBag attached.
    update_schema: (bool) overwrite schema if attribute schema is missing or
      incompatible.
    **attrs: attribute values that are converted to DataSlices with DataBag
      adoption.
```

### `set_schema(x, schema)` {#set_schema}

``` {.no-copy}
Returns a copy of `x` with the provided `schema`.

  If `schema` is an Entity schema and has a different DataBag than `x`, it is
  merged into the DataBag of `x`.

  It only changes the schemas of `x` and does not change the items in `x`. To
  change the items in `x`, use `kd.cast_to` instead. For example,

    kd.set_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
        `x` are not compatible with FLOAT32.
    kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

  When items in `x` are primitives or `schemas` is a primitive schema, it checks
  items and schema are compatible. When items are ItemIds and `schema` is a
  non-primitive schema, it does not check the underlying data matches the
  schema. For example,

    kd.set_schema(kd.ds([1, 2, 3], schema=kd.ANY), kd.INT32) -> kd.ds([1, 2, 3])
    kd.set_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail
    kd.set_schema(kd.ds(1).with_bag(kd.bag()), kd.new_schema(x=kd.INT32)) ->
    fail
    kd.set_schema(kd.new(x=1), kd.INT32) -> fail
    kd.set_schema(kd.new(x=1), kd.new_schema(x=kd.INT64)) -> work

  Args:
    x: DataSlice to change the schema of.
    schema: DataSlice containing the new schema.

  Returns:
    DataSlice with the new schema.
```

### `shallow_clone(x, schema, **overrides)` {#shallow_clone}

``` {.no-copy}
Eager version of `kde.shallow_clone` operator.
```

### `size(x)` {#size}

``` {.no-copy}
Returns the number of items in `x`, including missing items.

Args:
  x: A DataSlice.

Returns:
  The size of `x`.
```

### `slice` {#slice}

``` {.no-copy}
Creates a DataSlice from `value`.
If `schema` is set, that schema is used,
otherwise the schema is inferred from `value`.
```

### `sort(x, sort_by, descending)` {#sort}

``` {.no-copy}
Sorts the items in `x` over the last dimension.

When `sort_by` is specified, it is used to sort items in `x`. `sort_by` must
have the same shape as `x` and cannot be more sparse than `x`. Otherwise,
items in `x` are compared by their values. Missing items are put in the end of
the sorted list regardless of the value of `descending`.

Examples:
  ds = kd.slice([[[2, 1, None, 4], [4, 1]], [[5, 4, None]]])

  kd.sort(ds) -> kd.slice([[[1, 2, 4, None], [1, 4]], [[4, 5, None]]])

  kd.sort(ds, descending=True) ->
      kd.slice([[[4, 2, 1, None], [4, 1]], [[5, 4, None]]])

  sort_by = kd.slice([[[9, 2, 1, 3], [2, 3]], [[9, 7, 9]]])
  kd.sort(ds, sort_by) ->
      kd.slice([[[None, 1, 4, 2], [4, 1]], [[4, 5, None]]])

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4])) ->
      raise due to different shapes

  kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4, None])) ->
      raise as `sort_by` is more sparse than `x`

Args:
  x: DataSlice to sort.
  sort_by: DataSlice used for comparisons.
  descending: whether to do descending sort.

Returns:
  DataSlice with last dimension sorted.
```

### `split(x, sep)` {#split}

``` {.no-copy}
Returns x split by the provided separator.

Example:
  ds = kd.slice(['Hello world!', 'Goodbye world!'])
  kd.split(ds)  # -> kd.slice([['Hello', 'world!'], ['Goodbye', 'world!']])

Args:
  x: DataSlice: (can be text or bytes)
  sep: If specified, will split by the specified string not omitting empty
    strings, otherwise will split by whitespaces while omitting empty strings.
```

### `stack(*args, ndim)` {#stack}

``` {.no-copy}
Stacks the given DataSlices, creating a new dimension at index `rank-ndim`.

The given DataSlices must have the same rank, and the shapes of the first
`rank-ndim` dimensions must match. If they have incompatible shapes, consider
using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
to bring them to compatible shapes first.

The result has the following shape:
  1) the shape of the first `rank-ndim` dimensions remains the same
  2) a new dimension is inserted at `rank-ndim` with uniform shape `len(args)`
  3) the shapes of the last `ndim` dimensions are interleaved within the
    groups implied by the newly-inserted dimension

Alteratively, if we think of each input DataSlice as a nested Python list,
this operator simultaneously iterates over the inputs at depth `rank-ndim`,
wrapping the corresponding nested sub-lists from each input in new lists.

For example,
a = kd.slice([[1, None, 3], [4]])
b = kd.slice([[7, 7, 7], [7]])

kd.stack(a, b, ndim=0) -> [[[1, 7], [None, 7], [3, 7]], [[4, 7]]]
kd.stack(a, b, ndim=1) -> [[[1, None, 3], [7, 7, 7]], [[4], [7]]]
kd.stack(a, b, ndim=2) -> [[[1, None, 3], [4]], [[7, 7, 7], [7]]]
kd.stack(a, b, ndim=4) -> raise an exception
kd.stack(a, b) -> the same as kd.stack(a, b, ndim=0)

Args:
  args: The DataSlices to stack.
  ndim: The number of last dimensions to stack (default 0).

Returns:
  The stacked DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.
```

### `str(x)` {#str}

``` {.no-copy}
Returns kd.slice(x, kd.STRING).
```

### `stub(x, attrs)` {#stub}

``` {.no-copy}
Copies a DataSlice's schema stub to a new DataBag.

The "schema stub" of a slice is a subset of its schema (including embedded
schemas) that contains just enough information to support direct updates to
that slice.

Optionally copies `attrs` schema attributes to the new DataBag as well.

This method works for items, objects, and for lists and dicts stored as items
or objects. The intended usage is to add new attributes to the object in the
new bag, or new items to the dict in the new bag, and then to be able
to merge the bags to obtain a union of attributes/values. For lists, we
extract the list with stubs for list items, which also works recursively so
nested lists are deep-extracted. Note that if you modify the list afterwards
by appending or removing items, you will no longer be able to merge the result
with the original bag.

Args:
  x: DataSlice to extract the schema stub from.
  attrs: Optional list of additional schema attribute names to copy. The
    schemas for those attributes will be copied recursively (so including
    attributes of those attributes etc).

Returns:
  DataSlice with the same schema stub in the new DataBag.
```

### `subslice(x, *slices)` {#subslice}

``` {.no-copy}
Slices `x` across all of its dimensions based on the provided `slices`.

`slices` is a variadic argument for slicing arguments where individual
slicing argument can be one of the following:

  1) INT32/INT64 DataItem or Python integer wrapped into INT32 DataItem. It is
     used to select a single item in one dimension. It reduces the number of
     dimensions in the resulting DataSlice by 1.
  2) INT32/INT64 DataSlice or Python nested list of integer wrapped into INT32
     DataSlice. It is used to select multiple items in one dimension.
  3) Python slice (e.g. slice(1), slice(1, 3), slice(2, -1)). It is used to
     select a slice of items in one dimension. 'step' is not supported and it
     results in no item if 'start' is larger than or equal to 'stop'.
  4) .../Ellipsis. It can appear at most once in `slices` and used to fill
     corresponding dimensions in `x` but missing in `slices`. It means
     selecting all items in these dimensions.

If the Ellipsis is not provided, it is added to the **beginning** of `slices`
by default, which is different from Numpy. Individual slicing argument is used
to slice corresponding dimension in `x`.

The slicing algorithm can be thought as:
  1) implode `x` recursively to a List DataItem
  2) explode the List DataItem recursively with the slicing arguments (i.e.
     imploded_x[slice])

Example 1:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 0)
    => kd.slice([[1, 3], [4], [7, 8]])

Example 2:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 0, 1, kd.item(0))
    => kd.item(3)

Example 3:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1))
    => kd.slice([[[1], []], [[4, 5]], [[], [8]]])

Example 4:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
     => kd.slice([[[2], []], [[5, 6]]])

Example 5 (also see Example 6/7 for using DataSlices for subslicing):
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), kd.slice(0))
    => kd.slice([[4, 4], [8, 7]])

Example 6:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), ...)
    => kd.slice([[[4, 5, 6]], [[7], [8, 9]]])

Example 7:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), ...)
    => kd.slice([[[4, 5, 6], [4, 5, 6]], [[8, 9], [7]]])

Example 8:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., slice(1, None))
    => kd.slice([[[2], []], [[5, 6]], [[], [9]]])

Example 9:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 2, ..., slice(1, None))
    => kd.slice([[], [9]])

Example 10:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, ..., 2, ...)
    => error as ellipsis can only appear once

Example 11:
  x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
  kd.subslice(x, 1, 2, 3, 4)
    => error as at most 3 slicing arguments can be provided

Note that there is a shortcut `ds.S[*slices] for this operator which is more
commonly used and the Python slice can be written as [start:end] format. For
example:
  kd.subslice(x, 0) == x.S[0]
  kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
  kd.subslice(x, slice(0, -1)) == x.S[0:-1]
  kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
    == x.S[0:-1, 0:1, 1:]
  kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
  kd.subslice(x, slice(1, None)) == x.S[1:]

Args:
  x: DataSlice to slice.
  *slices: variadic slicing argument.

Returns:
  A DataSlice with selected items
```

### `substr(x, start, end)` {#substr}

``` {.no-copy}
Returns a DataSlice of substrings with indices [start, end).

The usual Python rules apply:
  * A negative index is computed from the end of the string.
  * An empty range yields an empty string, for example when start >= end and
    both are positive.

The result is broadcasted to the common shape of all inputs.

Examples:
  ds = kd.slice([['Hello World!', 'Ciao bella'], ['Dolly!']])
  kd.substr(ds)         # -> kd.slice([['Hello World!', 'Ciao bella'],
                                       ['Dolly!']])
  kd.substr(ds, 5)      # -> kd.slice([[' World!', 'bella'], ['!']])
  kd.substr(ds, -2)     # -> kd.slice([['d!', 'la'], ['y!']])
  kd.substr(ds, 1, 5)   # -> kd.slice([['ello', 'iao '], ['olly']])
  kd.substr(ds, 5, -1)  # -> kd.slice([[' World', 'bell'], ['']])
  kd.substr(ds, 4, 100) # -> kd.slice([['o World!', ' bella'], ['y!']])
  kd.substr(ds, -1, -2) # -> kd.slice([['', ''], ['']])
  kd.substr(ds, -2, -1) # -> kd.slice([['d', 'l'], ['y']])

  # Start and end may also be multidimensional.
  ds = kd.slice('Hello World!')
  start = kd.slice([1, 2])
  end = kd.slice([[2, 3], [4]])
  kd.substr(ds, start, end) # -> kd.slice([['e', 'el'], ['ll']])

Args:
  x: Text or Bytes DataSlice. If text, then `start` and `end` are codepoint
    offsets. If bytes, then `start` and `end` are byte offsets.
  start: The start index of the substring. Inclusive. Assumed to be 0 if
    unspecified.
  end: The end index of the substring. Exclusive. Assumed to be the length of
    the string if unspecified.
```

### `subtract(x, y)` {#subtract}

``` {.no-copy}
Computes pointwise x - y.
```

### `sum(x)` {#sum}

``` {.no-copy}
Returns the sum of elements over all dimensions.

The result is a zero-dimensional DataItem.

Args:
  x: A DataSlice of numbers.
```

### `take(x, indices)` {#take}

``` {.no-copy}
Returns a new DataSlice with items at provided indices.

`indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
INT64 items.

Indices in the DataSlice `indices` are based on the last dimension of the
DataSlice `x`. Negative indices are supported and out-of-bound indices result
in missing items.

If ndim(x) - 1 > ndim(indices), indices are broadcasted to shape(x)[:-1].
If ndim(x) <= ndim(indices), indices are unchanged but shape(x)[:-1] must be
broadcastable to shape(indices).

Example:
  x = kd.slice([[1, None, 2], [3, 4]])
  kd.take(x, kd.item(1))  # -> kd.slice([[None, 4]])
  kd.take(x, kd.slice([0, 1]))  # -> kd.slice([1, 4])
  kd.take(x, kd.slice([[0, 1], [1]]))  # -> kd.slice([[1, None], [4]])
  kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
    # -> kd.slice([[[1, None]], []], [[4], [3]]])
  kd.take(x, kd.slice([3, -3]))  # -> kd.slice([None, None])
  kd.take(x, kd.slice([-1, -2]))  # -> kd.slice([2, 3])
  kd.take(x, kd.slice('1')) # -> dtype mismatch error
  kd.take(x, kd.slice([1, 2, 3])) -> incompatible shape

Args:
  x: DataSlice to be indexed
  indices: indices used to select items

Returns:
  A new DataSlice with items selected by indices.
```

### `text(x)` {#text}

``` {.no-copy}
Returns kd.slice(x, kd.STRING).
```

### `to_any(x)` {#to_any}

``` {.no-copy}
Casts `x` to ANY using explicit (permissive) casting rules.
```

### `to_bool(x)` {#to_bool}

``` {.no-copy}
Casts `x` to BOOLEAN using explicit (permissive) casting rules.
```

### `to_bytes(x)` {#to_bytes}

``` {.no-copy}
Casts `x` to BYTES using explicit (permissive) casting rules.
```

### `to_expr(x)` {#to_expr}

``` {.no-copy}
Casts `x` to EXPR using explicit (permissive) casting rules.
```

### `to_float32(x)` {#to_float32}

``` {.no-copy}
Casts `x` to FLOAT32 using explicit (permissive) casting rules.
```

### `to_float64(x)` {#to_float64}

``` {.no-copy}
Casts `x` to FLOAT64 using explicit (permissive) casting rules.
```

### `to_int32(x)` {#to_int32}

``` {.no-copy}
Casts `x` to INT32 using explicit (permissive) casting rules.
```

### `to_int64(x)` {#to_int64}

``` {.no-copy}
Casts `x` to INT64 using explicit (permissive) casting rules.
```

### `to_itemid(x)` {#to_itemid}

``` {.no-copy}
Casts `x` to ITEMID using explicit (permissive) casting rules.
```

### `to_mask(x)` {#to_mask}

``` {.no-copy}
Casts `x` to MASK using explicit (permissive) casting rules.
```

### `to_none(x)` {#to_none}

``` {.no-copy}
Casts `x` to NONE using explicit (permissive) casting rules.
```

### `to_object(x)` {#to_object}

``` {.no-copy}
Casts `x` to OBJECT using explicit (permissive) casting rules.
```

### `to_proto(x, /, message_class)` {#to_proto}

``` {.no-copy}
Converts a DataSlice or DataItem to one or more proto messages.

  If `x` is a DataItem, this returns a single proto message object. Otherwise,
  `x` must be a 1-D DataSlice, and this returns a list of proto message objects
  with the same size as the input.

  Koda data structures are converted to equivalent proto messages, primitive
  fields, repeated fields, maps, and enums, based on the proto schema. Koda
  entity attributes are converted to message fields with the same name, if
  those fields exist, otherwise they are ignored.

  Koda slices with mixed underlying dtypes are tolerated wherever the proto
  conversion is defined for all dtypes, regardless of schema.

  Koda entity attributes that are parenthesized fully-qualified extension
  paths (e.g. "(package_name.some_extension)") are converted to extensions,
  if those extensions exist in the descriptor pool of the messages' common
  descriptor, otherwise they are ignored.

  Args:
    x: DataSlice to convert.
    message_class: A proto message class.

  Returns:
    A converted proto message or list of converted proto messages.
```

### `to_py(ds, max_depth, obj_as_dict, include_missing_attrs)` {#to_py}

*No description*

### `to_pylist(x)` {#to_pylist}

``` {.no-copy}
Expands the outermost DataSlice dimension into a list of DataSlices.
```

### `to_pytree(ds, max_depth, include_missing_attrs)` {#to_pytree}

*No description*

### `to_schema(x)` {#to_schema}

``` {.no-copy}
Casts `x` to SCHEMA using explicit (permissive) casting rules.
```

### `to_str(x)` {#to_str}

``` {.no-copy}
Casts `x` to STRING using explicit (permissive) casting rules.
```

### `to_text(x)` {#to_text}

``` {.no-copy}
Casts `x` to STRING using explicit (permissive) casting rules.
```

### `trace_as_fn(*, name, py_fn)` {#trace_as_fn}

``` {.no-copy}
A decorator to customize the tracing behavior for a particular function.

  A function with this decorator is converted to an internally-stored functor.
  In traced expressions that call the function, that functor is invoked as a
  sub-functor via by 'kde.call', rather than the function being re-traced.
  Additionally, the result of 'kde.call' is also assigned a name, so that
  when auto_variables=True is used (which is the default in kdf.trace_py_fn),
  the functor for the decorated function will become an attribute of the
  functor for the outer function being traced.

  This can be used to avoid excessive re-tracing and recompilation of shared
  python functions, to quickly add structure to the functor produced by tracing
  for complex computations, or to conveniently embed a py_fn into a traced
  expression.

  This decorator is intended to be applied to standalone functions.

  When applying it to a lambda, consider specifying an explicit name, otherwise
  it will be called '<lambda>' or '<lambda>_0' etc, which is not very useful.

  When applying it to a class method, it is likely to fail in tracing mode
  because it will try to auto-box the class instance into an expr, which is
  likely not supported.
```

### `translate(keys_to, keys_from, values_from)` {#translate}

``` {.no-copy}
Translates `keys_to` based on `keys_from`->`values_from` mapping.

The translation is done by matching keys from `keys_from` to `keys_to` over
the last dimension of `keys_to`. `keys_from` cannot have duplicate keys within
each group of the last dimension. Also see kd.translate_group.

`values_from` is first broadcasted to `keys_from` and the first N-1 dimensions
of `keys_from` and `keys_to` must be the same. The resulting DataSlice has the
same shape as `keys_to` and the same DataBag as `values_from`.

`keys_from` and `keys_to` must have the same schema.

Missing items or items with no matching keys in `keys_from` result in missing
items in the resulting DataSlice.

For example:

keys_to = kd.slice([['a', 'd'], ['c', None]])
keys_from = kd.slice([['a', 'b'], ['c', None]])
values_from = kd.slice([[1, 2], [3, 4]])
kd.translate(keys_to, keys_from, values_from) ->
    kd.slice([[1, None], [3, None]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.
```

### `translate_group(keys_to, keys_from, values_from)` {#translate_group}

``` {.no-copy}
Translates `keys_to` based on `keys_from`->`values_from` mapping.

The translation is done by creating an additional dimension under `keys_to`
and putting items in `values_from` to this dimension by matching keys from
`keys_from` to `keys_to` over the last dimension of `keys_to`.
`keys_to` can have duplicate keys within each group of the last
dimension.

`values_from` and `keys_from` must have the same shape and the first N-1
dimensions of `keys_from` and `keys_to` must be the same. The shape of
resulting DataSlice is the combination of the shape of `keys_to` and an
injected group_by dimension.

`keys_from` and `keys_to` must have the same schema.

Missing items or items with no matching keys in `keys_from` result in empty
groups in the resulting DataSlice.

For example:

keys_to = kd.slice(['a', 'c', None, 'd', 'e'])
keys_from = kd.slice(['a', 'c', 'b', 'c', 'a', 'e'])
values_from = kd.slice([1, 2, 3, 4, 5, 6])
kd.translate_group(keys_to, keys_from, values_from) ->
  kd.slice([[1, 5], [2, 4], [], [], [6]])

Args:
  keys_to: DataSlice of keys to be translated.
  keys_from: DataSlice of keys to be matched.
  values_from: DataSlice of values to be matched.

Returns:
  A DataSlice of translated values.
```

### `unique(x, sort)` {#unique}

``` {.no-copy}
Returns a DataSlice with unique values within each dimension.

The resulting DataSlice has the same rank as `x`, but a different shape.
The first `get_ndim(x) - 1` dimensions are unchanged. The last dimension
contains the unique values.

If `sort` is False elements are ordered by the appearance of the first object.

If `sort` is True:
1. Elements are ordered by the value.
2. Mixed types are not supported.
3. ExprQuote and DType are not supported.

Example 1:
  x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
  sort: False
  result: kd.unique([1, 3, 2])

Example 2:
  x: kd.slice([[1, 2, 1, 3, 1, 3], [3, 1, 1]])
  sort: False
  result: kd.slice([[1, 2, 3], [3, 1]])

Example 3:
  x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
  sort: False
  result: kd.slice([1, 3, 2])

  Missing values are ignored.

Example 4:
  x: kd.slice([[1, 3, 2, 1, 3, 1, 3], [3, 1, 1]])
  sort: True
  result: kd.slice([[1, 2, 3], [1, 3]])

Args:
  x: DataSlice to find unique values in.
  sort: whether elements must be ordered by the value.

Returns:
  DataSlice with the same rank and schema as `x` with unique values in the
  last dimension.
```

### `update_schema(obj, **attr_schemas)` {#update_schema}

``` {.no-copy}
Updates the schema of `obj` DataSlice using given schemas for attrs.
```

### `updated(ds, *bag)` {#updated}

``` {.no-copy}
Returns a copy of a DataSlice with DataBag(s) of updates applied.

Values in `*bag` take precedence over the ones in the original DataBag of
`ds`.

The DataBag attached to the result is a new immutable DataBag that falls back
to the DataBag of `ds` if present and then to `*bag`.

`updated(x, a, b)` is equivalent to `updated(updated(x, b), a)`, and so on
for additional DataBag args.

Args:
  ds: DataSlice.
  *bag: DataBag(s) of updates.

Returns:
  DataSlice with additional fallbacks.
```

### `updated_bag(*bags)` {#updated_bag}

``` {.no-copy}
Creates a new immutable DataBag updated by `bags`.

 It adds `bags` as fallbacks rather than merging the underlying data thus
 the cost is O(1).

 Databags later in the list have higher priority.
 `updated_bag(bag1, bag2, bag3)` is equivalent to
 `updated_bag(bag1, updated_bag(bag2, bag3)`, and so on for additional
 DataBag args.

Args:
  *bags: DataBag(s) for updating.

Returns:
  An immutable DataBag updated by `bags`.
```

### `uu(*, seed, schema, update_schema, db, **attrs)` {#uu}

``` {.no-copy}
Creates UuEntities with given attrs.

  Args:
    seed: string to seed the uuid computation with.
    schema: optional DataSlice schema. If not specified, a UuSchema
      will be automatically created based on the schemas of the passed **attrs.
      Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
      schema instead.
    update_schema: if schema attribute is missing and the attribute is being set
      through `attrs`, schema is successfully updated.
    db: optional DataBag where entities are created.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
```

### `uu_schema(seed, db, **attrs)` {#uu_schema}

``` {.no-copy}
Creates a uu_schema in the given DataBag.

  Args:
    seed: optional string to seed the uuid computation with.
    db: optional DataBag where the schema is created. If not provided, a new
      Databag is created.
    **attrs: attrs to set on the schema. Must be schemas.

  Returns:
    data_slice.DataSlice with the given attrs and kd.SCHEMA schema.
```

### `uuid(*, seed, **kwargs)` {#uuid}

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

Args:
  seed: text seed for the uuid computation.
  kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `uuid_for_dict(*, seed, **kwargs)` {#uuid_for_dict}

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying dict items.

e.g.

kd.dict(['a', 'b'], [1, 2], itemid=kd.uuid_for_dict(seed='seed', a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `uuid_for_list(*, seed, **kwargs)` {#uuid_for_list}

``` {.no-copy}
Creates a DataSlice whose items are Fingerprints identifying arguments.

To be used for keying list items.

e.g.

kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed='seed', a=ds(1)))

Args:
  seed: text seed for the uuid computation.
  kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
  item from each kwarg value.
```

### `uuids_with_allocation_size(*, seed, size)` {#uuids_with_allocation_size}

``` {.no-copy}
Creates a DataSlice whose items are uuids.

The uuids are allocated in a single allocation. They are all distinct.
You can think of the result as a DataSlice created with:
[fingerprint(seed, size, i) for i in range(size)]

Args:
  seed: text seed for the uuid computation.
  size: the size of the allocation. It will also be used for the uuid
    computation.

Returns:
  A 1-dimensional DataSlice with `size` distinct uuids.
```

### `uuobj(*, seed, **kwargs)` {#uuobj}

``` {.no-copy}
Creates Object(s) whose ids are uuid(s) with the provided attributes.

In order to create a different id from the same arguments, use
`seed` argument with the desired value, e.g.

kd.uuobj(seed='type_1', x=[1, 2, 3], y=[4, 5, 6])

and

kd.uuobj(seed='type_2', x=[1, 2, 3], y=[4, 5, 6])

have different ids.

Args:
  seed: text seed for the uuid computation.
  kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
    values must be alignable.

Returns:
  (DataSlice) of uuids. The provided attributes are also set in a newly
  created databag. The shape of this DataSlice is the result of aligning the
  shapes of the kwarg DataSlices.
```

### `val_like(x, val)` {#val_like}

``` {.no-copy}
Creates a DataSlice with `val` masked and expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, None])
  kd.core.val_like(x, 1) -> kd.slice([[1], [1, None]])
  kd.core.val_like(x, kd.slice([1, 2])) -> kd.slice([[1], [2, None]])
  kd.core.val_like(x, kd.slice([None, 2])) -> kd.slice([[None], [2, None]])

Args:
  x: DataSlice to match the shape and sparsity of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x` and masked by `x`.
```

### `val_shaped(shape, val)` {#val_shaped}

``` {.no-copy}
Creates a DataSlice with `val` expanded to the given shape.

Example:
  shape = kd.shapes.create_shape([2], [1, 2])
  kd.core.val_shaped(shape, 1) -> kd.slice([[1], [1, 1]])
  kd.core.val_shaped(shape, kd.slice([None, 2])) -> kd.slice([[None], [2, 2]])

Args:
  shape: shape to expand to.
  val: value to expand.

Returns:
  A DataSlice with the same shape as `shape`.
```

### `val_shaped_as(x, val)` {#val_shaped_as}

``` {.no-copy}
Creates a DataSlice with `val` expanded to the shape of `x`.

Example:
  x = kd.slice([0], [0, 0])
  kd.core.val_shaped_as(x, 1) -> kd.slice([[1], [1, 1]])
  kd.core.val_shaped_as(x, kd.slice([None, 2])) -> kd.slice([[None], [2, 2]])

Args:
  x: DataSlice to match the shape of.
  val: DataSlice to expand.

Returns:
  A DataSlice with the same shape as `x`.
```

### `with_attrs(x, /, *, update_schema, **attrs)` {#with_attrs}

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated attributes.
```

### `with_bag(ds, bag)` {#with_bag}

``` {.no-copy}
Returns a DataSlice with the given DataBatg attached.
```

### `with_db(ds, bag)` {#with_db}

``` {.no-copy}
Returns a DataSlice with the given DataBatg attached.
```

### `with_dict_update(x, keys, values)` {#with_dict_update}

``` {.no-copy}
Returns a DataSlice with a new DataBag containing updated dicts.

This operator has two forms:
  kde.with_dict_update(x, keys, values) where keys and values are slices
  kde.with_dict_update(x, dict_updates) where dict_updates is a slice of dicts

If both keys and values are specified, they must both be broadcastable to the
shape of `x`. If only keys is specified (as dict_updates), it must be
broadcastable to 'x'.

Args:
  x: DataSlice of dicts to update.
  keys: A DataSlice of keys, or a DataSlice of dicts of updates.
  values: A DataSlice of values, or unspecified if `keys` contains dicts.
```

### `with_merged_bag(ds)` {#with_merged_bag}

``` {.no-copy}
Returns a DataSlice with the DataBag of `ds` merged with its fallbacks.

Note that a DataBag has multiple fallback DataBags and fallback DataBags can
have fallbacks as well. This operator merges all of them into a new immutable
DataBag.

If `ds` has no attached DataBag, it raises an exception. If the DataBag of
`ds` does not have fallback DataBags, it is equivalent to `ds.freeze()`.

Args:
  ds: DataSlice to merge fallback DataBags of.

Returns:
  A new DataSlice with an immutable DataBags.
```

### `with_name(obj, name)` {#with_name}

``` {.no-copy}
Checks that the `name` is a string and returns `obj` unchanged.

  This method is useful in tracing workflows: when tracing, we will assign
  the given name to the subexpression computing `obj`. In eager mode, this
  method is effectively a no-op.

  Args:
    obj: Any object.
    name: The name to be used for this sub-expression when tracing this code.
      Must be a string.

  Returns:
    obj unchanged.
```

### `with_schema(x, schema)` {#with_schema}

``` {.no-copy}
Returns a copy of `x` with the provided `schema`.

If `schema` is an Entity schema, it must have no DataBag or the same DataBag
as `x`. To set schema with a different DataBag, use `kd.set_schema` instead.

It only changes the schemas of `x` and does not change the items in `x`. To
change the items in `x`, use `kd.cast_to` instead. For example,

  kd.with_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
      `x` are not compatible with FLOAT32.
  kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

When items in `x` are primitives or `schemas` is a primitive schema, it checks
items and schema are compatible. When items are ItemIds and `schema` is a
non-primitive schema, it does not check the underlying data matches the
schema. For example,

  kd.with_schema(kd.ds([1, 2, 3], schema=kd.ANY), kd.INT32) ->
      kd.ds([1, 2, 3])
  kd.with_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail

  db = kd.bag()
  kd.with_schema(kd.ds(1).with_bag(db), db.new_schema(x=kd.INT32)) -> fail due
      to incompatible schema
  kd.with_schema(db.new(x=1), kd.INT32) -> fail due to incompatible schema
  kd.with_schema(db.new(x=1), kd.new_schema(x=kd.INT32)) -> fail due to
      different DataBag
  kd.with_schema(db.new(x=1), kd.new_schema(x=kd.INT32).no_bag()) -> work
  kd.with_schema(db.new(x=1), db.new_schema(x=kd.INT64)) -> work

Args:
  x: DataSlice to change the schema of.
  schema: DataSlice containing the new schema.

Returns:
  DataSlice with the new schema.
```

### `with_schema_from_obj(x)` {#with_schema_from_obj}

``` {.no-copy}
Returns `x` with its embedded schema set as the schema.

* `x` must have OBJECT schema.
* All items in `x` must have the same embedded schema.
* At least one value in `x` must be present.

Args:
  x: An OBJECT DataSlice.
```

### `zip(*args)` {#zip}

``` {.no-copy}
Zips the given DataSlices into a new DataSlice with a new last dimension.

Input DataSlices are automatically aligned. The result has the shape of the
aligned inputs, plus a new last dimension with uniform shape `len(args)`
containing the values from each input.

For example,
a = kd.slice([1, 2, 3, 4])
b = kd.slice([5, 6, 7, 8])
c = kd.slice(['a', 'b', 'c', 'd'])
kd.zip(a, b, c) -> [[1, 5, 'a'], [2, 6, 'b'], [3, 7, 'c'], [4, 8, 'd']]

a = kd.slice([[1, None, 3], [4]])
b = kd.slice([7, None])
kd.zip(a, b) ->  [[[1, 7], [None, 7], [3, 7]], [[4, None]]]

Args:
  args: The DataSlices to zip.

Returns:
  The zipped DataSlice. If the input DataSlices come from different DataBags,
  this will refer to a merged immutable DataBag.
```

</section>

## `kdf` operators {#kdf-ops_category}

Operators under the `kdf` module used for Functor creation or miscellaneous
utilities.

<section class="zippy open">

**Operators**

### `allow_arbitrary_unused_inputs(fn_def)` {#allow_arbitrary_unused_inputs}

``` {.no-copy}
Returns a functor that allows unused inputs but otherwise behaves the same.

  This is done by adding a `**__extra_inputs__` argument to the signature if
  there is no existing variadic keyword argument there. If there is a variadic
  keyword argument, this function will return the original functor.

  This means that if the functor already accepts arbitrary inputs but fails
  on unknown inputs further down the line (for example, when calling another
  functor), this method will not fix it. In particular, this method has no
  effect on the return values of kdf.py_fn or kdf.bind. It does however work
  on the output of kdf.trace_py_fn.

  Args:
    fn_def: The input functor.

  Returns:
    The input functor if it already has a variadic keyword argument, or its copy
    but with an additional `**__extra_inputs__` variadic keyword argument if
    there is no existing variadic keyword argument.
```

### `as_fn(f, *, use_tracing, **kwargs)` {#as_fn}

``` {.no-copy}
Returns a Koda functor representing `f`.

  This is the most generic version of the kdf builder functions.
  It accepts all kdf supported function types including python functions,
  Koda Expr.

  Args:
    f: Python function, Koda Expr, Expr packed into a DataItem, or a Koda
      functor (the latter will be just returned unchanged).
    use_tracing: Whether tracing should be used for Python functions.
    **kwargs: Either variables or defaults to pass to the function. See the
      documentation of `fn` and `py_fn` for more details.

  Returns:
    A Koda functor representing `f`.
```

### `bind(fn_def, /, *, return_type_as, **kwargs)` {#bind}

``` {.no-copy}
Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  You can pass expressions with their own inputs as values in `kwargs`. Those
  inputs will become inputs of the resulting functor, will be used to compute
  those expressions, _and_ they will also be passed to the underying functor.
  Use kdf.call_fn for a more clear separation of those inputs.

  Example:
    f = kdf.bind(kdf.fn(I.x + I.y), x=0)
    kd.call(f, y=1)  # 1

  Args:
    fn_def: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **kwargs: Partial parameter binding. The values in this map may be Koda
      expressions or DataItems. When they are expressions, they must evaluate to
      a DataSlice/DataItem or a primitive that will be automatically wrapped
      into a DataItem. This function creates auxiliary variables with names
      starting with '_aux_fn', so it is not recommended to pass variables with
      such names.

  Returns:
    A new Koda functor with some parameters bound.
```

### `fn(returns, *, signature, auto_variables, **variables)` {#fn}

``` {.no-copy}
Creates a functor.

  Args:
    returns: What should calling a functor return. Will typically be an Expr to
      be evaluated, but can also be a DataItem in which case calling will just
      return this DataItem, or a primitive that will be wrapped as a DataItem.
      When this is an Expr, it either must evaluate to a DataSlice/DataItem, or
      the return_type_as= argument should be specified at kd.call time.
    signature: The signature of the functor. Will be used to map from args/
      kwargs passed at calling time to I.smth inputs of the expressions. When
      None, the default signature will be created based on the inputs from the
      expressions involved.
    auto_variables: When true, we create additional variables automatically
      based on the provided expressions for 'returns' and user-provided
      variables. All non-scalar DataSlice literals become their own variables,
      and all named subexpressions become their own variables. This helps
      readability and manipulation of the resulting functor.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataItem representing the functor.
```

### `get_signature(fn_def)` {#get_signature}

``` {.no-copy}
Retrieves the signature attached to the given functor.

  Args:
    fn_def: The functor to retrieve the signature for, or a slice thereof.

  Returns:
    The signature(s) attached to the functor(s).
```

### `py_fn(f, *, return_type_as, **defaults)` {#py_fn}

``` {.no-copy}
Returns a Koda functor wrapping a python function.

  This is the most flexible way to wrap a python function and is recommended
  for large, complex code.

  Functions wrapped with py_fn are not serializable.

  Note that unlike the functors created by kdf.fn from an Expr, this functor
  will have exactly the same signature as the original function. In particular,
  if the original function does not accept variadic keyword arguments and
  and unknown argument is passed when calling the functor, an exception will
  occur.

  Args:
    f: Python function. It is required that this function returns a
      DataSlice/DataItem or a primitive that will be automatically wrapped into
      a DataItem.
    return_type_as: The return type of the function is expected to be the same
      as the type of this value. This needs to be specified if the function does
      not return a DataSlice/DataItem or a primitive that would be auto-boxed
      into a DataItem. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kdf.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
```

### `trace_py_fn(f, *, auto_variables, **defaults)` {#trace_py_fn}

``` {.no-copy}
Returns a Koda functor created by tracing a given Python function.

  When 'f' has variadic positional (*args) or variadic keyword
  (**kwargs) arguments, their name must start with 'unused', and they
  must actually be unused inside 'f'.
  'f' must not use Python control flow operations such as if or for.

  Args:
    f: Python function.
    auto_variables: When true, we create additional variables automatically
      based on the traced expression. All DataSlice literals become their own
      variables, and all named subexpressions become their own variables. This
      helps readability and manipulation of the resulting functor. Note that
      this defaults to True here, while it defaults to False in kdf.fn.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kdf.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
```

</section>

## `kd_ext` operators {#kd-ext-ops_category}

Operators under the `kd_ext.xxx` modules for extension utilities. Importing from
the following module is needed:
`from koladata.ext import kd_ext`

<section class="zippy open">

**Operators**

### `npkd.ds_from_np(arr)` {#npkd.ds_from_np}

``` {.no-copy}
Converts a numpy array to a DataSlice.
```

### `npkd.ds_to_np(ds)` {#npkd.ds_to_np}

``` {.no-copy}
Converts a DataSlice to a numpy array.
```

### `npkd.get_elements_indices_from_ds(ds)` {#npkd.get_elements_indices_from_ds}

``` {.no-copy}
Returns a list of np arrays representing the DataSlice's indices.

  You can consider this as a n-dimensional coordinates of the items, p.ex. for a
  two-dimensional DataSlice:

  [[a, b],
   [],
   [c, d]] -> [[0, 0, 2, 2], [0, 1, 0, 1]]

   Let's explain this:
   - 'a' is in the first row and first column, its coordinates are (0, 0)
   - 'b' is in the first row and second column, its coordinates are (0, 1)
   - 'c' is in the third row and first column, its coordinates are (2, 0)
   - 'd' is in the third row and second column, its coordinates are (2, 1)

  if we write first y-coordinates, then x-coordinates, we get the following:
  [[0, 0, 2, 2], [0, 1, 0, 1]]

  The following conditions are satisfied:
  - result is always a two-dimensional array;
  - number of rows of the result equals the dimensionality of the input;
  - each row of the result has the same length and it corresponds to the total
  number of items in the DataSlice.

  Args:
    ds: DataSlice to get indices for.

  Returns:
    list of np arrays representing the DataSlice's elements indices.
```

### `npkd.reshape_based_on_indices(ds, indices)` {#npkd.reshape_based_on_indices}

``` {.no-copy}
Reshapes a DataSlice corresponding to the given indices.

  Inverse operation to get_elements_indices_from_ds.

  Let's explain this based on the following example:

  ds: [a, b, c, d]
  indices: [[0, 0, 2, 2], [0, 1, 0, 1]]
  result: [[a, b], [], [c, d]]

  Indices represent y- and x-coordinates of the items in the DataSlice.
  - 'a': according to the indices, its coordinates are (0, 0) (first element
  from the first and second row of indices conrrespondingly);
  it will be placed in the first row and first column of the result;
  - 'b': its coordinates are (0, 1); it will be placed in the first row and
  second column of the result;
  - 'c': its coordinates are (2, 0); it will be placed in the third row and
  first column of the result;
  - 'd': its coordinates are (2, 1); it will be placed in the third row and
  second column of the result.

  The result DataSlice will have the same number of items as the original
  DataSlice. Its dimensionality will be equal to the number of rows in the
  indices.

  Args:
    ds: DataSlice to reshape; can only be 1D.
    indices: list of np arrays representing the DataSlice's indices; it has to
      be a list of one-dimensional arrays where each row has equal number of
      elements corresponding to the number of items in the DataSlice.

  Returns:
    DataSlice reshaped based on the given indices.
```

### `pdkd.from_dataframe(df, as_obj)` {#pdkd.from_dataframe}

``` {.no-copy}
Creates a DataSlice from the given pandas DataFrame.

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
```

### `pdkd.to_dataframe(ds, cols)` {#pdkd.to_dataframe}

``` {.no-copy}
Creates a pandas DataFrame from the given DataSlice.

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
```

</section>

## `DataSlice` methods {#DataSlice_category}

`DataSlice` represents a jagged array of items (i.e. primitive values, ItemIds).

<section class="zippy open">

**Operators**

### `<DataSlice>.L` {#<DataSlice>.L}

``` {.no-copy}
ListSlicing helper for DataSlice.

  x.L on DataSlice returns a ListSlicingHelper, which treats the first dimension
  of DataSlice x as a a list.
```

### `<DataSlice>.S` {#<DataSlice>.S}

``` {.no-copy}
Slicing helper for DataSlice.

  It is a syntactic sugar for kd.subslice. That is, kd.subslice(ds, *slices)
  is equivalent to ds.S[*slices]. For example,
    kd.subslice(x, 0) == x.S[0]
    kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
    kd.subslice(x, slice(0, -1)) == x.S[0:-1]
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
      == x.S[0:-1, 0:1, 1:]
    kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
    kd.subslice(x, slice(1, None)) == x.S[1:]

  Please see kd.subslice for more detailed explanations and examples.
```

### `<DataSlice>.add_dim(self, sizes)` {#<DataSlice>.add_dim}

*No description*

### `<DataSlice>.append` {#<DataSlice>.append}

``` {.no-copy}
Append a value to each list in this DataSlice
```

### `<DataSlice>.as_any` {#<DataSlice>.as_any}

``` {.no-copy}
Returns a DataSlice with ANY schema.
```

### `<DataSlice>.as_itemid(self)` {#<DataSlice>.as_itemid}

*No description*

### `<DataSlice>.attrs(self, **attrs)` {#<DataSlice>.attrs}

*No description*

### `<DataSlice>.clear` {#<DataSlice>.clear}

``` {.no-copy}
Clears all dicts or lists in this DataSlice
```

### `<DataSlice>.clone(self, itemid, schema, **overrides)` {#<DataSlice>.clone}

*No description*

### `<DataSlice>.db` {#<DataSlice>.db}

``` {.no-copy}
This property is deprecated, please use .get_bag().
```

### `<DataSlice>.deep_clone(self, schema, **overrides)` {#<DataSlice>.deep_clone}

*No description*

### `<DataSlice>.deep_uuid(self, schema, *, seed)` {#<DataSlice>.deep_uuid}

*No description*

### `<DataSlice>.dict_size(self)` {#<DataSlice>.dict_size}

*No description*

### `<DataSlice>.dict_update(self, *args, **kwargs)` {#<DataSlice>.dict_update}

*No description*

### `<DataSlice>.embed_schema` {#<DataSlice>.embed_schema}

``` {.no-copy}
Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as '__schema__' attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.
```

### `<DataSlice>.enriched(self, *db)` {#<DataSlice>.enriched}

*No description*

### `<DataSlice>.expand_to(self, target, ndim)` {#<DataSlice>.expand_to}

*No description*

### `<DataSlice>.extract(self, schema)` {#<DataSlice>.extract}

*No description*

### `<DataSlice>.fingerprint` {#<DataSlice>.fingerprint}

``` {.no-copy}
Unique identifier of the value.
```

### `<DataSlice>.flatten(self, from_dim, to_dim)` {#<DataSlice>.flatten}

*No description*

### `<DataSlice>.follow(self)` {#<DataSlice>.follow}

*No description*

### `<DataSlice>.fork_bag(self)` {#<DataSlice>.fork_bag}

*No description*

### `<DataSlice>.fork_db(self)` {#<DataSlice>.fork_db}

*No description*

### `<DataSlice>.freeze` {#<DataSlice>.freeze}

``` {.no-copy}
Returns a frozen DataSlice equivalent to `self`.
```

### `<DataSlice>.from_vals` {#<DataSlice>.from_vals}

``` {.no-copy}
Creates a DataSlice from `value`.
If `schema` is set, that schema is used,
otherwise the schema is inferred from `value`.
```

### `<DataSlice>.get_attr` {#<DataSlice>.get_attr}

``` {.no-copy}
Gets attribute `attr_name` where missing items are filled from `default`.

Args:
  attr_name: name of the attribute to get.
  default: optional default value to fill missing items.
           Note that this value can be fully omitted.
```

### `<DataSlice>.get_bag` {#<DataSlice>.get_bag}

``` {.no-copy}
Returns the attached DataBag.
```

### `<DataSlice>.get_dtype(self)` {#<DataSlice>.get_dtype}

*No description*

### `<DataSlice>.get_item_schema(self)` {#<DataSlice>.get_item_schema}

*No description*

### `<DataSlice>.get_itemid(self)` {#<DataSlice>.get_itemid}

*No description*

### `<DataSlice>.get_key_schema(self)` {#<DataSlice>.get_key_schema}

*No description*

### `<DataSlice>.get_keys` {#<DataSlice>.get_keys}

``` {.no-copy}
Returns keys of all dicts in this DataSlice.
```

### `<DataSlice>.get_ndim(self)` {#<DataSlice>.get_ndim}

*No description*

### `<DataSlice>.get_obj_schema(self)` {#<DataSlice>.get_obj_schema}

*No description*

### `<DataSlice>.get_present_count(self)` {#<DataSlice>.get_present_count}

*No description*

### `<DataSlice>.get_schema` {#<DataSlice>.get_schema}

``` {.no-copy}
Returns a schema DataItem with type information about this DataSlice.
```

### `<DataSlice>.get_shape` {#<DataSlice>.get_shape}

``` {.no-copy}
Returns the shape of the DataSlice.
```

### `<DataSlice>.get_size(self)` {#<DataSlice>.get_size}

*No description*

### `<DataSlice>.get_value_schema(self)` {#<DataSlice>.get_value_schema}

*No description*

### `<DataSlice>.get_values` {#<DataSlice>.get_values}

``` {.no-copy}
Returns values of all dicts in this DataSlice.
```

### `<DataSlice>.has_attr(self, attr_name)` {#<DataSlice>.has_attr}

*No description*

### `<DataSlice>.internal_as_arolla_value` {#<DataSlice>.internal_as_arolla_value}

``` {.no-copy}
Converts primitive DataSlice / DataItem into an equivalent Arolla value.
```

### `<DataSlice>.internal_as_dense_array` {#<DataSlice>.internal_as_dense_array}

``` {.no-copy}
Converts primitive DataSlice to an Arolla DenseArray with appropriate qtype.
```

### `<DataSlice>.internal_as_py` {#<DataSlice>.internal_as_py}

``` {.no-copy}
Returns a Python object equivalent to this DataSlice.

If the values in this DataSlice represent objects, then the returned python
structure will contain DataItems.
```

### `<DataSlice>.internal_register_reserved_class_method_name` {#<DataSlice>.internal_register_reserved_class_method_name}

``` {.no-copy}
Registers a name to be reserved as a method of the DataSlice class.

You must call this when adding new methods to the class in Python.

Args:
  method_name: (str)
```

### `<DataSlice>.is_any_schema` {#<DataSlice>.is_any_schema}

``` {.no-copy}
Returns present iff this DataSlice is ANY Schema.
```

### `<DataSlice>.is_dict` {#<DataSlice>.is_dict}

``` {.no-copy}
Returns present iff this DataSlice contains only dicts.
```

### `<DataSlice>.is_dict_schema` {#<DataSlice>.is_dict_schema}

``` {.no-copy}
Returns present iff this DataSlice is a Dict Schema.
```

### `<DataSlice>.is_empty` {#<DataSlice>.is_empty}

``` {.no-copy}
Returns present iff this DataSlice is empty.
```

### `<DataSlice>.is_entity_schema` {#<DataSlice>.is_entity_schema}

``` {.no-copy}
Returns present iff this DataSlice represents an Entity Schema.

Note that the Entity schema includes List and Dict schemas.

Returns:
  Present iff this DataSlice represents an Entity Schema.
```

### `<DataSlice>.is_itemid_schema` {#<DataSlice>.is_itemid_schema}

``` {.no-copy}
Returns present iff this DataSlice is ITEMID Schema.
```

### `<DataSlice>.is_list` {#<DataSlice>.is_list}

``` {.no-copy}
Returns present iff this DataSlice contains only lists.
```

### `<DataSlice>.is_list_schema` {#<DataSlice>.is_list_schema}

``` {.no-copy}
Returns present iff this DataSlice is a List Schema.
```

### `<DataSlice>.is_mutable` {#<DataSlice>.is_mutable}

``` {.no-copy}
Returns present iff the attached DataBag is mutable.
```

### `<DataSlice>.is_primitive(self)` {#<DataSlice>.is_primitive}

*No description*

### `<DataSlice>.is_primitive_schema` {#<DataSlice>.is_primitive_schema}

``` {.no-copy}
Returns present iff this DataSlice is a primitive (scalar) Schema.
```

### `<DataSlice>.list_size(self)` {#<DataSlice>.list_size}

*No description*

### `<DataSlice>.maybe(self, attr_name)` {#<DataSlice>.maybe}

*No description*

### `<DataSlice>.no_bag` {#<DataSlice>.no_bag}

``` {.no-copy}
Returns a copy of DataSlice without DataBag.
```

### `<DataSlice>.no_db` {#<DataSlice>.no_db}

``` {.no-copy}
Returns a copy of DataSlice without DataBag.
```

### `<DataSlice>.qtype` {#<DataSlice>.qtype}

``` {.no-copy}
QType of the stored value.
```

### `<DataSlice>.ref(self)` {#<DataSlice>.ref}

*No description*

### `<DataSlice>.repeat(self, sizes)` {#<DataSlice>.repeat}

*No description*

### `<DataSlice>.reshape(self, shape)` {#<DataSlice>.reshape}

*No description*

### `<DataSlice>.select(self, fltr)` {#<DataSlice>.select}

*No description*

### `<DataSlice>.select_items(self, fltr)` {#<DataSlice>.select_items}

*No description*

### `<DataSlice>.select_keys(self, fltr)` {#<DataSlice>.select_keys}

*No description*

### `<DataSlice>.select_present(self)` {#<DataSlice>.select_present}

*No description*

### `<DataSlice>.select_values(self, fltr)` {#<DataSlice>.select_values}

*No description*

### `<DataSlice>.set_attr` {#<DataSlice>.set_attr}

``` {.no-copy}
Sets an attribute `attr_name` to `value`.
```

### `<DataSlice>.set_attrs` {#<DataSlice>.set_attrs}

``` {.no-copy}
Sets multiple attributes on an object / entity.

Args:
  update_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.
```

### `<DataSlice>.set_schema` {#<DataSlice>.set_schema}

``` {.no-copy}
Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
```

### `<DataSlice>.shallow_clone(self, itemid, schema, **overrides)` {#<DataSlice>.shallow_clone}

*No description*

### `<DataSlice>.stub(self)` {#<DataSlice>.stub}

*No description*

### `<DataSlice>.take(self, indices)` {#<DataSlice>.take}

*No description*

### `<DataSlice>.to_py(ds, max_depth, obj_as_dict, include_missing_attrs)` {#<DataSlice>.to_py}

``` {.no-copy}
Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    obj_as_dict: Whether to convert objects to python dicts. By default objects
      are converted to automatically constructed 'Obj' dataclass instances.
    include_missing_attrs: whether to include attributes with None value in
      objects.
```

### `<DataSlice>.to_pytree(ds, max_depth, include_missing_attrs)` {#<DataSlice>.to_pytree}

``` {.no-copy}
Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.
  Objects are converted to Python dicts.

  Same as kd.to_py(..., obj_as_dict=True)

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    include_missing_attrs: whether to include attributes with None value in
      objects.
```

### `<DataSlice>.updated(self, *db)` {#<DataSlice>.updated}

*No description*

### `<DataSlice>.with_attrs(self, **attrs)` {#<DataSlice>.with_attrs}

*No description*

### `<DataSlice>.with_bag` {#<DataSlice>.with_bag}

``` {.no-copy}
Returns a copy of DataSlice with DataBag `db`.
```

### `<DataSlice>.with_db` {#<DataSlice>.with_db}

``` {.no-copy}
Returns a copy of DataSlice with DataBag `db`.
```

### `<DataSlice>.with_dict_update(self, *args, **kwargs)` {#<DataSlice>.with_dict_update}

*No description*

### `<DataSlice>.with_merged_bag(self)` {#<DataSlice>.with_merged_bag}

*No description*

### `<DataSlice>.with_name(obj, name)` {#<DataSlice>.with_name}

``` {.no-copy}
Checks that the `name` is a string and returns `obj` unchanged.

  This method is useful in tracing workflows: when tracing, we will assign
  the given name to the subexpression computing `obj`. In eager mode, this
  method is effectively a no-op.

  Args:
    obj: Any object.
    name: The name to be used for this sub-expression when tracing this code.
      Must be a string.

  Returns:
    obj unchanged.
```

### `<DataSlice>.with_schema` {#<DataSlice>.with_schema}

``` {.no-copy}
Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
```

### `<DataSlice>.with_schema_from_obj(self)` {#<DataSlice>.with_schema_from_obj}

*No description*

### `<DataSlice>.db` {#<DataSlice>.db}

``` {.no-copy}
A DataBag associated with DataSlice.
```

</section>

## `DataBag` methods {#DataBag_category}

`DataBag` is a set of triples (Entity.Attribute => Value).

<section class="zippy open">

**Operators**

### `<DataBag>.adopt` {#<DataBag>.adopt}

``` {.no-copy}
Adopts all data reachable from the given slice into this DataBag.

Args:
  slice: DataSlice to adopt data from.

Returns:
  The DataSlice with this DataBag (including adopted data) attached.
```

### `<DataBag>.concat_lists(self, *lists)` {#<DataBag>.concat_lists}

``` {.no-copy}
Returns a DataSlice of Lists concatenated from the List items of `lists`.

  Each input DataSlice must contain only present List items, and the item
  schemas of each input must be compatible. Input DataSlices are aligned (see
  `kde.align`) automatically before concatenation.

  If `lists` is empty, this returns a single empty list.

  The specified `db` is used to create the new concatenated lists, and is the
  DataBag used by the result DataSlice. If `db` is not specified, a new DataBag
  is created for this purpose.

  Args:
    *lists: the DataSlices of Lists to concatenate
    db: optional DataBag to populate with the result

  Returns:
    DataSlice of concatenated Lists
```

### `<DataBag>.contents_repr` {#<DataBag>.contents_repr}

``` {.no-copy}
Returns a string representation of the contents of this DataBag.
```

### `<DataBag>.dict(self, items_or_keys, values, *, key_schema, value_schema, schema, itemid)` {#<DataBag>.dict}

``` {.no-copy}
Creates a Koda dict.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) a Python dict whose keys are either primitives or DataItems and values
       are primitives, DataItems, Python list/dict which can be converted to a
       List/Dict DataItem, or a DataSlice which can folded into a List DataItem:
       a single dict
    3) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict({1: 2, 3: 4}) -> returns a single new dict
  dict({1: [1, 2]}) -> returns a single dict, mapping 1->List[1, 2]
  dict({1: kd.slice([1, 2])}) -> returns a single dict, mapping 1->List[1, 2]
  dict({db.uuobj(x=1, y=2): 3}) -> returns a single dict, mapping uuid->3
  dict(kd.slice([1, 2]), kd.slice([3, 4])) -> returns a dict, mapping 1->3 and
  2->4
  dict(kd.slice([[1], [2]]), kd.slice([3, 4])) -> returns two dicts, one
  mapping
    1->3 and another mapping 2->4
  dict('key', 12) -> returns a single dict mapping 'key'->12

  Args:
    items_or_keys: a Python dict in case of items and a DataSlice in case of
      keys.
    values: a DataSlice. If provided, `items_or_keys` must be a DataSlice as
      keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dict.
```

### `<DataBag>.dict_like(self, shape_and_mask_from, items_or_keys, values, *, key_schema, value_schema, schema, itemid)` {#<DataBag>.dict_like}

``` {.no-copy}
Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to shape_and_mask_from
  shape, or one dimension higher.

  Args:
    self: the DataBag.
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired dicts.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar
      shape_and_mask_from.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
```

### `<DataBag>.dict_schema` {#<DataBag>.dict_schema}

``` {.no-copy}
Returns a dict schema from the schemas of the keys and values
```

### `<DataBag>.dict_shaped(self, shape, items_or_keys, values, *, key_schema, value_schema, schema, itemid)` {#<DataBag>.dict_shaped}

``` {.no-copy}
Creates new Koda dicts with the given shape.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    self: the DataBag.
    shape: the desired shape.
    items_or_keys: either a Python dict (if `values` is None) or a DataSlice
      with keys. The Python dict case is supported only for scalar shape.
    values: a DataSlice of values, when `items_or_keys` represents keys.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: The schema to use for the newly created Dict. If specified, then
        key_schema and value_schema must not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
```

### `<DataBag>.empty` {#<DataBag>.empty}

``` {.no-copy}
Returns an empty DataBag.
```

### `<DataBag>.fingerprint` {#<DataBag>.fingerprint}

``` {.no-copy}
Unique identifier of the value.
```

### `<DataBag>.fork` {#<DataBag>.fork}

``` {.no-copy}
Returns a newly created DataBag with the same content as self.

Changes to either DataBag will not be reflected in the other.

Args:
  mutable: If true (default), returns a mutable DataBag. If false, the DataBag
    will be immutable.
Returns:
  data_bag.DataBag
```

### `<DataBag>.freeze(self)` {#<DataBag>.freeze}

``` {.no-copy}
Returns a frozen DataBag equivalent to `self`.
```

### `<DataBag>.get_fallbacks` {#<DataBag>.get_fallbacks}

``` {.no-copy}
Returns the list of fallback DataBags in this DataBag.

The list will be empty if the DataBag does not have fallbacks.
```

### `<DataBag>.implode(self, x, ndim)` {#<DataBag>.implode}

``` {.no-copy}
Implodes a Dataslice `x` a specified number of times.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  A single list implosion is equivalent to `kd.list(x, db)`.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  The specified `db` is used to create any new Lists, and is the DataBag of the
  result DataSlice. If `db` is not specified, a new, empty DataBag is created
  for this purpose.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    db: optional DataBag where Lists are created from

  Returns:
    DataSlice of nested Lists
```

### `<DataBag>.is_mutable` {#<DataBag>.is_mutable}

``` {.no-copy}
Returns present iff this DataBag is mutable.
```

### `<DataBag>.list(self, items, *, item_schema, schema, itemid)` {#<DataBag>.list}

``` {.no-copy}
Creates list(s) by collapsing `items`.

  If there is no argument, returns an empty Koda List.
  If the argument is a DataSlice, creates a slice of Koda Lists.
  If the argument is a Python list, creates a nested Koda List.

  Examples:
  list() -> a single empty Koda List
  list([1, 2, 3]) -> Koda List with items 1, 2, 3
  list(kd.slice([1, 2, 3])) -> (same as above) Koda List with items 1, 2, 3
  list([[1, 2, 3], [4, 5]]) -> nested Koda List [[1, 2, 3], [4, 5]]
  list(kd.slice([[1, 2, 3], [4, 5]]))
    -> 1-D DataSlice with 2 lists [1, 2, 3], [4, 5]

  Args:
    items: The items to use. If not specified, an empty list of OBJECTs will be
      created.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the list/lists.
```

### `<DataBag>.list_like(self, shape_and_mask_from, items, *, item_schema, schema, itemid)` {#<DataBag>.list_like}

``` {.no-copy}
Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the
      desired lists.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
```

### `<DataBag>.list_schema` {#<DataBag>.list_schema}

``` {.no-copy}
Returns a list schema from the schema of the items
```

### `<DataBag>.list_shaped(self, shape, items, *, item_schema, schema, itemid)` {#<DataBag>.list_shaped}

``` {.no-copy}
Creates new Koda lists with the given shape.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not
      given, the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
```

### `<DataBag>.merge_fallbacks` {#<DataBag>.merge_fallbacks}

``` {.no-copy}
Returns a new DataBag with all the fallbacks merged.
```

### `<DataBag>.merge_inplace(self, other_bags, *, overwrite, allow_data_conflicts, allow_schema_conflicts)` {#<DataBag>.merge_inplace}

``` {.no-copy}
Copies all data from `other_bags` to this DataBag.

  Args:
    other_bags: Either a DataBag or a list of DataBags to merge into the current
      DataBag.
    overwrite: In case of conflicts, whether the new value (or the rightmost of
      the new values, if multiple) should be used instead of the old value. Note
      that this flag has no effect when allow_data_conflicts=False and
      allow_schema_conflicts=False. Note that db1.fork().inplace_merge(db2,
      overwrite=False) and db2.fork().inplace_merge(db1, overwrite=True) produce
      the same result.
    allow_data_conflicts: Whether we allow the same attribute to have different
      values in the bags being merged. When True, the overwrite= flag controls
      the behavior in case of a conflict. By default, both this flag and
      overwrite= are True, so we overwrite with the new values in case of a
      conflict.
    allow_schema_conflicts: Whether we allow the same attribute to have
      different types in an explicit schema. Note that setting this flag to True
      can be dangerous, as there might be some objects with the old schema that
      are not overwritten, and therefore will end up in an inconsistent state
      with their schema after the overwrite. When True, overwrite= flag controls
      the behavior in case of a conflict.

  Returns:
    self, so that multiple DataBag modifications can be chained.
```

### `<DataBag>.new` {#<DataBag>.new}

``` {.no-copy}
Creates Entities with given attrs.

Args:
  arg: optional Python object to be converted to an Entity.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
    itemid will only be set when the args is not a primitive or primitive slice
    if args present.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `<DataBag>.new_like` {#<DataBag>.new_like}

``` {.no-copy}
Creates new Entities with the shape and sparsity from shape_and_mask_from.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `<DataBag>.new_schema` {#<DataBag>.new_schema}

``` {.no-copy}
Creates new schema object with given types of attrs.
```

### `<DataBag>.new_shaped` {#<DataBag>.new_shaped}

``` {.no-copy}
Creates new Entities with the given shape.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  schema: optional DataSlice schema. If not specified, a new explicit schema
    will be automatically created based on the schemas of the passed **attrs.
    Pass schema=kd.ANY to avoid creating a schema and get a slice with kd.ANY
    schema instead.
  update_schema: if schema attribute is missing and the attribute is being set
    through `attrs`, schema is successfully updated.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `<DataBag>.obj` {#<DataBag>.obj}

``` {.no-copy}
Creates new Objects with an implicit stored schema.

Returned DataSlice has OBJECT schema.

Args:
  arg: optional Python object to be converted to an Object.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    itemid will only be set when the args is not a primitive or primitive slice
    if args presents.
  **attrs: attrs to set on the returned object.

Returns:
  data_slice.DataSlice with the given attrs and kd.OBJECT schema.
```

### `<DataBag>.obj_like` {#<DataBag>.obj_like}

``` {.no-copy}
Creates Objects with shape and sparsity from shape_and_mask_from.

Returned DataSlice has OBJECT schema.

Args:
  shape_and_mask_from: DataSlice, whose shape and sparsity the returned
    DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  db: optional DataBag where entities are created.
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `<DataBag>.obj_shaped` {#<DataBag>.obj_shaped}

``` {.no-copy}
Creates Objects with the given shape.

Returned DataSlice has OBJECT schema.

Args:
  shape: JaggedShape that the returned DataSlice will have.
  itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
  **attrs: attrs to set in the returned Entity.

Returns:
  data_slice.DataSlice with the given attrs.
```

### `<DataBag>.qtype` {#<DataBag>.qtype}

``` {.no-copy}
QType of the stored value.
```

### `<DataBag>.uu` {#<DataBag>.uu}

``` {.no-copy}
Creates an item whose ids are uuid(s) with the set attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uu(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uu(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

If 'schema' is provided, the resulting DataSlice has the provided schema.
Otherwise, uses the corresponding uuschema instead.

Args:
  seed: (str) Allows different item(s) to have different ids when created
    from the same inputs.
  schema: schema for the resulting DataSlice
  update_schema: if true, will overwrite schema attributes in the schema's
    corresponding db from the argument values.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
```

### `<DataBag>.uu_schema` {#<DataBag>.uu_schema}

``` {.no-copy}
Creates new uuschema from given types of attrs.
```

### `<DataBag>.uuobj` {#<DataBag>.uuobj}

``` {.no-copy}
Creates object(s) whose ids are uuid(s) with the provided attributes.

In order to create a different "Type" from the same arguments, use
`seed` key with the desired value, e.g.

kd.uuobj(seed='type_1', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

and

kd.uuobj(seed='type_2', x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))

have different ids.

Args:
  seed: (str) Allows different uuobj(s) to have different ids when created
    from the same inputs.
  **kwargs: key-value pairs of object attributes where values are DataSlices
    or can be converted to DataSlices using kd.new.

Returns:
  data_slice.DataSlice
```

### `<DataBag>.with_name(obj, name)` {#<DataBag>.with_name}

``` {.no-copy}
Checks that the `name` is a string and returns `obj` unchanged.

  This method is useful in tracing workflows: when tracing, we will assign
  the given name to the subexpression computing `obj`. In eager mode, this
  method is effectively a no-op.

  Args:
    obj: Any object.
    name: The name to be used for this sub-expression when tracing this code.
      Must be a string.

  Returns:
    obj unchanged.
```

</section>
