<!-- go/markdown -->

# Quick Recipes

This guide covers a comprehensive list of useful recipes for common tasks.

* TOC
{:toc}

## Use `kd.from_py` as a universal convertor

[`kd.from_py`](api_reference.md#kd.from_py) can be used as a universal convertor
to create all types of objects including primitives, entities, lists and dicts.

When inputs are Python primitives, `kd.from_py` works the same as
[`kd.item`](api_reference.md#kd.slices.item) and creates corresponding Koda
items. When inputs are lists, dicts or dataclass instances, `kd.from_py` works
similar to [`kd.list`](api_reference.md#kd.list),
[`kd.dict`](api_reference.md#kd.dicts.new) or
[`kd.new`](api_reference.md#kd.entities.new) but creates objects.

```py
# TODO: update this when from_py returns OBJECT schema by default
# Note the schema is INT32 rather than OBJECT
kd.from_py(1)  # DataItem(1, schema: INT32)
kd.from_py(1.2)  # DataItem(1.2, schema: FLOAT32)
kd.from_py('a')  # DataItem('a', schema: STRING)
kd.from_py(True)  # DataItem(True, schema: BOOLEAN)

kd.from_py(a=1, b='2')  # DataItem(Obj(a=1, b='2'), schema: OBJECT)
kd.from_py([1, 2, 3])  # DataItem(List[1, 2, 3], schema: OBJECT)
kd.from_py({'a': 1, 'b': 2})  # DataItem(Dict{'a'=1, 'b'=2}, schema: OBJECT)

import dataclasses

@dataclasses.dataclass
class PyObj:
  x: int
  y: float

kd.from_py(PyObj(x=1, y=2.0))  # DataItem(Obj(x=1, y=2.0), schema: OBJECT)
```

When inputs are Koda entities, lists or dicts, `kd.from_py` embeds DataSlice
schemas into entities, lists or dicts to create corresponding objects.

```py
kd.from_py(kd.new(a=1, b='2'))  # DataItem(Obj(a=1, b='2'), schema: OBJECT)
kd.from_py(kd.list([1, 2, 3]))  # DataItem(List[1, 2, 3], schema: OBJECT)
kd.from_py(kd.dict({'a': 1, 'b': 2}))  # DataItem(Dict{'a'=1, 'b'=2}, schema: OBJECT)
```

NOTE: Objects created through the schema embedding share the same schema whereas
objects created directly from [`kd.obj()`](api_reference.md#kd.objs.new) have
different embedded schemas. See
[link](common_pitfalls.md#kd_obj_vs_kd_obj_kd_new) for details.

When inputs are Koda objects, `kd.from_py` is a no-op.

```py
obj = kd.obj(a=1, b='2')
kd.from_py(obj)  # no-op, just returns obj
```

`kd.from_py` even accepts inputs with mixed primitives, lists, dicts, entities,
as long as each item can be converted to an object. All intermediate items are
converted to objects.

```py
obj1 = kd.from_py([1, [1, 2]])
obj1 # DataItem(List[1, List[1, 2]], schema: OBJECT)
obj1[0]  # DataItem(1, schema: OBJECT)
obj1[1]  # DataItem(List[1, 2], schema: OBJECT)

obj2 = kd.from_py([kd.new(a=1), {1: 2}])
obj2  # DataItem(List[Obj(a=1), Dict{1=2}], schema: OBJECT)
obj2[0]  # DataItem(Obj(a=1), schema: OBJECT)
obj2[1]  # DataItem(Dict{1=2}, schema: OBJECT)

obj3 = kd.from_py({'a': [1, 2], 'b': kd.obj(a=1)})
obj3  # DataItem(Dict{'b'=Obj(a=1), 'a'=List[1, 2]}, schema: OBJECT)
obj3['a']  # DataItem(List[1, 2], schema: OBJECT)
obj3['b']  # DataItem(Obj(a=1), schema: OBJECT)
```

The most useful use case of `kd.from_py` is to convert inputs with different
schemas into objects so that they can be mixed into one DataSlice.

```py
# It fails due to incomptabile schemas
# kd.slice([kd.new(a=1), kd.list([1, 2]), kd.dict({1: 3})])

# We need to wrap everything with kd.from_py to create objects
# We use kd.obj(a=1) rather than kd.from_py(a=1)
# because kd.from_py does not support keyword arguments
kd.slice([kd.obj(a=1), kd.from_py([1, 2]), kd.from_py({1: 3})])
```

## Use `kd.from_py(py_list, from_dim=)` to convert py_list dimensions to DataSlice dimensions

[`kd.list(py_list)`](api_reference.md#kd.list) converts the Python list
structure to corresponding Koda list structure and the result is a list
DataItem. [`kd.slice(py_list)`](api_reference.md#kd.slices.slice) converts the
Python list structure to the jagged shape of the result DataSlice.

What if we want to control what gets converted to Koda lists and what gets
converted to a jagged shape?
[`kd.from_py(py_list, from_dim=)`](api_reference.md#kd.from_py) allows us to do
that. The first `from_dim` dimensions of `py_list` get converted to DataSlice
jagged shape while remaining dimensions get converted to Koda lists.

```py
# Specify from_dim
kd.from_py([[1, 2], [3, 4]], from_dim=0)
# DataItem(List[List[1, 2], List[3, 4]], schema: OBJECT)

# Specify both from_dim and schema
kd.from_py([[1, 2], [3, 4]], from_dim=1, schema=kd.list_schema(kd.INT64))
# DataSlice([List[1, 2], List[3, 4]], schema: LIST[INT64], ndims: 1, size: 2)

kd.from_py([[1, 2], [3, 4]], from_dim=2)
# DataSlice([[1, 2], [3, 4]], schema: INT32, ndims: 2, size: 4)
```

## Find the first present item in a DataSlice

NOTE: It works no matter what shape (0D, 1D, 2D+) the DataSlice has.

```py
def first_present(x):
  return x.flatten().select_present().S[0]

first_present(kd.slice([[1, 2, 3], [4, 5]]))  # 1
first_present(kd.item(6))  # 6
first_present(kd.slice([ None, None, 3, 4]))  # 3

# Returns missing if no present item is found
first_present(kd.slice([1, 2, 3, 4]) & kd.missing)  # DataItem(None, schema: INT32)
```

## Convert a DataSlice with a jagged shape into one with a uniform shape

Suppose we have the following DataSlice with a jagged shape and we want to
convert it to a DataSlice with 3x4 uniform shape.

```py
x = kd.slice([[1, 2],
              [3, 4, 5],
              [6, 7, 8, 9]])
```

A naive attempt would be `x.S[:3, :4]`. However, unfortunately, it does not work
and returns a DataSlice which is the same as `x` because it does not **pad**
empty spaces with missing items. A correct way would be as follows.

```py
i = kd.range(3)
j = kd.tile(kd.range(4), kd.shapes.new(3))
x.S[i, j]
# [[1, 2, None, None],
#  [3, 4, 5,    None],
#  [6, 7, 8,    9]]
```

If we want to pad with a default value, we can simply add `x.S[i, j] |
default_value`.

We can even generalize it into a function working for any shape.

```py
def pad(x, shape, default_value=None):
  indices = []
  for i, dim in enumerate(shape):
    indices.append(kd.tile(kd.range(dim), kd.shapes.new(*shape[:i])))
  res = x.S[*indices]
  if default_value is not None:
    res = res | default_value
  return res

pad(x, (3, 4))
pad(x, (2, 5), 0)
# [[1, 2, 0, 0, 0],
#  [3, 4, 5, 0, 0]]
```

## Transpose a DataSlice as a matrix

Suppose we have the following DataSlice with a uniform 2D shape and we want to
transpose it by swapping axis 0 and axis 1.

```py
x = kd.slice([[1, 2, 3, 4],
              [5, 6, 7, 8],
              [9, 10, 11, 12]])

i = kd.range(4)
j = kd.tile(kd.range(3), kd.shapes.new(4))
x.S[j, i]
# [[1, 5, 9],
#  [2, 6, 10],
#  [3, 7, 11],
#  [4, 8, 12]]
```

What if the DataSlice has more than two dimensions? Koda DataSlices are not
designed to be manipulated as matrices. Performing transposition in Koda is
complicated and confusing. The shape of a DataSlice is designed to represent
data hierarchy. In the long run, Koda plans to support tensors as a primitive
data type and related matrix operations. For now, however, it is much easier and
potentially faster to delegate the work to other libraries (e.g. Numpy) for
matrix operation.

```py
x = kd.range(24).reshape(kd.shapes.new(3, 4, 2))
# [[[0, 1], [2, 3], [4, 5], [6, 7]],
#  [[8, 9], [10, 11], [12, 13], [14, 15]],
#  [[16, 17], [18, 19], [20, 21], [22, 23]]]

arr = kd_ext.npkd.to_array(x)
transposed_arr = np.transpose(arr, (2, 1, 0))
transposed_x = kd_ext.npkd.from_array(transposed_arr)
# [[[0, 8, 16], [2, 10, 18], [4, 12, 20], [6, 14, 22]],
#  [[1, 9, 17], [3, 11, 19], [5, 13, 21], [7, 15, 23]]]
```

<section class='zippy'>

Optional: What if you really want to know how to do it in Koda?

First, we need to understand transposition works. Suppose we want to swap axes
by `(2, 1, 0)`. That is, swap the first and third axes. The result
`transposed_x` should satisfy the condition `transposed_x.S[k, j, i] = x.S[i, j,
k]`. Thus we can have the following code.

```py
k = kd.range(2)
j = kd.tile(kd.range(4), kd.shapes.new(2))
i = kd.tile(kd.range(3), kd.shapes.new(2, 4))
x.S[i, j, k]
```

</section>

## Create a sliding window

Suppose we want to add a dimension to a DataSlice by moving a sliding window
across the last dimension and selecting items in the window.

```py
x = kd.slice([[1, 2, 3],
              [4, 5, 6, 7],
              [8, 9, 10, 11, 12]])

def slide_window(x, size):
  indices = kd.index(x)
  return x.S[indices: indices + size]

slide_window(x, 2)
# [[[1, 2], [2, 3], [3]],
#  [[4, 5], [5, 6], [6, 7], [7]],
#  [[8, 9], [9, 10], [10, 11], [11, 12], [12]]]
```

What if we want to add paddings?

```py
# To understand how it works, you can run the following code line by line.
def slide_window2(x, size):
  num_to_pad = size - 1

  # Pad heads and tails across the last dimension with missing items
  padded_value = kd.item(None, x.get_schema())
  padded_value = kd.expand_to_shape(padded_value, x.get_shape()[:-1]).repeat(num_to_pad)
  padded_x = kd.concat(padded_value, x, padded_value)

  # Calculate the indices for subslicing
  full_ds_for_shape = kd.present_shaped_as(padded_x)
  indices = kd.index(full_ds_for_shape)
  indices = indices.S[:kd.agg_size(x) + num_to_pad]
  return padded_x.S[indices: indices + size]

slide_window2(x, 3)
# [[[None, None, 1], [None, 1, 2], [1, 2, 3], [2, 3, None], [3, None, None]],
#  [[None, None, 4], [None, 4, 5], [4, 5, 6], [5, 6, 7], [6, 7, None], [7, None, None]],
#  [[None, None, 8], [None, 8, 9], [8, 9, 10], [9, 10, 11], [10, 11, 12], [11, 12, None], [12, None, None]]]
```

## Add a dimension by accumulating items up to current position

Suppose we want to add a dimension to a DataSlice and each item in the original
DataSlice has child items as items accumulating up to its position in the new
dimension. For example,

```py
x = kd.slice([[1, 2],
              [3, 4, 5],
              [6, 7, 8, 9]])

indices = kd.range(kd.index(x) + 1)
# [[[0], [0, 1]],
#  [[0], [0, 1], [0, 1, 2]],
#  [[0], [0, 1], [0, 1, 2], [0, 1, 2, 3]]]

x.S[indices]
# [[[1], [1, 2]],
#  [[3], [3, 4], [3, 4, 5]],
#  [[6], [6, 7], [6, 7, 8], [6, 7, 8, 9]]]

# It can be even simplified as
x.S[:kd.index(x) + 1]
```

When `x` is sparse, we need to decide if child items corresponding to missing
items in the new dimension should be empty or not.

```py
x = kd.slice([[1, 2],
              [3, None, 5],
              [6, None, None, 9]])

# By default, missing items reuslt in empty child dimensions
x.S[:kd.index(x) + 1]
# [[[1], [1, 2]],
#  [[3], [], [3, None, 5]],
#  [[6], [], [], [6, None, None, 9]]]

# We need to make 'x' full by adding a default value before calling kd.index
x.S[:kd.index(x | 0) + 1]
# [[[1], [1, 2]],
#  [[3], [3, None], [3, None, 5]],
#  [[6], [6, None], [6, None, None], [6, None, None, 9]]]

# If no good default value can be used, we can also do this
x.S[:kd.index(kd.present_shaped_as(x)) + 1]
```

## Implement cumulative operators using aggregational operators

Koda provides native cumulative operators (e.g.
[`kd.math.cum_sum`](api_reference.md#kd.math.cum_sum),
[`kd.math.cum_count`](api_reference.md#kd.slices.cum_count)) for common
operations. However, Koda does not provide a corresponding cumulative version
for every aggregational operators (e.g.
[`kd.strings.agg_join`](api_reference.md#kd.strings.agg_join)).

Support we want to implement a cumulative operator using an aggregational
operator. We can do it in two steps. First, add a new dimension by accumulating
items up to current position for each item in the last dimension. Then pass it
to the aggregational operator.

NOTE: Cumulative operators implemented this way has `O(N^2)` time complexity
whereas native version has `O(N)` time complexity.

```py
x = kd.slice([[1, 2],
              [3, None, 5],
              [6, None, None, 9]])

# Native version
kd.math.cum_sum(x)  # [[1, 3], [3, None, 8], [6, None, None, 15]]

# Note the items corresponding to missing items are 0 rather than missing
kd.agg_sum(x.S[:kd.index(x) + 1])  # [[1, 3], [3, 0, 8], [6, 0, 0, 15]]
# We need to mask by x's presence
kd.agg_sum(x.S[:kd.index(x) + 1]) & kd.has(x)  # [[1, 3], [3, None, 8], [6, None, None, 15]]

# We can also customize the behavior for missing items
kd.agg_sum(x.S[:kd.index(x | 0) + 1])  # [[1, 3], [3, 3, 8], [6, 6, 6, 15]]
```

## Changing the shape of DataSlices

Koda provides several ways to change the shape of existing DataSlices without
modifying their content. The two most common ones are
[`kd.flatten`](api_reference.md#kd.shapes.flatten) (merges adjacent dimensions)
and [`kd.reshape`](api_reference.md#kd.shapes.reshape) (attaches a new
JaggedShape without changing the number of items). These operators work by
modifying the DataSlice shapes rather than the data.

Suppose we have a DataSlice with a given shape of ndim `R` and wish to merge `N`
dimensions to create a DataSlice with ndim `R-N+1`, then `kd.flatten` can be
used:

```py
# By default, `kd.flatten` returns a 1-dimensional DataSlice - even for scalars.
kd.flatten(kd.slice([[1, 2], [3]]))  # [1, 2, 3]
kd.slice([[1, 2], [3]]).flatten()  # [1, 2, 3]
kd.item(0).flatten()  # [0]

# One can optionally provide `from_dim` and `to_dim` parameters to specify which
# consecutive dimensions should be merged.
kd.slice([[[1, 2], [3]], [[4], [5, 6]]]).flatten(1, 3)  # [[1, 2, 3], [4, 5, 6]]

# Alternatively, we can use negative values to specify that that last two
# dimension should be merged.
kd.slice([[[1, 2], [3]], [[4], [5, 6]]]).flatten(-2)  # [[1, 2, 3], [4, 5, 6]]

# If `from == to`, a size-1 dimension is inserted at `from_dim`.
kd.slice([1, 2, 3]).flatten(1, 1)  # [[1], [2], [3]]

# This ensures that:
#   `kd.flatten(ds, from, to).get_ndim() == ds.get_ndim() - (to - from) + 1`
# (assuming 0 <= from <= to <= ds.get_ndim()).
```

While `kd.flatten` operates on DataSlices, it's possible to use it in
combination with List implosions and explosions to flatten nested Lists:

```py
list_item = kd.list([[1, 2], [3]])
list_item[:][:].flatten().implode()  # kd.list([1, 2, 3])
```

Suppose instead that we have a DataSlice of size `N` (with arbitrary
dimensionality) that we wish to change the shape of, either by providing a new
JaggedShape (with the same size), or by providing a tuple of per-dimension
sizes. In such cases, `kd.reshape` can be used:

```py
ds = kd.slice([[1, 2], [3]])

# Providing a shape directly.
kd.reshape(ds), kd.shapes.new(3))  # [1, 2, 3]
ds.reshape(kd.shapes.new(3))  # [1, 2, 3]

# Providing a tuple of sizes. Each dimension size can either be a scalar (each
# row has the same number of elements), or a DataSlice if the dimension is
# Jagged.
ds.reshape((3,))  # [1, 2, 3]
ds.reshape((2, kd.slice([1, 2])))  # [[1], [2, 3]]

# One of the dimensions is also allowed to be `-1`, indicating that it should
# resolve to a uniform size (represented by a scalar) inferred from remaining
# dimensions and the size of the input.
ds.reshape((-1, kd.slice([1, 2])))  # [[1], [2, 3]]
kd.slice([1, 2, 3, 4, 5, 6]).reshape((2, kd.slice([1, 2]), -1))  # [[[1, 2]], [[3, 4], [5, 6]]]

# `kd.reshape_as` is a helper operator to reshape one DataSlice to the shape
# of another.
kd.reshape_as(ds, kd.slice([0, 0, 0]))  # [1, 2, 3]
# This is equivalent to
ds.reshape(kd.slice([0, 0, 0]).get_shape())  # [1, 2, 3]
```

NOTE: the old and new shapes must have the same size.

## Manual broadcasting of DataSlices

Koda has well-defined broadcasting rules
(go/koda-fundamentals#broadcasting-and-aligning) where one DataSlice can be
broadcasted to the shape of another if its shape is a prefix of the other shape.
Most of the time, broadcasting is done automatically and allows e.g. the
following to succeed without manual broadcasting:

```py
kd.slice([1, 2]) + kd.slice([[3, 4], [5]])  # [[4, 5], [7]]
```

In some cases, it's useful to perform manual broadcasting through
[`kd.expand_to`](api_reference.md#kd.slices.expand_to) or
[`kd.align`](api_reference.md#kd.slices.align) which allows for more
fine-grained behavior:

```py
# Expanding to another slice using normal broadcasting rules.
kd.slice([1, 2]).expand_to(kd.slice([[0, 0], [0]]))  # [[1, 1], [2]]

# Aligning to the "common shape"
a, b, c = kd.align(kd.item(1), kd.slice([['a', 'b'], ['c']]), kd.slice([1.0, 2.0]))
# a: kd.slice([[1, 1], [1]])
# b: kd.slice([['a', 'b'], ['c']])
# c: kd.slice([[1.0, 1.0], [2.0]])

# By providing `ndim`, we implode the last `ndim` dimensions, expand and then
# explode again. This allows us to implement e.g. cross-products, pairs and
# more:
x = kd.slice([1, 2, 3])
x_expanded = x.expand_to(x, ndim=1)  # [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
kd.zip(x, x_expanded).flatten(0, 2)  # [[1, 1], [1, 2], ..., [3, 2], [3, 3]]
```

## Concatenating DataSlices of different ranks

Koda supports DataSlice concatenation of variable number of inputs through
[`kd.concat`](api_reference.md#kd.slices.concat) (and stacking through
[`kd.stack`](api_reference.md#kd.slices.stack)). Due to the ambiguity explained
below, it is required that all inputs have the same rank and it's up to the
caller to ensure that this is the case.

Suppose we have the following inputs:

```py
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([['a', 'b'], ['c', 'd']])
```

If we call `kd.concat(a, b)`, we may have a couple of different expectations of
what will happen. For example:

```py
1) -> [[[1, 2, 'a', 'a'], [3, 'b']], [[5, 'c'], [7, 8, 'd', 'd']]]
2) -> [[[1, 2, 'a'], [3, 'b']], [[5, 'c'], [7, 8, 'd']]]
3) -> [[[1, 2, 'a', 'b'], [3, 'a', 'b']], [[5, 'c', 'd'], [7, 8, 'c', 'd']]]
```

To achieve these outcomes, we mainly have two tools at our disposal:

*   Aligning all inputs using Koda's standard broadcasting rules.
*   Adding dimensions by repeating data or by reshaping the input.

Expected outcome (1) uses the standard Koda broadcasting rules. `b` is
broadcasted to the shape of `a`, and data is repeated as needed. Here, we can
either use [`kd.align`]((api_reference.md#kd.slices.align) to align all inputs,
or [`kd.expand_to`](api_reference.md#kd.slices.expand_to) directly on specific
inputs:

```py
b_expanded = b.expand_to(a)  # [[['a', 'a'], ['b']], [['c'], ['d', 'd']]]
kd.concat(a, b_expanded)  # [[[1, 2, 'a', 'a'], [3, 'b']], [[5, 'c'], [7, 8, 'd', 'd']]]
# Same as `kd.concat(*kd.align(a, b))`
```

Expected outcome (2) adds a unit dimension to ensure that the ranks are the same
without duplicating data.

```py
b_repeated = b.repeat(1)  # [[['a'], ['b']], [['c'], ['d']]]
kd.concat(a, b_repeated)  # [[[1, 2, 'a'], [3, 'b']], [[5, 'c'], [7, 8, 'd']]]
```

Expected outcome (3) repeats each inner row of `b` once per element in `b`
before concatenating. This can be achieved through `kd.expand_to`:

```py
b_expanded = b.expand_to(b, ndim=1)  # [[['a', 'b'], ['a', 'b']], [['c', 'd'], ['c', 'd']]]
kd.concat(a, b_expanded)  # [[[1, 2, 'a', 'b'], [3, 'a', 'b']], [[5, 'c', 'd'], [7, 8, 'c', 'd']]]
```

## Grouping by keys and computing statistics

The [`kd.group_by`](api_reference.md#kd.slices.group_by) operator is a highly
versatile operator intended to group values based on some identifier, be it a
single one or multiple ones, and facilitate computing statistics, creating
hierarchical data, and can be combined with operators such as
[`kd.translate`](api_reference.md#kd.slices.translate) to perform translations
on groups rather than individual items.

Suppose we wish to find the unique values of a DataSlice, or to obtain a
representative value in one DataSlice for each group of some other Dataslice.
Then `kd.group_by` can be used:

```py
# Finding unique values.
grouped = kd.group_by(kd.slice([1, 2, 3, 1, None, 2]))  # [[1, 1], [2, 2], [3]]
grouped.S[0]  # [1, 2, 3]
# Equivalent to:
kd.unique(kd.slice([1, 2, 3, 1, None, 2]))  # [1, 2, 3]

# Multi-dimensional DataSlices are grouped by the final dimension.
kd.group_by(kd.slice([[1, 2, 1], [3, 1]])).S[0]  # [[1, 2], [3, 1]]

# Finding representative values based on ids of another slice.
values = kd.slice(['a', 'b', 'c', 'd', 'e'])
ids = kd.slice([1, 1, 2, 3, 2])
# `values` are grouped by `ids`.
grouped = kd.group_by(values, ids)  # [['a', 'b'], ['c', 'e'], ['d']]
grouped.S[0]  # ['a', 'c', 'd']

# Finding representative values based on several ids.
values = kd.slice(['a', 'b', 'c', 'd', 'e'])
ids_1 = kd.slice([1, 1, 2, 3, 2])
ids_2 = kd.slice([1, 1, 3, 3, 2])
# `values` are grouped by pairs of `ids_1` and `ids_2`.
grouped = kd.group_by(values, ids_1, ids_2)  # [['a', 'b'], ['c'], ['d'], ['e']]
```

NOTE: [`kd.unique(ds)`](api_reference.md#kd.slices.unique) is a faster and
clearer alternative to `kd.group_by(ds).S[0]` and should be preferred for
computing unique values.

Suppose instead we have a DataSlice of `Books` containing, among other things,
the attributes `year` (specifying the year the book was written) and `pages`
(specifying the number of pages in the book):

```py
Book = kd.named_schema('Book')
books = Book(
  year=kd.slice([1997, 2001, 1928, 1928, 2001]),
  pages=kd.slice([212, 918, 331, 512, 331]),
  ...
)
```

`kd.group_by` allows us to compute statistics based on these attributes, or to
create hierarchical data:

```py
# Group by year
grouped_books = kd.group_by(books, books.year)
# [
#   [Book(pages=212, year=1997)],
#   [Book(pages=918, year=2001), Book(pages=331, year=2001)],
#   [Book(pages=331, year=1928), Book(pages=512, year=1928)],
# ]

# Computing the average page count per year
kd.math.agg_mean(grouped_books.pages)  # [212.0, 624.5, 421.5]
```

## Translating values through key-value mappings

Suppose we have two DataSlices `docs` (representing some document with `id`,
`visits` and `domain`) and a DataSlice `doc_ids` (representing documents of
interest), and we wish to find the visits per document.
[`kd.translate`](api_reference.md#kd.slices.translate) can then be useful:

```py
Doc = kd.named_schema('Doc')
docs = Doc(
  id=kd.slice([0, 1, 2, 3, 4]),
  visits=kd.slice([11, 212, 99, 123, 44]),
  domain=kd.slice(['a', 'b', 'a', 'c', 'd']),
)
doc_ids = kd.slice([1, 9, 0])
kd.translate(doc_ids, docs.id, docs.visits)  # [212, None, 11]
```

Note that this requires `docs.id` to be unique within the final dimension.

If, on the other hand, the keys are not unique, we may still wish to perform a
translation. Suppose, for example, that we are interested in the number of
visits for all documents of a selection of `domains`. Multiple documents may
have the same domain, so `kd.translate` is not appropriate. Instead,
[`kd.translate_group`](api_reference.md#kd.slices.translate_group) can be used:

```py
domains = kd.slice(['a', 'b', 'f'])
visits = kd.translate_group(domains, docs.domain, docs.visits)  # [[11, 99], [212], []]
kd.agg_sum(visits)  # [110, 212, 0]
```

`kd.translate_group` can also be mimicked through a combination of
`kd.translate` and [`kd.group_by`](api_reference.md#kd.slices.group_by), which
is a powerful combination that can be tweaked for more advanced transformations:

```py
groups = kd.group_by(docs, docs.domain)
# [
#   [Doc(domain='a', id=0, visits=11), Doc(domain='a', id=2, visits=99)],
#   [Doc(domain='b', id=1, visits=212)],
#   [Doc(domain='c', id=3, visits=123)],
#   [Doc(domain='d', id=4, visits=44)],
# ]
keys_from = groups.S[0].domain  # ['a', 'b', 'c', 'd']
values_from = groups.visits.implode()  # [List[11, 99], List[212], List[123], List[44]]
visits = kd.translate(domains, keys_from, values_from)  # [List[11, 99], List[212], None]
kd.agg_sum(visits[:])  # [110, 212, 0]
```
