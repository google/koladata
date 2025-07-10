<!-- go/markdown-->

# Technical Deep Dive: JaggedShape

This guide offers a deep dive into `JaggedShape` and is part of a
[Koda Technical Deep Dive](overview.md) series.

* TOC
{:toc}

## JaggedShape

JaggedShapes describe the data partitioning of a DataSlice as a (potentially)
multidimensional and *jagged* partition tree that is representable through a
sequence of size arrays. The number of size arrays corresponds to the *rank*
(i.e. number of dimensions) of the JaggedShape, while the sizes themselves
represent the number of columns for each row. Together with a *flat* array of
data (or a scalar data point in case of scalars), they form a multidimensional
DataSlice.

For example:

```py
x = kd.slice(
  [
    [
      ['a', 'b'],
      ['c']
    ],
    [
      ['d', 'e', 'f']
    ]
  ]
)
```

is a DataSlice with a 3-dimensional JaggedShape. The sizes are `[[2], [2, 1],
[2, 1, 3]]`, indicating that:

*   dim-0 has 1 row with 2 columns.
*   dim-1 has 2 rows, where row-0 has 2 columns, and row-1 has 1 column.
*   dim-2 has 3 rows, where row-0 has 2 columns, row-1 has 1 column, and row-2
    has 3 columns.

NOTE: By convention, *uniform* dimensions (where all column sizes are the same)
are represented with a single scalar. The repr of the above JaggedShape is
therefore `JaggedShape(2, [2, 1], [2, 1, 3])`.

### Broadcasting

Koda differs from most numerical libraries that support multidimensional arrays
in that broadcasting is done based on the *prefix* (lower dimensions) rather
than the *suffix* (higher dimensions) of the shapes.

For a shape `s1` to be considered *broadcastable* or *expandable* to `s2`, we
require that `s1` is a *prefix* of `s2`. For example, in Koda the following is
the case:

```py
ds_1 = kd.slice(['a', 'b'])
ds_2 = kd.slice([['c', 'd', 'e'], ['f', 'g', 'h']])

# Succeeds. `ds_1.get_shape() -> JaggedShape(2)` is a prefix of
# `ds_2.get_shape() -> JaggedShape(2, 3)`.
kd.expand_to_shape(ds_1, ds_2.get_shape()) # kd.slice([['a', 'a', 'a'],
                                           #           ['b', 'b', 'b']])

# Fails. `ds_2.get_shape()` is _not_ a prefix of `ds_1.get_shape()`.
kd.expand_to_shape(ds_2, ds_1.get_shape())

# Fails. `ds_3.get_shape()` is _not_ a prefix of `ds_2.get_shape()`.
ds_3 = kd.slice(['a', 'b', 'c'])
kd.expand_to_shape(ds_3, ds_2.get_shape())
```

NOTE: The *common shape* of a collection of `shapes`, if one exists, is defined
as the shape in `shapes` that all other shapes can be broadcasted to.

In Numpy, on the other hand, a shape `s1` is considered broadcastable to `s2`
only if `s1` is a *suffix* of `s2`. For example:

```py
arr_1 = np.array(['a', 'b'])
arr_2 = np.array([['c', 'd', 'e'], ['f', 'g', 'h']])

# Fails. `arr_1.shape` is _not_ a suffix of `arr_2.shape`.
np.broadcast_to(arr_1, arr_2.shape)

# Succeeds. `arr_3.shape` is a suffix of `arr_2.shape`.
arr_3 = ['a', 'b', 'c']
np.broadcast_to(arr_3, arr_2.shape)  # [['a', 'b', 'c'], ['a', 'b', 'c']]
```

As a consequence of these rules, in Koda the data is broadcasted by repeating
each element for each corresponding partition in the new shape. For
`kd.expand_to_shape(ds_1, ds_2.get_shape())`, `'a'` is repeated once for each
element in `['c', 'd', 'e']`. In Numpy, the entire input is instead repeated for
each corresponding row. For `np.brodcast_to(arr_3, arr_2.shape)`, `['a', 'b',
'c']` is repeated once per row.

The primary motivation is that these broadcast rules more closely adhere to the
Koda data model. A DataSlice is considered a multi-tiered hierarchical
structure, where dimension `i` is a "parent" of dimensions `j` where `i < j`.
Consider a collection of `queries` as a 1d DataSlice, and a collection of
associated documents `docs`, then the broadcasting rules allow us to easily
associate the `queries` with the `docs` in order to do computations:

```py
queries = kd.slice(['query_1', 'query_2'])
docs = kd.slice([['doc_1', 'doc_2'], ['doc_3']])

kd.expand_to(queries, docs)  #  [['query_1', 'query_1'], ['query_2']]
```

The secondary motivation for this difference is technical: in Numpy, all
dimensions are *uniform* and thereby independent of each other - one can freely
change one dimension without affecting the others. For JaggedShapes with
non-uniform dimensions, changing `dim[i]` (or adding prefix dimensions) has a
cascading effect on all `dim[j]`, where `i < j`, as subsequent dimensions are no
longer compatible with the new sizes except in trivial cases. This practically
restricts broadcasting to a prohibitively limited set of cases given how common
jaggedness is in Koda.

NOTE: In Koda, broadcasting normally happens implicitly. For example, `kd.add(x,
y)` produces a result with the *common shape* of `x` and `y`.

### Indexing

Indexing and slicing is done by traversing the dimensions from left to right to
obtain the correct indices in the flattened data. Consider the following
example:

```py
# Has shape: JaggedShape(3, [2, 1, 3])
x = kd.slice(
  [
    ['a', 'b'],
    ['c'],
    ['d', 'e', 'f'],
  ]
)
```

To retrieve `x.S[2, 1]`, we may for illustrative purposes consider the
equivalent (but slower) form `x.S[2, ...].S[1]`:

*   `x.S[2, ...]` returns `kd.slice(['d', 'e', 'f'])` with sizes `[[3]]`.
*   `x.S[2, ...].S[1]` returns `kd.slice('e')` with sizes `[]` - i.e. a scalar.

An observant reader may realize that, to compute the intermediate result of each
level (e.g. `kd.slice(['d', 'e', 'f']))` for `x.S[2, ...]`), we must compute the
`start` and `end` positions in the flattened `data = ['a', 'b', 'c', 'd', 'e',
'f']`. For `x.S[2, ...]`, this is computed through:

```py
# Our index to use.
i = 2
# The sizes of the relevant dimension. Since indexing reduces the
# dimensionality, this is the second dimension.
sizes = [2, 1, 3]

start = sum(sizes[:i])  # -> 3
end = start + sizes[i]  # -> 6
data[start:end]  # -> ['d', 'e', 'f']
```

Note that a linear number of operations is required to compute the start value
from the sizes. This property is shared among many common shape operations, such
as flattening, slicing etc. This is part of the reason why the real
implementation uses *split points* instead of sizes.

### Split Points

The split point representation is simply the cumulative sum of the sizes that we
have previously discussed. For example, `JaggedShape(3, [2, 1, 3])` has split
points `[[0, 3], [0, 2, 3, 6]]`.

We return to above example of `x.S[2, ...]` and show how this is simplified
through split points. For each intermediate step, we have to compute the `start`
and `end` positions of the flattened data in the same manner as with the sizes
implementation. For `x.S[2, ...]`, this is simply:

```py
# Our index to use.
i = 2
# The sizes of the relevant dimension.
split_points = [0, 2, 3, 6]

start = split_points[i]  # -> 3
end = split_points[i + 1]  # -> 6
data[start:end]  # -> ['d', 'e', 'f']
```

The computation involves a few lookups in the split points which is more
efficient than the iteration we saw in the sizes representation.

In short, the split points representation allows us to (e.g. for `split_points =
[0, 2, 3, 6]`):

*   Quickly compute the number of rows for each dimension. This is the same as
    the total number of elements in the previous dimension (if one exists).
    *   `len(split_points) - 1`.
*   Quickly compute the number of columns of each row.
    *   `split_points[i + 1] - split_points[i]`.
*   Quickly compute the total number of elements that are present in each
    dimension. This is the same as the number of rows of the next dimension (if
    one exists).
    *   `split_points[-1]`.
*   Simplify and speed up common operations on shapes such as indexing or
    flattening.

### Edges

The real JaggedShape implementation is represented through a list of *split
point edges*, each representing a dimension. An *edge* is an Arolla abstraction
representing potentially unordered mapping from a parent array to a child array.
Each edge has a `parent_size`, representing the size of the parent, a
`child_size`, representing the size of the child, and a "mapping" from the
parent to the child. For JaggedShapes, the "mapping" is in the form of split
points, which implicitly imposes the restriction that the "mapping" is
monotonically non-decreasing. Additionally, for a JaggedShape with dimensions
`dim`:

*   `dim[0].parent_size() == 1`
*   For all `0 <= i < dim.size() - 1`: `dim[i].child_size() == dim[i +
    1].parent_size()`.

That is, the `parent_size` of each dimension corresponds to the number of
columns in the previous dimension, and the `child_size` corresponds to the
number of rows in the next dimension.

For the 3-dimensional JaggedShape represented by sizes `JaggedShape(2, [2, 1],
[2, 1, 3])`, the implementation looks like:

```py
JaggedShape(
  Edge(split_points=[0, 2], parent_size=1, child_size=2),
  Edge(split_points=[0, 2, 3], parent_size=2, child_size=3),
  Edge(split_points=[0, 2, 3, 6], parent_size=3, child_size=6),
)
```

### Implementation and API

Koda operations are mostly done on slices, and shape modifications during
broadcasting, indexing, and similar are handled automatically. In some cases,
it's still useful to be able to inspect the shape, and to manipulate it
manually. For this, the following functionality is available:

*   `shape = ds.get_shape()` returns the JaggedShape of the DataSlice `ds`.
*   `shape.rank()` returns the rank, i.e. the number of dimensions.
    *   Other methods can be found in the
        [Python implementation](http://py/koladata/types/jagged_shape.py).
*   `kd.shapes.flatten(shape, from_dim=..., to_dim=...)` flattens the shape
    between `from_dim` and `to_dim`.
    *   Other shape operators can be found under the
        [`kd.shapes`](/koladata/g3doc/api_reference.md#kd.shapes)
        section of the Koda API Reference.
