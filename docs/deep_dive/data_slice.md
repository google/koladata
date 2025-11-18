<!-- go/markdown-->

# Technical Deep Dive: DataSlice

This guide offers a deep dive into `DataSlice` and is part of a
[Koda Technical Deep Dive](overview.md) series.

* TOC
{:toc}

## DataSlice

A DataSlice is a fundamental data structure in Koda which represents a
collection of items. It encapsulates not just raw data values but also their
structure, type, and association with a `DataBag` for certain types of data like
Entities. It serves as the primary interface for most operations in the Koda
library. Conceptually, a DataSlice combines several key components:

1.  **Raw Data**: The actual values being represented. Internally, this is
    either a flat array of items for a non-scalar DataSlice, or a single item
    for a scalar DataSlice. A scalar DataSlice is called a DataItem. This
    specialization is also encoded in a sub-type relationship: class DataItem
    inherits from class DataSlice.
2.  **JaggedShape**: Describes the multi-dimensional and jagged partition tree
    of the DataSlice. It dictates how the flattened raw data is structured into
    logical rows and columns across multiple dimensions.
    *   See the [JaggedShape](jagged_shape.md) page for a deep dive into this
        topic.
3.  **Schema**: Describes the data type of the elements within the DataSlice,
    such as `INT32`, `STRING`, or more complex struct schemas such as `LIST`s,
    `DICT`s and Entities. The schema dictates how the data is interpreted and
    what operations are valid.
    *   See the [Schema](schema.md) page for a deep dive into this topic.
4.  **DataBag (optional reference)**: An optional link to a
    [DataBag](data_bag.md). A DataBag is a collection of
    `Entity.Attribute=>Value` triples that represent structured data. The
    `Entity` in a triple can be any `ItemId` (e.g. of a `LIST` or `DICT` or a
    Koda Entity) with which we can associate data. When a DataSlice references a
    DataBag, then the DataBag typically holds the actual structured data for the
    ItemIds mentioned in the DataSlice.
    *   See the [DataBag](data_bag.md) page for a deep dive into this topic.

Examples:

```py
# 2D jagged DataSlice of integers:
ds = kd.slice([[1, 2], [3]])

ds.flatten().to_py()  # Python list: [1, 2, 3]
ds.get_schema()       # INT32
ds.get_bag()          # None - no DataBag reference.
                      # Values of Koda primitives like INT32s are stored inside
                      # the DataSlice, so no DataBag is needed.

# 1D DataSlice of mixed data:
ds = kd.slice(['a', 2])

ds.to_py()       # raw data: ['a', 2]
ds.get_schema()  # OBJECT - specifies that the actual type of the data is given
                 # by the individual items in the slice, and is not a property
                 # of the entire DataSlice.
ds.get_bag()     # None - STRING and INT are Koda primitives, so this behaves
                 # just like INT32 above.
```

DataSlices are most commonly operated on in a vectorized manner, where a
primitive operation is applied to each element (or a collection of elements) in
the larger DataSlice. See the [Operators](#dataslice-operators) section for more
details.

While Koda doesn't use traditional object-oriented subclasses for different data
types within a DataSlice, it leverages the flexible DataSlice container to
represent various data types. The behavior of a DataSlice is heavily influenced
by the Schema of its contained items. For example, an `INT32` DataSlice will
behave differently from a `LIST[STRING]` DataSlice, even though both are
represented by the same `DataSlice` Python class. For some scalar data, there
exists subclasses of `DataItem` such as Lists, Dicts and Schemas.

### DataSlice Internals

The implementation is split across C++ and Python. Implementation details are
listed in this section, with more in-depth exploration of how different types
behave follows in the later section
[Structured Data and DataBag Interaction](#structured-data-and-databag-interaction)
.

#### C++ Internals

The C++ implementation has a DataSlice class with fields that store various
pieces of information, corresponding to the components listed above:

*   Raw Data: a variant of either `internal::DataItem`, representing scalar
    data, or `internal::DataSliceImpl`, representing non-scalar data.
*   Shape: A `JaggedShape` specifying the shape of the DataSlice.
*   Schema: An `internal::DataItem` specifying the data type of the raw data.
*   DataBag: optional reference to an associated bag with structured data.

When `JaggedShape` has rank 0, the data variant is always an
`internal::DataItem`. Otherwise, the data variant is always an
`internal::DataSliceImpl`. Methods on the high-level DataSlice that access data
commonly dispatch to corresponding implementations on the `internal::DataItem`,
the `internal::DataSliceImpl`, or to a DataBag method overload that accepts one
of the two implementations.

Upon construction, the size of the `JaggedShape` is validated to be the same as
the size of the data. The Schema is validated to correspond to the stored data.

The high-level DataSlice has an associated `DATA_SLICE` Arolla QType,
integrating it with the Arolla framework and allowing it to be used as part of
Expressions. For more information about QTypes and Koda schemas, see the
[QTypes and Koda schemas](functors.md#qtypes-and-koda-schemas) section of the
Functors deep dive. For guarantees about the how long serialized versions of a
DataSlice can be deserialized with the latest Koda code, please see the
[Koda Persistent Storage](/koladata/g3doc/persistent_storage.md)
guide.

##### `internal::DataItem` Internals

Consider `kd.slice(3)` (or alternatively `kd.item(3)` or `kd.int32(3)`) as an
example of the DataSlice we wish to represent. The `internal::DataItem` class
holds a variant of `MissingValue`, an `ItemId` (in C++ code it is called
`ObjectId`) that typically references structured data in an associated DataBag,
or a primitive data value.

```
one of {
  MissingValue,
  ObjectId,  # 128-bit "pointer" to structured data - the C++ implementation of
             # `ItemId`. Considered "dangling" if there is no associated DataBag
             # in the DataSlice, or if the DataBag doesn't contain an associated
             # triple.
  int32_t,
  int64_t,
  float,
  double,
  bool,
  arolla::Unit,
  arolla::Text,  # Unicode
  arolla::Bytes,
  arolla::expr::ExprQuote,  # e.g. for sub-expressions of Functors
  schema::DType,  # e.g. for schema constants kd.INT32, kd.OBJECT, etc.
}
```

Note that primitive data is stored directly in the `internal::DataItem` as part
of the `DataSlice` while structured data is stored as an ID (pointer) that
references data in an associated DataBag. Most operations on DataSlices with
primitive values can therefore be done without an associated DataBag, but a
DataBag is often required when working with structured data.

The `kd.slice(3)` example mentioned above is represented by an instance of the
DataSlice class with the following values:

*   Raw data: `internal::DataItem(int32_t{3})`.
*   Shape: a `JaggedShape` of rank 0 (scalar).
*   Schema: `internal::DataItem(dtype{kInt32})`.
*   DataBag: `nullptr`.

Since a DataItem can only hold a single element, it cannot contain mixed data.

##### `internal::DataSliceImpl` Internals

Consider the following DataSlice containing mixed primitive data:
`kd.slice(['a', 2])`. We show below how this is represented internally.

The `internal::DataSliceImpl` class holds a vector of variants of
`arolla::DenseArray<T>`s (an array type supporting missing values) that holds
data of type `T`. Conceptually, the `internal::DataSliceImpl` is the union of
all `arolla::DenseArray`s. The supported types `T` are the same as those
supported by `internal::DataItem`, except for `MissingValue` since missing
values is supported internally by `arolla::DenseArray`. To support the mixed
slice `kd.slice(['a', 2])`, more than one `arolla::DenseArray` is therefore
needed.

Mixed data is represented through multiple `arolla::DenseArray`s instead of a
single array of `internal::DataItem` since it improves locality of data, allows
better integration with Arolla tools that only support primitives, and
simplifies the common case when the data is not mixed.

The collection of `arolla::DenseArray` has the following restrictions:

*   Each type `T` can only occur once in the collection of `arolla::DenseArray`.
*   All `arolla::DenseArrays` have the same size, which is equal to the size of
    the `internal::DataSliceImpl`.
*   For a given index, at most one `arolla::DenseArray` may have a present value
    at that index.
*   The value at index `i` of the `internal::DataSliceImpl` is given by the
    (only) present element at index `i` in one of the underlying
    `arolla::DenseArrays`, or missing if no such value exists.

The `internal::DataSliceImpl` is considered empty if all `arolla::DenseArrays`
are empty. If more than one `arolla::DenseArray` is present, the
`internal::DataSliceImpl` is considered to hold mixed data. Operations on an
`internal::DataSliceImpl` usually involves repeated application of the operation
on all underlying `arolla::DenseArrays`.

The `kd.slice(['a', 2])` example mentioned above is represented by an instance
of the DataSlice class with the following values:

*   Raw data: `internal::DataSliceImpl(arolla::DenseArray<arolla::Text>(['a',
    _]), arolla::DenseArray<int32_t>([_, 2]))`.
*   Shape: a `JaggedShape(2)` of rank 1 with size 2.
*   Schema: `internal::DataItem(dtype{kObject})`.
*   DataBag: `nullptr`.

#### Python Internals

In Python, there exists a `DataSlice` class that supports both scalar and
non-scalar data. This class is a thin wrapper around the equivalent C++ class,
with methods local to the Python implementation.

A `DataItem` class that subclasses `DataSlice` is used for scalar `DataSlices`.
One can view a DataSlice as a collection of DataItem(s). Note that the
`DataItem` class does *not* wrap the low-level `internal::DataItem` since it
doesn't hold a Schema or a DataBag reference. In addition, several subclasses of
`DataItem` exist to provide specialized behavior for `Lists`, `Dicts` and
`Schemas`. See later sections for more details.

Examples of working with DataSlices in Python:

```py
item = kd.item(3.14)      # equivalent to kd.slice(3.14), but also validates
                          # that the provided data is scalar.
item.to_py()              # raw data: 3.14
item.get_schema()         # FLOAT32
item.get_bag()            # None - FLOAT32 values are primitive in Koda.

# Schema is also a DataItem.
assert isinstance(item.get_schema(), kd.types.DataItem)

schema_item = item.get_schema()  # or kd.FLOAT32
schema_item.get_schema()         # SCHEMA
```

Examples of using structured data:

```py
entity = kd.new(a=42, b='xyz')
entity.get_itemid()  # a 128-bit ITEMID value. Normally rendered using base62
                     # encoding, looking like $002nmLwHjsX78ztufxnsOa.
entity.get_schema()  # SCHEMA(a=INT32, b=STRING)
entity.get_bag()     # DataBag with an ID (e.g. $f30f)
```

Methods and operators on a `DataItem` normally invoke a corresponding method on
the C++ `DataSlice` class, which in turn has an implementation for the scalar
`internal::DataItem` structure. Conceptually, the implementation forms an `X`
with two axes that yield four cases: the first axis is the Python separation
between scalar and non-scalar data, which both dispatch to the high-level
DataSlice class in C++, which in turn dispatches to the second axis with the
separate `internal::DataItem` and `internal::DataSliceImpl` low-level
implementations.

### Working with DataSlices

#### DataSlice Operators

DataSlices are central to most operations in Koda. Most
[operators](functors.md#operators) accept DataSlices as inputs and return
DataSlices as outputs.

There are many operators available in Koda. For a full list, please consult the
[Koda API Reference](/koladata/g3doc/api_reference.md). Operators
can informally be categorized as:

*   Pointwise operators: an operation is applied on each item of DataSlice(s)
    independently.
*   Aggregational operators: an operation is applied on multiple items within a
    DataSlice, e.g. within a particular dimension. These operators can change
    the shape of the DataSlices, but not necessarily.
*   Shape changing operators (see [JaggedShape](jagged_shape.md)): these can
    include broadcasting, reshaping, but also grouping operators, such as
    [kd.group_by](/koladata/g3doc/api_reference.md#kd.slices.group_by).
*   Structured Data operators: these are operators that either create Entities,
    Lists, Dicts, Objects, etc. or perform a particular operation on them such
    as accessing or updating associated data.

Most operators perform automatic [schema resolution](schema.md#type-promotion),
[value boxing](schema.md#boxing) and
[shape broadcasting](jagged_shape.md#broadcasting).

For example:

```py
# Pointwise:
kd.slice([[1, 2], [3]]) + kd.slice([[4, 5], [6]])  # [[5, 7], [9]]
kd.slice([1, 2]) + 4  # [5, 6] - both boxing of Python value 4 and broadcasting
                      # into kd.slice([4, 4]) is done automatically.

# Aggregational:
kd.math.agg_sum(kd.slice([[1, 2], [5]]))   # [3, 5] - dimension is reduced.
kd.cum_max(kd.slice([[2, 1], [4, 3, 5]]))  # [[2, 2], [4, 4, 5]] - same shape.

# Group By:
kd.group_by(kd.slice([['a', 'b', 'a', 'b'], ['d', 'c', 'd'], ['a', 'a', 'c']]))
# [[['a', 'a'], ['b', 'b']], [['c'], ['d', 'd']], [['a', 'a'], ['c']]]
# The result above has one more dimension compared to the input DataSlice.
kd.agg_count(
  kd.group_by(
    kd.slice([['a', 'b', 'a', 'b'], ['d', 'c', 'd'], ['a', 'a', 'c']]),
    sort=True,  # s.t. 'c' comes before 'd', etc.
  )
)  # [[2, 2], [1, 2], [2, 1]]

# Structured Data operators (using vectorized initialization):
entity_ds = kd.new(x=kd.slice([1, 2]), y=kd.slice(['a', 'b']))
# [Entity(x=1, y='a'), Entity(x=2, y='b')]
entity_ds.x  # accessing data [1, 2]
entity_ds = entity_ds.with_attrs(z=3.14)  # updating data - immutable update.
# [Entity(x=1, y='a', z=3.14), Entity(x=2, y='b', z=3.14)]
entity_ds.z  # [3.14, 3.14]
```

#### Indexing and Navigating DataSlices

DataSlices can be navigated in a few ways:

*   `.L`: Access dimensions as if they were nested Python lists. `ds.L[i]`
    returns the i-th sub-DataSlice at the first dimension. This is useful for
    iteration.
*   `.S`: Multi-dimensional indexing and slicing. `ds.S[i, j, ...]` allows you
    to select elements or sub-DataSlices across multiple dimensions
    simultaneously. Examples:
    *   `ds.S[0]`: Selects the 0th element along the last dimension.
    *   `ds.S[..., :2]`: Selects the first two elements along the last
        dimension.

See the [Indexing](jagged_shape.md#indexing) section in the JaggedShape
deep-dive for more details.

#### Sparsity

Koda fully supports sparsity, where each item in a DataSlice can be either
present or missing. The `kd.has()` operator returns a `kd.MASK` DataSlice,
indicating which elements are present (`kd.present`) and which are missing
(`kd.missing`).

Most pointwise operators propagate missing values: if any input item is missing,
the output item is also missing. Aggregational operators, however, typically
ignore missing values.

Key operators for working with sparsity include:

*   `~`: Invert a mask.
*   `&`: Mask application (e.g., `ds & (ds > 0)` keeps only positive values).
*   `|`: Coalesce (e.g., `ds1 | ds2` fills missing values in `ds1` with values
    from `ds2`).
*   `kd.cond(mask, true_ds, false_ds)`: Conditional selection.
*   `kd.select(ds, mask)`: Filters out items where the mask is missing,
    potentially changing the DataSlice's shape.
*   `kd.select_present(ds)`: Keeps only the present items.

Comparison operators like `>`, `==`, etc., are pointwise and return mask
DataSlices.

### Structured Data and DataBag Interaction

Non-primitive items (Entities, Lists, or Dicts) are represented by `ItemIds`
within the DataSlice's raw data. The actual values of attributes, List elements,
or Dict entries associated with these `ItemId`s are stored and managed within an
accompanying [DataBag](data_bag.md), represented through
`Entity.Attribute=>Value` triples. `ItemId`s in DataSlices can be viewed as
pointers to the data stored in DataBags.

#### Entities

An Entity is a collection of attributes, represented in a DataBag as
`Entity.Attribute=>Value` triples. Entity DataSlices share a common Entity
Schema for efficiency.

```py
entity_ds = kd.new(
  a=kd.slice([12, 42]),
  b=kd.slice(['abc', 'xyz'])
)
# [Entity(a=12, b='abc'), Entity(a=42, b='xyz')]

entity.get_bag().contents_repr()
# DataBag $51f7:
# $00A77l961a3rn8tAp7TfkG.a => 12
# $00A77l961a3rn8tAp7TfkG.b => abc
# $00A77l961a3rn8tAp7TfkH.a => 42
# $00A77l961a3rn8tAp7TfkH.b => xyz

# SchemaBag:
# $7IuxbXOk7p4FXg61l8TiuV.a => INT32
# $7IuxbXOk7p4FXg61l8TiuV.b => STRING
```

See [Technical Deep Dive: DataBag](databag.md) for more details on how this is
represented inside of a DataBag, how attribute access works and more.

#### Schemas

A [schema](schema.md) represents the data type of a DataSlice. A schema is also
a DataItem, so we can form DataSlices of schemas, for example:

```py
kd.slice([
  kd.INT32,
  kd.FLOAT32,
  kd.OBJECT,
  kd.new(a=32).get_schema(),
  kd.list_schema(kd.INT32),
])
```

Schemas determine the behavior of operators and are vital when working with
structured data. The schema allows us to distinguish between Lists, Dicts and
other Entities, even when data is missing. They also determine casting behavior,
are used for type checking, and more, as described in the [Schemas](schema.md)
deep dive article.

#### Lists

A List represents an ordered sequence of items. Lists have list schemas, whose
special list item schema attribute indicates the schema / type of list items. A
list has a `LIST` schema, which is one kind of "struct" schema. Struct schemas
have `ItemIds` and can be associated with attributes. In the case of a `LIST`
schema, it has one attribute, namely `'__items__'`, which stores the schema of
the list items. Lists are DataItems and they hence behave like scalars in
operations. That differs from DataSlices, which represent a collection of scalar
items. So we can form a DataSlice of Lists, but not a List of DataSlices.

```py
lists = kd.slice([kd.list([1, 2, 3]), kd.list([4, 5])])
# [List([1, 2, 3]), List([4, 5])]

lists.get_bag().contents_repr()
# DataBag $2b07:
# $0FG3JnauZRgAfhMVj5lbIS[:] => [1, 2, 3]
# $0FG3JnauZRgAfhMVj5lbIT[:] => [4, 5]

# SchemaBag:
# #7QUhePdHCvsoCyAWAHwtzx.get_item_schema() => INT32
```

##### List Explosion

List explosions, normally done through `[:]`, unpack the contents of a List into
a DataSlice with one more dimension compared to the DataSlice of lists being
exploded. This converts a scalar List into a 1-d DataSlice, changing the
behavior of operators to allow for aggregations or pointwise computations on
each value.

```py
lists[:]

# Data:
# lists.S[0]: $0FG3JnauZRgAfhMVj5lbIS[:] => [1, 2, 3]
# lists.S[1]: $0FG3JnauZRgAfhMVj5lbIT[:] => [4, 5]

# Schema:
# 7QUhePdHCvsoCyAWAHwtzx.get_item_schema() => INT32

# Result:
kd.slice([[1, 2, 3], [4, 5]])  # shape is "exploded" shape of `lists`.
```

##### List Implosion

Implosion is the inverse of a List explosion and reduces the trailing dimensions
to create Lists from "collapsed" items. Examples:

```py
items = kd.slice([[[1, 2], [3]], [[4, 5]]])
items.implode()
# kd.slice([
#   [kd.list([1, 2]), kd.list([3])],
#   [kd.list([4, 5])],
# ])
# 3 dim reduced to 2 dim.

items.implode(ndim=2)
# kd.slice([
#   kd.list([kd.list([1, 2]), kd.list([3])]),
#   kd.list([kd.list([4, 5])]),
# ])
# 3 dim reduced to 1 dim.

kd.list([[[1, 2], [3]], [[4, 5]]])  # equivalent to items.implode(-1)
# kd.list([
#   kd.list([kd.list([1, 2]), kd.list([3])]),
#   kd.list([kd.list([4, 5])]),
# ])
# 3 dim reduced to a zero-dimensional DataItem.
```

##### List Indexing / Slicing

Similar to indexing the DataSlice using `.S` notation, one can index and slice
the List items in a DataSlice of Lists using `[i]` notation. This operation
performs vectorized indexing across all Lists in the DataSlice:

```py
lists[1:]

# Data:
# lists.S[0]: $0FG3JnauZRgAfhMVj5lbIS[1:] -> [2, 3]
# lists.S[1]: $0FG3JnauZRgAfhMVj5lbIT[1:] -> [5]

# Result:
kd.slice([[2, 3], [5]])  # shape is "sliced" shape of `lists`.
```

Indexing example:

```py
lists[2]

# Data:
# lists.S[0]: $0FG3JnauZRgAfhMVj5lbIS[2] -> 3
# lists.S[1]: $0FG3JnauZRgAfhMVj5lbIT[2] -> None

# Result:
kd.slice([3, None])  # shape is inherited from `lists`.
```

#### Dicts

A Dict is a collection of key-value pairs. A Dict has a `DICT` schema, which is
another kind of struct schema. In this case, it has a `'__keys__'` attribute
that stores the schema of the keys, and a `'__values__'` attribute that stores
the schema of the values.

```py
dicts = kd.dict(
  kd.slice([['a', 'b'], ['a'], ['b', 'c']]),
  kd.slice([[12, 42], [15], [37, 11]]),
)
# [Dict{'b'=42, 'a'=12}, Dict{'a'=15}, Dict{'c'=11, 'b'=37}]
# I.e. a DataSlice with 3 Dict items, as we will also see in the DataBag below.

dicts.get_bag().contents_repr()
# DataBag $ef37:
# $0UTIrFFXP0pECOp6mDjKDg['a'] => 12
# $0UTIrFFXP0pECOp6mDjKDg['b'] => 42
# $0UTIrFFXP0pECOp6mDjKDh['a'] => 15
# $0UTIrFFXP0pECOp6mDjKDi['b'] => 37
# $0UTIrFFXP0pECOp6mDjKDi['c'] => 11

# SchemaBag:
# #7QRy2BAblHHNytHxFmpaGL.get_key_schema() => STRING
# #7QRy2BAblHHNytHxFmpaGL.get_value_schema() => INT32
```

##### Key / Value Access

`get_keys()` can be used to obtain the keys of a Dict. As with
[List explosion](#list_explosion), this operation is vectorized.

```py
dicts.get_keys()

# Data:
# dicts.S[0]: $0UTIrFFXP0pECOp6mDjKDg.get_keys() -> ['a', 'b']
# dicts.S[1]: $0UTIrFFXP0pECOp6mDjKDh.get_keys() -> ['a']
# dicts.S[2]: $0UTIrFFXP0pECOp6mDjKDi.get_keys() -> ['b', 'c']

# Schema:
#7QRy2BAblHHNytHxFmpaGL.get_key_schema() => STRING

# Result:
kd.slice([['a', 'b'], ['a'], ['b', 'c']])
```

Similarly, dict values can be access in the same way:

```py
dicts.get_values()  # [[12, 42], [15], [37, 11]]
```

##### Dict lookup

Vectorized Dict lookup is possible through `[key]`:

```py
dicts['a']

# Data:
# dicts.S[0]: $0UTIrFFXP0pECOp6mDjKDg['a'] -> [12]
# dicts.S[1]: $0UTIrFFXP0pECOp6mDjKDh['a'] -> [15]
# dicts.S[2]: $0UTIrFFXP0pECOp6mDjKDi['a'] -> [None]

# Result:
kd.slice([12, 15, None])  # shape is inherited from `dicts`.
```

The key lookup can be done with a slice of keys (set of keys for each dict
item):

```py
dicts[kd.slice([['a', 'a', 'c'], ['b', 'a'], ['b', 'b']])]

# Data:
# dicts.S[0]: $0UTIrFFXP0pECOp6mDjKDg['a', 'a', 'c'] -> [12, 12, None]
# dicts.S[1]: $0UTIrFFXP0pECOp6mDjKDh['b', 'a'] -> [None, 15]
# dicts.S[2]: $0UTIrFFXP0pECOp6mDjKDi['b', 'b'] -> [37, 37]

# Result:
kd.slice([[12, 12, None], [None, 15], [37, 37]])
```

#### Objects

While many DataSlices contain elements of a single, uniform type (e.g., all
`INT32` or all entities of a specific struct schema), Koda also supports
heterogeneous DataSlices through the concept of Objects. A DataSlice with an
`OBJECT` schema can hold values of mixed types, where each element effectively
carries its own type information.

Examples:

```py
kd.slice(['xyz', 42, 3.14, kd.obj(kd.list([1, 2, 3]))])
kd.obj(a=42, b=3.14)
```

While flexible, the dynamic nature of OBJECTs can lead to slower performance
compared to homogeneous DataSlices with fixed schemas. See the
[OBJECT](schema.md#object) section for more details.

#### Functor

Functors represent computations. They accept input(s) and produce output(s). The
computation is stored in a collection of `EXPR` attributes, whose values are
expressions that may include calls to other functors such as nested ones.

```py
def fn(x, y):
  return x + y

functor = kd.fn(fn)
functor(kd.slice([1, 2, None]), kd.slice(4))  # [5, 6, None]
```

As with other types of DataSlices, functors can be updated, introspected, and
serialized. See [Functors](functors.md#functors) for more details.
