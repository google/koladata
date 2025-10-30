# Working with `DataSlice` in C++

* TOC
{:toc}

This guide provides a look into how to efficiently work with `DataSlices` in C++, and is mainly intended for internal developers already familiar with the `DataSlice` concept. See [Koda Fundamentals](fundamentals.md) for an introduction into the topic.

## `DataSlice`

`DataSlice` contains data, shape, schema, and an optional link to a `DataBag`.

```c++
// Simplified code to illustrate what it contains.
class DataSlice {
  ...
  std::variant<internal::DataItem, internal::DataSliceImpl> data;
  JaggedShape shape;
  internal::DataItem schema;
  DataBagPtr db;  // nullable

  // Optimization-related flag. A DataSlice is whole if we know that all of
  // the data in its DataBag are reachable from items in the DataSlice. This
  // allows certain optimizations, like skipping extraction.
  // Makes sense only if `db` is present.
  Wholeness wholeness;  // kWhole or kNotWhole
};
```

Data can be either `internal::DataItem` (in case of a zero-dimensional slice),
or `internal::DataSliceImpl`. If (and only if) it is `DataItem`, then
`shape.rank()` should be zero.

There are several functions to create `DataSlice`:

### `DataSlice::CreatePrimitive(scalar)`

The easiest way to create a scalar `DataSlice`. It will be initialized with

- `data = internal::DataItem(scalar)`
- `shape = JaggedShape::Empty()`
- `schema = internal::DataItem(schema::GetDType<T>())`
- `db = nullptr`

`scalar` must be a primitive type or `MissingValue`. This function doesn't
support `ObjectId` because its schema is more complicated than just a DType.

### `DataSlice::Create(item, schema, db=nullptr, wholeness=kNotWhole)`

Creates scalar `DataSlice` with explicit schema and `DataBag`. Shape is
initialized as `JaggedShape::Empty()`.

### `DataSlice::Create(slice_impl, shape, schema, db=nullptr, wholeness=kNotWhole)`

Creates a non-scalar `DataSlice`.

An example:

```c++
ASSIGN_OR_RETURN(auto edge, DenseArrayEdge::FromSplitPoints(
    arolla::CreateFullDenseArray({1, 3, 7})));
ASSIGN_OR_RETURN(auto shape, DataSlice::JaggedShape::FromEdges({edge}));
ASSIGN_OR_RETURN(DataSlice slice, DataSlice::Create(
    internal::DataSliceImpl(arolla::CreateConstDenseArray<float>(7, 3.14f)),
    shape,
    internal::DataItem(schema::kFloat32)
    // db=nullptr  - DataBag not needed here since it is a primitive slice
    //               without links to any other data,
));
// slice = [[3.14], [3.14, 3.14], [3.14, 3.14, 3.14, 3.14]]
```

Note: see section about `DataSliceImpl` creation below.

### `DataSlice::CreateWithSchemaFromData(slice_impl, shape, db, wholeness=kNotWhole)`

Same as above, but deduces `schema` from data. Works only with primitive types.

### `DataSlice::CreateWithFlatShape(slice_impl, schema, db, wholeness=kNotWhole)`

Create rank-1 DataSlice. Shape is created as
`JaggedShape::FlatFromSize(slice_impl.size())`.

### Operations on `DataSlice`

The recommended way of implementing unary and binary operations is
[UnaryOpEval](http://cs///koladata/operators/unary_op.h)
and
[BinaryOpEval](http://cs///koladata/operators/binary_op.h).
It allows to create a pointwise operation directly from a scalar C++ functor.
It automatically adds vectorization, broadcasting, and casting to a common type;
can propagate absl::Status; supports optional output.

Limitations:

- It doesn't support multitype slices (e.g. mixed slice with strings and floats
    at the same time).
- It doesn't support aggregation.
- It doesn't support special handling of missing arguments. If in one of
  the arguments the value is missing, the output is also missing.

Here is a simple example:

```
struct MultiplyOp {
  using run_on_missing = std::true_type;

  template <typename T>
  T operator()(T lhs, T rhs) const { return lhs * rhs; }
};

struct AbsOp {
  using run_on_missing = std::true_type;

  template <typename T>
  T operator()(T a) const { return a >= 0 ? a : -a; }
};

absl::StatusOr<DataSlice> Multiply(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<MultiplyOp>(x, y, NumericArgs("x", "y"));
}

absl::StatusOr<DataSlice> Abs(const DataSlice& x) {
  return UnaryOpEval<AbsOp>(x, NumericArgs("x"));
}
```

Note: The line `using run_on_missing = std::true_type` is optional. If present,
the functor will be evaluated even on missing uninitialized inputs, but
the result will be ignored. It is useful for very cheap operations to avoid
one extra branch per item.

`NumericArgs` (implemented in [operators/utils.h](http://cs///koladata/operators/utils.h))
defines a constexpr boolean `kIsInvocable<...>` which limits available type
combinations (to prevent e.g. instantiation of the overload
`MultiplyOp::operator()<Text, Text>` which wouldn't compile), and provides
a function `CheckArgs` to format error messages. This argument is optional.
If this argument is missing, then UnaryOpEval/BinaryOpEval supports all
type combinations supported by the functor.

An example without args checker argument:

```
struct SqrtOp {
  float operator()(int32_t x) const { return std::sqrt(static_cast<float>(x)); }
  float operator()(int64_t x) const { return std::sqrt(static_cast<float>(x)); }
  float operator()(float x) const { return std::sqrt(x); }
  double operator()(double x) const { return std::sqrt(x); }
};

absl::StatusOr<DataSlice> Sqrt(const DataSlice& x) {
  return UnaryOpEval<SqrtOp>(x);
}
```

#### BinaryOpEval Bytes/Text example

In case of `arolla::Bytes` and `arolla::Text` the UnaryOpEval/BinaryOpEval
operation definition should also have overload on `absl::string_view` (because
`DataSliceImpl` internally stores all string data in a single buffer and only
provides a view; getting actual Bytes/Text would require copying of
the underlying data).

Some operations may require different handling of Bytes and Text. In this case
`arolla::meta::type<T>` can be used as a tag to distinguish the types.
See example:

```
struct OccurrenceCountOp {
  // Implementation on Bytes.
  // Used for evaluation in scalar case, but also important for overload
  // deduction (i.e. that [Bytes, Bytes] args are supported) in batch case.
  int64_t operator()(const arolla::Bytes& str,
                     const arolla::Bytes& substr) const {
    return arolla::BytesSubstringOccurrenceCountOp{}(str, substr);
  }

  // Implementation on Text.
  // Used for evaluation in scalar case, but also important for overload
  // deduction (i.e. that [Text, Text] args are supported) in batch case.
  int64_t operator()(const arolla::Text& str,
                     const arolla::Text& substr) const {
    return arolla::TextSubstringOccurrenceCountOp{}(str, substr);
  }

  // Implementation on view_type of Bytes.
  // `arolla::meta::type` args are needed to distinguish from Text which has
  // the same view type.
  int64_t operator()(arolla::meta::type<arolla::Bytes>,
                     arolla::meta::type<arolla::Bytes>, absl::string_view str,
                     absl::string_view substr) const {
    return arolla::BytesSubstringOccurrenceCountOp{}(str, substr);
  }

  int64_t operator()(arolla::meta::type<arolla::Text>,
                     arolla::meta::type<arolla::Text>, absl::string_view str,
                     absl::string_view substr) const {
    return arolla::TextSubstringOccurrenceCountOp{}(str, substr);
  }
};

absl::StatusOr<DataSlice> Count(const DataSlice& x, const DataSlice& substr) {
  return BinaryOpEval<OccurrenceCountOp>(x, substr,
                                         ConsistentStringOrBytes("x", "substr"),
                                         BinaryOpReturns(schema::kInt64));
}
```

The last (optional) argument of BinaryOpEval is a policy to choose the output
schema if one of the inputs is either `schema::kObject` or `schema::kNone` (in
this case the output schema can't be deduced from the functor defining
the operation). `BinaryOpReturns` is a simple policy returning a fixed output
schema regardless of the inputs:

```
constexpr auto BinaryOpReturns(schema::DType t) {
  return [t](schema::DType t1, schema::DType t2) -> schema::DType { return t; };
}
```

#### DataSliceOp

In the cases where UnaryOpEval/BinaryOpEval functionality is not enough,
there is an option to have a manual implementation of a unary/binary operation
using
[`DataSliceOp`](https://github.com/google/koladata/blob/main//koladata/data_slice_op.h).
It is a utility for invoking operator functors on DataSlices that supports unary
and binary operators with overloads for `DataItem` and `DataSliceImpl`.

Unary operators are simply invoked with the provided `DataItem`/`DataSliceImpl`.
The overloads may return either `DataItem` or `DataSliceImpl` irrespective
of the input type. The return type may optionally be wrapped with
`absl::StatusOr`.

```c++
struct MyUnaryOp {
  absl::StatusOr<DataItem> operator()(const DataItem& item);
  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& slice);
};

// Invokes the appropriate MyUnaryOp overload.
DataSliceOp<MyUnaryOp>(slice, std::move(result_shape),
                       std::move(result_schema), std::move(result_bag));
```

For binary operators, the behavior depends on the available overloads. At
minimum, there must exist a `(DataItem, DataItem)` and a `(DataSliceImpl,
DataSliceImpl)` overload. In case two scalar DataSlices are passed, the
`(DataItem, DataItem)` overload will be invoked. In all other cases, the inputs
are *aligned* (broadcasted to a common shape), before the `(DataSliceImpl,
DataSliceImpl)` overload is invoked.

In addition, a mix of input types is supported: `(DataItem, DataSliceImpl)` and
`(DataSliceImpl, DataItem)`. These overloads will be invoked in case a mix of
scalars and non-scalars are passed as inputs. This avoids the need to broadcast
scalar inputs which is often times more performant.

```c++
struct MyBinaryOp {
  DataItem operator()(const DataItem& i1, const DataItem& i2);
  DataSliceImpl operator()(const DataSliceImpl& s1, const DataSliceImpl& s2);
  // Optional optimizations.
  // DataSliceImpl operator()(const DataSliceImpl& slice, const DataItem& item);
  // DataSliceImpl operator()(const DataItem& item, const DataSliceImpl& slice);
};
```

As with the unary case, the output type is independent of the input type, and
the output may be wrapped with `absl::StatusOr`.

## `internal::DataSliceImpl`

Here are some ways to create `DataSliceImpl`. It is not comprehensive list, see
[`internal/data_slice.h`](https://github.com/google/koladata/blob/main//koladata/internal/data_slice.h)
and
[`internal/slice_builder.h`](https://github.com/google/koladata/blob/main//koladata/internal/slice_builder.h)
headers for more information.

### Create `DataSliceImpl` from `DenseArray`

```c++
auto impl = DataSliceImpl::Create<T>(std::move(dense_array));
```

Note: see
[here](https://github.com/google/arolla/blob/main/arolla/dense_array/README.md)
how to create `DenseArray`.

### Create `DataSliceImpl` from `DenseArray<ObjectId>`

For performance reasons `DataSliceImpl` has a list of AllocationId of all
ObjectId it contains. It is used in DataBagImpl to filter data sources.

It is possible to use `DataSliceImpl::Create` with `ObjectId` same way as for
primitive types, but it will iterate over data to gather AllocationIds. If
allocation ids are already known (e.g. if all objects are from the same
allocation), use `CreateWithAllocIds` to reduce the overhead:

```c++
auto impl = DataSliceImpl::CreateWithAllocIds<ObjectId>(alloc_ids, dense_array);
```

### Create `DataSliceImpl` with `SliceBuilder`

`SliceBuilder` is the most universal way of creating DataSliceImpl.

```c++
internal::SliceBuilder bldr(/*size=*/5);
bldr.InsertIfNotSet<float>(0, 3.14f);
bldr.InsertIfNotSet<arolla::Text>(2, arolla::Text("abc"));
bldr.InsertIfNotSet(3, DataItem());
bldr.InsertIfNotSet(4, DataItem(7));
internal::DataSliceImpl slice = std::move(bldr).Build();
// result: { 3.14f, UNSET, "abc", REMOVED, 7 }
```

After `SliceBuilder` creation all values are UNSET. Each value can be assigned
only once (i.e. while it is still UNSET), next assignments of the same value
will be ignored. Assigning `nullopt` or empty `DataItem()` will set it to
REMOVED. The difference between UNSET and REMOVED is important when data is
stored in `DataBag`. If value is UNSET, it will be searched in fallback data
bags as well. REMOVED means explicitly removed, regardless of fallbacks.

When adding many values of the same type use typed view of slice builder in
order to reduce overhead. For example, if we want half of the values to be ints,
and half to be floats:

```c++
internal::SliceBuilder bldr(8);
auto bldr_int = bldr.typed<int>();
auto bldr_float = bldr.typed<float>();
for (size_t i = 0; i < 8; i += 2) {
  bldr_int.InsertIfNotSet(i, i);
  bldr_float.InsertIfNotSet(i, i + 0.5);
}
internal::DataSliceImpl slice = std::move(bldr).Build();
// result: { 0, 0.5f, 2, 2.5f, 4, 4.5f, 6, 6.5f }
```

Or batched version:

```c++
bldr.InsertIfNotSet(const arolla::bitmap::Bitmap& mask,
                    const arolla::bitmap::Bitmap& presence,
                    const arolla::Buffer<T>& values);
```

`mask` selects the elements to be assigned (others will remain UNSET).

`mask & ~presence` are elements to change from UNSET to REMOVED.

`mask & presence` are elements to be assigned from `values` (unless the were
previously set to something else).

Example:

```c++
arolla::DenseArray<int> int_data = GetSomeIntegers();
arolla::DenseArray<float> float_data = GetSomeFloats();

// e.g. int_data   = { 1,  {}, {},   2 }
//      float_data = {0.5, {}, 3.2, {} }

DCHECK_EQ(int_data.size(), float_data.size());

SliceBuilder bldr(int_data.size());

// Assign only values which are present in `int_data`. Others remain UNSET.
bldr.InsertIfNotSet(int_data.bitmap, Bitmap(), int_data.values);

// Assign all values which are not assigned yet. The values which are missing
// in `float_data` will become REMOVED.
// After this SliceBuilder will be considered finalized (no UNSET values remain)
// and all subsequent modifications will be no-op.
bldr.InsertIfNotSet(Bitmap(), float_data.bitmap, float_data.values);

bldr.InsertIfNotSet(1, arolla::Text("abc"));  // ignored since already assigned

internal::DataSliceImpl slice = std::move(bldr).Build();
// result: { 1, REMOVED, 3.2, 2 }
```

### Performance recommendations

If you want to create `DataSliceImpl` then
(in descending order of priority)

1. Look through `internal/data_slice.h` to check if there is a function
optimized for your specific use case. E.g. `DataSliceImpl::AllocateEmptyObjects`
allocates a new batch of ObjectIds and returns a slice with all these ids, and
it is much faster than any generic way of DataSlice creation.
2. If all elements are of the same type and it is easy to create `DenseArray` of
a primitive type - use `DataSliceImpl::Create<T>(std::move(dense_array))`.
3. If you need to combine data from several DenseArrays in some non-trivial
way - use batched version of `SliceBuilder::InsertIfNotSet`.
4. Scalar version of `SliceBuilder::InsertIfNotSet<T>`. When adding multiple
values of a same type to a potentially multitype DataSlice - use
`SliceBuilder::typed<T>()` - adding values to typed view of the builder has
less overhead.
5. Use `SliceBuilder::InsertIfNotSet(DataItem)` only if nothing above is
applicable. Note that it has a per-value overhead for dispatching by type.
6. Avoid `DataSliceImpl::Create(Span<const DataItem>)`,
`DataSliceImpl::Create(DenseArray<DataItem>)` - they are intended for tests
only.
