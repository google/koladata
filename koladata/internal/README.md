# Koda Internal Library

The internal library provides data structures and tools for low-level data
manipulation and storage.

## ObjectId and AllocationId

Koda provides storage and manipulation libraries for different primitive
types and objects.

Objects are represented as `ObjectId`. `ObjectId` is 128 bit "persistent"
pointer that satisfies the following requirements:

1. "Persistent" pointer value can be used across processes and machines.
2. Asynchronized allocations have low collision probability across
   machines/processes.
3. Allocations within a single process are guaranteed to have no collisions.
   That makes it safe to use in the majority of production use cases.

Objects from the same allocation have consecutive `ObjectId::Offset`.
This gives us the ability to create data structure with fast lookup for the
collection of objects from the same `AllocationId`.

`AllocationId` serves as a lookup key for the data related to its objects.
`AllocationId` is always embedded into the `ObjectId`.

The following operations are designed to be fast:
1. `AllocationId::Contains(ObjectId)` verifies that the object belongs to the
   allocation.
2. `AllocationId::ObjectByOffset(int64_t)` returns `ObjectId` with the given
   offset within the allocation.
3. `ObjectId::Offset()` returns offset from [0, allocation_size).
   `AllocationId` and offset uniquely identify `ObjectId`.
   An offset can be used as an index in the array for fast lookup in the data
   structures.

Additional object information is stored externally in the `DataBag`. To access
this information we need to "dereference" the pointer and access `DataBag`
internal data structures. To save on rather expensive dereferencing, some meta
information is embedded into `AllocationId` and `ObjectId`:
1. Builtin object type: List, Dict, Schema, UUID.
2. Type of the Koda Schema: implicit, explicit, nofollow and etc.

There is a special kind of `AllocationId`/`ObjectId` called `UUID`.
Such allocations can be deterministically created across different machines from
the arguments. Embedded metadata will contain the fact that object is `UUID` and
it will never collide with allocated objects.

There are two ways to create `UUID` object with different performance
properties for the downstream code.
1. `CreateUuidObject(arolla::Fingerprint)` would create a small allocation with
   capacity equal to 1. Most of the bits of arolla::Fingerprint will be used
   directly in the resulting object. Embedded metadata and offset bits will be
   not used from `Fingerprint`.
2. `CreateUuidWithMainObject(ObjectId main, arolla::Fingerprint salt)` will
   create `UUID` object that derives some properties from the main `ObjectId`.
   The capacity of the allocation and `Offset` will be derived from the main
   object. Calling `CreateUuidWithMainObject` with two objects with the same
   `AllocationId` and the same fingerprint will result with objects from the
   same `AllocationId`.

In Koda data structures we generally assume that objects from the same
`AllocationId` will be often queried together (partially or with repetition).
Objects from the small allocations (capacity <= 2) are typically treated as
individual objects.

## DataItem and DataSliceImpl

`DataItem` represents a type-erased single Koda value. It may store one
of the primitive types (integers, floats, boolean, strings, expressions, schema)
or `ObjectId`. `DataItem` can be missing with unknown type.

The underlying representation is based on `std::variant` with an additional
special `MissingValue` type. `DataItem::dtype` returns runtime type information
via `arolla::QTypePtr`. `arolla::GetNothingQType` is used for missing values.

`DataItem::value<T>`, `DataItem::holds_value<T>` and `DataItem::VisitValue` can
be used to access the value stored in the `DataItem`.

`DataItem` can be used as a key in a map, but Hash, Eq, and Less functors should
be specified explicitly:
```
// hash map example
absl::flat_hash_map<DataItem, Value, DataItem::Hash, DataItem::Eq>

// ordered map example
absl::btree_map<DataItem, Value, DataItem::Less>
```

Note that DataItem::Hash, DataItem::Eq considers int32 DataItem equal to int64
DataItem (and float DataItem equal to double DataItem) if the values are equal.
But float/double DataItem is not equal to int32/int64 DataItem even if the
underlying values are equal.

`DataSliceImpl` represents an array of `DataItem`s.
The underlying representation is optimized for single type slices, so that we
can perform `O(1)` conversion to `arolla::DenseArray<T>`
(via `DataSliceImpl::values<T>`). Internally we store a separate `DenseArray`
for each type. Most of the functions in Koda are optimized for single type
slices (`DataSliceImpl::is_single_dtype()`).

`DataSliceImpl::operator[]` is the most straightforward way to access
`DataItem` elements. But there are more efficient ways to do it.
E.g., `DataSliceImpl::VisitValues(visitor)` will call `visitor` for each
internal array.

`DataSliceImpl` also maintains `AllocationIdSet` for its `ObjectId` elements.
We use it to perform faster access to the internal `DataBag` structures.
`AllocationIdSet` is allowed to be a superset, but that can slow down `DataBag`
operations.

`DataSliceImpl::Create` creates `DataSliceImpl` from one or several
`arolla::DenseArray`. `SliceBuilder` can be used for more general cases.
`DataSliceImpl::CreateWithAllocIds` and `Builder::GetMutableAllocationIds()`
can be used in order to avoid `AllocationIdSet` recomputations.

