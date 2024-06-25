// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef KOLADATA_INTERNAL_OBJECT_ID_H_
#define KOLADATA_INTERNAL_OBJECT_ID_H_

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/numeric/int128.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata::internal {

class AllocationId;

constexpr size_t kSmallAllocMaxBits = 1;
constexpr size_t kSmallAllocMaxCapacity = 1 << kSmallAllocMaxBits;

// ObjectId is a Databag persistent 128 bit pointer.
//
// Allocated `ObjectId` embed two concepts:
// 1. `AllocationId` that is used for looking up data associated for objects
//    grouped into one allocation.
// 2. `Offset` inside of the allocation.
//    That allow fast lookup within allocation.
//
// Internal representation:
// 1. 52 bits of machine/process specific allocator id.
//    This id is assigned randomly for the process and we expect low probability
//    of collision.
//    Collision is likely with 2**(52/2) (>65M) communicating processes.
// 2. 6 bits encode number of bits in Offset.
// 3. 6 bits reserved for embedded metadata.
// 4. 8 bytes - (number of bits in offset) for allocation id
//    in case of overflow bits will be added to allocator id.
// 5. Up to 40 bits for offset inside of the allocation
class ObjectId {
 public:
  // There is no zero initialization for performance reasons.
  ObjectId() = default;

  // Returns offset inside of the allocation.
  int64_t Offset() const { return id_ & ((1ull << offset_bits_) - 1); }

  // Object embedded metadata accessors.
  // Returns true if object was allocated via one of the Allocate* function.
  bool IsAllocated() const { return !IsUuid(); }
  // Returns true if object was constructed from Fingerprint.
  bool IsUuid() const { return (metadata_ & kUuidFlag) != 0; }
  // Returns true if the object is a list.
  bool IsList() const { return metadata_ == kListFlag; }
  // Returns true if the object is a dict.
  bool IsDict() const { return metadata_ == kDictFlag; }
  // Returns true if the object is a schema.
  bool IsSchema() const { return metadata_ >= kStartSchemaFlag; }
  // Returns true if the object is an explicit schema.
  bool IsExplicitSchema() const {
    return metadata_ >= kStartExplicitSchemaFlag;
  }
  // Returns true if the object is an implicit schema.
  bool IsImplicitSchema() const { return metadata_ == kUuidImplicitSchemaFlag; }
  // Returns true if the object is a nofollow schema.
  bool IsNoFollowSchema() const {
    return (metadata_ & kNoFollowSchemaFlag) == kNoFollowSchemaFlag;
  }

  // Returns the `uint128` numeric value representation of the object id.
  absl::uint128 ToRawInt128() const {
    return absl::MakeUint128(InternalHigh64(), InternalLow64());
  }

  static ObjectId NoFollowObjectSchemaId();

  // Returns true if object belongs to the small allocation
  // with <= kSmallAllocMaxCapacity capacity.
  bool IsSmallAlloc() const { return offset_bits_ <= kSmallAllocMaxBits; }

  friend bool operator==(ObjectId lhs, ObjectId rhs) {
    return lhs.InternalHigh64() == rhs.InternalHigh64() &&
           lhs.InternalLow64() == rhs.InternalLow64();
  }
  friend std::strong_ordering operator<=>(ObjectId lhs, ObjectId rhs) {
    return std::tuple{lhs.InternalHigh64(), lhs.InternalLow64()} <=>
           std::tuple{rhs.InternalHigh64(), rhs.InternalLow64()};
  }

  std::string DebugString() const {
    return absl::StrCat(absl::Hex(InternalHigh64(), absl::kZeroPad16),
                        absl::Hex(InternalLow64(), absl::kZeroPad16));
  }

  friend std::ostream& operator<<(std::ostream& os, const ObjectId& obj) {
    os << absl::StrCat(obj.DebugString());
    return os;
  }

  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
    hasher->CombineRawBytes(this, sizeof(ObjectId));
  }

  template <typename H>
  friend H AbslHashValue(H h, const ObjectId& obj) {
    return H::combine_contiguous(
        std::move(h), reinterpret_cast<const uint8_t*>(&obj), sizeof(ObjectId));
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const ObjectId& obj) {
    sink.Append(obj.DebugString());
  }

  // (metadata_ & kUuidFlag) != 0 means that object is UUID.
  static constexpr int64_t kUuidFlag = 0b000001;

  // metadata_ == kListFlag means that object is List.
  // NOTE: In case UUID lists are needed, use kListFlag | kUuidFlag.
  static constexpr int64_t kListFlag = 0b000010;

  // metadata_ == kDictFlag means that object is Dict.
  // NOTE: In case UUID dicts are needed, use kDictFlag | kUuidFlag.
  static constexpr int64_t kDictFlag = 0b000100;

  // Schema flags:
  // Schema operations are frequent, so reserving an end-range of metadata_
  // values, so that we can have efficient checks.
  static constexpr int64_t kStartSchemaFlag =                    0b111000;
  static constexpr int64_t kUuidImplicitSchemaFlag =             0b111001;
  static constexpr int64_t kStartExplicitSchemaFlag =            0b111010;
  // NoFollow behaves as explicit schema.
  // Deterministic, but non-UUID.
  static constexpr int64_t kNoFollowSchemaFlag =                 0b111010;
  static constexpr int64_t kNoFollowObjectSchemaFlag =        // 0b111010
      kNoFollowSchemaFlag;
  static constexpr int64_t kNoFollowUuidImplicitSchemaFlag =  // 0b111011
      kUuidImplicitSchemaFlag | kNoFollowSchemaFlag;
  static constexpr int64_t kExplicitSchemaFlag =                 0b111100;
  static constexpr int64_t kUuidExplicitSchemaFlag =             0b111101;
  static constexpr int64_t kNoFollowExplicitSchemaFlag =      // 0b111110
      kExplicitSchemaFlag | kNoFollowSchemaFlag;
  static constexpr int64_t kNoFollowUuidExplicitSchemaFlag =  // 0b111111
      kUuidExplicitSchemaFlag | kNoFollowSchemaFlag;

  // Schema flags invariants in regard to UUIDs:
  static_assert((kUuidImplicitSchemaFlag & kUuidFlag) != 0);
  static_assert((kUuidExplicitSchemaFlag & kUuidFlag) != 0);
  static_assert((kExplicitSchemaFlag & kUuidFlag) == 0);
  static_assert((kExplicitSchemaFlag | kUuidFlag) == kUuidExplicitSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kNoFollowUuidImplicitSchemaFlag) ==
                kNoFollowSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kNoFollowExplicitSchemaFlag) ==
                kNoFollowSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kNoFollowUuidExplicitSchemaFlag) ==
                kNoFollowSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kUuidImplicitSchemaFlag) !=
                kNoFollowSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kExplicitSchemaFlag) !=
                kNoFollowSchemaFlag);
  static_assert((kNoFollowSchemaFlag & kUuidExplicitSchemaFlag) !=
                kNoFollowSchemaFlag);

  // Used in serialization to get the underlying data. There is no guarantee
  // on what these numbers mean.
  uint64_t InternalHigh64() const {
    return *reinterpret_cast<const uint64_t*>(this);
  }
  uint64_t InternalLow64() const { return id_; }
  static ObjectId UnsafeCreateFromInternalHighLow(uint64_t hi, uint64_t lo) {
    ObjectId id;
    *reinterpret_cast<uint64_t*>(&id) = hi;
    id.id_ = lo;
    return id;
  }

 private:
  friend struct AllocationId;
  friend AllocationId Allocate(size_t size);
  friend ObjectId AllocateSingleObject();
  template <int64_t uuid_flag>
  friend ObjectId CreateUuidWithMainObject(ObjectId, arolla::Fingerprint);
  friend ObjectId CreateUuidObjectWithMetadata(arolla::Fingerprint, int64_t);
  friend ObjectId CreateUuidObject(arolla::Fingerprint);
  friend ObjectId CreateUuidExplicitSchema(arolla::Fingerprint);
  friend ObjectId CreateNoFollowWithMainObject(ObjectId);
  friend ObjectId GetOriginalFromNoFollow(ObjectId);
  friend AllocationId AllocateLists(size_t size);
  friend ObjectId AllocateSingleList();
  friend AllocationId AllocateDicts(size_t size);
  friend ObjectId AllocateSingleDict();
  friend ObjectId AllocateExplicitSchema();

#ifndef ABSL_IS_LITTLE_ENDIAN
#error "Unsupported byte order: Only ABSL_IS_LITTLE_ENDIAN is supported"
#endif  // byte order

  // High64

  // 6.5 bytes for machine specific allocator_id.
  // the last bits can be used by allocation_id in case id_ is overflowed.
  uint64_t allocator_id_ : 52;
  // number of bits in id that corresponds to offset in the allocation
  uint64_t offset_bits_ : 6;
  // reserved 6 bits for embedded metadata with the following properties:
  // * lowest bit is 0 for allocated objects and 1 for uuid objects.
  // * lists use 0b000010 (which allows us to use 0b000011 for UUID lists in
  //   the future).
  // * dicts use 0b000100 (which allows us to use 0b000101 for UUID dicts in
  //   the future).
  // * all values >= 0b111011 are reserved for schemas:
  //   * those that end with bit 1 are uuid objects (uuid explicit, implicit and
  //     nofollow schemas).
  //   * 0b111110 and 0b111111 are used for allocated and explicit schema
  //     respectively, so they differ only by kUuidFlag value.
  uint64_t metadata_ : 6;

  // Low64

  // id inside of the allocation.
  // offset_bits lowest bits correspond to offset in the single allocation
  // other bits are part of allocation id.
  uint64_t id_;
};

template <int64_t uuid_flag>
ObjectId CreateUuidWithMainObject(ObjectId main_object_id,
                                  arolla::Fingerprint fp);
inline ObjectId CreateUuidWithMainObject(ObjectId main_object_id,
                                         arolla::Fingerprint fp) {
  return CreateUuidWithMainObject<ObjectId::kUuidFlag>(main_object_id, fp);
}

static_assert(sizeof(ObjectId) == 16);

using ObjectIdArray = ::arolla::DenseArray<ObjectId>;

// AllocationId represents group of ObjectId that were allocated together.
//
// AllocationId is always embedded into ObjectId.
// Koladata library has general assumption that objects allocated together will
// commonly be used together.
class AllocationId {
 public:
  // There is no zero initialization for performance reasons.
  AllocationId() = default;

  // Creates allocation id from any representative ObjectId.
  explicit AllocationId(ObjectId obj_id) {
    allocation_id_ = obj_id;
    // clean last `offset_bits` of id.
    allocation_id_.id_ =
        ((obj_id.id_ >> obj_id.offset_bits_) << obj_id.offset_bits_);
  }

  // Return true if AllocationId is small. That could be used as
  // an optimization for internal storage.
  bool IsSmall() const { return allocation_id_.IsSmallAlloc(); }

  // Returns true if the allocation is a lists allocation.
  bool IsListsAlloc() const { return allocation_id_.IsList(); }

  // Returns true if the allocation is a dicts allocation.
  bool IsDictsAlloc() const { return allocation_id_.IsDict(); }

  // Returns true if the allocation is a schemas allocation.
  bool IsSchemasAlloc() const { return allocation_id_.IsSchema(); }

  // Returns true if the allocation is an explicit schemas allocation.
  bool IsExplicitSchemasAlloc() const {
    return allocation_id_.IsExplicitSchema();
  }

  // Returns capacity of this allocation.
  size_t Capacity() const { return (1ull << allocation_id_.offset_bits_); }

  // Returns true if object belongs to the allocation.
  bool Contains(const ObjectId& obj_id) const {
    return allocation_id_.InternalHigh64() == obj_id.InternalHigh64() &&
           ((obj_id.InternalLow64() ^ allocation_id_.InternalLow64()) >>
            allocation_id_.offset_bits_) == 0;
  }

  // Returns ObjectId with given offset within this allocation.
  ObjectId ObjectByOffset(int64_t offset) const {
    DCHECK_LT(offset, Capacity());
    ObjectId result = allocation_id_;
    result.id_ |= offset;
    return result;
  }

  friend auto operator<=>(const AllocationId& lhs,
                          const AllocationId& rhs) = default;

  friend std::ostream& operator<<(std::ostream& os, const AllocationId& id) {
    os << id.allocation_id_;
    return os;
  }

  template <typename H>
  friend H AbslHashValue(H h, const AllocationId& alloc) {
    return H::combine(std::move(h), alloc.allocation_id_);
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AllocationId& alloc) {
    sink.Append(alloc.allocation_id_.DebugString());
  }

 private:
  friend AllocationId AllocateLists(size_t size);
  friend AllocationId AllocateDicts(size_t size);

  ObjectId allocation_id_;
};

// Returns newly allocated ObjectId.
ObjectId AllocateSingleObject();

// Returns new allocation id for the size objects.
AllocationId Allocate(size_t size);

// Returns ObjectId of a newly created empty list.
inline ObjectId AllocateSingleList() {
  ObjectId res = AllocateSingleObject();
  res.metadata_ |= ObjectId::kListFlag;
  return res;
}

// Returns new lists allocation.
inline AllocationId AllocateLists(size_t size) {
  AllocationId res = Allocate(size);
  res.allocation_id_.metadata_ |= ObjectId::kListFlag;
  return res;
}

// Returns ObjectId of a newly created empty dict.
inline ObjectId AllocateSingleDict() {
  ObjectId res = AllocateSingleObject();
  res.metadata_ |= ObjectId::kDictFlag;
  return res;
}

// Returns new dicts allocation.
inline AllocationId AllocateDicts(size_t size) {
  AllocationId res = Allocate(size);
  res.allocation_id_.metadata_ |= ObjectId::kDictFlag;
  return res;
}

// Returns ObjectId of a newly created explicit schema.
inline ObjectId AllocateExplicitSchema() {
  ObjectId res = AllocateSingleObject();
  res.metadata_ |= ObjectId::kExplicitSchemaFlag;
  return res;
}

// Returns UUID object from the provided FingerPrint
// and based on the main object.
// FingerPrint must be mixed with AllocationId of the main_object.
// Resulted object will have the same offset and allocation capacity
// as the main object. Resulted `AllocationId` will be deterministically
// defined from `AllocationId` of the main object and fingerprint.
// So calling `CreateUuidWithMainObject` with two objects with the same
// `AllocationId` and the same fingerprint will result with objects from the
// same `AllocationId`.
// IsUuid() will be true.
//
// `uuid_flag` can be any of the pre-defined UUID flags, e.g.
// kUuidImplicitSchemaFlag or kUuidNoFollowSchemaFlag. In this case, appropriate
// ObjectId properties may also be true, e.g. IsSchema(), IsImplicitSchema(),
// etc.
//
// Passing non-UUID flag for `uuid_flag` is not possible.
template <int64_t uuid_flag>
ObjectId CreateUuidWithMainObject(ObjectId main_object_id,
                                  arolla::Fingerprint fp) {
  static_assert((uuid_flag & ObjectId::kUuidFlag) == ObjectId::kUuidFlag);
  static_assert(sizeof(ObjectId) == sizeof(arolla::Fingerprint));
  ObjectId id;
  std::memcpy(&id, reinterpret_cast<const void*>(&fp), sizeof(ObjectId));
  // Replace metadata and offset bits. We assume that bits are well distributed.
  id.metadata_ = uuid_flag;
  id.offset_bits_ = main_object_id.offset_bits_;
  // Replace offset with provided from main_object_id.
  size_t mask = (1ull << id.offset_bits_) - 1;
  id.id_ ^= (main_object_id.id_ ^ id.id_) & mask;
  return id;
}

inline ObjectId CreateUuidObjectWithMetadata(arolla::Fingerprint fp,
                                             int64_t flag) {
  static_assert(sizeof(ObjectId) == sizeof(arolla::Fingerprint));
  ObjectId id;
  std::memcpy(&id, reinterpret_cast<const void*>(&fp), sizeof(ObjectId));
  // Replace metadata and offset bits. We assume that bits are well distributed.
  id.metadata_ = flag;
  id.offset_bits_ = 0;
  return id;
}

// Returns UUID object from the provided FingerPrint.
// IsUuid() will be true. AllocationId capacity will be 1.
inline ObjectId CreateUuidObject(arolla::Fingerprint fp) {
  return CreateUuidObjectWithMetadata(fp, ObjectId::kUuidFlag);
}

// Returns UUID Explicit Schema object from the provided FingerPrint.
// IsUuid(), IsSchema() and IsExplicitSchema() will all be true. AllocationId
// capacity will be 1.
inline ObjectId CreateUuidExplicitSchema(arolla::Fingerprint fp) {
  return CreateUuidObjectWithMetadata(fp, ObjectId::kUuidExplicitSchemaFlag);
}

inline ObjectId CreateNoFollowWithMainObject(ObjectId main_object_id) {
  DCHECK(main_object_id.IsSchema());
  ObjectId id = main_object_id;
  id.metadata_ |= ObjectId::kNoFollowSchemaFlag;
  return id;
}

inline ObjectId GetOriginalFromNoFollow(ObjectId nofollow_object_id) {
  DCHECK(nofollow_object_id.IsNoFollowSchema() &&
         nofollow_object_id != ObjectId::NoFollowObjectSchemaId());
  ObjectId id = nofollow_object_id;
  static_assert((ObjectId::kNoFollowSchemaFlag & ~ObjectId::kStartSchemaFlag) ==
                0b000010);
  // Clear NoFollow bit.
  id.metadata_ &=
      ~(ObjectId::kNoFollowSchemaFlag & ~ObjectId::kStartSchemaFlag);
  return id;
}

// Represents set of unique allocation ids.
class AllocationIdSet {
 public:
  using value_type = AllocationId;

  AllocationIdSet() = default;

  explicit AllocationIdSet(bool contains_small_allocation_id)
      : contains_small_allocation_id_(contains_small_allocation_id) {}

  explicit AllocationIdSet(AllocationId id)
      : contains_small_allocation_id_(id.IsSmall()),
        ids_(contains_small_allocation_id_ ? 0 : 1, id) {}

  explicit AllocationIdSet(absl::Span<const AllocationId> ids)
      : ids_(ids.begin(), ids.end()) {
    auto new_end = std::remove_if(ids_.begin(), ids_.end(),
                                  [](AllocationId id) { return id.IsSmall(); });
    contains_small_allocation_id_ = new_end != ids_.end();
    if (new_end - ids_.begin() > 1) {
      std::sort(ids_.begin(), new_end);
      ids_.erase(std::unique(ids_.begin(), new_end), ids_.end());
    } else {
      ids_.erase(new_end, ids_.end());
    }
  }

  // Insert id and return true if it is a new element.
  bool Insert(AllocationId id) {
    if (id.IsSmall()) {
      return !std::exchange(contains_small_allocation_id_, true);
    }
    if (ABSL_PREDICT_FALSE(ids_.empty())) {
      ids_.emplace_back(id);
      return true;
    }
    if (ABSL_PREDICT_TRUE(ids_[0] == id)) {
      return false;
    }
    return InsertBigAllocationSlow(id);
  }

  void Insert(const AllocationIdSet& new_ids);

  // Returns number of big AllocationIds.
  size_t size() const { return ids_.size(); }

  // Returns true if size() == 0.
  bool empty() const { return ids_.empty(); }

  // Returns iterators for big AllocationIds.
  auto begin() const { return ids_.begin(); }
  auto end() const { return ids_.end(); }

  // Returns true if set contains small AllocationId.
  bool contains_small_allocation_id() const {
    return contains_small_allocation_id_;
  }

  void InsertSmallAllocationId() { contains_small_allocation_id_ = true; }

  // Returns AllocationIds in the set in unspecified order.
  absl::Span<const AllocationId> ids() const { return ids_; }

  friend bool operator==(const AllocationIdSet& lhs,
                         const AllocationIdSet& rhs);

  friend bool operator!=(const AllocationIdSet& lhs,
                         const AllocationIdSet& rhs);

  std::string DebugString() const;

  friend std::ostream& operator<<(std::ostream& os,
                                  const AllocationIdSet& id_set);

  template <typename H>
  friend H AbslHashValue(H h, const AllocationIdSet& id_set) {
    H res = H::combine(std::move(h), id_set.contains_small_allocation_id_);
    for (const auto& id : id_set.ids_) {
      res = H::combine(std::move(res), id);
    }
    return res;
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const AllocationIdSet& id_set) {
    sink.Append(id_set.DebugString());
  }

 private:
  bool InsertBigAllocationSlow(AllocationId id);

  bool contains_small_allocation_id_ = false;
  absl::InlinedVector<AllocationId, 1> ids_;  // sorted set of allocations
};

}  // namespace koladata::internal

namespace arolla {

template <>
struct ReprTraits<::koladata::internal::ObjectId> {
  ReprToken operator()(const ::koladata::internal::ObjectId& value) const {
    return ReprToken{value.DebugString()};
  }
};

AROLLA_DECLARE_SIMPLE_QTYPE(OBJECT_ID, ::koladata::internal::ObjectId);

}  // namespace arolla

#endif  // KOLADATA_INTERNAL_OBJECT_ID_H_
