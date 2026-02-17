// Copyright 2025 Google LLC
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
#include "koladata/internal/object_id.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/numeric/bits.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "arolla/qtype/simple_qtype.h"
#include "koladata/internal/pseudo_random.h"

namespace koladata::internal {
namespace {

uint64_t AllocatorId() {
  // Only 52 bits matter in the result.
  return PseudoRandomEpochId() & ((1ull << 52) - 1);
}

}  // namespace

ObjectId ObjectId::NoFollowObjectSchemaId() {
  ObjectId id;
  id.allocator_id_ = 0;
  id.offset_bits_ = 0;
  id.id_ = 0;
  id.metadata_ = kNoFollowObjectSchemaFlag;
  return id;
}

std::string ObjectId::DebugString() const {
  return ObjectIdStr(*this, /*show_flag_prefix=*/true);
}

AllocationId Allocate(size_t size) {
  if (size <= 1) {
    return AllocationId(AllocateSingleObject());
  }
  ObjectId id;
  id.metadata_ = 0;
  id.allocator_id_ = AllocatorId();
  // capacity is the closest power of 2 that fit size elements
  id.offset_bits_ = 64 - absl::countl_zero(size - 1);

  // We do not allow allocating more than 1000 billions elements.
  // Array of ObjectId of such size would take 16 terabytes of RAM.
  constexpr uint64_t kMaxOffsetBits = 40;
  CHECK_LE(id.offset_bits_, kMaxOffsetBits)
      << "allocations larger than 2**" << kMaxOffsetBits
      << " are not supported.";

  static std::array<std::atomic_uint64_t, kMaxOffsetBits + 1>
      global_thread_id_per_offset_ = {0};

  // Returns number of bits (K) reserved for thread local allocation id
  // We allocate 2**K ids consequently using thread_local storage.
  // After that we grab the next thread_id using atomic operation.
  // Larger the allocation, more often we have to lookup to global storage.
  // 5 <= K <= 15, E.g., for allocation of size 1000, K = 13.
  constexpr auto local_allocation_bit_count = [](uint32_t offset_bits) {
    return (64 - offset_bits) / 4;
  };

  constexpr auto compute_thead_id_fn =
      [local_allocation_bit_count](uint32_t offset_bits) {
        uint64_t id = global_thread_id_per_offset_[offset_bits].fetch_add(
            1, std::memory_order_relaxed);

        // id_bit_count is reserved for local allocation and offset bits.
        uint64_t id_bit_count =
            local_allocation_bit_count(offset_bits) + offset_bits;

        return std::make_pair(
            // id_bit_count highest bits are added to allocator_id_
            // as lowest bits.
            id >> (64 - id_bit_count),
            // (64-id_bit_count) lowest bits are added to id_ as highest
            // bits.
            id << id_bit_count);
      };

  thread_local std::array<std::pair<uint64_t, uint64_t>, kMaxOffsetBits + 1>
      thread_id_per_offset_;

  thread_local std::array<uint64_t, kMaxOffsetBits + 1>
      allocation_id_per_offset_ = {0};

  uint64_t& allocation_id = allocation_id_per_offset_[id.offset_bits_];
  if (ABSL_PREDICT_FALSE(allocation_id == 0)) {
    thread_id_per_offset_[id.offset_bits_] =
        compute_thead_id_fn(id.offset_bits_);
  }

  // id_ has the following bit structure:
  // `|thread_id|allocation_id_in_this_thread|offset|`
  // offset has exactly `offset_bits_` 0 bits
  // allocation_id_in_this_thread has `local_allocation_bit_count(offset_bits_)`
  // thread_id fills the rest of the bits
  id.id_ = (allocation_id << id.offset_bits_) |
           thread_id_per_offset_[id.offset_bits_].second;
  // add overflow bits of thread_id to allocator_id_
  id.allocator_id_ += thread_id_per_offset_[id.offset_bits_].first;

  allocation_id = (allocation_id + 1) &
                  ((1ull << local_allocation_bit_count(id.offset_bits_)) - 1);

  return AllocationId(id);
}

ObjectId AllocateSingleObject() {
  ObjectId id;
  // Fill metadata_ and offset_bits_ as 0s.
  std::memset(&id, 0, sizeof(uint64_t));

  // We allocate 2**kIdBitCount ids consequently.
  // After that we grab the next thread_id using atomic operation.
  constexpr uint64_t kIdBitCount = 24;

  static std::atomic_uint64_t global_thread_id_ = 0;
  constexpr auto compute_thead_id_fn = []() {
    uint64_t id = global_thread_id_.fetch_add(1, std::memory_order_relaxed);

    return std::make_pair(
        // kIdBitCount highest bits are added to allocator_id_
        // as lowest bits.
        id >> (64 - kIdBitCount),
        // (64-kIdBitCount) lowest bits are added to id_ as highest bits.
        id << kIdBitCount);
  };

  thread_local std::pair<uint64_t, uint64_t> thread_id_ = compute_thead_id_fn();
  thread_local uint64_t allocation_id_ = 0;

  id.allocator_id_ = AllocatorId() + thread_id_.first;
  // id_ has the following bit structure:
  // `|thread_id|allocation_id_in_this_thread|`
  // allocation_id_in_this_thread has `kIdBitCount` bits
  // thread_id fills the rest of the bits
  id.id_ = allocation_id_ | thread_id_.second;
  ++allocation_id_;
  if (ABSL_PREDICT_FALSE(allocation_id_ == (1ull << kIdBitCount))) {
    allocation_id_ = 0;
    thread_id_ = compute_thead_id_fn();
  }

  return id;
}

bool AllocationIdSet::InsertBigAllocationSlow(AllocationId id) {
  auto sorted_end =
      ids_.size() > kMaxSortedSize ? ids_.begin() + kMaxSortedSize : ids_.end();
  auto sorted_it = std::lower_bound(ids_.begin(), sorted_end, id);
  if (sorted_it != sorted_end && *sorted_it == id) {
    return false;
  }
  if (ids_.size() <= kMaxSortedSize) {
    ids_.insert(sorted_it, id);
    if (ids_.size() > kMaxSortedSize) {
      unsorted_ids_.emplace();
      // Last element is outside of the sorted range.
      unsorted_ids_->insert(ids_.back());
    }
    return true;
  }
  if (unsorted_ids_->insert(id).second) {
    ids_.push_back(id);
    return true;
  }
  return false;
}

void AllocationIdSet::Insert(const AllocationIdSet& new_ids) {
  contains_small_allocation_id_ |= new_ids.contains_small_allocation_id_;
  if (ids_.empty()) {
    ids_.assign(new_ids.begin(), new_ids.end());
    unsorted_ids_ = new_ids.unsorted_ids_;
    return;
  }
  size_t offset = 0;
  size_t min_size = std::min(ids_.size(), new_ids.size());
  while (offset != min_size && ids_[offset] == new_ids.ids_[offset]) {
    ++offset;
  }
  if (offset == new_ids.size()) {
    return;
  }
  ids_.insert(ids_.end(), new_ids.ids_.begin() + offset, new_ids.ids_.end());
  // We may have many ids here, but after duplicate removal we may stay in the
  // small mode.
  if (ids_.size() <= kMaxSortedSize) {
    std::inplace_merge(ids_.begin() + offset,
                       ids_.end() - (new_ids.size() - offset), ids_.end());
    ids_.erase(std::unique(ids_.begin() + offset, ids_.end()), ids_.end());
  } else {
    std::sort(ids_.begin(), ids_.end());
    ids_.erase(std::unique(ids_.begin(), ids_.end()), ids_.end());
  }
  // Check whether we are in big mode after duplicate removal.
  if (ids_.size() > kMaxSortedSize) {
    if (!unsorted_ids_.has_value()) {
      unsorted_ids_.emplace();
    }
    // Note that unsorted_ids_ may contain leftover ids that are within sorted
    // range. It is fine.
    for (size_t i = kMaxSortedSize; i < ids_.size(); ++i) {
      unsorted_ids_->insert(ids_[i]);
    }
  }
}

bool operator==(const AllocationIdSet& lhs, const AllocationIdSet& rhs) {
  bool fast_check =
      lhs.contains_small_allocation_id_ == rhs.contains_small_allocation_id_ &&
      lhs.ids_.size() == rhs.ids_.size();
  if (!fast_check) {
    return false;
  }
  DCHECK(lhs.ids_.size() == rhs.ids_.size());
  if (lhs.ids_.size() > AllocationIdSet::kMaxSortedSize) {
    DCHECK_GT(rhs.ids_.size(), AllocationIdSet::kMaxSortedSize);
    absl::flat_hash_set<AllocationId> lhs_set(*lhs.unsorted_ids_);
    for (size_t i = 0; i != AllocationIdSet::kMaxSortedSize; ++i) {
      lhs_set.insert(lhs.ids_[i]);
    }
    for (const AllocationId& id : rhs.ids_) {
      if (!lhs_set.contains(id)) {
        return false;
      }
    }
    return true;
  } else {
    DCHECK_LE(rhs.ids_.size(), AllocationIdSet::kMaxSortedSize);
    return lhs.ids_ == rhs.ids_;
  }
}

std::string AllocationIdSet::DebugString() const {
  return absl::StrCat(contains_small_allocation_id_, " {",
                      absl::StrJoin(ids_, ","), "}");
}

std::ostream& operator<<(std::ostream& os, const AllocationIdSet& id_set) {
  os << absl::StrCat(id_set.DebugString());
  return os;
}

}  // namespace koladata::internal

namespace arolla {
AROLLA_DEFINE_SIMPLE_QTYPE(OBJECT_ID, ::koladata::internal::ObjectId);
}  // namespace arolla
