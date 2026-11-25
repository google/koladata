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
#include "koladata/data_bag.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "koladata/data_bag_repr.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/object_id.h"

namespace koladata {

const DataBagPtr& DataBag::GetStaticEmpty() {
  static const absl::NoDestructor<DataBagPtr> empty_bag(DataBag::Empty());
  return *empty_bag;
}

absl::StatusOr<DataBagPtr> DataBag::ImmutableEmptyWithFallbacks(
    absl::Span<const DataBagPtr absl_nullable> fallbacks) {
  auto res = DataBagPtr::Make(DataBag::immutable_t());
  std::vector<DataBagPtr> non_null_fallbacks;
  non_null_fallbacks.reserve(fallbacks.size());
  for (const DataBagPtr& fb : fallbacks) {
    if (fb != nullptr) {
      if (fb->IsMutable()) {
        return absl::InvalidArgumentError("fallbacks must be immutable");
      }
      non_null_fallbacks.push_back(fb);
    }
  }
  res->fallbacks_ = std::move(non_null_fallbacks);
  return res;
}

DataBagPtr DataBag::FallbackFreeFork(bool immutable) {
  DCHECK(fallbacks_.empty());
  DataBagPtr new_db;
  if (immutable) {
    new_db = DataBagPtr::Make(DataBag::immutable_t());
  } else {
    new_db = DataBagPtr::Make();
  }
  new_db->impl_ = impl_->PartiallyPersistentFork();
  new_db->impl_->AssignToDataBag();
  impl_->Freeze();
  if (immutable) {
    new_db->impl_->Freeze();
  }

  // If the original DataBag is mutable, we need to assign a new implementation
  // to it, because it can be modified and the modifications will affect the
  // new DataBag.
  // We do it lazily to ensure thread safety: the new implementation is only
  // assigned when GetMutableImpl() is called. Clients are expected to ensure
  // thread safety for GetMutableImpl() calls externally.
  if (is_mutable_) {
    forked_ = true;
  }
  return new_db;
}

absl::StatusOr<DataBagPtr> DataBag::Fork(bool immutable) {
  if (!fallbacks_.empty()) {
    return absl::FailedPreconditionError(
        "forking with fallbacks is not supported. Please merge fallbacks "
        "instead.");
  }
  return FallbackFreeFork(immutable);
}

DataBagPtr DataBag::Freeze() {
  if (IsMutable()) {
    DCHECK(GetFallbacks().empty())
        << "DataBag with fallbacks cannot be mutable.";
    return FallbackFreeFork(/*immutable=*/true);
  }
  return DataBagPtr::NewRef(this);
}

DataBagPtr DataBag::FromImpl(internal::DataBagImplPtr impl) {
  auto res = DataBagPtr::Make();
  res->impl_ = std::move(impl);
  res->impl_->AssignToDataBag();
  return res;
}

namespace {

constexpr absl::string_view kDataBagQValueSpecializationKey =
    "::koladata::python::DataBag";

constexpr absl::string_view kNullDataBagQValueSpecializationKey =
    "::koladata::python::NullDataBag";

absl::StatusOr<internal::DataBagImplPtr> MergeFallbacksToForkedImpl(
    const DataBag& db) {
  DCHECK(db.GetImpl().IsFrozen());
  auto forked_impl = db.GetImpl().PartiallyPersistentFork();
  FlattenFallbackFinder fallback_finder(db);
  auto keep_original = internal::MergeOptions{
      .data_conflict_policy = internal::MergeOptions::kKeepOriginal,
      .schema_conflict_policy = internal::MergeOptions::kKeepOriginal};
  for (const auto& fallback : fallback_finder.GetFlattenFallbacks()) {
    RETURN_IF_ERROR(forked_impl->MergeInplace(*fallback, keep_original));
  }
  return forked_impl;
}

// Tracks cumulative merged input weight for `ImmutableCompactWithFallbacks`:
// - Fresh leaves initialize to `max(1, GetApproxTotalSize())` rather than 1 so
//   small deltas of total weight d << N onto a large N-triple base bag cost
//   O(d log T) without merging the base bag (achieving
//   O(sum_i w_i (1 + log(W / w_i))) instead of O(W log T)). Byte size
//   (`GetApproxTotalByteSize()`) would better reflect large string/bytes
//   payloads, but scanning items to sum string lengths is too expensive here.
// - Merged tiers inherit the sum of their inputs' weights rather than
//   recomputing deduplicated size: each tier's weight grows >= 1.5x per merge
//   (avoiding O(N * T) re-merging when overwrites keep live size < 2 * delta,
//   at the cost of O(log W) space amplification on overwritten keys) and skips
//   rescanning `merged_bag`. Under tree/linear accumulation total weight equals
//   the sum of leaf sizes, while saturating addition below guards against
//   exponential weight doubling across diamond DAGs of distinct merged bags.
struct CompactionWeight {
  int64_t weight;
};

const internal::ObjectId& CompactionWeightCacheKey() {
  static const internal::ObjectId kKey = internal::AllocateSingleObject();
  return kKey;
}

int64_t GetOrInitCompactionWeight(DataBag& bag) {
  DCHECK(!bag.IsMutable());
  DCHECK(bag.GetFallbacks().empty());
  const internal::ObjectId& key = CompactionWeightCacheKey();
  if (auto cached = bag.GetCachedMetadataOrNull<CompactionWeight>(key);
      cached != nullptr) {
    return cached->weight;
  }
  int64_t weight = std::max<int64_t>(1, bag.GetImpl().GetApproxTotalSize());
  return bag
      .SetCachedMetadata<CompactionWeight>(key, CompactionWeight{weight})
      ->weight;
}

}  // namespace

absl::StatusOr<DataBagPtr> DataBag::ImmutableCompactWithFallbacks(
    absl::Span<const DataBagPtr absl_nullable> fallbacks, bool compact_tail) {
  ASSIGN_OR_RETURN(DataBagPtr root, ImmutableEmptyWithFallbacks(fallbacks));
  std::vector<DataBagPtr> leaves;
  leaves.reserve(root->GetFallbacks().size());
  VisitFallbacks(*root, [&](DataBagPtr cur) {
    if (!cur->GetImpl().IsEmpty()) {
      DCHECK(cur->GetFallbacks().empty());
      leaves.push_back(std::move(cur));
    }
  });

  if (leaves.empty()) {
    return DataBag::Empty();
  }
  if (leaves.size() == 1) {
    return std::move(leaves[0]);
  }

  struct WeightedBag {
    DataBagPtr bag;
    int64_t weight;
  };
  // Earlier tiers have higher priority when `compact_tail` is true; otherwise
  // leaves are pushed in reverse so later tiers have higher priority.
  const auto conflict_policy = compact_tail
                                   ? internal::MergeOptions::kKeepOriginal
                                   : internal::MergeOptions::kOverwrite;
  const internal::MergeOptions merge_options{
      .data_conflict_policy = conflict_policy,
      .schema_conflict_policy = conflict_policy};
  std::vector<WeightedBag> tiers;
  tiers.reserve(leaves.size());

  auto push_and_compact = [&](DataBagPtr leaf) -> absl::Status {
    int64_t leaf_weight = GetOrInitCompactionWeight(*leaf);
    tiers.push_back(WeightedBag{std::move(leaf), leaf_weight});
    int64_t merged_weight = tiers.back().weight;
    size_t merge_from = tiers.size() - 1;
    while (merge_from > 0 &&
           (merged_weight > std::numeric_limits<int64_t>::max() / 2 ||
            tiers[merge_from - 1].weight < 2 * merged_weight)) {
      --merge_from;
      if (merged_weight <=
          std::numeric_limits<int64_t>::max() - tiers[merge_from].weight) {
        merged_weight += tiers[merge_from].weight;
      } else {
        merged_weight = std::numeric_limits<int64_t>::max();
      }
    }
    if (merge_from == tiers.size() - 1) {
      return absl::OkStatus();
    }
    auto merged_impl = internal::DataBagImpl::CreateEmptyDatabag();
    for (size_t j = merge_from; j < tiers.size(); ++j) {
      RETURN_IF_ERROR(
          merged_impl->MergeInplace(tiers[j].bag->GetImpl(), merge_options));
    }
    DataBagPtr merged_bag = FromImpl(std::move(merged_impl));
    merged_bag->UnsafeMakeImmutable();
    merged_bag->SetCachedMetadata<CompactionWeight>(
        CompactionWeightCacheKey(), CompactionWeight{merged_weight});
    tiers.resize(merge_from);
    tiers.push_back(WeightedBag{std::move(merged_bag), merged_weight});
    return absl::OkStatus();
  };

  if (compact_tail) {
    for (DataBagPtr& leaf : leaves) {
      RETURN_IF_ERROR(push_and_compact(std::move(leaf)));
    }
  } else {
    for (auto it = leaves.rbegin(); it != leaves.rend(); ++it) {
      RETURN_IF_ERROR(push_and_compact(std::move(*it)));
    }
  }

  if (tiers.size() == 1) {
    return std::move(tiers[0].bag);
  }
  std::vector<DataBagPtr> compact_fallbacks;
  compact_fallbacks.reserve(tiers.size());
  if (compact_tail) {
    for (WeightedBag& tier : tiers) {
      compact_fallbacks.push_back(std::move(tier.bag));
    }
  } else {
    for (auto it = tiers.rbegin(); it != tiers.rend(); ++it) {
      compact_fallbacks.push_back(std::move(it->bag));
    }
  }
  return ImmutableEmptyWithFallbacks(compact_fallbacks);
}

absl::StatusOr<DataBagPtr> DataBag::MergeFallbacks() {
  if (fallbacks_.empty()) {
    return FallbackFreeFork();
  }
  DCHECK(!is_mutable_);  // a bag with fallbacks can't be mutable
  ASSIGN_OR_RETURN(auto impl_fork, MergeFallbacksToForkedImpl(*this));
  return FromImpl(std::move(impl_fork));
}

absl::Status DataBag::MergeInplace(const DataBagPtr& other_db, bool overwrite,
                                   bool allow_data_conflicts,
                                   bool allow_schema_conflicts) {
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_impl, GetMutableImpl());
  auto conflict_side = overwrite ? internal::MergeOptions::kOverwrite
                                 : internal::MergeOptions::kKeepOriginal;
  internal::MergeOptions merge_options;
  if (allow_data_conflicts) {
    merge_options.data_conflict_policy = conflict_side;
  }
  if (allow_schema_conflicts) {
    merge_options.schema_conflict_policy = conflict_side;
  }
  if (other_db->GetFallbacks().empty()) {
    return db_impl.MergeInplace(other_db->GetImpl(), merge_options);
  }
  ASSIGN_OR_RETURN(auto other_db_impl, MergeFallbacksToForkedImpl(*other_db));
  other_db_impl->Freeze();
  return db_impl.MergeInplace(*other_db_impl, merge_options);
}

absl::StatusOr<DataBagPtr> DataBag::CreateOverwritingMergeUpdate(
    const DataBagPtr& other_db) const {
  if (!GetFallbacks().empty()) {
    return absl::InvalidArgumentError(
        "DataBag shouldn't have fallbacks for overwriting merge update. "
        "Use merge_fallbacks() before creating update.");
  }
  const auto& db = GetImpl();
  auto update_impl = [&]() -> absl::StatusOr<internal::DataBagImplPtr> {
    if (other_db->GetFallbacks().empty()) {
      return db.CreateOverwritingMergeUpdate(other_db->GetImpl());
    }
    ASSIGN_OR_RETURN(auto other_db_impl, MergeFallbacksToForkedImpl(*other_db));
    return db.CreateOverwritingMergeUpdate(*other_db_impl);
  };
  ASSIGN_OR_RETURN(auto update, update_impl());
  auto update_db = FromImpl(std::move(update));
  update_db->UnsafeMakeImmutable();
  return update_db;
}

void VisitFallbacks(const DataBag& bag,
                    absl::FunctionRef<void(DataBagPtr)> visit_fn) {
  std::vector<DataBagPtr> stack(bag.GetFallbacks().rbegin(),
                                bag.GetFallbacks().rend());
  absl::flat_hash_set<const DataBag*> seen_db;
  seen_db.reserve(stack.size() + 1);
  seen_db.insert(&bag);

  while (!stack.empty()) {
    DataBagPtr fallback = stack.back();
    DCHECK(fallback != nullptr);
    stack.pop_back();
    if (seen_db.insert(fallback.get()).second) {
      const auto cur_fallbacks = fallback->GetFallbacks();
      for (auto it = cur_fallbacks.rbegin(); it != cur_fallbacks.rend(); ++it) {
        stack.push_back(*it);
      }
      visit_fn(std::move(fallback));
    }
  }
}

std::string GetBagIdRepr(const DataBagPtr& db) {
  DCHECK_NE(db, nullptr);
  std::string fp_hex =
      arolla::TypedValue::FromValue(db).GetFingerprint().AsString();
  // Use 4 hex digits as a compromise between simplicity and risk of conflicts,
  // which results in ~2^8 DataBags needed for 50% probability of a clash.
  return absl::StrCat("$", absl::string_view(fp_hex).substr(fp_hex.size() - 4));
}

// Non-recursive destruction to avoid stack overflows on deep fallbacks.
DataBag::~DataBag() {
  if (fallbacks_.empty()) {
    return;
  }
  constexpr size_t kMaxDepth = 32;

  // Array for postponing removing fallbacks.
  //
  // NOTE(b/343432263): NoDestructor is used to avoid issues with the
  // destruction order of globals vs thread_locals.
  thread_local absl::NoDestructor<std::deque<std::vector<DataBagPtr>>>
      fallbacks;

  // The first destructed node will perform clean up of postponed removals.
  thread_local size_t destructor_depth = 0;

  if (destructor_depth > kMaxDepth) {
    // Postpone removing to avoid deep recursion.
    fallbacks->push_back(std::move(fallbacks_));
    return;
  }

  destructor_depth++;
  absl::Cleanup decrease_depth = [&] { --destructor_depth; };
  // Will cause calling ~DataBag for fallbacks_ with increased destructor_depth.
  fallbacks_.clear();

  if (destructor_depth == 1 && !fallbacks->empty()) {
    while (!fallbacks->empty()) {
      // Move out the first element of `fallbacks`.
      // Destructor may cause adding more elements to `fallbacks`.
      auto tmp = std::move(fallbacks->back());
      // `pop_back` will remove empty vector, so
      // `pop_back` will *not* cause any DataBag destructions.
      fallbacks->pop_back();
    }
    // Avoid holding heap memory for standby threads.
    fallbacks->shrink_to_fit();
  }
}

bool DataBag::IsEmpty() const {
  if (!impl_->IsEmpty()) {
    return false;
  }
  FlattenFallbackFinder fallback_finder(*this);
  for (const internal::DataBagImpl* fallback :
       fallback_finder.GetFlattenFallbacks()) {
    if (!fallback->IsEmpty()) {
      return false;
    }
  }
  return true;
}

int64_t DataBag::GetApproxByteSize() const {
  FlattenFallbackFinder fallback_finder(*this);
  int64_t size = GetImpl().GetApproxTotalByteSize();
  for (const internal::DataBagImpl* fallback :
       fallback_finder.GetFlattenFallbacks()) {
    size += fallback->GetApproxTotalByteSize();
  }
  return size;
}

}  // namespace koladata

namespace arolla {

QTypePtr QTypeTraits<::koladata::DataBagPtr>::type() {
  struct DataBagPtrQType final : SimpleQType {
    DataBagPtrQType() : SimpleQType(
        meta::type<::koladata::DataBagPtr>(), "DATA_BAG") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* source) const final {
      if (*static_cast<const ::koladata::DataBagPtr*>(source) != nullptr) {
        return ::koladata::kDataBagQValueSpecializationKey;
      } else {
        return ::koladata::kNullDataBagQValueSpecializationKey;
      }
    }
  };
  static const absl::NoDestructor<DataBagPtrQType> result;
  return result.get();
}

void FingerprintHasherTraits<::koladata::DataBagPtr>::operator()(
    FingerprintHasher* hasher, const ::koladata::DataBagPtr& value) const {
  if (value != nullptr) {
    hasher->Combine(value->fingerprint());
  } else {
    hasher->Combine(absl::string_view("NullDataBag"));
  }
}

ReprToken ReprTraits<::koladata::DataBagPtr>::operator()(
    const ::koladata::DataBagPtr& value) const {
  if (value == nullptr) {
    return ReprToken("DataBag(null)");
  }
  absl::StatusOr<std::string> statistics = koladata::DataBagStatistics(value);
  if (statistics.ok()) {
    return ReprToken{*std::move(statistics)};
  }
  return ReprToken{std::string(statistics.status().message())};
}

}  // namespace arolla
