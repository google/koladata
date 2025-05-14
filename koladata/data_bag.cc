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
#include "koladata/data_bag.h"

#include <cstddef>
#include <deque>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
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
#include "arolla/util/status_macros_backport.h"

namespace koladata {

DataBagPtr DataBag::ImmutableEmptyWithFallbacks(
    absl::Span<const DataBagPtr> fallbacks) {
  auto res = DataBagPtr::Make(DataBag::immutable_t());
  std::vector<DataBagPtr> non_null_fallbacks;
  non_null_fallbacks.reserve(fallbacks.size());
  for (int i = 0; i < fallbacks.size(); ++i) {
    if (fallbacks[i] != nullptr) {
      non_null_fallbacks.push_back(fallbacks[i]);
      if (fallbacks[i]->IsMutable() || fallbacks[i]->HasMutableFallbacks()) {
        res->has_mutable_fallbacks_ = true;
      }
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
  // TODO: Re-think forking in the context of DataBag with
  // mutable fallbacks.
  if (!fallbacks_.empty()) {
    return absl::FailedPreconditionError(
        "forking with fallbacks is not supported. Please merge fallbacks "
        "instead.");
  }
  return FallbackFreeFork(immutable);
}

DataBagPtr DataBag::FreezeWithFallbacks() {
  std::vector<DataBagPtr> leaf_fallbacks;
  leaf_fallbacks.reserve(GetFallbacks().size());
  VisitFallbacks(*this, [&leaf_fallbacks](const DataBagPtr fallback) {
    // TODO: DCHECK that non-leaf fallbacks are empty?
    if (fallback->GetFallbacks().empty()) {
      leaf_fallbacks.push_back(fallback->FallbackFreeFork(/*immutable=*/true));
    }
  });
  for (auto& fallback : leaf_fallbacks) {
    if (fallback->IsMutable()) {
      // Since a leaf fallback has no fallbacks by definition, we can call
      // FallbackFreeFork on it.
      fallback = fallback->FallbackFreeFork(/*immutable=*/true);
    }
  }
  return DataBag::ImmutableEmptyWithFallbacks(std::move(leaf_fallbacks));
}

DataBagPtr DataBag::Freeze() {
  if (IsMutable() || !GetFallbacks().empty()) {
    if (GetFallbacks().empty()) {
      return FallbackFreeFork(/*immutable=*/true);
    } else {
      return FreezeWithFallbacks();
    }
  }
  return DataBagPtr::NewRef(this);
}

DataBagPtr DataBag::CommonDataBag(absl::Span<const DataBagPtr> databags) {
  if (databags.size() == 1) {
    return databags.back();
  }
  if (databags.empty()) {
    return nullptr;
  }
  std::vector<DataBagPtr> non_null_databags;
  non_null_databags.reserve(databags.size());
  absl::flat_hash_set<const DataBag*> visited_bags;
  visited_bags.reserve(databags.size());
  for (const auto& db : databags) {
    if (db != nullptr && !visited_bags.contains(db.get())) {
      visited_bags.insert(db.get());
      non_null_databags.push_back(db);
    }
  }
  if (non_null_databags.size() == 1) {
    return std::move(non_null_databags.back());
  }
  if (non_null_databags.empty()) {
    return nullptr;
  }
  return ImmutableEmptyWithFallbacks(non_null_databags);
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

}  // namespace

absl::StatusOr<DataBagPtr> DataBag::MergeFallbacks() {
  ASSIGN_OR_RETURN(auto impl_fork, MergeFallbacksToForkedImpl(*this));
  // Make sure that modifications to the new DataBag don't affect the original.
  this->impl_ = this->impl_->PartiallyPersistentFork();
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
  return db_impl.MergeInplace(*other_db_impl, merge_options);
}

void VisitFallbacks(const DataBag& bag,
                    absl::FunctionRef<void(const DataBagPtr&)> visit_fn) {
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
      visit_fn(fallback);
      const auto cur_fallbacks = fallback->GetFallbacks();
      for (auto it = cur_fallbacks.rbegin(); it != cur_fallbacks.rend(); ++it) {
        stack.push_back(*it);
      }
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
    return ReprToken{statistics.value()};
  }
  return ReprToken{std::string(statistics.status().message())};
}

}  // namespace arolla
