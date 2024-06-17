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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/triples.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

DataBagPtr DataBag::ImmutableEmptyWithFallbacks(
    std::vector<DataBagPtr> fallbacks) {
  auto res = std::make_shared<DataBag>(DataBag::immutable_t());
  std::vector<DataBagPtr> non_null_fallbacks;
  non_null_fallbacks.reserve(fallbacks.size());
  for (int i = 0; i < fallbacks.size(); ++i) {
    if (fallbacks[i] != nullptr) {
      non_null_fallbacks.push_back(std::move(fallbacks[i]));
    }
  }
  res->fallbacks_ = std::move(non_null_fallbacks);
  return res;
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
  return ImmutableEmptyWithFallbacks(std::move(non_null_databags));
}

DataBagPtr DataBag::FromImpl(internal::DataBagImplPtr impl) {
  auto res = std::make_shared<DataBag>();
  res->impl_ = std::move(impl);
  return res;
}

uint64_t DataBag::GetRandomizedDataBagId() {
  static absl::NoDestructor<absl::BitGen> bitgen;
  if (!randomized_data_bag_id_.has_value()) {
    randomized_data_bag_id_ =
        // NOTE: Given that we use address of a DataBag for its fingerprint,
        // this has a problem of allocating a different DataBag at the same
        // address if the first one gets deallocated. In order to prevent these
        // 2 DataBags to have same fingerprints, we inject a random portion to
        // the fingerprint.
        (*bitgen)()
        // Also using Address of DataBag as part of its fingerprint value.
        ^ reinterpret_cast<uintptr_t>(this);
  }
  return *randomized_data_bag_id_;
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
  auto other_db_impl = other_db->GetImpl().PartiallyPersistentFork();
  FlattenFallbackFinder fallback_finder(*other_db);
  auto keep_original = internal::MergeOptions{
      .data_conflict_policy = internal::MergeOptions::kKeepOriginal,
      .schema_conflict_policy = internal::MergeOptions::kKeepOriginal};
  for (const auto& fallback : fallback_finder.GetFlattenFallbacks()) {
    RETURN_IF_ERROR(other_db_impl->MergeInplace(*fallback, keep_original));
  }
  return db_impl.MergeInplace(*other_db_impl, merge_options);
}

void FlattenFallbackFinder::CollectFlattenFallbacks(
    const DataBag& bag, const std::vector<DataBagPtr>& fallbacks) {
  absl::flat_hash_set<const DataBag*> seen_db;
  seen_db.reserve(fallbacks.size() + 1);
  seen_db.insert(&bag);

  auto add_fallback = [&](const DataBag* fallback) {
    if (seen_db.insert(fallback).second) {
      fallback_holder_.push_back(&fallback->GetImpl());
      return true;
    }
    return false;
  };

  std::vector<const DataBag*> stack;
  stack.reserve(fallbacks.size());
  for (auto it = fallbacks.rbegin(); it != fallbacks.rend(); ++it) {
    stack.push_back(it->get());
  }

  while (!stack.empty()) {
    const DataBag* fallback = stack.back();
    DCHECK(fallback != nullptr);
    stack.pop_back();
    if (add_fallback(fallback)) {
      const auto cur_fallbacks = fallback->GetFallbacks();
      for (auto it = cur_fallbacks.rbegin(); it != cur_fallbacks.rend(); ++it) {
        stack.push_back(it->get());
      }
    }
  }
  fallback_span_ = absl::MakeConstSpan(fallback_holder_);
}

std::string GetBagIdRepr(const DataBagPtr& db) {
  DCHECK_NE(db, nullptr);
  std::string fp_hex =
      arolla::TypedValue::FromValue(db).GetFingerprint().AsString();
  // Use 4 hex digits as a compromise between simplicity and risk of conflicts,
  // which results in ~2^8 DataBags needed for 50% probability of a clash.
  return absl::StrCat("$", absl::string_view(fp_hex).substr(fp_hex.size() - 4));
}

}  // namespace koladata

namespace arolla {

void FingerprintHasherTraits<::koladata::DataBagPtr>::operator()(
    FingerprintHasher* hasher, const ::koladata::DataBagPtr& value) const {
  hasher->Combine(value->GetRandomizedDataBagId());
}

// TODO: Implement proper Repr for DataBag(s). The current version
// is useful for debugging during development.
ReprToken ReprTraits<::koladata::DataBagPtr>::operator()(
    const ::koladata::DataBagPtr& value) const {
  std::string result;
  using Triples = ::koladata::internal::debug::Triples;
  Triples main_triples(value->GetImpl().ExtractContent().value());
  absl::StrAppend(&result, main_triples);
  ::koladata::FlattenFallbackFinder fb_finder(*value);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  for (int i = 0; i < fallbacks.size(); ++i) {
    absl::StrAppend(
        &result,
        absl::StrFormat("\n\nfallback #%d:\n%v", i + 1,
                        Triples(fallbacks[i]->ExtractContent().value())));
  }
  return ReprToken{result};
}

AROLLA_DEFINE_SIMPLE_QTYPE(DATA_BAG, ::koladata::DataBagPtr);

}  // namespace arolla
