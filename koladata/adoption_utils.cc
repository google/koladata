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
#include "koladata/adoption_utils.h"

#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/extract_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

absl::Status AdoptionQueue::AdoptInto(DataBag& db) const {
  absl::flat_hash_set<const DataBag*> visited_bags{&db};
  for (const DataBagPtr& other_db : bags_to_merge_) {
    if (visited_bags.contains(other_db.get())) {
      continue;
    }
    visited_bags.insert(other_db.get());
    RETURN_IF_ERROR(db.MergeInplace(other_db, /*overwrite=*/false,
                                    /*allow_data_conflicts=*/false,
                                    /*allow_schema_conflicts=*/false));
  }
  for (const DataSlice& slice : slices_to_merge_) {
    if (visited_bags.contains(slice.GetDb().get())) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice extracted_slice,
                     extract_utils_internal::Extract(slice));
    const auto& extracted_db = extracted_slice.GetDb();
    if (extracted_db == nullptr) {
      continue;
    }
    RETURN_IF_ERROR(db.MergeInplace(extracted_db, /*overwrite=*/false,
                                    /*allow_data_conflicts=*/false,
                                    /*allow_schema_conflicts=*/false));
  }
  return absl::OkStatus();
}

absl::StatusOr<absl::Nullable<DataBagPtr>> AdoptionQueue::GetCommonOrMergedDb()
    const {
  // Check whether all bags (and slices' bags) are the same bag. If so, we
  // return that bag instead of merging.
  bool has_multiple_bags = false;
  const DataBagPtr* single_bag = nullptr;
  for (const DataBagPtr& bag : bags_to_merge_) {
    if (single_bag == nullptr) {
      single_bag = &bag;
    } else if (*single_bag != bag) {
      has_multiple_bags = true;
      break;
    }
  }
  if (!has_multiple_bags) {
    for (const DataSlice& slice : slices_to_merge_) {
      const DataBagPtr& slice_bag = slice.GetDb();
      if (single_bag == nullptr) {
        single_bag = &slice_bag;
      } else if (*single_bag != slice_bag) {
        has_multiple_bags = true;
        break;
      }
    }
  }

  DCHECK_EQ(slices_to_merge_.empty() && bags_to_merge_.empty(),
            single_bag == nullptr);
  if (single_bag == nullptr) {
    return nullptr;
  } else if (!has_multiple_bags) {
    return *single_bag;
  } else {
    auto res = DataBag::Empty();
    RETURN_IF_ERROR(AdoptInto(*res));
    return std::move(res)->Fork(/*immutable=*/true);
  }
}

absl::Nonnull<DataBagPtr> AdoptionQueue::GetDbWithFallbacks() const {
  // Collect unique DataBags from all Add calls.
  absl::flat_hash_set<const DataBag*> visited_bags;
  std::vector<DataBagPtr> fallbacks;
  fallbacks.reserve(slices_to_merge_.size() + bags_to_merge_.size());
  for (const DataBagPtr& bag : bags_to_merge_) {
    if (visited_bags.contains(bag.get())) {
      continue;
    }
    visited_bags.insert(bag.get());
    fallbacks.push_back(bag);
  }
  for (const DataSlice& slice : slices_to_merge_) {
    const DataBagPtr& bag = slice.GetDb();
    if (visited_bags.contains(bag.get())) {
      continue;
    }
    visited_bags.insert(bag.get());
    fallbacks.push_back(bag);
  }

  return DataBag::ImmutableEmptyWithFallbacks(fallbacks);
}

}  // namespace koladata
