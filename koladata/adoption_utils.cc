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
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_bag.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

absl::Status AdoptionQueue::AdoptInto(DataBag& db) const {
  ASSIGN_OR_RETURN(internal::DataBagImpl& db_impl, db.GetMutableImpl());
  absl::flat_hash_set<const internal::DataBagImpl*> visited_bags{&db_impl};
  for (const DataBagPtr& other_db : bags_to_merge_) {
    if (visited_bags.contains(&other_db->GetImpl())) {
      continue;
    }
    visited_bags.insert(&other_db->GetImpl());
    RETURN_IF_ERROR(db.MergeInplace(other_db, /*overwrite=*/false,
                                    /*allow_data_conflicts=*/false,
                                    /*allow_schema_conflicts=*/false));
  }
  return absl::OkStatus();
}

absl::StatusOr<DataBagPtr> AdoptionQueue::GetDbOrMerge() const {
  if (bags_to_merge_.empty()) {
    return nullptr;
  } else if (bags_to_merge_.size() == 1) {
    // This is the case, when all data bags are the same because we merge
    // consequent equal on the fly.
    return bags_to_merge_.front();
  } else {
    auto res = DataBag::Empty();
    RETURN_IF_ERROR(AdoptInto(*res));
    return res;
  }
}

}  // namespace koladata
