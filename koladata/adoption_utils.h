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
#ifndef KOLADATA_ADOPTION_UTILS_H_
#define KOLADATA_ADOPTION_UTILS_H_

#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata {

// Used to track information about source data bags when we perform an operation
// on a data bag using data from other data bags.
class AdoptionQueue {
 public:
  AdoptionQueue() = default;
  AdoptionQueue(const AdoptionQueue&) = delete;
  AdoptionQueue(AdoptionQueue&&) = delete;

  void Add(const DataSlice& slice) {
    if (auto db = slice.GetDb();
        db != nullptr && (empty() || bags_to_merge_.back() != db)) {
      bags_to_merge_.emplace_back(std::move(db));
    }
  }

  void Add(absl::Nullable<DataBagPtr> db) {
    if (db != nullptr) {
      bags_to_merge_.push_back(std::move(db));
    }
  }

  bool empty() const { return bags_to_merge_.empty(); }

  // Gets access to the added DataBagPtr for rendering useful error message.
  absl::Span<const DataBagPtr> bags() const { return bags_to_merge_; }

  // Merges all tracked data into the given DataBag.
  absl::Status AdoptInto(DataBag& db) const;

  // Returns nullptr if no DataBag was added, or returns the tracked DataBag (if
  // it is a single DataBag), or creates a new DataBag merging all the tracked
  // data.
  absl::StatusOr<DataBagPtr> GetDbOrMerge() const;

 private:
  std::vector<DataBagPtr> bags_to_merge_;
};

}  // namespace koladata

#endif  // KOLADATA_ADOPTION_UTILS_H_
