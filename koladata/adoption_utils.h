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
#ifndef KOLADATA_ADOPTION_UTILS_H_
#define KOLADATA_ADOPTION_UTILS_H_

#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
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

  // Tracks triples reachable from `slice` (including via its schema), which
  // will be present in the result DataBag. The DataBag used by `slice` must not
  // be mutated before calling `AdoptInto` or `GetCommonOrMergedDb`.
  void Add(const DataSlice& slice) {
    if (slice.GetBag() != nullptr) {
      slices_to_merge_.emplace_back(slice);
    }
  }

  // Tracks all triples in `db`, which will be present in the result databag.
  void Add(absl_nullable DataBagPtr db) {
    if (db != nullptr) {
      bags_to_merge_.push_back(std::move(db));
    }
  }

  bool empty() const {
    return slices_to_merge_.empty() && bags_to_merge_.empty();
  }

  // Merges all tracked triples into the given DataBag, which must be mutable.
  // Returns an error on merge conflicts.
  absl::Status AdoptInto(DataBag& db) const;

  // Returns the result common or merged DataBag. There are three cases:
  // 1. If no non-null DataBags have been tracked, returns nullptr.
  // 2. If all tracked triples are from the same DataBag, returns that DataBag.
  // 3. Else, returns a new immutable DataBag containing all tracked triples, or
  //    an error if this causes a merge conflict.
  absl::StatusOr<absl_nullable DataBagPtr> GetCommonOrMergedDb() const;

  // Returns a new empty immutable DataBag with all tracked DataBags and tracked
  // slices' DataBags as fallbacks. The fallback order is unspecified but
  // deterministic. This is useful for cheaply and reliably getting a complete
  // DataBag for error messages, but does not check for merge conflicts, and
  // should not be used to define user-facing operator behavior.
  absl_nonnull DataBagPtr GetBagWithFallbacks() const;

 private:
  std::vector<DataSlice> slices_to_merge_;
  std::vector<DataBagPtr> bags_to_merge_;
};

// Copies the schema stub of x to the DataBag db.
absl::Status AdoptStub(const DataBagPtr& db, const DataSlice& x);

// Returns a DataBag containing the contents of `db` and the extracted contents
// of `slice`. The following cases exist:
// 1. If `db == nullptr`, returns `slice.GetBag()`.
// 2. Else if `slice.GetBag() == nullptr`, returns `db`.
// 3. Else, `slice` is extracted, and the "common bag" is computed and returned
//    with `slice` having a higher precedence.
absl::StatusOr<absl_nullable DataBagPtr> WithAdoptedValues(
    const absl_nullable DataBagPtr& db, const DataSlice& slice);

}  // namespace koladata

#endif  // KOLADATA_ADOPTION_UTILS_H_
