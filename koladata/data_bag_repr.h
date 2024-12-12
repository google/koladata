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
#ifndef KOLADATA_DATA_BAG_REPR_H_
#define KOLADATA_DATA_BAG_REPR_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/status/statusor.h"
#include "koladata/data_bag.h"

namespace koladata {

constexpr int kDefaultTripleReprLimit = 1000;

// Returns the string representation of DataBag.
absl::StatusOr<std::string> DataBagToStr(
    const DataBagPtr& db, int64_t triple_limit = kDefaultTripleReprLimit);

absl::StatusOr<std::string> DataOnlyBagToStr(
    const DataBagPtr& db, int64_t triple_limit = kDefaultTripleReprLimit);

absl::StatusOr<std::string> SchemaOnlyBagToStr(
    const DataBagPtr& db, int64_t triple_limit = kDefaultTripleReprLimit);

// Returns the stats string about the number of lists, dicts, schemas, objects,
// entity, and top attributes in the DataBag.
//
// top_attr_limit is the number of top attributes to print. The statistics will
// include the data in fallback databags.
absl::StatusOr<std::string> DataBagStatistics(const DataBagPtr& db,
                                              size_t top_attr_limit = 5);
}  // namespace koladata

#endif  // KOLADATA_DATA_BAG_REPR_H_
