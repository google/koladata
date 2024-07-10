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
#ifndef KOLADATA_REPR_UTILS_H_
#define KOLADATA_REPR_UTILS_H_

#include <cstdint>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata {

// Returns the string for python __str__.
absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds);

// Returns the string representation of DataBag.
absl::StatusOr<std::string> DataBagToStr(const DataBagPtr& db);

// Creates the readable error message and sets it in the payload of Status if
// the Status is not ok. On OkStatus, returns it unchanged.
absl::Status AssembleErrorMessage(const absl::Status& status,
                                  absl::Span<const koladata::DataBagPtr> dbs);

// Returns the stats string about the triples and attributes in the DataBag.
//
// Items in the list are counted as multiple triples: one for each item. E.g.
// $123[:] -> [1, 2, 3] counts as three triples. A key/value pair in the dict is
// counted as one triple for the attribute derived from the key. For
// example, a dict with two values $456[1] = 2 and $456[3] = 4 counts as one
// triples for the 1->2 pair, one triples for the 3->4 pair.
absl::StatusOr<std::string> DataBagStatistics(const DataBagPtr& db,
                                              int64_t top_attr_limit = 5);

}  // namespace koladata

#endif  // KOLADATA_REPR_UTILS_H_
