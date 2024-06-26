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
}  // namespace koladata

#endif  // KOLADATA_REPR_UTILS_H_
