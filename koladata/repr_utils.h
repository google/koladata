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

#include <optional>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata {

// The additional data collected for assemble a more user friendly error
// message.
struct SupplementalData {
  absl::Nullable<const koladata::DataBagPtr> db;
  std::optional<const koladata::DataSlice> ds;
};

// Creates the readable error message and sets it in the payload of Status if
// the Status is not ok. On OkStatus, returns it unchanged.
absl::Status AssembleErrorMessage(const absl::Status& status,
                                  const SupplementalData& data);

// Returns the KodaError payload and readable error message if the
// error is caused by incompatible schema. Otherwise, returns the status
// unchanged.
absl::Status KodaErrorCausedByIncompableSchemaError(absl::Status status,
                                                    const DataBagPtr& lhs_bag,
                                                    const DataBagPtr& rhs_bag,
                                                    const DataSlice& ds);

// Returns the KodaError payload and readable error message if the
// error is caused by incompatible schema. Otherwise, returns the status
// unchanged.
absl::Status KodaErrorCausedByIncompableSchemaError(
    absl::Status status, const DataBagPtr& lhs_bag,
    absl::Span<const DataSlice> slices, const DataSlice& ds);

// Returns the KodaError payload and readable error message if the
// error is caused by DataBag merge conflict. Otherwise, returns the status
// unchanged.
absl::AnyInvocable<absl::Status(absl::Status)>
KodaErrorCausedByMergeConflictError(const DataBagPtr& lhs_bag,
                                    const DataBagPtr& rhs_bag);

// Returns the KodaError payload and readable error message if the
// error is caused by missing object schema. Otherwise, returns the status
// unchanged.
absl::Status KodaErrorCausedByMissingObjectSchemaError(absl::Status status,
                                                       const DataSlice& self);

// Returns the KodaError payload and readable error message if the
// error is caused by no common schema. Otherwise, returns the status
// unchanged.
absl::Status KodaErrorCausedByNoCommonSchemaError(absl::Status status,
                                                  const DataBagPtr& db);

// Creates an KodaError that further explains why creating items fails.
// If it is caused by another KodaError, the cause is propagated. Otherwise,
// the error message of the status is set in the cause.
absl::Status CreateItemCreationError(const absl::Status& status,
                                     const std::optional<DataSlice>& schema);

}  // namespace koladata

#endif  // KOLADATA_REPR_UTILS_H_
