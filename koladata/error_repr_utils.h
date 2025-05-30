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
#ifndef KOLADATA_ERROR_REPR_UTILS_H_
#define KOLADATA_ERROR_REPR_UTILS_H_

#include <optional>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata {

// Clarifies the error if it is caused by missing collection item schema.
// Otherwise, returns the status unchanged.
absl::Status KodaErrorCausedByMissingCollectionItemSchemaError(
    absl::Status status, const DataBagPtr& db);

// Clarifies the error if it is caused by incompatible schema. Otherwise,
// returns the status unchanged.
absl::Status KodaErrorCausedByIncompableSchemaError(absl::Status status,
                                                    const DataBagPtr& lhs_bag,
                                                    const DataBagPtr& rhs_bag,
                                                    const DataSlice& ds);

// Clarifies the error if it is caused by incompatible schema. Otherwise,
// returns the status unchanged.
absl::Status KodaErrorCausedByIncompableSchemaError(
    absl::Status status, const DataBagPtr& lhs_bag,
    absl::Span<const DataSlice> slices, const DataSlice& ds);

// Clarifies the error if it is caused by DataBag merge conflict. Otherwise,
// returns the status unchanged.
absl::AnyInvocable<absl::Status(absl::Status)>
KodaErrorCausedByMergeConflictError(const DataBagPtr& lhs_bag,
                                    const DataBagPtr& rhs_bag);

// Clarifies the error if it is caused by missing object schema. Otherwise,
// returns the status unchanged.
absl::Status KodaErrorCausedByMissingObjectSchemaError(absl::Status status,
                                                       const DataSlice& self);

// Clarifies the error if it is caused by no common schema. Otherwise, returns
// the status unchanged.
absl::Status KodaErrorCausedByNoCommonSchemaError(absl::Status status,
                                                  const DataBagPtr& db);

// Clarifies the error if it is caused by incompatible shapes of attributes.
// Otherwise, returns the status unchanged.
absl::Status KodaErrorCausedByShapeAlignmentError(
    absl::Status status, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values);

// Creates an error with the repr of the schema. The cause is propagated.
absl::Status CreateItemCreationError(const absl::Status& status,
                                     const std::optional<DataSlice>& schema);

}  // namespace koladata

#endif  // KOLADATA_ERROR_REPR_UTILS_H_
