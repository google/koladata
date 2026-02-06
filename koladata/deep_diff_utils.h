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
#ifndef KOLADATA_DEEP_DIFF_UTILS_H_
#define KOLADATA_DEEP_DIFF_UTILS_H_

#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata {

// Returns a string representation of the given path.
std::string GetPathRepr(
    absl::Span<const internal::TraverseHelper::TransitionKey> path,
    absl::string_view side);

// Returns a string representation of a schema mismatch between the given two
// DataSlices.
absl::StatusOr<std::string> SchemaMismatchRepr(
    absl::Span<const internal::TraverseHelper::TransitionKey> path,
    const DataSlice& lhs, absl::string_view lhs_name,
    const DataSlice& rhs, absl::string_view rhs_name,
    ReprOption schema_repr_option);

// Returns a string representation of a diff item.
// `diff` is a DataItem in the `result_db_impl` that contains the diff
// between the lhs and rhs values. It is expected to have `lhs_attr_name`
// attribute that points to the lhs value, and `rhs_attr_name` that points to
// the rhs value.
// For the representation of lhs and rhs values, the `lhs_db` and `rhs_db` are
// used as accordingly.
absl::StatusOr<std::string> DiffItemRepr(
    const internal::DeepDiff::DiffItem& diff,
    const internal::DataBagImpl& result_db_impl, const DataBagPtr& lhs_db,
    absl::string_view lhs_attr_name, absl::string_view lhs_name,
    const DataBagPtr& rhs_db, absl::string_view rhs_attr_name,
    absl::string_view rhs_name, const ReprOption& repr_option);

}  // namespace koladata

#endif  // KOLADATA_DEEP_DIFF_UTILS_H_
