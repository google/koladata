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
#ifndef KOLADATA_UUID_UTILS_H_
#define KOLADATA_UUID_UTILS_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"

namespace koladata {

// For QExpr operators defined without operator families, but with functors /
// free functions that accept argument that can be unspecified by the user,
// there is a need for a sentinel value of DATA_SLICE QType.
//
// Returns a DataSlice that represents an unspecified value with DATA_SLICE
// QType.
const DataSlice& UnspecifiedDataSlice();

// Returns true if `value` represents a special sentinel value. Otherwise,
// returns false.
bool IsUnspecifiedDataSlice(const DataSlice& value);

// Creates a DataSlice whose items are Fingerprints identifying `args`.
//
// In order to create a different "Type" from the same arguments, use `seed` key
// with the desired value, e.g.
//
// kd.uuid(seed='type_1', x=[1, 2, 3], y=[4, 5, 6])
//
// and
//
// kd.uuid(seed='type_2', x=[1, 2, 3], y=[4, 5, 6])
//
// have different ids.
absl::StatusOr<DataSlice> CreateUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values);

// Creates a DataSlice of uuids with list flag set.
absl::StatusOr<DataSlice> CreateListUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values);

// Creates a DataSlice of uuids with dict flag set.
absl::StatusOr<DataSlice> CreateDictUuidFromFields(
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values);

}  // namespace koladata

#endif  // KOLADATA_UUID_UTILS_H_
