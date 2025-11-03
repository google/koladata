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
#ifndef KOLADATA_SCHEMA_UTILS_H_
#define KOLADATA_SCHEMA_UTILS_H_

#include <optional>
#include <string>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"

namespace koladata {

// Returns the common schema of the underlying data. If the schema is ambiguous
// (e.g. if the data is mixed but there is no common type), the schema of the
// original slice is returned.
//
// Example:
//  * GetNarrowedSchema(kd.slice([1])) -> INT32.
//  * GetNarrowedSchema(kd.slice([1, 2.0], OBJECT)) -> FLOAT32.
//  * GetNarrowedSchema(kd.slice([None, None], OBJECT)) -> NONE.
internal::DataItem GetNarrowedSchema(const DataSlice& slice);

// Returns OK if the DataSlice contains data of the provided DType.
absl::Status ExpectDType(absl::string_view arg_name, const DataSlice& arg,
                         schema::DType dtype);

// Returns OK if the DataSlice's schema is a numeric type or narrowed to it.
absl::Status ExpectNumeric(absl::string_view arg_name, const DataSlice& arg);

// Returns OK if the DataSlice contains integer values.
absl::Status ExpectInteger(absl::string_view arg_name, const DataSlice& arg);

// Returns OK if the DataSlice contains strings.
inline absl::Status ExpectString(absl::string_view arg_name,
                                 const DataSlice& arg) {
  return ExpectDType(arg_name, arg, schema::kString);
}

// Returns OK if the DataSlice contains bytes.
inline absl::Status ExpectBytes(absl::string_view arg_name,
                                const DataSlice& arg) {
  return ExpectDType(arg_name, arg, schema::kBytes);
}

// Returns OK if the DataSlice contains mask.
inline absl::Status ExpectMask(absl::string_view arg_name,
                                const DataSlice& arg) {
  return ExpectDType(arg_name, arg, schema::kMask);
}

// Returns OK if the DataSlice contains schemas.
inline absl::Status ExpectSchema(absl::string_view arg_name,
                                 const DataSlice& arg) {
  return ExpectDType(arg_name, arg, schema::kSchema);
}

// Returns OK if the DataSlice contains a present scalar.
absl::Status ExpectPresentScalar(
    absl::string_view arg_name, const DataSlice& arg);

// Returns OK if the DataSlice contains a present scalar castable to the
// expected dtype.
absl::Status ExpectPresentScalar(absl::string_view arg_name,
                                 const DataSlice& arg,
                                 schema::DType expected_dtype);

// Returns OK if the DataSlice contains values of the types that can be added
// using the + operator (numerics, bytes or string).
absl::Status ExpectCanBeAdded(absl::string_view arg_name, const DataSlice& arg);

// Returns OK if the DataSlice contains values of the types that can be ordered
// (numerics, boolean, mask, bytes or string).
absl::Status ExpectCanBeOrdered(absl::string_view arg_name,
                                const DataSlice& arg);

// Returns a human-readable description of the schema of the DataSlice. Can be
// used in custom versions of Expect* functions.
std::string DescribeSliceSchema(const DataSlice& slice);

namespace schema_utils_internal {

// (internal) Implementation of ExpectConsistentStringOrBytes.
absl::Status ExpectConsistentStringOrBytesImpl(
    absl::Span<const absl::string_view> arg_names,
    absl::Span<const DataSlice* absl_nonnull const> args);

}  // namespace schema_utils_internal

// Returns OK if the DataSlices contain either strings or byteses, but not their
// mix.
template <typename... DataSlices>
absl::Status ExpectConsistentStringOrBytes(
    absl::Span<const absl::string_view> arg_names, const DataSlices&... args) {
  return schema_utils_internal::ExpectConsistentStringOrBytesImpl(arg_names,
                                                                  {&args...});
}

// Returns OK if the DataSlice contains either strings or byteses, but not their
// mix.
inline absl::Status ExpectConsistentStringOrBytes(absl::string_view arg_name,
                                                  const DataSlice& arg) {
  return schema_utils_internal::ExpectConsistentStringOrBytesImpl({arg_name},
                                                                  {&arg});
}

// Returns OK if the DataSlices contain values castable to a common type.
absl::Status ExpectHaveCommonSchema(
    absl::Span<const absl::string_view> arg_names, const DataSlice& lhs,
    const DataSlice& rhs);

// Returns OK if the DataSlices contain values castable to a common primitive
// type.
// NOTE: arg_names must have exactly 2 elements.
absl::Status ExpectHaveCommonPrimitiveSchema(
    absl::Span<const absl::string_view> arg_names, const DataSlice& lhs,
    const DataSlice& rhs);

}  // namespace koladata

#endif  // KOLADATA_SCHEMA_UTILS_H_
