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
#ifndef KOLADATA_FUNCTOR_PARALLEL_SOURCE_LOCATION_UTILS_H_
#define KOLADATA_FUNCTOR_PARALLEL_SOURCE_LOCATION_UTILS_H_

#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"

namespace koladata::functor::parallel {

// Verifies that the given types correspond to the fields of a source location:
// - function_name: Text
// - file_name: Text
// - line: int32
// - column: int32
// - line_text: Text
inline absl::Status VerifySourceLocationTypes(
    absl::Span<const arolla::QTypePtr> source_location_types) {
  if (source_location_types.size() != 5) {
    return absl::InvalidArgumentError(
        "source location requires exactly 5 arguments");
  }
  if (source_location_types[0] != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError("function name must be a string");
  }
  if (source_location_types[1] != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError("file name must be a string");
  }
  if (source_location_types[2] != arolla::GetQType<int>()) {
    return absl::InvalidArgumentError("line number must be an integer");
  }
  if (source_location_types[3] != arolla::GetQType<int>()) {
    return absl::InvalidArgumentError("column number must be an integer");
  }
  if (source_location_types[4] != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError("line text must be a string");
  }
  return absl::OkStatus();
}

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_SOURCE_LOCATION_UTILS_H_
