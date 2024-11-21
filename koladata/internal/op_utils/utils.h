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
#ifndef KOLADATA_INTERNAL_OP_UTILS_UTILS_H_
#define KOLADATA_INTERNAL_OP_UTILS_UTILS_H_

#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace koladata::internal {

// Returns a status with a Koda error payload containing the given error message
// and the given status as the cause.
//
// By default, the error message is taken from the status, but can be overridden
// by passing a custom error_message.
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message = "");

// absl::Status adaptor that attaches operator name and wraps the given status
// into an OperatorEvalError.
inline auto ToOperatorEvalError(absl::string_view operator_name) {
  return [operator_name](absl::Status status) {
    return OperatorEvalError(std::move(status), operator_name);
  };
}

// Returns a absl::InvalidArgumentError status with a Koda error payload
// containing the given error message.
absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_UTILS_H_
