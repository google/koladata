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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ERROR_H_
#define KOLADATA_INTERNAL_OP_UTILS_ERROR_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace koladata::internal {

// Turns the given `status` into a Koda error and amends the error message with
// the operator name.
//
// By default the function does not put the original `status` as a cause, and
// just amends its error message. However, if `error_message` is provided, or if
// `status` has a non-Koda payload, it will create an new layer of Koda error,
// keeping the original `status` as a cause.
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name);
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message);

// Returns a absl::InvalidArgumentError status with a Koda error payload
// containing the given error message.
absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ERROR_H_
