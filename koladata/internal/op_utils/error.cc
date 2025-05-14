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
#include "koladata/internal/op_utils/error.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status.h"
#include "koladata/internal/error_utils.h"

namespace koladata::internal {

absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name) {
  if (absl::StartsWith(status.message(), operator_name)) {
    return status;
  }
  return arolla::WithUpdatedMessage(
      std::move(status),
      absl::StrFormat("%s: %s", operator_name, status.message()));
}

absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message) {
  return KodaErrorFromCause(
      absl::StrFormat("%s: %s", operator_name, error_message),
      std::move(status));
}

absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message) {
  return absl::InvalidArgumentError(
      absl::StrFormat("%s: %s", operator_name, error_message));
}

}  // namespace koladata::internal
