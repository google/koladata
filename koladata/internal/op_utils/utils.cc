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
#include "koladata/internal/op_utils/utils.h"

#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"

namespace koladata::internal {

absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message) {
  internal::Error error;
  std::optional<internal::Error> cause = internal::GetErrorPayload(status);
  if (cause) {
    *error.mutable_cause() = *std::move(cause);
  } else if (!error_message.empty()) {
    error.mutable_cause()->set_error_message(status.message());
  } else {
    error_message = status.message();
  }
  error.set_error_message(
      absl::StrFormat("%s: %s", operator_name, error_message));
  return internal::WithErrorPayload(std::move(status), error);
}

absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message) {
  internal::Error error;
  error.set_error_message(
      absl::StrFormat("%s: %s", operator_name, error_message));
  // Note error_message inside the status is not used by the error handling
  // logic in the CPython layer.
  return internal::WithErrorPayload(absl::InvalidArgumentError(error_message),
                                    error);
}

}  // namespace koladata::internal
