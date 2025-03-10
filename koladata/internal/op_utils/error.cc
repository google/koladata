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

#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "arolla/util/status.h"

namespace koladata::internal {

absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name) {
  auto previous_koda_error = arolla::GetPayload<internal::Error>(status);

  std::string error_message = (previous_koda_error != nullptr &&
                               !previous_koda_error->error_message().empty())
                                  ? previous_koda_error->error_message()
                                  : std::string(status.message());
  if (!absl::StartsWith(error_message, operator_name)) {
    error_message = absl::StrFormat("%s: %s", operator_name, error_message);
  }

  // To preserve non-Koda payloads we keep the original error as a cause.
  if (arolla::GetPayload(status) != nullptr && previous_koda_error == nullptr) {
    auto result = KodaErrorFromCause(error_message, std::move(status));
    return result;
  }
  internal::Error error =
      previous_koda_error != nullptr ? *previous_koda_error : internal::Error();
  error.set_error_message(std::move(error_message));
  return internal::WithErrorPayload(std::move(status), std::move(error));
}

absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message) {
  return OperatorEvalError(KodaErrorFromCause(error_message, std::move(status)),
                           operator_name);
}

absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message) {
  return OperatorEvalError(absl::InvalidArgumentError(error_message),
                           operator_name);
}

}  // namespace koladata::internal
