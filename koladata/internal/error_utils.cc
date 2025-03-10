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
#include "koladata/internal/error_utils.h"

#include <any>
#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// TODO: b/374841918 - Consider removing this function in favor of
// arolla::GetPayload<Error>.
std::optional<Error> GetErrorPayload(const absl::Status& status) {
  const Error* error = arolla::GetPayload<Error>(status);
  if (error != nullptr) {
    return *error;
  }
  return std::nullopt;
}

absl::Status WithErrorPayload(absl::Status status, Error error) {
  if (status.ok()) {
    return absl::OkStatus();
  }
  return arolla::WithPayload(std::move(status), std::move(error));
}

absl::Status WithErrorPayload(absl::Status status,
                              absl::StatusOr<Error> error) {
  if (status.ok()) {
    return absl::OkStatus();
  }
  if (!error.ok()) {
    RETURN_IF_ERROR(std::move(status))
        << "Error when creating KodaError: " << error.status().message();
  }
  return WithErrorPayload(std::move(status), std::move(error.value()));
}

absl::Status AsKodaError(absl::Status status) {
  if (status.ok()) {
    return status;
  }
  if (GetErrorPayload(status).has_value()) {
    return status;
  }
  internal::Error error;
  error.set_error_message(status.message());
  return internal::WithErrorPayload(std::move(status), std::move(error));
}

absl::Status KodaErrorFromCause(absl::string_view msg, absl::Status cause) {
  if (cause.ok()) {
    return cause;
  }
  Error error;
  error.set_error_message(msg);
  auto status = internal::WithErrorPayload(absl::Status(cause.code(), msg),
                                           std::move(error));
  return arolla::WithCause(std::move(status), std::move(cause));
}

}  // namespace koladata::internal
