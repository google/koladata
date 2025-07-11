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
#include "koladata/internal/error_utils.h"

#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/util/status.h"
#include "koladata/s11n/codec.pb.h"

namespace koladata::internal {

absl::Status KodaErrorFromCause(absl::string_view msg, absl::Status cause) {
  if (cause.ok()) {
    return cause;
  }
  absl::Status status = absl::Status(cause.code(), msg);
  return arolla::WithCause(std::move(status), std::move(cause));
}

}  // namespace koladata::internal
