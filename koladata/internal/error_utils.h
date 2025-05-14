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
#ifndef KOLADATA_INTERNAL_ERROR_UTILS_H_
#define KOLADATA_INTERNAL_ERROR_UTILS_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/serialization_base/base.pb.h"
#include "koladata/s11n/codec.pb.h"

namespace koladata::internal {

// Creates Error with `msg` from the status and sets the cause to `cause`.
// The result's status code is the same as `cause`.
//
absl::Status KodaErrorFromCause(absl::string_view msg, absl::Status cause);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_ERROR_UTILS_H_
