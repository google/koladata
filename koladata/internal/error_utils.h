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

#include <optional>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/error.pb.h"
#include "koladata/s11n/codec.pb.h"

namespace koladata::internal {

constexpr absl::string_view kErrorUrl = "koladata.internal.Error";

// Gets the Error proto payload from status.
std::optional<Error> GetErrorPayload(const absl::Status& status);

// Returns the encoded proto of the Schema. The `item` must contain
// ObjectId or DType.
s11n::KodaV1Proto::DataItemProto EncodeSchema(const DataItem& item);

// Sets the `error` in the payload of the `status` if not ok. Otherwise, returns
// the `status`.
absl::Status WithErrorPayload(absl::Status status, const Error& error);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_ERROR_UTILS_H_
