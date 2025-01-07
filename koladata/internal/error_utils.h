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
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/error.pb.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/serialization_base/base.pb.h"

namespace koladata::internal {

constexpr absl::string_view kErrorUrl = "koladata.internal.Error";

// Gets the Error proto payload from status.
std::optional<Error> GetErrorPayload(const absl::Status& status);

// Sets the `error` in the payload of the `status` if not ok. Otherwise, returns
// the `status`.
absl::Status WithErrorPayload(absl::Status status, const Error& error);

// Sets the `error` in the payload of the `status` if not ok. If `error` is not
// ok, the error message of `error` will be appended to the `status`.
absl::Status WithErrorPayload(absl::Status status, absl::StatusOr<Error> error);

// Creates the no common schema error proto from the given schema id and dtype.
absl::StatusOr<Error> CreateNoCommonSchemaError(
    const DataItem& common_schema, const DataItem& conflicting_schema);

// Encodes the DataItem to ContainerProto.
absl::StatusOr<arolla::serialization_base::ContainerProto> EncodeDataItem(
    const DataItem& item);

// Decodes the ContainerProto to DataItem.
absl::StatusOr<DataItem> DecodeDataItem(
    const arolla::serialization_base::ContainerProto& item_proto);

// Creates KodaError with an error message from the status. If the status
// already is a KodaError, returns the status as is.
absl::Status AsKodaError(absl::Status status);

// Creates KodaError with `msg` from the status and sets the cause to `cause`.
// The result's status code and .message() are the same as `cause`.
absl::Status KodaErrorFromCause(absl::string_view msg, absl::Status cause);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_ERROR_UTILS_H_
