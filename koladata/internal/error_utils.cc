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
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/error.pb.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization/utils.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

using arolla::TypedValue;
using arolla::serialization_base::ContainerProto;

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
    return status;
  }
  return arolla::WithPayload(std::move(status), std::move(error));
}

absl::Status WithErrorPayload(absl::Status status,
                              absl::StatusOr<Error> error) {
  if (!error.ok()) {
    std::string annotated;
    absl::StrAppend(
        &annotated, status.message(),
        "; Error when creating KodaError: ", error.status().message());
    absl::Status ret_status = absl::Status(status.code(), annotated);
    return ret_status;
  }
  return WithErrorPayload(std::move(status), std::move(error.value()));
}

absl::StatusOr<Error> CreateNoCommonSchemaError(
    const internal::DataItem& common_schema,
    const internal::DataItem& conflicting_schema) {
  internal::Error error;
  ASSIGN_OR_RETURN(*error.mutable_no_common_schema()->mutable_common_schema(),
                   internal::EncodeDataItem(common_schema));
  ASSIGN_OR_RETURN(
      *error.mutable_no_common_schema()->mutable_conflicting_schema(),
      internal::EncodeDataItem(conflicting_schema));
  return error;
}

absl::StatusOr<ContainerProto> EncodeDataItem(const DataItem& item) {
  return arolla::serialization::Encode({TypedValue::FromValue(item)}, {});
}

absl::StatusOr<DataItem> DecodeDataItem(const ContainerProto& item_proto) {
  ASSIGN_OR_RETURN(TypedValue result,
                   arolla::serialization::DecodeValue(item_proto));
  ASSIGN_OR_RETURN(DataItem item, result.As<DataItem>());
  return item;
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
  internal::Error error;
  error.set_error_message(msg);
  return arolla::WithCause(
      // TODO: b/374841918 - It is strange that we use message from the cause
      // instead `msg`.
      internal::WithErrorPayload(cause, std::move(error)), cause);
}

}  // namespace koladata::internal
