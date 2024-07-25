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
#include "koladata/repr_utils.h"

#include <optional>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using DataItemProto = ::koladata::s11n::KodaV1Proto::DataItemProto;
using ::koladata::internal::DecodeDataItem;
using ::koladata::internal::Error;
using ::koladata::internal::GetErrorPayload;

absl::StatusOr<Error> SetNoCommonSchemaError(
    Error cause, absl::Nullable<const DataBagPtr>& db) {
  if (!db) {
    return absl::InvalidArgumentError("db is missing");
  }
  ASSIGN_OR_RETURN(internal::DataItem common_schema_item,
                   DecodeDataItem(cause.no_common_schema().common_schema()));
  ASSIGN_OR_RETURN(
      internal::DataItem conflict_schema_item,
      DecodeDataItem(cause.no_common_schema().conflicting_schema()));

  ASSIGN_OR_RETURN(DataSlice common_schema,
                   DataSlice::Create(common_schema_item,
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(DataSlice conflict_schema,
                   DataSlice::Create(conflict_schema_item,
                                     internal::DataItem(schema::kSchema), db));

  ASSIGN_OR_RETURN(std::string common_schema_str,
                   DataSliceToStr(common_schema));
  ASSIGN_OR_RETURN(std::string conflict_schema_str,
                   DataSliceToStr(conflict_schema));

  Error error;
  error.set_error_message(
      absl::StrFormat("\ncannot find a common schema for provided schemas\n\n"
                      " the common schema(s) %s: %s\n"
                      " the first conflicting schema %s: %s",
                      common_schema_item.DebugString(), common_schema_str,
                      conflict_schema_item.DebugString(), conflict_schema_str));
  *error.mutable_cause() = std::move(cause);
  return error;
}

}  // namespace

absl::Status AssembleErrorMessage(const absl::Status& status,
                                  const SupplementalData& data) {
  std::optional<Error> cause = GetErrorPayload(status);
  if (!cause) {
    return status;
  }
  if (cause->has_no_common_schema()) {
    ASSIGN_OR_RETURN(Error error,
                     SetNoCommonSchemaError(*std::move(cause), data.db));
    return WithErrorPayload(status, error);
  }
  return status;
}

}  // namespace koladata
