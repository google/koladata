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
#include "koladata/operators/utils.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::Status VerifyNamedTuple(arolla::QTypePtr qtype) {
  if (!IsNamedTupleQType(qtype)) {
    return absl::InvalidArgumentError(
        "requires last argument to be NamedTuple");
  }
  for (const auto& value_slot : qtype->type_fields()) {
    if (value_slot.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          "requires all values of named tuple argument to be DataSlices");
    }
  }
  return absl::OkStatus();
}

absl::Status VerifyIsNonDeterministicToken(arolla::QTypePtr qtype) {
  if (qtype != arolla::GetQType<internal::NonDeterministicToken>()) {
    return absl::InvalidArgumentError(
        "requires last argument to be NON_DETERMINISTIC_TOKEN");
  }
  return absl::OkStatus();
}

bool IsDataSliceOrUnspecified(arolla::QTypePtr type) {
  return type == arolla::GetQType<DataSlice>() ||
         type == arolla::GetUnspecifiedQType();
}

std::vector<absl::string_view> GetFieldNames(
    arolla::TypedSlot named_tuple_slot) {
  auto qtype = named_tuple_slot.GetType();
  std::vector<absl::string_view> attr_names;
  absl::Span<const std::string> field_names = arolla::GetFieldNames(qtype);
  return std::vector<absl::string_view>(
      field_names.begin(), field_names.end());
}

std::vector<DataSlice> GetValueDataSlices(arolla::TypedSlot named_tuple_slot,
                                          arolla::FramePtr frame) {
  std::vector<DataSlice> values;
  values.reserve(named_tuple_slot.SubSlotCount());
  for (size_t index = 0; index < named_tuple_slot.SubSlotCount(); ++index) {
    auto field_slot = named_tuple_slot.SubSlot(index);
    auto data_slice_slot = field_slot.UnsafeToSlot<DataSlice>();
    values.push_back(frame.Get(data_slice_slot));
  }
  return values;
}

absl::StatusOr<bool> GetBoolArgument(const DataSlice& slice,
                                     absl::string_view arg_name) {
  RETURN_IF_ERROR(ExpectPresentScalar(arg_name, slice, schema::kBool));
  return slice.item().value<bool>();
}

absl::StatusOr<absl::string_view> GetStringArgument(
    const DataSlice& slice, absl::string_view arg_name) {
  RETURN_IF_ERROR(ExpectPresentScalar(arg_name, slice, schema::kString));
  return slice.item().value<arolla::Text>().view();
}

absl::StatusOr<int64_t> GetIntegerArgument(
    const DataSlice& slice, absl::string_view arg_name) {
  RETURN_IF_ERROR(ExpectPresentScalar(arg_name, slice, schema::kInt64));
  // ExpectPresentScalar only checks that item is castable to int64_t
  if (slice.item().holds_value<int64_t>()) {
    return slice.item().value<int64_t>();
  } else {
    return slice.item().value<int>();
  }
}

DataSlice AsMask(bool b) {
  return *DataSlice::Create(
      b ? internal::DataItem(arolla::kUnit) : internal::DataItem(),
      internal::DataItem(schema::kMask));
}

}  // namespace koladata::ops
