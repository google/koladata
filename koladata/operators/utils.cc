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
#include "koladata/operators/utils.h"

#include <cstddef>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

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

std::vector<absl::string_view> GetAttrNames(
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
  if (!slice.is_item() || !slice.item().holds_value<bool>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("requires `%s` to be DataItem holding bool, got %s",
                        arg_name, arolla::Repr(slice)));
  }
  return slice.item().value<bool>();
}

absl::StatusOr<absl::string_view> GetStringArgument(
    const DataSlice& slice, absl::string_view arg_name) {
  if (!slice.is_item() || !slice.item().holds_value<arolla::Text>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("requires `%s` to be DataItem holding string, got %s",
                        arg_name, arolla::Repr(slice)));
  }
  return slice.item().value<arolla::Text>().view();
}

DataSlice AsMask(bool b) {
  return *DataSlice::Create(
      b ? internal::DataItem(arolla::kUnit) : internal::DataItem(),
      internal::DataItem(schema::kMask));
}

}  // namespace koladata::ops
