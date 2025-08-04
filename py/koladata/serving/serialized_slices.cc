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
#include "py/koladata/serving/serialized_slices.h"

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/riegeli.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::serving {

absl::StatusOr<DataSlice> ParseSerializedSlice(absl::string_view data) {
  ASSIGN_OR_RETURN(arolla::serialization::DecodeResult decode_result,
                   arolla::serialization::DecodeFromRiegeliData(data),
                   [](absl::Status status) {
                     return absl::InternalError(absl::StrCat(
                         "invalid serialized data: ", status.message()));
                   }(_));
  if (!decode_result.exprs.empty()) {
    return absl::InternalError(
        "serialized data should not contain any expressions");
  }
  if (decode_result.values.size() != 1) {
    return absl::InternalError(
        "serialized data must contain exactly one slice");
  }
  return decode_result.values[0].As<DataSlice>();
}

absl::StatusOr<SliceMap> ParseSerializedSlices(absl::string_view data) {
  ASSIGN_OR_RETURN(arolla::serialization::DecodeResult decode_result,
                   arolla::serialization::DecodeFromRiegeliData(data),
                   [](absl::Status status) {
                     return absl::InternalError(absl::StrCat(
                         "invalid embedded data: ", status.message()));
                   }(_));
  if (!decode_result.exprs.empty()) {
    return absl::InternalError(
        "embedded data should not contain any expressions");
  }
  if (decode_result.values.empty()) {
    return absl::InternalError("embedded data is empty");
  }
  ASSIGN_OR_RETURN(const DataSlice& names_slice,
                   decode_result.values[0].As<DataSlice>());
  if (names_slice.GetShape().rank() != 1) {
    return absl::InternalError("names must be a full data slice of rank 1");
  }
  if (names_slice.slice().dtype() != arolla::GetQType<arolla::Text>()) {
    return absl::InternalError("names must be a slice of strings");
  }
  const arolla::DenseArray<arolla::Text>& names_array =
      names_slice.slice().values<arolla::Text>();
  if (!names_array.IsFull()) {
    return absl::InternalError("names must be a full data slice");
  }
  if (names_array.size() != decode_result.values.size() - 1) {
    return absl::InternalError("number of names must match number of slices");
  }
  SliceMap result;
  result.reserve(names_array.size());
  for (int64_t i = 0; i < names_array.size(); ++i) {
    absl::string_view name = names_array[i].value;
    ASSIGN_OR_RETURN(const DataSlice& value,
                     decode_result.values[i + 1].As<DataSlice>());
    result[name] = value.FreezeBag();
  }
  return result;
}

absl::StatusOr<DataSlice> GetSliceByName(const SliceMap& slices,
                                         absl::string_view name) {
  auto it = slices.find(name);
  if (it == slices.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("embedded slice not found: ", name));
  }
  return it->second;
}

}  // namespace koladata::serving
