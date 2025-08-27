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
#ifndef KOLADATA_OPERATORS_UTILS_H_
#define KOLADATA_OPERATORS_UTILS_H_

#include <cstdint>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// Verifies that a qtype is named tuple and has only DataSlice values.
absl::Status VerifyNamedTuple(arolla::QTypePtr qtype);

// Returns true if the qtype is NonDeterministicToken QType.
absl::Status VerifyIsNonDeterministicToken(arolla::QTypePtr qtype);

// Returns true if the qtype is either DataSlice or Unspecified.
bool IsDataSliceOrUnspecified(arolla::QTypePtr type);

// Returns the names of fields associated with an already validated named tuple
// slot.
std::vector<absl::string_view> GetFieldNames(
    arolla::TypedSlot named_tuple_slot);

// Returns the values of an already validated named tuple slot which has all
// DataSlice values.
std::vector<DataSlice> GetValueDataSlices(
    arolla::TypedSlot named_tuple_slot,
    arolla::FramePtr frame);

// Returns the value of a bool argument that is expected to be a DataItem.
absl::StatusOr<bool> GetBoolArgument(const DataSlice& slice,
                                     absl::string_view arg_name);

// Returns the view to a value of a string argument that is expected to be a
// DataItem.
absl::StatusOr<absl::string_view> GetStringArgument(const DataSlice& slice,
                                                    absl::string_view arg_name);

// Returns the value of an expected scalar integer argument as int64_t.
absl::StatusOr<int64_t> GetIntegerArgument(const DataSlice& slice,
                                           absl::string_view arg_name);

// Returns a present DataItem if b is true, otherwise missing.
DataSlice AsMask(bool b);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_UTILS_H_
