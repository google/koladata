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
#ifndef KOLADATA_OPERATORS_UTILS_H_
#define KOLADATA_OPERATORS_UTILS_H_

#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// Verifies that a qtype is named tuple and has only DataSlice values.
absl::Status VerifyNamedTuple(arolla::QTypePtr qtype);

// Returns the names of attributes associated with an already validated
// named tuple slot.
std::vector<absl::string_view> GetAttrNames(
    arolla::TypedSlot named_tuple_slot);

// Returns the values of an already validated named tuple slot which has all
// DataSlice values.
std::vector<DataSlice> GetValueDataSlices(
    arolla::TypedSlot named_tuple_slot,
    arolla::FramePtr frame);

// Returns a present DataItem if b is true, otherwise missing.
DataSlice AsMask(bool b);

// Returns a status with a Koda error payload containing the given error message
// and the given status as the cause.
absl::Status OperatorEvalError(absl::Status status,
                               absl::string_view operator_name,
                               absl::string_view error_message);

// Returns a absl::InvalidArgumentError status with a Koda error payload
// containing the given error message.
absl::Status OperatorEvalError(absl::string_view operator_name,
                               absl::string_view error_message);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_UTILS_H_
