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
#ifndef KOLADATA_FUNCTOR_STACK_TRACE_H_
#define KOLADATA_FUNCTOR_STACK_TRACE_H_

#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// LINT.IfChange
constexpr absl::string_view kFunctionNameAttrName = "function_name";
constexpr absl::string_view kFileNameAttrName = "file_name";
constexpr absl::string_view kLineNumberAttrName = "line_number";
constexpr absl::string_view kLineTextAttrName = "line_text";
// LINT.ThenChange(//py/koladata/functor/stack_trace.py)

// Arolla error payload with the semantic meaning of "additional stack trace
// frame to the GetCause()".
struct StackTraceFrame {
  std::string function_name;
  std::string file_name;
  int32_t line_number = 0;  // Starting from 1; 0 indicates an unknown line.
  std::string line_text;
};

// Populates the stack trace frame from the given data item, using its
// function_name, file_name and line_number attributes. Returns std::nullopt if
// the data slice does not contain the required attributes.
std::optional<StackTraceFrame> ReadStackTraceFrame(
    const DataSlice& stack_trace_frame_item);

// Reads StackTraceFrame from StackTraceFrame and attaches it as a payload to
// the given status. Returns the original status if the data slice does not
// contain the required attributes.
absl::Status MaybeAddStackTraceFrame(absl::Status status,
                                     const DataSlice& stack_trace_frame_item);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_STACK_TRACE_H_
