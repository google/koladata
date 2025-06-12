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
#include "koladata/functor/stack_trace.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

std::optional<StackTraceFrame> ReadStackTraceFrame(
    const DataSlice& stack_trace_frame_item) {
  StackTraceFrame frame;
  ASSIGN_OR_RETURN(
      auto function_name_slice,
      stack_trace_frame_item.GetAttrOrMissing(kFunctionNameAttrName),
      std::nullopt);
  if (function_name_slice.is_item() &&
      function_name_slice.item().holds_value<arolla::Text>()) {
    frame.function_name =
        std::string(function_name_slice.item().value<arolla::Text>().view());
  }

  ASSIGN_OR_RETURN(auto file_name_slice,
                   stack_trace_frame_item.GetAttrOrMissing(kFileNameAttrName),
                   std::nullopt);
  if (file_name_slice.is_item() &&
      file_name_slice.item().holds_value<arolla::Text>()) {
    frame.file_name =
        std::string(file_name_slice.item().value<arolla::Text>().view());
  }

  ASSIGN_OR_RETURN(auto line_text_slice,
                   stack_trace_frame_item.GetAttrOrMissing(kLineTextAttrName),
                   std::nullopt);
  if (line_text_slice.is_item() &&
      line_text_slice.item().holds_value<arolla::Text>()) {
    frame.line_text =
        std::string(line_text_slice.item().value<arolla::Text>().view());
  }

  ASSIGN_OR_RETURN(auto line_slice,
                   stack_trace_frame_item.GetAttrOrMissing(kLineNumberAttrName),
                   std::nullopt);
  if (line_slice.is_item() && line_slice.item().holds_value<int32_t>()) {
    frame.line_number = line_slice.item().value<int32_t>();
  }

  if (frame.function_name.empty() && frame.file_name.empty() &&
      frame.line_number == 0 && frame.line_text.empty()) {
    return std::nullopt;
  }
  return frame;
}

absl::Status MaybeAddStackTraceFrame(absl::Status status,
                                     const DataSlice& stack_trace_frame_item) {
  std::optional<StackTraceFrame> stack_frame =
      ReadStackTraceFrame(stack_trace_frame_item);
  if (!stack_frame.has_value()) {
    return status;
  }

  std::string new_message = absl::StrCat(status.message(), "\n\n");
  if (!stack_frame->file_name.empty()) {
    absl::StrAppend(&new_message, stack_frame->file_name);
    if (stack_frame->line_number > 0) {
      absl::StrAppend(&new_message, ":", stack_frame->line_number, ", ");
    }
  }
  absl::StrAppend(&new_message, "in ", stack_frame->function_name);
  if (!stack_frame->line_text.empty()) {
    absl::StrAppend(&new_message, "\n", stack_frame->line_text);
  }

  absl::Status result(status.code(), new_message);
  return arolla::WithPayloadAndCause(std::move(result), *std::move(stack_frame),
                                     std::move(status));
};

}  // namespace koladata::functor
