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
// Registers error converter to handle koladata::functor::StackTraceFrame.

#include <Python.h>

#include <string>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"
#include "koladata/functor/stack_trace.h"
#include "py/arolla/py_utils/error_converter_registry.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

void ConvertStackTraceFrame(const absl::Status& status) {
  arolla::python::DCheckPyGIL();
  const auto* stack_frame =
      arolla::GetPayload<functor::StackTraceFrame>(status);
  CHECK(stack_frame != nullptr);  // Only called when this payload is present.
  auto* cause = arolla::GetCause(status);
  if (cause == nullptr) {
    PyErr_Format(PyExc_ValueError,
                 "invalid StackTraceFrame(status.code=%d, "
                 "status.message='%s', function_name='%s', "
                 "file_name='%s', line_number=%d, line_text='%s')",
                 status.code(), absl::Utf8SafeCEscape(status.message()).c_str(),
                 absl::Utf8SafeCEscape(stack_frame->function_name).c_str(),
                 absl::Utf8SafeCEscape(stack_frame->file_name).c_str(),
                 stack_frame->line_number,
                 absl::Utf8SafeCEscape(stack_frame->line_text).c_str());
    return;
  }

  arolla::python::SetPyErrFromStatus(*cause);
  arolla::python::PyTraceback_Add(stack_frame->function_name.c_str(),
                                  stack_frame->file_name.c_str(),
                                  stack_frame->line_number);
}

AROLLA_INITIALIZER(.init_fn = []() -> absl::Status {
  RETURN_IF_ERROR(
      arolla::python::RegisterErrorConverter<functor::StackTraceFrame>(
          ConvertStackTraceFrame));
  return absl::OkStatus();
})

}  // namespace koladata::python
