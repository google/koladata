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
// Python extension module exposing some endpoints for testing purposes.

#include <Python.h>

#include <functional>
#include <optional>
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status.h"
#include "koladata/data_slice.h"
#include "koladata/functor/stack_trace.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/functional.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"

namespace koladata::python {
namespace {

PYBIND11_MODULE(testing_clib, m) {
  m.def("reraise_error_with_stack_trace_frame",
        [](const std::function<void()> raise_error_fn,
           const arolla::TypedValue& qvalue_frame) {
          try {
            raise_error_fn();
          } catch (pybind11::error_already_set& ex) {
            ex.restore();
          }
          absl::Status status = arolla::python::StatusWithRawPyErr(
              absl::StatusCode::kInvalidArgument, "unused");
          CHECK(!status.ok());
          const DataSlice& frame = qvalue_frame.UnsafeAs<DataSlice>();
          arolla::python::SetPyErrFromStatus(
              functor::MaybeAddStackTraceFrame(status, frame));
          throw pybind11::error_already_set();
        });
  m.def("raise_error_with_incorrect_stack_trace_frame",
        [](const std::string error_message,
           const arolla::TypedValue& qvalue_frame) {
          const DataSlice& frame = qvalue_frame.UnsafeAs<DataSlice>();
          std::optional<functor::StackTraceFrame> stack_frame =
              functor::ReadStackTraceFrame(frame);
          arolla::python::SetPyErrFromStatus(
              arolla::WithPayload(absl::FailedPreconditionError(error_message),
                                  *std::move(stack_frame)));
          throw pybind11::error_already_set();
        });
}

}  // namespace
}  // namespace koladata::python
