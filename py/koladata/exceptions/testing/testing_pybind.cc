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
#include <Python.h>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

namespace koladata::python {

using ::koladata::internal::Error;
using ::koladata::internal::WithErrorPayload;

PYBIND11_MODULE(testing_pybind, m) {
  m.def("raise_from_status_with_payload", [](absl::string_view message) {
    Error error;
    error.set_error_message(message);
    SetKodaPyErrFromStatus(
        WithErrorPayload(absl::InternalError(message), error));
    throw pybind11::error_already_set();
  });

  m.def("raise_from_status_without_payload", [](absl::string_view message) {
    SetKodaPyErrFromStatus(absl::InternalError(message));
    throw pybind11::error_already_set();
  });
};
}  // namespace koladata::python
