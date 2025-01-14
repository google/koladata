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
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"  // IWYU pragma: keep, registration for HandleKodaPyErrStatus
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/util/init_arolla.h"

namespace koladata::python {

using ::arolla::python::SetPyErrFromStatus;
using ::koladata::internal::Error;
using ::koladata::internal::WithErrorPayload;

PYBIND11_MODULE(testing_pybind, m) {
  arolla::InitArolla();

  m.def("raise_from_status_with_payload", [](absl::string_view message) {
    Error error;
    error.set_error_message(message);
    SetPyErrFromStatus(WithErrorPayload(absl::InternalError(message), error));
    throw pybind11::error_already_set();
  });

  m.def("raise_from_status_without_payload", [](absl::string_view message) {
    SetPyErrFromStatus(absl::InternalError(message));
    throw pybind11::error_already_set();
  });

  m.def("raise_from_status_with_serialized_payload",
        [](absl::string_view message, absl::string_view serialized_payload) {
          Error error;
          error.ParseFromString(serialized_payload);
          SetPyErrFromStatus(
              WithErrorPayload(absl::InternalError(message), error));
          throw pybind11::error_already_set();
        });

  m.def("create_koda_exception_raises", [](absl::string_view message) {
    absl::Status status = absl::InternalError(message);
    status.SetPayload(internal::kErrorUrl, absl::Cord("malformed message"));
    SetPyErrFromStatus(status);
    throw pybind11::error_already_set();
  });
};
}  // namespace koladata::python
