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

#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"

namespace koladata::python {

using ::arolla::python::SetPyErrFromStatus;
using ::koladata::internal::Error;
using ::koladata::internal::WithErrorPayload;

absl::Status CreateErrorFromCause(absl::string_view cause_message) {
  absl::Status status = absl::InternalError(cause_message);
  Error error;
  error.set_error_message(cause_message);
  return WithErrorPayload(status, std::move(error));
}

PYBIND11_MODULE(error_converters_testing_clib, m) {
  arolla::InitArolla();

  m.def("raise_from_status_with_payload",
        [](absl::string_view status_message, absl::string_view koda_message) {
          Error error;
          error.set_error_message(koda_message);
          SetPyErrFromStatus(
              WithErrorPayload(absl::InternalError(status_message), error));
          throw pybind11::error_already_set();
        });

  m.def("raise_from_status_without_payload", [](absl::string_view message) {
    SetPyErrFromStatus(absl::InternalError(message));
    throw pybind11::error_already_set();
  });

  m.def("raise_from_status_with_payload_and_cause",
        [](absl::string_view message, absl::string_view koda_message,
           absl::string_view cause_message,
           absl::string_view cause_koda_message) {
          Error error;
          error.set_error_message(koda_message);
          Error cause_error;
          cause_error.set_error_message(cause_koda_message);
          SetPyErrFromStatus(arolla::WithPayloadAndCause(
              absl::InternalError(message), std::move(error),
              arolla::WithPayload(absl::InternalError(cause_message),
                                  std::move(cause_error))));
          throw pybind11::error_already_set();
        });
};

}  // namespace koladata::python
