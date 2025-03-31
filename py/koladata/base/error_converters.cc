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

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/error.pb.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"

namespace koladata::python {

namespace {

using ::arolla::python::PyObjectPtr;

bool HandleKodaPyErrStatus(const absl::Status& status) {
  const auto* error = arolla::GetPayload<internal::Error>(status);
  if (error == nullptr) {
    return false;
  }

  PyObjectPtr py_exception_cause;
  if (auto* cause = arolla::GetCause(status)) {
    arolla::python::SetPyErrFromStatus(*cause);
    py_exception_cause = arolla::python::PyErr_FetchRaisedException();
  }

  std::string error_message = error->error_message();
  // TODO: b/389032294 - Remove manual appending of the cause.
  if (py_exception_cause != nullptr) {
    PyObjectPtr py_cause_error_message =
        PyObjectPtr::Own(PyObject_Str(py_exception_cause.get()));
    if (py_cause_error_message != nullptr) {
      const char* cause_error_message =
          PyUnicode_AsUTF8(py_cause_error_message.get());
      if (cause_error_message != nullptr) {
        absl::StrAppend(&error_message,
                        "\n\nThe cause is: ", cause_error_message);
      }
    }
    PyErr_Clear();
  }

  PyErr_SetString(PyExc_ValueError, error_message.c_str());

  if (py_exception_cause != nullptr) {
    PyObjectPtr py_exception = arolla::python::PyErr_FetchRaisedException();
    arolla::python::PyException_SetCauseAndContext(
        py_exception.get(), std::move(py_exception_cause));
    arolla::python::PyErr_RestoreRaisedException(std::move(py_exception));
  }
  return true;
}

}  // namespace


AROLLA_INITIALIZER(.init_fn = [] {
  return arolla::python::RegisterStatusHandler(HandleKodaPyErrStatus);
})

}  // namespace koladata::python
