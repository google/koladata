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
#include "py/koladata/exceptions/py_exception_utils.h"

#include <Python.h>

#include <string>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/error.pb.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/py_utils/status_payload_handler_registry.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status.h"

namespace koladata::python {

namespace {

using ::arolla::python::PyObjectPtr;

absl::NoDestructor<arolla::python::PyObjectPtr> exception_factory;

bool HandleKodaPyErrStatus(const absl::Status& status) {
  const auto* error = arolla::GetPayload<internal::Error>(status);
  if (error == nullptr) {
    return false;
  }
  if (exception_factory->get() == nullptr) {
    PyErr_SetString(PyExc_AssertionError, "Koda exception factory is not set");
    return true;
  }
  // TODO: b/374841918 - Avoid serialization.
  std::string serialized_error;
  error->SerializeToString(&serialized_error);
  auto py_serialized_error = PyObjectPtr::Own(PyBytes_FromStringAndSize(
      serialized_error.data(), serialized_error.size()));
  if (py_serialized_error == nullptr) {
    return true;  // Error already set.
  }
  auto py_exception = PyObjectPtr::Own(
      PyObject_CallOneArg(exception_factory->get(), py_serialized_error.get()));
  if (py_exception == nullptr) {
    return true;  // Error already set.
  }
  if (Py_IsNone(py_exception.get())) {
    return false;
  }
  if (auto* cause = arolla::GetCause(status)) {
    arolla::python::SetPyErrFromStatus(*cause);
    arolla::python::PyException_SetCauseAndContext(
        py_exception.get(), arolla::python::PyErr_FetchRaisedException());
  }
  PyErr_SetObject(reinterpret_cast<PyObject*>(Py_TYPE(py_exception.get())),
                  py_exception.get());
  return true;
}

}  // namespace

absl::Nullable<PyObject*> PyRegisterExceptionFactory(PyObject* /*module*/,
                                                     PyObject* factory) {
  if (*exception_factory == nullptr) {
    *exception_factory = arolla::python::PyObjectPtr::NewRef(factory);
  }
  Py_RETURN_NONE;
}

AROLLA_INITIALIZER(.init_fn = [] {
  return arolla::python::RegisterStatusHandler(HandleKodaPyErrStatus);
})

}  // namespace koladata::python
