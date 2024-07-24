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

#include <algorithm>
#include <cstddef>
#include <optional>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

namespace {

using ::arolla::python::PyObjectPtr;

absl::NoDestructor<arolla::python::PyObjectPtr> exception_factory;

absl::Nullable<PyObject*> CreateKodaException(const absl::Cord& payload) {
  PyObjectPtr py_bytes =
      PyObjectPtr::Own(PyBytes_FromStringAndSize(nullptr, payload.size()));
  auto dest = PyBytes_AS_STRING(py_bytes.get());
  for (const absl::string_view chunk : payload.Chunks()) {
    dest = std::copy_n(chunk.data(), chunk.size(), dest);
  }
  // NOTE: If the exception_factory is not set, i.e. the python module is not
  // loaded, the process will crash.
  return PyObject_CallOneArg(exception_factory->get(), py_bytes.release());
}

}  // namespace

PyObject* PyRegisterExceptionFactory(PyObject* /*module*/, PyObject* factory) {
  if (*exception_factory == nullptr) {
    *exception_factory = arolla::python::PyObjectPtr::NewRef(factory);
  }
  Py_RETURN_NONE;
}

std::nullptr_t SetKodaPyErrFromStatus(const absl::Status& status) {
  DCHECK(!status.ok());
  std::optional<absl::Cord> payload = status.GetPayload(internal::kErrorUrl);
  if (!payload) {
    return arolla::python::SetPyErrFromStatus(status);
  }
  PyObject* py_exception = CreateKodaException(payload.value());
  if (Py_IsNone(py_exception)) {
    return arolla::python::SetPyErrFromStatus(
        internal::Annotate(status,
                           "error message is empty. A code path failed to "
                           "generate user readable error message."));
  }
  if (py_exception != nullptr) {
    PyErr_SetObject((PyObject*)Py_TYPE(py_exception), py_exception);
  }
  return nullptr;
}

}  // namespace koladata::python
