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
#include <Python.h>

#include "absl/strings/string_view.h"
#include "koladata/internal/op_utils/print.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.base.init";

void SetPrintCallback() {
  ::koladata::internal::GetSharedPrinter().SetPrintCallback(
      [](absl::string_view message) {
        arolla::python::AcquirePyGIL gil_guard;
        PyObject* str =
            PyUnicode_FromStringAndSize(message.data(), message.size());
        if (str == nullptr) {
          return;
        }
        PySys_FormatStdout("%U", str);
      });
}

struct PyModuleDef init = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"Initializes koladata base.",
    -1,
    /*methods=*/nullptr,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_init(void) {
  SetPrintCallback();
  return PyModule_Create(&init);
}

}  // namespace
}  // namespace koladata::python
