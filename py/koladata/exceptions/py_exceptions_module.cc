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

#include "py/koladata/exceptions/py_exception_utils.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.exceptions.py_exceptions";

PyMethodDef kPyExceptionsModule_methods[] = {
    {"register_koda_exception", (PyCFunction)PyRegisterExceptionFactory, METH_O,
     "Register the KodaError factory function in C++."},
    {nullptr} /* sentinel */
};

struct PyModuleDef py_exceptions_module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"Koda exceptions.",
    -1,
    /*methods=*/kPyExceptionsModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_exceptions_py_ext(void) {
  return PyModule_Create(&py_exceptions_module);
}

}  // namespace
}  // namespace koladata::python
