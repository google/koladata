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

#include "py/koladata/functions/tests/test.pb.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName =
    "koladata.functions.tests.test_cc_proto";

PyMethodDef kPyModule_methods[] = {
    {nullptr} /* sentinel */
};

struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"For static dependencies on test.proto.",
    -1,
    /*methods=*/kPyModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_test_cc_proto_py_ext(void) {
  koladata::functions::testing::MessageA message_a;  // Forces linking.
  koladata::functions::testing::MessageAExtension message_a_extension;

  PyObject* m = PyModule_Create(&module);
  return m;
}

}  // namespace
}  // namespace koladata::python
