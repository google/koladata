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

#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/testing/traversing_test_utils.h"

namespace koladata::python {
namespace {

using ::arolla::python::PyObjectPtr;

constexpr const char* kThisModuleName =
    "koladata.testing.traversing_test_utils";

PyMethodDef kTraversingTestUtilsModule_methods[] = {
    {"assert_deep_equivalent", (PyCFunction)PyAssertDeepEquivalent,
     METH_FASTCALL | METH_KEYWORDS,
     ("assert_deep_equivalent(lhs, rhs)\n--\n\n"
      "Asserts that two DataSlices are deep equivalent.")},
    {nullptr} /* sentinel */
};

struct PyModuleDef traversing_test_utils_module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"Koda traversing test utils.",
    -1,
    /*methods=*/kTraversingTestUtilsModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_traversing_test_utils_py_ext(void) {
  auto result =
      PyObjectPtr::Own(PyModule_Create(&traversing_test_utils_module));
  return result.release();
}

}  // namespace
}  // namespace koladata::python
