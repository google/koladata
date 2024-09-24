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

#include "py/koladata/types/data_slice.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.types.data_slice";

struct PyModuleDef data_slice = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"A DataSlice definition.",
    -1,
    /*methods=*/nullptr,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_data_slice_py_ext(void) {
  PyObject* m = PyModule_Create(&data_slice);
  PyTypeObject* data_slice_type = PyDataSlice_Type();
  if (PyModule_AddType(m, data_slice_type) < 0) {
    // NOTE: When the PyDataSlice_Type() is called for the first type, we
    // increment the refcount.
    Py_DECREF(data_slice_type);
    Py_DECREF(m);
    return nullptr;
  }
  return m;
}

}  // namespace
}  // namespace koladata::python
