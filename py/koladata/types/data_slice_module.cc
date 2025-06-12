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

#include "py/koladata/types/data_slice.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.types.data_slice";

PyMethodDef kPyDataSliceModule_methods[] = {
    {"internal_register_reserved_class_method_name",
     (PyCFunction)PyDataSliceModule_register_reserved_class_method_name, METH_O,
     "internal_register_reserved_class_method_name(method_name, /)\n"
     "--\n\n"
     R"""(Registers a name to be reserved as a method of DataSlice or its subclasses.

You must call this when adding new methods to the class in Python.

Args:
  method_name: (str)
)"""},
    {"internal_is_compliant_attr_name",
     (PyCFunction)PyDataSliceModule_is_compliant_attr_name, METH_O,
     "internal_is_compliant_attr_name(attr_name, /)\n"
     "--\n\n"
     "Returns true iff `attr_name` can be accessed through "
     "`getattr(slice, attr_name)`."},
    {"internal_get_reserved_attrs",
     (PyCFunction)PyDataSliceModule_get_reserved_attrs, METH_NOARGS,
     "internal_get_reserved_attrs() -> frozenset[str]\n"
     "--\n\n"
     "Returns a set of reserved attributes without leading underscore."},
    {nullptr} /* sentinel */
};

struct PyModuleDef data_slice = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"A DataSlice definition.",
    -1,
    /*methods=*/kPyDataSliceModule_methods,
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
