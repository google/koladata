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

#include "py/koladata/types/py_misc.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.types.py_misc";

PyMethodDef kPyMiscModule_methods[] = {
    {"make_literal_operator", PyMakeLiteralOperator, METH_O,
     "Constructs an operator holding the provided QValue."},
    {"literal", PyMakeLiteralExpr, METH_O,
     "Constructs an expr with a LiteralOperator wrapping the provided QValue."},
    {"add_schema_constants", PyModule_AddSchemaConstants,
     METH_NOARGS, "Creates schema constants and adds them to the module."},
    {nullptr} /* sentinel */
};

struct PyModuleDef py_misc_module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"LiteralOperator QValue specialization.",
    -1,
    /*methods=*/kPyMiscModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_misc_py_ext(void) {
  return PyModule_Create(&py_misc_module);
}

}  // namespace
}  // namespace koladata::python
