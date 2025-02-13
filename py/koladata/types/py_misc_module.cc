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

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_misc_py_ext(void) {
  static PyMethodDef py_methods[] = {
      kDefPyLiteral,
      kDefPyAddSchemaConstants,
      kDefPyFlattenPyList,
      {nullptr} /* sentinel */
  };
  static PyModuleDef py_module = {
      .m_base = PyModuleDef_HEAD_INIT,
      .m_name = kThisModuleName,
      .m_doc = "Miscellaneous Python utilities.",
      .m_size = -1,
      .m_methods = py_methods,
  };
  return PyModule_Create(&py_module);
}

}  // namespace
}  // namespace koladata::python
