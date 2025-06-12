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

#include <string>

#include "koladata/functor/call.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/functor/py_functors.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.functor.py_functors";

PyMethodDef kPyFunctorsModule_methods[] = {
    {"auto_variables", (PyCFunction)PyAutoVariables, METH_FASTCALL,
     "Returns a functor with auto-variables extracted."},
    {nullptr} /* sentinel */
};

struct PyModuleDef py_functors_module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"The module with Python bindings for Koda functors.",
    -1,
    /*methods=*/kPyFunctorsModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_functors_py_ext(void) {
  auto module =
      arolla::python::PyObjectPtr::Own(PyModule_Create(&py_functors_module));
  if (module == nullptr) {
    return nullptr;
  }
  if (PyModule_AddStringConstant(
          module.get(), "STACK_TRACE_FRAME_ATTR",
          std::string(functor::kStackFrameAttrName).c_str()) < 0) {
    return nullptr;
  }
  return module.release();
}

}  // namespace
}  // namespace koladata::python
