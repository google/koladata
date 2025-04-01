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

#include "py/koladata/functor/py_functors.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.functor.py_functors";

PyMethodDef kPyFunctorsModule_methods[] = {
    {"positional_only_parameter_kind", PyPositionalOnlyParameterKind,
     METH_NOARGS,
     "Returns the constant representing a positional only parameter kind."},
    {"positional_or_keyword_parameter_kind", PyPositionalOrKeywordParameterKind,
     METH_NOARGS,
     "Returns the constant representing a positional or keyword parameter "
     "kind."},
    {"var_positional_parameter_kind", PyVarPositionalParameterKind, METH_NOARGS,
     "Returns the constant representing a variadic positional parameter kind."},
    {"keyword_only_parameter_kind", PyKeywordOnlyParameterKind, METH_NOARGS,
     "Returns the constant representing a keyword only parameter kind."},
    {"var_keyword_parameter_kind", PyVarKeywordParameterKind, METH_NOARGS,
     "Returns the constant representing a variadic keyword parameter kind."},
    {"no_default_value_marker", PyNoDefaultValueMarker, METH_NOARGS,
     "Returns the constant representing lack of a default value for a "
     "parameter."},
    {"create_functor", (PyCFunction)PyCreateFunctor,
     METH_FASTCALL | METH_KEYWORDS, "Creates a new functor."},
    {"is_fn", PyIsFn, METH_O,
     "Checks if a given DataSlice represents a functor."},
    {"auto_variables", PyAutoVariables, METH_O,
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
  return PyModule_Create(&py_functors_module);
}

}  // namespace
}  // namespace koladata::python
