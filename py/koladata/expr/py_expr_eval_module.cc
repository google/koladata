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

#include "py/koladata/expr/py_expr_eval.h"

namespace koladata::python {
namespace {

constexpr const char* kThisModuleName = "koladata.expr.py_expr_eval";

PyMethodDef kPyExprEvalModule_methods[] = {
    {"eval_expr", (PyCFunction)PyEvalExpr, METH_FASTCALL | METH_KEYWORDS,
     "Evaluates an expression on provided input QValues."},
    {"clear_eval_cache", PyClearEvalCache, METH_NOARGS,
     "Clears Koda specific eval caches."},
    {"unspecified_self_input", (PyCFunction)PyUnspecifiedSelfInput, METH_NOARGS,
     "Returns the constant representing the unspecified self input."},
    {"eval_op", (PyCFunction)PyEvalOp, METH_FASTCALL | METH_KEYWORDS,
     ("eval_op(op, *args, **kwrgas)\n--\n\n"
      "Evaluates an operator on the provided arguments.")},
    {nullptr} /* sentinel */
};

struct PyModuleDef py_expr_eval_module = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"Koda Expr evaluation.",
    -1,
    /*methods=*/kPyExprEvalModule_methods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_expr_eval_py_ext(void) {
  return PyModule_Create(&py_expr_eval_module);
}

}  // namespace
}  // namespace koladata::python
