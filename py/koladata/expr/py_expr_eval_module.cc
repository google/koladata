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

#include "koladata/expr/non_determinism.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/expr/py_expr_eval.h"

namespace koladata::python {
namespace {

using ::arolla::python::PyObjectPtr;
using ::koladata::expr::kNonDeterministicTokenLeafKey;

constexpr const char* kThisModuleName = "koladata.expr.py_expr_eval";

PyMethodDef kPyExprEvalModule_methods[] = {
    {"eval_expr", (PyCFunction)PyEvalExpr, METH_FASTCALL | METH_KEYWORDS,
     "Evaluates an expression on provided input QValues."},
    {"clear_compilation_cache", PyClearCompilationCache, METH_NOARGS,
     "clear_compilation_cache()\n"
     "--\n\n"
     "Clears Koda specific eval caches."},
    {"unspecified_self_input", (PyCFunction)PyUnspecifiedSelfInput, METH_NOARGS,
     "Returns the constant representing the unspecified self input."},
    {"eval_op", (PyCFunction)PyEvalOp, METH_FASTCALL | METH_KEYWORDS,
     ("eval_op(op, *args, **kwrgas)\n--\n\n"
      "Evaluates an operator on the provided arguments.")},
    {"new_non_deterministic_token", (PyCFunction)NewNonDeterministicToken,
     METH_NOARGS,
     "Returns a new unique value for argument marked with non_deterministic "
     "marker."},
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
  auto result = PyObjectPtr::Own(PyModule_Create(&py_expr_eval_module));
  if (result == nullptr) {
    return nullptr;
  }
  if (PyModule_AddStringConstant(
          result.get(), "NON_DETERMINISTIC_TOKEN_LEAF_KEY",
          std::string(kNonDeterministicTokenLeafKey).c_str()) < 0) {
    return nullptr;
  }
  return result.release();
}

}  // namespace
}  // namespace koladata::python
