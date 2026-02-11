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
#ifndef PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_
#define PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Evaluates an expression on provided input QValues.
//
// NOTE: This function relies on the compilation cache to reduce evaluation
// costs.
PyObject* absl_nullable PyEvalExpr(PyObject* /*self*/, PyObject** py_args,
                                   Py_ssize_t nargs, PyObject* py_kwnames);

// Evaluates an operator on the provided arguments.
//
// NOTE: This function relies on the compilation cache to reduce evaluation
// costs.
PyObject* absl_nullable PyEvalOp(PyObject* /*self*/, PyObject** py_args,
                                 Py_ssize_t nargs, PyObject* py_kwnames);

// Evaluates an operator on the provided arguments (if all arguments are eager)
// or returns an expression that will evaluate the operator when executed.
//
// NOTE: This function relies on the compilation cache to reduce evaluation
// costs.
PyObject* absl_nullable PyEvalOrBindOp(PyObject* /*self*/, PyObject** py_args,
                                       Py_ssize_t nargs, PyObject* py_kwnames);

// Returns the constant representing the unspecified self input.
PyObject* PyUnspecifiedSelfInput(PyObject* /*self*/, PyObject* /*py_args*/);

// Clears the Koda-specific compilation cache.
PyObject* PyClearCompilationCache(PyObject* /*self*/, PyObject* /*py_args*/);

// Returns an Expr containing a new non-deterministic token.
PyObject* NewNonDeterministicToken(PyObject* /*self*/, PyObject* /*py_args*/);

}  // namespace koladata::python

#endif  // PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_
