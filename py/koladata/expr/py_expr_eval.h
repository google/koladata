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
#ifndef THIRD_PARTY_PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_
#define THIRD_PARTY_PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Evaluates an expression on provided input QValues.
absl::Nullable<PyObject*> PyEvalExpr(PyObject* /*self*/, PyObject** py_args,
                                     Py_ssize_t nargs, PyObject* py_kwnames);

// Returns the constant representing the unspecified self input.
PyObject* PyUnspecifiedSelfInput(PyObject* /*self*/, PyObject* /*py_args*/);

// Clears the Koda-specific eval cache.
PyObject* PyClearEvalCache(PyObject* /*self*/, PyObject* /*py_args*/);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_EXPR_PY_EXPR_EVAL_H_
