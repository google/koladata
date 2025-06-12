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
#ifndef THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_H_
#define THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Returns placeholder for an Expr used in an f-string.
PyObject* PyFStringExprPlaceholder(PyObject* /*module*/, PyObject* const* args,
                                   Py_ssize_t nargs);

// Evaluates transformed f-string into DataSlice.
PyObject* PyEagerFStringEval(PyObject* /*module*/, PyObject* arg);

// Evaluates transformed f-string into expression.
PyObject* absl_nullable PyCreateFstrExpr(PyObject* /*module*/, PyObject* arg);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_H_
