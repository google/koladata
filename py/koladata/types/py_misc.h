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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_PY_MISC_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_PY_MISC_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Constructs an operator holding the provided QValue.
absl::Nullable<PyObject*> PyMakeLiteralOperator(PyObject* /*module*/,
                                                PyObject* value);

// Constructs an ExprNode with a LiteralOperator wrapping value.
absl::Nullable<PyObject*> PyMakeLiteralExpr(PyObject* /*module*/,
                                            PyObject* value);

// Registers all Schema Constants as Python objects into module `m`. Returns
// Py_None, but raises Error on failure.
absl::Nullable<PyObject*> PyModule_AddSchemaConstants(PyObject* m);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_PY_MISC_H_
