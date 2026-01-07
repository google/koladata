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
#ifndef PY_KOLADATA_TYPES_DATA_BAG_H_
#define PY_KOLADATA_TYPES_DATA_BAG_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Returns a PyType of PyDataBagObject(s).
PyTypeObject* absl_nullable PyDataBag_Type();

// Returns True if `a` and `b` are exactly equal DataBags or if both are
// NullDataBags.
PyObject* absl_nullable PyDataBagModule_exactly_equal(
    PyObject* /*module*/, PyObject* const* args, Py_ssize_t nargs);

}  // namespace koladata::python

#endif  // PY_KOLADATA_TYPES_DATA_BAG_H_
