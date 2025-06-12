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
#ifndef KOLADATA_INTERNAL_PY_CONVERSIONS_TO_PY_H_
#define KOLADATA_INTERNAL_PY_CONVERSIONS_TO_PY_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Implementation of `to_py` method. Returns a new reference to a Python object.
// In case of error, sets an exception and returns nullptr.
// The arguments match the ones in `DataSlice._to_py_impl`.
// TODO: Move the `to_py` function definition here instead of
// making it a member of `DataSlice`.
PyObject* absl_nullable PyDataSlice_to_py(PyObject* self,
                                          PyObject* const* py_args,
                                          Py_ssize_t nargs);

}  // namespace koladata::python

#endif  // KOLADATA_INTERNAL_PY_CONVERSIONS_TO_PY_H_
