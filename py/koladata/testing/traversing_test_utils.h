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
#ifndef THIRD_PARTY_PY_KOLADATA_TESTING_TRAVERSING_TEST_UTILS_H_
#define THIRD_PARTY_PY_KOLADATA_TESTING_TRAVERSING_TEST_UTILS_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Asserts that two DataSlices are deep equivalent.
// Takes two DataSlices as positional arguments.
// Returns None if the slices are equivalent, otherwise sets a Python exception
// with the description of the mismatches (and returns nullptr).
PyObject* absl_nullable PyAssertDeepEquivalent(PyObject* /*module*/,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TESTING_TRAVERSING_TEST_UTILS_H_
