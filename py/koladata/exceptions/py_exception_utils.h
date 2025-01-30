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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_PY_EXCEPTION_UTILS_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_PY_EXCEPTION_UTILS_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Registers the KodaError python exception factory function.
// The factory function in Python should have the signature:
// def func(proto: bytes) -> KodaError
absl::Nullable<PyObject*> PyRegisterExceptionFactory(PyObject* /*module*/,
                                                     PyObject* factory);

// Sets the KodaError config in C++
absl::Nullable<PyObject*> PySetErrorConfig(PyObject* /*module*/,
                                           PyObject* enable_verbose_errors);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_PY_EXCEPTION_UTILS_H_
