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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_DATA_SLICE_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_DATA_SLICE_H_

#include <Python.h>

#include "absl/base/nullability.h"

namespace koladata::python {

// Returns a PyType of Python DataSlice.
PyTypeObject* /*absl_nullable*/ PyDataSlice_Type();

// Returns true iff `attr_name` can be accessed through:
//   getattr(slice, attr_name)
PyObject* /*absl_nullable*/ PyDataSliceModule_is_compliant_attr_name(
    PyObject* /*module*/, PyObject* attr_name);

// Registers a name in method_name to be reserved as a method of the DataSlice
// class or its subclasses. For these registered method names, getattr will
// invoke a PyObject_GenericGetAttr.
//
// This must be called when adding new methods to the class or its subclasses in
// Python.
PyObject* /*absl_nullable*/ PyDataSliceModule_register_reserved_class_method_name(
    PyObject* /*module*/, PyObject* method_name);

// Returns a frozenset of method names that are either defined on the CPython
// DataSlice implementation or specifically reserved using
// register_reserved_class_method_name.
PyObject* /*absl_nullable*/ PyDataSliceModule_get_reserved_attrs(
    PyObject* /*module*/);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_DATA_SLICE_H_
