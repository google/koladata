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
#ifndef PY_KOLADATA_TYPES_PY_MISC_H_
#define PY_KOLADATA_TYPES_PY_MISC_H_

#include <Python.h>

namespace koladata::python {

// def literal(...)
extern const PyMethodDef kDefPyLiteral;

// def add_schema_constants(...)
extern const PyMethodDef kDefPyAddSchemaConstants;

// def flatten_py_list(...)
extern const PyMethodDef kDefPyFlattenPyList;

// def get_jagged_shape_qtype(...)
extern const PyMethodDef kDefPyGetJaggedShapeQType;

}  // namespace koladata::python

#endif  // PY_KOLADATA_TYPES_PY_MISC_H_
