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
#ifndef THIRD_PARTY_PY_KOLADATA_BASE_PY_FUNCTORS_BASE_H_
#define THIRD_PARTY_PY_KOLADATA_BASE_PY_FUNCTORS_BASE_H_

#include <Python.h>

namespace koladata::python {

// def positional_only_parameter_kind(...)
extern const PyMethodDef kDefPyPositionalOnlyParameterKind;

// def positional_or_keyword_parameter_kind(...)
extern const PyMethodDef kDefPyPositionalOrKeywordParameterKind;

// def var_positional_parameter_kind(...)
extern const PyMethodDef kDefPyVarPositionalParameterKind;

// def keyword_only_parameter_kind(...)
extern const PyMethodDef kDefPyKeywordOnlyParameterKind;

// def var_keyword_parameter_kind(...)
extern const PyMethodDef kDefPyVarKeywordParameterKind;

// def no_default_value_marker(...)
extern const PyMethodDef kDefPyNoDefaultValueMarker;

// def create_functor(...)
extern const PyMethodDef kDefPyCreateFunctor;

// def is_fn(...)
extern const PyMethodDef kDefPyIsFn;

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_BASE_PY_FUNCTORS_BASE_H_
