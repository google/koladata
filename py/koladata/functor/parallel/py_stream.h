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
#ifndef THIRD_PARTY_PY_KOLADATA_FUNCTOR_PARALLEL_PY_STREAM_H_
#define THIRD_PARTY_PY_KOLADATA_FUNCTOR_PARALLEL_PY_STREAM_H_

#include <Python.h>

namespace koladata::python {

// Returns PyTypeObject corresponding to PyStream type (or nullptr and
// sets a python exception).
PyTypeObject* PyStreamType();

// Returns PyTypeObject corresponding to PyStreamWriter type (or nullptr and
// sets a python exception).
PyTypeObject* PyStreamWriterType();

// Returns PyTypeObject corresponding to PyStreamReader type (or nullptr and
// sets a python exception).
PyTypeObject* PyStreamReaderType();

// def make_stream(...)
extern const PyMethodDef kDefPyMakeStream;

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_FUNCTOR_PARALLEL_PY_STREAM_H_
