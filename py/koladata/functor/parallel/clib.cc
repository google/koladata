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
#include "py/arolla/abc/pybind11_utils.h"
#include "py/koladata/functor/parallel/py_executor.h"
#include "pybind11/attr.h"
#include "pybind11/cast.h"
#include "pybind11/pybind11.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  m.add_object("Executor", arolla::python::pybind11_steal_or_throw<py::type>(
                               PyExecutorType()));
}

}  // namespace
}  // namespace koladata::python
