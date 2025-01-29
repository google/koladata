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
#include <Python.h>

#include "koladata/data_slice.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/koladata/base/to_py_object.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(to_py_object_testing_clib, m) {
  m.def("py_object_from_data_item", [](const arolla::TypedValue& qvalue_item) {
    const DataSlice& ds = qvalue_item.UnsafeAs<DataSlice>();
    return py::reinterpret_steal<py::object>(
        PyObjectFromDataItem(ds.item(), ds.GetSchema().item(), ds.GetBag())
            .release());
  });

  m.def("py_object_from_data_slice", [](const arolla::TypedValue& qvalue_item) {
    return py::reinterpret_steal<py::object>(
        PyObjectFromDataSlice(qvalue_item.UnsafeAs<DataSlice>()).release());
  });
}

}  // namespace
}  // namespace koladata::python
