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
#include <Python.h>

#include <cstddef>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_conversions/dataclasses_util.h"
#include "pybind11/pytypes.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
namespace koladata::python {
namespace {
namespace py = pybind11;
PYBIND11_MODULE(testing_clib, m) {
  py::class_<DataClassesUtil>(m, "DataClassesUtil")
      .def(py::init<>())
      .def("make_dataclass_instance",
           [](DataClassesUtil& self,
              absl::Span<const absl::string_view> attr_names) -> py::object {
             arolla::python::PyObjectPtr py_obj =
                 arolla::python::pybind11_unstatus_or(
                     self.MakeDataClassInstance(attr_names));
             return py::reinterpret_steal<py::object>(py_obj.release());
           })
      .def("get_class_field_type",
           [](DataClassesUtil& self, py::handle py_class,
              absl::string_view attr_name, bool for_primitive) -> py::object {
             arolla::python::PyObjectPtr py_obj =
                 arolla::python::pybind11_unstatus_or(self.GetClassFieldType(
                     arolla::python::PyObjectPtr::NewRef(py_class.ptr()),
                     attr_name, for_primitive));
             return py::reinterpret_steal<py::object>(py_obj.release());
           })
      .def("has_optional_field",
           [](DataClassesUtil& self, py::handle py_class,
              absl::string_view attr_name) -> bool {
             return arolla::python::pybind11_unstatus_or(self.HasOptionalField(
                 arolla::python::PyObjectPtr::NewRef(py_class.ptr()),
                 attr_name));
           })
      .def("get_attr_values",
           [](DataClassesUtil& self, py::handle dataclass_obj,
              absl::Span<const absl::string_view> attr_names) -> py::object {
             auto attr_values = arolla::python::pybind11_unstatus_or(
                 self.GetAttrValues(dataclass_obj.ptr(), attr_names));
             PyObject* res = PyList_New(attr_names.size());
             for (size_t i = 0; i < attr_names.size(); ++i) {
               PyList_SET_ITEM(res, i, attr_values[i]);
             }
             return py::reinterpret_steal<py::object>(res);
           })
      .def("create_class_instance_kwargs",
           [](DataClassesUtil& self, py::handle py_class,
              absl::Span<const absl::string_view> attr_names,
              absl::Span<const py::handle> attr_values) -> py::object {
             std::vector<arolla::python::PyObjectPtr> attr_values_ptrs;
             for (const auto& attr_value : attr_values) {
               attr_values_ptrs.push_back(
                   arolla::python::PyObjectPtr::NewRef(attr_value.ptr()));
             }
             std::vector<std::string> attr_names_vec(attr_names.begin(),
                                                     attr_names.end());

             auto res = arolla::python::pybind11_unstatus_or(
                 self.CreateClassInstanceKwargs(
                     arolla::python::PyObjectPtr::NewRef(py_class.ptr()),
                     attr_names_vec, attr_values_ptrs));
             return py::reinterpret_steal<py::object>(res.release());
           })
      .def("get_simple_namespace_class",
           [](DataClassesUtil& self) -> py::object {
             arolla::python::PyObjectPtr py_obj =
                 arolla::python::pybind11_unstatus_or(
                     self.GetSimpleNamespaceClass());
             return py::reinterpret_steal<py::object>(py_obj.release());
           });
}
}  // namespace
}  // namespace koladata::python
