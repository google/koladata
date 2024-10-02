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
#include "py/koladata/types/py_attr_provider.h"

#include <Python.h>

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

namespace {

constexpr const char* kDataClassesModuleName = "dataclasses";

// Returns true if `py_obj` is dataclasses.dataclass.
bool IsDataclasses(PyObject* module_ptr, PyObject* py_obj) {
  auto py_is_dataclass = arolla::python::PyObjectPtr::Own(
      PyObject_CallMethod(module_ptr, "is_dataclass", "O", py_obj));
  if (py_is_dataclass.get() == nullptr) {
    PyErr_Clear();
    return false;
  }
  return py_is_dataclass.get() == Py_True;
}

}  // namespace

AttrProvider::AttrProvider() {
  static PyObject* module_name = PyUnicode_InternFromString(
      kDataClassesModuleName);
  dataclasses_module_ = arolla::python::PyObjectPtr::Own(
      PyImport_GetModule(module_name));
  if (dataclasses_module_.get() == nullptr) {
    // NOTE: Client code can still work with empty module, just won't be able to
    // parse dataclasses.
    PyErr_Clear();
  }
}

absl::StatusOr<std::optional<AttrProvider::AttrResult>>
AttrProvider::GetAttrNamesAndValues(PyObject* py_obj) {
  if (dataclasses_module_.get() == nullptr) {
    return std::nullopt;
  }
  if (!IsDataclasses(dataclasses_module_.get(), py_obj)) {
    return std::nullopt;
  }
  auto py_fields = arolla::python::PyObjectPtr::Own(
      PyObject_CallMethod(dataclasses_module_.get(), "fields",
                          "O", py_obj));
  if (py_fields.get() == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument, "");
  }
  if (!PyTuple_Check(py_fields.get())) {
    return absl::InternalError(
        "dataclasses.fields is expected to return a tuple");
  }
  size_t num_fields = PyTuple_Size(py_fields.get());
  std::vector<absl::string_view> attr_names;
  attr_names.reserve(num_fields);
  std::vector<PyObject*> values;
  values.reserve(num_fields);
  for (size_t i = 0; i < num_fields; ++i) {
    PyObject* py_field = PyTuple_GET_ITEM(py_fields.get(), i);
    auto field_name = arolla::python::PyObjectPtr::Own(
        PyObject_GetAttrString(py_field, "name"));
    if (field_name.get() == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument, "");
    }
    Py_ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(field_name.get(), &size);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid unicode object");
    }
    attr_names.push_back(absl::string_view(data, size));
    // New reference which should be decremented after usage.
    owned_values_.push_back(
        arolla::python::PyObjectPtr::Own(PyObject_GetAttr(py_obj,
                                                          field_name.get())));
    if (owned_values_.back().get() == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument, "");
    }
    values.push_back(owned_values_.back().get());
  }
  return AttrResult{std::move(attr_names), std::move(values)};
}

}  // namespace koladata::python
