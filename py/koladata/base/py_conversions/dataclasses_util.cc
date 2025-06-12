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
#include "py/koladata/base/py_conversions/dataclasses_util.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/fingerprint.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

constexpr const char* kDataClassesUtilModuleName =
    "koladata.base.py_conversions.dataclasses_util";

constexpr const char* kMakeDataClassFnName = "make_dataclass";
constexpr const char* kFieldsNamesAndValuesFnName = "fields_names_and_values";

using arolla::python::PyObjectPtr;

std::vector<absl::string_view> GetSortedAttrNames(
    absl::Span<const absl::string_view> attr_names) {
  std::vector<absl::string_view> sorted_attr_names{attr_names.begin(),
                                                   attr_names.end()};
  std::sort(sorted_attr_names.begin(), sorted_attr_names.end());
  return sorted_attr_names;
}

arolla::Fingerprint GetAttrNamesFingerprint(
    absl::Span<const absl::string_view> sorted_attr_names) {
  arolla::FingerprintHasher hasher("dataclass");
  for (const absl::string_view& attr_name : sorted_attr_names) {
    hasher.Combine(attr_name);
  }
  return std::move(hasher).Finish();
}

absl::StatusOr<PyObjectPtr> InitFnFromModule(absl::string_view fn_name) {
  static PyObject* util_module_name =
      PyUnicode_InternFromString(kDataClassesUtilModuleName);
  PyObjectPtr dataclasses_util_module =
      PyObjectPtr::Own(PyImport_GetModule(util_module_name));

  if (dataclasses_util_module.get() == nullptr) {
    if (!PyErr_Occurred()) {
      PyErr_Format(PyExc_ImportError,
                   "failed to import '%s'; add the following to the "
                   "upper caller: 'import %s as _'",
                   kDataClassesUtilModuleName, kDataClassesUtilModuleName);
    }
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrCat("could not import module ", kDataClassesUtilModuleName));
  }

  PyObjectPtr fn = PyObjectPtr::Own(
      PyObject_GetAttrString(dataclasses_util_module.get(), fn_name.data()));

  if (fn.get() == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not import %s from %s ", fn_name,
                        kDataClassesUtilModuleName));
  }
  return fn;
}

}  // namespace

absl::StatusOr<PyObjectPtr> DataClassesUtil::MakeDataClass(
    absl::Span<const absl::string_view> attr_names) {
  if (fn_make_dataclass_.get() == nullptr) {
    ASSIGN_OR_RETURN(fn_make_dataclass_,
                     InitFnFromModule(kMakeDataClassFnName));
  }

  const std::vector<absl::string_view> sorted_attr_names =
      GetSortedAttrNames(attr_names);
  const size_t attr_names_size = sorted_attr_names.size();
  const arolla::Fingerprint fp = GetAttrNamesFingerprint(sorted_attr_names);
  if (auto it = dataclasses_cache_.find(fp); it != dataclasses_cache_.end()) {
    return it->second;
  }

  PyObjectPtr py_list = PyObjectPtr::Own(PyList_New(attr_names_size));
  if (py_list.get() == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a new list of size %d",
                        attr_names_size));
  }

  for (size_t i = 0; i < attr_names_size; ++i) {
    const absl::string_view& attr_name = sorted_attr_names[i];
    const PyObject* attr_name_py =
        PyUnicode_FromStringAndSize(attr_name.data(), attr_name.size());
    if (attr_name_py == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInternal,
          absl::StrFormat("could not create a string for attr name %s",
                          attr_name));
    }
    PyList_SET_ITEM(py_list.get(), i, attr_name_py);
  }

  PyObjectPtr dataclass = PyObjectPtr::Own(
      PyObject_CallOneArg(fn_make_dataclass_.get(), /*args=*/py_list.get()));
  if (dataclass == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal, "could not create a new dataclass");
  }

  dataclasses_cache_[fp] = dataclass;
  return dataclass;
}

absl::StatusOr<PyObjectPtr> DataClassesUtil::MakeDataClassInstance(
    absl::Span<const absl::string_view> attr_names) {
  ASSIGN_OR_RETURN(PyObjectPtr py_dataclass, MakeDataClass(attr_names));

  PyObjectPtr py_instance =
      PyObjectPtr::Own(PyObject_CallNoArgs(py_dataclass.get()));

  if (py_instance == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        "could not create a new dataclass instance");
  }
  return py_instance;
}

absl::StatusOr<std::vector<PyObject*>> DataClassesUtil::GetAttrValues(
    PyObject* py_obj, absl::Span<const absl::string_view> attr_names) {
  std::vector<PyObject*> values;
  values.reserve(attr_names.size());
  for (absl::string_view attr_name : attr_names) {
    PyObject* py_value = PyObject_GetAttrString(py_obj, attr_name.data());
    if (py_value == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument, "");
    }
    owned_values_.push_back(PyObjectPtr::NewRef(py_value));
    values.push_back(py_value);
  }
  return values;
}

absl::StatusOr<std::optional<DataClassesUtil::AttrResult>>
DataClassesUtil::GetAttrNamesAndValues(PyObject* py_obj) {
  if (fn_fields_.get() == nullptr) {
    ASSIGN_OR_RETURN(fn_fields_, InitFnFromModule(kFieldsNamesAndValuesFnName));
  }

  PyObjectPtr py_fields = arolla::python::PyObjectPtr::Own(
      PyObject_CallOneArg(fn_fields_.get(), /*args=*/py_obj));
  if (py_fields.get() == nullptr || PyErr_Occurred()) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument, "");
  }
  if (py_fields.get() == Py_None) {
    // py_obj is not a dataclass.
    return std::nullopt;
  }

  if (!PyList_Check(py_fields.get())) {
    return absl::InternalError(
        "dataclasses.fields is expected to return a list");
  }
  size_t num_fields = PyList_GET_SIZE(py_fields.get());
  std::vector<std::string> attr_names;
  attr_names.reserve(num_fields);
  std::vector<PyObject*> values;
  values.reserve(num_fields);
  for (size_t i = 0; i < num_fields; ++i) {
    PyObject* py_field = PyList_GET_ITEM(py_fields.get(), i);
    if (!PyTuple_Check(py_field)) {
      return absl::InternalError(
          "dataclasses.fields is expected to return a list of tuples");
    }
    if (PyTuple_GET_SIZE(py_field) != 2) {
      return absl::InternalError(
          "dataclasses.fields is expected to return a list of tuples with 2 "
          "elements");
    }

    PyObject* field_name = PyTuple_GET_ITEM(py_field, 0);
    if (field_name == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument, "");
    }
    Py_ssize_t size;
    const char* data = PyUnicode_AsUTF8AndSize(field_name, &size);
    if (data == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInvalidArgument, "invalid unicode object");
    }
    attr_names.emplace_back(std::string(data, size));
    // New reference which should be decremented after usage.
    PyObjectPtr field_value =
        PyObjectPtr::NewRef(PyTuple_GET_ITEM(py_field, 1));
    if (field_value == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument, "");
    }
    values.push_back(field_value.get());
    owned_values_.push_back(std::move(field_value));
  }
  return AttrResult{std::move(attr_names), std::move(values)};
}

}  // namespace koladata::python
