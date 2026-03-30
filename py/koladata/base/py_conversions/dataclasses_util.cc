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

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
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
constexpr const char* kGetClassFieldTypeFnName = "get_class_field_type";
constexpr const char* kHasOptionalFieldFnName = "has_optional_field";

constexpr const char* kSimpleNamespaceClassName = "_simple_namespace_class";

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

  if (dataclasses_util_module == nullptr) {
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

  if (fn == nullptr) {
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
  RETURN_IF_ERROR(InitFns());

  const std::vector<absl::string_view> sorted_attr_names =
      GetSortedAttrNames(attr_names);
  const size_t attr_names_size = sorted_attr_names.size();
  const arolla::Fingerprint fp = GetAttrNamesFingerprint(sorted_attr_names);
  if (auto it = dataclasses_cache_.find(fp); it != dataclasses_cache_.end()) {
    return it->second;
  }

  PyObjectPtr py_list = PyObjectPtr::Own(PyList_New(attr_names_size));
  if (py_list == nullptr) {
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

absl::StatusOr<arolla::python::PyObjectPtr>
DataClassesUtil::CreateClassInstanceKwargs(
    PyObjectPtr absl_nonnull py_class, absl::Span<const std::string> attr_names,
    absl::Span<const PyObjectPtr absl_nonnull> attr_values) {
  const size_t attr_names_size = attr_names.size();
  if (attr_names_size != attr_values.size()) {
    return absl::InvalidArgumentError(
        "attr_names and attr_values should have the same size");
  }
  PyObjectPtr py_dict = PyObjectPtr::Own(PyDict_New());
  if (py_dict == nullptr) {
    return arolla::python::StatusCausedByPyErr(absl::StatusCode::kInternal,
                                               "could not create a new dict");
  }

  for (size_t i = 0; i < attr_names_size; ++i) {
    if (PyDict_SetItemString(py_dict.get(), attr_names[i].c_str(),
                             attr_values[i].get()) < 0) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInternal,
          absl::StrFormat("could not set value for attr %s", attr_names[i]));
    }
  }

  PyObjectPtr py_args = PyObjectPtr::Own(PyTuple_New(0));
  if (py_args == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal, "could not create a new empty tuple");
  }

  PyObjectPtr py_instance = PyObjectPtr::Own(
      PyObject_Call(py_class.get(), py_args.get(), py_dict.get()));
  if (py_instance == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        "could not create a new instance of the class");
  }
  return py_instance;
}

absl::StatusOr<arolla::python::PyObjectPtr>
DataClassesUtil::GetSimpleNamespaceClass() {
  RETURN_IF_ERROR(InitFns());
  return simple_namespace_class_;
}

absl::StatusOr<PyObjectPtr> DataClassesUtil::MakeDataClassInstance(
    absl::Span<const absl::string_view> attr_names) {
  ASSIGN_OR_RETURN(PyObjectPtr py_dataclass, MakeDataClass(attr_names));

  PyObjectPtr py_instance =
      PyObjectPtr::Own(PyObject_CallNoArgs(py_dataclass.get()));

  if (py_instance == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat(
            "could not create a new dataclass instance with attr names %s",
            absl::StrJoin(attr_names, ",")));
  }
  return py_instance;
}

absl::StatusOr<std::vector<PyObject*>> DataClassesUtil::GetAttrValues(
    PyObject* py_obj, absl::Span<const absl::string_view> attr_names) {
  std::vector<PyObject*> values;
  values.reserve(attr_names.size());
  for (absl::string_view attr_name : attr_names) {
    PyObject* py_value =
        PyObject_GetAttrString(py_obj, std::string(attr_name).c_str());
    if (py_value == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("could not get attr %s", attr_name));
    }
    owned_values_.push_back(PyObjectPtr::NewRef(py_value));
    values.push_back(py_value);
  }
  return values;
}

absl::StatusOr<PyObjectPtr> DataClassesUtil::GetClassFieldType(
    PyObjectPtr absl_nonnull py_class, absl::string_view attr_name,
    bool for_primitive) {
  if (attr_name.empty()) {
    return absl::InvalidArgumentError("attr_name is empty");
  }
  RETURN_IF_ERROR(InitFns());
  PyObjectPtr args = PyObjectPtr::Own(PyTuple_New(4));
  if (args == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal, "could not create a new tuple of size 4");
  }
  PyTuple_SET_ITEM(args.get(), 0, py_class.release());
  PyObject* py_attr_name =
      PyUnicode_FromStringAndSize(attr_name.data(), attr_name.size());
  if (py_attr_name == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a string for attr name %s",
                        attr_name));
  }
  PyTuple_SET_ITEM(args.get(), 1, py_attr_name);
  PyObject* py_for_primitive = PyBool_FromLong(for_primitive);
  PyTuple_SET_ITEM(args.get(), 2, py_for_primitive);
  PyTuple_SET_ITEM(args.get(), 3,
                   PyObjectPtr::NewRef(type_hints_cache_.get()).release());
  PyObjectPtr py_attr = PyObjectPtr::Own(PyObject_Call(
      fn_get_class_field_type_.get(), /*args=*/args.get(), nullptr));
  if (py_attr == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("Could not get attribute %s from class", attr_name));
  }
  return py_attr;
}

absl::StatusOr<std::optional<DataClassesUtil::AttrResult>>
DataClassesUtil::GetAttrNamesAndValues(PyObject* py_obj) {
  RETURN_IF_ERROR(InitFns());

  PyObjectPtr py_fields = arolla::python::PyObjectPtr::Own(
      PyObject_CallOneArg(fn_fields_.get(), /*args=*/py_obj));
  if (py_fields == nullptr) {
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

absl::StatusOr<bool> DataClassesUtil::HasOptionalField(
    PyObjectPtr absl_nonnull py_class, absl::string_view attr_name) {
  if (attr_name.empty()) {
    return absl::InvalidArgumentError("attr_name is empty");
  }
  RETURN_IF_ERROR(InitFns());
  PyObjectPtr args = PyObjectPtr::Own(PyTuple_New(3));
  PyTuple_SET_ITEM(args.get(), 0, py_class.release());
  PyObject* py_attr_name =
      PyUnicode_FromStringAndSize(attr_name.data(), attr_name.size());
  if (py_attr_name == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a string for attr name %s",
                        attr_name));
  }
  PyTuple_SET_ITEM(args.get(), 1, py_attr_name);
  PyTuple_SET_ITEM(args.get(), 2, Py_NewRef(type_hints_cache_.get()));

  PyObjectPtr py_attr = PyObjectPtr::Own(PyObject_Call(
      fn_has_optional_field_.get(), /*args=*/args.get(), nullptr));
  if (py_attr == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("could not check dataclass field %s", attr_name));
  }
  return Py_IsTrue(py_attr.get());
}

absl::Status DataClassesUtil::InitFns() {
  if (fns_initialized_) {
    return absl::OkStatus();
  }

  ASSIGN_OR_RETURN(fn_make_dataclass_, InitFnFromModule(kMakeDataClassFnName));
  ASSIGN_OR_RETURN(fn_fields_, InitFnFromModule(kFieldsNamesAndValuesFnName));
  ASSIGN_OR_RETURN(fn_get_class_field_type_,
                   InitFnFromModule(kGetClassFieldTypeFnName));
  ASSIGN_OR_RETURN(fn_has_optional_field_,
                   InitFnFromModule(kHasOptionalFieldFnName));
  type_hints_cache_ = PyObjectPtr::Own(PyDict_New());
  if (type_hints_cache_ == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a new dict for type hints cache"));
  }

  ASSIGN_OR_RETURN(simple_namespace_class_,
                   InitFnFromModule(kSimpleNamespaceClassName));
  if (simple_namespace_class_ == nullptr) {
    return absl::InternalError("could not get simple namespace class");
  }

  fns_initialized_ = true;
  return absl::OkStatus();
}

}  // namespace koladata::python
