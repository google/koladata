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

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/boxing.h"

namespace koladata::python {
namespace {

constexpr const char* kDataClassesUtilModuleName =
    "koladata.base.py_conversions.dataclasses_util";

constexpr const char* kFieldsNamesAndValuesFnName = "fields_names_and_values";
constexpr const char* kGetClassFieldTypeFnName = "get_class_field_type";
constexpr const char* kMaybeDecayOptionalFnName = "maybe_decay_optional";

using ::arolla::python::PyObjectPtr;

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

absl::StatusOr<bool> IsSimpleNamespace(PyObject* py_obj) {
  ASSIGN_OR_RETURN(PyObject * simple_namespace_type, GetSimpleNamespaceType());
  return PyObject_TypeCheck(
      py_obj, reinterpret_cast<PyTypeObject*>(simple_namespace_type));
}

}  // namespace

absl::StatusOr<PyObject* absl_nonnull> GetSimpleNamespaceType() {
  arolla::python::DCheckPyGIL();
  // Note: this lazy static cache relies on the GIL and will need
  // synchronization if free-threaded Python is enabled.
  static PyObject* simple_namespace_type = nullptr;
  if (simple_namespace_type == nullptr) {
    PyObjectPtr types_module = PyObjectPtr::Own(PyImport_ImportModule("types"));
    PyObjectPtr ns_type;
    if (types_module != nullptr) {
      ns_type = PyObjectPtr::Own(
          PyObject_GetAttrString(types_module.get(), "SimpleNamespace"));
    }
    if (ns_type == nullptr) {
      return arolla::python::StatusCausedByPyErr(
          absl::StatusCode::kInternal, "could not get types.SimpleNamespace");
    }
    if (!PyType_Check(ns_type.get())) {
      return absl::InternalError("types.SimpleNamespace is not a type");
    }
    simple_namespace_type = ns_type.release();
  }
  return simple_namespace_type;
}

absl::StatusOr<PyObject*> DataClassesUtil::GetPyStr(absl::string_view str) {
  auto it = py_str_cache_.find(str);
  if (it != py_str_cache_.end()) {
    return it->second.get();
  }
  PyObject* raw_str = PyUnicode_FromStringAndSize(str.data(), str.size());
  if (raw_str != nullptr) {
    PyUnicode_InternInPlace(&raw_str);
    PyObjectPtr py_str = PyObjectPtr::Own(raw_str);
    Py_ssize_t size;
    if (const char* data = PyUnicode_AsUTF8AndSize(py_str.get(), &size)) {
      PyObject* result = py_str.get();
      py_str_cache_[absl::string_view(data, size)] = std::move(py_str);
      return result;
    }
  }
  return arolla::python::StatusCausedByPyErr(
      absl::StatusCode::kInternal, "failed to construct a python string");
}

absl::StatusOr<PyObjectPtr> DataClassesUtil::CreateClassInstanceKwargs(
    PyObjectPtr absl_nonnull py_class,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const PyObjectPtr absl_nonnull> attr_values) {
  const size_t attr_names_size = attr_names.size();
  if (attr_names_size != attr_values.size()) {
    return absl::InvalidArgumentError(
        "attr_names and attr_values should have the same size");
  }
  std::vector<PyObject*> args(attr_names_size);
  PyObjectPtr kwnames = PyObjectPtr::Own(PyTuple_New(attr_names_size));
  if (kwnames == nullptr) {
    return arolla::python::StatusCausedByPyErr(absl::StatusCode::kInternal,
                                               "could not create a new tuple");
  }
  for (size_t i = 0; i < attr_names.size(); ++i) {
    ASSIGN_OR_RETURN(PyObject * py_str, GetPyStr(attr_names[i]));
    PyTuple_SET_ITEM(kwnames.get(), i, Py_NewRef(py_str));
    args[i] = attr_values[i].get();
  }
  PyObjectPtr py_instance = PyObjectPtr::Own(
      PyObject_Vectorcall(py_class.get(), args.data(), 0, kwnames.get()));
  if (py_instance == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        "could not create a new instance of the class");
  }
  return py_instance;
}

absl::StatusOr<std::vector<PyObject*>> DataClassesUtil::GetDictValues(
    PyObject* py_dict, absl::Span<const absl::string_view> attr_names) {
  std::vector<PyObject*> values;
  values.reserve(attr_names.size());
  for (absl::string_view attr_name : attr_names) {
    ASSIGN_OR_RETURN(PyObject * py_attr_name, GetPyStr(attr_name));
    PyObject* py_value = PyDict_GetItemWithError(py_dict, py_attr_name);
    if (py_value == nullptr) {
      if (PyErr_Occurred()) {
        return arolla::python::StatusWithRawPyErr(
            absl::StatusCode::kInvalidArgument,
            absl::StrFormat("could not get attr %s", attr_name));
      }
      values.push_back(Py_None);
      continue;
    }
    owned_values_.push_back(PyObjectPtr::NewRef(py_value));
    values.push_back(py_value);
  }
  return values;
}

absl::StatusOr<std::vector<PyObject*>> DataClassesUtil::GetAttrValues(
    PyObject* py_obj, absl::Span<const absl::string_view> attr_names) {
  if (PyDict_CheckExact(py_obj)) {
    Py_ssize_t pos = 0;
    PyObject* py_key = nullptr;
    while (PyDict_Next(py_obj, &pos, &py_key, nullptr)) {
      if (!PyUnicode_Check(py_key)) {
        RETURN_IF_ERROR(PyDictKeyAsStringView(py_key).status());
      }
    }
    return GetDictValues(py_obj, attr_names);
  }
  if (PyDict_Check(py_obj)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "object with unsupported type: %s", Py_TYPE(py_obj)->tp_name));
  }
  ASSIGN_OR_RETURN(bool is_simple_namespace, IsSimpleNamespace(py_obj));
  if (is_simple_namespace) {
    PyObjectPtr py_dict =
        PyObjectPtr::Own(PyObject_GenericGetDict(py_obj, nullptr));
    if (py_dict == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument,
          "could not get __dict__ from SimpleNamespace");
    }
    // Look up directly in __dict__ instead of PyObject_GetAttr to avoid raising
    // and clearing AttributeError on missing attributes.
    return GetDictValues(py_dict.get(), attr_names);
  }
  std::vector<PyObject*> values;
  values.reserve(attr_names.size());
  for (absl::string_view attr_name : attr_names) {
    ASSIGN_OR_RETURN(PyObject * py_attr_name, GetPyStr(attr_name));
    PyObject* py_value = PyObject_GetAttr(py_obj, py_attr_name);
    if (py_value == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat("could not get attr %s", attr_name));
    }
    owned_values_.push_back(PyObjectPtr::Own(py_value));
    values.push_back(py_value);
  }
  return values;
}

absl::StatusOr<std::optional<DataClassesUtil::FieldTypeDescriptor>>
DataClassesUtil::GetClassFieldType(PyObjectPtr absl_nonnull py_class,
                                   absl::string_view attr_name) {
  if (attr_name.empty()) {
    return absl::InvalidArgumentError("attr_name is empty");
  }
  RETURN_IF_ERROR(InitFns());
  ASSIGN_OR_RETURN(PyObject * py_attr_name, GetPyStr(attr_name));
  PyObject* call_args[] = {py_class.get(), py_attr_name,
                           type_hints_cache_.get()};
  PyObjectPtr py_attr = PyObjectPtr::Own(PyObject_Vectorcall(
      fn_get_class_field_type_.get(), call_args, 3, nullptr));
  if (py_attr == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("Could not get attribute %s from class", attr_name));
  }
  if (Py_IsNone(py_attr.get())) {
    return std::nullopt;
  }
  if (!PyTuple_Check(py_attr.get()) || PyTuple_GET_SIZE(py_attr.get()) != 2) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat(
            "dataclasses.get_class_field_type is expected to return a tuple "
            "with 2 elements, got %s",
            py_attr.get()->ob_type->tp_name));
  }
  return FieldTypeDescriptor{
      .type = PyObjectPtr::NewRef(PyTuple_GET_ITEM(py_attr.get(), 0)),
      .is_optional = PyTuple_GET_ITEM(py_attr.get(), 1) == Py_True};
}

absl::StatusOr<std::optional<DataClassesUtil::AttrResult>>
DataClassesUtil::GetAttrNamesAndValues(PyObject* py_obj) {
  RETURN_IF_ERROR(InitFns());

  PyObjectPtr py_fields =
      PyObjectPtr::Own(PyObject_CallOneArg(fn_fields_.get(), /*args=*/py_obj));
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

absl::StatusOr<DataClassesUtil::FieldTypeDescriptor>
DataClassesUtil::MaybeDecayOptional(PyObjectPtr absl_nonnull py_type) {
  RETURN_IF_ERROR(InitFns());
  PyObjectPtr py_result = PyObjectPtr::Own(
      PyObject_CallOneArg(fn_maybe_decay_optional_.get(), py_type.get()));
  if (py_result == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument, "could not decay optional type");
  }
  if (!PyTuple_Check(py_result.get()) ||
      PyTuple_GET_SIZE(py_result.get()) != 2) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInvalidArgument,
        absl::StrFormat("koladata.base.py_conversions.dataclasses_util.maybe_"
                        "decay_optional is expected to return a tuple "
                        "with 2 elements, got %s",
                        py_result.get()->ob_type->tp_name));
  }
  return FieldTypeDescriptor{
      .type = PyObjectPtr::NewRef(PyTuple_GET_ITEM(py_result.get(), 0)),
      .is_optional = PyTuple_GET_ITEM(py_result.get(), 1) == Py_True,
  };
}

absl::Status DataClassesUtil::InitFns() {
  if (fns_initialized_) {
    return absl::OkStatus();
  }

  ASSIGN_OR_RETURN(fn_fields_, InitFnFromModule(kFieldsNamesAndValuesFnName));
  ASSIGN_OR_RETURN(fn_get_class_field_type_,
                   InitFnFromModule(kGetClassFieldTypeFnName));
  ASSIGN_OR_RETURN(fn_maybe_decay_optional_,
                   InitFnFromModule(kMaybeDecayOptionalFnName));
  type_hints_cache_ = PyObjectPtr::Own(PyDict_New());
  if (type_hints_cache_ == nullptr) {
    return arolla::python::StatusCausedByPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a new dict for type hints cache"));
  }

  fns_initialized_ = true;
  return absl::OkStatus();
}

}  // namespace koladata::python
