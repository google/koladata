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
#include "py/koladata/types/wrap_utils.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "arolla/jagged_shape/array/qtype/qtype.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::python {

namespace {

std::nullptr_t NotDataSliceError(PyObject* py_obj,
                                 absl::string_view name_for_error) {
  PyErr_Format(PyExc_TypeError, "expecting %s to be a DataSlice, got %s",
               std::string(name_for_error).c_str(), Py_TYPE(py_obj)->tp_name);
  return nullptr;
}

std::optional<DataBagPtr> NotDataBagError(PyObject* py_obj,
                                          absl::string_view name_for_error) {
  PyErr_Format(PyExc_TypeError, "expecting %s to be a DataBag, got %s",
               std::string(name_for_error).c_str(), Py_TYPE(py_obj)->tp_name);
  return std::nullopt;
}

std::nullptr_t NotJaggedShapeError(PyObject* py_obj,
                                   absl::string_view name_for_error) {
  PyErr_Format(PyExc_TypeError, "expecting %s to be a JaggedShape, got %s",
               std::string(name_for_error).c_str(), Py_TYPE(py_obj)->tp_name);
  return nullptr;
}

}  // namespace

absl::Nullable<const DataSlice*> UnwrapDataSlice(
    PyObject* py_obj, absl::string_view name_for_error) {
  if (!arolla::python::IsPyQValueInstance(py_obj)) {
    return NotDataSliceError(py_obj, name_for_error);
  }
  const auto& typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
  if (typed_value.GetType() != arolla::GetQType<DataSlice>()) {
    return NotDataSliceError(py_obj, name_for_error);
  }
  return &typed_value.UnsafeAs<DataSlice>();
}

absl::Nullable<PyObject*> WrapPyDataSlice(DataSlice&& ds) {
  return arolla::python::WrapAsPyQValue(
      arolla::TypedValue::FromValue(std::move(ds)));
}

bool UnwrapDataSliceOptionalArg(PyObject* py_obj,
                                absl::string_view name_for_error,
                                std::optional<DataSlice>& arg) {
  if (Py_IsNone(py_obj) || py_obj == nullptr) {
    arg = std::nullopt;
    return true;
  }
  const DataSlice* ds = UnwrapDataSlice(py_obj, name_for_error);
  if (ds == nullptr) {
    return false;
  }
  arg = *ds;
  return true;
}

const DataSlice& UnsafeDataSliceRef(PyObject* py_obj) {
  return arolla::python::UnsafeUnwrapPyQValue(py_obj).UnsafeAs<DataSlice>();
}

absl::Nullable<PyObject*> WrapDataBagPtr(DataBagPtr db) {
  return arolla::python::WrapAsPyQValue(
      arolla::TypedValue::FromValue(std::move(db)));
}

std::optional<DataBagPtr> UnwrapDataBagPtr(PyObject* py_obj,
                                           absl::string_view name_for_error) {
  if (py_obj == Py_None) {
    return nullptr;
  }
  if (!arolla::python::IsPyQValueInstance(py_obj)) {
    return NotDataBagError(py_obj, name_for_error);
  }
  const auto& db_typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
  if (db_typed_value.GetType() != arolla::GetQType<DataBagPtr>()) {
    return NotDataBagError(py_obj, name_for_error);
  }
  return db_typed_value.UnsafeAs<DataBagPtr>();
}

const DataBagPtr& UnsafeDataBagPtr(PyObject* py_obj) {
  return arolla::python::UnsafeUnwrapPyQValue(py_obj).UnsafeAs<DataBagPtr>();
}

absl::Nullable<const DataSlice::JaggedShape*> UnwrapJaggedShape(
    PyObject* py_obj, absl::string_view name_for_error) {
  if (!arolla::python::IsPyQValueInstance(py_obj)) {
    return NotJaggedShapeError(py_obj, name_for_error);
  }
  const auto& shape_typed_value = arolla::python::UnsafeUnwrapPyQValue(py_obj);
  if (shape_typed_value.GetType() !=
      arolla::GetQType<DataSlice::JaggedShape>()) {
    return NotJaggedShapeError(py_obj, name_for_error);
  }
  return &shape_typed_value.UnsafeAs<DataSlice::JaggedShape>();
}

absl::Nullable<PyObject*> WrapPyJaggedShape(DataSlice::JaggedShape shape) {
  return arolla::python::WrapAsPyQValue(
      arolla::TypedValue::FromValue(std::move(shape)));
}

}  // namespace koladata::python
