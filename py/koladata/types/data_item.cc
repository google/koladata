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
#include "py/koladata/types/data_item.h"

#include <cstdint>

#include "absl/base/nullability.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/types/data_slice.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/util/unit.h"

namespace koladata::python {

namespace {

int PyDataItem_bool(PyObject* self) {
  arolla::python::DCheckPyGIL();
  auto error = []() {
    PyErr_SetString(PyExc_ValueError,
                    "Cannot cast a non-MASK DataItem to bool. For BOOLEAN, "
                    "explicit conversion to MASK (e.g. ds == True) is needed. "
                    "See go/koda-basics#mask_vs_bool.");
    return -1;
  };
  const auto& ds = UnsafeDataSliceRef(self);
  if (!ds.GetSchemaImpl().holds_value<schema::DType>()) {
    return error();
  }
  if (auto dtype = ds.GetSchemaImpl().value<schema::DType>();
      dtype != schema::kMask && dtype != schema::kAny &&
      dtype != schema::kObject && dtype != schema::kNone) {
    return error();
  }
  if (ds.item().holds_value<arolla::Unit>() || !ds.item().has_value()) {
    return ds.item().holds_value<arolla::Unit>();
  }
  return error();
}

absl::Nullable<PyObject*> PyDataItem_index(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  if (ds.item().holds_value<int>()) {
    return PyLong_FromLong(ds.item().value<int>());
  }
  if (ds.item().holds_value<int64_t>()) {
    return PyLong_FromLongLong(ds.item().value<int64_t>());
  }
  PyErr_SetString(PyExc_ValueError,
                  "Only INT32/INT64 DataItem can be passed to built-in int");
  return nullptr;
}

absl::Nullable<PyObject*> PyDataItem_float(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  if (ds.item().holds_value<float>()) {
    return PyFloat_FromDouble(ds.item().value<float>());
  }
  if (ds.item().holds_value<double>()) {
    return PyFloat_FromDouble(ds.item().value<double>());
  }
  PyErr_SetString(
      PyExc_ValueError,
      "Only FLOAT32/FLOAT64 DataItem can be passed to built-in float");
  return nullptr;
}

// Creates and initializes PyTypeObject for Python DataItem class.
PyTypeObject* InitPyDataItemType() {
  arolla::python::CheckPyGIL();
  PyTypeObject* py_data_slice_type = PyDataSlice_Type();
  if (py_data_slice_type == nullptr) {
    return nullptr;
  }
  PyType_Slot slots[] = {
      // By being a subclass of DataSlice, it is also a subclass of QValue.
      {Py_tp_base, py_data_slice_type},
      {Py_nb_bool, (void*)PyDataItem_bool},
      {Py_nb_index, (void*)PyDataItem_index},
      {Py_nb_float, (void*)PyDataItem_float},
      // NOTE: It inherits all the methods DataSlice has.
      {0, nullptr},
  };

  PyType_Spec spec = {
      .name = "koladata.types.data_item.DataItem",
      .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE),
      .slots = slots,
  };

  PyObject* qvalue_subtype = PyType_FromSpec(&spec);
  if (!arolla::python::RegisterPyQValueSpecializationByKey(
          kDataItemQValueSpecializationKey, qvalue_subtype)) {
    return nullptr;
  }
  return reinterpret_cast<PyTypeObject*>(qvalue_subtype);
}

}  // namespace

PyTypeObject* PyDataItem_Type() {
  arolla::python::CheckPyGIL();
  static PyTypeObject* type = InitPyDataItemType();
  return type;
}

}  // namespace koladata::python
