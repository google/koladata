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

#include <optional>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/strings/string_view.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/jagged_shape_qtype.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "pybind11/pytypes.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"

namespace koladata::python {
namespace {
namespace py = pybind11;
using internal::DataItem;
using JaggedShape = arolla::JaggedDenseArrayShape;

PYBIND11_MODULE(wrap_utils_testing_clib, m) {
  m.def("unwrap_wrap_data_slice", [](py::handle dataslice_obj) -> py::object {
    PyObject* py_obj = dataslice_obj.ptr();
    const DataSlice* absl_nullable ds = UnwrapDataSlice(py_obj, "input arg");
    if (ds == nullptr) {
      throw pybind11::error_already_set();
    }
    DataSlice ds_copy = *ds;
    PyObject* absl_nullable res_obj = WrapPyDataSlice(std::move(ds_copy));
    if (py_obj == nullptr) {
      throw pybind11::error_already_set();
    }
    return py::reinterpret_steal<py::object>(res_obj);
  });

  m.def(
      "unsafe_ref_wrap_data_slice", [](py::handle dataslice_obj) -> py::object {
        PyObject* py_obj = dataslice_obj.ptr();
        const DataSlice& ds = UnsafeDataSliceRef(py_obj);
        DataSlice ds_copy = ds;
        PyObject* absl_nullable res_obj = WrapPyDataSlice(std::move(ds_copy));
        if (py_obj == nullptr) {
          throw pybind11::error_already_set();
        }
        return py::reinterpret_steal<py::object>(res_obj);
      });

  m.def("unwrap_optional_wrap_data_slice",
        [](py::handle dataslice_obj) -> py::object {
          PyObject* py_obj = dataslice_obj.ptr();
          std::optional<DataSlice> ds = DataSlice();
          const bool res = UnwrapDataSliceOptionalArg(py_obj, "input arg", ds);
          if (res == false) {
            throw pybind11::error_already_set();
          }
          if (!ds.has_value()) {
            // The result is std::nullopt; we return None to test it in Python.
            return py::none();
          }
          PyObject* absl_nullable res_obj = WrapPyDataSlice(std::move(*ds));
          if (py_obj == nullptr) {
            throw pybind11::error_already_set();
          }
          return py::reinterpret_steal<py::object>(res_obj);
        });

  m.def("unwrap_wrap_data_bag", [](py::handle dataslice_obj) -> py::object {
    PyObject* py_obj = dataslice_obj.ptr();
    std::optional<DataBagPtr> db = UnwrapDataBagPtr(py_obj, "input arg");
    if (!db.has_value()) {
      throw pybind11::error_already_set();
    }
    if (*db == nullptr) {
      // The result is nullptr; we return None to test it in Python.
      return py::none();
    }
    PyObject* absl_nullable res_obj = WrapDataBagPtr(*db);
    if (py_obj == nullptr) {
      throw pybind11::error_already_set();
    }
    return py::reinterpret_steal<py::object>(res_obj);
  });

  m.def("unwrap_unsafe_wrap_data_bag",
        [](py::handle dataslice_obj) -> py::object {
          PyObject* py_obj = dataslice_obj.ptr();
          std::optional<DataBagPtr> db = UnsafeDataBagPtr(py_obj);
          if (!db.has_value()) {
            throw pybind11::error_already_set();
          }
          if (db == nullptr) {
            // The result is nullptr; we return None to test it in Python.
            return py::none();
          }
          PyObject* absl_nullable res_obj = WrapDataBagPtr(*db);
          if (py_obj == nullptr) {
            throw pybind11::error_already_set();
          }
          return py::reinterpret_steal<py::object>(res_obj);
        });

  m.def("unwrap_wrap_jagged_shape", [](py::handle dataslice_obj) -> py::object {
    PyObject* py_obj = dataslice_obj.ptr();
    const JaggedShape* absl_nullable shape =
        UnwrapJaggedShape(py_obj, "input arg");
    if (shape == nullptr) {
      throw pybind11::error_already_set();
    }
    JaggedShape shape_copy = *shape;
    PyObject* absl_nullable res_obj = WrapPyJaggedShape(std::move(shape_copy));
    if (py_obj == nullptr) {
      throw pybind11::error_already_set();
    }
    return py::reinterpret_steal<py::object>(res_obj);
  });
  // convenience functions to create dataslices/bags/shapes
  m.def("make_empty_ds",
        []() { return arolla::TypedValue::FromValue(DataSlice()); });

  m.def("make_int_ds", []() {
    auto ds_impl = internal::DataSliceImpl::Create(
        arolla::CreateFullDenseArray<int>({1, 2, 3}));
    auto ds = DataSlice::Create(ds_impl, JaggedShape::FlatFromSize(3),
                                DataItem(schema::kInt32));
    return arolla::TypedValue::FromValue(std::move(*ds));
  });

  m.def("make_text_item_ds", []() {
    auto ds = DataSlice::Create(DataItem(arolla::Text("abc")),
                                DataItem(schema::kString));
    return arolla::TypedValue::FromValue(std::move(*ds));
  });

  m.def("make_shape", []() -> arolla::TypedValue {
    auto typed_value = arolla::TypedValue::FromValueWithQType(
        JaggedShape::FlatFromSize(3), GetJaggedShapeQType());
    if (!typed_value.ok()) {
      throw pybind11::error_already_set();
    }
    return *std::move(typed_value);
  });

  m.def("make_data_bag", []() {
    DataBagPtr db = DataBag::EmptyMutable();
    return arolla::TypedValue::FromValue(std::move(db));
  });
}
}  // namespace
}  // namespace koladata::python
