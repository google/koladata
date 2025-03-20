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
#include "py/koladata/base/to_py_object.h"

#include <Python.h>

#include <cstdint>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "arolla/dense_array/edge.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using arolla::python::PyObjectPtr;

// The following functions Return a new reference to a Python object, equivalent
// to `value`.
PyObject* PyObjectFromValue(int value) { return PyLong_FromLongLong(value); }

PyObject* PyObjectFromValue(int64_t value) {
  return PyLong_FromLongLong(value);
}

PyObject* PyObjectFromValue(float value) { return PyFloat_FromDouble(value); }

PyObject* PyObjectFromValue(double value) { return PyFloat_FromDouble(value); }

PyObject* PyObjectFromValue(bool value) { return PyBool_FromLong(value); }

PyObject* PyObjectFromValue(arolla::Unit value) {
  auto ds_or = DataSlice::Create(internal::DataItem(value),
                                 internal::DataItem(schema::kMask));
  // NOTE: `schema` is already consistent with `value` as otherwise DataSlice
  // would not even be created.
  DCHECK_OK(ds_or);
  return WrapPyDataSlice(*std::move(ds_or));
}

PyObject* PyObjectFromValue(const arolla::Text& value) {
  absl::string_view text_view = value;
  return PyUnicode_DecodeUTF8(text_view.data(), text_view.size(), nullptr);
}

PyObject* PyObjectFromValue(const ::arolla::Bytes& value) {
  absl::string_view bytes_view = value;
  return PyBytes_FromStringAndSize(bytes_view.data(), bytes_view.size());
}

PyObject* PyObjectFromValue(const arolla::expr::ExprQuote& value) {
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(value));
}

// NOTE: Although DType is also a QValue, we don't want to expose it to user, as
// it is an internal type.
PyObject* PyObjectFromValue(schema::DType value) {
  auto ds_or = DataSlice::Create(internal::DataItem(value),
                                 internal::DataItem(schema::kSchema));
  // NOTE: `schema` is already consistent with `value` as otherwise DataSlice
  // would not even be created.
  DCHECK_OK(ds_or);
  return WrapPyDataSlice(*std::move(ds_or));
}

PyObject* PyObjectFromValue(const internal::MissingValue& value) {
  return Py_NewRef(Py_None);
}

ItemToPyConverter GetDataItemConverter(const DataSlice& ds) {
  return [schema = ds.GetSchemaImpl(),
          bag = ds.GetBag()](const internal::DataItem& item) {
    return PyObjectFromDataItem(item, schema, bag);
  };
}

}  // namespace

// Returns a new reference to a Python object, equivalent to the value stored in
// a `internal::DataItem`.
absl::StatusOr<PyObjectPtr> PyObjectFromDataItem(
    const internal::DataItem& item, const internal::DataItem& schema,
    const DataBagPtr& db) {
  return item.VisitValue([&](const auto& value) -> absl::StatusOr<PyObjectPtr> {
    using T = std::decay_t<decltype(value)>;
    PyObjectPtr res;
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      ASSIGN_OR_RETURN(
          DataSlice ds,
          DataSlice::Create(internal::DataItem(value), schema, db));
      // NOTE: `schema` is already consistent with `value` as otherwise
      // DataSlice would not even be created.
      res = PyObjectPtr::Own(WrapPyDataSlice(std::move(ds)));
    } else {
      res = PyObjectPtr::Own(PyObjectFromValue(value));
    }
    if (res == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInternal,
          absl::StrFormat("could not create a Python object from DataItem: %v",
                          item));
    }
    return res;
  });
}

absl::StatusOr<PyObjectPtr> PyObjectFromDataSlice(
    const DataSlice& ds, const ItemToPyConverter& optional_converter) {
  arolla::python::DCheckPyGIL();

  const ItemToPyConverter item_to_py_converter = optional_converter == nullptr
                                                     ? GetDataItemConverter(ds)
                                                     : optional_converter;

  if (ds.is_item()) {
    DCHECK_EQ(ds.size(), 1);  // Invariant ensured by DataSlice creation.
    return item_to_py_converter(ds.item());
  }
  // Starting from a flat list of PyObject* equivalent to DataItems.
  PyObjectPtr py_list = PyObjectPtr::Own(PyList_New(/*len=*/ds.size()));
  if (py_list == nullptr) {
    return arolla::python::StatusWithRawPyErr(
        absl::StatusCode::kInternal,
        absl::StrFormat("could not create a list of size %d", ds.size()));
  }
  const auto& ds_impl = ds.slice();
  for (int64_t i = 0; i < ds.size(); ++i) {
    ASSIGN_OR_RETURN(PyObjectPtr val, item_to_py_converter(ds_impl[i]));
    PyList_SET_ITEM(py_list.get(), i, val.release());
  }
  // Nesting the flat list by iterating through JaggedShape in reverse. The last
  // Edge in shape is ignored, because the result is already produced.
  const auto& edges = ds.GetShape().edges();
  for (int64_t edge_i = edges.size() - 1; edge_i > 0; --edge_i) {
    const auto& edge = edges[edge_i];
    DCHECK_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    PyObjectPtr new_list =
        PyObjectPtr::Own(PyList_New(/*len=*/edge.parent_size()));
    if (new_list == nullptr) {
      return arolla::python::StatusWithRawPyErr(
          absl::StatusCode::kInternal,
          absl::StrFormat("could not create a list of size %d",
                          edge.parent_size()));
    }
    int64_t offset = 0;
    absl::Status status;
    edge.edge_values()
        .Slice(1, edge.parent_size())
        .ForEach([&](int64_t id, bool present, int64_t pt) {
          if (!status.ok()) {
            return;
          }
          DCHECK(present);
          PyObject* sub_list = PyList_New(/*len=*/pt - offset);
          if (sub_list == nullptr) {
            status = arolla::python::StatusWithRawPyErr(
                absl::StatusCode::kInternal,
                absl::StrFormat("could not create a list of size %d",
                                pt - offset));
            return;
          }
          for (int64_t i = 0; i < pt - offset; ++i) {
            PyList_SET_ITEM(
                sub_list, i,
                Py_NewRef(PyList_GET_ITEM(py_list.get(), offset + i)));
          }
          PyList_SET_ITEM(new_list.get(), id, sub_list);
          offset = pt;
        });
    if (!status.ok()) {
      return status;
    }
    py_list = std::move(new_list);
  }
  return py_list;
}

}  // namespace koladata::python
