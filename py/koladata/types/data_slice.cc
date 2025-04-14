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
#include "py/koladata/types/data_slice.h"

#include <Python.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/slices.h"
#include "koladata/proto/to_proto.h"
#include "koladata/repr_utils.h"
#include "koladata/uuid_utils.h"
#include "google/protobuf/message.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/boxing.h"
#include "py/koladata/base/py_conversions/to_py.h"
#include "py/koladata/base/py_utils.h"
#include "py/koladata/base/to_py_object.h"
#include "py/koladata/base/wrap_utils.h"
#include "py/koladata/fstring/fstring_processor.h"
#include "py/koladata/types/pybind11_protobuf_wrapper.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

// If `db` is not nullptr, adopt collected DataBags into it. This utility is
// useful, in order to rely on error reporting from lower-level utilities that
// do not work on DataSlices without DataBags.
absl::Status TryAdoptInto(const AdoptionQueue& adoption_queue,
                          absl::Nullable<const DataBagPtr>& db) {
  if (db == nullptr) {
    return absl::OkStatus();
  }
  return adoption_queue.AdoptInto(*db);
}

absl::Nullable<PyObject*> PyDataSlice_get_bag(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  auto db = UnsafeDataSliceRef(self).GetBag();
  if (db == nullptr) {
    Py_RETURN_NONE;
  }
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(db));
}

absl::Nullable<PyObject*> PyDataSlice_with_bag(PyObject* self, PyObject* db) {
  arolla::python::DCheckPyGIL();
  if (db == Py_None) {
    return WrapPyDataSlice(UnsafeDataSliceRef(self).WithBag(nullptr));
  }
  std::optional<DataBagPtr> db_ptr = UnwrapDataBagPtr(db, "db");
  if (!db_ptr) {
    return nullptr;
  }
  return WrapPyDataSlice(UnsafeDataSliceRef(self).WithBag(*std::move(db_ptr)));
}

absl::Nullable<PyObject*> PyDataSlice_no_bag(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  return WrapPyDataSlice(UnsafeDataSliceRef(self).WithBag(nullptr));
}

// classmethod
absl::Nullable<PyObject*> PyDataSlice_from_vals(PyTypeObject* cls,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* list = py_args[0];
  std::optional<DataSlice> schema;
  if (!UnwrapDataSliceOptionalArg(args.pos_kw_values[0], "schema", schema)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto ds, DataSliceFromPyValueWithAdoption(list, schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(ds));
}

// classmethod
// Low-level interface for implementing `kd.from_py` behavior. See the docstring
// of `kd.from_py` for details.
absl::Nullable<PyObject*> PyDataSlice_from_py(PyTypeObject* cls,
                                              PyObject* const* py_args,
                                              Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 5) {
    PyErr_Format(PyExc_ValueError,
                 "DataSlice._from_py_impl accepts exactly 5 arguments, got %d",
                 nargs);
    return nullptr;
  }
  if (!PyBool_Check(py_args[1])) {
    PyErr_Format(PyExc_TypeError, "expecting dict_as_obj to be a bool, got %s",
                 Py_TYPE(py_args[1])->tp_name);
    return nullptr;
  }
  bool dict_as_obj = py_args[1] == Py_True;
  if (!PyLong_Check(py_args[4])) {
    PyErr_Format(PyExc_TypeError, "expecting from_dim to be an int, got %s",
                 Py_TYPE(py_args[4])->tp_name);
    return nullptr;
  }
  size_t from_dim = PyLong_AsSize_t(py_args[4]);
  if (PyErr_Occurred()) {
    return nullptr;
  }
  std::optional<DataSlice> itemid;
  std::optional<DataSlice> schema_arg;
  if (!UnwrapDataSliceOptionalArg(py_args[2], "itemid", itemid) ||
      !UnwrapDataSliceOptionalArg(py_args[3], "schema", schema_arg)) {
    return nullptr;
  }

  ASSIGN_OR_RETURN(auto res,
                   GenericFromPyObject(py_args[0], dict_as_obj, schema_arg,
                                       from_dim, itemid),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_internal_as_py(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  absl::StatusOr<arolla::python::PyObjectPtr> res = PyObjectFromDataSlice(ds);
  if (!res.ok()) {
    arolla::python::SetPyErrFromStatus(res.status());
    return nullptr;
  }
  return res->release();
}

absl::Nullable<PyObject*> PyDataSlice_internal_as_arolla_value(PyObject* self,
                                                               PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto value, DataSliceToArollaValue(ds),
                   arolla::python::SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(value);
}

absl::Nullable<PyObject*> PyDataSlice_internal_as_dense_array(PyObject* self,
                                                              PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto array, DataSliceToDenseArray(ds),
                   arolla::python::SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(array);
}

absl::Nullable<PyObject*> PyDataSlice_to_proto(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataSlice& slice = UnsafeDataSliceRef(self);
  const size_t num_messages = slice.present_count();
  if (slice.GetShape().rank() > 1) {
    PyErr_Format(PyExc_ValueError,
                 "to_proto expects a DataSlice with ndim 0 or 1, got ndim=%d",
                 slice.GetShape().rank());
    return nullptr;
  }

  if (nargs != 1) {
    PyErr_Format(PyExc_ValueError,
                 "to_proto accepts exactly 1 arguments, got %d", nargs);
    return nullptr;
  }
  if (!PyType_Check(py_args[0])) {
    PyErr_Format(PyExc_TypeError,
                 "to_proto expects message_class to be a proto class, got %s",
                 Py_TYPE(py_args[0])->tp_name);
    return nullptr;
  }

  // Construct an empty python message object using the python message class,
  // then convert it to a C++ message object, to get access to the New method.
  // This allows us to create additional mutable C++ message objects directly.
  PyObject* py_message_class = py_args[0];  // Borrowed.
  auto py_empty_message =
      arolla::python::PyObjectPtr::Own(PyObject_CallNoArgs(py_message_class));
  if (py_empty_message == nullptr) {
    return nullptr;
  }
  bool using_fast_cpp_proto = IsFastCppPyProtoMessage(py_empty_message.get());
  ASSIGN_OR_RETURN((auto [empty_message, empty_message_owner]),
                   UnwrapPyProtoMessage(py_empty_message.get()),
                   arolla::python::SetPyErrFromStatus(_));

  std::vector<std::unique_ptr<google::protobuf::Message>> messages;
  messages.reserve(num_messages);
  std::vector<google::protobuf::Message*> message_ptrs;
  message_ptrs.reserve(num_messages);
  for (size_t i = 0; i < num_messages; ++i) {
    messages.emplace_back(empty_message->New());
    message_ptrs.push_back(messages.back().get());
  }

  ASSIGN_OR_RETURN(auto flat_slice,
                   slice.Reshape(slice.GetShape().FlatFromSize(slice.size())),
                   arolla::python::SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(auto mask, ops::Has(flat_slice),
                   arolla::python::SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(auto dense_flat_slice,
                   ops::Select(std::move(flat_slice), mask,
                               DataSlice::CreateFromScalar(false)),
                   arolla::python::SetPyErrFromStatus(_));

  RETURN_IF_ERROR(ToProto(std::move(dense_flat_slice), message_ptrs))
      .With(arolla::python::SetPyErrFromStatus);

  // If the input was a DataItem, return a single message (or None).
  if (slice.is_item()) {
    if (num_messages == 0) {
      return Py_None;
    } else {
      auto py_message = WrapProtoMessage(
          std::move(messages[0]), py_message_class, using_fast_cpp_proto);
      if (py_message == nullptr) {
        return nullptr;
      }
      return py_message.release();
    }
  }

  const auto& mask_impl = mask.impl<internal::DataSliceImpl>();
  const arolla::DenseArray<arolla::Unit>* mask_values =
      mask_impl.is_empty_and_unknown() ? nullptr
                                       : &mask_impl.values<arolla::Unit>();

  auto py_result_list =
      arolla::python::PyObjectPtr::Own(PyList_New(slice.size()));
  if (py_result_list == nullptr) {
    return nullptr;
  }
  Py_ssize_t i_message = 0;
  for (Py_ssize_t i = 0; i < slice.size(); ++i) {
    if (mask_values == nullptr || !mask_values->present(i)) {
      PyList_SET_ITEM(py_result_list.get(), i, Py_None);
      continue;
    }
    auto py_message = WrapProtoMessage(std::move(messages[i_message]),
                                       py_message_class, using_fast_cpp_proto);
    ++i_message;
    if (py_message == nullptr) {
      return nullptr;
    }
    PyList_SET_ITEM(py_result_list.get(), i, py_message.release());
  }
  DCHECK_EQ(i_message, num_messages);

  return py_result_list.release();
}

// NOTE: Used to optimize the GetAttr by calling GenericGetAttr only if it is a
// method.
absl::flat_hash_set<absl::string_view>&
PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore();

// Returns true if the given attr_name is be possible to access through
// `getattr(slice, my_attr)`. For example, it should not be an already reserved
// attr.
bool IsCompliantAttrName(absl::string_view attr_name) {
  return (attr_name.empty() || attr_name[0] != '_') &&
         !PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore().contains(
             attr_name);
}

absl::Nullable<PyObject*> PyDataSlice_getattro(PyObject* self,
                                               PyObject* attr_name) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(attr_name, &size);
  if (attr_name_ptr == nullptr) {
    return nullptr;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  // NOTE: DataBag attributes starting with '_' or that share the same name as
  // some of the methods/reserved attrs can still be fetched by using a method
  // <DataSlice>.get_attr.
  if (!IsCompliantAttrName(attr_name_view)) {
    // Calling GenericGetAttr conditionally for performance reasons, as that
    // function covers a lot of Python's core functionality (fetching method
    // names, which is a lookup into __class__ and its base classes, etc.)
    return PyObject_GenericGetAttr(self, attr_name);
  }
  DataSlice self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.GetAttr(attr_name_view),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_get_attr(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "default");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(py_args[0], &size);
  if (attr_name_ptr == nullptr) {
    return nullptr;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  const auto& self_ds = UnsafeDataSliceRef(self);
  std::optional<DataSlice> res;
  if (args.pos_kw_values[0] == nullptr) {
    ASSIGN_OR_RETURN(res, self_ds.GetAttr(attr_name_view),
                     arolla::python::SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(auto default_value,
                     DataSliceFromPyValueNoAdoption(args.pos_kw_values[0]),
                     arolla::python::SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(res,
                     self_ds.GetAttrWithDefault(attr_name_view, default_value),
                     arolla::python::SetPyErrFromStatus(_));
  }
  return WrapPyDataSlice(*std::move(res));
}

int PyDataSlice_setattro(PyObject* self, PyObject* attr_name, PyObject* value) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(attr_name, &size);
  if (attr_name_ptr == nullptr) {
    return -1;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  // NOTE: DataBag attributes starting with '_' or that share the same name as
  // some of the methods/reserved attrs can still be set by using a method
  // <DataSlice>.set_attr.
  if (!IsCompliantAttrName(attr_name_view)) {
    return PyObject_GenericSetAttr(self, attr_name, value);
  }
  const auto& self_ds = UnsafeDataSliceRef(self);
  if (value == nullptr) {
    if (auto status = self_ds.DelAttr(attr_name_view); !status.ok()) {
      arolla::python::SetPyErrFromStatus(status);
      return -1;
    }
    return 0;
  }
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(auto value_ds,
                   AssignmentRhsFromPyValue(self_ds, value, adoption_queue),
                   (arolla::python::SetPyErrFromStatus(_), -1));
  auto status = self_ds.SetAttr(attr_name_view, value_ds);
  if (status.ok()) {
    status = adoption_queue.AdoptInto(*self_ds.GetBag());
  }
  if (!status.ok()) {
    arolla::python::SetPyErrFromStatus(KodaErrorCausedByIncompableSchemaError(
        std::move(status), self_ds.GetBag(), value_ds.GetBag(), self_ds));
    return -1;
  }
  return 0;
}

absl::Nullable<PyObject*> PyDataSlice_set_attr(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false, "overwrite_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(py_args[0], &size);
  if (attr_name_ptr == nullptr) {
    return nullptr;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  const auto& self_ds = UnsafeDataSliceRef(self);
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(
      auto value_ds,
      AssignmentRhsFromPyValue(self_ds, py_args[1], adoption_queue),
      arolla::python::SetPyErrFromStatus(_));
  bool overwrite_schema = false;
  if (PyObject* py_overwrite_schema = args.pos_kw_values[0];
      py_overwrite_schema != nullptr) {
    if (!PyBool_Check(py_overwrite_schema)) {
      PyErr_Format(PyExc_TypeError,
                   "expected bool for `overwrite_schema`, got: %s",
                   Py_TYPE(py_overwrite_schema)->tp_name);
      return nullptr;
    }
    overwrite_schema = PyObject_IsTrue(py_overwrite_schema);
  }
  RETURN_IF_ERROR(self_ds.SetAttr(attr_name_view, value_ds, overwrite_schema))
      .With([&](absl::Status status) {
        return arolla::python::SetPyErrFromStatus(
            KodaErrorCausedByIncompableSchemaError(std::move(status),
                                                   self_ds.GetBag(),
                                                   value_ds.GetBag(), self_ds));
      });
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetBag()))
      .With(arolla::python::SetPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_set_attrs(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor parser(
      FastcallArgParser(/*pos_only_n=*/0, /*parse_kwargs=*/true,
                        /*kw_only_arg_names=*/{"overwrite_schema"}));

  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  bool overwrite_schema = false;
  if (!ParseBoolArg(args, "overwrite_schema", overwrite_schema)) {
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(self_ds.GetBag(), args.kw_values, adoption_queue),
      arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(self_ds.SetAttrs(args.kw_names, values, overwrite_schema))
      .With([&](absl::Status status) {
        // TODO: b/361573497 - Move both adoption and error handling to SetAttr.
        return arolla::python::SetPyErrFromStatus(
            KodaErrorCausedByIncompableSchemaError(
                std::move(status), self_ds.GetBag(), values, self_ds));
      });
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetBag()))
      .With(arolla::python::SetPyErrFromStatus);
  Py_RETURN_NONE;
}

// Converts `key` into a DataItem if `key` is a supported Python scalar or
// returns `key` as a DataSlice if it already is a DataSlice. Otherwise, returns
// an Error.
absl::StatusOr<DataSlice> ConvertKeyToDataSlice(PyObject* key) {
  ASSIGN_OR_RETURN(DataSlice key_ds, DataSliceFromPyValueNoAdoption(key));
  if (key_ds.GetShape().rank() > 0 &&
      !PyType_IsSubtype(Py_TYPE(key), PyDataSlice_Type())) {
    return absl::InvalidArgumentError(
        "passing a Python list/tuple to a Koda operation is ambiguous; "
        "please use kd.slice(...) to create a slice or a multi-dimensional "
        "slice, and kd.list(...) to create a single Koda list.");
  }
  return key_ds;
}

absl::Nullable<PyObject*> PyDataSlice_subscript(PyObject* self, PyObject* key) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  if (key && PySlice_Check(key)) {
    Py_ssize_t start, stop, step;
    if (PySlice_Unpack(key, &start, &stop, &step) != 0) {
      return nullptr;
    }
    if (step != 1) {
      PyErr_SetString(PyExc_ValueError,
                      "Slice with step != 1 is not supported");
      return nullptr;
    }
    std::optional<int64_t> stop_or_end =
        stop == PY_SSIZE_T_MAX ? std::optional<int64_t>(std::nullopt)
                               : std::optional<int64_t>(stop);
    if (self_ds.ShouldApplyListOp()) {
      ASSIGN_OR_RETURN(auto res, self_ds.ExplodeList(start, stop_or_end),
                       arolla::python::SetPyErrFromStatus(_));
      return WrapPyDataSlice(std::move(res));
    } else {
      if (start != 0 || stop_or_end.has_value()) {
        PyErr_SetString(PyExc_ValueError,
                        "slice with start or stop is not supported for "
                        "dictionaries");
        return nullptr;
      }
      ASSIGN_OR_RETURN(auto res, self_ds.GetDictValues(),
                       arolla::python::SetPyErrFromStatus(_));
      return WrapPyDataSlice(std::move(res));
    }
  }
  ASSIGN_OR_RETURN(auto key_ds, ConvertKeyToDataSlice(key),
                   arolla::python::SetPyErrFromStatus(_));
  ASSIGN_OR_RETURN(auto res, self_ds.GetItem(key_ds),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

int PyDataSlice_ass_subscript(PyObject* self, PyObject* key, PyObject* value) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  std::optional<DataSlice> value_ds;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  AdoptionQueue adoption_queue;
  if (value) {
    ASSIGN_OR_RETURN(value_ds,
                     AssignmentRhsFromPyValue(self_ds, value, adoption_queue),
                     (arolla::python::SetPyErrFromStatus(_), -1));
    RETURN_IF_ERROR(TryAdoptInto(adoption_queue, self_ds.GetBag()))
        .With([&](const absl::Status& status) {
          arolla::python::SetPyErrFromStatus(status);
          return -1;
        });
    value_ds = std::move(value_ds)->WithBag(self_ds.GetBag());
  }
  absl::Status status;
  if (key && PySlice_Check(key)) {
    Py_ssize_t start, stop, step;
    if (PySlice_Unpack(key, &start, &stop, &step) != 0) {
      return -1;
    }
    if (step != 1) {
      PyErr_SetString(PyExc_ValueError,
                      "slices with step != 1 are not supported");
      return -1;
    }
    std::optional<int64_t> stop_or_end =
        stop == PY_SSIZE_T_MAX ? std::optional<int64_t>(std::nullopt)
                               : std::optional<int64_t>(stop);
    if (value_ds.has_value()) {
      status = self_ds.ReplaceInList(start, stop_or_end, *value_ds);
    } else {
      status = self_ds.RemoveInList(start, stop_or_end);
    }
  } else {
    // NOTE: In case of Dicts, key.GetBag(), if key is a DataSlice, gets adopted
    // inside SetInDict. No adoption needed in case of Lists.
    ASSIGN_OR_RETURN(DataSlice key_ds, ConvertKeyToDataSlice(key),
                     (arolla::python::SetPyErrFromStatus(_), -1));
    if (self_ds.ShouldApplyListOp()) {
      if (value_ds.has_value()) {
        status = self_ds.SetInList(key_ds, *value_ds);
      } else {
        status = self_ds.RemoveInList(key_ds);
      }
    } else {
      if (!value_ds.has_value()) {
        ASSIGN_OR_RETURN(value_ds,
                         DataSlice::Create(internal::DataItem(),
                                           internal::DataItem(schema::kNone)),
                         (arolla::python::SetPyErrFromStatus(_), -1));
      }
      status = self_ds.SetInDict(key_ds, *value_ds);
    }
  }
  if (!status.ok()) {
    arolla::python::SetPyErrFromStatus(status);
    return -1;
  }
  return 0;
}

PyObject* PyDataSlice_str_with_options(PyObject* self,
                                       const ReprOption& option) {
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  std::string result;
  absl::StatusOr<std::string> item_str = DataSliceToStr(self_ds, option);
  if (item_str.ok()) {
    result = item_str.value();
  } else {
    self_ds.VisitImpl(
        [&](const auto& impl) { return absl::StrAppend(&result, impl); });
  }

  return PyUnicode_FromStringAndSize(result.c_str(), result.size());
}

PyObject* PyDataSlice_str(PyObject* self) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  return PyDataSlice_str_with_options(
      self, ReprOption{.strip_quotes = true, .show_attributes = true});
}

PyObject* PyDataSlice_repr_with_params(PyObject* self, PyObject* const* py_args,
                                       Py_ssize_t nargs, PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, /*kw_only_arg_names=*/
      {"depth", "item_limit", "unbounded_type_max_len", "format_html"}));

  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  Py_ssize_t depth = 1;
  PyObject* py_depth = args.kw_only_args["depth"];
  if (py_depth != nullptr) {
    if (!PyLong_Check(py_depth)) {
      PyErr_SetString(PyExc_TypeError, "depth must be an integer");
      return nullptr;
    }
    depth = PyLong_AsSsize_t(py_depth);
  }

  Py_ssize_t item_limit = 20;
  PyObject* py_item_limit = args.kw_only_args["item_limit"];
  if (py_item_limit != nullptr) {
    if (!PyLong_Check(py_item_limit)) {
      PyErr_SetString(PyExc_TypeError, "item_limit must be an integer");
      return nullptr;
    }
    item_limit = PyLong_AsSsize_t(py_item_limit);
  }

  int32_t unbounded_type_max_len = -1;
  PyObject* py_unbounded_type_max_len =
      args.kw_only_args["unbounded_type_max_len"];
  if (py_unbounded_type_max_len != nullptr) {
    if (!PyLong_Check(py_unbounded_type_max_len)) {
      PyErr_SetString(PyExc_TypeError,
                      "unbounded_type_max_len must be an integer");
      return nullptr;
    }
    unbounded_type_max_len = PyLong_AsLong(py_unbounded_type_max_len);
  }

  bool format_html = false;
  PyObject* py_format_html = args.kw_only_args["format_html"];
  if (py_format_html != nullptr) {
    if (!PyBool_Check(py_format_html)) {
      PyErr_SetString(PyExc_TypeError, "format_html must be a boolean");
      return nullptr;
    }
    format_html = PyObject_IsTrue(py_format_html);
  }

  return PyDataSlice_str_with_options(
      self, ReprOption{.depth = depth,
                       .item_limit = static_cast<size_t>(item_limit),
                       .strip_quotes = false,
                       .format_html = format_html,
                       .unbounded_type_max_len = unbounded_type_max_len});
}

absl::Nullable<PyObject*> PyDataSlice_get_keys(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  DataSlice self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.GetDictKeys(),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_append(PyObject* self,
                                             PyObject* const* args,
                                             Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs != 1) {
    PyErr_SetString(
        PyExc_ValueError,
        "DataSlice.append accepts exactly 1 argument: the value to add");
    return nullptr;
  }
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(auto items,
                   AssignmentRhsFromPyValue(self_ds, args[0], adoption_queue),
                   arolla::python::SetPyErrFromStatus(_));
  RETURN_IF_ERROR(TryAdoptInto(adoption_queue, self_ds.GetBag()))
      .With(arolla::python::SetPyErrFromStatus);
  RETURN_IF_ERROR(self_ds.AppendToList(items.WithBag(self_ds.GetBag())))
      .With([&](absl::Status status) {
        return arolla::python::SetPyErrFromStatus(
            KodaErrorCausedByIncompableSchemaError(
                std::move(status), self_ds.GetBag(), items.GetBag(), self_ds));
      });
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_pop(PyObject* self, PyObject* const* args,
                                          Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  if (nargs > 1) {
    PyErr_SetString(PyExc_ValueError,
                    "DataSlice.pop accepts either 0 or 1 argument: list index");
    return nullptr;
  }
  DataSlice res;
  DataSlice self_ds = UnsafeDataSliceRef(self);
  if (nargs == 1) {
    ASSIGN_OR_RETURN(auto index, DataSliceFromPyValueNoAdoption(args[0]),
                     arolla::python::SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(res, self_ds.PopFromList(index),
                     arolla::python::SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(res, self_ds.PopFromList(),
                     arolla::python::SetPyErrFromStatus(_));
  }
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_clear(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  RETURN_IF_ERROR(UnsafeDataSliceRef(self).ClearDictOrList())
      .With(arolla::python::SetPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_get_shape(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyJaggedShape(ds.GetShape());
}

absl::Nullable<PyObject*> PyDataSlice_get_schema(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(ds.GetSchema());
}

DataSlice AsMask(bool b) {
  return *DataSlice::Create(
      b ? internal::DataItem(arolla::kUnit) : internal::DataItem(),
      internal::DataItem(schema::kMask));
}

absl::Nullable<PyObject*> PyDataSlice_is_list_schema(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsListSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_entity_schema(PyObject* self,
                                                       PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsEntitySchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_dict_schema(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsDictSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_dict(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsDict()));
}

absl::Nullable<PyObject*> PyDataSlice_is_list(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsList()));
}

absl::Nullable<PyObject*> PyDataSlice_is_entity(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsEntity()));
}

absl::Nullable<PyObject*> PyDataSlice_is_primitive_schema(PyObject* self,
                                                          PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsPrimitiveSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_internal_is_itemid_schema(PyObject* self,
                                                                PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsItemIdSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_empty(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsEmpty()));
}

absl::Nullable<PyObject*> PyDataSlice_is_mutable(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  const auto& db = ds.GetBag();
  return WrapPyDataSlice(AsMask(db != nullptr && db->IsMutable()));
}

absl::Nullable<PyObject*> PyDataSlice_freeze_bag(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(ds.FreezeBag());
}

absl::Nullable<PyObject*> PyDataSlice_with_schema(PyObject* self,
                                                  PyObject* schema) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  auto schema_ds = UnwrapDataSlice(schema, "schema");
  if (schema_ds == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto res, ds.WithSchema(*schema_ds),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_set_schema(PyObject* self,
                                                 PyObject* schema) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  const auto& ds = UnsafeDataSliceRef(self);
  auto schema_ds = UnwrapDataSlice(schema, "schema");
  if (schema_ds == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto res, ds.SetSchema(*schema_ds),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_embed_schema(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  auto& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.EmbedSchema(),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_get_attr_names(PyObject* self,
                                                     PyObject* const* py_args,
                                                     Py_ssize_t nargs,
                                                     PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  static const absl::NoDestructor parser(FastcallArgParser(
      /*pos_only_n=*/0, /*parse_kwargs=*/false, {"intersection"}));
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* py_intersection = args.kw_only_args["intersection"];
  if (py_intersection == nullptr) {
    PyErr_SetString(PyExc_TypeError,
                    "get_attr_names() missing 1 required keyword-only "
                    "argument: 'intersection'");
    return nullptr;
  }
  if (!PyBool_Check(py_intersection)) {
    PyErr_Format(PyExc_TypeError,
                 "get_attr_names() expected bool for `intersection`, got: %s",
                 Py_TYPE(py_intersection)->tp_name);
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto attr_names,
                   UnsafeDataSliceRef(self).GetAttrNames(
                       /*union_object_attrs=*/PyObject_Not(py_intersection)),
                   arolla::python::SetPyErrFromStatus(_));
  auto attr_name_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/attr_names.size()));
  int i = 0;
  for (const auto& attr_name : attr_names) {
    PyObject* py_attr_name =
        PyUnicode_DecodeUTF8(attr_name.data(), attr_name.size(), nullptr);
    if (py_attr_name == nullptr) {
      return nullptr;
    }
    PyList_SetItem(attr_name_list.get(), i++, py_attr_name);
  }
  return attr_name_list.release();
}

absl::Nullable<PyObject*> PyDataSlice_format(PyObject* self, PyObject* arg) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  Py_ssize_t size;
  const char* format_spec = PyUnicode_AsUTF8AndSize(arg, &size);
  if (format_spec == nullptr) {
    return nullptr;
  }
  if (size == 0) {
    return PyDataSlice_str(self);
  }
  ASSIGN_OR_RETURN(
      auto placeholder,
      fstring::ToDataSlicePlaceholder(UnsafeDataSliceRef(self),
                                      absl::string_view(format_spec, size)),
      arolla::python::SetPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(
      placeholder.c_str(), static_cast<Py_ssize_t>(placeholder.size()));
}

absl::Nullable<PyObject*> PyDataSlice_unspecified(PyTypeObject*, PyObject*) {
  auto unspecified = UnspecifiedDataSlice();
  return WrapPyDataSlice(std::move(unspecified));
}

absl::Nullable<PyObject*> PyDataSlice_debug_repr(PyObject* self) {
  arolla::python::DCheckPyGIL();
  arolla::python::PyCancellationScope cancellation_scope;
  std::string debug_repr = DataSliceRepr(UnsafeDataSliceRef(self));
  return PyUnicode_FromStringAndSize(
      debug_repr.c_str(), static_cast<Py_ssize_t>(debug_repr.size()));
}

PyMethodDef kPyDataSlice_methods[] = {
    {"get_bag", PyDataSlice_get_bag, METH_NOARGS,
     "get_bag()\n"
     "--\n\n"
     "Returns the attached DataBag."},
    {"with_bag", PyDataSlice_with_bag, METH_O,
     "with_bag(bag, /)\n"
     "--\n\n"
     "Returns a copy of DataSlice with DataBag `db`."},
    {"no_bag", PyDataSlice_no_bag, METH_NOARGS,
     "no_bag()\n"
     "--\n\n"
     "Returns a copy of DataSlice without DataBag."},
    {"from_vals", (PyCFunction)PyDataSlice_from_vals,
     METH_CLASS | METH_FASTCALL | METH_KEYWORDS,
     "from_vals(x, /, schema=None)\n"
     "--\n\n"
     R"""(Returns a DataSlice created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`.

Args:
  x: a Python value or a DataSlice. If it is a (nested) Python list or tuple,
    a multidimensional DataSlice is created.
  schema: schema DataItem to set. If `x` is already a DataSlice, this will
    cast it to the given schema.
)"""},
    {"_from_py_impl", (PyCFunction)PyDataSlice_from_py,
     METH_CLASS | METH_FASTCALL | METH_KEYWORDS,
     "_from_py_impl(py_obj, /, itemid=None, schema=None, from_dim=None)\n"
     "--\n\n"
     "Creates a complex Koda object by parsing recursive `py_obj` structure."},
    {"_unspecified", (PyCFunction)PyDataSlice_unspecified,
     METH_CLASS | METH_NOARGS,
     "_unspecified()\n"
     "--\n\n"
     "Returns an UNSPECIFIED with DataSlice QType."},
    {"_to_py_impl", (PyCFunction)PyDataSlice_to_py, METH_FASTCALL,
     "_to_py_impl(ds, /, max_depth=-1, obj_as_dict=False, "
     "include_missing_attrs=True)\n"
     "--\n\n"
     "Returns a Python object equivalent to this DataSlice.\n"},
    {"internal_as_py", PyDataSlice_internal_as_py, METH_NOARGS,
     "internal_as_py()\n"
     "--\n\n"
     "Returns a Python object equivalent to this DataSlice.\n"
     "\n"
     "If the values in this DataSlice represent objects, then the returned "
     "python\nstructure will contain DataItems.\n"},
    {"internal_as_arolla_value", PyDataSlice_internal_as_arolla_value,
     METH_NOARGS,
     "internal_as_arolla_value()\n"
     "--\n\n"
     "Converts primitive DataSlice / DataItem into an equivalent Arolla "
     "value."},
    {"internal_as_dense_array", PyDataSlice_internal_as_dense_array,
     METH_NOARGS,
     "internal_as_dense_array()\n"
     "--\n\n"
     "Converts primitive DataSlice to an Arolla DenseArray with appropriate "
     "qtype."},
    {"_to_proto", (PyCFunction)PyDataSlice_to_proto, METH_FASTCALL,
     "to_proto(message_class)\n"
     "--\n\n"
     "Converts this DataSlice to a proto message or list of proto messages."},
    {"get_shape", PyDataSlice_get_shape, METH_NOARGS,
     "get_shape()\n"
     "--\n\n"
     "Returns the shape of the DataSlice."},
    {"get_schema", PyDataSlice_get_schema, METH_NOARGS,
     "get_schema()\n"
     "--\n\n"
     "Returns a schema DataItem with type information about this DataSlice."},
    {"is_list_schema", PyDataSlice_is_list_schema, METH_NOARGS,
     "is_list_schema()\n"
     "--\n\n"
     "Returns present iff this DataSlice is a List Schema."},
    {"is_entity_schema", PyDataSlice_is_entity_schema, METH_NOARGS,
     "is_entity_schema()\n"
     "--\n\n"
     "Returns present iff this DataSlice represents an Entity Schema."},
    {"is_dict", PyDataSlice_is_dict, METH_NOARGS,
     "is_dict()\n"
     "--\n\n"
     "Returns present iff this DataSlice has Dict schema or contains only "
     "dicts."},
    {"is_list", PyDataSlice_is_list, METH_NOARGS,
     "is_list()\n"
     "--\n\n"
     "Returns present iff this DataSlice has List schema or contains only "
     "lists."},
    {"is_entity", PyDataSlice_is_entity, METH_NOARGS,
     "is_entity()\n"
     "--\n\n"
     "Returns present iff this DataSlice has Entity schema or contains only "
     "entities."},
    {"is_dict_schema", PyDataSlice_is_dict_schema, METH_NOARGS,
     "is_dict_schema()\n"
     "--\n\n"
     "Returns present iff this DataSlice is a Dict Schema."},
    {"is_primitive_schema", PyDataSlice_is_primitive_schema, METH_NOARGS,
     "is_primitive_schema()\n"
     "--\n\n"
     "Returns present iff this DataSlice is a primitive (scalar) Schema."},
    {"internal_is_itemid_schema", PyDataSlice_internal_is_itemid_schema,
     METH_NOARGS,
     "internal_is_itemid_schema()\n"
     "--\n\n"
     "Returns present iff this DataSlice is ITEMID Schema."},
    {"is_empty", PyDataSlice_is_empty, METH_NOARGS,
     "is_empty()\n"
     "--\n\n"
     "Returns present iff this DataSlice is empty."},
    {"is_mutable", PyDataSlice_is_mutable, METH_NOARGS,
     "is_mutable()\n"
     "--\n\n"
     "Returns present iff the attached DataBag is mutable."},
    {"freeze_bag", PyDataSlice_freeze_bag, METH_NOARGS,
     "freeze_bag()\n"
     "--\n\n"
     "Returns a frozen DataSlice equivalent to `self`."},
    {"with_schema", PyDataSlice_with_schema, METH_O,
     "with_schema(schema, /)\n"
     "--\n\n"
     R"""(Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
)"""},
    {"set_schema", PyDataSlice_set_schema, METH_O,
     "set_schema(schema, /)\n"
     "--\n\n"
     R"""(Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
)"""},
    {"get_keys", PyDataSlice_get_keys, METH_NOARGS,
     "get_keys()\n"
     "--\n\n"
     "Returns keys of all dicts in this DataSlice."},
    {"get_attr", (PyCFunction)PyDataSlice_get_attr,
     METH_FASTCALL | METH_KEYWORDS,
     "get_attr(attr_name, /, default=None)\n"
     "--\n\n"
     "Gets attribute `attr_name` where missing items are filled from "
     "`default`.\n\n"
     "Args:\n"
     "  attr_name: name of the attribute to get.\n"
     "  default: optional default value to fill missing items.\n"
     "           Note that this value can be fully omitted."},
    // TODO: Add proper docstring when the rest of functionality in
    // terms of dicts and lists is done.
    {"set_attr", (PyCFunction)PyDataSlice_set_attr,
     METH_FASTCALL | METH_KEYWORDS,
     "set_attr(attr_name, value, /, overwrite_schema=False)\n"
     "--\n\n"
     "Sets an attribute `attr_name` to `value`."},
    {"set_attrs", (PyCFunction)PyDataSlice_set_attrs,
     METH_FASTCALL | METH_KEYWORDS,
     "set_attrs(*, overwrite_schema=False, **attrs)\n"
     "--\n\n"
     R"""(Sets multiple attributes on an object / entity.

Args:
  overwrite_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.
)"""},
    {"append", (PyCFunction)PyDataSlice_append, METH_FASTCALL,
     "append(value, /)\n"
     "--\n\n"
     "Append a value to each list in this DataSlice"},
    {"_internal_pop", (PyCFunction)PyDataSlice_pop, METH_FASTCALL,
     "Pop a value from each list in this DataSlice"},
    {"clear", PyDataSlice_clear, METH_NOARGS,
     "clear()\n"
     "--\n\n"
     "Clears all dicts or lists in this DataSlice"},
    {"embed_schema", PyDataSlice_embed_schema, METH_NOARGS,
     "embed_schema()\n"
     "--\n\n"
     R"""(Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as '__schema__' attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.
)"""},
    {"get_attr_names", (PyCFunction)PyDataSlice_get_attr_names,
     METH_FASTCALL | METH_KEYWORDS,
     "get_attr_names(*, intersection)\n"
     "--\n\n"
     R"""(Returns a sorted list of unique attribute names of this DataSlice.

In case of OBJECT schema, attribute names are fetched from the `__schema__`
attribute. In case of Entity schema, the attribute names are fetched from the
schema. In case of primitives, an empty list is returned.

Args:
  intersection: If True, the intersection of all object attributes is returned.
    Otherwise, the union is returned.

Returns:
  A list of unique attributes sorted by alphabetical order.
)"""},
    {"__format__", (PyCFunction)PyDataSlice_format, METH_O,
     "Returns a format representation with a special support for non empty "
     "specification.\n\nDataSlice will be replaced with base64 encoded "
     "DataSlice.\nMust be used with kd.fstr or kde.fstr."},
    {"_repr_with_params", (PyCFunction)PyDataSlice_repr_with_params,
     METH_FASTCALL | METH_KEYWORDS,
     "_repr_with_params("
     "*, depth=1, item_limit=20, unbounded_type_max_len=-1, "
     "format_html=False)\n"
     "--\n\n"
     "Used to generate str representation for interactive repr in Colab."},
    {"_debug_repr", (PyCFunction)PyDataSlice_debug_repr, METH_NOARGS,
     "_debug_repr()\n"
     "--\n\n"
     "Returns a string representation of the DataSlice for debugging "
     "purposes."},
    {nullptr}, /* sentinel */
};

// NOTE: Used to optimize the GetAttr by calling GenericGetAttr only if it is a
// method.
absl::flat_hash_set<absl::string_view>&
PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore() {
  static absl::NoDestructor<absl::flat_hash_set<absl::string_view>> methods(
      []() {
        absl::flat_hash_set<absl::string_view> res;
        PyTypeObject* type = PyDataSlice_Type();
        while (type != nullptr) {
          const PyMethodDef* method = type->tp_methods;
          for (; method->ml_name != nullptr; ++method) {
            if (method->ml_name[0] != '_') {
              res.emplace(method->ml_name);
            }
          }
          const PyGetSetDef* getter = type->tp_getset;
          if (getter != nullptr) {
            for (; getter->name != nullptr; ++getter) {
              if (getter->name[0] != '_') {
                res.emplace(getter->name);
              }
            }
          }
          type = type->tp_base;
        }
        return res;
      }());
  return *methods;
}

PyObject* PyDataSlice_richcompare_not_implemented(PyObject* self,
                                                  PyObject* other, int op) {
  // NOTE: Python documentation recommends returning NotImplemented, instead of
  // raising an Error, because then Python runtime tries to find other type's
  // richcompare method to compare it with. Given that we overwrite these magic
  // methods in Python and just erase QValue's (base class) richcompare method,
  // raising TypeError here to prevent Python's interpretation of
  // NotImplemented, is justified.
  PyErr_SetString(PyExc_TypeError,
                  "RichCompare methods are overwritten in Python");
  return nullptr;
}

PyGetSetDef kPyDataSlice_getset[] = {
    {nullptr}, /* sentinel */
};

// Creates and initializes PyTypeObject for Python DataSlice class.
PyTypeObject* InitPyDataSliceType() {
  arolla::python::CheckPyGIL();
  PyTypeObject* py_qvalue_type = arolla::python::PyQValueType();
  if (py_qvalue_type == nullptr) {
    return nullptr;
  }
  PyType_Slot slots[] = {
      {Py_tp_base, py_qvalue_type},
      // NOTE: For now there is no need for alloc/dealloc, as everything is
      // handled by PyQValueObject's dealloc method.
      {Py_tp_getattro, (void*)PyDataSlice_getattro},
      {Py_tp_setattro, (void*)PyDataSlice_setattro},
      {Py_tp_methods, kPyDataSlice_methods},
      {Py_tp_getset, kPyDataSlice_getset},
      {Py_tp_hash, (void*)PyObject_HashNotImplemented},
      {Py_tp_richcompare, (void*)PyDataSlice_richcompare_not_implemented},
      {Py_tp_str, (void*)PyDataSlice_str},
      {Py_mp_subscript, (void*)PyDataSlice_subscript},
      {Py_mp_ass_subscript, (void*)PyDataSlice_ass_subscript},
      {0, nullptr},
  };

  PyType_Spec spec = {
      .name = "koladata.types.data_slice.DataSlice",
      .flags = (Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE),
      .slots = slots,
  };

  PyObject* qvalue_subtype = PyType_FromSpec(&spec);
  // NOTE: Registering the DataSlice base class specialization by QType, so that
  // we can have a fallback for subclasses in unit tests for which we do not
  // need to register all QValue specializations, but still require this not to
  // be just a QValue.
  if (!arolla::python::RegisterPyQValueSpecializationByQType(
          arolla::GetQType<DataSlice>(), qvalue_subtype)) {
    return nullptr;
  }
  return reinterpret_cast<PyTypeObject*>(qvalue_subtype);
}

}  // namespace

PyTypeObject* PyDataSlice_Type() {
  arolla::python::CheckPyGIL();
  static PyTypeObject* type = InitPyDataSliceType();
  return type;
}

// data_slice_py_ext module methods
absl::Nullable<PyObject*> PyDataSliceModule_is_compliant_attr_name(
    PyObject* /*module*/, PyObject* attr_name) {
  arolla::python::DCheckPyGIL();
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(attr_name, &size);
  if (attr_name_ptr == nullptr) {
    return nullptr;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  return PyBool_FromLong(IsCompliantAttrName(attr_name_view));
}

absl::Nullable<PyObject*> PyDataSliceModule_register_reserved_class_method_name(
    PyObject* /*module*/, PyObject* method_name) {
  arolla::python::DCheckPyGIL();
  if (!PyUnicode_Check(method_name)) {
    PyErr_SetString(PyExc_TypeError, "method name must be a string");
    return nullptr;
  }
  Py_ssize_t size;
  const char* method_name_ptr = PyUnicode_AsUTF8AndSize(method_name, &size);
  if (method_name_ptr == nullptr) {
    return nullptr;
  }
  auto method_name_view = absl::string_view(method_name_ptr, size);
  if (size == 0 || method_name_view[0] != '_') {
    PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore().insert(
        method_name_view);
  }
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*>
PyDataSliceModule_get_reserved_attrs(PyObject* /*module*/) {
  arolla::python::DCheckPyGIL();
  const absl::flat_hash_set<absl::string_view>& attrs =
      PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore();
  // We create a new FrozenSet, and are thus allowed to PySet_Add to it before
  // exposing it to other code.
  auto py_set = arolla::python::PyObjectPtr::Own(PyFrozenSet_New(nullptr));
  for (const auto& attr : attrs) {
    // We have to own the string in case of insertion failure. Unlike
    // PyList_New, PySet_New does not 'steal' ownership of the item reference.
    auto py_attr = arolla::python::PyObjectPtr::Own(
        PyUnicode_FromStringAndSize(attr.data(), attr.size()));
    if (PySet_Add(py_set.get(), py_attr.get()) == -1) {
      return nullptr;
    }
  }
  return py_set.release();
}

}  // namespace koladata::python
