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

#include <cstdint>
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
#include "koladata/internal/dtype.h"
#include "koladata/uuid_utils.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "py/koladata/fstring/fstring_processor.h"
#include "py/koladata/types/boxing.h"
#include "py/koladata/types/py_utils.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

absl::Nullable<PyObject*> PyDataSlice_get_bag(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  auto db = UnsafeDataSliceRef(self).GetBag();
  if (db == nullptr) {
    Py_RETURN_NONE;
  }
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(db));
}

// TODO: Remove this alias.
absl::Nullable<PyObject*> PyDataSlice_db_getter(PyObject* self, void*) {
  return PyDataSlice_get_bag(self, nullptr);
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
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* list = py_args[0];
  const DataSlice* dtype = nullptr;
  if (PyObject* py_schema = args.pos_kw_values[0];
      py_schema != nullptr && py_schema != Py_None) {
    if (!PyType_IsSubtype(Py_TYPE(py_schema), PyDataSlice_Type())) {
      PyErr_Format(PyExc_TypeError, "expected DataItem for `schema`, got: %s",
                   Py_TYPE(py_schema)->tp_name);
      return nullptr;
    }
    dtype = &UnsafeDataSliceRef(py_schema);
  }
  ASSIGN_OR_RETURN(auto ds, DataSliceFromPyValueWithAdoption(list, dtype),
                   koladata::python::SetKodaPyErrFromStatus(_));
  if (ds.GetShape().rank() != 0 && cls != PyDataSlice_Type()) {
    PyErr_SetString(PyExc_TypeError,
                    "DataItem and other 0-rank class method `from_vals` "
                    "cannot create multi-dim slices");
    return nullptr;
  }
  return WrapPyDataSlice(std::move(ds));
}

absl::Nullable<PyObject*> PyDataSlice_internal_as_py(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return DataSliceToPyValue(ds);
}

absl::Nullable<PyObject*> PyDataSlice_as_arolla_value(PyObject* self,
                                                      PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto value, DataSliceToArollaValue(ds),
                   SetKodaPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(value);
}

absl::Nullable<PyObject*> PyDataSlice_as_dense_array(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto array, DataSliceToDenseArray(ds),
                   SetKodaPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(array);
}

// NOTE: Used to optimize the GetAttr by calling GenericGetAttr only if it is a
// method.
absl::flat_hash_set<absl::string_view>&
PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore();

absl::Nullable<PyObject*> PyDataSlice_getattro(PyObject* self,
                                               PyObject* attr_name) {
  arolla::python::DCheckPyGIL();
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(attr_name, &size);
  if (attr_name_ptr == nullptr) {
    return nullptr;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  // NOTE: DataBag attributes starting with '_' or that share the same name as
  // some of the methods/reserved attrs can still be fetched by using a method
  // <DataSlice>.get_attr.
  if ((size > 0 && attr_name_view[0] == '_') ||
      PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore().contains(
          attr_name_view)) {
    // Calling GenericGetAttr conditionally for performance reasons, as that
    // function covers a lot of Python's core functionality (fetching method
    // names, which is a lookup into __class__ and its base classes, etc.)
    return PyObject_GenericGetAttr(self, attr_name);
  }
  DataSlice self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(
      auto res, self_ds.GetAttr(attr_name_view),
      SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_get_attr(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
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
    ASSIGN_OR_RETURN(
        res, self_ds.GetAttr(attr_name_view),
        SetKodaPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(auto default_value,
                     DataSliceFromPyValueNoAdoption(args.pos_kw_values[0]),
                     SetKodaPyErrFromStatus(_));
    ASSIGN_OR_RETURN(
        res, self_ds.GetAttrWithDefault(attr_name_view, default_value),
        SetKodaPyErrFromStatus(_));
  }
  return WrapPyDataSlice(*std::move(res));
}

int PyDataSlice_setattro(PyObject* self, PyObject* attr_name, PyObject* value) {
  arolla::python::DCheckPyGIL();
  Py_ssize_t size;
  const char* attr_name_ptr = PyUnicode_AsUTF8AndSize(attr_name, &size);
  if (attr_name_ptr == nullptr) {
    return -1;
  }
  auto attr_name_view = absl::string_view(attr_name_ptr, size);
  // NOTE: DataBag attributes starting with '_' or that share the same name as
  // some of the methods/reserved attrs can still be set by using a method
  // <DataSlice>.set_attr.
  if ((size > 0 && attr_name_view[0] == '_') ||
      PyDataSlice_GetReservedAttrsWithoutLeadingUnderscore().contains(
          attr_name_view)) {
    return PyObject_GenericSetAttr(self, attr_name, value);
  }
  const auto& self_ds = UnsafeDataSliceRef(self);
  if (value == nullptr) {
    if (auto status = self_ds.DelAttr(attr_name_view); !status.ok()) {
      SetKodaPyErrFromStatus(status);
      return -1;
    }
    return 0;
  }
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(auto value_ds,
                   AssignmentRhsFromPyValue(self_ds, value, adoption_queue),
                   (SetKodaPyErrFromStatus(_), -1));
  auto status = self_ds.SetAttr(attr_name_view, value_ds);
  if (status.ok()) {
    status = adoption_queue.AdoptInto(*self_ds.GetBag());
  }
  if (!status.ok()) {
    SetKodaPyErrFromStatus(status);
    return -1;
  }
  return 0;
}

absl::Nullable<PyObject*> PyDataSlice_set_attr(PyObject* self,
                                               PyObject* const* py_args,
                                               Py_ssize_t nargs,
                                               PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false, "update_schema");
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
      SetKodaPyErrFromStatus(_));
  bool update_schema = false;
  if (PyObject* py_update_schema = args.pos_kw_values[0];
      py_update_schema != nullptr) {
    if (!PyBool_Check(py_update_schema)) {
      PyErr_Format(PyExc_TypeError,
                   "expected bool for `update_schema`, got: %s",
                   Py_TYPE(py_update_schema)->tp_name);
      return nullptr;
    }
    update_schema = PyObject_IsTrue(py_update_schema);
  }
  if (update_schema) {
    RETURN_IF_ERROR(self_ds.SetAttrWithUpdateSchema(attr_name_view, value_ds))
        .With(SetKodaPyErrFromStatus);
  } else {
    RETURN_IF_ERROR(self_ds.SetAttr(attr_name_view, value_ds))
        .With(SetKodaPyErrFromStatus);
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetBag()))
      .With(SetKodaPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_set_attrs(PyObject* self,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/0, /*parse_kwargs=*/true, "update_schema");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  bool update_schema = false;
  if (!ParseBoolArg(args, /*arg_pos=*/0, "update_schema", update_schema)) {
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(self_ds.GetBag(), args.kw_values, adoption_queue),
      SetKodaPyErrFromStatus(_));
  RETURN_IF_ERROR(self_ds.SetAttrs(args.kw_names, values, update_schema))
      .With(SetKodaPyErrFromStatus);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetBag()))
      .With(SetKodaPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_subscript(PyObject* self, PyObject* key) {
  arolla::python::DCheckPyGIL();
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
    ASSIGN_OR_RETURN(
        auto res, self_ds.ExplodeList(start, stop_or_end),
        SetKodaPyErrFromStatus(_));
    return WrapPyDataSlice(std::move(res));
  }
  ASSIGN_OR_RETURN(auto key_ds, DataSliceFromPyValueNoAdoption(key),
                   SetKodaPyErrFromStatus(_));
  ASSIGN_OR_RETURN(auto res, self_ds.GetItem(key_ds),
                   SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

int PyDataSlice_ass_subscript(PyObject* self, PyObject* key, PyObject* value) {
  arolla::python::DCheckPyGIL();
  std::optional<DataSlice> value_ds;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  AdoptionQueue adoption_queue;
  if (value) {
    ASSIGN_OR_RETURN(value_ds,
                     AssignmentRhsFromPyValue(self_ds, value, adoption_queue),
                     (SetKodaPyErrFromStatus(_), -1));
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
    ASSIGN_OR_RETURN(auto key_ds, DataSliceFromPyValue(key, adoption_queue),
                     (SetKodaPyErrFromStatus(_), -1));
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
                         (SetKodaPyErrFromStatus(_), -1));
      }
      status = self_ds.SetInDict(key_ds, *value_ds);
    }
  }
  if (status.ok()) {
    status = adoption_queue.AdoptInto(*self_ds.GetBag());
  }
  if (!status.ok()) {
    SetKodaPyErrFromStatus(status);
    return -1;
  }
  return 0;
}

PyObject* PyDataSlice_str(PyObject* self) {
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  std::string result;
  absl::StatusOr<std::string> item_str =
      DataSliceToStr(self_ds, ReprOption{.strip_quotes = true});
  if (item_str.ok()) {
    result = item_str.value();
  } else {
    self_ds.VisitImpl(
        [&](const auto& impl) { return absl::StrAppend(&result, impl); });
  }

  return PyUnicode_FromStringAndSize(result.c_str(), result.size());
}

absl::Nullable<PyObject*> PyDataSlice_get_keys(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  DataSlice self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(
      auto res, self_ds.GetDictKeys(),
      SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_get_values(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  DataSlice self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.GetDictValues(),
                   SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_append(PyObject* self,
                                             PyObject* const* args,
                                             Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs != 1) {
    PyErr_SetString(
        PyExc_ValueError,
        "DataSlice.append accepts exactly 1 argument: the value to add");
    return nullptr;
  }
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(auto ds,
                   AssignmentRhsFromPyValue(self_ds, args[0], adoption_queue),
                   SetKodaPyErrFromStatus(_));
  RETURN_IF_ERROR(self_ds.AppendToList(ds)).With(SetKodaPyErrFromStatus);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetBag()))
      .With(SetKodaPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_pop(PyObject* self, PyObject* const* args,
                                          Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  if (nargs > 1) {
    PyErr_SetString(PyExc_ValueError,
                    "DataSlice.pop accepts either 0 or 1 argument: list index");
    return nullptr;
  }
  DataSlice res;
  DataSlice self_ds = UnsafeDataSliceRef(self);
  if (nargs == 1) {
    ASSIGN_OR_RETURN(auto index, DataSliceFromPyValueNoAdoption(args[0]),
                     SetKodaPyErrFromStatus(_));
    ASSIGN_OR_RETURN(
        res, self_ds.PopFromList(index),
        SetKodaPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(
        res, self_ds.PopFromList(),
        SetKodaPyErrFromStatus(_));
  }
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_clear(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  RETURN_IF_ERROR(UnsafeDataSliceRef(self).ClearDictOrList())
      .With(SetKodaPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_get_shape(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyJaggedShape(ds.GetShape());
}

absl::Nullable<PyObject*> PyDataSlice_get_schema(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
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
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsListSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_entity_schema(PyObject* self,
                                                       PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsEntitySchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_dict_schema(PyObject* self,
                                                     PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsDictSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_dict(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.ContainsOnlyDicts()));
}

absl::Nullable<PyObject*> PyDataSlice_is_list(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.ContainsOnlyLists()));
}

absl::Nullable<PyObject*> PyDataSlice_is_primitive_schema(PyObject* self,
                                                          PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsPrimitiveSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_any_schema(PyObject* self,
                                                    PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsAnySchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_itemid_schema(PyObject* self,
                                                       PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsItemIdSchema()));
}

absl::Nullable<PyObject*> PyDataSlice_is_empty(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(AsMask(ds.IsEmpty()));
}

absl::Nullable<PyObject*> PyDataSlice_is_mutable(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  const auto& db = ds.GetBag();
  return WrapPyDataSlice(AsMask(db != nullptr && db->IsMutable()));
}

absl::Nullable<PyObject*> PyDataSlice_freeze(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto frozen_ds, ds.Freeze(), SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(frozen_ds));
}

absl::Nullable<PyObject*> PyDataSlice_with_schema(PyObject* self,
                                                  PyObject* schema) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  auto schema_ds = UnwrapDataSlice(schema, "schema");
  if (schema_ds == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto res, ds.WithSchema(*schema_ds),
                   SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_set_schema(PyObject* self,
                                                 PyObject* schema) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  auto schema_ds = UnwrapDataSlice(schema, "schema");
  if (schema_ds == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto res, ds.SetSchema(*schema_ds),
                   SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_as_any(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN(
      auto res,
      UnsafeDataSliceRef(self).WithSchema(internal::DataItem(schema::kAny)),
      SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_embed_schema(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  auto& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.EmbedSchema(), SetKodaPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_dir(PyObject* self, PyObject*) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN(auto attr_names, UnsafeDataSliceRef(self).GetAttrNames(),
                   SetKodaPyErrFromStatus(_));
  auto attr_name_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/attr_names.size()));
  int i = 0;
  for (const auto& attr_name : attr_names) {
    PyObject* py_attr_name = PyUnicode_DecodeUTF8(
        attr_name.data(), attr_name.size(), nullptr);
    if (py_attr_name == nullptr) {
      return nullptr;
    }
    PyList_SetItem(attr_name_list.get(), i++, py_attr_name);
  }
  return attr_name_list.release();
}

absl::Nullable<PyObject*> PyDataSlice_format(PyObject* self,
                                             PyObject* arg) {
  arolla::python::DCheckPyGIL();
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
      fstring::ToDataSlicePlaceholder(
          UnsafeDataSliceRef(self), absl::string_view(format_spec, size)),
      SetKodaPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(
      placeholder.c_str(), static_cast<Py_ssize_t>(placeholder.size()));
}

absl::Nullable<PyObject*> PyDataSlice_unspecified(PyTypeObject*, PyObject*) {
  auto unspecified = UnspecifiedDataSlice();
  return WrapPyDataSlice(std::move(unspecified));
}

// classmethod
absl::Nullable<PyObject*>
PyDataSlice_internal_register_reserved_class_method_name(
    PyTypeObject* cls, PyObject* method_name) {
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

PyMethodDef kPyDataSlice_methods[] = {
    {"get_bag", PyDataSlice_get_bag, METH_NOARGS,
     "Returns the attached DataBag."},
    {"with_bag", PyDataSlice_with_bag, METH_O,
     "Returns a copy of DataSlice with DataBag `db`."},
    {"no_bag", PyDataSlice_no_bag, METH_NOARGS,
     "Returns a copy of DataSlice without DataBag."},
    {"from_vals", (PyCFunction)PyDataSlice_from_vals,
     METH_CLASS | METH_FASTCALL | METH_KEYWORDS,
     "Creates a DataSlice from `value`.\n"
     "If `schema` is set, that schema is used,\n"
     "otherwise the schema is inferred from `value`."},
    {"_unspecified", (PyCFunction)PyDataSlice_unspecified,
     METH_CLASS | METH_NOARGS,
     "Returns an UNSPECIFIED with DataSlice QType."
     ""},
    {"internal_as_py", PyDataSlice_internal_as_py, METH_NOARGS,
     "Returns a Python object equivalent to this DataSlice.\n"
     "\n"
     "If the values in this slice represent objects, then the returned python\n"
     "structure will contain DataItems.\n"},
    {"as_arolla_value", PyDataSlice_as_arolla_value, METH_NOARGS,
     "Converts primitive slice / item into an equivalent Arolla value."},
    {"as_dense_array", PyDataSlice_as_dense_array, METH_NOARGS,
     "Converts primitive slice to an arolla.dense_array with appropriate "
     "qtype."},
    {"get_shape", PyDataSlice_get_shape, METH_NOARGS,
     "Returns the shape of the DataSlice."},
    {"get_schema", PyDataSlice_get_schema, METH_NOARGS,
     "Returns a schema slice with type information about this DataSlice."},
    {"is_list_schema", PyDataSlice_is_list_schema, METH_NOARGS,
     "Returns present iff this DataSlice is a List Schema."},
    {"is_entity_schema", PyDataSlice_is_entity_schema, METH_NOARGS,
     R"""(Returns present iff this DataSlice represents an Entity Schema.

Note that the Entity schema includes List and Dict schemas.

Returns:
  Present iff this DataSlice represents an Entity Schema.
     )"""},
    {"is_dict", PyDataSlice_is_dict, METH_NOARGS,
     "Returns present iff this DataSlice contains only dicts."},
    {"is_list", PyDataSlice_is_list, METH_NOARGS,
     "Returns present iff this DataSlice contains only lists."},
    {"is_dict_schema", PyDataSlice_is_dict_schema, METH_NOARGS,
     "Returns present iff this DataSlice is a Dict Schema."},
    {"is_primitive_schema", PyDataSlice_is_primitive_schema, METH_NOARGS,
     "Returns present iff this DataSlice is a primitive (scalar) Schema."},
    {"is_any_schema", PyDataSlice_is_any_schema, METH_NOARGS,
     "Returns present iff this DataSlice is ANY Schema."},
    {"is_itemid_schema", PyDataSlice_is_itemid_schema, METH_NOARGS,
     "Returns present iff this DataSlice is ITEMID Schema."},
    {"is_empty", PyDataSlice_is_empty, METH_NOARGS,
     "Returns present iff this DataSlice is empty."},
    {"is_mutable", PyDataSlice_is_mutable, METH_NOARGS,
     "Returns present iff the attached DataBag is mutable."},
    {"freeze", PyDataSlice_freeze, METH_NOARGS,
     "Returns a frozen DataSlice equivalent to `self`."},
    {"with_schema", PyDataSlice_with_schema, METH_O,
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
     R"""(Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
)"""},
    {"as_any", PyDataSlice_as_any, METH_NOARGS,
     "Returns a DataSlice with ANY schema."},
    {"get_keys", PyDataSlice_get_keys, METH_NOARGS,
     "Returns keys of all dicts in this DataSlice."},
    {"get_values", PyDataSlice_get_values, METH_NOARGS,
     "Returns values of all dicts in this DataSlice."},
    {"get_attr", (PyCFunction)PyDataSlice_get_attr,
     METH_FASTCALL | METH_KEYWORDS,
     "Gets attribute `attr_name` where missing items are filled from "
     "`default`."},
    // TODO: Add proper docstring when the rest of functionality in
    // terms of dicts and lists is done.
    {"set_attr", (PyCFunction)PyDataSlice_set_attr,
     METH_FASTCALL | METH_KEYWORDS,
     "Sets an attribute `attr_name` to `value`."},
    {"set_attrs", (PyCFunction)PyDataSlice_set_attrs,
     METH_FASTCALL | METH_KEYWORDS,
     R"""(Sets multiple attributes on an object / entity.

Args:
  update_schema: (bool) overwrite schema if attribute schema is missing or
    incompatible.
  **attrs: attribute values that are converted to DataSlices with DataBag
    adoption.
)"""},
    {"append", (PyCFunction)PyDataSlice_append, METH_FASTCALL,
     "Append a value to each list in this DataSlice"},
    {"_internal_pop", (PyCFunction)PyDataSlice_pop, METH_FASTCALL,
     "Pop a value from each list in this DataSlice"},
    {"clear", PyDataSlice_clear, METH_NOARGS,
     "Clears all dicts or lists in this DataSlice"},
    {"embed_schema", PyDataSlice_embed_schema, METH_NOARGS,
     R"""(Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as '__schema__' attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.
)"""},
    {"__dir__", PyDataSlice_dir, METH_NOARGS,
     "Returns a list of attributes available."},
    {"__format__", (PyCFunction)PyDataSlice_format, METH_O,
     "Returns a format representation with a special support for non empty "
     "specification.\n\nDataSlice will be replaced with base64 encoded slice."
     "\nMust be used with kd.fstr or kde.fstr."},
    {"internal_register_reserved_class_method_name",
     (PyCFunction)PyDataSlice_internal_register_reserved_class_method_name,
     METH_CLASS | METH_O,
     "Registers a name to be reserved as a method of the DataSlice class.\n"
     "\n"
     "You must call this when adding new methods to the class in Python.\n"
     "\n"
     "Args:\n"
     "  method_name: (str)\n"},
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
            res.emplace(method->ml_name);
          }
          const PyGetSetDef* getter = type->tp_getset;
          if (getter != nullptr) {
            for (; getter->name != nullptr; ++getter) {
              res.emplace(getter->name);
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

// TODO: Remove this alias.
PyGetSetDef kPyDataSlice_getset[] = {
    {
        .name = "db",
        .get = PyDataSlice_db_getter,
        .doc = "This property is deprecated, please use .get_bag().",
    },
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
      .name = "data_slice.DataSlice",
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

}  // namespace koladata::python
