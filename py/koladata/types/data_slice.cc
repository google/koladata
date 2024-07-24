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
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "py/koladata/types/boxing.h"
#include "py/koladata/types/py_utils.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

using arolla::python::SetPyErrFromStatus;

absl::Nullable<PyObject*> PyDataSlice_get_db(PyObject* self, void*) {
  arolla::python::DCheckPyGIL();
  auto db = UnsafeDataSliceRef(self).GetDb();
  if (db == nullptr) {
    Py_RETURN_NONE;
  }
  return arolla::python::WrapAsPyQValue(arolla::TypedValue::FromValue(db));
}

absl::Nullable<PyObject*> PyDataSlice_with_db(PyObject* self, PyObject* db) {
  arolla::python::DCheckPyGIL();
  if (db == Py_None) {
    return WrapPyDataSlice(UnsafeDataSliceRef(self).WithDb(nullptr));
  }
  DataBagPtr db_ptr = UnwrapDataBagPtr(db, "db");
  if (db_ptr == nullptr) {
    return nullptr;
  }
  return WrapPyDataSlice(UnsafeDataSliceRef(self).WithDb(std::move(db_ptr)));
}

absl::Nullable<PyObject*> PyDataSlice_no_db(PyObject* self) {
  arolla::python::DCheckPyGIL();
  return WrapPyDataSlice(UnsafeDataSliceRef(self).WithDb(nullptr));
}

absl::Nullable<PyObject*> PyDataSlice_with_fallback(PyObject* self,
                                                    PyObject* db) {
  arolla::python::DCheckPyGIL();
  DataBagPtr db_ptr = UnwrapDataBagPtr(db, "db");
  if (db_ptr == nullptr) {
    return nullptr;
  }
  const DataSlice& self_slice = UnsafeDataSliceRef(self);
  if (self_slice.GetDb() == nullptr) {
    return WrapPyDataSlice(self_slice.WithDb(
        DataBag::ImmutableEmptyWithFallbacks({std::move(db_ptr)})));
  }
  return WrapPyDataSlice(self_slice.WithDb(DataBag::ImmutableEmptyWithFallbacks(
      {self_slice.GetDb(), std::move(db_ptr)})));
}

// classmethod
absl::Nullable<PyObject*> PyDataSlice_from_vals(PyTypeObject* cls,
                                                PyObject* const* py_args,
                                                Py_ssize_t nargs,
                                                PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "dtype");
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }
  PyObject* list = py_args[0];
  const DataSlice* dtype = nullptr;
  if (PyObject* py_dtype = args.pos_kw_values[0];
      py_dtype != nullptr && py_dtype != Py_None) {
    if (!PyType_IsSubtype(Py_TYPE(py_dtype), PyDataSlice_Type())) {
      PyErr_Format(PyExc_TypeError, "expected DataItem for `dtype`, got: %s",
                   Py_TYPE(py_dtype)->tp_name);
      return nullptr;
    }
    dtype = &UnsafeDataSliceRef(py_dtype);
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

absl::Nullable<PyObject*> PyDataSlice_internal_as_py(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return DataSliceToPyValue(ds);
}

absl::Nullable<PyObject*> PyDataSlice_as_arolla_value(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto value, DataSliceToArollaValue(ds),
                   SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(value);
}

absl::Nullable<PyObject*> PyDataSlice_as_dense_array(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto array, DataSliceToDenseArray(ds),
                   SetPyErrFromStatus(_));
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
  ASSIGN_OR_RETURN(auto res, UnsafeDataSliceRef(self).GetAttr(attr_name_view),
                   SetPyErrFromStatus(_));
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
    ASSIGN_OR_RETURN(res, self_ds.GetAttr(attr_name_view),
                     SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(auto default_value,
                     DataSliceFromPyValueNoAdoption(args.pos_kw_values[0]),
                     SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(res,
                     self_ds.GetAttrWithDefault(attr_name_view, default_value),
                     SetPyErrFromStatus(_));
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
      SetPyErrFromStatus(status);
      return -1;
    }
    return 0;
  }
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(auto value_ds,
                   AssignmentRhsFromPyValue(self_ds, value, adoption_queue),
                   (SetPyErrFromStatus(_), -1));
  auto status = self_ds.SetAttr(attr_name_view, value_ds);
  if (status.ok()) {
    status = adoption_queue.AdoptInto(*self_ds.GetDb());
  }
  if (!status.ok()) {
    SetPyErrFromStatus(status);
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
      SetPyErrFromStatus(_));
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
        .With(SetPyErrFromStatus);
  } else {
    RETURN_IF_ERROR(self_ds.SetAttr(attr_name_view, value_ds))
        .With(SetPyErrFromStatus);
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetDb()))
      .With(SetPyErrFromStatus);
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
  if (!ParseUpdateSchemaArg(args, /*arg_pos=*/0, update_schema)) {
    return nullptr;
  }
  AdoptionQueue adoption_queue;
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(
      std::vector<DataSlice> values,
      ConvertArgsToDataSlices(self_ds.GetDb(), args.kw_values, adoption_queue),
      SetPyErrFromStatus(_));
  RETURN_IF_ERROR(self_ds.SetAttrs(args.kw_names, values, update_schema))
      .With(SetPyErrFromStatus);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetDb()))
      .With(SetPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_subscript(PyObject* self, PyObject* key) {
  arolla::python::DCheckPyGIL();
  if (key && PySlice_Check(key)) {
    Py_ssize_t start, stop, step;
    if (PySlice_Unpack(key, &start, &stop, &step) != 0) {
      return nullptr;
    }
    if (step != 1) {
      PyErr_SetString(PyExc_ValueError,
                      "slices with step != 1 are not supported");
      return nullptr;
    }
    std::optional<int64_t> stop_or_end =
        stop == PY_SSIZE_T_MAX ? std::optional<int64_t>(std::nullopt)
                               : std::optional<int64_t>(stop);
    ASSIGN_OR_RETURN(auto res,
                     UnsafeDataSliceRef(self).ExplodeList(start, stop_or_end),
                     SetPyErrFromStatus(_));
    return WrapPyDataSlice(std::move(res));
  }
  ASSIGN_OR_RETURN(auto key_ds, DataSliceFromPyValueNoAdoption(key),
                   SetPyErrFromStatus(_));
  const DataSlice& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res,
                   self_ds.ShouldApplyListOp() ?
                   self_ds.GetFromList(key_ds) : self_ds.GetFromDict(key_ds),
                   SetPyErrFromStatus(_));
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
                     (SetPyErrFromStatus(_), -1));
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
                     (SetPyErrFromStatus(_), -1));
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
                         (SetPyErrFromStatus(_), -1));
      }
      status = self_ds.SetInDict(key_ds, *value_ds);
    }
  }
  if (status.ok()) {
    status = adoption_queue.AdoptInto(*self_ds.GetDb());
  }
  if (!status.ok()) {
    SetPyErrFromStatus(status);
    return -1;
  }
  return 0;
}

PyObject* PyDataSlice_str(PyObject* self) {
  const DataSlice& self_ds = UnsafeDataSliceRef(self);

  ASSIGN_OR_RETURN(std::string s, DataSliceToStr(self_ds),
                   SetPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(s.c_str(), s.size());
}

absl::Nullable<PyObject*> PyDataSlice_get_keys(PyObject* self) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN(auto res, UnsafeDataSliceRef(self).GetDictKeys(),
                   SetPyErrFromStatus(_));
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
                   SetPyErrFromStatus(_));
  RETURN_IF_ERROR(self_ds.AppendToList(ds)).With(SetPyErrFromStatus);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*self_ds.GetDb()))
      .With(SetPyErrFromStatus);
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
  if (nargs == 1) {
    ASSIGN_OR_RETURN(auto index, DataSliceFromPyValueNoAdoption(args[0]),
                     SetPyErrFromStatus(_));
    ASSIGN_OR_RETURN(res, UnsafeDataSliceRef(self).PopFromList(index),
                     SetPyErrFromStatus(_));
  } else {
    ASSIGN_OR_RETURN(res, UnsafeDataSliceRef(self).PopFromList(),
                     SetPyErrFromStatus(_));
  }
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_clear(PyObject* self) {
  arolla::python::DCheckPyGIL();
  RETURN_IF_ERROR(UnsafeDataSliceRef(self).ClearDictOrList())
      .With(SetPyErrFromStatus);
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyDataSlice_get_present_count(PyObject* self) {
  arolla::python::DCheckPyGIL();
  return PyLong_FromSize_t(UnsafeDataSliceRef(self).present_count());
}

absl::Nullable<PyObject*> PyDataSlice_get_size(PyObject* self) {
  arolla::python::DCheckPyGIL();
  return PyLong_FromSize_t(UnsafeDataSliceRef(self).size());
}

absl::Nullable<PyObject*> PyDataSlice_get_ndim(PyObject* self) {
  arolla::python::DCheckPyGIL();
  return PyLong_FromSize_t(UnsafeDataSliceRef(self).GetShape().rank());
}

absl::Nullable<PyObject*> PyDataSlice_get_shape(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyJaggedShape(ds.GetShape());
}

absl::Nullable<PyObject*> PyDataSlice_get_schema(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return WrapPyDataSlice(ds.GetSchema());
}

absl::Nullable<PyObject*> PyDataSlice_is_list_schema(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return PyBool_FromLong(ds.IsListSchema());
}

absl::Nullable<PyObject*> PyDataSlice_is_dict_schema(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  return PyBool_FromLong(ds.IsDictSchema());
}

absl::Nullable<PyObject*> PyDataSlice_with_schema(PyObject* self,
                                                  PyObject* schema) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  auto schema_ds = UnwrapDataSlice(schema, "schema");
  if (schema_ds == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto res, ds.WithSchema(*schema_ds), SetPyErrFromStatus(_));
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
  ASSIGN_OR_RETURN(auto res, ds.SetSchema(*schema_ds), SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_as_any(PyObject* self) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN(
      auto res,
      UnsafeDataSliceRef(self).WithSchema(internal::DataItem(schema::kAny)),
      SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_embed_schema(PyObject* self) {
  arolla::python::DCheckPyGIL();
  auto& self_ds = UnsafeDataSliceRef(self);
  ASSIGN_OR_RETURN(auto res, self_ds.EmbedSchema(), SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(res));
}

absl::Nullable<PyObject*> PyDataSlice_dir(PyObject* self) {
  arolla::python::DCheckPyGIL();
  ASSIGN_OR_RETURN(auto attr_names, UnsafeDataSliceRef(self).GetAttrNames(),
                   SetPyErrFromStatus(_));
  auto attr_name_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/attr_names.size()));
  int i = 0;
  for (const auto& attr_name : attr_names) {
    PyObject* py_attr_name = PyUnicode_DecodeUTF8(
        attr_name.view().data(), attr_name.view().size(), nullptr);
    if (py_attr_name == nullptr) {
      return nullptr;
    }
    PyList_SetItem(attr_name_list.get(), i++, py_attr_name);
  }
  return attr_name_list.release();
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

PyGetSetDef kPyDataSlice_getset[] = {
    {
        .name = "db",
        .get = PyDataSlice_get_db,
        .doc = "Attached DataBag.",
    },
    {nullptr}, /* sentinel */
};

PyMethodDef kPyDataSlice_methods[] = {
    {"with_db", (PyCFunction)PyDataSlice_with_db, METH_O,
     "Returns a copy of DataSlice with DataBag `db`."},
    {"no_db", (PyCFunction)PyDataSlice_no_db, METH_NOARGS,
     "Returns a copy of DataSlice without DataBag."},
    {"with_fallback", (PyCFunction)PyDataSlice_with_fallback, METH_O,
     R"""(Returns a copy of DataSlice with DataBag with two fallbacks.

Original and provided DataBags are used as fallbacks. Modifications of
fallbacks will be reflected in the new DataBag. Modifications to the new
DataBag will be local and not going to be visible in the fallbacks.

Args:
  db: Fallback DataBag. Must be not None.
Returns:
  DataSlice with new empty DataBag and up to two fallbacks. One fallback will be
  present in case DataSlice has no DataBag.
)"""},
    {"from_vals", (PyCFunction)PyDataSlice_from_vals,
     METH_CLASS | METH_FASTCALL | METH_KEYWORDS,
     "Creates a DataSlice from `value`"},
    {"internal_as_py", (PyCFunction)PyDataSlice_internal_as_py, METH_NOARGS,
     "Returns a Python object equivalent to this DataSlice.\n"
     "\n"
     "If the values in this slice represent objects, then the returned python\n"
     "structure will contain DataItems.\n"},
    {"as_arolla_value", (PyCFunction)PyDataSlice_as_arolla_value, METH_NOARGS,
     "Converts primitive slice / item into an equivalent Arolla value."},
    {"as_dense_array", (PyCFunction)PyDataSlice_as_dense_array, METH_NOARGS,
     "Converts primitive slice to an arolla.dense_array with appropriate "
     "qtype."},
    {"get_ndim", (PyCFunction)PyDataSlice_get_ndim, METH_NOARGS,
     "Returns the number of dimensions of the DataSlice, a.k.a. the rank or "
     "nesting level."},
    {"rank", (PyCFunction)PyDataSlice_get_ndim, METH_NOARGS,
     "Returns the number of dimensions of the DataSlice, a.k.a. the rank or "
     "nesting level."},
    {"get_shape", (PyCFunction)PyDataSlice_get_shape, METH_NOARGS,
     "Returns the shape of the DataSlice."},
    {"get_schema", (PyCFunction)PyDataSlice_get_schema, METH_NOARGS,
     "Returns a schema slice with type information about this DataSlice."},
    {"is_list_schema", (PyCFunction)PyDataSlice_is_list_schema, METH_NOARGS,
     "Returns True, if this DataSlice is a List Schema."},
    {"is_dict_schema", (PyCFunction)PyDataSlice_is_dict_schema, METH_NOARGS,
     "Returns True, if this DataSlice is a Dict Schema."},
    {"with_schema", (PyCFunction)PyDataSlice_with_schema, METH_O,
     R"""(Returns a copy of DataSlice with the provided `schema`.

`schema` must have no DataBag or the same DataBag as the DataSlice. If `schema`
has a different DataBag, use `set_schema` instead. See kd.with_schema for more
details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
)"""},
    {"set_schema", (PyCFunction)PyDataSlice_set_schema, METH_O,
     R"""(Returns a copy of DataSlice with the provided `schema`.

If `schema` has a different DataBag than the DataSlice, `schema` is merged into
the DataBag of the DataSlice. See kd.set_schema for more details.

Args:
  schema: schema DataSlice to set.
Returns:
  DataSlice with the provided `schema`.
)"""},
    {"as_any", (PyCFunction)PyDataSlice_as_any, METH_NOARGS,
     "Returns a DataSlice with ANY schema."},
    {"get_keys", (PyCFunction)PyDataSlice_get_keys, METH_NOARGS,
     "Returns a slice with all keys from all dicts in this DataSlice."},
    {"get_present_count", (PyCFunction)PyDataSlice_get_present_count,
     METH_NOARGS, "Returns number of present items in DataSlice."},
    {"get_size", (PyCFunction)PyDataSlice_get_size, METH_NOARGS,
     "Returns number of items in DataSlice."},
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
    {"clear", (PyCFunction)PyDataSlice_clear, METH_NOARGS,
     "Clears all dicts or lists in this DataSlice"},
    {"embed_schema", (PyCFunction)PyDataSlice_embed_schema, METH_NOARGS,
     R"""(Returns a DataSlice with OBJECT schema.

* For primitives no data change is done.
* For Entities schema is stored as '__schema__' attribute.
* Embedding Entities requires a DataSlice to be associated with a DataBag.
)"""},
    {"__dir__", (PyCFunction)PyDataSlice_dir, METH_NOARGS,
     "Returns a list of attributes available."},
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
          for (; getter->name != nullptr; ++getter) {
            res.emplace(getter->name);
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
  // raising TypeError here to prevent Python's interpration of NotImplemented,
  // is justified.
  PyErr_SetString(PyExc_TypeError,
                  "RichCompare methods are overwritten in Python");
  return nullptr;
}

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
