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
#include "py/koladata/types/data_item.h"

#include <Python.h>

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/boxing.h"
#include "py/koladata/base/py_args.h"
#include "py/koladata/base/to_py_object.h"
#include "py/koladata/base/wrap_utils.h"
#include "py/koladata/types/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

int PyDataItem_bool(PyObject* self) {
  arolla::python::DCheckPyGIL();
  auto error = []() {
    PyErr_SetString(PyExc_ValueError,
                    "cannot cast a non-MASK DataItem to bool. For BOOLEAN, "
                    "explicit conversion to MASK (e.g. ds == True) is needed. "
                    "See go/koda-common-pitfalls#masks-vs-booleans.");
    return -1;
  };
  const auto& ds = UnsafeDataSliceRef(self);
  if (!ds.GetSchemaImpl().holds_value<schema::DType>()) {
    return error();
  }
  if (auto dtype = ds.GetSchemaImpl().value<schema::DType>();
      dtype != schema::kMask && dtype != schema::kObject &&
      dtype != schema::kNone) {
    return error();
  }
  if (ds.item().holds_value<arolla::Unit>() || !ds.item().has_value()) {
    return ds.item().holds_value<arolla::Unit>();
  }
  return error();
}

PyObject* absl_nullable PyDataItem_index(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  if (ds.item().holds_value<int>()) {
    return PyLong_FromLong(ds.item().value<int>());
  }
  if (ds.item().holds_value<int64_t>()) {
    return PyLong_FromLongLong(ds.item().value<int64_t>());
  }
  return PyErr_Format(
      PyExc_ValueError,
      "only INT32/INT64 DataItem can be used for indexing Python objects, "
      "got %s",
      arolla::Repr(ds).c_str());
}

PyObject* absl_nullable PyDataItem_int(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  DCHECK_EQ(ds.GetShape().rank(), 0);
  if (ds.dtype() == arolla::GetQType<internal::ObjectId>()) {
    return PyErr_Format(
        PyExc_ValueError,
        "int() argument cannot be a DataItem that holds an ItemId, got %s",
        arolla::Repr(ds).c_str());
  }
  ASSIGN_OR_RETURN(arolla::python::PyObjectPtr py_obj,
                   PyObjectFromDataSlice(ds),
                   arolla::python::SetPyErrFromStatus(_));
  return PyNumber_Long(py_obj.get());
}

PyObject* absl_nullable PyDataItem_float(PyObject* self) {
  arolla::python::DCheckPyGIL();
  const auto& ds = UnsafeDataSliceRef(self);
  DCHECK_EQ(ds.GetShape().rank(), 0);
  if (ds.dtype() == arolla::GetQType<internal::ObjectId>()) {
    return PyErr_Format(
        PyExc_ValueError,
        "float() argument cannot be a DataItem that holds an ItemId, got %s",
        arolla::Repr(ds).c_str());
  }
  ASSIGN_OR_RETURN(arolla::python::PyObjectPtr py_obj,
                   PyObjectFromDataSlice(ds),
                   arolla::python::SetPyErrFromStatus(_));
  return PyNumber_Float(py_obj.get());
}

// classmethod
PyObject* absl_nullable PyDataItem_from_vals(PyTypeObject* cls,
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
  PyObject* py_obj = py_args[0];
  if (PyList_CheckExact(py_obj) || PyTuple_CheckExact(py_obj)) {
    PyErr_SetString(PyExc_TypeError,
                    "DataItem and other 0-rank class method `from_vals` "
                    "cannot create multi-dim DataSlice");
    return nullptr;
  }
  std::optional<DataSlice> schema;
  if (!UnwrapDataSliceOptionalArg(args.pos_kw_values[0], "schema", schema)) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto ds, DataItemFromPyValue(py_obj, schema),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(ds));
}

PyMethodDef kPyDataItem_methods[] = {
    {"from_vals", (PyCFunction)PyDataItem_from_vals,
     METH_CLASS | METH_FASTCALL | METH_KEYWORDS,
     "from_vals(x, /, schema=None)\n"
     "--\n\n"
     R"""(Returns a DataItem created from `x`.

If `schema` is set, that schema is used, otherwise the schema is inferred from
`x`. Python value must be convertible to Koda scalar and the result cannot
be multidimensional DataSlice.

Args:
  x: a Python value or a DataItem.
  schema: schema DataItem to set. If `x` is already a DataItem, this will cast
    it to the given schema.
)"""},
    {nullptr}, /* sentinel */
};

// Creates and initializes PyTypeObject for Python DataItem class.
PyTypeObject* InitPyDataItemType() {
  arolla::python::DCheckPyGIL();
  PyTypeObject* py_data_slice_type = PyDataSlice_Type();
  if (py_data_slice_type == nullptr) {
    return nullptr;
  }
  PyType_Slot slots[] = {
      // By being a subclass of DataSlice, it is also a subclass of QValue.
      {Py_tp_base, py_data_slice_type},
      {Py_tp_methods, kPyDataItem_methods},
      {Py_nb_bool, (void*)PyDataItem_bool},
      {Py_nb_index, (void*)PyDataItem_index},
      {Py_nb_int, (void*)PyDataItem_int},
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
  PyObject_SetAttrString(qvalue_subtype, "_COLAB_HAS_SAFE_REPR", Py_True);
  if (!arolla::python::RegisterPyQValueSpecializationByKey(
          kDataItemQValueSpecializationKey, qvalue_subtype)) {
    return nullptr;
  }
  return reinterpret_cast<PyTypeObject*>(qvalue_subtype);
}

}  // namespace

PyTypeObject* PyDataItem_Type() {
  arolla::python::DCheckPyGIL();
  static PyTypeObject* type = InitPyDataItemType();
  return type;
}

}  // namespace koladata::python
