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
#include "py/koladata/types/py_misc.h"

#include <Python.h>  // IWYU pragma: keep

#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/schema_constants.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/types/boxing.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

absl::Nullable<PyObject*> PyMakeLiteralOperator(PyObject* /*module*/,
                                                PyObject* value) {
  arolla::python::DCheckPyGIL();
  if (!arolla::python::IsPyQValueInstance(value)) {
    PyErr_Format(PyExc_TypeError,
                 "`value` must be a QValue to be wrapped into a "
                 "LiteralOperator, got: %s",
                 Py_TYPE(value)->tp_name);
    return nullptr;
  }
  return arolla::python::WrapAsPyQValue(
      arolla::TypedValue::FromValue(arolla::expr::ExprOperatorPtr(
          koladata::expr::LiteralOperator::MakeLiteralOperator(
              arolla::python::UnsafeUnwrapPyQValue(value)))));
}

// NOTE: kd.literal does not do any implicit boxing of Python values into
// QValues. In future, implicit boxing that matches that of auto-boxing applied
// during Koda Expr evaluation may be added.
absl::Nullable<PyObject*> PyMakeLiteralExpr(PyObject* /*module*/,
                                            PyObject* value) {
  arolla::python::DCheckPyGIL();
  if (!arolla::python::IsPyQValueInstance(value)) {
    PyErr_Format(PyExc_TypeError,
                 "`value` must be a QValue to be wrapped into a "
                 "LiteralExpr, got: %s",
                 Py_TYPE(value)->tp_name);
    return nullptr;
  }
  return arolla::python::WrapAsPyExpr(koladata::expr::MakeLiteral(
      arolla::python::UnsafeUnwrapPyQValue(value)));
}

absl::Nullable<PyObject*> PyModule_AddSchemaConstants(PyObject* m, PyObject*) {
  arolla::python::DCheckPyGIL();
  for (const auto& schema_const : SupportedSchemas()) {
    PyObject* py_schema_const = WrapPyDataSlice(DataSlice(schema_const));
    if (py_schema_const == nullptr) {
      return nullptr;
    }
    // Takes ownership of py_schema_const.
    if (PyModule_AddObject(m, schema_const.item().DebugString().c_str(),
                           py_schema_const) < 0) {
      return nullptr;
    }
  }
  Py_RETURN_NONE;
}

absl::Nullable<PyObject*> PyFlattenPyList(PyObject* /*module*/,
                                          PyObject* py_obj) {
  ASSIGN_OR_RETURN((auto [py_objects, shape]), FlattenPyList(py_obj),
                   arolla::python::SetPyErrFromStatus(_));
  auto py_list =
      arolla::python::PyObjectPtr::Own(PyList_New(/*len=*/py_objects.size()));
  for (int i = 0; i < py_objects.size(); ++i) {
    PyList_SetItem(py_list.get(), i, Py_NewRef(py_objects[i]));
  }
  auto py_shape =
      arolla::python::PyObjectPtr::Own(arolla::python::WrapAsPyQValue(
          arolla::TypedValue::FromValue(std::move(shape))));
  return PyTuple_Pack(2, py_list.release(), py_shape.release());
}

}  // namespace koladata::python
