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

#include <Python.h>

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
#include "py/koladata/base/wrap_utils.h"
#include "py/koladata/types/boxing.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

// NOTE: kd.literal does not do any implicit boxing of Python values into
// QValues. In future, implicit boxing that matches that of auto-boxing applied
// during Koda Expr evaluation may be added.
absl::Nullable<PyObject*> PyLiteral(PyObject* /*module*/, PyObject* value) {
  arolla::python::DCheckPyGIL();
  if (!arolla::python::IsPyQValueInstance(value)) {
    return PyErr_Format(
        PyExc_TypeError,
        "`value` must be a QValue to be wrapped into a literal, got: %s",
        Py_TYPE(value)->tp_name);
  }
  return arolla::python::WrapAsPyExpr(
      koladata::expr::MakeLiteral(arolla::python::UnsafeUnwrapPyQValue(value)));
}

absl::Nullable<PyObject*> PyModule_AddSchemaConstants(PyObject* m, PyObject*) {
  arolla::python::DCheckPyGIL();
  for (const auto& schema_const : SupportedSchemas()) {
    auto py_schema_const = arolla::python::PyObjectPtr::Own(
        WrapPyDataSlice(DataSlice(schema_const)));
    if (py_schema_const == nullptr) {
      return nullptr;
    }
    if (PyModule_AddObjectRef(m, schema_const.item().DebugString().c_str(),
                              py_schema_const.get()) < 0) {
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

}  // namespace

const PyMethodDef kDefPyLiteral = {
    "literal", PyLiteral, METH_O,
    "literal(value)\n"
    "--\n\n"
    "Constructs an expr with a LiteralOperator wrapping the provided QValue."};

const PyMethodDef kDefPyAddSchemaConstants = {
    "add_schema_constants", PyModule_AddSchemaConstants, METH_NOARGS,
    "Creates schema constants and adds them to the module."};

const PyMethodDef kDefPyFlattenPyList = {
    "flatten_py_list", PyFlattenPyList, METH_O,
    "Converts a Python nested list/tuple into a tuple of flat list and "
    "shape."};

}  // namespace koladata::python
