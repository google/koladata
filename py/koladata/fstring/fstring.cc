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
#include "py/koladata/fstring/fstring.h"

#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_utils.h"
#include "py/koladata/fstring/fstring_processor.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

PyObject* PyFStringExprPlaceholder(PyObject* /*module*/,
                                   PyObject* const* py_args, Py_ssize_t nargs) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/false);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, nullptr, args)) {
    return nullptr;
  }

  // Parse expr.
  DCHECK_EQ(nargs, 2);  // Checked above in FastcallArgParser::Parse.
  auto expr = arolla::python::UnwrapPyExpr(py_args[0]);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(
        PyExc_TypeError,
        "kd._fstr_expr_placeholder() expects an expression, got: %s",
        Py_TYPE(py_args[0])->tp_name);
  }
  Py_ssize_t size;
  const char* format_spec = PyUnicode_AsUTF8AndSize(py_args[1], &size);
  if (format_spec == nullptr) {
    return nullptr;
  }
  if (size == 0) {
    PyErr_SetString(PyExc_TypeError, "expected non empty format_spec");
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      auto placeholder,
      fstring::ToExprPlaceholder(expr, absl::string_view(format_spec, size)),
      arolla::python::SetPyErrFromStatus(_));
  return PyUnicode_FromStringAndSize(
      placeholder.c_str(), static_cast<Py_ssize_t>(placeholder.size()));
}

PyObject* PyEagerFStringEval(PyObject* /*module*/, PyObject* arg) {
  arolla::python::DCheckPyGIL();
  Py_ssize_t size;
  const char* fstr = PyUnicode_AsUTF8AndSize(arg, &size);
  if (fstr == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(
      auto tv, fstring::EvaluateFStringDataSlice(absl::string_view(fstr, size)),
      arolla::python::SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyQValue(std::move(tv));
}

absl::Nullable<PyObject*> PyCreateFstrExpr(PyObject* /*module*/,
                                           PyObject* arg) {
  arolla::python::DCheckPyGIL();
  Py_ssize_t size;
  const char* fstr = PyUnicode_AsUTF8AndSize(arg, &size);
  if (fstr == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto expr,
                   fstring::CreateFStringExpr(absl::string_view(fstr, size)),
                   arolla::python::SetPyErrFromStatus(_));
  return arolla::python::WrapAsPyExpr(std::move(expr));
}

}  // namespace koladata::python
