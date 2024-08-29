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
#include "py/koladata/expr/py_expr_eval.h"

#include <Python.h>  // IWYU pragma: keep

#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_eval.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "py/koladata/types/py_utils.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

absl::Nullable<PyObject*> PyEvalExpr(PyObject* /*self*/, PyObject** py_args,
                                     Py_ssize_t nargs, PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  // Parse expr.
  DCHECK_GE(nargs, 1);  // Checked above in FastcallArgParser::Parse.
  auto expr = arolla::python::UnwrapPyExpr(py_args[0]);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError,
                        "kd.eval() expects an expression, got expr: %s",
                        Py_TYPE(py_args[0])->tp_name);
  }

  // Parse inputs.
  std::vector<std::pair<std::string, arolla::TypedRef>> input_qvalues;
  input_qvalues.reserve(args.kw_names.size());
  for (int i = 0; i < args.kw_names.size(); ++i) {
    const auto* typed_value = arolla::python::UnwrapPyQValue(args.kw_values[i]);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "kd.eval() expects all inputs to be QValues, got: %s=%s",
          std::string(args.kw_names[i]).c_str(),
          Py_TYPE(args.kw_values[i])->tp_name);
    }
    input_qvalues.emplace_back(args.kw_names[i], typed_value->AsRef());
  }

  // Evaluate the expression.
  absl::StatusOr<arolla::TypedValue> result_or_error;
  {
    // We leave the Python world here, so we no longer need the GIL.
    arolla::python::ReleasePyGIL guard;
    result_or_error =
        koladata::expr::EvalExprWithCompilationCache(expr, input_qvalues, {});
  }
  ASSIGN_OR_RETURN(auto result, std::move(result_or_error),
                   (koladata::python::SetKodaPyErrFromStatus(_), nullptr));
  return arolla::python::WrapAsPyQValue(std::move(result));
}

PyObject* PyClearEvalCache(PyObject* /*self*/, PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  {
    arolla::python::ReleasePyGIL guard;
    koladata::expr::ClearCompilationCache();
  }
  Py_RETURN_NONE;
}

}  // namespace koladata::python
