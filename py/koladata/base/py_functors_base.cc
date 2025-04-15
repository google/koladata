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
#include "py/koladata/base/py_functors_base.h"

#include <Python.h>  // IWYU pragma: keep

#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_storage.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

PyObject* /*absl_nullable*/ PyPositionalOnlyParameterKind(PyObject* /*self*/,
                                                      PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::PositionalOnlyParameterKind();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyPositionalOrKeywordParameterKind(
    PyObject* /*self*/, PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::PositionalOrKeywordParameterKind();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyVarPositionalParameterKind(PyObject* /*self*/,
                                                     PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::VarPositionalParameterKind();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyKeywordOnlyParameterKind(PyObject* /*self*/,
                                                   PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::KeywordOnlyParameterKind();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyVarKeywordParameterKind(PyObject* /*self*/,
                                                  PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::VarKeywordParameterKind();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyNoDefaultValueMarker(PyObject* /*self*/,
                                               PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::NoDefaultValueMarker();
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyCreateFunctor(PyObject* /*self*/, PyObject** py_args,
                                        Py_ssize_t nargs,
                                        PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  if (nargs < 2) {
    return PyErr_Format(
        PyExc_ValueError,
        "kd.functor.expr_fn() expects exactly two positional inputs");
  }
  const auto* returns = UnwrapDataSlice(py_args[0], "returns");
  if (returns == nullptr) {
    return nullptr;
  }
  DataSlice signature;
  if (py_args[1] != Py_None) {
    const auto* signature_ptr = UnwrapDataSlice(py_args[1], "signature");
    if (signature_ptr == nullptr) {
      return nullptr;
    }
    signature = *signature_ptr;
  }
  std::vector<absl::string_view> variable_names;
  std::vector<DataSlice> variable_values;
  variable_names.reserve(args.kw_names.size());
  variable_values.reserve(args.kw_names.size());
  for (int i = 0; i < args.kw_names.size(); ++i) {
    const auto* variable = UnwrapDataSlice(
        args.kw_values[i], absl::StrFormat("variable [%s]", args.kw_names[i]));
    if (variable == nullptr) {
      return nullptr;
    }
    variable_names.emplace_back(args.kw_names[i]);
    variable_values.emplace_back(*variable);
  }
  // Evaluate the expression.
  absl::StatusOr<DataSlice> result_or_error;
  {
    // We leave the Python world here, so we must release the GIL,
    // otherwise we can get a deadlock between GIL and the C++ locks
    // that are used by the Expr compilation cache.
    arolla::python::ReleasePyGIL guard;
    result_or_error =
        functor::CreateFunctor(*returns, signature, std::move(variable_names),
                               std::move(variable_values));
  }
  ASSIGN_OR_RETURN(auto result, std::move(result_or_error),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

PyObject* /*absl_nullable*/ PyIsFn(PyObject* /*self*/, PyObject* fn) {
  arolla::python::DCheckPyGIL();
  const auto* unwrapped_fn = UnwrapDataSlice(fn, "fn");
  if (unwrapped_fn == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, functor::IsFunctor(*unwrapped_fn),
                   arolla::python::SetPyErrFromStatus(_));
  if (result) {
    Py_RETURN_TRUE;
  } else {
    Py_RETURN_FALSE;
  }
}

}  // namespace

const PyMethodDef kDefPyPositionalOnlyParameterKind = {
    "positional_only_parameter_kind", PyPositionalOnlyParameterKind,
    METH_NOARGS,
    "Returns the constant representing a positional only parameter kind."};

const PyMethodDef kDefPyPositionalOrKeywordParameterKind = {
    "positional_or_keyword_parameter_kind", PyPositionalOrKeywordParameterKind,
    METH_NOARGS,
    "Returns the constant representing a positional or keyword parameter "
    "kind."};

const PyMethodDef kDefPyVarPositionalParameterKind = {
    "var_positional_parameter_kind", PyVarPositionalParameterKind, METH_NOARGS,
    "Returns the constant representing a variadic positional parameter "
    "kind."};

const PyMethodDef kDefPyKeywordOnlyParameterKind = {
    "keyword_only_parameter_kind", PyKeywordOnlyParameterKind, METH_NOARGS,
    "Returns the constant representing a keyword only parameter kind."};

const PyMethodDef kDefPyVarKeywordParameterKind = {
    "var_keyword_parameter_kind", PyVarKeywordParameterKind, METH_NOARGS,
    "Returns the constant representing a variadic keyword parameter kind."};

const PyMethodDef kDefPyNoDefaultValueMarker = {
    "no_default_value_marker", PyNoDefaultValueMarker, METH_NOARGS,
    "Returns the constant representing lack of a default value for a "
    "parameter."};

const PyMethodDef kDefPyCreateFunctor = {
    "create_functor", (PyCFunction)PyCreateFunctor,
    METH_FASTCALL | METH_KEYWORDS, "Creates a new functor."};

const PyMethodDef kDefPyIsFn = {
    "is_fn", PyIsFn, METH_O,
    "Checks if a given DataSlice represents a functor."};

}  // namespace koladata::python
