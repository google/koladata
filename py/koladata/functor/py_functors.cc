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
#include "py/koladata/functor/py_functors.h"

#include <Python.h>  // IWYU pragma: keep

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_storage.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "py/koladata/types/py_utils.h"
#include "py/koladata/types/wrap_utils.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

absl::Nullable<PyObject*> PyPositionalOnlyParameterKind(PyObject* /*self*/,
                                                        PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::PositionalOnlyParameterKind();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyPositionalOrKeywordParameterKind(
    PyObject* /*self*/, PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::PositionalOrKeywordParameterKind();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyVarPositionalParameterKind(PyObject* /*self*/,
                                                       PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::VarPositionalParameterKind();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyKeywordOnlyParameterKind(PyObject* /*self*/,
                                                     PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::KeywordOnlyParameterKind();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyVarKeywordParameterKind(PyObject* /*self*/,
                                                    PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::VarKeywordParameterKind();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyNoDefaultValueMarker(PyObject* /*self*/,
                                                 PyObject* /*py_args*/) {
  arolla::python::DCheckPyGIL();
  // We make a copy since WrapPyDataSlice() takes ownership.
  DataSlice result = functor::NoDefaultValueMarker();
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyCreateFunctor(PyObject* /*self*/,
                                          PyObject** py_args, Py_ssize_t nargs,
                                          PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/2, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  if (nargs < 2) {
    return PyErr_Format(PyExc_ValueError,
                        "kdf.fn() expects exactly two positional inputs");
  }
  const auto* returns = UnwrapDataSlice(py_args[0], "returns");
  if (returns == nullptr) {
    return nullptr;
  }
  std::optional<DataSlice> signature = std::nullopt;
  if (py_args[1] != Py_None) {
    const auto* signature_ptr = UnwrapDataSlice(py_args[1], "signature");
    if (signature_ptr == nullptr) {
      return nullptr;
    }
    signature = *signature_ptr;
  }
  std::vector<std::pair<std::string, DataSlice>> variables;
  variables.reserve(args.kw_names.size());
  for (int i = 0; i < args.kw_names.size(); ++i) {
    const auto* variable = UnwrapDataSlice(
        args.kw_values[i], absl::StrFormat("variable [%s]", args.kw_names[i]));
    if (variable == nullptr) {
      return nullptr;
    }
    variables.emplace_back(args.kw_names[i], *variable);
  }
  ASSIGN_OR_RETURN(auto result,
                   functor::CreateFunctor(*returns, signature, variables),
                   (koladata::python::SetKodaPyErrFromStatus(_), nullptr));
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyCall(PyObject* /*self*/, PyObject** py_args,
                                 Py_ssize_t nargs, PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  FastcallArgParser parser(/*pos_only_n=*/nargs, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser.Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  if (nargs < 1) {
    return PyErr_Format(PyExc_ValueError,
                        "kd.call() expects at least one positional input");
  }
  const auto* functor = UnwrapDataSlice(py_args[0], "functor");
  if (functor == nullptr) {
    return nullptr;
  }
  std::vector<arolla::TypedRef> input_args;
  input_args.reserve(nargs - 1);
  for (int i = 1; i < nargs; ++i) {
    const auto* typed_value = arolla::python::UnwrapPyQValue(py_args[i]);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(PyExc_TypeError,
                          "kd.call() expects all inputs to be QValues, got: %s",
                          Py_TYPE(args.kw_values[i])->tp_name);
    }
    input_args.emplace_back(typed_value->AsRef());
  }
  std::vector<std::pair<std::string, arolla::TypedRef>> input_kwargs;
  input_kwargs.reserve(args.kw_names.size());
  for (int i = 0; i < args.kw_names.size(); ++i) {
    const auto* typed_value = arolla::python::UnwrapPyQValue(args.kw_values[i]);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "kd.call() expects all inputs to be QValues, got: %s=%s",
          std::string(args.kw_names[i]).c_str(),
          Py_TYPE(args.kw_values[i])->tp_name);
    }
    input_kwargs.emplace_back(args.kw_names[i], typed_value->AsRef());
  }
  absl::StatusOr<arolla::TypedValue> result_or_error;
  {
    // We leave the Python world here, so we no longer need the GIL.
    arolla::python::ReleasePyGIL guard;
    result_or_error = functor::CallFunctorWithCompilationCache(
        *functor, input_args, input_kwargs);
  }
  ASSIGN_OR_RETURN(auto result, std::move(result_or_error),
                   (koladata::python::SetKodaPyErrFromStatus(_), nullptr));
  return arolla::python::WrapAsPyQValue(std::move(result));
}

absl::Nullable<PyObject*> PyIsFn(PyObject* /*self*/, PyObject* fn) {
  arolla::python::DCheckPyGIL();
  const auto* unwrapped_fn = UnwrapDataSlice(fn, "fn");
  if (unwrapped_fn == nullptr) {
    return nullptr;
  }
  ASSIGN_OR_RETURN(auto result, functor::IsFunctor(*unwrapped_fn),
                   (koladata::python::SetKodaPyErrFromStatus(_), nullptr));
  if (result) {
    Py_RETURN_TRUE;
  } else {
    Py_RETURN_FALSE;
  }
}

}  // namespace koladata::python
