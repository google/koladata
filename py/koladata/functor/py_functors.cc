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
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_storage.h"
#include "py/arolla/abc/py_fingerprint.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "py/koladata/types/py_utils.h"
#include "arolla/util/fingerprint.h"
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
    return PyErr_Format(
        PyExc_ValueError,
        "kd.functor.expr_fn() expects exactly two positional inputs");
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
  // Evaluate the expression.
  absl::StatusOr<DataSlice> result_or_error;
  {
    // We leave the Python world here, so we must release the GIL,
    // otherwise we can get a deadlock between GIL and the C++ locks
    // that are used by the Expr compilation cache.
    arolla::python::ReleasePyGIL guard;
    result_or_error = functor::CreateFunctor(*returns, signature, variables);
  }
  ASSIGN_OR_RETURN(auto result, std::move(result_or_error),
                   arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

absl::Nullable<PyObject*> PyIsFn(PyObject* /*self*/, PyObject* fn) {
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

absl::Nullable<PyObject*> PyAutoVariables(PyObject* /*self*/,
                                          PyObject** py_args,
                                          Py_ssize_t nargs) {
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/false, "fn", "extra_nodes_to_extract");
  arolla::python::DCheckPyGIL();
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, nullptr, args)) {
    return nullptr;
  }
  const auto* fn = UnwrapDataSlice(args.pos_only_args[0], "fn");
  if (fn == nullptr) {
    return nullptr;
  }
  absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract;
  PyObject* fingerprint_list = args.pos_kw_values[0];
  if (fingerprint_list != nullptr) {
    if (!PyList_Check(fingerprint_list)) {
      return PyErr_Format(PyExc_ValueError,
                          "second argument should be a list of fingerprints");
    }
    Py_ssize_t count = PyList_Size(fingerprint_list);
    extra_nodes_to_extract.reserve(count);
    for (Py_ssize_t i = 0; i < count; ++i) {
      const arolla::Fingerprint* fingerprint =
          arolla::python::UnwrapPyFingerprint(
              PyList_GetItem(fingerprint_list, i));
      if (fingerprint == nullptr) {
        return nullptr;
      }
      extra_nodes_to_extract.insert(*fingerprint);
    }
  }
  ASSIGN_OR_RETURN(
      DataSlice result,
      functor::AutoVariables(*fn, std::move(extra_nodes_to_extract)),
      arolla::python::SetPyErrFromStatus(_));
  return WrapPyDataSlice(std::move(result));
}

}  // namespace koladata::python
