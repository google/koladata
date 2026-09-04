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
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/call.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/wrap_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(py_functors_py_ext, m) {
  m.doc() = "The module with Python bindings for Koda functors.";

  m.def(
      "auto_variables",
      [](py::object py_fn,
         const std::vector<arolla::Fingerprint>& extra_nodes_to_extract_py) {
        const DataSlice* fn = UnwrapDataSlice(py_fn.ptr(), "fn");
        if (fn == nullptr) {
          throw py::error_already_set();
        }
        absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract(
            extra_nodes_to_extract_py.begin(), extra_nodes_to_extract_py.end());
        absl::StatusOr<DataSlice> res_or_error;
        {
          arolla::python::ReleasePyGIL guard;
          res_or_error =
              functor::AutoVariables(*fn, std::move(extra_nodes_to_extract));
        }
        DataSlice res =
            arolla::python::pybind11_unstatus_or(std::move(res_or_error));
        return arolla::python::pybind11_steal_or_throw<py::object>(
            WrapPyDataSlice(std::move(res)));
      },
      py::arg("fn"),
      py::arg("extra_nodes_to_extract") = std::vector<arolla::Fingerprint>{},
      "Returns a functor with auto-variables extracted.");

  m.def(
      "get_variable_evaluation_order",
      [](py::object py_fn) {
        const DataSlice* fn = UnwrapDataSlice(py_fn.ptr(), "fn");
        if (fn == nullptr) {
          throw py::error_already_set();
        }
        absl::StatusOr<std::vector<std::string>> res_or_error;
        {
          arolla::python::ReleasePyGIL guard;
          res_or_error = functor::GetVariableEvaluationOrder(*fn);
        }
        return arolla::python::pybind11_unstatus_or(std::move(res_or_error));
      },
      py::arg("fn"),
      "Returns the topologically-sorted variable evaluation order of a "
      "functor.");

  m.def(
      "call_functor",
      [](py::object py_fn, const std::vector<arolla::TypedValue>& args,
         const std::vector<std::string>& kwnames) {
        arolla::python::PyCancellationScope cancellation_scope;
        const DataSlice* fn = UnwrapDataSlice(py_fn.ptr(), "fn");
        if (fn == nullptr) {
          throw py::error_already_set();
        }
        std::vector<arolla::TypedRef> arg_refs;
        arg_refs.reserve(args.size());
        for (const auto& arg : args) {
          arg_refs.push_back(arg.AsRef());
        }
        absl::StatusOr<arolla::TypedValue> result_or_error;
        {
          arolla::python::ReleasePyGIL guard;
          result_or_error =
              functor::CallFunctorWithCompilationCache(*fn, arg_refs, kwnames);
        }
        return arolla::python::pybind11_unstatus_or(std::move(result_or_error));
      },
      py::arg("fn"), py::arg("args") = std::vector<arolla::TypedValue>{},
      py::arg("kwnames") = std::vector<std::string>{},
      "Calls a functor with the given arguments and returns the result "
      "QValue.");
}

}  // namespace
}  // namespace koladata::python
