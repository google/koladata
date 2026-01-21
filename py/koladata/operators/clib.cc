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
#include "absl/strings/string_view.h"
#include "koladata/expr/non_determinism.h"
#include "py/koladata/operators/py_unified_binding_policy.h"
#include "pybind11/options.h"
#include "pybind11/pybind11.h"
#include "pybind11/pytypes.h"
#include "pybind11_abseil/absl_casters.h"

namespace koladata::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  // NOTE: We disable prepending of function signatures to docstrings because
  // pybind11 generates signatures that are incompatible with
  // `__text_signature__`.
  py::options options;
  options.disable_function_signatures();

  m.attr("NON_DETERMINISTIC_PARAM_NAME") =
      py::str(expr::kNonDeterministicParamName);
  m.attr("UNIFIED_POLICY_OPT_POSITIONAL_ONLY") =
      py::str(&kUnifiedPolicyOptPositionalOnly, 1);
  m.attr("UNIFIED_POLICY_OPT_POSITIONAL_OR_KEYWORD") =
      py::str(&kUnifiedPolicyOptPositionalOrKeyword, 1);
  m.attr("UNIFIED_POLICY_OPT_VAR_POSITIONAL") =
      py::str(&kUnifiedPolicyOptVarPositional, 1);
  m.attr("UNIFIED_POLICY_OPT_REQUIRED_KEYWORD_ONLY") =
      py::str(&kUnifiedPolicyOptRequiredKeywordOnly, 1);
  m.attr("UNIFIED_POLICY_OPT_OPTIONAL_KEYWORD_ONLY") =
      py::str(&kUnifiedPolicyOptOptionalKeywordOnly, 1);
  m.attr("UNIFIED_POLICY_OPT_VAR_KEYWORD") =
      py::str(&kUnifiedPolicyOptVarKeyword, 1);
  m.attr("UNIFIED_POLICY_OPT_NON_DETERMINISTIC") =
      py::str(&kUnifiedPolicyOptNonDeterministic, 1);

  m.def(
      "register_unified_aux_binding_policy",
      [](absl::string_view aux_policy_name) {
        if (!RegisterUnifiedBindingPolicy(aux_policy_name)) {
          throw py::error_already_set();
        }
      },
      py::arg("aux_policy_name"), py::pos_only(),
      py::doc("register_unified_aux_binding_policy(aux_policy_name, /)\n"
              "--\n\n"
              "Registers the unified binding policy under the given name."));
}

}  // namespace
}  // namespace koladata::python
