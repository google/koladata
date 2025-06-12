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
#include <Python.h>

#include <string>

#include "koladata/expr/non_determinism.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/operators/py_unified_binding_policy.h"

namespace koladata::python {
namespace {

using ::arolla::python::PyObjectPtr;

constexpr const char* kThisModuleName = "koladata.optools.py_optools";

PyMethodDef kMethods[] = {
    {nullptr} /* sentinel */
};

PyModuleDef kModule = {
    PyModuleDef_HEAD_INIT,
    kThisModuleName,
    /*module docstring=*/"Koda optools.",
    -1,
    /*methods=*/kMethods,
};

// NOTE: This PyInit function must be named this way
// (PyInit_{py_extension.name}). Otherwise it does not get initialized.
PyMODINIT_FUNC PyInit_py_optools_py_ext(void) {
  if (!RegisterUnifiedBindingPolicy()) {
    return nullptr;
  }
  auto result = PyObjectPtr::Own(PyModule_Create(&kModule));
  if (result == nullptr) {
    return nullptr;
  }
  if (PyModule_AddStringConstant(
          result.get(), "NON_DETERMINISTIC_PARAM_NAME",
          std::string(expr::kNonDeterministicParamName).c_str()) <
          0 ||
      PyModule_AddStringConstant(result.get(), "UNIFIED_POLICY",
                                 std::string(kUnifiedPolicy).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_POSITIONAL_ONLY",
          std::string(1, kUnifiedPolicyOptPositionalOnly).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_POSITIONAL_OR_KEYWORD",
          std::string(1, kUnifiedPolicyOptPositionalOrKeyword).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_VAR_POSITIONAL",
          std::string(1, kUnifiedPolicyOptVarPositional).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_REQUIRED_KEYWORD_ONLY",
          std::string(1, kUnifiedPolicyOptRequiredKeywordOnly).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_OPTIONAL_KEYWORD_ONLY",
          std::string(1, kUnifiedPolicyOptOptionalKeywordOnly).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_VAR_KEYWORD",
          std::string(1, kUnifiedPolicyOptVarKeyword).c_str()) < 0 ||
      PyModule_AddStringConstant(
          result.get(), "UNIFIED_POLICY_OPT_NON_DETERMINISTIC",
          std::string(1, kUnifiedPolicyOptNonDeterministic).c_str()) < 0) {
    return nullptr;
  }
  return result.release();
}

}  // namespace
}  // namespace koladata::python
