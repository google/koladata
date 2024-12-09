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
// Unified Binding Policy.
//
// This is a binding policy for Koladata operators, adding support for
// positional-only, keyword-only, and variadic keyword parameters.
//
// It encodes additional information about parameters in signature.aux_policy
// using the following format:
//
//    koladata_unified_binding_policy:<options>
//
// Each character in '<options>' represents a parameter and encodes its kind:
//
//   `_` -- positional-only parameter
//   `p` -- positional-or-keyword parameter
//   `P` -- variadic-positional (*args)
//   `k` -- keyword-only, no default
//   `d` -- keyword-only, with default value
//   `K` -- variadic-keyword (**kwargs)
//   `H` -- non-deterministic input
//

#ifndef THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_
#define THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_

#include "absl/strings/string_view.h"

namespace koladata::python {

constexpr absl::string_view kUnifiedPolicy = "koladata_unified_binding_policy";

constexpr char kUnifiedPolicyOptPositionalOnly = '_';
constexpr char kUnifiedPolicyOptPositionalOrKeyword = 'p';
constexpr char kUnifiedPolicyOptVarPositional = 'P';
constexpr char kUnifiedPolicyOptRequiredKeywordOnly = 'k';
constexpr char kUnifiedPolicyOptOptionalKeywordOnly = 'd';
constexpr char kUnifiedPolicyOptVarKeyword = 'K';
constexpr char kUnifiedPolicyOptNonDeterministic = 'H';

// Registers the unified binding policy. If the function fails, it returns
// `false` and sets a Python exception.
bool RegisterUnifiedBindingPolicy();

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_
