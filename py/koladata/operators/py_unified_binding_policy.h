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
// Unified Binding Policy.
//
// This is a binding policy for Koladata operators, adding support for
// positional-only, keyword-only, and variadic keyword parameters.
//
// It encodes additional information about parameters in signature.aux_policy
// using the following format:
//
//    koladata_unified_binding_policy:<binding_options>[:<boxing_options>]
//
// Each character in '<binding_options>' represents a parameter and encodes its
// kind:
//
//   `_` -- positional-only parameter
//   `p` -- positional-or-keyword parameter
//   `P` -- variadic-positional (*args)
//   `k` -- keyword-only, no default
//   `d` -- keyword-only, with default value
//   `K` -- variadic-keyword (**kwargs)
//   `H` -- non-deterministic input
//
// Boxing options are structured as follows:
//
//   (boxing_fn_name_i;){0,9}[0-9]+
//
// Where `boxing_fn_name_i` is the name of a boxing function (up to 9) within
// the `koladata.types.py_boxing` module. Digits represent parameters and encode
// a boxing policy for each parameter. `0` represents the default boxing policy,
// which is `as_qvalue_or_expr`, and trailing `0`s can be omitted.
//
// Example:
//
//   Given an operator declared as:
//
//       @as_py_function_operator(
//         ...
//         custom_boxing_fn_name_per_parameter=dict(
//             fn=WITH_PY_FUNCTION_TO_PY_OBJECT
//         ),
//     )
//     def apply_py(fn, *args, return_type_as=arolla.unspecified(), **kwargs):
//       ...
//
//  The resulting `aux_options` (excluding the common prefix) would be:
//
//     ┌ First parameter is positional-or-keyword
//     │┌ Second paramter is variadic-positional
//     ││┌ Third parameter is keyword-only, with default
//     │││┌ Fourth parameter is variadic-keyword
//     ││││
//
//     pPdK:as_qvalue_or_expr_with_py_function_to_py_object_support;1
//
//          ────────────────────────┬────────────────────────────── │
//                                  │                               │
//          Name of the first custom boxing function                │
//                                                                  │
//                   First parameter uses the first boxing function ┘
//                   (Remaining parameters use the default boxing)
//
#ifndef THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_
#define THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_

#include "absl/strings/string_view.h"

namespace koladata::python {

inline constexpr absl::string_view kUnifiedPolicy =
    "koladata_unified_binding_policy";

inline constexpr char kUnifiedPolicyOptPositionalOnly = '_';
inline constexpr char kUnifiedPolicyOptPositionalOrKeyword = 'p';
inline constexpr char kUnifiedPolicyOptVarPositional = 'P';
inline constexpr char kUnifiedPolicyOptRequiredKeywordOnly = 'k';
inline constexpr char kUnifiedPolicyOptOptionalKeywordOnly = 'd';
inline constexpr char kUnifiedPolicyOptVarKeyword = 'K';
inline constexpr char kUnifiedPolicyOptNonDeterministic = 'H';

// Registers the unified binding policy. If the function fails, it returns
// `false` and sets a Python exception.
bool RegisterUnifiedBindingPolicy();

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_OPERATORS_PY_OPTOOLS_H_
