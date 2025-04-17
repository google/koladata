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
#ifndef KOLADATA_EXPR_NON_DETERMINISM_H_
#define KOLADATA_EXPR_NON_DETERMINISM_H_

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"

namespace koladata::expr {

// Name of the leaf used to pass a non-deterministic value to the expression.
// This leaf is not visible in the expression and is not passed to "eval".
constexpr absl::string_view kNonDeterministicTokenLeafKey =
    "_koladata_non_deterministic_token_leaf";

// Name of the hidden parameter used to indicate non-deterministic input.
constexpr absl::string_view kNonDeterministicParamName =
    "_non_deterministic_token";

// Returns a non-deterministic token.
absl::StatusOr<arolla::expr::ExprNodePtr> GenNonDeterministicToken();

}  // namespace koladata::expr

#endif  // KOLADATA_EXPR_NON_DETERMINISM_H_
