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
#include "koladata/expr/non_determinism.h"

#include <cstdint>
#include <memory>

#include "absl/base/no_destructor.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/registered_expr_operator.h"
#include "koladata/internal/pseudo_random.h"

namespace koladata::expr {

using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::RegisteredOperator;

absl::StatusOr<arolla::expr::ExprNodePtr> GenNonDeterministicToken() {
  static const absl::NoDestructor op(
      std::make_shared<RegisteredOperator>("koda_internal.non_deterministic"));
  static const absl::NoDestructor leaf(Leaf(kNonDeterministicTokenLeafKey));
  return MakeOpNode(
      *op,
      {*leaf, Literal(static_cast<int64_t>(internal::PseudoRandomUint64()))});
}

}  // namespace koladata::expr
