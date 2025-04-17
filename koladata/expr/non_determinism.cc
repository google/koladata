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
#include "koladata/expr/non_determinism.h"

#include <cstdint>
#include <limits>
#include <memory>

#include "absl/base/no_destructor.h"
#include "absl/random/random.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/registered_expr_operator.h"

namespace koladata::expr {

using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::RegisteredOperator;

absl::StatusOr<arolla::expr::ExprNodePtr> GenNonDeterministicToken() {
  static const absl::NoDestructor op(
      std::make_shared<RegisteredOperator>("koda_internal.non_deterministic"));
  static const absl::NoDestructor leaf(Leaf(kNonDeterministicTokenLeafKey));
  thread_local absl::BitGen bitgen;
  auto seed = absl::Uniform<int64_t>(absl::IntervalClosed, bitgen, 0,
                                     std::numeric_limits<int64_t>::max());
  return MakeOpNode(*op, {*leaf, Literal(seed)});
}

}  // namespace koladata::expr
