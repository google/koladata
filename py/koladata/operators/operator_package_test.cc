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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/init_arolla.h"

namespace {

// Test that aliases are equivalent to the canonical operator after
// package serialization.
TEST(OperatorPackageTest, AliasesAreEquivalent) {
  arolla::InitArolla();
  ASSERT_OK_AND_ASSIGN(auto functor_for,
                       arolla::expr::LookupOperator("kd.functor.for_"));
  ASSERT_OK_AND_ASSIGN(auto functor_for_decay,
                       arolla::expr::DecayRegisteredOperator(functor_for));

  ASSERT_OK_AND_ASSIGN(auto for_op, arolla::expr::LookupOperator("kd.for_"));
  ASSERT_OK_AND_ASSIGN(auto for_decay,
                       arolla::expr::DecayRegisteredOperator(for_op));
  ASSERT_EQ(functor_for_decay->fingerprint(), for_decay->fingerprint());
}

}  // namespace
