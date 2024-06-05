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
#include "koladata/internal/expr_quote_utils.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/util/init_arolla.h"

namespace koladata::internal {
namespace {

TEST(ExprQuoteUtils, StableFingerprint) {
  ASSERT_OK(arolla::InitArolla());
  ASSERT_OK_AND_ASSIGN(
      auto expr_1,
      arolla::expr::CallOp("math.add",
                           {arolla::expr::Leaf("x"),
                            arolla::expr::Literal(0)}));
  ASSERT_OK_AND_ASSIGN(
      auto expr_2,
      arolla::expr::CallOp("math.add",
                           {arolla::expr::Leaf("y"),
                            arolla::expr::Literal(0)}));

  EXPECT_EQ(StableFingerprint(arolla::expr::ExprQuote(expr_1)),
            StableFingerprint(arolla::expr::ExprQuote(expr_1)));

  EXPECT_NE(StableFingerprint(arolla::expr::ExprQuote(expr_1)),
            StableFingerprint(arolla::expr::ExprQuote(expr_2)));
}

TEST(ExprQuoteUtils, StableFingerprint_MissingExpr_Error) {
  EXPECT_EQ(StableFingerprint(arolla::expr::ExprQuote(nullptr)),
            "<uninitialized ExprQuote>");
}

TEST(ExprQuoteUtils, StableFingerprint_SerializationFailed_Error) {
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<arolla::expr::testing::DummyOp>(
          "op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_OK_AND_ASSIGN(auto expr,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("x")}));
  // Missing codec for DummyOp.
  EXPECT_EQ(StableFingerprint(arolla::expr::ExprQuote(expr)),
            "<unsupported ExprQuote>");
}

TEST(ExprQuoteUtils, ExprQuoteDebugString) {
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<arolla::expr::testing::DummyOp>(
          "op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_OK_AND_ASSIGN(auto expr_1,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("x")}));
  ASSERT_OK_AND_ASSIGN(auto expr_2,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("y")}));

  EXPECT_EQ(ExprQuoteDebugString(arolla::expr::ExprQuote(expr_1)), "op(L.x)");
  EXPECT_EQ(ExprQuoteDebugString(arolla::expr::ExprQuote()),
            "<uninitialized ExprQuote>");
}

}  // namespace
}  // namespace koladata::internal
