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
// Most of the logic is tested in Python in expr_eval_test.py.
// This is a basic test to make sure that the C++ API is working.

#include "koladata/expr/expr_eval.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"

namespace koladata::expr {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

TEST(ExprEvalTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("I")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  ASSERT_OK_AND_ASSIGN(
      auto result,
      EvalExprWithCompilationCache(expr, {{"foo", foo_value.AsRef()}}, {}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(1));
}

TEST(ExprEvalTest, Variables) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp(
          "math.subtract",
          {arolla::expr::CallOp("koda_internal.input",
                                {arolla::expr::Literal(arolla::Text("I")),
                                 arolla::expr::Literal(arolla::Text("foo"))}),
           arolla::expr::CallOp(
               "koda_internal.input",
               {arolla::expr::Literal(arolla::Text("V")),
                arolla::expr::Literal(arolla::Text("foo"))})}));
  auto i_foo_value = arolla::TypedValue::FromValue(1);
  auto v_foo_value = arolla::TypedValue::FromValue(2);
  ASSERT_OK_AND_ASSIGN(auto result, EvalExprWithCompilationCache(
                                        expr, {{"foo", i_foo_value.AsRef()}},
                                        {{"foo", v_foo_value.AsRef()}}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(-1));
}

TEST(ExprEvalTest, MissingInput) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("I")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  EXPECT_THAT(EvalExprWithCompilationCache(expr, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "kd.eval() has missing inputs for: [I.foo]"));
}

TEST(ExprEvalTest, DuplicateInput) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("I")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(
          expr, {{"foo", foo_value.AsRef()}, {"foo", foo_value.AsRef()}}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "input [foo] passed twice to kd.eval()"));
}

TEST(ExprEvalTest, MissingVariable) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("V")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  EXPECT_THAT(EvalExprWithCompilationCache(expr, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "kd.eval() has missing inputs for: [V.foo]"));
}

TEST(ExprEvalTest, DuplicateVariable) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("V")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(
          expr, {}, {{"foo", foo_value.AsRef()}, {"foo", foo_value.AsRef()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "variable [foo] passed twice to kd.eval()"));
}

TEST(ExprEvalTest, UnknownContainer) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp("koda_internal.input",
                           {arolla::expr::Literal(arolla::Text("Z")),
                            arolla::expr::Literal(arolla::Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(expr, {{"foo", foo_value.AsRef()}}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "unknown input container: [Z]"));
}

TEST(GetExprVariablesTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp(
          "math.add",
          {arolla::expr::CallOp("koda_internal.input",
                                {arolla::expr::Literal(arolla::Text("V")),
                                 arolla::expr::Literal(arolla::Text("foo"))}),
           arolla::expr::CallOp(
               "math.add",
               {arolla::expr::CallOp(
                    "koda_internal.input",
                    {arolla::expr::Literal(arolla::Text("V")),
                     arolla::expr::Literal(arolla::Text("bar"))}),
                arolla::expr::CallOp(
                    "koda_internal.input",
                    {arolla::expr::Literal(arolla::Text("I")),
                     arolla::expr::Literal(arolla::Text("baz"))})})}));
  EXPECT_THAT(GetExprVariables(expr), IsOkAndHolds(ElementsAre("bar", "foo")));
}

TEST(GetExprInputsTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      arolla::expr::CallOp(
          "math.add",
          {arolla::expr::CallOp("koda_internal.input",
                                {arolla::expr::Literal(arolla::Text("I")),
                                 arolla::expr::Literal(arolla::Text("foo"))}),
           arolla::expr::CallOp(
               "math.add",
               {arolla::expr::CallOp(
                    "koda_internal.input",
                    {arolla::expr::Literal(arolla::Text("I")),
                     arolla::expr::Literal(arolla::Text("bar"))}),
                arolla::expr::CallOp(
                    "koda_internal.input",
                    {arolla::expr::Literal(arolla::Text("V")),
                     arolla::expr::Literal(arolla::Text("baz"))})})}));
  EXPECT_THAT(GetExprInputs(expr), IsOkAndHolds(ElementsAre("bar", "foo")));
}

}  // namespace

}  // namespace koladata::expr
