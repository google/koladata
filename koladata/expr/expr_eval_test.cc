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
// Most of the logic is tested in Python in expr_eval_test.py.
// This is a basic test to make sure that the C++ API is working.

#include "koladata/expr/expr_eval.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/testing/testing.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/status.h"
#include "arolla/util/testing/status_matchers.h"
#include "arolla/util/text.h"

namespace koladata::expr {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::SourceLocationPayload;
using ::arolla::Text;
using ::arolla::expr::CallOp;
using ::arolla::expr::Literal;
using ::arolla::expr::Placeholder;
using ::arolla::expr::VerboseRuntimeError;
using ::arolla::testing::CausedBy;
using ::arolla::testing::PayloadIs;
using ::arolla::testing::WithSourceLocationAnnotation;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsSupersetOf;
using ::testing::Not;

TEST(ExprEvalTest, Basic) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("I")), Literal(Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  ASSERT_OK_AND_ASSIGN(
      auto result,
      EvalExprWithCompilationCache(expr, {{"foo", foo_value.AsRef()}}, {}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(1));
}

TEST(ExprEvalTest, Variables) {
  ASSERT_OK_AND_ASSIGN(
      auto expr, CallOp("math.subtract",
                        {CallOp("koda_internal.input",
                                {Literal(Text("I")), Literal(Text("foo"))}),
                         CallOp("koda_internal.input",
                                {Literal(Text("V")), Literal(Text("foo"))})}));
  auto i_foo_value = arolla::TypedValue::FromValue(1);
  auto v_foo_value = arolla::TypedValue::FromValue(2);
  ASSERT_OK_AND_ASSIGN(auto result, EvalExprWithCompilationCache(
                                        expr, {{"foo", i_foo_value.AsRef()}},
                                        {{"foo", v_foo_value.AsRef()}}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(-1));
}

TEST(ExprEvalTest, MissingInput) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("I")), Literal(Text("foo"))}));
  EXPECT_THAT(EvalExprWithCompilationCache(expr, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "kd.eval() has missing inputs for: [I.foo]"));
}

TEST(ExprEvalTest, DuplicateInput) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("I")), Literal(Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(
          expr, {{"foo", foo_value.AsRef()}, {"foo", foo_value.AsRef()}}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "input [foo] passed twice to kd.eval()"));
}

TEST(ExprEvalTest, MissingVariable) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("V")), Literal(Text("foo"))}));
  EXPECT_THAT(EvalExprWithCompilationCache(expr, {}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "kd.eval() has missing inputs for: [V.foo]"));
}

TEST(ExprEvalTest, DuplicateVariable) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("V")), Literal(Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(
          expr, {}, {{"foo", foo_value.AsRef()}, {"foo", foo_value.AsRef()}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "variable [foo] passed twice to kd.eval()"));
}

TEST(ExprEvalTest, UnknownContainer) {
  ASSERT_OK_AND_ASSIGN(auto expr,
                       CallOp("koda_internal.input",
                              {Literal(Text("Z")), Literal(Text("foo"))}));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalExprWithCompilationCache(expr, {{"foo", foo_value.AsRef()}}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "unknown input container: [Z]"));
}

TEST(ExprEvalTest, ErrorPropagation) {
  ASSERT_OK_AND_ASSIGN(
      auto x_expr,
      CallOp("koda_internal.input", {Literal(Text("I")), Literal(Text("x"))}));
  ASSERT_OK_AND_ASSIGN(
      auto y_expr,
      CallOp("koda_internal.input", {Literal(Text("I")), Literal(Text("y"))}));
  ASSERT_OK_AND_ASSIGN(auto floordiv_expr,
                       CallOp("math.floordiv", {x_expr, y_expr}));
  ASSERT_OK_AND_ASSIGN(
      auto expr, WithSourceLocationAnnotation(floordiv_expr, "foo", "bar.py",
                                              57, 0, "  return x // y"));

  auto x_value = arolla::TypedValue::FromValue(int64_t{1});
  auto y_value = arolla::TypedValue::FromValue(int64_t{0});

  EXPECT_THAT(EvalExprWithCompilationCache(
                  expr, {{"x", x_value.AsRef()}, {"y", y_value.AsRef()}}, {}),
              AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                             HasSubstr("bar.py:57, in foo\n  return x // y")),
                    PayloadIs<SourceLocationPayload>(),
                    CausedBy(AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                            "division by zero"),
                                   Not(PayloadIs<VerboseRuntimeError>())))));
}

TEST(GetExprVariablesTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {CallOp("koda_internal.input",
                     {Literal(Text("V")), Literal(Text("foo"))}),
              CallOp("math.add",
                     {CallOp("koda_internal.input",
                             {Literal(Text("V")), Literal(Text("bar"))}),
                      CallOp("koda_internal.input",
                             {Literal(Text("I")), Literal(Text("baz"))})})}));
  EXPECT_THAT(GetExprVariables(expr), IsOkAndHolds(ElementsAre("bar", "foo")));
}

TEST(GetExprInputsTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto expr,
      CallOp("math.add",
             {CallOp("koda_internal.input",
                     {Literal(Text("I")), Literal(Text("foo"))}),
              CallOp("math.add",
                     {CallOp("koda_internal.input",
                             {Literal(Text("I")), Literal(Text("bar"))}),
                      CallOp("koda_internal.input",
                             {Literal(Text("V")), Literal(Text("baz"))})})}));
  EXPECT_THAT(GetExprInputs(expr), IsOkAndHolds(ElementsAre("bar", "foo")));
}

TEST(OpEvalTest, Basic) {
  ASSERT_OK_AND_ASSIGN(
      auto op, arolla::expr::MakeLambdaOperator(
                   CallOp("math.add", {Placeholder("x"), Placeholder("x")})));
  auto foo_value = arolla::TypedValue::FromValue(1);
  ASSERT_OK_AND_ASSIGN(auto result,
                       EvalOpWithCompilationCache(op, {foo_value.AsRef()}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(2));
}

TEST(OpEvalTest, DifferentTypes) {
  ASSERT_OK_AND_ASSIGN(
      auto op, arolla::expr::MakeLambdaOperator(
                   CallOp("math.add", {Placeholder("x"), Placeholder("x")})));
  auto int_value = arolla::TypedValue::FromValue(1);
  auto float_value = arolla::TypedValue::FromValue(1.0);
  ASSERT_OK_AND_ASSIGN(auto int_result,
                       EvalOpWithCompilationCache(op, {int_value.AsRef()}));
  EXPECT_THAT(int_result.As<int32_t>(), IsOkAndHolds(2));
  ASSERT_OK_AND_ASSIGN(auto float_result,
                       EvalOpWithCompilationCache(op, {float_value.AsRef()}));
  EXPECT_THAT(float_result.As<double>(), IsOkAndHolds(2.0));
}

TEST(OpEvalTest, WrongNumberOfArgs) {
  ASSERT_OK_AND_ASSIGN(
      auto op, arolla::expr::MakeLambdaOperator(
                   CallOp("math.add", {Placeholder("x"), Placeholder("x")})));
  auto foo_value = arolla::TypedValue::FromValue(1);
  EXPECT_THAT(
      EvalOpWithCompilationCache(op, {foo_value.AsRef(), foo_value.AsRef()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("too many positional arguments")));
}

TEST(OpEvalTest, ErrorPropagation) {
  ASSERT_OK_AND_ASSIGN(
      auto floordiv_expr,
      CallOp("math.floordiv", {Placeholder("x"), Placeholder("y")}));
  ASSERT_OK_AND_ASSIGN(
      auto annotated_floordiv_expr,
      WithSourceLocationAnnotation(floordiv_expr, "foo", "bar.py", 57, 0,
                                   "  return x // y"));
  ASSERT_OK_AND_ASSIGN(
      auto op,
      arolla::expr::MakeLambdaOperator(
          "my_lambda", arolla::expr::ExprOperatorSignature{{"x"}, {"y"}},
          annotated_floordiv_expr));

  auto x_value = arolla::TypedValue::FromValue(int64_t{1});
  auto y_value = arolla::TypedValue::FromValue(int64_t{0});
  EXPECT_THAT(
      EvalOpWithCompilationCache(op, {x_value.AsRef(), y_value.AsRef()}),
      AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                     HasSubstr("bar.py:57, in foo\n  return x // y")),
            PayloadIs<SourceLocationPayload>(),
            CausedBy(AllOf(StatusIs(absl::StatusCode::kInvalidArgument,
                                    "division by zero"),
                           Not(PayloadIs<VerboseRuntimeError>())))));
}

}  // namespace
}  // namespace koladata::expr
