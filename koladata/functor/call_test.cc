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
// This file contains only basic tests, more comprehensive tests are in Python.

#include "koladata/functor/call.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/testing/matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation_context.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;

absl::StatusOr<arolla::expr::ExprNodePtr> CreateInput(absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("I")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<arolla::expr::ExprNodePtr> CreateVariable(
    absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("V")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<DataSlice> WrapExpr(
    absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error) {
  ASSIGN_OR_RETURN(auto expr, expr_or_error);
  return DataSlice::Create(
      internal::DataItem(arolla::expr::ExprQuote(std::move(expr))),
      internal::DataItem(schema::kExpr));
}

TEST(CallTest, VariableRhombus) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  Signature::Parameter p3 = {
      .name = "c",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2, p3}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp("math.multiply",
                                    {CreateInput("a"), CreateVariable("a")})));
  ASSERT_OK_AND_ASSIGN(
      auto var_a_expr,
      WrapExpr(arolla::expr::CallOp(
          "math.add", {CreateVariable("b"), CreateVariable("c")})));
  ASSERT_OK_AND_ASSIGN(
      auto var_b_expr,
      WrapExpr(arolla::expr::CallOp(
          "math.add", {CreateVariable("d"), arolla::expr::Literal(5)})));
  ASSERT_OK_AND_ASSIGN(
      auto var_c_expr,
      WrapExpr(arolla::expr::CallOp("math.add",
                                    {CreateVariable("d"), CreateInput("c")})));
  ASSERT_OK_AND_ASSIGN(auto var_d_expr, WrapExpr(CreateInput("b")));
  ASSERT_OK_AND_ASSIGN(auto fn, CreateFunctor(returns_expr, koda_signature,
                                              {{"d", var_d_expr},
                                               {"a", var_a_expr},
                                               {"c", var_c_expr},
                                               {"b", var_b_expr}}));
  std::vector<arolla::TypedValue> inputs = {
      arolla::TypedValue::FromValue(2),
      arolla::TypedValue::FromValue(3),
      arolla::TypedValue::FromValue(4),
  };
  ASSERT_OK_AND_ASSIGN(auto result, CallFunctorWithCompilationCache(
                                        fn,
                                        /*args=*/
                                        {
                                            inputs[0].AsRef(),
                                            inputs[1].AsRef(),
                                            inputs[2].AsRef(),
                                        },
                                        /*kwargs=*/{}, /*eval_options=*/{}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(2 * ((3 + 5) + (3 + 4))));

  ASSERT_OK_AND_ASSIGN(
      result,
      CallFunctorWithCompilationCache(
          fn,
          /*args=*/
          {
              inputs[0].AsRef(),
          },
          /*kwargs=*/{{"c", inputs[1].AsRef()}, {"b", inputs[2].AsRef()}},
          /*eval_options=*/{}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(2 * ((4 + 5) + (4 + 3))));
}

TEST(CallTest, VariableCycle) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, WrapExpr(CreateVariable("b")));
  ASSERT_OK_AND_ASSIGN(auto var_b_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature,
                                     {{"a", var_a_expr}, {"b", var_b_expr}}));
  EXPECT_THAT(CallFunctorWithCompilationCache(fn, /*args=*/{}, /*kwargs=*/{},
                                              /*eval_options=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "variable [a] has a dependency cycle"));
}

TEST(CallTest, JustLiteral) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}));
  ASSERT_OK_AND_ASSIGN(
      auto result, CallFunctorWithCompilationCache(
                       fn, /*args=*/{}, /*kwargs=*/{}, /*eval_options=*/{}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(57));
}

TEST(CallTest, MustBeScalar) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}));
  ASSERT_OK_AND_ASSIGN(fn, fn.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  EXPECT_THAT(CallFunctorWithCompilationCache(fn, /*args=*/{}, /*kwargs=*/{},
                                              /*eval_options=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the first argument of kd.call must be a functor"));
}

TEST(CallTest, NoBag) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}));
  EXPECT_THAT(
      CallFunctorWithCompilationCache(fn.WithBag(nullptr), /*args=*/{},
                                      /*kwargs=*/{}, /*eval_options=*/{}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "the first argument of kd.call must be a functor"));
}

TEST(CallTest, DataSliceVariable) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  ASSERT_OK_AND_ASSIGN(auto var_a,
                       DataSlice::Create(internal::DataItem(57),
                                         internal::DataItem(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(
      auto fn, CreateFunctor(returns_expr, koda_signature, {{"a", var_a}}));
  ASSERT_OK_AND_ASSIGN(auto result, CallFunctorWithCompilationCache(
                                        fn, /*args=*/{},
                                        /*kwargs=*/{}, /*eval_options=*/{}));
  EXPECT_THAT(result.As<DataSlice>(),
              IsOkAndHolds(IsEquivalentTo(var_a.WithBag(fn.GetBag()))));
}

TEST(CallTest, EvalError) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("foo")));
  ASSERT_OK_AND_ASSIGN(
      auto var_expr,
      WrapExpr(arolla::expr::CallOp(
          "math.add", {CreateInput("a"), arolla::expr::Literal(57)})));
  ASSERT_OK_AND_ASSIGN(auto fn, CreateFunctor(returns_expr, koda_signature,
                                              {{"foo", var_expr}}));
  ASSERT_OK_AND_ASSIGN(auto input,
                       DataSlice::Create(internal::DataItem(43),
                                         internal::DataItem(schema::kInt32)));
  // This error message should be improved, in particular it should actually
  // mention that we are evaluating a functor, which variable, etc.
  // It is OK to only improve this on the Python side, the C++ error is not
  // so important.
  EXPECT_THAT(
      CallFunctorWithCompilationCache(
          fn,
          /*args=*/{arolla::TypedRef::FromValue(input)},
          /*kwargs=*/{}, /*eval_options=*/{}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected numerics, got x: DATA_SLICE; while calling math.add "
               "with args {annotation.qtype(L['I.a'], DATA_SLICE), 57}; while "
               "transforming M.math.add(L['I.a'], 57); while compiling the "
               "expression"));
}

TEST(CallTest, Cancellation) {
  ASSERT_OK_AND_ASSIGN(
      auto signature,
      Signature::Create({
          {.name = "a",
           .kind = Signature::Parameter::Kind::kPositionalOrKeyword},
      }));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  // returns_expr = I.x + I.x + I.x
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp(
          "math.add", {arolla::expr::CallOp(
                           "math.add", {CreateInput("a"), CreateInput("a")}),
                       CreateInput("a")})));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}));

  {
    int op_count = 2;  // Stop after the second operator.
    auto cancel_ctx = arolla::CancellationContext::Make(
        /*no cooldown*/ absl::Nanoseconds(-1),
        /*no countdown*/ -1, [&op_count] {
          return --op_count > 0 ? absl::OkStatus() : absl::CancelledError("");
        });
    expr::EvalOptions eval_options{.cancellation_context = cancel_ctx.get()};
    EXPECT_THAT(CallFunctorWithCompilationCache(
                    fn, /*args=*/{arolla::TypedRef::FromValue(1)},
                    /*kwargs=*/{}, eval_options),
                StatusIs(absl::StatusCode::kCancelled));
  }
  {
    int op_count = 3;  // Should stop after the third operator;
                       // however, there are only two operators.
    auto cancel_ctx = arolla::CancellationContext::Make(
        /*no cooldown*/ absl::Nanoseconds(-1),
        /*no countdown*/ -1, [&op_count] {
          return --op_count > 0 ? absl::OkStatus() : absl::CancelledError("");
        });
    expr::EvalOptions eval_options{.cancellation_context = cancel_ctx.get()};
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunctorWithCompilationCache(
                             fn, /*args=*/{arolla::TypedRef::FromValue(1)},
                             /*kwargs=*/{}, eval_options));
    EXPECT_THAT(result.As<int>(), IsOkAndHolds(3));
  }
}

}  // namespace

}  // namespace koladata::functor
