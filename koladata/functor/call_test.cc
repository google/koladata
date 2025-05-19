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
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/testing/status_matchers.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_storage.h"
#include "koladata/functor/stack_trace.h"
#include "koladata/object_factories.h"
#include "koladata/operators/core.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CancellationContext;
using ::arolla::testing::PayloadIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::AllOf;
using ::testing::Field;

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
  return test::DataItem(arolla::expr::ExprQuote(std::move(expr)));
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
  ASSERT_OK_AND_ASSIGN(
      auto fn, CreateFunctor(returns_expr, koda_signature, {"d", "a", "c", "b"},
                             {var_d_expr, var_a_expr, var_c_expr, var_b_expr}));
  std::vector<arolla::TypedValue> inputs = {
      arolla::TypedValue::FromValue(2),
      arolla::TypedValue::FromValue(3),
      arolla::TypedValue::FromValue(4),
  };
  ASSERT_OK_AND_ASSIGN(
      auto result,
      CallFunctorWithCompilationCache(
          fn,
          /*args=*/{inputs[0].AsRef(), inputs[1].AsRef(), inputs[2].AsRef()},
          /*kwnames=*/{}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(2 * ((3 + 5) + (3 + 4))));

  ASSERT_OK_AND_ASSIGN(
      result,
      CallFunctorWithCompilationCache(
          fn,
          /*args=*/{inputs[0].AsRef(), inputs[1].AsRef(), inputs[2].AsRef()},
          /*kwnames=*/{"c", "b"}));
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
                       CreateFunctor(returns_expr, koda_signature, {"a", "b"},
                                     {var_a_expr, var_b_expr}));
  EXPECT_THAT(CallFunctorWithCompilationCache(fn, /*args=*/{}, /*kwnames=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "variable [a] has a dependency cycle"));
}

TEST(CallTest, JustLiteral) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto result, CallFunctorWithCompilationCache(
                                        fn, /*args=*/{}, /*kwnames=*/{}));
  EXPECT_THAT(result.As<int32_t>(), IsOkAndHolds(57));
}

TEST(CallTest, MustBeScalar) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));
  ASSERT_OK_AND_ASSIGN(fn, fn.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  EXPECT_THAT(CallFunctorWithCompilationCache(fn, /*args=*/{}, /*kwnames=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the first argument of kd.call must be a functor"));
}

TEST(CallTest, NoBag) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(arolla::expr::Literal(57)));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));
  EXPECT_THAT(CallFunctorWithCompilationCache(fn.WithBag(nullptr), /*args=*/{},
                                              /*kwnames=*/{}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the first argument of kd.call must be a functor"));
}

TEST(CallTest, DataSliceVariable) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto returns_expr, WrapExpr(CreateVariable("a")));
  auto var_a = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto fn, CreateFunctor(returns_expr, koda_signature, {"a"}, {var_a}));
  ASSERT_OK_AND_ASSIGN(auto result,
                       CallFunctorWithCompilationCache(fn, /*args=*/{},
                                                       /*kwnames=*/{}));
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
                                              {"foo"}, {var_expr}));

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice frame_slice,
      ObjectCreator::FromAttrs(
          db, {"function_name", "file_name", "line_number", "line_text"},
          {test::DataItem(arolla::Text("my_func")),
           test::DataItem(arolla::Text("my_file.cc")), test::DataItem(57),
           test::DataItem(arolla::Text("  z = x + y"))}));
  ASSERT_OK_AND_ASSIGN(
      fn, ops::WithAttr(fn, test::DataItem(arolla::Text("_stack_trace_frame")),
                        frame_slice, test::DataItem(false)));

  auto input = test::DataItem(43);
  // This error message should be improved, in particular it should actually
  // mention that we are evaluating a functor, which variable, etc.
  // It is OK to only improve this on the Python side, the C++ error is not
  // so important.
  EXPECT_THAT(
      CallFunctorWithCompilationCache(
          fn,
          /*args=*/{arolla::TypedRef::FromValue(input)},
          /*kwnames=*/{}),
      AllOf(StatusIs(
                absl::StatusCode::kInvalidArgument,
                "expected numerics, got x: DATA_SLICE; while calling math.add "
                "with args {annotation.qtype(L['I.a'], "
                "DATA_SLICE):Attr(qtype=DATA_SLICE), 57}; while transforming "
                "M.math.add(L['I.a'], 57); while compiling the expression\n"
                "\n"
                "my_file.cc:57, in my_func\n"
                "  z = x + y"),
            PayloadIs<StackTraceFrame>(
                AllOf(Field(&StackTraceFrame::function_name, "my_func"),
                      Field(&StackTraceFrame::file_name, "my_file.cc"),
                      Field(&StackTraceFrame::line_number, 57)))));
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
  ASSERT_OK_AND_ASSIGN(auto returns_expr,
                       WrapExpr(arolla::expr::CallOp(
                           "core._identity_with_cancel", {CreateInput("a")})));
  ASSERT_OK_AND_ASSIGN(auto fn,
                       CreateFunctor(returns_expr, koda_signature, {}, {}));
  {  // Without cancellation scope.
    EXPECT_THAT(CallFunctorWithCompilationCache(
                    fn,
                    /*args=*/{arolla::TypedRef::FromValue(1)},
                    /*kwnames=*/{}),
                StatusIs(absl::StatusCode::kFailedPrecondition));
  }
  {  // With cancellation scope.
    CancellationContext::ScopeGuard cancellation_scope;
    EXPECT_THAT(CallFunctorWithCompilationCache(
                    fn, /*args=*/{arolla::TypedRef::FromValue(1)},
                    /*kwnames=*/{}),
                StatusIs(absl::StatusCode::kCancelled));
  }
}

}  // namespace
}  // namespace koladata::functor
