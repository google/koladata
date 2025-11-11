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
#include "koladata/functor/parallel/parallel_call_utils.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/signature.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::testing::HasSubstr;

absl::StatusOr<arolla::expr::ExprNodePtr> CreateInput(absl::string_view name) {
  return arolla::expr::CallOp("koda_internal.input",
                              {arolla::expr::Literal(arolla::Text("I")),
                               arolla::expr::Literal(arolla::Text(name))});
}

absl::StatusOr<DataSlice> WrapExpr(
    absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error) {
  ASSIGN_OR_RETURN(auto expr, expr_or_error);
  return DataSlice::CreatePrimitive(arolla::expr::ExprQuote(std::move(expr)));
}

// Returns a functor: lambda x, y: kd.tuple(y, x)
absl::StatusOr<DataSlice> CreateSwapFunctor() {
  ASSIGN_OR_RETURN(
      auto signature,
      Signature::Create(
          {{.name = "x",
            .kind = Signature::Parameter::Kind::kPositionalOrKeyword},
           {.name = "y",
            .kind = Signature::Parameter::Kind::kPositionalOrKeyword}}));
  ASSIGN_OR_RETURN(auto koda_signature, CppSignatureToKodaSignature(signature));
  ASSIGN_OR_RETURN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp("core.make_tuple",
                                    {CreateInput("y"), CreateInput("x")})));
  return CreateFunctor(returns_expr, koda_signature, {}, {});
}
// Returns a functor: lambda fn, x, y: fn(x, y)
absl::StatusOr<DataSlice> CreateCallFunctor(arolla::TypedValue return_type_as) {
  ASSIGN_OR_RETURN(
      auto signature,
      Signature::Create(
          {{.name = "fn",
            .kind = Signature::Parameter::Kind::kPositionalOrKeyword},
           {.name = "x",
            .kind = Signature::Parameter::Kind::kPositionalOrKeyword},
           {.name = "y",
            .kind = Signature::Parameter::Kind::kPositionalOrKeyword}}));
  ASSIGN_OR_RETURN(auto koda_signature, CppSignatureToKodaSignature(signature));
  ASSIGN_OR_RETURN(
      auto returns_expr,
      WrapExpr(arolla::expr::CallOp(
          "kd.call",
          {CreateInput("fn"),
           /*args=*/
           arolla::expr::CallOp("core.make_tuple",
                                {CreateInput("x"), CreateInput("y")}),
           /*return_type_as=*/arolla::expr::Literal(std::move(return_type_as)),
           /*kwargs=*/arolla::expr::Literal(arolla::MakeEmptyNamedTuple()),
           arolla::expr::Literal(internal::NonDeterministicTokenValue())})));
  return CreateFunctor(returns_expr, koda_signature, {}, {});
}

TEST(ParallelCallUtilsTest, AsParallel) {
  ASSERT_OK_AND_ASSIGN(auto future, AsParallel(arolla::TypedRef::FromValue(1)));
  ASSERT_EQ(future.GetType(), GetFutureQType(arolla::GetQType<int>()));
  bool called = false;
  future.UnsafeAs<FuturePtr>()->AddConsumer(
      [&](absl::StatusOr<arolla::TypedValue> value) {
        ASSERT_THAT(value, IsOkAndHolds(QValueWith<int>(1)));
        called = true;
      });
  ASSERT_TRUE(called);
}

TEST(ParallelCallUtilsTest, TransformToParallelCallFn) {
  ASSERT_OK_AND_ASSIGN(auto functor, CreateSwapFunctor());
  ASSERT_OK_AND_ASSIGN(auto config, GetDefaultParallelTransformConfig());
  ASSERT_OK_AND_ASSIGN(auto parallel_call_fn,
                       TransformToParallelCallFn(config, functor));
  auto executor = GetEagerExecutor();
  auto [future_x, writer_x] = MakeFuture(arolla::GetQType<int>());
  auto [future_y, writer_y] = MakeFuture(arolla::GetQType<int>());
  std::move(writer_x).SetValue(arolla::TypedValue::FromValue(34));
  std::move(writer_y).SetValue(arolla::TypedValue::FromValue(17));
  ASSERT_OK_AND_ASSIGN(auto result,
                       parallel_call_fn({arolla::TypedRef::FromValue(executor),
                                         MakeFutureQValue(future_x).AsRef(),
                                         MakeFutureQValue(future_y).AsRef()},
                                        {"x", "y"}));
  ASSERT_EQ(result.GetType(),
            arolla::MakeTupleQType({GetFutureQType(arolla::GetQType<int>()),
                                    GetFutureQType(arolla::GetQType<int>())}));
  ASSERT_THAT(result.GetField(0).UnsafeAs<FuturePtr>()->GetValueForTesting(),
              IsOkAndHolds(QValueWith<int>(17)));
  ASSERT_THAT(result.GetField(1).UnsafeAs<FuturePtr>()->GetValueForTesting(),
              IsOkAndHolds(QValueWith<int>(34)));
}

TEST(ParallelCallUtilsTest, TransformToSimpleParallelCallFn) {
  ASSERT_OK_AND_ASSIGN(auto functor, CreateSwapFunctor());
  ASSERT_OK_AND_ASSIGN(auto simple_parallel_call_fn,
                       TransformToSimpleParallelCallFn(
                           functor, /*allow_runtime_transforms=*/false));
  CurrentExecutorScopeGuard executor_guard(GetEagerExecutor());
  ASSERT_OK_AND_ASSIGN(
      auto result, simple_parallel_call_fn({arolla::TypedRef::FromValue(20),
                                            arolla::TypedRef::FromValue(10)},
                                           {}));
  ASSERT_EQ(result.GetType(),
            GetFutureQType(arolla::MakeTupleQType(
                {arolla::GetQType<int>(), arolla::GetQType<int>()})));
  ASSERT_OK_AND_ASSIGN(auto tuple,
                       result.UnsafeAs<FuturePtr>()->GetValueForTesting());
  ASSERT_THAT(tuple.GetField(0), QValueWith<int>(10));
  ASSERT_THAT(tuple.GetField(1), QValueWith<int>(20));
}

TEST(ParallelCallUtilsTest,
     TransformToSimpleParallelCallFnAllowRuntimeTransforms) {
  ASSERT_OK_AND_ASSIGN(auto functor, CreateCallFunctor(arolla::MakeTuple(
                                         {arolla::TypedValue::FromValue(-1),
                                          arolla::TypedValue::FromValue(-1)})));
  ASSERT_OK_AND_ASSIGN(auto simple_parallel_call_fn,
                       TransformToSimpleParallelCallFn(
                           functor, /*allow_runtime_transforms=*/true));
  auto executor = GetEagerExecutor();
  CurrentExecutorScopeGuard executor_guard(GetEagerExecutor());
  ASSERT_OK_AND_ASSIGN(auto swap_functor, CreateSwapFunctor());
  ASSERT_OK_AND_ASSIGN(
      auto result,
      simple_parallel_call_fn(
          {arolla::TypedRef::FromValue(swap_functor),
           arolla::TypedRef::FromValue(34), arolla::TypedRef::FromValue(17)},
          {}));
  ASSERT_EQ(result.GetType(),
            GetFutureQType(arolla::MakeTupleQType(
                {arolla::GetQType<int>(), arolla::GetQType<int>()})));
  ASSERT_OK_AND_ASSIGN(auto tuple,
                       result.UnsafeAs<FuturePtr>()->GetValueForTesting());
  ASSERT_THAT(tuple.GetField(0), QValueWith<int>(17));
  ASSERT_THAT(tuple.GetField(1), QValueWith<int>(34));

  ASSERT_THAT(
      TransformToSimpleParallelCallFn(functor,
                                      /*allow_runtime_transforms=*/false),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("you can pass allow_runtime_transforms=True")));
}

}  // namespace
}  // namespace koladata::functor::parallel
