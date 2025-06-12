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
#include "koladata/functor/parallel/transform.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/create_execution_context.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::TypedValueWith;
using ::koladata::testing::IsEquivalentTo;

// Most of the tests are in koda_internal_parallel_transform_test.py, this
// is a basic sanity check only.
TEST(TransformTest, Basic) {
  ExecutorPtr executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(
      auto missing_object,
      DataSlice::Create(internal::DataItem(),
                        internal::DataItem(schema::kObject), DataBag::Empty()));
  ASSERT_OK_AND_ASSIGN(auto context,
                       CreateExecutionContext(executor, missing_object));
  ASSERT_OK_AND_ASSIGN(expr::InputContainer input_container,
                       expr::InputContainer::Create("I"));
  ASSERT_OK_AND_ASSIGN(auto returns_expr,
                       arolla::expr::CallOp("kd.add",
                                            {input_container.CreateInput("a"),
                                             input_container.CreateInput("b")},
                                            {}));
  DataSlice returns =
      DataSlice::CreateFromScalar(arolla::expr::ExprQuote(returns_expr));
  ASSERT_OK_AND_ASSIGN(DataSlice functor,
                       CreateFunctor(returns, missing_object, {}, {}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice transformed_functor,
      TransformToParallel(context, functor, internal::NonDeterministicToken()));
  auto [future_a, writer_a] = MakeFuture(arolla::GetQType<DataSlice>());
  std::move(writer_a).SetValue(
      arolla::TypedValue::FromValue(DataSlice::CreateFromScalar(1)));
  auto [future_b, writer_b] = MakeFuture(arolla::GetQType<DataSlice>());
  std::move(writer_b).SetValue(
      arolla::TypedValue::FromValue(DataSlice::CreateFromScalar(2)));
  auto future_a_value = MakeFutureQValue(future_a);
  auto future_b_value = MakeFutureQValue(future_b);
  ASSERT_OK_AND_ASSIGN(
      auto result,
      CallFunctorWithCompilationCache(
          transformed_functor, {future_a_value.AsRef(), future_b_value.AsRef()},
          {"a", "b"}));
  ASSERT_OK_AND_ASSIGN(FuturePtr result_value, result.As<FuturePtr>());
  EXPECT_THAT(result_value->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(3)))));
}

}  // namespace
}  // namespace koladata::functor::parallel
