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
#include "koladata/functor/parallel/async_eval.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/barrier.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/std_function_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/asio_executor.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor::parallel {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::GetQType;
using ::arolla::QType;
using ::arolla::Text;
using ::arolla::TypedRef;
using ::arolla::TypedValue;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Literal;
using ::arolla::expr::LookupOperator;
using ::arolla::expr::MakeLambdaOperator;
using ::arolla::expr::Placeholder;
using ::arolla::expr_operators::StdFunctionOperator;
using ::arolla::testing::TypedValueWith;
using ::koladata::testing::IsEquivalentTo;
using ::testing::HasSubstr;

TEST(AsyncEvalTest, Simple) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(auto inner_op, LookupOperator("kd.add"));
  auto x = DataSlice::CreateFromScalar(1);
  auto y = DataSlice::CreateFromScalar(2);
  ASSERT_OK_AND_ASSIGN(
      auto res_future,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  EXPECT_THAT(res_future->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(3)))));
}

TEST(AsyncEvalTest, FutureInputs) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(auto inner_op, LookupOperator("kd.add"));
  auto [x_future, x_writer] = MakeFuture(GetQType<DataSlice>());
  std::move(x_writer).SetValue(
      TypedValue::FromValue(DataSlice::CreateFromScalar(1)));
  auto [y_future, y_writer] = MakeFuture(GetQType<DataSlice>());
  std::move(y_writer).SetValue(
      TypedValue::FromValue(DataSlice::CreateFromScalar(2)));
  auto x_future_value = MakeFutureQValue(x_future);
  auto y_future_value = MakeFutureQValue(y_future);
  ASSERT_OK_AND_ASSIGN(
      auto res_future,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {x_future_value.AsRef(), y_future_value.AsRef()},
          GetQType<DataSlice>()));
  EXPECT_THAT(res_future->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(3)))));
}

TEST(AsyncEvalTest, Nested) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(auto op, LookupOperator("kd.add"));
  auto x = DataSlice::CreateFromScalar(1);
  auto y = DataSlice::CreateFromScalar(2);
  auto z = DataSlice::CreateFromScalar(3);
  ASSERT_OK_AND_ASSIGN(
      auto inner_future,
      AsyncEvalWithCompilationCache(
          executor, op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  auto inner_future_value = MakeFutureQValue(inner_future);
  ASSERT_OK_AND_ASSIGN(
      auto outer_future,
      AsyncEvalWithCompilationCache(
          executor, op, {inner_future_value.AsRef(), TypedRef::FromValue(z)},
          GetQType<DataSlice>()));
  EXPECT_THAT(outer_future->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(6)))));
}

TEST(AsyncEvalTest, ErrorPropagation) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(auto inner_op, LookupOperator("kd.get_attr"));
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto x,
      EntityCreator::FromAttrs(
          db, {"a"}, {DataSlice::CreateFromScalar(1)}, /*schema=*/std::nullopt,
          /*overwrite_schema=*/false, /*itemid=*/std::nullopt));
  auto y = DataSlice::CreateFromScalar(Text("b"));
  ASSERT_OK_AND_ASSIGN(
      auto res_future,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  EXPECT_THAT(res_future->GetValueForTesting(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("failed to get attribute 'b'")));
}

TEST(AsyncEvalTest, NestedErrorPropagation) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(auto inner_op, LookupOperator("kd.get_attr"));
  ASSERT_OK_AND_ASSIGN(
      auto outer_op,
      MakeLambdaOperator(CallOp(
          "kd.add", {Placeholder("x"), Literal(DataSlice::CreateFromScalar(2))},
          {})));

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto x,
      EntityCreator::FromAttrs(
          db, {"a"}, {DataSlice::CreateFromScalar(1)}, /*schema=*/std::nullopt,
          /*overwrite_schema=*/false, /*itemid=*/std::nullopt));
  auto y = DataSlice::CreateFromScalar(Text("b"));

  ASSERT_OK_AND_ASSIGN(
      auto inner_future,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  auto inner_future_value = MakeFutureQValue(inner_future);
  ASSERT_OK_AND_ASSIGN(auto outer_future,
                       AsyncEvalWithCompilationCache(
                           executor, outer_op, {inner_future_value.AsRef()},
                           GetQType<DataSlice>()));
  EXPECT_THAT(outer_future->GetValueForTesting(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("failed to get attribute 'b'")));
}

TEST(AsyncEvalTest, TwoErrors) {
  auto executor = GetEagerExecutor();

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto x,
      EntityCreator::FromAttrs(
          db, {"a"}, {DataSlice::CreateFromScalar(1)}, /*schema=*/std::nullopt,
          /*overwrite_schema=*/false, /*itemid=*/std::nullopt));
  auto y = DataSlice::CreateFromScalar(Text("b"));
  auto z = DataSlice::CreateFromScalar(Text("c"));

  ASSERT_OK_AND_ASSIGN(auto inner_op, LookupOperator("kd.get_attr"));
  ASSERT_OK_AND_ASSIGN(auto outer_op, LookupOperator("kd.add"));
  ASSERT_OK_AND_ASSIGN(
      auto inner_future1,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(z)},
          GetQType<DataSlice>()));
  ASSERT_OK_AND_ASSIGN(
      auto inner_future2,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  auto inner_future1_value = MakeFutureQValue(inner_future1);
  auto inner_future2_value = MakeFutureQValue(inner_future2);
  ASSERT_OK_AND_ASSIGN(auto outer_future, AsyncEvalWithCompilationCache(
                                              executor, outer_op,
                                              {inner_future1_value.AsRef(),
                                               inner_future2_value.AsRef()},
                                              GetQType<DataSlice>()));
  // With the eager executor, we can predict which of the two errors will happen
  // first.
  EXPECT_THAT(outer_future->GetValueForTesting(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("failed to get attribute 'c'")));
}

TEST(AsyncEvalTest, OpReturnsFuture) {
  auto executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(
      auto inner_op,
      MakeLambdaOperator(
          ExprOperatorSignature{{"x"}, {"y"}},
          CallOp("koda_internal.parallel.as_future",
                 {CallOp("kd.add", {Placeholder("x"), Placeholder("y")}, {})},
                 {})));
  auto x = DataSlice::CreateFromScalar(1);
  auto y = DataSlice::CreateFromScalar(2);
  ASSERT_OK_AND_ASSIGN(
      auto res_future,
      AsyncEvalWithCompilationCache(
          executor, inner_op, {TypedRef::FromValue(x), TypedRef::FromValue(y)},
          GetQType<DataSlice>()));
  EXPECT_THAT(res_future->GetValueForTesting(),
              IsOkAndHolds(TypedValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(3)))));
}

TEST(AsyncEvalTest, Multithreaded) {
  absl::BitGen rng;

  // Our computation consists of multiple waves, where each wave
  // depends only on items from previous waves, and we have a barrier
  // in the beginning of each wave to make sure that all items from one
  // wave end up being executed in parallel.
  constexpr int kNumWaves = 100;
  constexpr int kWaveSize = 10;
  constexpr int kModulo = 998244353;

  // Make sure that the executor has exactly enough threads to execute all
  // items in parallel.
  auto executor = MakeAsioExecutor(kWaveSize);

  ASSERT_OK_AND_ASSIGN(
      auto add_modulo_op,
      MakeLambdaOperator(
          ExprOperatorSignature{{"x"}, {"y"}},
          CallOp("kd.math.mod",
                 {CallOp("kd.add", {Placeholder("x"), Placeholder("y")}, {}),
                  Literal(DataSlice::CreateFromScalar(kModulo))},
                 {})));
  auto x_signature = ExprOperatorSignature{{"x"}};
  auto get_data_slice_output_type =
      [](absl::Span<const QType* const> input_qtypes) {
        return GetQType<DataSlice>();
      };

  std::vector<TypedValue> all_items;
  std::vector<FuturePtr> sum_futures;
  std::vector<int> expected_values;
  for (int i = 0; i < kWaveSize; ++i) {
    all_items.push_back(TypedValue::FromValue(DataSlice::CreateFromScalar(i)));
    expected_values.push_back(i);
  }
  absl::BlockingCounter counter(kNumWaves * kWaveSize);
  for (int wave_id = 0; wave_id < kNumWaves; ++wave_id) {
    std::vector<TypedValue> items_in_wave;
    absl::Barrier* barrier = new absl::Barrier(kWaveSize);
    // In production Koda stream code, please do not use barriers or
    // other synchronization primitives that expect concrete
    // computations to be executed in parallel. Instead, please rely
    // on the streams themselves to communicate between parts of
    // the computation. This use of barriers is for testing purposes
    // only.
    auto wait_for_barrier_op = std::make_shared<StdFunctionOperator>(
        "internal", x_signature, "", get_data_slice_output_type,
        [barrier, &counter](absl::Span<const TypedRef> inputs) {
          if (barrier->Block()) delete barrier;
          counter.DecrementCount();
          return TypedValue(inputs[0]);
        });
    for (int i = 0; i < kWaveSize; ++i) {
      // At least one of the inputs must be from the previous wave, otherwise
      // this item might start executing before the previous wave, and
      // we won't have enough threads not to block.
      int a = absl::Uniform<int>(rng, all_items.size() - kWaveSize,
                                 all_items.size());
      int b = absl::Uniform<int>(rng, 0, all_items.size());
      if (absl::Uniform<int>(rng, 0, 2) == 0) {
        std::swap(a, b);
      }
      ASSERT_OK_AND_ASSIGN(FuturePtr sum,
                           AsyncEvalWithCompilationCache(
                               executor, add_modulo_op,
                               {all_items[a].AsRef(), all_items[b].AsRef()},
                               GetQType<DataSlice>()));
      sum_futures.push_back(sum);
      expected_values.push_back((expected_values[a] + expected_values[b]) %
                                kModulo);
      auto sum_value = MakeFutureQValue(sum);
      ASSERT_OK_AND_ASSIGN(FuturePtr sum_wait,
                           AsyncEvalWithCompilationCache(
                               executor, wait_for_barrier_op,
                               {sum_value.AsRef()}, GetQType<DataSlice>()));
      items_in_wave.push_back(MakeFutureQValue(sum_wait));
    }
    all_items.insert(all_items.end(), items_in_wave.begin(),
                     items_in_wave.end());
  }
  counter.Wait();

  for (int i = 0; i < kNumWaves * kWaveSize; ++i) {
    EXPECT_THAT(
        sum_futures[i]->GetValueForTesting(),
        IsOkAndHolds(TypedValueWith<DataSlice>(IsEquivalentTo(
            DataSlice::CreateFromScalar(expected_values[i + kWaveSize])))));
  }
}

}  // namespace koladata::functor::parallel
