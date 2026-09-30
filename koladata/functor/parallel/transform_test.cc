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

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/call.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/parallel/create_transform_config.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/functor/parallel/future_qtype.h"
#include "koladata/functor/parallel/transform_config.h"
#include "koladata/functor/parallel/transform_config.pb.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::testing::QValueWith;
using ::koladata::testing::IsEquivalentTo;

absl::StatusOr<DataSlice> MissingObject() {
  return DataSlice::Create(internal::DataItem(),
                           internal::DataItem(schema::kObject),
                           DataBag::EmptyMutable());
}

// Returns a config without operator replacements.
absl::StatusOr<ParallelTransformConfigPtr> MakeEmptyConfig() {
  ASSIGN_OR_RETURN(DataSlice missing_object, MissingObject());
  return CreateParallelTransformConfig(missing_object);
}

// Returns a functor computing `I.a + I.b`, backed by an immutable DataBag.
absl::StatusOr<DataSlice> MakeAddFunctor() {
  ASSIGN_OR_RETURN(expr::InputContainer input_container,
                   expr::InputContainer::Create("I"));
  ASSIGN_OR_RETURN(arolla::expr::ExprNodePtr returns_expr,
                   arolla::expr::CallOp("kd.math.add",
                                        {input_container.CreateInput("a"),
                                         input_container.CreateInput("b")},
                                        {}));
  DataSlice returns =
      DataSlice::CreatePrimitive(arolla::expr::ExprQuote(returns_expr));
  ASSIGN_OR_RETURN(DataSlice missing_object, MissingObject());
  return CreateFunctor(returns, missing_object, {}, {});
}

// Most of the tests are in koda_internal_parallel_transform_test.py, this
// is a basic sanity check only.
TEST(TransformTest, Basic) {
  ExecutorPtr executor = GetEagerExecutor();
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config, MakeEmptyConfig());
  ASSERT_OK_AND_ASSIGN(DataSlice functor, MakeAddFunctor());
  ASSERT_OK_AND_ASSIGN(DataSlice transformed_functor,
                       TransformToParallel(config, functor));
  auto [future_a, writer_a] = MakeFuture(arolla::GetQType<DataSlice>());
  std::move(writer_a).SetValue(
      arolla::TypedValue::FromValue(DataSlice::CreatePrimitive(1)));
  auto [future_b, writer_b] = MakeFuture(arolla::GetQType<DataSlice>());
  std::move(writer_b).SetValue(
      arolla::TypedValue::FromValue(DataSlice::CreatePrimitive(2)));
  auto future_a_value = MakeFutureQValue(future_a);
  auto future_b_value = MakeFutureQValue(future_b);
  ASSERT_OK_AND_ASSIGN(auto result,
                       CallFunctorWithCompilationCache(
                           transformed_functor,
                           {arolla::TypedRef::FromValue(executor),
                            future_a_value.AsRef(), future_b_value.AsRef()},
                           {"a", "b"}));
  ASSERT_OK_AND_ASSIGN(FuturePtr result_value, result.As<FuturePtr>());
  EXPECT_THAT(result_value->GetValueForTesting(),
              IsOkAndHolds(QValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreatePrimitive(3)))));
}

TEST(TransformToParallelTest, CachesImmutableFunctors) {
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config, MakeEmptyConfig());
  ASSERT_OK_AND_ASSIGN(DataSlice functor, MakeAddFunctor());
  ASSERT_FALSE(functor.GetBag()->IsMutable());
  ASSERT_OK_AND_ASSIGN(DataSlice transformed1,
                       TransformToParallel(config, functor));
  ASSERT_OK_AND_ASSIGN(DataSlice transformed2,
                       TransformToParallel(config, functor));
  EXPECT_EQ(transformed1.GetBag(), transformed2.GetBag());
  EXPECT_THAT(transformed2, IsEquivalentTo(transformed1));
}

TEST(TransformToParallelTest, DoesNotCacheMutableFunctors) {
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config, MakeEmptyConfig());
  ASSERT_OK_AND_ASSIGN(DataSlice immutable_functor, MakeAddFunctor());
  ASSERT_OK_AND_ASSIGN(DataSlice functor, immutable_functor.ForkBag());
  ASSERT_OK_AND_ASSIGN(DataSlice transformed1,
                       TransformToParallel(config, functor));
  ASSERT_OK_AND_ASSIGN(DataSlice transformed2,
                       TransformToParallel(config, functor));
  EXPECT_NE(transformed1.GetBag(), transformed2.GetBag());
}

TEST(TransformToParallelTest, CachesPerConfig) {
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config, MakeEmptyConfig());
  ParallelTransformConfigProto other_config_proto;
  other_config_proto.set_allow_runtime_transforms(true);
  ASSERT_OK_AND_ASSIGN(
      ParallelTransformConfigPtr other_config,
      CreateParallelTransformConfigFromProto(other_config_proto));
  ASSERT_OK_AND_ASSIGN(DataSlice functor, MakeAddFunctor());
  ASSERT_OK_AND_ASSIGN(DataSlice transformed,
                       TransformToParallel(config, functor));
  ASSERT_OK_AND_ASSIGN(DataSlice other_transformed1,
                       TransformToParallel(other_config, functor));
  ASSERT_OK_AND_ASSIGN(DataSlice other_transformed2,
                       TransformToParallel(other_config, functor));
  EXPECT_NE(transformed.GetBag(), other_transformed1.GetBag());
  EXPECT_EQ(other_transformed1.GetBag(), other_transformed2.GetBag());
}

// Marks when the DataBag it is attached to (as cached metadata) is destroyed.
struct DataBagLifetimeSentinel {};

TEST(TransformToParallelTest, CacheDoesNotKeepFunctorAlive) {
  // Transforms the functor argument of kd.call, so that sub-functors are
  // transformed (and cached) too.
  ParallelTransformConfigProto config_proto;
  auto* call_replacement = config_proto.add_operator_replacements();
  call_replacement->set_from_op("kd.call");
  call_replacement->set_to_op("kd.call");
  call_replacement->mutable_argument_transformation()
      ->add_functor_argument_indices(0);
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config,
                       CreateParallelTransformConfigFromProto(config_proto));
  std::weak_ptr<const DataBagLifetimeSentinel> sentinel;
  {
    // Returns `kd.call(V.inner, a=V.x, b=V.x)` (only transformed, never
    // evaluated). Both the non-expr variable `x` and the transformed `inner`
    // are embedded into the transformed functor as literals, so this checks
    // that neither references the original DataBag.
    ASSERT_OK_AND_ASSIGN(DataSlice inner, MakeAddFunctor());
    ASSERT_OK_AND_ASSIGN(
        DataSlice x, ObjectCreator::FromAttrs(DataBag::EmptyMutable(), {"a"},
                                              {DataSlice::CreatePrimitive(1)}));
    ASSERT_OK_AND_ASSIGN(expr::InputContainer variable_container,
                         expr::InputContainer::Create("V"));
    ASSERT_OK_AND_ASSIGN(arolla::expr::ExprNodePtr x_expr,
                         variable_container.CreateInput("x"));
    ASSERT_OK_AND_ASSIGN(
        arolla::expr::ExprNodePtr returns_expr,
        arolla::expr::CallOp(
            "kd.call",
            {variable_container.CreateInput("inner"),
             /*args=*/arolla::expr::CallOp("core.make_tuple", {}),
             /*return_type_as=*/
             arolla::expr::Literal(DataSlice::CreatePrimitive(0)),
             /*kwargs=*/
             arolla::expr::CallOp(
                 "namedtuple.make",
                 {arolla::expr::Literal(arolla::Text("a,b")), x_expr, x_expr}),
             arolla::expr::Literal(internal::NonDeterministicTokenValue())}));
    ASSERT_OK_AND_ASSIGN(DataSlice missing_object, MissingObject());
    ASSERT_OK_AND_ASSIGN(
        DataSlice functor,
        CreateFunctor(
            DataSlice::CreatePrimitive(arolla::expr::ExprQuote(returns_expr)),
            missing_object, {"inner", "x"}, {inner, x}));
    sentinel = functor.GetBag()->SetCachedMetadata<DataBagLifetimeSentinel>(
        functor.item().value<internal::ObjectId>(),
        std::make_shared<const DataBagLifetimeSentinel>());
    ASSERT_OK(TransformToParallel(config, functor));
    ASSERT_FALSE(sentinel.expired());
  }
  EXPECT_TRUE(sentinel.expired());
}

}  // namespace
}  // namespace koladata::functor::parallel
