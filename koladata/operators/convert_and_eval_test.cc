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
#include "koladata/operators/convert_and_eval.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::ops {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using DataSliceEdge = ::koladata::DataSlice::JaggedShape::Edge;

DataSliceEdge EdgeFromSizes(absl::Span<const int64_t> sizes) {
  std::vector<arolla::OptionalValue<int64_t>> split_points;
  split_points.reserve(sizes.size() + 1);
  split_points.push_back(0);
  for (int64_t size : sizes) {
    split_points.push_back(split_points.back().value + size);
  }
  return *DataSliceEdge::FromSplitPoints(
      arolla::CreateDenseArray<int64_t>(split_points));
}

TEST(ArollaEval, SimplePointwiseEval) {
  arolla::InitArolla();
  {
    // Eval through operator.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = test::DataSlice<int64_t>(
        {int64_t{3}, int64_t{-3}, std::nullopt, int64_t{-1}}, y_shape,
        schema::kObject);
    ASSERT_OK_AND_ASSIGN(auto result, SimplePointwiseEval("math.add", {x, y}));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<int64_t>(
                    {int64_t{4}, int64_t{-2}, std::nullopt, std::nullopt},
                    y_shape, schema::kObject)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimplePointwiseEval("math.add", {x, y},
                                             internal::DataItem(schema::kAny)));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<int64_t>(
                    {int64_t{4}, int64_t{-2}, std::nullopt, std::nullopt},
                    y_shape, schema::kAny)));
  }
  {
    // One empty and unknown slice.
    DataSlice x = test::EmptyDataSlice(3, schema::kObject);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = test::DataSlice<int64_t>(
        {int64_t{3}, int64_t{-3}, std::nullopt, int64_t{-1}}, y_shape,
        schema::kObject);
    ASSERT_OK_AND_ASSIGN(auto result, SimplePointwiseEval("math.add", {x, y}));
    EXPECT_THAT(
        result,
        IsEquivalentTo(
            *test::EmptyDataSlice(4, schema::kObject).Reshape(y_shape)));
    // TODO: This should be true once fully empty DenseArrays are
    // represented as empty-and-unknown. This check is kept to ensure that this
    // is changed in the future.
    EXPECT_FALSE(result.impl_empty_and_unknown());
  }
  {
    // One empty and unknown slice - not supported type error.
    DataSlice x = test::EmptyDataSlice(3, schema::kObject);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = test::DataSlice<arolla::Text>(
        {"foo", "bar", std::nullopt, "baz"}, y_shape, schema::kObject);
    EXPECT_THAT(
        SimplePointwiseEval("math.add", {x, y}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got x: DENSE_ARRAY_TEXT")));
  }
  {
    // All empty and unknown slice - schema and shape broadcasting works.
    DataSlice x = test::EmptyDataSlice(3, schema::kObject);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = *test::EmptyDataSlice(4, schema::kInt32).Reshape(y_shape);
    ASSERT_OK_AND_ASSIGN(auto result, SimplePointwiseEval("math.add", {x, y}));
    EXPECT_THAT(
        result,
        IsEquivalentTo(
            *test::EmptyDataSlice(4, schema::kObject).Reshape(y_shape)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimplePointwiseEval("math.add", {x, y},
                                             internal::DataItem(schema::kAny)));
    EXPECT_THAT(result,
                IsEquivalentTo(
                    *test::EmptyDataSlice(4, schema::kAny).Reshape(y_shape)));
  }
}

TEST(ArollaEval, SimpleAggIntoEval) {
  arolla::InitArolla();
  {
    // Eval through operator.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x =
        test::DataSlice<int>({1, 2, 3, std::nullopt}, shape, schema::kObject);
    ASSERT_OK_AND_ASSIGN(auto result, SimpleAggIntoEval("math.sum", {x}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int>(
                            {3, 3, 0}, shape.RemoveDims(1), schema::kObject)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(
        result,
        SimpleAggIntoEval("math.sum", {x}, internal::DataItem(schema::kAny)));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int>(
                            {3, 3, 0}, shape.RemoveDims(1), schema::kAny)));
  }
  {
    // Empty and unknown slice.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    ASSERT_OK_AND_ASSIGN(
        DataSlice x, test::EmptyDataSlice(4, schema::kObject).Reshape(shape));
    ASSERT_OK_AND_ASSIGN(auto result, SimpleAggIntoEval("core.agg_count", {x}));
    EXPECT_THAT(result, IsEquivalentTo(*test::EmptyDataSlice(3, schema::kObject)
                                            .Reshape(shape.RemoveDims(1))));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimpleAggIntoEval("core.agg_count", {x},
                                           internal::DataItem(schema::kMask)));
    EXPECT_THAT(result, IsEquivalentTo(*test::EmptyDataSlice(3, schema::kMask)
                                            .Reshape(shape.RemoveDims(1))));
  }
  {
    // Scalar input error.
    DataSlice x = test::DataItem(1, schema::kObject);
    EXPECT_THAT(SimpleAggIntoEval("math.sum", {x}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected rank(x) > 0")));
  }
  {
    // Mixed input error.
    DataSlice x = test::MixedDataSlice<int, float>(
        {1, std::nullopt}, {std::nullopt, 2.0f}, schema::kObject);
    EXPECT_THAT(
        SimpleAggIntoEval("math.sum", {x}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
  }
  {
    // Eval with several inputs - default edge index.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<arolla::Text>(
        {"foo", "bar", "baz", std::nullopt}, x_shape);
    DataSlice sep = test::DataItem(arolla::Text(","));
    // `strings.agg_join` takes args: (x, edge, sep).
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimpleAggIntoEval("strings.agg_join", {x, sep}));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<arolla::Text>(
                    {"foo,bar", "baz", std::nullopt}, x_shape.RemoveDims(1))));
  }
  {
    // Eval with several inputs - non-default edge index.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 2, 1})});
    DataSlice x =
        test::DataSlice<float>({1.0f, 2.0f, 3.0f, 4.0f, std::nullopt}, x_shape);
    // `math.correlation` takes args: (x, y, edge).
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggIntoEval("math.correlation", {x, x},
                                       /*output_schema=*/internal::DataItem(),
                                       /*edge_arg_index=*/2));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<float>(
                    {1.0f, 1.0f, std::nullopt}, x_shape.RemoveDims(1))));
  }
}

TEST(ArollaEval, SimpleAggOverEval) {
  arolla::InitArolla();
  {
    // Eval through operator.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({2}), EdgeFromSizes({3, 2})});
    DataSlice x = test::DataSlice<int>({1, 2, 0, 1, std::nullopt}, shape,
                                       schema::kObject);
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimpleAggOverEval("array.inverse_mapping", {x}));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<int>({2, 0, 1, std::nullopt, 0},
                                                    shape, schema::kObject)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimpleAggOverEval("array.inverse_mapping", {x},
                                           internal::DataItem(schema::kAny)));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int>(
                            {2, 0, 1, std::nullopt, 0}, shape, schema::kAny)));
  }
  {
    // Empty and unknown slice.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({2}), EdgeFromSizes({3, 2})});
    ASSERT_OK_AND_ASSIGN(
        DataSlice x, test::EmptyDataSlice(5, schema::kObject).Reshape(shape));
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimpleAggOverEval("array.inverse_mapping", {x}));
    EXPECT_THAT(result, IsEquivalentTo(x));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimpleAggOverEval("array.inverse_mapping", {x},
                                           internal::DataItem(schema::kAny)));
    EXPECT_THAT(result, IsEquivalentTo(
                            *x.WithSchema(internal::DataItem(schema::kAny))));
  }
  {
    // Scalar input error.
    DataSlice x = test::DataItem(1, schema::kObject);
    EXPECT_THAT(SimpleAggOverEval("array.inverse_mapping", {x}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected rank(x) > 0")));
  }
  {
    // Mixed input error.
    DataSlice x = test::MixedDataSlice<int, int64_t>(
        {1, std::nullopt}, {std::nullopt, int64_t{2}}, schema::kObject);
    EXPECT_THAT(
        SimpleAggOverEval("array.inverse_mapping", {x}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
  }
  {
    // Eval with several inputs - default edge index.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>({1, 2, 3, std::nullopt}, x_shape);
    DataSlice desc = test::DataItem(true);
    // `array.dense_rank` takes args: (x, edge, descending).
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggOverEval(
                         "array.dense_rank", {x, desc},
                         /*output_schema=*/internal::DataItem(schema::kInt64)));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int64_t>(
                            {int64_t{1}, int64_t{0}, int64_t{0}, std::nullopt},
                            x_shape)));
  }
  {
    // Eval with several inputs - non-default edge index.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>({1, 2, 3, std::nullopt}, x_shape);
    DataSlice desc = test::DataItem(true);
    // `array.ordinal_rank` takes args: (x, tie_breaker, edge, descending).
    ASSERT_OK_AND_ASSIGN(
        auto result,
        SimpleAggOverEval("array.ordinal_rank", {x, x, desc},
                          /*output_schema=*/internal::DataItem(schema::kInt64),
                          /*edge_arg_index=*/2));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int64_t>(
                            {int64_t{1}, int64_t{0}, int64_t{0}, std::nullopt},
                            x_shape)));
  }
}

TEST(ArollaEval, EvalExpr) {
  {
    // Success.
    auto x = arolla::CreateDenseArray<int>({1, 2, std::nullopt});
    auto y = arolla::CreateDenseArray<int>({2, 3, 4});
    auto x_tv = arolla::TypedValue::FromValue(x);
    auto y_tv = arolla::TypedValue::FromValue(y);
    ASSERT_OK_AND_ASSIGN(auto result,
                         EvalExpr("math.add", {x_tv.AsRef(), y_tv.AsRef()}));
    EXPECT_THAT(result.UnsafeAs<arolla::DenseArray<int>>(),
                ElementsAre(3, 5, std::nullopt));
  }
  {
    // Compilation error.
    auto x = arolla::CreateDenseArray<int>({1, 2, std::nullopt});
    auto y = arolla::CreateDenseArray<arolla::Unit>(
        {arolla::kUnit, arolla::kUnit, arolla::kUnit});
    auto x_tv = arolla::TypedValue::FromValue(x);
    auto y_tv = arolla::TypedValue::FromValue(y);
    EXPECT_THAT(
        EvalExpr("math.add", {x_tv.AsRef(), y_tv.AsRef()}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got y: DENSE_ARRAY_UNIT")));
  }
  {
    // Runtime error.
    auto x_tv = arolla::TypedValue::FromValue(1);
    auto y_tv = arolla::TypedValue::FromValue(0);
    EXPECT_THAT(EvalExpr("math.floordiv", {x_tv.AsRef(), y_tv.AsRef()}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("division by zero")));
  }
}

TEST(PrimitiveArollaSchemaTest, PrimitiveSchema_DataItem) {
  {
    // Empty and unknown.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kNone)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kAny)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
  }
  {
    // Missing with primitive schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kText)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Present values with OBJECT / ANY schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(1, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(arolla::Text("foo"), schema::kAny)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Present values with corresponding schema schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(1, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(arolla::Text("foo"), schema::kText)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Entity schema error.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(std::nullopt, internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with Entity schema is not supported")));
  }
  {
    // Unsupported internal data.
    EXPECT_THAT(GetPrimitiveArollaSchema(test::DataItem(
                    internal::AllocateSingleObject(), schema::kObject)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("DataSlice has no primitive schema")));
  }
}

TEST(PrimitiveArollaSchemaTest, PrimitiveSchema_DataSlice) {
  {
    // Empty and unknown.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kNone)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
    EXPECT_THAT(GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kAny)),
                IsOkAndHolds(IsEquivalentTo(internal::DataItem())));
  }
  {
    // Missing with primitive schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kText)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Present values with OBJECT / ANY schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataSlice<int>({1}, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataSlice<arolla::Text>({"foo"}, schema::kAny)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Present values with corresponding schema schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataSlice<int>({1}, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataSlice<arolla::Text>({"foo"}, schema::kText)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kText))));
  }
  {
    // Entity schema error.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::EmptyDataSlice(3, internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with Entity schema is not supported")));
  }
  {
    // Unsupported internal data.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::AllocateDataSlice(3, schema::kObject)),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice has no primitive schema")));
  }
  {
    // Mixed data.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::MixedDataSlice<int, float>(
            {1, std::nullopt}, {std::nullopt, 2.0f}, schema::kObject)),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
  }
}

}  // namespace
}  // namespace koladata::ops
