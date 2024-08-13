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
#include <memory>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::ops {
namespace {

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

TEST(SimplePointwiseEvalTest, SimpleEval) {
  arolla::InitArolla();
  {
    // Eval through operator.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = test::DataSlice<int64_t>(
        {int64_t{3}, int64_t{-3}, std::nullopt, int64_t{-1}}, y_shape,
        schema::kObject);
    ASSERT_OK_AND_ASSIGN(
        auto result,
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<int64_t>(
                    {int64_t{4}, int64_t{-2}, std::nullopt, std::nullopt},
                    y_shape, schema::kObject)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(
        result,
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}, internal::DataItem(schema::kAny)));
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
    ASSERT_OK_AND_ASSIGN(
        auto result,
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}));
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
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got x: DENSE_ARRAY_TEXT")));
  }
  {
    // All empty and unknown slice - schema and shape broadcasting works.
    DataSlice x = test::EmptyDataSlice(3, schema::kObject);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = *test::EmptyDataSlice(4, schema::kInt32).Reshape(y_shape);
    ASSERT_OK_AND_ASSIGN(
        auto result,
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}));
    EXPECT_THAT(
        result,
        IsEquivalentTo(
            *test::EmptyDataSlice(4, schema::kObject).Reshape(y_shape)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(
        result,
        SimplePointwiseEval(
            std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
            {x, y}, internal::DataItem(schema::kAny)));
    EXPECT_THAT(result,
                IsEquivalentTo(
                    *test::EmptyDataSlice(4, schema::kAny).Reshape(y_shape)));
  }
}

TEST(EvalExpr, SimpleEval) {
  {
    // Success.
    auto x = arolla::CreateDenseArray<int>({1, 2, std::nullopt});
    auto y = arolla::CreateDenseArray<int>({2, 3, 4});
    auto x_tv = arolla::TypedValue::FromValue(x);
    auto y_tv = arolla::TypedValue::FromValue(y);
    ASSERT_OK_AND_ASSIGN(
        auto result,
        EvalExpr(std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
                 {x_tv.AsRef(), y_tv.AsRef()}));
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
        EvalExpr(std::make_shared<arolla::expr::RegisteredOperator>("math.add"),
                 {x_tv.AsRef(), y_tv.AsRef()}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got y: DENSE_ARRAY_UNIT")));
  }
  {
    // Runtime error.
    auto x_tv = arolla::TypedValue::FromValue(1);
    auto y_tv = arolla::TypedValue::FromValue(0);
    EXPECT_THAT(EvalExpr(std::make_shared<arolla::expr::RegisteredOperator>(
                             "math.floordiv"),
                         {x_tv.AsRef(), y_tv.AsRef()}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("division by zero")));
  }
}

}  // namespace
}  // namespace koladata::ops
