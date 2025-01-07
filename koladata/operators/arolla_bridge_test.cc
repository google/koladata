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
#include "koladata/operators/arolla_bridge.h"

#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::ops {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::Eq;
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
    EXPECT_TRUE(result.impl_empty_and_unknown());
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
  {
    // Schema derived from Arolla output is different from inputs' schemas.
    DataSlice x = test::DataSlice<int>({6, 6, std::nullopt}, schema::kInt32);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice y = test::DataSlice<int64_t>(
        {int64_t{2}, int64_t{3}, std::nullopt, int64_t{-1}}, y_shape,
        schema::kInt64);
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimplePointwiseEval("math.divide", {x, y}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<float>(
                            {3.0, 2.0, std::nullopt, std::nullopt}, y_shape,
                            schema::kFloat32)));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimplePointwiseEval("math.divide", {x, y},
                                             internal::DataItem(schema::kAny)));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<float>(
                            {3.0, 2.0, std::nullopt, std::nullopt}, y_shape,
                            schema::kAny)));
  }
  {
    // Large arity.
    DataSlice fmt = test::DataItem(arolla::Text("%s"), schema::kString);
    DataSlice v = test::DataItem(arolla::Text("foo"), schema::kString);
    std::vector<DataSlice> args = {fmt};
    for (int i = 0; i < 30; ++i) {
      args.push_back(v);
    }
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimplePointwiseEval("strings.printf", args));
    EXPECT_THAT(result, IsEquivalentTo(
                            test::DataItem<arolla::Text>(arolla::Text("foo"))));
  }
  {
    // invalid input: entity.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice::JaggedShape y_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    auto db = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(auto y, EntityCreator::Shaped(db, y_shape, {}, {}));
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with Struct schema is not supported")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                HasSubstr("DataSlice with Struct schema is not supported"));
  }
  {
    // invalid input: mixed types.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice y = test::MixedDataSlice<int, arolla::Bytes>(
        {4, 5, std::nullopt}, {std::nullopt, std::nullopt, "six"},
        schema::kObject);
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                HasSubstr("DataSlice with mixed types is not supported"));
  }
  {
    // Invalid input: object.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice y =
        test::DataItem(internal::AllocateSingleObject(), schema::kObject);
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("DataSlice has no primitive schema")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                HasSubstr("DataSlice has no primitive schema"));
  }
  {
    // Incompatible shapes.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice y = test::DataSlice<int>({1}, schema::kInt32);
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible: JaggedShape(1) "
                                   "vs JaggedShape(3)")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                Eq("cannot align inputs to a common shape"));
    EXPECT_THAT(
        payload->cause().error_message(),
        HasSubstr(
            "shapes are not compatible: JaggedShape(1) vs JaggedShape(3)"));
  }
  {
    // Incompatible shapes for all missing inputs.
    DataSlice x = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt}, schema::kObject);
    DataSlice y = test::DataSlice<int>({std::nullopt}, schema::kObject);
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible: JaggedShape(1) "
                                   "vs JaggedShape(3)")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                Eq("cannot align inputs to a common shape"));
    EXPECT_THAT(
        payload->cause().error_message(),
        HasSubstr(
            "shapes are not compatible: JaggedShape(1) vs JaggedShape(3)"));
  }
  {
    // Arolla op compilation error.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice y =
        test::DataSlice<arolla::Text>({"1", "2", "3"}, schema::kString);
    auto status = SimplePointwiseEval("math.add", {x, y}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got y: DENSE_ARRAY_TEXT")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                HasSubstr("expected numerics, got y: DENSE_ARRAY_TEXT"));
  }
  {
    // Arolla op eval error.
    DataSlice x = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
    DataSlice y = test::DataSlice<int>({0, 0, 0}, schema::kInt32);
    auto status = SimplePointwiseEval("math.floordiv", {x, y}).status();
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                                 HasSubstr("division by zero")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(), HasSubstr("division by zero"));
  }
}

TEST(ArollaEval, SimplePointwiseEvalWithPrimaryOperands) {
  {
    // Specifying two primary operands: normal operation.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<arolla::Text>(
        {"foo", "bar", std::nullopt, "baz"}, x_shape, schema::kObject);
    DataSlice substr = test::DataSlice<arolla::Text>({"oo", "ar", "ee", "baz"},
                                                     x_shape, schema::kString);
    DataSlice start = test::DataSlice<int>({1, 2, 0}, schema::kInt32);
    DataSlice end = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt}, schema::kInt32);
    ASSERT_OK_AND_ASSIGN(
        auto result, SimplePointwiseEval(
                         "strings.find", {x, substr, start, end},
                         /*output_schema=*/internal::DataItem(schema::kInt64),
                         /*primary_operand_indices=*/{{0, 1}}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int64_t>(
                            {1, 1, std::nullopt, 0}, x_shape, schema::kInt64)));
  }
  {
    // Specifying two primary operands with mismatching types STRING and BYTES.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<arolla::Text>(
        {"foo", "bar", std::nullopt, "baz"}, x_shape, schema::kObject);
    DataSlice substr = test::DataSlice<arolla::Bytes>({"oo", "ar", "ee", "baz"},
                                                      x_shape, schema::kBytes);
    DataSlice start = test::DataSlice<int>({1, 2, 0}, schema::kInt32);
    DataSlice end = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt}, schema::kInt32);
    EXPECT_THAT(SimplePointwiseEval(
                    "strings.find", {x, substr, start, end},
                    /*output_schema=*/internal::DataItem(schema::kInt64),
                    /*primary_operand_indices=*/{{0, 1}}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("incompatible types s: DENSE_ARRAY_TEXT and "
                                   "substr: DENSE_ARRAY_BYTES")));
  }
  {
    // Passing a non-primary operand with unknown schema.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<arolla::Text>(
        {"foo", "bar", std::nullopt, "baz"}, x_shape, schema::kObject);
    DataSlice substr = test::DataSlice<arolla::Text>({"oo", "ar", "ee", "baz"},
                                                     x_shape, schema::kString);
    DataSlice start = test::DataSlice<int>({1, 2, 0}, schema::kInt32);
    // This schema is unknown:
    DataSlice end = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt}, schema::kAny);
    EXPECT_THAT(SimplePointwiseEval(
                    "strings.find", {x, substr, start, end},
                    /*output_schema=*/internal::DataItem(schema::kInt64),
                    /*primary_operand_indices=*/{{0, 1}}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("DataSlice for the non-primary operand 4 "
                                   "should have a primitive schema")));
  }
  {
    // Passing a non-primary operand that does not have a primitive schema.
    DataSlice::JaggedShape x_shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<arolla::Text>(
        {"foo", "bar", std::nullopt, "baz"}, x_shape, schema::kObject);
    DataSlice substr = test::DataSlice<arolla::Text>({"oo", "ar", "ee", "baz"},
                                                     x_shape, schema::kString);
    DataSlice start = test::DataSlice<int>({1, 2, 0}, schema::kInt32);
    // This has no primitive schema:
    DataSlice end =
        test::DataItem(std::nullopt, internal::AllocateExplicitSchema());
    EXPECT_THAT(SimplePointwiseEval(
                    "strings.find", {x, substr, start, end},
                    /*output_schema=*/internal::DataItem(schema::kInt64),
                    /*primary_operand_indices=*/{{0, 1}}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("DataSlice for the non-primary operand 4 "
                                   "should have a primitive schema")));
  }
}

TEST(ArollaEval, SimpleAggIntoEval) {
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
    // Computes common schema for input and output
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x =
        test::DataSlice<int>({1, 2, 3, std::nullopt}, shape, schema::kInt32);
    ASSERT_OK_AND_ASSIGN(auto result, SimpleAggIntoEval("math.mean", {x}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<float>(
                            {1.5, 3.0, std::nullopt}, shape.RemoveDims(1),
                            schema::kFloat32)));
  }
  {
    // Empty and unknown slice.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    ASSERT_OK_AND_ASSIGN(
        DataSlice x, test::EmptyDataSlice(4, schema::kObject).Reshape(shape));
    ASSERT_OK_AND_ASSIGN(auto result,
                         SimpleAggIntoEval("slices.agg_count", {x}));
    EXPECT_THAT(result, IsEquivalentTo(*test::EmptyDataSlice(3, schema::kObject)
                                            .Reshape(shape.RemoveDims(1))));
    // With output schema set.
    ASSERT_OK_AND_ASSIGN(result,
                         SimpleAggIntoEval("slices.agg_count", {x},
                                           internal::DataItem(schema::kMask)));
    EXPECT_THAT(result, IsEquivalentTo(*test::EmptyDataSlice(3, schema::kMask)
                                            .Reshape(shape.RemoveDims(1))));
  }
  {
    // Scalar input error.
    DataSlice x = test::DataItem(1, schema::kObject);
    absl::Status status = SimpleAggIntoEval("math.sum", {x}).status();
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected rank(x) > 0")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(), Eq("math.sum: expected "
                                             "rank(x) > 0"));
  }
  {
    // Mixed input error.
    DataSlice x = test::MixedDataSlice<int, float>(
        {1, std::nullopt}, {std::nullopt, 2.0f}, schema::kObject);
    absl::Status status = SimpleAggIntoEval("math.sum", {x}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(), Eq("math.sum: invalid inputs"));
    EXPECT_THAT(payload->cause().error_message(),
                HasSubstr("DataSlice with mixed types is not supported"));
  }
  {
    DataSlice x =
        test::DataSlice<arolla::Text>({"foo", "bar", "baz", std::nullopt});
    absl::Status status = SimpleAggIntoEval("math.max", {x}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("expected numerics, got x: DENSE_ARRAY_TEXT")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                Eq("math.max: successfully converted input DataSlice(s) to "
                   "DenseArray(s) but failed to "
                   "evaluate the Arolla operator"));
    EXPECT_THAT(payload->cause().error_message(),
                HasSubstr("expected numerics, got x: DENSE_ARRAY_TEXT"));
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

TEST(ArollaEval, SimpleAggIntoEvalWithPrimaryOperands) {
  {
    // Specifying all primary operands: normal operation.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x =
        test::DataSlice<int>({1, 2, 3, std::nullopt}, shape, schema::kObject);
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggIntoEval("math.sum", {x},
                                       /*output_schema=*/internal::DataItem(),
                                       /*edge_arg_index=*/1,
                                       /*primary_operand_indices=*/{{0}}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int>(
                            {3, 3, 0}, shape.RemoveDims(1), schema::kObject)));
  }
  {
    // Specifying one primary operand. The second argument is not used to set
    // the schema of the first value.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, shape,
        schema::kObject);
    DataSlice unbiased = test::DataItem(true);
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggIntoEval("math.std", {x, unbiased},
                                       /*output_schema=*/internal::DataItem(),
                                       /*edge_arg_index=*/1,
                                       /*primary_operand_indices=*/{{0}}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<float>(
                            {std::nullopt, std::nullopt, std::nullopt},
                            shape.RemoveDims(1), schema::kObject)));
  }
  {
    // Non-primitive schema for non-primary data.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, shape,
        schema::kObject);
    DataSlice unbiased = test::DataItem(std::nullopt, schema::kObject);
    EXPECT_THAT(SimpleAggIntoEval("math.std", {x, unbiased},
                                  /*output_schema=*/internal::DataItem(),
                                  /*edge_arg_index=*/1,
                                  /*primary_operand_indices=*/{{0}}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("DataSlice for the non-primary operand 2 "
                                   "should have a primitive schema")));
  }
}

TEST(ArollaEval, SimpleAggOverEval) {
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
    absl::Status status = SimpleAggOverEval("math.sum", {x}).status();
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("expected rank(x) > 0")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(), Eq("math.sum: expected rank(x) > 0"));
  }
  {
    // Mixed input error.
    DataSlice x = test::MixedDataSlice<int, int64_t>(
        {1, std::nullopt}, {std::nullopt, int64_t{2}}, schema::kObject);
    absl::Status status =
        SimpleAggOverEval("array.inverse_mapping", {x}).status();
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with mixed types is not supported")));
    std::optional<internal::Error> payload = internal::GetErrorPayload(status);
    EXPECT_TRUE(payload.has_value());
    EXPECT_THAT(payload->error_message(),
                Eq("array.inverse_mapping: invalid inputs"));
    EXPECT_THAT(payload->cause().error_message(),
                HasSubstr("DataSlice with mixed types is not supported"));
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

TEST(ArollaEval, SimpleAggOverEvalWithPrimaryOperands) {
  {
    // Specifying all primary operands: normal operation.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x =
        test::DataSlice<int>({1, 2, 3, std::nullopt}, shape, schema::kObject);
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggOverEval("array.agg_index", {x},
                                       /*output_schema=*/internal::DataItem(),
                                       /*edge_arg_index=*/1,
                                       /*primary_operand_indices=*/{{0}}));
    EXPECT_THAT(result, IsEquivalentTo(test::DataSlice<int64_t>(
                            {int64_t{0}, int64_t{1}, int64_t{0}, std::nullopt},
                            shape, schema::kObject)));
  }
  {
    // Specifying one primary operand. The second argument is not used to set
    // the schema of the first value.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, shape,
        schema::kObject);
    DataSlice descending = test::DataItem(true);
    ASSERT_OK_AND_ASSIGN(
        auto result, SimpleAggOverEval("array.dense_rank", {x, descending},
                                       /*output_schema=*/internal::DataItem(),
                                       /*edge_arg_index=*/1,
                                       /*primary_operand_indices=*/{{0}}));
    EXPECT_THAT(result,
                IsEquivalentTo(test::DataSlice<int64_t>(
                    {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
                    shape, schema::kObject)));
  }
  {
    // Non-primitive schema for non-primary data.
    DataSlice::JaggedShape shape = *DataSlice::JaggedShape::FromEdges(
        {EdgeFromSizes({3}), EdgeFromSizes({2, 1, 1})});
    DataSlice x = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt}, shape,
        schema::kObject);
    DataSlice descending = test::DataItem(std::nullopt, schema::kObject);
    EXPECT_THAT(SimpleAggOverEval("array.dense_rank", {x, descending},
                                  /*output_schema=*/internal::DataItem(),
                                  /*edge_arg_index=*/1,
                                  /*primary_operand_indices=*/{{0}}),
                StatusIs(absl::StatusCode::kInternal,
                         HasSubstr("DataSlice for the non-primary operand 2 "
                                   "should have a primitive schema")));
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
        GetPrimitiveArollaSchema(test::DataItem(std::nullopt, schema::kString)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Present values with OBJECT / ANY schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(1, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(arolla::Text("foo"), schema::kAny)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Present values with corresponding schema schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataItem(1, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(arolla::Text("foo"), schema::kString)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Entity schema error.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataItem(std::nullopt, internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with Struct schema is not supported")));
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
        GetPrimitiveArollaSchema(test::EmptyDataSlice(3, schema::kString)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Present values with OBJECT / ANY schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataSlice<int>({1}, schema::kObject)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataSlice<arolla::Text>({"foo"}, schema::kAny)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Present values with corresponding schema schema.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(test::DataSlice<int>({1}, schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kInt32))));
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::DataSlice<arolla::Text>({"foo"}, schema::kString)),
        IsOkAndHolds(IsEquivalentTo(internal::DataItem(schema::kString))));
  }
  {
    // Entity schema error.
    EXPECT_THAT(
        GetPrimitiveArollaSchema(
            test::EmptyDataSlice(3, internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("DataSlice with Struct schema is not supported")));
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

TEST(EvalCompilerCacheTest, Equal) {
  using compiler_internal::Key;
  using compiler_internal::KeyEq;
  using compiler_internal::LookupKey;
  KeyEq eq;

  auto tv_int_1 = arolla::TypedValue::FromValue(1);
  auto tv_int_2 = arolla::TypedValue::FromValue(2);
  auto tv_float_1 = arolla::TypedValue::FromValue(1.0f);
  auto int_1 = tv_int_1.AsRef();
  auto int_2 = tv_int_2.AsRef();
  auto float_1 = tv_float_1.AsRef();
  auto int_qtype = int_1.GetType();
  auto float_qtype = float_1.GetType();
  std::string foo = "foo";
  {
    // <Key, Key>.
    // Equal.
    std::string foo = "foo";
    EXPECT_TRUE(eq(Key{"foo", {}}, Key{"foo", {}}));
    EXPECT_TRUE(eq(Key{foo, {}}, Key{"foo", {}}));
    EXPECT_TRUE(eq(Key{"foo", {arolla::GetQType<int>()}},
                   Key{"foo", {arolla::GetQType<int>()}}));
    EXPECT_TRUE(
        eq(Key{"foo", {arolla::GetQType<int>(), arolla::GetQType<float>()}},
           Key{"foo", {arolla::GetQType<int>(), arolla::GetQType<float>()}}));
    // Not equal.
    EXPECT_FALSE(eq(Key{"foo", {}}, Key{"bar", {}}));
    EXPECT_FALSE(eq(Key{"foo", {arolla::GetQType<int>()}},
                    Key{"foo", {arolla::GetQType<float>()}}));
    EXPECT_FALSE(
        eq(Key{"foo", {arolla::GetQType<int>(), arolla::GetQType<int>()}},
           Key{"foo", {arolla::GetQType<int>()}}));
    EXPECT_FALSE(
        eq(Key{"foo", {arolla::GetQType<float>(), arolla::GetQType<int>()}},
           Key{"foo", {arolla::GetQType<int>(), arolla::GetQType<float>()}}));
  }
  {
    // <Key, LookupKey>.
    // Equal.
    EXPECT_TRUE(eq(Key{"foo", {}}, LookupKey{"foo", {}}));
    EXPECT_TRUE(eq(Key{foo, {}}, LookupKey{"foo", {}}));
    EXPECT_TRUE(eq(Key{"foo", {int_qtype}}, LookupKey{"foo", {int_1}}));
    EXPECT_TRUE(eq(Key{"foo", {int_qtype}}, LookupKey{"foo", {int_2}}));
    EXPECT_TRUE(eq(Key{"foo", {int_qtype, float_qtype}},
                   LookupKey{"foo", {int_1, float_1}}));
    // Not equal.
    EXPECT_FALSE(eq(Key{"foo", {}}, LookupKey{"bar", {}}));
    EXPECT_FALSE(eq(Key{"foo", {int_qtype}}, LookupKey{"foo", {float_1}}));
    EXPECT_FALSE(
        eq(Key{"foo", {int_qtype, int_qtype}}, LookupKey{"foo", {int_1}}));
    EXPECT_FALSE(eq(Key{"foo", {float_qtype, int_qtype}},
                    LookupKey{"foo", {int_1, float_1}}));
  }
  {
    // <LookupKey, Key>.
    // Equal.
    EXPECT_TRUE(eq(LookupKey{"foo", {}}, Key{"foo", {}}));
    EXPECT_TRUE(eq(LookupKey{foo, {}}, Key{"foo", {}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_1}}, Key{"foo", {int_qtype}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_2}}, Key{"foo", {int_qtype}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_1, float_1}},
                   Key{"foo", {int_qtype, float_qtype}}));
    // Not equal.
    EXPECT_FALSE(eq(LookupKey{"foo", {}}, Key{"bar", {}}));
    EXPECT_FALSE(eq(LookupKey{"foo", {float_1}}, Key{"foo", {int_qtype}}));
    EXPECT_FALSE(eq(LookupKey{"foo", {int_1, int_1}}, Key{"foo", {int_qtype}}));
    EXPECT_FALSE(eq(LookupKey{"foo", {int_1, float_1}},
                    Key{"foo", {float_qtype, int_qtype}}));
  }
  {
    // <LookupKey, LookupKey>.
    // Equal.
    EXPECT_TRUE(eq(LookupKey{"foo", {}}, LookupKey{"foo", {}}));
    EXPECT_TRUE(eq(LookupKey{foo, {}}, LookupKey{"foo", {}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_1}}, LookupKey{"foo", {int_1}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_2}}, LookupKey{"foo", {int_1}}));
    EXPECT_TRUE(eq(LookupKey{"foo", {int_1, float_1}},
                   LookupKey{"foo", {int_2, float_1}}));
    // Not equal.
    EXPECT_FALSE(eq(LookupKey{"foo", {}}, LookupKey{"bar", {}}));
    EXPECT_FALSE(eq(LookupKey{"foo", {float_1}}, LookupKey{"foo", {int_1}}));
    EXPECT_FALSE(
        eq(LookupKey{"foo", {int_1, int_1}}, LookupKey{"foo", {int_1}}));
    EXPECT_FALSE(eq(LookupKey{"foo", {int_1, float_1}},
                    LookupKey{"foo", {float_1, int_1}}));
  }
}

TEST(EvalCompilerCacheTest, Hash) {
  using compiler_internal::Key;
  using compiler_internal::LookupKey;

  auto tv_int_1 = arolla::TypedValue::FromValue(1);
  auto tv_int_2 = arolla::TypedValue::FromValue(2);
  auto tv_float_1 = arolla::TypedValue::FromValue(1.0f);
  auto int_1 = tv_int_1.AsRef();
  auto int_2 = tv_int_2.AsRef();
  auto float_1 = tv_float_1.AsRef();
  auto int_qtype = int_1.GetType();
  auto float_qtype = float_1.GetType();
  std::string foo = "foo";
  {
    // Verify that the hash is consistent with the equality.
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
        std::make_tuple(
            // Keys.
            Key{"foo", {}}, Key{"bar", {}}, Key{foo, {}},
            Key{"foo", {int_qtype}}, Key{"foo", {int_qtype, float_qtype}},
            Key{"foo", {float_qtype, int_qtype}},
            Key{"foo", {int_qtype, int_qtype}},
            // LookupKeys.
            LookupKey{"foo", {}}, LookupKey{"bar", {}}, LookupKey{foo, {}},
            LookupKey{"foo", {int_1}}, LookupKey{"foo", {int_2}},
            LookupKey{"foo", {int_1, int_1}},
            LookupKey{"foo", {int_1, float_1}},
            LookupKey{"foo", {int_2, float_1}},
            LookupKey{"foo", {float_1, int_1}}),
        compiler_internal::KeyEq()));
  }
  {
    // Sanity check that KeyHash works as expected.
    compiler_internal::KeyHash hash;
    EXPECT_EQ(hash(Key{"foo", {int_qtype, float_qtype}}),
              hash(LookupKey({"foo", {int_1, float_1}})));
    EXPECT_NE(hash(Key{"foo", {int_qtype, float_qtype}}),
              hash(LookupKey({"foo", {float_1, int_1}})));
  }
}

TEST(EvalCompilerCacheTest, CacheHits) {
  auto tv_int_1 = arolla::TypedValue::FromValue(1);
  auto tv_int_2 = arolla::TypedValue::FromValue(2);
  auto tv_float_1 = arolla::TypedValue::FromValue(1.0f);
  auto int_1 = tv_int_1.AsRef();
  auto int_2 = tv_int_2.AsRef();
  auto float_1 = tv_float_1.AsRef();

  EXPECT_FALSE(compiler_internal::Lookup("math.add", {int_1, float_1}));
  EXPECT_OK(EvalExpr("math.add", {int_1, float_1}).status());
  EXPECT_TRUE(compiler_internal::Lookup("math.add", {int_1, float_1}));
  EXPECT_TRUE(compiler_internal::Lookup("math.add", {int_2, float_1}));

  ClearCompilationCache();
  EXPECT_FALSE(compiler_internal::Lookup("math.add", {int_1, float_1}));
  EXPECT_FALSE(compiler_internal::Lookup("math.add", {int_2, float_1}));
}

TEST(EvalCompilerCacheTest, DifferentOrderOfOperands) {
  auto tv_int_1 = arolla::TypedValue::FromValue(1);
  auto tv_int_2 = arolla::TypedValue::FromValue(2);
  auto tv_float_1 = arolla::TypedValue::FromValue(1.0f);
  auto int_1 = tv_int_1.AsRef();
  auto int_2 = tv_int_2.AsRef();
  auto float_1 = tv_float_1.AsRef();

  EXPECT_FALSE(compiler_internal::Lookup("math.add", {int_1, float_1}));
  EXPECT_OK(EvalExpr("math.add", {int_1, float_1}).status());
  EXPECT_TRUE(compiler_internal::Lookup("math.add", {int_1, float_1}));
  EXPECT_TRUE(compiler_internal::Lookup("math.add", {int_2, float_1}));

  EXPECT_FALSE(compiler_internal::Lookup("math.add", {float_1, int_1}));
  EXPECT_OK(EvalExpr("math.add", {float_1, int_1}).status());
  EXPECT_TRUE(compiler_internal::Lookup("math.add", {float_1, int_1}));
}

}  // namespace
}  // namespace koladata::ops
