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
#include "koladata/operators/matrix_helpers.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata::ops::matrix_helpers {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

// Helper: build a 2D JaggedShape for a single matrix (m x n).
absl::StatusOr<JaggedShape> MakeMatrixShape(int64_t m, int64_t n) {
  // Row edge: 1 parent -> m children.
  auto row_edge = test::EdgeFromSplitPoints({0, m});
  // Col edge: m parents -> n children each.
  std::vector<int64_t> col_splits(m + 1);
  col_splits[0] = 0;
  for (int64_t i = 0; i < m; ++i) {
    col_splits[i + 1] = col_splits[i] + n;
  }
  auto col_edge = test::EdgeFromSplitPoints(col_splits);
  auto shape = JaggedShape::FlatFromSize(1);
  return shape.AddDims({std::move(row_edge), std::move(col_edge)});
}

// Helper: build a 1D vector JaggedShape (single vector of size k).
absl::StatusOr<JaggedShape> MakeVectorShape(int64_t k) {
  auto edge = test::EdgeFromSplitPoints({0, k});
  auto shape = JaggedShape::FlatFromSize(1);
  return shape.AddDims({std::move(edge)});
}

// =========================================================================
// ExtractFlat tests
// =========================================================================

TEST(ExtractFlatTest, Int64FullyPresent) {
  auto ds = test::DataSlice<int64_t>({1, 2, 3, 4});
  auto result = ExtractFlat<int64_t>(ds);
  EXPECT_THAT(result, ElementsAre(1, 2, 3, 4));
}

TEST(ExtractFlatTest, Float64FullyPresent) {
  auto ds = test::DataSlice<double>({1.5, 2.5, 3.5});
  auto result = ExtractFlat<double>(ds);
  EXPECT_THAT(result, ElementsAre(1.5, 2.5, 3.5));
}

TEST(ExtractFlatTest, Float32FullyPresent) {
  auto ds = test::DataSlice<float>({1.0f, 2.0f, 3.0f});
  auto result = ExtractFlat<float>(ds);
  EXPECT_THAT(result, ElementsAre(1.0f, 2.0f, 3.0f));
}

TEST(ExtractFlatTest, Int32FullyPresent) {
  auto ds = test::DataSlice<int32_t>({10, 20, 30});
  auto result = ExtractFlat<int32_t>(ds);
  EXPECT_THAT(result, ElementsAre(10, 20, 30));
}

TEST(ExtractFlatTest, Int64WithMissing) {
  auto ds = test::DataSlice<int64_t>({1, std::nullopt, 3, std::nullopt});
  auto result = ExtractFlat<int64_t>(ds);
  EXPECT_THAT(result, ElementsAre(1, 0, 3, 0));
}

TEST(ExtractFlatTest, Float64WithMissing) {
  auto ds = test::DataSlice<double>({std::nullopt, 2.0, std::nullopt, 4.0});
  auto result = ExtractFlat<double>(ds);
  EXPECT_THAT(result, ElementsAre(0.0, 2.0, 0.0, 4.0));
}

TEST(ExtractFlatTest, EmptyAndUnknown) {
  auto ds = test::EmptyDataSlice(3, schema::kFloat64);
  auto result = ExtractFlat<double>(ds);
  EXPECT_THAT(result, ElementsAre(0.0, 0.0, 0.0));
}

TEST(ExtractFlatTest, EmptySize) {
  auto ds = test::DataSlice<int64_t>({});
  auto result = ExtractFlat<int64_t>(ds);
  EXPECT_TRUE(result.empty());
}

// =========================================================================
// BuildFromFlat tests
// =========================================================================

TEST(BuildFromFlatTest, Int64) {
  auto shape = JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, BuildFromFlat<int64_t>({10, 20, 30}, shape));
  EXPECT_EQ(ds.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  EXPECT_EQ(ds.GetShape().size(), 3);
  EXPECT_EQ(ds.present_count(), 3);
  EXPECT_THAT(ExtractFlat<int64_t>(ds), ElementsAre(10, 20, 30));
}

TEST(BuildFromFlatTest, Float32) {
  auto shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto ds, BuildFromFlat<float>({1.0f, 2.0f}, shape));
  EXPECT_EQ(ds.GetSchemaImpl(), internal::DataItem(schema::kFloat32));
  EXPECT_EQ(ds.GetShape().size(), 2);
  EXPECT_THAT(ExtractFlat<float>(ds), ElementsAre(1.0f, 2.0f));
}

TEST(BuildFromFlatTest, Int32) {
  auto shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto ds, BuildFromFlat<int32_t>({5, 6}, shape));
  EXPECT_EQ(ds.GetSchemaImpl(), internal::DataItem(schema::kInt32));
  EXPECT_THAT(ExtractFlat<int32_t>(ds), ElementsAre(5, 6));
}

TEST(BuildFromFlatTest, WithJaggedShape) {
  // Build a 2D shape: 1 parent -> [3 children]
  auto edge = test::EdgeFromSplitPoints({0, 3});
  auto base = JaggedShape::FlatFromSize(1);
  ASSERT_OK_AND_ASSIGN(auto shape, base.AddDims({std::move(edge)}));
  ASSERT_OK_AND_ASSIGN(auto ds, BuildFromFlat<double>({1.0, 2.0, 3.0}, shape));
  EXPECT_EQ(ds.GetShape().rank(), 2);
  EXPECT_EQ(ds.GetShape().size(), 3);
  EXPECT_THAT(ExtractFlat<double>(ds), ElementsAre(1.0, 2.0, 3.0));
}

// =========================================================================
// GetNarrowedMatrixSchema tests
// =========================================================================

TEST(GetNarrowedMatrixSchemaTest, Int32Schema) {
  auto ds = test::DataSlice<int32_t>({1, 2, 3});
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kInt32)));
}

TEST(GetNarrowedMatrixSchemaTest, Int64Schema) {
  auto ds = test::DataSlice<int64_t>({1, 2, 3});
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kInt64)));
}

TEST(GetNarrowedMatrixSchemaTest, Float32Schema) {
  auto ds = test::DataSlice<float>({1.0f, 2.0f});
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kFloat32)));
}

TEST(GetNarrowedMatrixSchemaTest, Float64Schema) {
  auto ds = test::DataSlice<double>({1.0, 2.0});
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kFloat64)));
}

TEST(GetNarrowedMatrixSchemaTest, NoneSchema) {
  auto ds = test::EmptyDataSlice(3, schema::kNone);
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kInt32)));
}

TEST(GetNarrowedMatrixSchemaTest, ObjectSchemaWithInt64Data) {
  // Create an OBJECT-schema DataSlice with int64 data.
  auto ds = test::DataSlice<int64_t>({1, 2, 3}, schema::kObject);
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kInt64)));
}

TEST(GetNarrowedMatrixSchemaTest, ObjectSchemaWithFloat32Data) {
  auto ds = test::DataSlice<float>({1.0f, 2.0f}, schema::kObject);
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kFloat32)));
}

TEST(GetNarrowedMatrixSchemaTest, ObjectSchemaEmpty) {
  auto ds = test::EmptyDataSlice(3, schema::kObject);
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              IsOkAndHolds(internal::DataItem(schema::kInt32)));
}

TEST(GetNarrowedMatrixSchemaTest, ObjectSchemaWithMixedData) {
  // Mixed dtype OBJECT schema should return an error.
  auto ds = test::MixedDataSlice<arolla::Text, double>(
      {"foo", std::nullopt}, {std::nullopt, 2.0}, JaggedShape::FlatFromSize(2),
      schema::kObject);
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(GetNarrowedMatrixSchemaTest, UnsupportedSchema) {
  // STRING schema should be unsupported for matrix operations.
  auto shape = JaggedShape::FlatFromSize(1);
  auto impl = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<arolla::Text>({arolla::Text("hello")}));
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSlice::Create(std::move(impl), std::move(shape),
                                         internal::DataItem(schema::kString)));
  EXPECT_THAT(GetNarrowedMatrixSchema(ds),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// =========================================================================
// ExtractVectorInfos tests
// =========================================================================

TEST(ExtractVectorInfosTest, SingleVector) {
  // 1D shape: [3]
  auto shape = JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractVectorInfos(shape));
  ASSERT_EQ(infos.size(), 1);
  EXPECT_EQ(infos[0].offset, 0);
  EXPECT_EQ(infos[0].m, 3);
  EXPECT_EQ(infos[0].n, 1);  // Vectors always have n=1.
}

TEST(ExtractVectorInfosTest, BatchOfVectors) {
  // 2D shape: 3 vectors of sizes [2, 3, 1].
  auto shape = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 5, 6}});
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractVectorInfos(shape));
  ASSERT_EQ(infos.size(), 3);
  EXPECT_EQ(infos[0].offset, 0);
  EXPECT_EQ(infos[0].m, 2);
  EXPECT_EQ(infos[0].n, 1);
  EXPECT_EQ(infos[1].offset, 2);
  EXPECT_EQ(infos[1].m, 3);
  EXPECT_EQ(infos[1].n, 1);
  EXPECT_EQ(infos[2].offset, 5);
  EXPECT_EQ(infos[2].m, 1);
  EXPECT_EQ(infos[2].n, 1);
}

TEST(ExtractVectorInfosTest, EmptyVector) {
  // 1D shape with 0 elements.
  auto shape = JaggedShape::FlatFromSize(0);
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractVectorInfos(shape));
  ASSERT_EQ(infos.size(), 1);
  EXPECT_EQ(infos[0].m, 0);
  EXPECT_EQ(infos[0].n, 1);
}

TEST(ExtractVectorInfosTest, ErrorOn0D) {
  auto shape = JaggedShape::Empty();
  EXPECT_THAT(ExtractVectorInfos(shape),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// =========================================================================
// ExtractMatrix2DInfos tests
// =========================================================================

TEST(ExtractMatrix2DInfosTest, Single2x3Matrix) {
  ASSERT_OK_AND_ASSIGN(auto shape, MakeMatrixShape(2, 3));
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractMatrix2DInfos(shape));
  ASSERT_EQ(infos.size(), 1);
  EXPECT_EQ(infos[0].offset, 0);
  EXPECT_EQ(infos[0].m, 2);
  EXPECT_EQ(infos[0].n, 3);
}

TEST(ExtractMatrix2DInfosTest, BatchOf2Matrices) {
  // 3D shape: batch of 2 matrices, 2x2 and 3x2.
  // Batch edge: 1 -> 2
  // Row edge: [0, 2, 5] (2 rows + 3 rows)
  // Col edge: [0, 2, 4, 6, 8, 10] (each row has 2 cols)
  auto shape =
      test::ShapeFromSplitPoints({{0, 2}, {0, 2, 5}, {0, 2, 4, 6, 8, 10}});
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractMatrix2DInfos(shape));
  ASSERT_EQ(infos.size(), 2);
  EXPECT_EQ(infos[0].offset, 0);
  EXPECT_EQ(infos[0].m, 2);
  EXPECT_EQ(infos[0].n, 2);
  EXPECT_EQ(infos[1].offset, 4);
  EXPECT_EQ(infos[1].m, 3);
  EXPECT_EQ(infos[1].n, 2);
}

TEST(ExtractMatrix2DInfosTest, EmptyMatrix0x0) {
  // 2D shape: single 0x0 matrix (no rows).
  auto shape = test::ShapeFromSplitPoints({{0, 0}, {0}});
  ASSERT_OK_AND_ASSIGN(auto infos, ExtractMatrix2DInfos(shape));
  ASSERT_EQ(infos.size(), 1);
  EXPECT_EQ(infos[0].m, 0);
  EXPECT_EQ(infos[0].n, 0);
}

TEST(ExtractMatrix2DInfosTest, NonUniformRowsFails) {
  // 2D shape: 2 rows with different widths (3, 2) -> non-uniform.
  auto shape = test::ShapeFromSplitPoints({{0, 2}, {0, 3, 5}});
  EXPECT_THAT(ExtractMatrix2DInfos(shape),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExtractMatrix2DInfosTest, ErrorOn1D) {
  auto shape = JaggedShape::FlatFromSize(5);
  EXPECT_THAT(ExtractMatrix2DInfos(shape),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(ExtractMatrix2DInfosTest, ErrorOn0D) {
  auto shape = JaggedShape::Empty();
  EXPECT_THAT(ExtractMatrix2DInfos(shape),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// =========================================================================
// BroadcastBatchDims tests
// =========================================================================

TEST(BroadcastBatchDimsTest, BothScalar) {
  // Both inputs have 0 batch dims -> 1 output batch element.
  auto shape_x = JaggedShape::FlatFromSize(3);  // 1D vector
  auto shape_y = JaggedShape::FlatFromSize(4);  // 1D vector
  ASSERT_OK_AND_ASSIGN(auto bc, BroadcastBatchDims(shape_x, 0, shape_y, 0));
  EXPECT_EQ(bc.out_batch_size, 1);
  EXPECT_THAT(bc.x_batch_map, ElementsAre(0));
  EXPECT_THAT(bc.y_batch_map, ElementsAre(0));
}

TEST(BroadcastBatchDimsTest, IdenticalBatchDims) {
  // Both inputs have batch shape [2].
  auto shape_x = test::ShapeFromSplitPoints({{0, 2}, {0, 3, 6}});
  auto shape_y = test::ShapeFromSplitPoints({{0, 2}, {0, 4, 8}});
  ASSERT_OK_AND_ASSIGN(auto bc, BroadcastBatchDims(shape_x, 1, shape_y, 1));
  EXPECT_EQ(bc.out_batch_size, 2);
  EXPECT_THAT(bc.x_batch_map, ElementsAre(0, 1));
  EXPECT_THAT(bc.y_batch_map, ElementsAre(0, 1));
}

TEST(BroadcastBatchDimsTest, XLongerBatch) {
  // x has batch shape [2, [2,3]] -> 5 batch elements.
  // y has batch shape [2] -> 2 batch elements, broadcast.
  auto shape_x =
      test::ShapeFromSplitPoints({{0, 2}, {0, 2, 5}, {0, 3, 6, 9, 12, 15}});
  auto shape_y = test::ShapeFromSplitPoints({{0, 2}, {0, 4, 8}});
  // x_batch_rank = 2 (first 2 edges), y_batch_rank = 1 (first 1 edge).
  ASSERT_OK_AND_ASSIGN(auto bc, BroadcastBatchDims(shape_x, 2, shape_y, 1));
  EXPECT_EQ(bc.out_batch_size, 5);
  // x maps identity.
  EXPECT_THAT(bc.x_batch_map, ElementsAre(0, 1, 2, 3, 4));
  // y is broadcast: y[0] covers x[0],x[1]; y[1] covers x[2],x[3],x[4].
  EXPECT_THAT(bc.y_batch_map, ElementsAre(0, 0, 1, 1, 1));
}

TEST(BroadcastBatchDimsTest, YLongerBatch) {
  // Same as above but reversed: y is longer.
  auto shape_y =
      test::ShapeFromSplitPoints({{0, 2}, {0, 2, 5}, {0, 3, 6, 9, 12, 15}});
  auto shape_x = test::ShapeFromSplitPoints({{0, 2}, {0, 4, 8}});
  ASSERT_OK_AND_ASSIGN(auto bc, BroadcastBatchDims(shape_x, 1, shape_y, 2));
  EXPECT_EQ(bc.out_batch_size, 5);
  EXPECT_THAT(bc.x_batch_map, ElementsAre(0, 0, 1, 1, 1));
  EXPECT_THAT(bc.y_batch_map, ElementsAre(0, 1, 2, 3, 4));
}

TEST(BroadcastBatchDimsTest, MismatchedBatchDimsFails) {
  // x batch shape [2], y batch shape [3] -> mismatch.
  auto shape_x = test::ShapeFromSplitPoints({{0, 2}, {0, 3, 6}});
  auto shape_y = test::ShapeFromSplitPoints({{0, 3}, {0, 4, 8, 12}});
  EXPECT_THAT(BroadcastBatchDims(shape_x, 1, shape_y, 1),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(BroadcastBatchDimsTest, ScalarVsBatched) {
  // x has 0 batch dims (scalar), y has batch shape [3].
  auto shape_x = JaggedShape::FlatFromSize(4);  // 1D vector
  auto shape_y = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 5, 7}});
  ASSERT_OK_AND_ASSIGN(auto bc, BroadcastBatchDims(shape_x, 0, shape_y, 1));
  EXPECT_EQ(bc.out_batch_size, 3);
  // x is broadcast: all map to 0.
  EXPECT_THAT(bc.x_batch_map, ElementsAre(0, 0, 0));
  EXPECT_THAT(bc.y_batch_map, ElementsAre(0, 1, 2));
}

// =========================================================================
// SetupBroadcastBinaryOp tests
// =========================================================================

TEST(SetupBroadcastBinaryOpTest, MatrixMatrix) {
  // x: 2x3, y: 3x2.
  ASSERT_OK_AND_ASSIGN(auto shape_x, MakeMatrixShape(2, 3));
  ASSERT_OK_AND_ASSIGN(auto shape_y, MakeMatrixShape(3, 2));
  ASSERT_OK_AND_ASSIGN(auto setup,
                       SetupBroadcastBinaryOp(shape_x, shape_y, 2, 2));
  EXPECT_EQ(setup.broadcast_result.out_batch_size, 1);
  ASSERT_EQ(setup.x_infos.size(), 1);
  EXPECT_EQ(setup.x_infos[0].m, 2);
  EXPECT_EQ(setup.x_infos[0].n, 3);
  ASSERT_EQ(setup.y_infos.size(), 1);
  EXPECT_EQ(setup.y_infos[0].m, 3);
  EXPECT_EQ(setup.y_infos[0].n, 2);
}

TEST(SetupBroadcastBinaryOpTest, VectorVector) {
  // x: vector(3), y: vector(3). x_trailing=1, y_trailing=1.
  ASSERT_OK_AND_ASSIGN(auto shape_x, MakeVectorShape(3));
  ASSERT_OK_AND_ASSIGN(auto shape_y, MakeVectorShape(3));
  ASSERT_OK_AND_ASSIGN(auto setup,
                       SetupBroadcastBinaryOp(shape_x, shape_y, 1, 1));
  // x vector with trailing_dims=1 is the LEFT operand, so it's transposed
  // to a row vector (1, 3).
  ASSERT_EQ(setup.x_infos.size(), 1);
  EXPECT_EQ(setup.x_infos[0].m, 1);
  EXPECT_EQ(setup.x_infos[0].n, 3);
  // y with trailing_dims=1 remains a column vector (3, 1).
  ASSERT_EQ(setup.y_infos.size(), 1);
  EXPECT_EQ(setup.y_infos[0].m, 3);
  EXPECT_EQ(setup.y_infos[0].n, 1);
}

TEST(SetupBroadcastBinaryOpTest, VectorMatrix) {
  // x: vector(3) as left operand (trailing_dims=1, transposed to 1x3).
  // y: 3x2 matrix (trailing_dims=2).
  ASSERT_OK_AND_ASSIGN(auto shape_x, MakeVectorShape(3));
  ASSERT_OK_AND_ASSIGN(auto shape_y, MakeMatrixShape(3, 2));
  ASSERT_OK_AND_ASSIGN(auto setup,
                       SetupBroadcastBinaryOp(shape_x, shape_y, 1, 2));
  ASSERT_EQ(setup.x_infos.size(), 1);
  EXPECT_EQ(setup.x_infos[0].m, 1);
  EXPECT_EQ(setup.x_infos[0].n, 3);
  ASSERT_EQ(setup.y_infos.size(), 1);
  EXPECT_EQ(setup.y_infos[0].m, 3);
  EXPECT_EQ(setup.y_infos[0].n, 2);
}

// =========================================================================
// DispatchNumericBinaryOp tests
// =========================================================================
//
// DispatchNumericBinaryOp narrows the schemas of both inputs, finds a common
// numeric type, casts both inputs to that type, calls the user-provided
// compute_fn, and wraps the result in a DataSlice with the correct output
// schema. The tests below exercise each dispatch path (INT32, INT64, FLOAT32,
// FLOAT64), mixed-type promotion, OBJECT schema handling, missing value
// treatment (missing → 0 via ExtractFlat), and error cases.
//
// We use a trivial "element-wise add" as the compute_fn throughout, since
// the purpose is to test the dispatch/cast/build logic, not the math.

namespace {

// A simple element-wise add, suitable as a compute_fn for
// DispatchNumericBinaryOp.
auto kAddFn = [](const auto& x, const auto& y, auto& result) {
  for (int i = 0; i < x.size(); ++i) {
    result[i] = x[i] + y[i];
  }
};

}  // namespace

TEST(DispatchNumericBinaryOpTest, BothInt64) {
  auto x = test::DataSlice<int64_t>({1, 2, 3});
  auto y = test::DataSlice<int64_t>({10, 20, 30});
  auto out_shape = JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 3, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  EXPECT_THAT(ExtractFlat<int64_t>(result), ElementsAre(11, 22, 33));
}

TEST(DispatchNumericBinaryOpTest, BothInt32) {
  auto x = test::DataSlice<int32_t>({1, 2, 3});
  auto y = test::DataSlice<int32_t>({10, 20, 30});
  auto out_shape = JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 3, std::move(out_shape), kAddFn));
  // INT32 + INT32 → compute as INT64, output as INT32.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt32));
  EXPECT_THAT(ExtractFlat<int32_t>(result), ElementsAre(11, 22, 33));
}

TEST(DispatchNumericBinaryOpTest, BothFloat64) {
  auto x = test::DataSlice<double>({1.5, 2.5});
  auto y = test::DataSlice<double>({10.0, 20.0});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(11.5, 22.5));
}

TEST(DispatchNumericBinaryOpTest, BothFloat32) {
  auto x = test::DataSlice<float>({1.0f, 2.0f});
  auto y = test::DataSlice<float>({10.0f, 20.0f});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // FLOAT32 + FLOAT32 → compute as FLOAT64, output as FLOAT32.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat32));
  EXPECT_THAT(ExtractFlat<float>(result), ElementsAre(11.0f, 22.0f));
}

// --- Mixed type promotion ---

TEST(DispatchNumericBinaryOpTest, Int32PlusInt64ProducesInt64) {
  auto x = test::DataSlice<int32_t>({1, 2});
  auto y = test::DataSlice<int64_t>({10, 20});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // CommonSchema(INT32, INT64) = INT64.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  EXPECT_THAT(ExtractFlat<int64_t>(result), ElementsAre(11, 22));
}

TEST(DispatchNumericBinaryOpTest, Int64PlusFloat32ProducesFloat32) {
  auto x = test::DataSlice<int64_t>({1, 2});
  auto y = test::DataSlice<float>({10.5f, 20.5f});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // CommonSchema(INT64, FLOAT32) = FLOAT32.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat32));
  EXPECT_THAT(ExtractFlat<float>(result), ElementsAre(11.5f, 22.5f));
}

TEST(DispatchNumericBinaryOpTest, Int64PlusFloat64ProducesFloat64) {
  auto x = test::DataSlice<int64_t>({1, 2});
  auto y = test::DataSlice<double>({10.5, 20.5});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // CommonSchema(INT64, FLOAT64) = FLOAT64.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(11.5, 22.5));
}

TEST(DispatchNumericBinaryOpTest, Float32PlusFloat64ProducesFloat64) {
  auto x = test::DataSlice<float>({1.0f, 2.0f});
  auto y = test::DataSlice<double>({10.5, 20.5});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // CommonSchema(FLOAT32, FLOAT64) = FLOAT64.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(11.5, 22.5));
}

TEST(DispatchNumericBinaryOpTest, Int32PlusFloat64ProducesFloat64) {
  auto x = test::DataSlice<int32_t>({1, 2});
  auto y = test::DataSlice<double>({10.5, 20.5});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(11.5, 22.5));
}

// --- Missing values (treated as 0 by ExtractFlat) ---

TEST(DispatchNumericBinaryOpTest, MissingValuesTreatedAsZero) {
  // Missing values in inputs should be treated as 0.
  auto x = test::DataSlice<int64_t>({std::nullopt, std::nullopt, 3});
  auto y = test::DataSlice<int64_t>({std::nullopt, 20, 30});
  auto out_shape = JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 3, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  // x = [0, 0, 3], y = [0, 20, 30] → result = [0, 20, 33].
  EXPECT_THAT(ExtractFlat<int64_t>(result), ElementsAre(0, 20, 33));
}

TEST(DispatchNumericBinaryOpTest, AllMissingInput) {
  // Both inputs are entirely missing → all zeros.
  auto x = test::DataSlice<double>({std::nullopt, std::nullopt});
  auto y = test::DataSlice<double>({std::nullopt, std::nullopt});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(0.0, 0.0));
}

// --- OBJECT schema ---

TEST(DispatchNumericBinaryOpTest, ObjectSchemaInt64) {
  // OBJECT schema with int64 data → output should be wrapped as OBJECT.
  auto x = test::DataSlice<int64_t>({1, 2}, schema::kObject);
  auto y = test::DataSlice<int64_t>({10, 20}, schema::kObject);
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  // Output schema should be OBJECT when either input is OBJECT.
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kObject));
  // Value correctness is verified by non-OBJECT tests; here we only check
  // that the OBJECT wrapper is applied and the data is fully present.
  // (ExtractFlat DCHECKs that the schema matches T, so we cannot use it on
  // OBJECT-schema DataSlices.)
  EXPECT_EQ(result.present_count(), 2);
}

TEST(DispatchNumericBinaryOpTest, ObjectSchemaFloat64) {
  auto x = test::DataSlice<double>({1.5, 2.5}, schema::kObject);
  auto y = test::DataSlice<double>({10.0, 20.0}, schema::kObject);
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kObject));
  EXPECT_EQ(result.present_count(), 2);
}

TEST(DispatchNumericBinaryOpTest, OneObjectOneExplicit) {
  // One input OBJECT, one explicit → output OBJECT.
  auto x = test::DataSlice<int64_t>({1, 2}, schema::kObject);
  auto y = test::DataSlice<int64_t>({10, 20});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kObject));
  EXPECT_EQ(result.present_count(), 2);
}

// --- NONE schema (all-missing DataSlice) ---

TEST(DispatchNumericBinaryOpTest, NoneSchemaProducesInt32) {
  // NONE + NONE: GetNarrowedMatrixSchema returns INT32, so
  // CommonSchema(INT32, INT32) = INT32 → compute as INT64, output as INT32.
  auto x = test::EmptyDataSlice(2, schema::kNone);
  auto y = test::EmptyDataSlice(2, schema::kNone);
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt32));
  EXPECT_THAT(ExtractFlat<int32_t>(result), ElementsAre(0, 0));
}

TEST(DispatchNumericBinaryOpTest, NonePlusFloat64ProducesFloat64) {
  // NONE + FLOAT64: narrowed NONE→INT32, CommonSchema(INT32, FLOAT64)=FLOAT64.
  auto x = test::EmptyDataSlice(2, schema::kNone);
  auto y = test::DataSlice<double>({10.5, 20.5});
  auto out_shape = JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 2, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  // x is all-missing → extracted as [0.0, 0.0].
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(10.5, 20.5));
}

// --- Empty inputs ---

TEST(DispatchNumericBinaryOpTest, EmptyInputs) {
  auto x = test::DataSlice<int64_t>({});
  auto y = test::DataSlice<int64_t>({});
  auto out_shape = JaggedShape::FlatFromSize(0);
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 0, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  EXPECT_EQ(result.GetShape().size(), 0);
}

// --- Output shape differs from input ---

TEST(DispatchNumericBinaryOpTest, OutputSizeDiffersFromInput) {
  // Simulate an op where the output has a different size than the inputs,
  // e.g. matmul of (1×3) × (3×1) → (1×1), so out_total=1.
  auto x = test::DataSlice<double>({1.0, 2.0, 3.0});
  auto y = test::DataSlice<double>({4.0, 5.0, 6.0});
  auto out_shape = JaggedShape::FlatFromSize(1);
  // Compute a dot product (sum of element-wise products) as a 1-element output.
  auto dot_fn = [](const auto& xd, const auto& yd, auto& result) {
    result[0] = 0;
    for (int i = 0; i < xd.size(); ++i) {
      result[0] += xd[i] * yd[i];
    }
  };
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 1, std::move(out_shape), dot_fn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kFloat64));
  // 1*4 + 2*5 + 3*6 = 32.
  EXPECT_THAT(ExtractFlat<double>(result), ElementsAre(32.0));
}

// --- JaggedShape output ---

TEST(DispatchNumericBinaryOpTest, JaggedOutputShape) {
  // Verify that the output DataSlice correctly adopts the provided
  // JaggedShape, not just a flat shape.
  auto x = test::DataSlice<int64_t>({1, 2, 3, 4, 5, 6});
  auto y = test::DataSlice<int64_t>({10, 20, 30, 40, 50, 60});
  // Shape: batch=2, inner=[3, 3].
  auto out_shape = test::ShapeFromSplitPoints({{0, 2}, {0, 3, 6}});
  ASSERT_OK_AND_ASSIGN(auto result, DispatchNumericBinaryOp(
                                        x, y, 6, std::move(out_shape), kAddFn));
  EXPECT_EQ(result.GetSchemaImpl(), internal::DataItem(schema::kInt64));
  EXPECT_EQ(result.GetShape().rank(), 2);
  EXPECT_EQ(result.GetShape().size(), 6);
  EXPECT_THAT(ExtractFlat<int64_t>(result),
              ElementsAre(11, 22, 33, 44, 55, 66));
}

// --- Error cases ---

TEST(DispatchNumericBinaryOpTest, NonNumericSchemaFails) {
  // STRING schema is not supported for matrix operations.
  auto shape = JaggedShape::FlatFromSize(1);
  auto impl = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<arolla::Text>({arolla::Text("hello")}));
  ASSERT_OK_AND_ASSIGN(auto x,
                       DataSlice::Create(std::move(impl), shape,
                                         internal::DataItem(schema::kString)));
  auto y = test::DataSlice<int64_t>({1});
  auto out_shape = JaggedShape::FlatFromSize(1);
  EXPECT_THAT(DispatchNumericBinaryOp(x, y, 1, std::move(out_shape), kAddFn),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(DispatchNumericBinaryOpTest, MixedObjectSchemaFails) {
  // OBJECT schema with mixed dtypes (e.g. Text + double) should fail.
  auto x = test::MixedDataSlice<arolla::Text, double>(
      {"foo", std::nullopt}, {std::nullopt, 2.0}, JaggedShape::FlatFromSize(2),
      schema::kObject);
  auto y = test::DataSlice<double>({1.0, 2.0});
  auto out_shape = JaggedShape::FlatFromSize(2);
  EXPECT_THAT(DispatchNumericBinaryOp(x, y, 2, std::move(out_shape), kAddFn),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// =========================================================================
// BuildBatchedMatrixShape tests
// =========================================================================

TEST(BuildBatchedMatrixShapeTest, SingleMatrix) {
  auto batch_shape = JaggedShape::FlatFromSize(1);
  ASSERT_OK_AND_ASSIGN(auto shape,
                       BuildBatchedMatrixShape(batch_shape, {2}, {3}));
  EXPECT_EQ(shape.rank(), 3);  // batch(1) + row + col
  EXPECT_EQ(shape.size(), 6);  // 2 * 3
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 1));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(), ElementsAre(0, 2));
  EXPECT_THAT(shape.edges()[2].edge_values().values.span(),
              ElementsAre(0, 3, 6));
}

TEST(BuildBatchedMatrixShapeTest, BatchOfMatrices) {
  auto batch_shape = JaggedShape::FlatFromSize(2);
  // First matrix: 2x3, second: 3x2.
  std::vector<int64_t> rows = {2, 4};
  std::vector<int64_t> cols = {3, 5};
  ASSERT_OK_AND_ASSIGN(auto shape,
                       BuildBatchedMatrixShape(batch_shape, rows, cols));
  EXPECT_EQ(shape.rank(), 3);   // batch + row + col
  EXPECT_EQ(shape.size(), 26);  // 2*3 + 4*5 = 26
  ASSERT_EQ(shape.edges().size(), 3);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 2));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(),
              ElementsAre(0, 2, 6));
  EXPECT_THAT(shape.edges()[2].edge_values().values.span(),
              ElementsAre(0, 3, 6, 11, 16, 21, 26));
}

TEST(BuildBatchedMatrixShapeTest, EmptyBatch) {
  auto batch_shape = JaggedShape::FlatFromSize(0);
  ASSERT_OK_AND_ASSIGN(auto shape,
                       BuildBatchedMatrixShape(batch_shape, {}, {}));
  EXPECT_EQ(shape.rank(), 3);
  EXPECT_EQ(shape.size(), 0);
  ASSERT_EQ(shape.edges().size(), 3);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 0));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(), ElementsAre(0));
  EXPECT_THAT(shape.edges()[2].edge_values().values.span(), ElementsAre(0));
}

TEST(BuildBatchedMatrixShapeTest, ZeroDimMatrix) {
  // A 0x5 matrix.
  auto batch_shape = JaggedShape::FlatFromSize(1);
  ASSERT_OK_AND_ASSIGN(auto shape,
                       BuildBatchedMatrixShape(batch_shape, {0}, {5}));
  EXPECT_EQ(shape.rank(), 3);
  EXPECT_EQ(shape.size(), 0);  // 0 rows -> 0 elements.
  ASSERT_EQ(shape.edges().size(), 3);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 1));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(), ElementsAre(0, 0));
  EXPECT_THAT(shape.edges()[2].edge_values().values.span(), ElementsAre(0));
}

// =========================================================================
// BuildBatchedVectorShape tests
// =========================================================================

TEST(BuildBatchedVectorShapeTest, SingleVector) {
  auto batch_shape = JaggedShape::FlatFromSize(1);
  ASSERT_OK_AND_ASSIGN(auto shape, BuildBatchedVectorShape(batch_shape, {5}));
  EXPECT_EQ(shape.rank(), 2);  // batch + vector dim
  EXPECT_EQ(shape.size(), 5);
  ASSERT_EQ(shape.edges().size(), 2);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 1));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(), ElementsAre(0, 5));
}

TEST(BuildBatchedVectorShapeTest, BatchOfVectors) {
  auto batch_shape = JaggedShape::FlatFromSize(3);
  std::vector<int64_t> counts = {2, 3, 4};
  ASSERT_OK_AND_ASSIGN(auto shape,
                       BuildBatchedVectorShape(batch_shape, counts));
  EXPECT_EQ(shape.rank(), 2);
  EXPECT_EQ(shape.size(), 9);  // 2 + 3 + 4
  ASSERT_EQ(shape.edges().size(), 2);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 3));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(),
              ElementsAre(0, 2, 5, 9));
}

TEST(BuildBatchedVectorShapeTest, EmptyVector) {
  auto batch_shape = JaggedShape::FlatFromSize(1);
  ASSERT_OK_AND_ASSIGN(auto shape, BuildBatchedVectorShape(batch_shape, {0}));
  EXPECT_EQ(shape.rank(), 2);
  EXPECT_EQ(shape.size(), 0);
  ASSERT_EQ(shape.edges().size(), 2);
  EXPECT_THAT(shape.edges()[0].edge_values().values.span(), ElementsAre(0, 1));
  EXPECT_THAT(shape.edges()[1].edge_values().values.span(), ElementsAre(0, 0));
}

// =========================================================================
// Integration-style tests
// =========================================================================

TEST(IntegrationTest, ExtractAndBuildRoundTrip) {
  // Build a flat DataSlice, extract, and rebuild. Verify the result.
  auto shape = JaggedShape::FlatFromSize(4);
  std::vector<int64_t> data = {10, 20, 30, 40};
  ASSERT_OK_AND_ASSIGN(auto ds,
                       BuildFromFlat<int64_t>({10, 20, 30, 40}, shape));
  auto extracted = ExtractFlat<int64_t>(ds);
  EXPECT_THAT(extracted, ElementsAre(10, 20, 30, 40));
}

}  // namespace
}  // namespace koladata::ops::matrix_helpers
