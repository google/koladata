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
#include "koladata/shape_utils.h"

#include <initializer_list>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/status.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/errors.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::shape {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::CreateFullDenseArray;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(ShapeUtilsTest, GetCommonShape) {
  {
    // 1 input.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::CreateWithSchemaFromData(
                             internal::DataSliceImpl::Create(values), shape_1));
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape({ds}));
    EXPECT_THAT(shape_1, IsEquivalentTo(common_shape));
  }
  {
    // Same shape.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_1));
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape({ds_1, ds_2}));
    EXPECT_THAT(shape_1, IsEquivalentTo(common_shape));
  }
  {
    // Normal expansion required.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto shape_2 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 6}});
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_2));
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape({ds_1, ds_2}));
    EXPECT_THAT(shape_2, IsEquivalentTo(common_shape));
  }
  {
    // Non-compatible shapes.
    auto shape_1 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 6}});
    auto shape_2 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 5}});
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_2));
    absl::Status status = GetCommonShape({ds_1, ds_2}).status();
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                                 HasSubstr("shapes are not compatible")));
    const internal::ShapeAlignmentError* shape_alignment_error =
        arolla::GetPayload<internal::ShapeAlignmentError>(status);
    ASSERT_NE(shape_alignment_error, nullptr);
    EXPECT_EQ(shape_alignment_error->common_shape_id, 0);
    EXPECT_EQ(shape_alignment_error->incompatible_shape_id, 1);
  }
  {
    // No inputs.
    std::vector<DataSlice> slices;
    EXPECT_THAT(
        GetCommonShape(slices),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("computing a common shape requires at least 1 input")));
  }
}

TEST(ShapeUtilsTest, Align) {
  {
    // 1 input.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::CreateWithSchemaFromData(
                             internal::DataSliceImpl::Create(values), shape_1));
    ASSERT_OK_AND_ASSIGN(auto aligned_slices, Align(std::vector{ds}));
    EXPECT_EQ(aligned_slices.size(), 1);
    EXPECT_THAT(aligned_slices[0], IsEquivalentTo(ds));
  }
  {
    // Same shape.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_1));
    ASSERT_OK_AND_ASSIGN(auto aligned_slices, Align(std::vector{ds_1, ds_2}));
    EXPECT_EQ(aligned_slices.size(), 2);
    EXPECT_THAT(aligned_slices[0], IsEquivalentTo(ds_1));
    EXPECT_THAT(aligned_slices[1], IsEquivalentTo(ds_2));
  }
  {
    // Non-compatible shapes.
    auto shape_1 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 6}});
    auto shape_2 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 5}});
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_2));
    EXPECT_THAT(Align(std::vector{ds_1, ds_2}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible")));
  }
  {
    // Compatible, but different shapes that are being aligned.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto shape_2 = test::ShapeFromSplitPoints({{0, 3}, {0, 2, 4, 6}});
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2, DataSlice::CreateWithSchemaFromData(
                       internal::DataSliceImpl::Create(values_2), shape_2));

    ASSERT_OK_AND_ASSIGN(auto aligned, Align(std::vector{ds_1, ds_2}));
    EXPECT_THAT(aligned[0].GetShape(), IsEquivalentTo(shape_2));
    EXPECT_THAT(aligned[0].slice().template values<int>(),
                ElementsAre(1, 1, 2, 2, 3, 3));
    EXPECT_THAT(aligned[1].GetShape(), IsEquivalentTo(shape_2));
    EXPECT_THAT(aligned[1].slice().template values<int>(),
                ElementsAre(1, 2, 3, 4, 5, 6));
  }
}

TEST(ShapeUtilsTest, AlignNonScalars) {
  {
    // 1 input - non-scalar.
    ASSERT_OK_AND_ASSIGN(auto ds, DataSlice::CreateWithSchemaFromData(
                                      internal::DataSliceImpl::Create(
                                          CreateFullDenseArray<int>({1, 2, 3})),
                                      DataSlice::JaggedShape::FlatFromSize(3)));
    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars({ds}));
    EXPECT_EQ(aligned_slices.size(), 1);
    EXPECT_THAT(aligned_slices[0], IsEquivalentTo(ds));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds.GetShape()));
  }
  {
    // 1 input - scalar.
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1})),
            DataSlice::JaggedShape::Empty()));
    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars({ds}));
    EXPECT_EQ(aligned_slices.size(), 1);
    EXPECT_THAT(aligned_slices[0], IsEquivalentTo(ds));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds.GetShape()));
  }
  {
    // Mix of scalars and non-scalars.
    ASSERT_OK_AND_ASSIGN(
        auto ds_rank_0,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1})),
            DataSlice::JaggedShape::Empty()));
    ASSERT_OK_AND_ASSIGN(auto ds_rank_1,
                         DataSlice::CreateWithSchemaFromData(
                             internal::DataSliceImpl::Create(
                                 CreateFullDenseArray<int>({1, 2, 3})),
                             DataSlice::JaggedShape::FlatFromSize(3)));
    ASSERT_OK_AND_ASSIGN(
        auto ds_rank_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(
                CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6})),
            DataSlice::JaggedShape::FromEdges(
                {
                    DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3)
                        .value(),
                    DataSlice::JaggedShape::Edge::FromUniformGroups(3, 2)
                        .value(),
                })
                .value()));

    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars({ds_rank_0, ds_rank_1, ds_rank_2}));
    EXPECT_EQ(aligned_slices.size(), 3);
    EXPECT_THAT(aligned_slices[0], IsEquivalentTo(ds_rank_0));
    EXPECT_THAT(aligned_slices[1].GetShape(),
                IsEquivalentTo(ds_rank_2.GetShape()));
    EXPECT_THAT(aligned_slices[1].slice().template values<int>(),
                ElementsAre(1, 1, 2, 2, 3, 3));
    EXPECT_THAT(aligned_slices[2].GetShape(),
                IsEquivalentTo(ds_rank_2.GetShape()));
    EXPECT_THAT(aligned_slices[2].slice().template values<int>(),
                ElementsAre(1, 2, 3, 4, 5, 6));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds_rank_2.GetShape()));
  }
  {
    // Non-compatible shapes.
    ASSERT_OK_AND_ASSIGN(auto ds_1,
                         DataSlice::CreateWithSchemaFromData(
                             internal::DataSliceImpl::Create(
                                 CreateFullDenseArray<int>({1, 2, 3})),
                             DataSlice::JaggedShape::FlatFromSize(3)));
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1, 2})),
            DataSlice::JaggedShape::FlatFromSize(2)));
    EXPECT_THAT(AlignNonScalars({ds_1, ds_2}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible")));
  }
}

TEST(ShapeBuilderTest, BuildWithSimpleParentShape) {
  DataSlice::JaggedShape prev_shape = DataSlice::JaggedShape::FlatFromSize(3);
  ShapeBuilder builder(prev_shape);
  builder.Add(1);
  builder.Add(4);
  builder.Add(7);
  ASSERT_OK_AND_ASSIGN(DataSlice::JaggedShape shape,
                       std::move(builder).Build());

  DataSlice::JaggedShape expected_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 5, 12}});
  EXPECT_THAT(shape, IsEquivalentTo(expected_shape));
}

TEST(ShapeBuilderTest, Build) {
  DataSlice::JaggedShape prev_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});
  ShapeBuilder builder(prev_shape);
  builder.Add(1);
  builder.Add(7);
  builder.Add(4);
  ASSERT_OK_AND_ASSIGN(DataSlice::JaggedShape shape,
                       std::move(builder).Build());

  ASSERT_OK_AND_ASSIGN(
      DataSlice::JaggedShape expected_shape,
      prev_shape.AddDims({test::EdgeFromSplitPoints({0, 1, 8, 12})}));

  EXPECT_THAT(shape, IsEquivalentTo(expected_shape));
}

TEST(ShapeBuilderTest, BuildWithEmptyParentShape) {
  DataSlice::JaggedShape prev_shape;
  ShapeBuilder builder(prev_shape);
  builder.Add(4);
  ASSERT_OK_AND_ASSIGN(DataSlice::JaggedShape shape,
                       std::move(builder).Build());
  DataSlice::JaggedShape expected_shape =
      DataSlice::JaggedShape::FlatFromSize(4);
  EXPECT_THAT(shape, IsEquivalentTo(expected_shape));
}

TEST(ShapeBuilderTest, NotEnoughGroupsFails) {
  DataSlice::JaggedShape prev_shape = DataSlice::JaggedShape::FlatFromSize(3);
  ShapeBuilder builder(prev_shape);
  builder.Add(1);
  builder.Add(2);
  EXPECT_THAT(std::move(builder).Build(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("split points must be full")));
}

TEST(ShapeBuilderTest, NegativeGroupSizeFails) {
  DataSlice::JaggedShape prev_shape = DataSlice::JaggedShape::FlatFromSize(3);
  ShapeBuilder builder(prev_shape);
  builder.Add(1);
  builder.Add(2);
  builder.Add(-3);
  EXPECT_THAT(std::move(builder).Build(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("split points must be sorted")));
}

}  // namespace
}  // namespace koladata::shape
