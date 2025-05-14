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
#include "koladata/internal/op_utils/inverse_select.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::CreateFullDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;

absl::StatusOr<JaggedDenseArrayShape> CreateJaggedShape(
    const std::vector<std::vector<int64_t>>& split_points) {
  JaggedDenseArrayShape::EdgeVec edges;
  edges.reserve(split_points.size());
  for (const auto& edge_split_points : split_points) {
    ASSIGN_OR_RETURN(
        auto edge,
        DenseArrayEdge::FromSplitPoints(CreateFullDenseArray<int64_t>(
            edge_split_points.begin(), edge_split_points.end())));
    edges.push_back(std::move(edge));
  }
  return JaggedDenseArrayShape::FromEdges(std::move(edges));
}

TEST(InverseSelectTest, NoDimensions) {
  ASSERT_OK_AND_ASSIGN(auto edge, CreateJaggedShape({}));
  ASSERT_OK_AND_ASSIGN(
      auto res, InverseSelectOp()(DataItem(), edge, DataItem(kMissing), edge));
  ASSERT_FALSE(res.has_value());

  ASSERT_OK_AND_ASSIGN(
      res, InverseSelectOp()(DataItem(1), edge, DataItem(kPresent), edge));
  ASSERT_TRUE(res.holds_value<int>());
  ASSERT_EQ(res.value<int>(), 1);
}

TEST(InverseSelectTest, NoDimensionsWithError) {
  ASSERT_OK_AND_ASSIGN(auto edge, CreateJaggedShape({}));
  EXPECT_THAT(InverseSelectOp()(DataItem(1), edge, DataItem(kMissing), edge),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the shape of `ds` and the shape of the present "
                       "elements in `fltr` do not match: because both are "
                       "DataItems but have different presences"));
  EXPECT_THAT(InverseSelectOp()(DataItem(), edge, DataItem(kPresent), edge),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the shape of `ds` and the shape of the present "
                       "elements in `fltr` do not match: because both are "
                       "DataItems but have different presences"));
  EXPECT_THAT(InverseSelectOp()(DataItem(1), edge, DataItem(1), edge),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "fltr argument must have all items of MASK dtype"));
}

TEST(InverseSelectTest, OneDimension) {
  ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({{0, 2}}));
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<arolla::Unit>({kPresent, kPresent, kMissing}));
  ASSERT_OK_AND_ASSIGN(auto filter_edge, CreateJaggedShape({{0, 3}}));

  // Int
  {
    auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(res.values<int>(), ElementsAre(1, std::nullopt, std::nullopt));
  }
  // Float
  {
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<float>({1.0, std::nullopt}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(res.values<float>(),
                ElementsAre(1.0, std::nullopt, std::nullopt));
  }
  // Text
  {
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<Text>({Text("a"), std::nullopt}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(res.values<Text>(),
                ElementsAre(Text("a"), std::nullopt, std::nullopt));
  }
  // All missing
  {
    auto ds = DataSliceImpl::CreateEmptyAndUnknownType(2);
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    ASSERT_TRUE(res.is_empty_and_unknown());
    ASSERT_EQ(res.size(), 3);
  }
  // Mixed
  {
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt}),
                              CreateDenseArray<float>({std::nullopt, 2.0}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(res, ElementsAre(DataItem(1), DataItem(2.0), DataItem()));
  }
  // ObjectIds with single allocation id
  {
    AllocationId alloc_id = Allocate(2);
    auto objects =
        CreateDenseArray<ObjectId>({alloc_id.ObjectByOffset(0), std::nullopt});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(
        res.values<ObjectId>(),
        ElementsAre(alloc_id.ObjectByOffset(0), std::nullopt, std::nullopt));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  // ObjectIds with single multiple allocation id2
  {
    AllocationId alloc_id = Allocate(2);
    AllocationId alloc_id_2 = Allocate(2);
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(0), alloc_id_2.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id, alloc_id_2}));
    ASSERT_OK_AND_ASSIGN(auto res,
                         InverseSelectOp()(ds, ds_edge, filter, filter_edge));
    EXPECT_THAT(res.values<ObjectId>(),
                ElementsAre(alloc_id.ObjectByOffset(0),
                            alloc_id_2.ObjectByOffset(1), std::nullopt));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
}

TEST(InverseSelectTest, OneDimensionWithEmptyFilter) {
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(0);
  ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({{0, 0}}));
  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN(auto filter_edge, CreateJaggedShape({{0, 3}}));
  ASSERT_OK_AND_ASSIGN(auto res,
                       InverseSelectOp()(ds, ds_edge, filter, filter_edge));
  ASSERT_TRUE(res.is_empty_and_unknown());
  ASSERT_EQ(res.size(), 3);
}

TEST(InverseSelectTest, OneDimensionWithError) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1}));
  ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({{0, 1}}));
  ASSERT_OK_AND_ASSIGN(auto filter_edge, CreateJaggedShape({{0, 3}}));

  // Invalid dtype
  {
    auto filter = DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
    EXPECT_THAT(InverseSelectOp()(ds, ds_edge, filter, filter_edge),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "fltr argument must have all items of MASK dtype"));
  }

  // Invalid shape
  {
    auto filter = DataSliceImpl::Create(
        CreateDenseArray<arolla::Unit>({kPresent, kPresent, kMissing}));
    EXPECT_THAT(
        InverseSelectOp()(ds, ds_edge, filter, filter_edge),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "the shape of `ds` and the shape of the present elements in "
                 "`fltr` do not match: JaggedShape(1) vs JaggedShape(2)"));
  }
}

TEST(InverseSelectTest, TwoDimensions) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt, 2}));
  ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({{0, 3}, {0, 2, 3, 3}}));
  auto filter = DataSliceImpl::Create(CreateDenseArray<arolla::Unit>(
      {kPresent, kPresent, kMissing, kPresent, kMissing, kMissing}));
  ASSERT_OK_AND_ASSIGN(auto filter_edge,
                       CreateJaggedShape({{0, 3}, {0, 3, 5, 6}}));

  ASSERT_OK_AND_ASSIGN(auto res,
                       InverseSelectOp()(ds, ds_edge, filter, filter_edge));
  EXPECT_THAT(res.values<int>(), ElementsAre(1, std::nullopt, std::nullopt, 2,
                                             std::nullopt, std::nullopt));
}

TEST(InverseSelectTest, ThreeDimensionsWithError) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt, 2}));
  ASSERT_OK_AND_ASSIGN(auto ds_edge,
                       CreateJaggedShape({{0, 2}, {0, 2, 3}, {0, 2, 3, 3}}));

  // Invalid dtype
  {
    auto filter =
        DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3, 4, 5, 6}));
    ASSERT_OK_AND_ASSIGN(auto filter_edge,
                         CreateJaggedShape({{0, 2}, {0, 2, 3}, {0, 3, 5, 6}}));
    EXPECT_THAT(InverseSelectOp()(ds, ds_edge, filter, filter_edge),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "fltr argument must have all items of MASK dtype"));
  }

  // Last dimension is invalid
  {
    auto filter = DataSliceImpl::Create(CreateDenseArray<arolla::Unit>(
        {kPresent, kPresent, kPresent, kPresent, kMissing, kMissing}));
    ASSERT_OK_AND_ASSIGN(auto filter_edge,
                         CreateJaggedShape({{0, 2}, {0, 2, 3}, {0, 3, 5, 6}}));
    EXPECT_THAT(
        InverseSelectOp()(ds, ds_edge, filter, filter_edge),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "the shape of `ds` and the shape of the present elements in "
                 "`fltr` do not match: JaggedShape(2, [2, 1], [2, 1, 0]) vs "
                 "JaggedShape(2, [2, 1], [3, 1, 0])"));
  }

  // Second dimension is invalid
  {
    auto filter = DataSliceImpl::Create(CreateDenseArray<arolla::Unit>(
        {kPresent, kPresent, kMissing, kPresent, kMissing, kMissing}));
    ASSERT_OK_AND_ASSIGN(auto filter_edge,
                         CreateJaggedShape({{0, 2}, {0, 1, 3}, {0, 3, 5, 6}}));
    EXPECT_THAT(
        InverseSelectOp()(ds, ds_edge, filter, filter_edge),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "the shape of `ds` and the shape of the present elements in "
                 "`fltr` do not match: JaggedShape(2, [2, 1], [2, 1, 0]) vs "
                 "JaggedShape(2, [1, 2], [2, 1, 0])"));
  }
}

TEST(InverseSelectTest, InvalidCase) {
  {
    auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1, 2}));
    ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({{0, 2}}));
    auto filter = DataItem();
    ASSERT_OK_AND_ASSIGN(auto filter_edge, CreateJaggedShape({}));
    EXPECT_THAT(InverseSelectOp()(ds, ds_edge, filter, filter_edge),
                StatusIs(absl::StatusCode::kInternal,
                         "invalid case ensured by the caller"));
  }

  {
    auto ds = DataItem();
    ASSERT_OK_AND_ASSIGN(auto ds_edge, CreateJaggedShape({}));
    auto filter = DataSliceImpl::Create(
        CreateDenseArray<arolla::Unit>({kPresent, kPresent}));
    ASSERT_OK_AND_ASSIGN(auto filter_edge, CreateJaggedShape({{0, 2}}));
    EXPECT_THAT(InverseSelectOp()(ds, ds_edge, filter, filter_edge),
                StatusIs(absl::StatusCode::kInternal,
                         "invalid case ensured by the caller"));
  }
}

}  // namespace
}  // namespace koladata::internal
