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
#include "koladata/internal/op_utils/select.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/test_utils.h"

namespace koladata::internal {
namespace {
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::SizeIs;

using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;
using ::arolla::Unit;

TEST(SelectTest, DataSlicePrimitiveValues_Int_NoExpand) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<int>({1, std::nullopt, 4, std::nullopt, 2, 8}));

  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 3, 4, 6}});

  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kPresent, kMissing}));
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_THAT(res_ds.values<int>(),
              ElementsAre(1, std::nullopt, 4, std::nullopt));
  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 4);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 2));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 3, 4));
}

TEST(SelectTest, DataSlicePrimitiveValues_Int_ExpandFilter) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<int>({1, std::nullopt, 2}));
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kPresent, kMissing}));
  JaggedDenseArrayShape shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));
  EXPECT_THAT(res_ds.values<int>(), ElementsAre(1, std::nullopt));
  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 2);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 3));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 1, 2, 2));
}

TEST(SelectTest, DataSlicePrimitiveValues_Float_NoExpand) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<float>(
      {0.618, std::nullopt, 6.21, std::nullopt, 3.14, 114.514}));
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 3, 4, 6}});

  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kMissing, kPresent}));
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_THAT(res_ds.values<float>(),
              ElementsAre(0.618, std::nullopt, 6.21, 3.14, 114.514));
  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 5);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 2));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 3, 5));
}

TEST(SelectTest, DataSlicePrimitiveValues_Float_ExpandFilter) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<float>({0.618, std::nullopt, 6.21}));
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kMissing, kPresent}));

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_THAT(res_ds.values<float>(), ElementsAre(0.618, 6.21));
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 2);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(1));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 2));
}

TEST(SelectTest, DataSlicePrimitiveValues_Text_NoExpand) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<Text>(
      {Text("abc"), std::nullopt, Text("zyx"), std::nullopt, Text("el")}));
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 3, 4, 6}});

  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kPresent, kMissing}));
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_THAT(res_ds.size(), 4);
  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 4);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 2));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 3, 4));
}

TEST(SelectTest, DataSlicePrimitiveValues_Text_ExpandFilter) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<Text>({Text("abc"), std::nullopt, Text("zyx")}));
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kPresent, kMissing}));
  JaggedDenseArrayShape shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_THAT(res_ds.size(), 2);
  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 2);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 3));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 1, 2, 2));
}

TEST(SelectTest, DataSlicePrimitiveValues_EmptyFilter_UnkownDataSliceImpl) {
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 0);
}

TEST(SelectTest, DataSlicePrimitiveValues_EmptyFilter_TypedDataSliceImpl) {
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 0);
}

TEST(SelectTest,
     DataSlicePrimitiveValues_EmptyFilter_UnkownDataSliceImpl_NoExpand) {
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());
  JaggedDenseArrayShape expected_shape =
      test::ShapeFromSplitPoints({{0, 0}, {0}});
  EXPECT_TRUE(res_shape.IsEquivalentTo(expected_shape));
}

TEST(SelectTest,
     DataSlicePrimitiveValues_EmptyFilter_NoExpand_NoExtraDimension) {
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(0);
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}, {0, 0, 0, 0}});

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);
  JaggedDenseArrayShape filter_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());
  JaggedDenseArrayShape expected_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 0, 0, 0}, {0}});
  EXPECT_TRUE(res_shape.IsEquivalentTo(expected_shape));
}

TEST(SelectTest, DataSlicePrimitiveValues_AllMissingFilter) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<Text>({Text("abc"), std::nullopt, Text("zyx")}));
  arolla::JaggedDenseArrayShape ds_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));
  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 0);
}

TEST(SelectTest, DataSlicePrimitiveValues_EmptyResult_MultiDimension) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt}));
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  auto filter = DataSliceImpl::CreateEmptyAndUnknownType(3);
  auto filter_shape = test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, ds_shape, filter, filter_shape));

  EXPECT_EQ(res_ds.size(), 0);
  EXPECT_TRUE(res_ds.is_empty_and_unknown());

  EXPECT_EQ(res_shape.rank(), 2);
  EXPECT_EQ(res_shape.size(), 0);

  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(2));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 3));
  EXPECT_THAT(edges[1].edge_values().values, ElementsAre(0, 0, 0, 0));
}

TEST(SelectTest, DataSlicePrimitiveValues_FilterToOneResult) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<Text>({Text("abc"), Text("xyz"), Text("def")}));
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kMissing, kMissing, kPresent}));

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_EQ(res_ds.size(), 1);
  EXPECT_EQ(res_ds.dtype(), arolla::GetQType<Text>());
  EXPECT_THAT(res_ds.values<Text>(), ElementsAre(Text("def")));
}

TEST(SelectTest, DataSliceMixedPrimitiveValues) {
  auto values_int = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
  auto values_float =
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt, 2.71});

  auto ds = DataSliceImpl::Create(values_int, values_float);
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kMissing, kPresent, kPresent}));

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(4);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  ASSERT_TRUE(res_ds.is_mixed_dtype());
  EXPECT_THAT(res_ds.size(), 3);
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 3);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(1));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 3));
}

TEST(SelectTest, DataSliceMixedPrimitiveValues_FilterToEmpty) {
  auto values_int = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
  auto values_float =
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt, 2.71});

  auto ds = DataSliceImpl::Create(values_int, values_float);
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kMissing, kMissing, kMissing, kPresent}));

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(4);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  ASSERT_FALSE(res_ds.is_mixed_dtype());
  EXPECT_THAT(res_ds.size(), 1);
  EXPECT_EQ(res_ds.dtype(), arolla::GetQType<float>());
  EXPECT_THAT(res_ds.values<float>(), ElementsAre(2.71));
}

TEST(SelectTest, DataSliceObjectId_MixedType) {
  auto obj_id_1 = AllocateSingleObject();
  auto obj_id_2 = AllocateSingleObject();
  auto objects = CreateDenseArray<ObjectId>(
      {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
  auto values_int =
      CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt, 12});
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kMissing, kMissing, kMissing, kMissing}));
  auto ds = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({AllocationId(obj_id_1), AllocationId(obj_id_2)}),
      objects, values_int);

  ASSERT_TRUE(ds.is_mixed_dtype());

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(4);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 0);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(1));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 0));
}

TEST(SelectTest, DataSliceObjectId_EmptyResult) {
  auto obj_id_1 = AllocateSingleObject();
  auto obj_id_2 = AllocateSingleObject();
  auto objects = CreateDenseArray<ObjectId>(
      {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
  auto values_int =
      CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt, 12});
  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kMissing, kPresent, kPresent}));
  auto ds = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({AllocationId(obj_id_1), AllocationId(obj_id_2)}),
      objects, values_int);

  ASSERT_TRUE(ds.is_mixed_dtype());

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(4);

  ASSERT_OK_AND_ASSIGN((auto [res_ds, res_shape]),
                       SelectOp()(ds, shape, filter, shape));

  EXPECT_EQ(res_ds.allocation_ids(), ds.allocation_ids());
  EXPECT_EQ(res_shape.rank(), 1);
  EXPECT_EQ(res_shape.size(), 3);
  absl::Span<const DenseArrayEdge> edges = res_shape.edges();
  ASSERT_THAT(edges, SizeIs(1));
  EXPECT_THAT(edges[0].edge_values().values, ElementsAre(0, 3));
}

TEST(SelectTest, ShapeMismatch) {
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<float>({3.14, 2.71, std::nullopt}));
  arolla::JaggedDenseArrayShape ds_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kPresent, kPresent, kPresent}));
  auto filter_shape = test::ShapeFromSplitPoints({{0, 3}, {0, 1, 2, 3}});

  EXPECT_THAT(SelectOp()(ds, ds_shape, filter, filter_shape),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot have a higher rank")));
}

TEST(SelectTest, ErrorOnNotBroadcast) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<float>({3.14, 2.71}));
  arolla::JaggedDenseArrayShape ds_shape =
      JaggedDenseArrayShape::FlatFromSize(2);

  auto filter = DataSliceImpl::Create(
      CreateDenseArray<Unit>({kMissing, kMissing, kPresent}));
  arolla::JaggedDenseArrayShape filter_shape =
      JaggedDenseArrayShape::FlatFromSize(3);

  EXPECT_THAT(SelectOp()(ds, ds_shape, filter, filter_shape),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("should be broadcastable to")));
}

TEST(SelectTest, TypeMismatch) {
  auto ds = DataSliceImpl::Create(CreateDenseArray<float>({3.14, 2.71}));
  auto filter = DataSliceImpl::Create(CreateDenseArray<int>({1, 0}));

  arolla::JaggedDenseArrayShape shape =
      JaggedDenseArrayShape::FlatFromSize(2);

  EXPECT_THAT(SelectOp()(ds, shape, filter, shape),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("must have all items of MASK dtype")));
}

}  // namespace
}  // namespace koladata::internal
