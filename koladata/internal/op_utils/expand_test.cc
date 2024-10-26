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
#include "koladata/internal/op_utils/expand.h"

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::Text;

constexpr size_t kLargeAllocSize = 2000;

static_assert(kLargeAllocSize > kSmallAllocMaxCapacity);

arolla::DenseArrayEdge CreateEdge(std::initializer_list<int64_t> split_points) {
  return *arolla::DenseArrayEdge::FromSplitPoints(
      arolla::CreateFullDenseArray(std::vector<int64_t>(split_points)));
}

TEST(ExpandTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto values = CreateDenseArray<int>({1, std::nullopt, 12});
    auto ds = DataSliceImpl::Create(values);
    auto edge = CreateEdge({0, 2, 4, 7});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    EXPECT_THAT(res.values<int>(),
                ElementsAre(1, 1, std::nullopt, std::nullopt, 12, 12, 12));
  }
  {
    // Float.
    auto values = CreateDenseArray<float>({3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values);
    auto edge = CreateEdge({0, 2, 4, 7});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    EXPECT_THAT(
        res.values<float>(),
        ElementsAre(3.14, 3.14, std::nullopt, std::nullopt, 2.71, 2.71, 2.71));
  }
  {
    // Text.
    auto values =
        CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt});
    auto ds = DataSliceImpl::Create(values);
    auto edge = CreateEdge({0, 2, 4, 7});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    EXPECT_THAT(res.values<Text>(),
                ElementsAre(Text("abc"), Text("abc"), Text("xyz"), Text("xyz"),
                            std::nullopt, std::nullopt, std::nullopt));
  }
}

TEST(ExpandTest, DataSliceMixedPrimitiveValues) {
  auto values_int = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
  auto values_float =
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt, 2.71});
  auto ds = DataSliceImpl::Create(values_int, values_float);
  ASSERT_TRUE(ds.is_mixed_dtype());
  auto edge = CreateEdge({0, 2, 4, 6, 7});

  ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
  ASSERT_TRUE(res.is_mixed_dtype());
  EXPECT_THAT(res, ElementsAre(1, 1, DataItem(), DataItem(), 12, 12, 2.71f));
}

TEST(ExpandTest, DataSliceObjectId) {
  for (size_t alloc_size : {(size_t)2, (size_t)4, kLargeAllocSize}) {
    // Single allocation id.
    AllocationId alloc_id = Allocate(alloc_size);
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(0), std::nullopt, alloc_id.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    auto edge = CreateEdge({0, 2, 4, 7});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(
        res.values<ObjectId>(),
        ElementsAre(alloc_id.ObjectByOffset(0), alloc_id.ObjectByOffset(0),
                    std::nullopt, std::nullopt, alloc_id.ObjectByOffset(1),
                    alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Multiple allocation ids.
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);  // small allocations
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id_2.ObjectByOffset(0), obj_id, alloc_id_2.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id_2, alloc_id_1}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    ASSERT_EQ(ds.allocation_ids(),
              AllocationIdSet({alloc_id_2, AllocationId(obj_id)}));
    auto edge = CreateEdge({0, 2, 4, 7});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(
        res.values<ObjectId>(),
        ElementsAre(alloc_id_2.ObjectByOffset(0), alloc_id_2.ObjectByOffset(0),
                    obj_id, obj_id, alloc_id_2.ObjectByOffset(1),
                    alloc_id_2.ObjectByOffset(1),
                    alloc_id_2.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Mixed.
    auto obj_id_1 = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id_1);
    auto obj_id_2 = AllocateSingleObject();  // small allocation
    auto alloc_id_2 = AllocationId(obj_id_2);
    auto objects = CreateDenseArray<ObjectId>(
        {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
    auto values_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, 12});
    auto ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({alloc_id_1, alloc_id_2}), objects, values_int);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto edge = CreateEdge({0, 2, 4, 5, 6});

    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(ds, edge));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(res, ElementsAre(1, 1, obj_id_1, obj_id_1, obj_id_2, 12));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
}

TEST(ExpandTest, DataItemPrimitiveValue) {
  auto item = DataItem(12);
  auto edge = CreateEdge({0, 3});
  ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(item, edge));
  ASSERT_EQ(res.dtype(), arolla::GetQType<int>());
  EXPECT_TRUE(res.allocation_ids().empty());
  EXPECT_THAT(res.values<int>(), ElementsAre(12, 12, 12));

  item = DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(res, ExpandOp()(item, edge));
  ASSERT_EQ(res.dtype(), arolla::GetQType<double>());
  EXPECT_TRUE(res.allocation_ids().empty());
  EXPECT_THAT(res.values<double>(), ElementsAre(3.14, 3.14, 3.14));

  item = DataItem(Text("abc"));
  ASSERT_OK_AND_ASSIGN(res, ExpandOp()(item, edge));
  ASSERT_EQ(res.dtype(), arolla::GetQType<Text>());
  EXPECT_TRUE(res.allocation_ids().empty());
  EXPECT_THAT(res.values<Text>(),
              ElementsAre(Text("abc"), Text("abc"), Text("abc")));
}

TEST(ExpandTest, DataItemObjectId) {
  {
    // Non-empty.
    auto item = DataItem(AllocateSingleObject());
    auto obj_id = item.value<ObjectId>();
    auto alloc_id = AllocationId(obj_id);
    auto edge = CreateEdge({0, 3});
    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(item, edge));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(res.allocation_ids(), AllocationIdSet(alloc_id));
    EXPECT_THAT(res.values<ObjectId>(),
                ElementsAre(obj_id, obj_id, obj_id));
  }
  {
    // Empty.
    auto item = DataItem();
    auto edge = CreateEdge({0, 3});
    ASSERT_OK_AND_ASSIGN(auto res, ExpandOp()(item, edge));
    EXPECT_EQ(res.dtype(), arolla::GetNothingQType());
    EXPECT_EQ(res.size(), 3);
    EXPECT_TRUE(res.is_empty_and_unknown());
    EXPECT_TRUE(res.allocation_ids().empty());
  }
}

}  // namespace
}  // namespace koladata::internal
