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
#include "koladata/internal/op_utils/at.h"

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

TEST(AtTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<int>({1, 1, std::nullopt, 12}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 3, 1});
    auto ds_to_common = CreateEdge({0, 4});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    EXPECT_THAT(res, ElementsAre(1, std::nullopt, 1, 12, 1));
  }
  {
    // Float.
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<float>({3.14, 3.14, std::nullopt, 2.71}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 3, 1});
    auto ds_to_common = CreateEdge({0, 4});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    EXPECT_THAT(res.values<float>(),
                ElementsAre(3.14, std::nullopt, 3.14, 2.71, 3.14));
  }
  {
    // Text.
    auto ds = DataSliceImpl::Create(CreateDenseArray<Text>(
        {Text("abc"), Text("abc"), std::nullopt, Text("xyz")}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 3, 1});
    auto ds_to_common = CreateEdge({0, 4});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    EXPECT_THAT(res, ElementsAre(Text("abc"), std::nullopt, Text("abc"),
                                 Text("xyz"), Text("abc")));
  }
}

TEST(AtTest, DataSliceMixedPrimitiveValues) {
  {
    // Result still has mixed dtype.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 3, 2});
    auto ds_to_common = CreateEdge({0, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), 1, 12, 3.14f));
  }
  {
    // Result has a single dtype.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 3});
    auto ds_to_common = CreateEdge({0, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_single_dtype());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), 1, 12));
  }
  {
    // All missing result.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt});
    auto ds_to_common = CreateEdge({0, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem()));
  }
}

TEST(AtTest, DataSliceObjectId) {
  for (size_t alloc_size : {(size_t)2, (size_t)4, kLargeAllocSize}) {
    // Single allocation id.
    AllocationId alloc_id = Allocate(alloc_size);
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(0), alloc_id.ObjectByOffset(0), std::nullopt,
         alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(1),
         alloc_id.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 3});
    auto ds_to_common = CreateEdge({0, 6});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(res, ElementsAre(alloc_id.ObjectByOffset(0), std::nullopt,
                                 alloc_id.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Multiple allocation ids.
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);  // small allocations
    auto objects = CreateDenseArray<ObjectId>(
        {obj_id, alloc_id_2.ObjectByOffset(0), obj_id, obj_id,
         alloc_id_2.ObjectByOffset(1), alloc_id_2.ObjectByOffset(1),
         alloc_id_2.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id_2, alloc_id_1}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    ASSERT_EQ(ds.allocation_ids(),
              AllocationIdSet({alloc_id_2, AllocationId(obj_id)}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0});
    auto ds_to_common = CreateEdge({0, 7});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(
        res, ElementsAre(alloc_id_2.ObjectByOffset(0), std::nullopt, obj_id));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Mixed.
    auto obj_id_1 = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id_1);
    auto obj_id_2 = AllocateSingleObject();  // small allocation
    auto alloc_id_2 = AllocationId(obj_id_2);
    auto objects = CreateDenseArray<ObjectId>({std::nullopt, std::nullopt,
                                               obj_id_1, obj_id_1, obj_id_2,
                                               std::nullopt, std::nullopt});
    auto values_int = CreateDenseArray<int>(
        {1, 1, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 12});
    auto ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({alloc_id_1, alloc_id_2}), objects, values_int);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 2, 4, 5});
    auto ds_to_common = CreateEdge({0, 7});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(res,
                ElementsAre(1, std::nullopt, obj_id_1, obj_id_2, std::nullopt));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
}

TEST(AtTest, EmptyDataSlice) {
  {
    // No variant.
    auto ds = DataSliceImpl::CreateEmptyAndUnknownType(7);
    auto indices = CreateDenseArray<int64_t>({0, std::nullopt, 4});
    auto ds_to_common = CreateEdge({0, 7});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
  {
    // Multiple variants.
    auto int_values = arolla::CreateEmptyDenseArray<int>(/*size=*/7);
    auto float_values = arolla::CreateEmptyDenseArray<float>(/*size=*/7);
    auto ds = DataSliceImpl::Create(int_values, float_values);
    auto indices = CreateDenseArray<int64_t>({0, std::nullopt, 4});
    auto ds_to_common = CreateEdge({0, 7});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, std::nullopt));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
}

TEST(AtTest, OutOfBoundIndices) {
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<int>({1, 1, std::nullopt, 12}));
  auto indices = CreateDenseArray<int64_t>({-1, std::nullopt, 4});
  auto ds_to_common = CreateEdge({0, 7});

  ASSERT_OK_AND_ASSIGN(auto res, AtOp(ds, indices, ds_to_common, std::nullopt));
  EXPECT_THAT(res, ElementsAre(std::nullopt, std::nullopt, std::nullopt));
}

TEST(AtTest, MultiDimDataSlicePrimitiveValues) {
  {
    // Int.
    auto ds =
        DataSliceImpl::Create(CreateDenseArray<int>({1, 1, std::nullopt, 12}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 1, 1});
    auto ds_to_common = CreateEdge({0, 2, 4});
    auto indices_to_common = CreateEdge({0, 2, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    EXPECT_THAT(res, ElementsAre(1, std::nullopt, std::nullopt, 12, 12));
  }
  {
    // Float.
    auto ds = DataSliceImpl::Create(
        CreateDenseArray<float>({3.14, 3.14, std::nullopt, 2.71}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 1, 1});
    auto ds_to_common = CreateEdge({0, 2, 4});
    auto indices_to_common = CreateEdge({0, 2, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    EXPECT_THAT(res.values<float>(),
                ElementsAre(3.14, std::nullopt, std::nullopt, 2.71, 2.71));
  }
  {
    // Text.
    auto ds = DataSliceImpl::Create(CreateDenseArray<Text>(
        {Text("abc"), Text("abc"), std::nullopt, Text("xyz")}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 1, 1});
    auto ds_to_common = CreateEdge({0, 2, 4});
    auto indices_to_common = CreateEdge({0, 2, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    EXPECT_THAT(res, ElementsAre(Text("abc"), std::nullopt, std::nullopt,
                                 Text("xyz"), Text("xyz")));
  }
}

TEST(AtTest, MultiDimDataSliceMixedPrimitiveValues) {
  {
    // Result still has mixed dtype.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 1});
    auto ds_to_common = CreateEdge({0, 2, 5});
    auto indices_to_common = CreateEdge({0, 2, 4});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), 3.14f, 12));
  }
  {
    // Result has a single dtype.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 2});
    auto ds_to_common = CreateEdge({0, 2, 5});
    auto indices_to_common = CreateEdge({0, 2, 4});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_single_dtype());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), 3.14f, 2.71f));
  }
  {
    // All missing result.
    auto values_int = CreateDenseArray<int>(
        {1, std::nullopt, std::nullopt, 12, std::nullopt});
    auto values_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values_int, values_float);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 4});
    auto ds_to_common = CreateEdge({0, 2, 5});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
}

TEST(AtTest, MultiDimDataSliceObjectId) {
  for (size_t alloc_size : {(size_t)2, (size_t)4, kLargeAllocSize}) {
    // Single allocation id.
    AllocationId alloc_id = Allocate(alloc_size);
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(0), alloc_id.ObjectByOffset(0), std::nullopt,
         alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(1),
         alloc_id.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 1});
    auto ds_to_common = CreateEdge({0, 2, 6});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(res, ElementsAre(alloc_id.ObjectByOffset(0), std::nullopt,
                                 alloc_id.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Multiple allocation ids.
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);  // small allocations
    auto objects = CreateDenseArray<ObjectId>(
        {obj_id, alloc_id_2.ObjectByOffset(0), obj_id, obj_id,
         alloc_id_2.ObjectByOffset(1), alloc_id_2.ObjectByOffset(1),
         alloc_id_2.ObjectByOffset(1)});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id_2, alloc_id_1}));
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
    ASSERT_EQ(ds.allocation_ids(),
              AllocationIdSet({alloc_id_2, AllocationId(obj_id)}));
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0});
    auto ds_to_common = CreateEdge({0, 2, 7});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(
        res, ElementsAre(alloc_id_2.ObjectByOffset(0), std::nullopt, obj_id));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Mixed.
    auto obj_id_1 = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id_1);
    auto obj_id_2 = AllocateSingleObject();  // small allocation
    auto alloc_id_2 = AllocationId(obj_id_2);
    auto objects = CreateDenseArray<ObjectId>({std::nullopt, std::nullopt,
                                               obj_id_1, obj_id_1, obj_id_2,
                                               std::nullopt, std::nullopt});
    auto values_int = CreateDenseArray<int>(
        {1, 1, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 12});
    auto ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({alloc_id_1, alloc_id_2}), objects, values_int);
    ASSERT_TRUE(ds.is_mixed_dtype());
    auto indices = CreateDenseArray<int64_t>({1, std::nullopt, 0, 2, 3});
    auto ds_to_common = CreateEdge({0, 2, 7});
    auto indices_to_common = CreateEdge({0, 2, 5});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(res,
                ElementsAre(1, std::nullopt, obj_id_1, obj_id_2, std::nullopt));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
}

TEST(AtTest, MultiDimEmptyDataSlice) {
  {
    // No variant.
    auto ds = DataSliceImpl::CreateEmptyAndUnknownType(7);
    auto indices = CreateDenseArray<int64_t>({0, std::nullopt, 2});
    auto ds_to_common = CreateEdge({0, 2, 7});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
  {
    // Single variant.
    auto ds = DataSliceImpl::CreateEmptyAndUnknownType(7);
    auto indices = CreateDenseArray<int64_t>({0, std::nullopt, 2});
    auto ds_to_common = CreateEdge({0, 2, 7});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
  {
    // Multiple variants.
    auto int_values = arolla::CreateEmptyDenseArray<int>(/*size=*/7);
    auto float_values = arolla::CreateEmptyDenseArray<float>(/*size=*/7);
    auto ds = DataSliceImpl::Create(int_values, float_values);
    auto indices = CreateDenseArray<int64_t>({0, std::nullopt, 2});
    auto ds_to_common = CreateEdge({0, 2, 7});
    auto indices_to_common = CreateEdge({0, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res,
                         AtOp(ds, indices, ds_to_common, indices_to_common));
    ASSERT_TRUE(res.is_empty_and_unknown());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), DataItem()));
  }
}

TEST(AtTest, MultiDimNegativeIndices) {
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<int>({1, 2, std::nullopt, 12}));
  auto indices = CreateDenseArray<int64_t>({-1, -2, -1});
  auto ds_to_common = CreateEdge({0, 2, 4});
  auto indices_to_common = CreateEdge({0, 2, 3});

  ASSERT_OK_AND_ASSIGN(auto res,
                       AtOp(ds, indices, ds_to_common, indices_to_common));
  EXPECT_THAT(res, ElementsAre(2, 1, 12));
}

TEST(AtTest, MultiDimOutOfBoundIndices) {
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<int>({1, 1, std::nullopt, 12}));
  auto indices = CreateDenseArray<int64_t>({-3, 3, 2});
  auto ds_to_common = CreateEdge({0, 2, 4});
  auto indices_to_common = CreateEdge({0, 2, 3});

  ASSERT_OK_AND_ASSIGN(auto res,
                       AtOp(ds, indices, ds_to_common, indices_to_common));
  EXPECT_THAT(res, ElementsAre(std::nullopt, std::nullopt, std::nullopt));
}

}  // namespace
}  // namespace koladata::internal
