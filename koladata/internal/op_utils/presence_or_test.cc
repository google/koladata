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
#include "koladata/internal/op_utils/presence_or.h"

#include <cstddef>
#include <initializer_list>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;
using ::arolla::Unit;

constexpr size_t kLargeAllocSize = 2000;

static_assert(kLargeAllocSize > kSmallAllocMaxCapacity);

TEST(PresenceOrTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto l = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
    auto r = CreateDenseArray<int>({2, 1, std::nullopt, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(1, 1, 12, std::nullopt));
  }
  {
    // Float.
    auto l = CreateDenseArray<float>({1., std::nullopt, 12., std::nullopt});
    auto r = CreateDenseArray<float>({2., 1.5, std::nullopt, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(1., 1.5, 12., std::nullopt));
  }
  {
    // Text.
    auto l = CreateDenseArray<Text>({Text("foo"), std::nullopt, std::nullopt});
    auto r = CreateDenseArray<Text>({Text("bar"), std::nullopt, Text("bar")});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(Text("foo"), std::nullopt, Text("bar")));
  }
}

TEST(PresenceOrTest, EmptyInputs) {
  {
    // Empty and unknown lhs.
    auto r = CreateDenseArray<int>({2, 1, std::nullopt});
    auto lds = DataSliceImpl::Builder(3).Build();
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(2, 1, std::nullopt));
  }
  {
    // Empty and unknown rhs.
    auto l = CreateDenseArray<int>({1, 2, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Builder(3).Build();

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(1, 2, std::nullopt));
  }
  {
    // Empty and typed rhs.
    auto l = CreateDenseArray<int>({1, 2, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::CreateEmptyAndUnknownType(3);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(1, 2, std::nullopt));
  }
  {
    // Effectively empty and typed rhs.
    auto l = CreateDenseArray<int>({1, 2, std::nullopt});
    auto lds = DataSliceImpl::Create(l);
    auto r = CreateDenseArray<float>({2.71, 3.14, std::nullopt});
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_THAT(res, ElementsAre(1, 2, std::nullopt));
  }
  {
    // Empty lhs and rhs.
    auto lds = DataSliceImpl::Builder(4).Build();
    auto rds = DataSliceImpl::Builder(4).Build();

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    EXPECT_TRUE(res.is_empty_and_unknown());
  }
}

TEST(PresenceOrTest, DataSliceMixedPrimitiveValues) {
  {
    // Non intersecting types.
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2.71, std::nullopt});
    auto r_text =
        CreateDenseArray<Text>({Text("a"), std::nullopt, Text("c"), Text("d")});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto rds = DataSliceImpl::Create(r_text);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(1, DataItem(), 2.71f, arolla::Text("d")));
  }
  {
    // With intersecting types.
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2.71, std::nullopt});
    auto r_float = CreateDenseArray<float>(
        {0.5, 1.5, std::nullopt, std::nullopt});
    auto r_text = CreateDenseArray<Text>(
        {std::nullopt, std::nullopt, Text("c"), Text("d")});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto rds = DataSliceImpl::Create(r_float, r_text);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(1, 1.5f, 2.71f, arolla::Text("d")));
  }
  {
    // With unused types from rhs.
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2.71, std::nullopt});
    auto r_float = CreateDenseArray<float>(
        {0.5, 1.5, std::nullopt, std::nullopt});
    auto r_text = CreateDenseArray<Text>(
        {std::nullopt, std::nullopt, Text("c"), std::nullopt});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto rds = DataSliceImpl::Create(r_float, r_text);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, rds));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(1, 1.5f, 2.71f, std::nullopt));
  }
}

TEST(PresenceOrTest, DataSliceObjectId) {
  {
    // Multiple allocation ids.
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);  // small allocations
    auto lhs_objects =
        CreateDenseArray<ObjectId>({alloc_id_2.ObjectByOffset(0), std::nullopt,
                                    alloc_id_2.ObjectByOffset(1)});
    auto rhs_objects =
        CreateDenseArray<ObjectId>({std::nullopt, obj_id, obj_id});
    auto lhs = DataSliceImpl::CreateObjectsDataSlice(
        lhs_objects, AllocationIdSet({alloc_id_2}));
    auto rhs = DataSliceImpl::CreateObjectsDataSlice(
        rhs_objects, AllocationIdSet({alloc_id_1}));
    ASSERT_EQ(lhs.dtype(), arolla::GetQType<ObjectId>());
    ASSERT_EQ(rhs.dtype(), arolla::GetQType<ObjectId>());

    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lhs, rhs));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(res, ElementsAre(alloc_id_2.ObjectByOffset(0), obj_id,
                                 alloc_id_2.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), AllocationIdSet({alloc_id_1, alloc_id_2}));
  }
}

TEST(PresenceOrTest, LengthMismatch) {
  auto l_float = CreateDenseArray<float>({3.14, 2.71});
  auto r_unit = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
  auto lds = DataSliceImpl::Create(l_float);
  auto rds = DataSliceImpl::Create(r_unit);

  EXPECT_THAT(PresenceOrOp()(lds, rds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "coalesce requires input slices to have the same size"));
}

TEST(PresenceOrTest, DataItem) {
  {
    // Empty lhs.
    auto litem = DataItem();
    auto ritem = DataItem(AllocateSingleObject());
    auto robj_id = ritem.value<ObjectId>();
    auto mask = DataItem(arolla::Unit());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(litem, ritem));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(res.value<ObjectId>(), robj_id);
  }
  {
    // Empty rhs.
    auto litem = DataItem(AllocateSingleObject());
    auto lobj_id = litem.value<ObjectId>();
    auto ritem = DataItem();
    auto mask = DataItem(arolla::Unit());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(litem, ritem));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(res.value<ObjectId>(), lobj_id);
  }
  {
    // Objects.
    auto litem = DataItem(AllocateSingleObject());
    auto lobj_id = litem.value<ObjectId>();
    auto ritem = DataItem(AllocateSingleObject());
    auto robj_id = ritem.value<ObjectId>();
    auto mask = DataItem(arolla::Unit());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(litem, ritem));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(res.value<ObjectId>(), lobj_id);
    EXPECT_NE(res.value<ObjectId>(), robj_id);
  }
  {
    // Mixed.
    auto litem = DataItem(5);
    auto ritem = DataItem(Text("a"));
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(litem, ritem));
    EXPECT_EQ(res.dtype(), arolla::GetQType<int>());
    EXPECT_EQ(res.value<int>(), 5);
  }
}

TEST(PresenceOrTest, DataSliceOrDataItem) {
  {
    // Mixed primitves.
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2.71, std::nullopt});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto ritem = DataItem(arolla::Text("abc"));
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, ritem));
    EXPECT_THAT(
        res, ElementsAre(1, arolla::Text("abc"), 2.71f, arolla::Text("abc")));
  }
  {
    // Mixed primitves.
    auto l_int =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto l_float = CreateDenseArray<float>(
        {std::nullopt, std::nullopt, 2.71, std::nullopt});
    auto lds = DataSliceImpl::Create(l_int, l_float);
    auto ritem = DataItem();
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, ritem));
    EXPECT_THAT(res, ElementsAre(1, std::nullopt, 2.71f, std::nullopt));
  }
  {
    // ObjectId and allocation ids.
    auto obj_id_1 = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id_1);
    auto obj_id_2 = AllocateSingleObject();  // small allocations
    auto alloc_id_2 = AllocationId(obj_id_2);
    auto l_objects = CreateDenseArray<ObjectId>({
      obj_id_1, std::nullopt, obj_id_2
    });
    auto lds = DataSliceImpl::CreateObjectsDataSlice(
        l_objects, AllocationIdSet({alloc_id_1, alloc_id_2}));
    auto ritem = DataItem(AllocateSingleObject());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceOrOp()(lds, ritem));
    EXPECT_THAT(res, ElementsAre(obj_id_1, ritem, obj_id_2));
    EXPECT_EQ(res.allocation_ids(),
              AllocationIdSet({alloc_id_1, alloc_id_2,
                               AllocationId(ritem.value<ObjectId>())}));
  }
}

}  // namespace
}  // namespace koladata::internal
