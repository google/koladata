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
#include "koladata/internal/op_utils/presence_and.h"

#include <cstddef>
#include <initializer_list>
#include <optional>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::koladata::testing::StatusIs;
using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;
using ::arolla::Unit;

constexpr size_t kLargeAllocSize = 2000;

static_assert(kLargeAllocSize > kSmallAllocMaxCapacity);

TEST(PresenceAndTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto values = CreateDenseArray<int>({1, std::nullopt, 12});
    auto presence_mask = CreateDenseArray<Unit>({kPresent, kPresent, kMissing});
    auto ds = DataSliceImpl::Create(values);
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_THAT(res.values<int>(), ElementsAre(1, std::nullopt, std::nullopt));
  }
  {
    // Float.
    auto values = CreateDenseArray<float>({3.14, std::nullopt, 2.71});
    auto presence_mask = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
    auto ds = DataSliceImpl::Create(values);
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_THAT(res.values<float>(),
                ElementsAre(std::nullopt, std::nullopt, 2.71));
  }
  {
    // Text.
    auto values =
        CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt});
    auto presence_mask = CreateDenseArray<Unit>({kPresent, kMissing, kPresent});
    auto ds = DataSliceImpl::Create(values);
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_THAT(res.values<Text>(),
                ElementsAre(Text("abc"), std::nullopt, std::nullopt));
  }
  {
    // Empty mask.
    auto values =
        CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt});
    auto ds = DataSliceImpl::Create(values);
    auto ds_presence = DataSliceImpl::Builder(3).Build();

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_EQ(res.size(), 3);
    EXPECT_TRUE(res.is_empty_and_unknown());
  }
  {
    // Empty result.
    auto values =
        CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt});
    auto presence_mask = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
    auto ds = DataSliceImpl::Create(values);
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_EQ(res.size(), 3);
    EXPECT_TRUE(res.is_empty_and_unknown());
  }
  {
    // Empty DataSlice.
    auto presence_mask = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
    auto ds = DataSliceImpl::Builder(3).Build();
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_EQ(res.size(), 3);
    EXPECT_TRUE(res.is_empty_and_unknown());
  }
}

TEST(PresenceAndTest, DataSliceMixedPrimitiveValues) {
  auto values_int = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
  auto values_float =
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt, 2.71});
  auto presence_mask =
      CreateDenseArray<Unit>({kPresent, kMissing, kMissing, kPresent});
  auto ds = DataSliceImpl::Create(values_int, values_float);
  auto ds_presence = DataSliceImpl::Create(presence_mask);
  ASSERT_TRUE(ds.is_mixed_dtype());

  ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
  ASSERT_TRUE(res.is_mixed_dtype());
  EXPECT_THAT(res, ElementsAre(1, DataItem(), DataItem(), 2.71f));
}

TEST(PresenceAndTest, DataSliceObjectId) {
  {
    // Multiple allocation ids.
    auto obj_id = Allocate(kLargeAllocSize).ObjectByOffset(1909);
    auto alloc_id_1 = AllocationId(obj_id);
    AllocationId alloc_id_2 = Allocate(2);  // small allocations
    auto objects = CreateDenseArray<ObjectId>(
        {alloc_id_2.ObjectByOffset(0), obj_id, alloc_id_2.ObjectByOffset(1)});
    auto presence_mask = CreateDenseArray<Unit>({kMissing, kPresent, kPresent});
    auto ds = DataSliceImpl::CreateObjectsDataSlice(
        objects, AllocationIdSet({alloc_id_1, alloc_id_2}));
    auto ds_presence = DataSliceImpl::Create(presence_mask);
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_EQ(res.dtype(), ds.dtype());
    EXPECT_THAT(
        res.values<ObjectId>(),
        ElementsAre(std::nullopt, obj_id, alloc_id_2.ObjectByOffset(1)));
    EXPECT_EQ(res.allocation_ids(), ds.allocation_ids());
  }
  {
    // Mixed.
    auto obj_id_1 = AllocateSingleObject();
    auto obj_id_2 = AllocateSingleObject();
    auto objects = CreateDenseArray<ObjectId>(
        {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
    auto values_int =
        CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt, 12});
    auto presence_mask =
        CreateDenseArray<Unit>({kPresent, kMissing, kPresent, kPresent});
    auto ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({AllocationId(obj_id_1), AllocationId(obj_id_2)}),
        objects, values_int);
    auto ds_presence = DataSliceImpl::Create(presence_mask);
    ASSERT_TRUE(ds.is_mixed_dtype());

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    ASSERT_TRUE(res.is_mixed_dtype());
    EXPECT_THAT(res, ElementsAre(DataItem(), DataItem(), obj_id_2, 12));
  }
  {
    // Empty result.
    auto obj_id_1 = AllocateSingleObject();
    auto obj_id_2 = AllocateSingleObject();
    auto objects = CreateDenseArray<ObjectId>(
        {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
    auto values_int =
        CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt, 12});
    auto ds = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({AllocationId(obj_id_1), AllocationId(obj_id_2)}),
        objects, values_int);
    auto presence_mask =
        CreateDenseArray<Unit>({kPresent, kMissing, kMissing, kMissing});
    auto ds_presence = DataSliceImpl::Create(presence_mask);

    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(ds, ds_presence));
    EXPECT_TRUE(res.is_empty_and_unknown());
    EXPECT_EQ(res.size(), 4);
    EXPECT_TRUE(res.allocation_ids().empty());
    EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
  }
}

TEST(PresenceAndTest, LengthMismatch) {
  auto values = CreateDenseArray<float>({3.14, 2.71});
  auto presence_mask = CreateDenseArray<Unit>({kMissing, kMissing, kPresent});
  auto ds = DataSliceImpl::Create(values);
  auto ds_presence = DataSliceImpl::Create(presence_mask);

  EXPECT_THAT(PresenceAndOp()(ds, ds_presence),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "ds.size() != presence_mask.size()"));
}

TEST(PresenceAndTest, TypeMismatch) {
  auto values = CreateDenseArray<float>({3.14, 2.71});
  auto presence_mask = CreateDenseArray<int>({1, 0});
  auto ds = DataSliceImpl::Create(values);
  auto ds_presence = DataSliceImpl::Create(presence_mask);

  EXPECT_THAT(PresenceAndOp()(ds, ds_presence),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Second argument to operator & (or apply_mask) must "
                       "have all items of MASK dtype"));
}

TEST(PresenceAndTest, DataItemObjectId) {
  {
    // Non-empty.
    auto item = DataItem(AllocateSingleObject());
    auto obj_id = item.value<ObjectId>();
    auto mask = DataItem(arolla::Unit());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(item, mask));
    EXPECT_EQ(res.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_EQ(res.value<ObjectId>(), obj_id);
  }
  {
    // Empty object.
    auto item = DataItem();
    auto mask = DataItem(arolla::Unit());
    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(item, mask));
    EXPECT_EQ(res.has_value(), false);
  }
  {
    // Empty mask.
    auto item = DataItem(AllocateSingleObject());
    auto mask = DataItem();
    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(item, mask));
    EXPECT_EQ(res.has_value(), false);
  }
  {
    // Empty object and mask.
    auto item = DataItem();
    auto mask = DataItem();
    ASSERT_OK_AND_ASSIGN(auto res, PresenceAndOp()(item, mask));
    EXPECT_EQ(res.has_value(), false);
  }
}

TEST(PresenceAndTest, DataItemTypeMismatch) {
  auto item = DataItem(AllocateSingleObject());
  auto mask = DataItem(AllocateSingleObject());

  EXPECT_THAT(PresenceAndOp()(item, mask),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Second argument to operator & (or apply_mask) must "
                       "have all items of MASK dtype"));
}

}  // namespace
}  // namespace koladata::internal
