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
#include "koladata/internal/op_utils/itemid.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(ItemIdBits, TestInvalidBitLength) {
  DataItem item(AllocateSingleObject());
  EXPECT_THAT(ItemIdBits()(item, -1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("between 0 and 64")));
  EXPECT_THAT(ItemIdBits()(item, 65),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("between 0 and 64")));
}

TEST(ItemIdBits, TestInvalidDataItemType) {
  DataItem item(1);
  EXPECT_THAT(
      ItemIdBits()(item, 10),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("on primitives")));
}

TEST(ItemIdBits, TestEmptyDataItem) {
  DataItem empty;
  ASSERT_OK_AND_ASSIGN(DataItem result, ItemIdBits()(empty, 10));
  EXPECT_FALSE(result.has_value());
}

TEST(ItemIdBits, TestDataItemBits) {
  ObjectId id = AllocateSingleObject();
  ASSERT_OK_AND_ASSIGN(DataItem result, ItemIdBits()(DataItem(id), 10));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result.holds_value<int64_t>());
  EXPECT_EQ(result.value<int64_t>(),
            static_cast<int64_t>(id.ToRawInt128() & 1023));
}

TEST(ItemIdBits, TestDataSliceImplInvalidBitLength) {
  ObjectId obj_id_1 = AllocateSingleObject();
  ObjectId obj_id_2 = AllocateSingleObject();
  DataSliceImpl slice =
      DataSliceImpl::Create(CreateDenseArray<ObjectId>({obj_id_1, obj_id_2}));
  EXPECT_THAT(ItemIdBits()(slice, -1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("between 0 and 64")));
  EXPECT_THAT(ItemIdBits()(slice, 65),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("between 0 and 64")));
}

TEST(ItemIdBits, TestInvalidDataSliceImplType) {
  DataSliceImpl slice = DataSliceImpl::Create(CreateDenseArray<int>({1, 2}));
  EXPECT_THAT(
      ItemIdBits()(slice, 10),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("on primitives")));
}

TEST(ItemIdBits, TestEmptyAndUnknownDataSliceImpl) {
  DataSliceImpl slice = DataSliceImpl::CreateEmptyAndUnknownType(2);
  ASSERT_OK_AND_ASSIGN(DataSliceImpl result, ItemIdBits()(slice, 10));
  EXPECT_EQ(result.present_count(), 0);
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result.dtype(), arolla::GetQType<int64_t>());
}

TEST(ItemIdBits, TestEmptyDataSliceImplNotObjectIdType) {
  DataSliceImpl empty =
      DataSliceImpl::Create(arolla::CreateEmptyDenseArray<int64_t>(2));
  EXPECT_THAT(ItemIdBits()(empty, 10),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("itemid_bits on primitives")));

  empty = DataSliceImpl::CreateEmptyAndUnknownType(2);
  ASSERT_OK_AND_ASSIGN(DataSliceImpl result, ItemIdBits()(empty, 10));
  EXPECT_EQ(result.present_count(), 0);
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result.dtype(), arolla::GetQType<int64_t>());
}

TEST(ItemIdBits, TestEmptyDataSliceImpl) {
  DataSliceImpl empty =
      DataSliceImpl::Create(arolla::CreateEmptyDenseArray<ObjectId>(2));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl result, ItemIdBits()(empty, 10));
  EXPECT_EQ(result.present_count(), 0);
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(result.dtype(), arolla::GetQType<int64_t>());
}

TEST(ItemIdBits, TestDataSliceImplBits) {
  ObjectId obj_id_1 = AllocateSingleObject();
  ObjectId obj_id_2 = AllocateSingleObject();
  DataSliceImpl slice =
      DataSliceImpl::Create(CreateDenseArray<ObjectId>({obj_id_1, obj_id_2}));
  EXPECT_THAT(ItemIdBits()(slice, 10),
              IsOkAndHolds(ElementsAre(
                  static_cast<int64_t>(obj_id_1.ToRawInt128() & 1023),
                  static_cast<int64_t>(obj_id_2.ToRawInt128() & 1023))));
}

}  // namespace
}  // namespace koladata::internal
