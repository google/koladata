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
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/base62.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
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
  EXPECT_TRUE(result.is_empty_and_unknown());
}

TEST(ItemIdBits, TestEmptyDataSliceImplNotObjectIdType) {
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(2);
  ASSERT_OK_AND_ASSIGN(DataSliceImpl result, ItemIdBits()(empty, 10));
  EXPECT_EQ(result.present_count(), 0);
  EXPECT_EQ(result.size(), 2);
  EXPECT_TRUE(result.is_empty_and_unknown());
}

TEST(ItemIdBits, TestEmptyDataSliceImpl) {
  DataSliceImpl empty =
      DataSliceImpl::Create(arolla::CreateEmptyDenseArray<ObjectId>(2));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl result, ItemIdBits()(empty, 10));
  EXPECT_EQ(result.present_count(), 0);
  EXPECT_EQ(result.size(), 2);
  EXPECT_TRUE(result.is_empty_and_unknown());
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

TEST(ItemIdStr, TestItemIdStr) {
  ObjectId id = AllocateSingleObject();
  DataItem item(id);
  ASSERT_OK_AND_ASSIGN(DataItem res, ItemIdStr()(item));
  EXPECT_EQ(id.ToRawInt128(), DecodeBase62(res.value<arolla::Text>()));
}

TEST(ItemIdStr, TestDataSliceImplStr) {
  ObjectId id_1 = AllocateSingleObject();
  ObjectId id_2 = AllocateSingleObject();
  DataSliceImpl slice =
      DataSliceImpl::Create(CreateDenseArray<ObjectId>({id_1, id_2}));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl res, ItemIdStr()(slice));
  EXPECT_EQ(id_1.ToRawInt128(),
            DecodeBase62(res.values<arolla::Text>()[0].value));
  EXPECT_EQ(id_2.ToRawInt128(),
            DecodeBase62(res.values<arolla::Text>()[1].value));
}

TEST(ItemIdStr, TestDataItemInvalidType) {
  DataItem item(1);
  EXPECT_THAT(ItemIdStr()(item), StatusIs(absl::StatusCode::kInvalidArgument,
                                          HasSubstr("on primitives")));
}

TEST(ItemIdStr, TestDataSliceImplInvalidType) {
  DataSliceImpl slice = DataSliceImpl::Create(CreateDenseArray<int>({1, 2}));
  EXPECT_THAT(ItemIdStr()(slice), StatusIs(absl::StatusCode::kInvalidArgument,
                                           HasSubstr("on primitives")));
}

}  // namespace
}  // namespace koladata::internal
