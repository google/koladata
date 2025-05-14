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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/base62.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::testing::HasSubstr;

TEST(EncodeItemId, TestDataItem) {
  ObjectId id = AllocateSingleObject();
  DataItem item(id);
  ASSERT_OK_AND_ASSIGN(DataItem res, EncodeItemId()(item));
  EXPECT_EQ(id.ToRawInt128(), DecodeBase62(res.value<arolla::Text>()));
}

TEST(EncodeItemId, TestDataSliceImpl) {
  ObjectId id_1 = AllocateSingleObject();
  ObjectId id_2 = AllocateSingleObject();
  DataSliceImpl slice =
      DataSliceImpl::Create(CreateDenseArray<ObjectId>({id_1, id_2}));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl res, EncodeItemId()(slice));
  EXPECT_EQ(id_1.ToRawInt128(),
            DecodeBase62(res.values<arolla::Text>()[0].value));
  EXPECT_EQ(id_2.ToRawInt128(),
            DecodeBase62(res.values<arolla::Text>()[1].value));
}

TEST(EncodeItemId, TestDataItemInvalidType) {
  DataItem item(1);
  EXPECT_THAT(EncodeItemId()(item), StatusIs(absl::StatusCode::kInvalidArgument,
                                             HasSubstr("on primitives")));
}

TEST(EncodeItemId, TestDataSliceImplInvalidType) {
  DataSliceImpl slice = DataSliceImpl::Create(CreateDenseArray<int>({1, 2}));
  EXPECT_THAT(EncodeItemId()(slice),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("on primitives")));
}

TEST(DecodeItemId, TestDataItem) {
  ObjectId id = AllocateSingleObject();
  DataItem item(id);
  ASSERT_OK_AND_ASSIGN(DataItem encoded, EncodeItemId()(item));
  EXPECT_THAT(DecodeItemId()(encoded), IsOkAndHolds(item));
}

TEST(DecodeItemId, TestDataSliceImpl) {
  ObjectId id_1 = AllocateSingleObject();
  ObjectId id_2 = AllocateSingleObject();
  DataSliceImpl slice =
      DataSliceImpl::Create(CreateDenseArray<ObjectId>({id_1, id_2}));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl encoded, EncodeItemId()(slice));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl decoded, DecodeItemId()(encoded));
  EXPECT_TRUE(slice.IsEquivalentTo(decoded));
}

TEST(DecodeItemId, TestDataItemInvalidType) {
  DataItem item(1);
  EXPECT_THAT(DecodeItemId()(item), StatusIs(absl::StatusCode::kInvalidArgument,
                                             HasSubstr("on non-text")));
}

TEST(DecodeItemId, TestDataSliceImplInvalidType) {
  DataSliceImpl slice = DataSliceImpl::Create(CreateDenseArray<int>({1, 2}));
  EXPECT_THAT(DecodeItemId()(slice),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("on non-text")));
}

}  // namespace
}  // namespace koladata::internal
