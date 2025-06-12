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
#include "koladata/internal/op_utils/has.h"

#include <initializer_list>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::kMissing;
using ::arolla::kPresent;
using ::arolla::Text;
using ::arolla::Unit;

TEST(HasTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto values = CreateDenseArray<int>({1, std::nullopt, 12});
    auto ds = DataSliceImpl::Create(values);

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_THAT(res.values<Unit>(), ElementsAre(kPresent, kMissing, kPresent));
  }
  {
    // Float.
    auto values = CreateDenseArray<float>({3.14, std::nullopt, 2.71});
    auto ds = DataSliceImpl::Create(values);

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_THAT(res.values<Unit>(), ElementsAre(kPresent, kMissing, kPresent));
  }
  {
    // Text.
    auto values =
        CreateDenseArray<Text>({Text("abc"), Text("xyz"), std::nullopt});
    auto ds = DataSliceImpl::Create(values);

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_THAT(res.values<Unit>(), ElementsAre(kPresent, kPresent, kMissing));
  }
  {
    // Empty.
    auto ds = SliceBuilder(3).Build();

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_TRUE(res.is_empty_and_unknown());
    EXPECT_EQ(res.size(), 3);
  }
  {
    // Empty result.
    auto values = CreateDenseArray<int>({std::nullopt, std::nullopt});
    auto ds = DataSliceImpl::Create(values);

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_TRUE(res.is_empty_and_unknown());
    EXPECT_EQ(res.size(), 2);
  }
}

TEST(HasTest, DataSliceMixedPrimitiveValues) {
  auto values_int = CreateDenseArray<int>({1, std::nullopt, 12, std::nullopt});
  auto values_float =
      CreateDenseArray<float>({std::nullopt, std::nullopt, std::nullopt, 2.71});
  auto ds = DataSliceImpl::Create(values_int, values_float);
  ASSERT_TRUE(ds.is_mixed_dtype());

  ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
  EXPECT_THAT(res.values<Unit>(),
              ElementsAre(kPresent, kMissing, kPresent, kPresent));
}

TEST(HasTest, DataSliceObjectId) {
  {
    // ObjectId.
    auto obj_id_1 = AllocateSingleObject();
    auto obj_id_2 = AllocateSingleObject();
    auto objects = CreateDenseArray<ObjectId>(
        {obj_id_1, std::nullopt, obj_id_2});
    auto ds = DataSliceImpl::Create(objects);
    ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_THAT(res.values<Unit>(), ElementsAre(kPresent, kMissing, kPresent));
  }
  {
    // Mixed.
    auto obj_id_1 = AllocateSingleObject();
    auto obj_id_2 = AllocateSingleObject();
    auto objects = CreateDenseArray<ObjectId>(
        {std::nullopt, obj_id_1, obj_id_2, std::nullopt});
    auto values_int =
        CreateDenseArray<int>({std::nullopt, std::nullopt, std::nullopt, 12});
    auto ds = DataSliceImpl::Create(objects, values_int);
    ASSERT_TRUE(ds.is_mixed_dtype());

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_THAT(res.values<Unit>(),
                ElementsAre(kMissing, kPresent, kPresent, kPresent));
  }
  {
    // Empty result.
    auto objects = CreateDenseArray<ObjectId>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
    auto ds = DataSliceImpl::Create(objects);

    ASSERT_OK_AND_ASSIGN(auto res, HasOp()(ds));
    EXPECT_EQ(res.size(), 4);
    EXPECT_TRUE(res.is_empty_and_unknown());
  }
}

TEST(HasTest, DataItemPrimitiveValue) {
  auto item = DataItem(12);
  auto res = *HasOp()(item);
  EXPECT_EQ(res.value<Unit>(), Unit());

  item = DataItem(3.14);
  res = *HasOp()(item);
  EXPECT_EQ(res.value<Unit>(), Unit());

  item = DataItem(Text("abc"));
  res = *HasOp()(item);
  EXPECT_EQ(res.value<Unit>(), Unit());
}

TEST(HasTest, DataItemObjectId) {
  {
    // Non-empty.
    auto item = DataItem(AllocateSingleObject());
    auto res = *HasOp()(item);
    EXPECT_EQ(res.value<Unit>(), Unit());
  }
  {
    // Empty.
    auto item = DataItem();
    auto res = *HasOp()(item);
    EXPECT_EQ(res.has_value(), false);
  }
}

}  // namespace
}  // namespace koladata::internal
