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
#include "koladata/internal/op_utils/new_ids_like.h"

#include <initializer_list>
#include <optional>

#include "gtest/gtest.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;

TEST(NewIdsLikeTest, OmitPrimitiveValues) {
  auto values = CreateDenseArray<int>({1, std::nullopt, 12});
  auto ds = DataSliceImpl::Create(values);

  auto res = NewIdsLike(ds);
  EXPECT_TRUE(res.is_empty_and_unknown());
}

TEST(NewIdsLikeTest, ObjectIds) {
  auto ids = DataSliceImpl::ObjectsFromAllocation(Allocate(5), 5);
  auto res = NewIdsLike(ids);
  EXPECT_EQ(res.size(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(res[i], ids[i]);
  }
  EXPECT_NE(res[0], res[1]);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(res[i].holds_value<ObjectId>());
    EXPECT_FALSE(res[i].is_list());
    EXPECT_FALSE(res[i].is_dict());
    EXPECT_FALSE(res[i].is_schema());
  }
}

TEST(NewIdsLikeTest, ListIds) {
  auto ids = DataSliceImpl::ObjectsFromAllocation(AllocateLists(5), 5);
  auto res = NewIdsLike(ids);
  EXPECT_EQ(res.size(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(res[i], ids[i]);
  }
  EXPECT_NE(res[0], res[1]);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(res[i].is_list());
  }
}

TEST(NewIdsLikeTest, DictIds) {
  auto ids = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(5), 5);
  auto res = NewIdsLike(ids);
  EXPECT_EQ(res.size(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(res[i], ids[i]);
  }
  EXPECT_NE(res[0], res[1]);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(res[i].is_dict());
  }
}

TEST(NewIdsLikeTest, SchemaIds) {
  auto ids =
      DataSliceImpl::ObjectsFromAllocation(AllocateExplicitSchemas(5), 5);
  auto res = NewIdsLike(ids);
  EXPECT_EQ(res.size(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(res[i], ids[i]);
  }
  EXPECT_NE(res[0], res[1]);
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(res[i].is_schema());
  }
}

TEST(NewIdsLikeTest, MixedSlice) {
  auto dict_alloc = AllocateDicts(3);
  auto list_alloc = AllocateLists(2);
  auto obj_alloc = Allocate(2);
  auto ids = DataSliceImpl::Create(
      CreateDenseArray<ObjectId>({dict_alloc.ObjectByOffset(0),
                                  dict_alloc.ObjectByOffset(2), std::nullopt,
                                  std::nullopt, obj_alloc.ObjectByOffset(0),
                                  list_alloc.ObjectByOffset(1)}),
      CreateDenseArray<int>({std::nullopt, std::nullopt, 1, std::nullopt,
                             std::nullopt, std::nullopt}));
  auto res = NewIdsLike(ids);
  EXPECT_EQ(res.size(), 6);
  EXPECT_EQ(res[2], std::nullopt);
  EXPECT_EQ(res[3], std::nullopt);
  EXPECT_TRUE(res[0].is_dict());
  EXPECT_TRUE(res[1].is_dict());
  EXPECT_TRUE(res[4].holds_value<ObjectId>());
  EXPECT_TRUE(res[5].is_list());
}

}  // namespace
}  // namespace koladata::internal
