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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types_buffer.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

namespace {

TEST(DataBagTest, ExtractSingleItemRemovedValues) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a = DataItem(AllocateSingleObject());
  ASSERT_OK(db->SetAttr(ds_a, "a", DataItem()));

  ASSERT_OK_AND_ASSIGN(auto content, db->ExtractContent());
  const auto& items = content.attrs["a"].items;
  ASSERT_EQ(items.size(), 1);
  EXPECT_EQ(items[0].object_id, ds_a.value<ObjectId>());
  EXPECT_EQ(items[0].value, DataItem());
}

TEST(DataBagTest, ExtractSingleItemBigAllocRemovedValues) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  size_t size = 5;
  auto ds_a = DataItem(Allocate(size).ObjectByOffset(3));
  ASSERT_OK(db->SetAttr(ds_a, "a", DataItem()));

  ASSERT_OK_AND_ASSIGN(auto content, db->ExtractContent());
  const auto& allocs = content.attrs["a"].allocs;
  ASSERT_EQ(allocs.size(), 1);
  EXPECT_EQ(allocs[0].alloc_id, AllocationId(ds_a.value<ObjectId>()));
  const auto& tb = allocs[0].values.types_buffer();
  EXPECT_THAT(
      tb.id_to_typeidx,
      ElementsAre(TypesBuffer::kUnset, TypesBuffer::kUnset, TypesBuffer::kUnset,
                  TypesBuffer::kRemoved, TypesBuffer::kUnset,
                  // Above the size.
                  TypesBuffer::kUnset, TypesBuffer::kUnset,
                  TypesBuffer::kUnset));
  EXPECT_THAT(allocs[0].values, ElementsAreArray(std::vector<DataItem>(
                                    std::bit_ceil(size), DataItem())));
}

TEST(DataBagTest, ExtractRemovedValues) {
  int64_t size = 5;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
  auto ds_b = DataSliceImpl::AllocateEmptyObjects(size);
  std::vector<DataItem> ds_a_values(size);
  std::vector<DataItem> ds_b_values(size);
  for (int i = 0; i < size; ++i) {
    ds_a_values[i] = ds_a[i];
    ds_b_values[i] = ds_b[i];
  }
  ds_a_values[1] = DataItem();
  ds_b_values[3] = DataItem();
  ds_a = DataSliceImpl::Create(ds_a_values);
  ds_b = DataSliceImpl::Create(ds_b_values);
  ASSERT_OK(db->SetAttr(ds_a, "a", ds_b));

  ASSERT_OK_AND_ASSIGN(auto content, db->ExtractContent());
  const auto& allocs = content.attrs["a"].allocs;
  ASSERT_EQ(allocs.size(), 1);
  EXPECT_EQ(allocs[0].alloc_id, ds_a.allocation_ids().ids()[0]);
  const auto& tb = allocs[0].values.types_buffer();
  EXPECT_THAT(tb.id_to_typeidx,
              ElementsAre(0, TypesBuffer::kUnset, 0, TypesBuffer::kRemoved, 0,
                          // Above the size.
                          TypesBuffer::kUnset, TypesBuffer::kUnset,
                          TypesBuffer::kUnset));
  ds_b_values[1] = DataItem();
  // Add elements above the size.
  ds_b_values.insert(ds_b_values.end(), 3, DataItem());
  EXPECT_THAT(allocs[0].values, ElementsAreArray(ds_b_values));
}

TEST(DataBagTest, ExtractDictRemovedValues) {
  auto dict = DataItem(AllocateSingleDict());

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(dict, DataItem(1), DataItem()));
  ASSERT_OK(db->SetInDict(dict, DataItem(2), DataItem(3)));

  ASSERT_OK_AND_ASSIGN(auto content, db->ExtractContent());
  const auto& dicts = content.dicts;
  ASSERT_EQ(dicts.size(), 1);
  const auto& d = dicts[0];
  EXPECT_EQ(d.dict_id, dict.value<ObjectId>());
  ASSERT_THAT(d.keys, UnorderedElementsAre(DataItem(1), DataItem(2)));
  ASSERT_THAT(d.values, UnorderedElementsAre(DataItem(), DataItem(3)));
  ASSERT_THAT((std::vector<std::pair<DataItem, DataItem>>{
                  {d.keys[0], d.values[0]}, {d.keys[1], d.values[1]}}),
              UnorderedElementsAre(Pair(DataItem(1), DataItem()),
                                   Pair(DataItem(2), DataItem(3))));
}

}  // namespace

}  // namespace
}  // namespace koladata::internal
