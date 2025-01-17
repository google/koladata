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
#include <cstdint>
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

namespace {

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

}  // namespace

}  // namespace
}  // namespace koladata::internal
