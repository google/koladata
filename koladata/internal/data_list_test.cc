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
#include "koladata/internal/data_list.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/memory_stats.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

struct DataListVectorTestFriend {
  using ListAndPtr = DataListVector::ListAndPtr;

  static bool is_map_mode(const DataListVector& v) { return v.is_map_mode(); }
};

namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

TEST(DataListTest, Constructors) {
  {
    DataList list(std::vector<std::optional<float>>{1.0f, std::nullopt, 5.0f});
    ASSERT_EQ(list.size(), 3);
    EXPECT_EQ(list.Get(0), DataItem(1.0f));
    EXPECT_EQ(list.Get(1), DataItem());
    EXPECT_EQ(list.Get(2), DataItem(5.0f));
  }
  {
    DataList list(
        std::vector<DataItem>{DataItem(1.0f), DataItem(3), DataItem(5.0f)});
    ASSERT_EQ(list.size(), 3);
    EXPECT_EQ(list.Get(0), DataItem(1.0f));
    EXPECT_EQ(list.Get(1), DataItem(3));
    EXPECT_EQ(list.Get(2), DataItem(5.0f));
  }
  {
    SliceBuilder bldr(3);
    bldr.InsertIfNotSet(0, DataItem(1.0f));
    bldr.InsertIfNotSet(2, DataItem(5));
    DataList list(std::move(bldr).Build());
    ASSERT_EQ(list.size(), 3);
    EXPECT_EQ(list.Get(0), DataItem(1.0f));
    EXPECT_EQ(list.Get(1), DataItem());
    EXPECT_EQ(list.Get(2), DataItem(5));
  }
  {
    SliceBuilder bldr(5);
    bldr.InsertIfNotSet(2, DataItem(5));
    DataList list(std::move(bldr).Build(), 1, 3);
    ASSERT_EQ(list.size(), 2);
    EXPECT_EQ(list.Get(0), DataItem());
    EXPECT_EQ(list.Get(1), DataItem(5));
  }
  {
    SliceBuilder bldr(5);
    bldr.InsertIfNotSet(0, DataItem(1.0f));
    bldr.InsertIfNotSet(2, DataItem(5));
    DataList list(std::move(bldr).Build(), 1, 3);
    ASSERT_EQ(list.size(), 2);
    EXPECT_EQ(list.Get(0), DataItem());
    EXPECT_EQ(list.Get(1), DataItem(5));
  }
  {
    DataList list(arolla::CreateDenseArray<int>({5, 4, 3, 2, 1}), 1, 3);
    ASSERT_EQ(list.size(), 2);
    EXPECT_EQ(list.Get(0), DataItem(4));
    EXPECT_EQ(list.Get(1), DataItem(3));
  }
}

TEST(DataListTest, Modifications) {
  DataList list;
  ASSERT_EQ(list.size(), 0);
  list.Insert(0, 3);
  ASSERT_EQ(list.size(), 1);
  list.Insert(1, 4);

  ASSERT_EQ(list.size(), 2);
  EXPECT_EQ(list.Get(0), DataItem(3));
  EXPECT_EQ(list.Get(1), DataItem(4));

  list.Set(1, 5);
  list.Insert(1, std::optional<int>());

  ASSERT_EQ(list.size(), 3);
  EXPECT_EQ(list.Get(0), DataItem(3));
  EXPECT_EQ(list.Get(1), DataItem());
  EXPECT_EQ(list.Get(2), DataItem(5));

  list.Insert(0, arolla::Bytes("abc"));
  list.Remove(2, 1);

  ASSERT_EQ(list.size(), 3);
  EXPECT_EQ(list.Get(0), DataItem(arolla::Bytes("abc")));
  EXPECT_EQ(list.Get(1), DataItem(3));
  EXPECT_EQ(list.Get(2), DataItem(5));

  list.Set(0, arolla::Bytes("cde"));
  list.Insert(list.size(), DataItem(true));

  ASSERT_EQ(list.size(), 4);
  EXPECT_EQ(list.Get(0), DataItem(arolla::Bytes("cde")));
  EXPECT_EQ(list.Get(1), DataItem(3));
  EXPECT_EQ(list.Get(2), DataItem(5));
  EXPECT_EQ(list.Get(3), DataItem(true));

  list.Resize(2);  // shrink
  list.Resize(3);  // extend with missing

  EXPECT_THAT(list, ElementsAre(DataItem(arolla::Bytes("cde")), DataItem(3),
                                DataItem()));

  list.InsertMissing(1, 2);

  EXPECT_THAT(list, ElementsAre(DataItem(arolla::Bytes("cde")), DataItem(),
                                DataItem(), DataItem(3), DataItem()));

  list.Set(1, 1);
  list.SetToMissing(0);

  EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(1), DataItem(),
                                DataItem(3), DataItem()));

  list.Set(1, MissingValue());

  EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(), DataItem(), DataItem(3),
                                DataItem()));

  list.Remove(0, list.size());
  ASSERT_EQ(list.size(), 0);
}

TEST(DataListTest, SetMissingRange) {
  DataList list;
  list.Resize(5);
  list.Set(0, 5);
  list.Set(1, 4);
  list.Set(2, 3);
  list.Set(3, 2);
  list.Set(4, 1);
  list.SetMissingRange(1, 3);
  EXPECT_THAT(list, ElementsAre(DataItem(5), DataItem(), DataItem(),
                                DataItem(2), DataItem(1)));
}

TEST(DataListTest, SetN) {
  DataList list;
  list.Resize(6);
  list.SetN(1, arolla::CreateDenseArray<int>({1, 2, 3, 4}));
  EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(1), DataItem(2),
                                DataItem(3), DataItem(4), DataItem()));
  list.SetN(0, arolla::CreateDenseArray<int>({5, {}, {}}));
  EXPECT_THAT(list, ElementsAre(DataItem(5), DataItem(), DataItem(),
                                DataItem(3), DataItem(4), DataItem()));
  list.SetN(2, arolla::CreateDenseArray<double>({1.0, {}, 3.0, 4.0}));
  EXPECT_THAT(list, ElementsAre(DataItem(5), DataItem(), DataItem(1.0),
                                DataItem(), DataItem(3.0), DataItem(4.0)));
}

TEST(DataListTest, AllMissing) {
  {
    DataList list;
    list.Insert(0, MissingValue());
    list.Insert(0, MissingValue());
    list.Insert(2, MissingValue());
    EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(), DataItem()));

    list.Remove(1, 1);
    EXPECT_THAT(list, ElementsAre(DataItem(), DataItem()));

    list.Insert(1, 5);
    EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(5), DataItem()));
  }
  {
    DataList list;
    list.Insert(0, MissingValue());
    list.Insert(0, MissingValue());
    list.Set(0, MissingValue());
    EXPECT_THAT(list, ElementsAre(DataItem(), DataItem()));

    list.Set(1, DataItem(5));
    EXPECT_THAT(list, ElementsAre(DataItem(), DataItem(5)));
  }
}

TEST(DataListTest, AddToDataSlice) {
  {  // offset
    DataList list(arolla::CreateDenseArray<int>({5, 4, std::nullopt, 2, 1}));
    SliceBuilder bldr(6);
    list.AddToDataSlice(bldr, 1);
    DataSliceImpl ds = std::move(bldr).Build();
    ASSERT_EQ(ds.dtype(), arolla::GetQType<int>());
    EXPECT_THAT(ds.values<int>(),
                ElementsAre(std::nullopt, 5, 4, std::nullopt, 2, 1));
  }
  {  // offset and slicing
    DataList list(arolla::CreateDenseArray<int>({5, 4, std::nullopt, 2, 1}));
    SliceBuilder bldr(6);
    list.AddToDataSlice(bldr, 2, 1, 4);
    DataSliceImpl ds = std::move(bldr).Build();
    ASSERT_EQ(ds.dtype(), arolla::GetQType<int>());
    EXPECT_THAT(ds.values<int>(),
                ElementsAre(std::nullopt, std::nullopt, 4, std::nullopt, 2,
                            std::nullopt));
  }
  {  // ObjectId
    ObjectId obj = AllocateSingleObject();
    AllocationId alloc = Allocate(47);
    DataList list(arolla::CreateDenseArray<ObjectId>(
        {alloc.ObjectByOffset(0), obj, alloc.ObjectByOffset(2)}));
    SliceBuilder bldr(3);
    list.AddToDataSlice(bldr, 0);
    DataSliceImpl ds = std::move(bldr).Build();
    EXPECT_THAT(
        ds, ElementsAre(alloc.ObjectByOffset(0), obj, alloc.ObjectByOffset(2)));
    EXPECT_TRUE(ds.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(ds.allocation_ids().ids(), ElementsAre(alloc));
  }
  {  // mixed types
    DataList list(arolla::CreateDenseArray<int>({5, 4, std::nullopt, 2, 1}));
    list.Set(1, 3.5f);
    AllocationId alloc = Allocate(47);
    list.Set(3, alloc.ObjectByOffset(3));
    SliceBuilder bldr(4);
    list.AddToDataSlice(bldr, 0, 1);
    DataSliceImpl ds = std::move(bldr).Build();
    EXPECT_THAT(ds, ElementsAre(3.5f, DataItem(), alloc.ObjectByOffset(3), 1));
    EXPECT_FALSE(ds.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(ds.allocation_ids().ids(), ElementsAre(alloc));
  }
}

TEST(DataListTest, DataListVectorSmallCapacity) {
  auto vec = std::make_shared<DataListVector>(4, /*update_size=*/1);
  ASSERT_EQ(vec->size(), 4);
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_EQ(vec->Get(2), nullptr);

  vec->GetMutable(2).Insert(0, 7);
  EXPECT_NE(vec->Get(2), nullptr);
  EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(7)));
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));

  auto derived_vec = std::make_shared<DataListVector>(vec, /*update_size=*/1);
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*derived_vec));
  derived_vec->GetMutable(1).Insert(0, 5);

  EXPECT_EQ(derived_vec->Get(0), nullptr);
  EXPECT_EQ(derived_vec->Get(1)->size(), 1);
  EXPECT_EQ(derived_vec->Get(2)->size(), 1);

  EXPECT_EQ(vec->Get(0), derived_vec->Get(0));  // unset list is nullptr
  EXPECT_NE(vec->Get(1), derived_vec->Get(1));  // list copied on modification
  EXPECT_EQ(vec->Get(2), derived_vec->Get(2));

  derived_vec->GetMutable(2).Insert(0, 9);
  EXPECT_NE(vec->Get(2), derived_vec->Get(2));
  EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(7)));
  EXPECT_THAT(*derived_vec->Get(2), ElementsAre(DataItem(9), DataItem(7)));
}

TEST(DataListTest, DataListVectorMapMode) {
  // Capacity 10: 30% of 10 is 3. Up to 3 elements in flat_hash_map.
  auto vec = std::make_shared<DataListVector>(10, /*update_size=*/1);
  ASSERT_EQ(vec->size(), 10);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_EQ(vec->Get(5), nullptr);

  // 1st element (10%)
  vec->GetMutable(1).Insert(0, 100);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(*vec->Get(1), ElementsAre(DataItem(100)));
  EXPECT_EQ(vec->Get(0), nullptr);

  // 2nd element (20%)
  vec->GetMutable(4).Insert(0, 400);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(*vec->Get(4), ElementsAre(DataItem(400)));

  // 3rd element (30%)
  vec->GetMutable(7).Insert(0, 700);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(*vec->Get(7), ElementsAre(DataItem(700)));

  // 4th element (40% > 30%) -> triggers transition to vector
  vec->GetMutable(9).Insert(0, 900);
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));

  // Verify all elements after transition to vector
  EXPECT_EQ(vec->Get(0), nullptr);
  EXPECT_THAT(*vec->Get(1), ElementsAre(DataItem(100)));
  EXPECT_EQ(vec->Get(2), nullptr);
  EXPECT_EQ(vec->Get(3), nullptr);
  EXPECT_THAT(*vec->Get(4), ElementsAre(DataItem(400)));
  EXPECT_EQ(vec->Get(5), nullptr);
  EXPECT_EQ(vec->Get(6), nullptr);
  EXPECT_THAT(*vec->Get(7), ElementsAre(DataItem(700)));
  EXPECT_EQ(vec->Get(8), nullptr);
  EXPECT_THAT(*vec->Get(9), ElementsAre(DataItem(900)));
}

TEST(DataListTest, DataListVectorDerivedMap) {
  auto parent = std::make_shared<DataListVector>(10, /*update_size=*/1);
  parent->GetMutable(2).Insert(0, 20);
  parent->GetMutable(5).Insert(0, 50);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*parent));

  auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/1);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));

  // Read unmodified from parent
  EXPECT_EQ(derived->Get(0), nullptr);
  EXPECT_EQ(derived->Get(2), parent->Get(2));
  EXPECT_THAT(*derived->Get(2), ElementsAre(DataItem(20)));
  EXPECT_EQ(derived->Get(5), parent->Get(5));
  EXPECT_THAT(*derived->Get(5), ElementsAre(DataItem(50)));

  // Modify 1 element in derived (copy-on-write from parent)
  derived->GetMutable(2).Insert(0, 21);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
  EXPECT_NE(derived->Get(2), parent->Get(2));
  EXPECT_THAT(*parent->Get(2), ElementsAre(DataItem(20)));
  EXPECT_THAT(*derived->Get(2), ElementsAre(DataItem(21), DataItem(20)));

  // Modify a previously unset element in derived
  derived->GetMutable(0).Insert(0, 1);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
  EXPECT_THAT(*derived->Get(0), ElementsAre(DataItem(1)));
  EXPECT_EQ(parent->Get(0), nullptr);

  // Modify 3rd element in derived (3/10 = 30%)
  derived->GetMutable(3).Insert(0, 30);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));

  // Modify 4th element in derived (4/10 = 40% > 30%) -> triggers transition to
  // vector
  derived->GetMutable(4).Insert(0, 40);
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*derived));

  // Verify all elements in derived after vector transition
  EXPECT_THAT(*derived->Get(0), ElementsAre(DataItem(1)));
  EXPECT_THAT(*derived->Get(2), ElementsAre(DataItem(21), DataItem(20)));
  EXPECT_THAT(*derived->Get(3), ElementsAre(DataItem(30)));
  EXPECT_THAT(*derived->Get(4), ElementsAre(DataItem(40)));
  EXPECT_EQ(derived->Get(5), parent->Get(5));
  EXPECT_THAT(*derived->Get(5), ElementsAre(DataItem(50)));
  EXPECT_EQ(derived->Get(1), nullptr);
  EXPECT_EQ(derived->Get(6), nullptr);

  // Parent should remain unaffected
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*parent));
  EXPECT_EQ(parent->Get(0), nullptr);
  EXPECT_THAT(*parent->Get(2), ElementsAre(DataItem(20)));
  EXPECT_THAT(*parent->Get(5), ElementsAre(DataItem(50)));
}

TEST(DataListTest, DataListVectorCapacityFive) {
  // Capacity 5: 30% of 5 is 1.5. 1 element (20%) fits in map mode.
  auto vec = std::make_shared<DataListVector>(5, /*update_size=*/1);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));

  vec->GetMutable(1).Insert(0, 10);
  EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(*vec->Get(1), ElementsAre(DataItem(10)));

  // 2nd element (40% > 30%) -> converts to array
  vec->GetMutable(2).Insert(0, 20);
  EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(*vec->Get(1), ElementsAre(DataItem(10)));
  EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(20)));
  EXPECT_EQ(vec->Get(0), nullptr);
  EXPECT_EQ(vec->Get(3), nullptr);
  EXPECT_EQ(vec->Get(4), nullptr);
}

TEST(DataListTest, DataListVectorEmptyMemoryStats) {
  // Map mode
  {
    DataListVector vec(10, /*update_size=*/1);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(vec));
    MemoryStatsEntry stats;
    vec.AppendMemoryStats(stats);
    EXPECT_EQ(stats.shallow_size, sizeof(DataListVector));
    EXPECT_EQ(stats.strings_size, 0);
  }
  // Vector mode
  {
    const int kSize = 2;
    DataListVector vec(kSize, /*update_size=*/1);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(vec));
    MemoryStatsEntry stats;
    vec.AppendMemoryStats(stats);
    EXPECT_EQ(stats.shallow_size,
              sizeof(DataListVector) +
                  kSize * sizeof(DataListVectorTestFriend::ListAndPtr));
    EXPECT_EQ(stats.strings_size, 0);
  }
}

TEST(DataListTest, DataListVectorUpdateSize) {
  // Capacity 10, update_size = 5 (> 30% of 10): directly in Array mode.
  {
    auto vec = std::make_shared<DataListVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));
    vec->GetMutable(2).Insert(0, 20);
    EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(20)));
  }

  // Capacity 10, update_size = 2 (<= 30% of 10): in Map mode.
  {
    auto vec = std::make_shared<DataListVector>(10, /*update_size=*/2);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
    vec->GetMutable(2).Insert(0, 20);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));
    EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(20)));
  }

  // Derived vector with large update_size -> directly in Array mode.
  {
    auto parent = std::make_shared<DataListVector>(10, /*update_size=*/1);
    parent->GetMutable(1).Insert(0, 10);
    auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/4);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(*derived->Get(1), ElementsAre(DataItem(10)));
    derived->GetMutable(2).Insert(0, 20);
    EXPECT_THAT(*derived->Get(2), ElementsAre(DataItem(20)));
    EXPECT_THAT(*derived->Get(1), ElementsAre(DataItem(10)));
  }

  // Derived vector with small update_size -> in Map mode.
  {
    auto parent = std::make_shared<DataListVector>(10, /*update_size=*/1);
    parent->GetMutable(1).Insert(0, 10);
    auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/2);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(*derived->Get(1), ElementsAre(DataItem(10)));
    derived->GetMutable(2).Insert(0, 20);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(*derived->Get(2), ElementsAre(DataItem(20)));
  }
}

TEST(DataListTest, DataListVectorForEachList) {
  // 1. Map mode (sparse, no parent)
  {
    auto vec = std::make_shared<DataListVector>(10, /*update_size=*/1);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*vec));

    // Empty vector: callback not called.
    int count = 0;
    EXPECT_OK(vec->ForEachList([&](size_t idx, const DataList& list) {
      ++count;
      return absl::OkStatus();
    }));
    EXPECT_EQ(count, 0);

    vec->GetMutable(2).Insert(0, 20);
    vec->GetMutable(7).Insert(0, 70);

    std::vector<size_t> visited_indices;
    EXPECT_OK(vec->ForEachList([&](size_t idx, const DataList& list) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(list, ElementsAre(DataItem(20)));
      } else if (idx == 7) {
        EXPECT_THAT(list, ElementsAre(DataItem(70)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 7));
  }

  // 2. Array mode
  {
    auto vec = std::make_shared<DataListVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*vec));

    vec->GetMutable(1).Insert(0, 10);
    vec->GetMutable(4).Insert(0, 40);

    std::vector<size_t> visited_indices;
    EXPECT_OK(vec->ForEachList([&](size_t idx, const DataList& list) {
      visited_indices.push_back(idx);
      if (idx == 1) {
        EXPECT_THAT(list, ElementsAre(DataItem(10)));
      } else if (idx == 4) {
        EXPECT_THAT(list, ElementsAre(DataItem(40)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(1, 4));
  }

  // 3. Derived with parent (both Map mode)
  {
    auto parent = std::make_shared<DataListVector>(10, /*update_size=*/1);
    parent->GetMutable(2).Insert(0, 20);
    parent->GetMutable(5).Insert(0, 50);

    auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Insert(0, 51);
    derived->GetMutable(8).Insert(0, 80);

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachList([&](size_t idx, const DataList& list) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(list, ElementsAre(DataItem(20)));
      } else if (idx == 5) {
        EXPECT_THAT(list, ElementsAre(DataItem(51), DataItem(50)));
      } else if (idx == 8) {
        EXPECT_THAT(list, ElementsAre(DataItem(80)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5, 8));
  }

  // 4. Derived with parent (Array mode)
  {
    auto parent = std::make_shared<DataListVector>(10, /*update_size=*/1);
    parent->GetMutable(2).Insert(0, 20);
    parent->GetMutable(5).Insert(0, 50);

    auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/5);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Insert(0, 51);

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachList([&](size_t idx, const DataList& list) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(list, ElementsAre(DataItem(20)));
      } else if (idx == 5) {
        EXPECT_THAT(list, ElementsAre(DataItem(51), DataItem(50)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5));
  }

  // 5. Derived in Map mode with parent in Array mode
  {
    auto parent = std::make_shared<DataListVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DataListVectorTestFriend::is_map_mode(*parent));
    parent->GetMutable(2).Insert(0, 20);
    parent->GetMutable(5).Insert(0, 50);

    // Case 5a: derived has empty map (no modifications yet)
    auto derived_empty =
        std::make_shared<DataListVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived_empty));
    {
      std::vector<size_t> visited_indices;
      EXPECT_OK(
          derived_empty->ForEachList([&](size_t idx, const DataList& list) {
            visited_indices.push_back(idx);
            if (idx == 2) {
              EXPECT_THAT(list, ElementsAre(DataItem(20)));
            } else if (idx == 5) {
              EXPECT_THAT(list, ElementsAre(DataItem(50)));
            }
            return absl::OkStatus();
          }));
      EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5));
    }

    // Case 5b: derived has non-empty map (overriding index 5, adding index 8)
    auto derived = std::make_shared<DataListVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DataListVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Insert(0, 51);
    derived->GetMutable(8).Insert(0, 80);

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachList([&](size_t idx, const DataList& list) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(list, ElementsAre(DataItem(20)));
      } else if (idx == 5) {
        EXPECT_THAT(list, ElementsAre(DataItem(51), DataItem(50)));
      } else if (idx == 8) {
        EXPECT_THAT(list, ElementsAre(DataItem(80)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5, 8));
  }

  // 6. Callback returning absl::Status
  {
    auto vec = std::make_shared<DataListVector>(10, /*update_size=*/1);
    vec->GetMutable(1).Insert(0, 10);
    vec->GetMutable(2).Insert(0, 20);
    vec->GetMutable(3).Insert(0, 30);

    // Success case
    int count = 0;
    EXPECT_OK(
        vec->ForEachList([&](size_t idx, const DataList& list) -> absl::Status {
          ++count;
          return absl::OkStatus();
        }));
    EXPECT_EQ(count, 3);

    // Early termination on error
    count = 0;
    auto status =
        vec->ForEachList([&](size_t idx, const DataList& list) -> absl::Status {
          ++count;
          return absl::InternalError("stop");
        });
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, "stop"));
    EXPECT_EQ(count, 1);
  }
}

}  // namespace
}  // namespace koladata::internal
