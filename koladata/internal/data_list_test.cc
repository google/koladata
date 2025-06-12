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

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

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

TEST(DataListTest, DataListVector) {
  auto vec = std::make_shared<DataListVector>(3);
  ASSERT_EQ(vec->size(), 3);
  EXPECT_EQ(vec->Get(2), nullptr);

  vec->GetMutable(2).Insert(0, 7);
  EXPECT_NE(vec->Get(2), nullptr);
  EXPECT_THAT(*vec->Get(2), ElementsAre(DataItem(7)));

  auto derived_vec = std::make_shared<DataListVector>(vec);
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

}  // namespace
}  // namespace koladata::internal
