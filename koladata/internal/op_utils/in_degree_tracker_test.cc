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
#include "koladata/internal/op_utils/in_degree_tracker.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

TEST(InDegreeTrackerTest, FilterToObjectsPureObjectIdSlice) {
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({obj0, std::nullopt, obj1}));

  auto filtered = InDegreeTracker::FilterToObjects(ds);
  EXPECT_EQ(filtered.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(filtered.size(), 3);
  EXPECT_EQ(filtered[0], DataItem(obj0));
  EXPECT_EQ(filtered[1], DataItem());
  EXPECT_EQ(filtered[2], DataItem(obj1));
}

TEST(InDegreeTrackerTest, FilterToObjectsPrimitiveSlice) {
  auto ds_int = DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3}));
  auto filtered = InDegreeTracker::FilterToObjects(ds_int);
  EXPECT_TRUE(filtered.is_empty_and_unknown());
  EXPECT_EQ(filtered.size(), 0);

  auto ds_text = DataSliceImpl::Create(
      arolla::CreateDenseArray<arolla::Text>({arolla::Text("abc")}));
  EXPECT_TRUE(InDegreeTracker::FilterToObjects(ds_text).is_empty_and_unknown());
}

TEST(InDegreeTrackerTest, FilterToObjectsEmptySlice) {
  auto ds_empty = DataSliceImpl::CreateEmptyAndUnknownType(0);
  EXPECT_TRUE(
      InDegreeTracker::FilterToObjects(ds_empty).is_empty_and_unknown());

  auto ds_empty_size = DataSliceImpl::CreateEmptyAndUnknownType(5);
  EXPECT_TRUE(
      InDegreeTracker::FilterToObjects(ds_empty_size).is_empty_and_unknown());
}

TEST(InDegreeTrackerTest, FilterToObjectsMixedSlice) {
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto ds_mixed = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>(
      {DataItem(obj0), DataItem(42), DataItem(), DataItem(obj1)}));
  EXPECT_TRUE(ds_mixed.is_mixed_dtype());

  auto filtered = InDegreeTracker::FilterToObjects(ds_mixed);
  EXPECT_EQ(filtered.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(filtered.size(), 4);
  EXPECT_EQ(filtered[0], DataItem(obj0));
  EXPECT_EQ(filtered[1], DataItem());  // Was int 42
  EXPECT_EQ(filtered[2], DataItem());  // Was missing
  EXPECT_EQ(filtered[3], DataItem(obj1));
}

TEST(InDegreeTrackerTest, FilterToObjectsMixedWithoutObjects) {
  auto ds_mixed = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>(
      {DataItem(42), DataItem(arolla::Text("foo")), DataItem()}));
  EXPECT_TRUE(ds_mixed.is_mixed_dtype());

  auto filtered = InDegreeTracker::FilterToObjects(ds_mixed);
  EXPECT_TRUE(filtered.is_empty_and_unknown());
}

TEST(InDegreeTrackerTest, IncrementAndGetNewBasic) {
  InDegreeTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto obj2 = AllocateSingleObject();

  auto ds1 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0, obj1}));

  // First previsit: both obj0 and obj1 reach in-degree 1.
  ASSERT_OK_AND_ASSIGN(auto res1, tracker.IncrementAndGetNew(ds1));
  EXPECT_THAT(res1.values<ObjectId>(), UnorderedElementsAre(obj0, obj1));

  // Second previsit with obj1 and obj2:
  // obj1 in-degree: 1 -> 2 (does not reach 1).
  // obj2 in-degree: 0 -> 1 (reaches 1, newly visited).
  auto ds2 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj1, obj2}));
  ASSERT_OK_AND_ASSIGN(auto res2, tracker.IncrementAndGetNew(ds2));
  EXPECT_THAT(res2.values<ObjectId>(), ElementsAre(obj2));
}

TEST(InDegreeTrackerTest, IncrementAndGetNewWithDuplicatesInSlice) {
  InDegreeTracker tracker;
  auto obj_a = AllocateSingleObject();
  auto obj_b = AllocateSingleObject();

  // obj_a appears 3 times, obj_b appears once.
  // obj_a reaches target 1 on the first occurrence, then increments to 3.
  // obj_a must appear in the result slice only once.
  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({obj_a, obj_b, obj_a, obj_a}));

  ASSERT_OK_AND_ASSIGN(auto res, tracker.IncrementAndGetNew(ds));
  EXPECT_THAT(res.values<ObjectId>(), UnorderedElementsAre(obj_a, obj_b));
}

TEST(InDegreeTrackerTest, DecrementAndGetLastBasic) {
  InDegreeTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto obj2 = AllocateSingleObject();

  // Setup initial in-degrees:
  // obj0: in-degree 1
  // obj1: in-degree 2
  // obj2: in-degree 1
  ASSERT_OK(tracker.IncrementAndGetNew(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0, obj1}))));
  ASSERT_OK(tracker.IncrementAndGetNew(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj1, obj2}))));

  // Process topological order slice: decrement obj0 and obj1.
  // obj0: 1 -> 0 (now free / last!).
  // obj1: 2 -> 1 (not free yet).
  auto ds_dec1 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0, obj1}));
  ASSERT_OK_AND_ASSIGN(auto res1, tracker.DecrementAndGetLast(ds_dec1));
  EXPECT_THAT(res1.values<ObjectId>(), ElementsAre(obj0));

  // Decrement obj1 again: 1 -> 0 (now free / last!).
  auto ds_dec2 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj1}));
  ASSERT_OK_AND_ASSIGN(auto res2, tracker.DecrementAndGetLast(ds_dec2));
  EXPECT_THAT(res2.values<ObjectId>(), ElementsAre(obj1));

  // Decrementing again below 0: 0 -> -1 (not returning).
  ASSERT_OK_AND_ASSIGN(auto res3, tracker.DecrementAndGetLast(ds_dec2));
  EXPECT_EQ(res3.size(), 0);
  EXPECT_THAT(res3, ElementsAre());
}

TEST(InDegreeTrackerTest, DecrementAndGetLastWithDuplicates) {
  InDegreeTracker tracker;
  auto obj = AllocateSingleObject();

  // Increment obj twice: 0 -> 1 -> 2.
  ASSERT_OK(tracker.IncrementAndGetNew(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj}))));
  ASSERT_OK(tracker.IncrementAndGetNew(
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj}))));

  // Decrement obj twice in the same slice: 2 -> 1 -> 0 (reaches 0).
  // Must return obj once.
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj, obj}));
  ASSERT_OK_AND_ASSIGN(auto res, tracker.DecrementAndGetLast(ds));
  EXPECT_THAT(res.values<ObjectId>(), ElementsAre(obj));
}

TEST(InDegreeTrackerTest, ProcessPrimitivesAndNullopts) {
  InDegreeTracker tracker;
  auto obj = AllocateSingleObject();

  // Primitive slice: no objects tracked, returns empty.
  auto ds_int = DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3}));
  ASSERT_OK_AND_ASSIGN(auto res_int, tracker.IncrementAndGetNew(ds_int));
  EXPECT_TRUE(res_int.is_empty_and_unknown() || res_int.size() == 0);

  // Mixed slice with primitives and nullopt:
  auto ds_mixed = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>(
      {DataItem(123), DataItem(), DataItem(obj)}));
  ASSERT_OK_AND_ASSIGN(auto res_mixed, tracker.IncrementAndGetNew(ds_mixed));
  EXPECT_THAT(res_mixed.values<ObjectId>(), ElementsAre(obj));

  // Nullopt ObjectId:
  auto ds_nullopt =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({std::nullopt}));
  ASSERT_OK_AND_ASSIGN(auto res_null, tracker.IncrementAndGetNew(ds_nullopt));
  EXPECT_EQ(res_null.size(), 0);

  // Empty and unknown slice:
  ASSERT_OK_AND_ASSIGN(
      auto res_empty,
      tracker.IncrementAndGetNew(DataSliceImpl::CreateEmptyAndUnknownType(0)));
  EXPECT_TRUE(res_empty.is_empty_and_unknown());
}

TEST(InDegreeTrackerTest, BigAndSmallAllocations) {
  InDegreeTracker tracker;
  auto small0 = AllocateSingleObject();
  auto small1 = AllocateSingleObject();

  AllocationId alloc = Allocate(4);
  auto big0 = alloc.ObjectByOffset(0);
  auto big1 = alloc.ObjectByOffset(1);

  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({small0, big0, small1, big1, small0}));

  // small0 reaches 1 on first occurrence, ends at 2.
  // big0, small1, big1 each reach 1.
  ASSERT_OK_AND_ASSIGN(auto res, tracker.IncrementAndGetNew(ds));
  EXPECT_THAT(res.values<ObjectId>(),
              UnorderedElementsAre(small0, small1, big0, big1));

  // Decrement big0 and small1 to 0.
  auto ds_dec =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({big0, small1}));
  ASSERT_OK_AND_ASSIGN(auto res_dec, tracker.DecrementAndGetLast(ds_dec));
  EXPECT_THAT(res_dec.values<ObjectId>(), UnorderedElementsAre(big0, small1));
}

}  // namespace
}  // namespace koladata::internal
