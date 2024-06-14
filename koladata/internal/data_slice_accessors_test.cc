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
#include "koladata/internal/data_slice_accessors.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/sparse_source.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::arolla::OptionalValue;
using ::arolla::Unit;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsEmpty;

TEST(DataSliceAccessorsTest, GetAttributeFromSources_SingleSourceObjects) {
  constexpr int64_t kSize = 19;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds_a.allocation_ids().size(), 1);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> source,
      DenseSource::CreateReadonly(ds.allocation_ids().ids()[0], ds_a));

  DataItem ds_a_5_item = GetAttributeFromSources(ds.values<ObjectId>()[5].value,
                                                 {source.get()}, {});
  EXPECT_EQ(ds_a_5_item.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds_a_5_item.value<ObjectId>(), ds_a.values<ObjectId>()[5]);

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                       GetAttributeFromSources(ds, {source.get()}, {}));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(),
              ElementsAreArray(ds_a.allocation_ids()));
  EXPECT_EQ(ds_a_get.values<ObjectId>().size(), kSize);
  EXPECT_THAT(ds_a_get.values<ObjectId>(),
              ElementsAreArray(ds_a.values<ObjectId>()));

  auto ds_other = DataSliceImpl::AllocateEmptyObjects(kSize);
  DataSliceImpl ds_mixed =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {ds.values<ObjectId>()[0], ds_other.values<ObjectId>()[1],
           ds.values<ObjectId>()[2]}));
  // `ds_other` doesn't present in sources, so the second item will be missing.
  EXPECT_THAT(GetAttributeFromSources(ds_mixed, {source.get()}, {}),
              ::koladata::testing::IsOkAndHolds(
                  ElementsAre(ds_a[0], std::nullopt, ds_a[2])));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_SingleSourceObjectsPartial) {
  constexpr int64_t kSize = 19;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds_a.allocation_ids().size(), 1);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> source,
      DenseSource::CreateReadonly(ds.allocation_ids().ids()[0], ds_a));

  auto ds_f = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateFullDenseArray<ObjectId>(
          {ds.values<ObjectId>().values[0],
           ds.values<ObjectId>().values[5]}),
      ds.allocation_ids());

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                       GetAttributeFromSources(ds_f, {source.get()}, {}));

  EXPECT_EQ(ds_a_get.size(), 2);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(),
              ElementsAreArray(ds_a.allocation_ids()));
  EXPECT_EQ(ds_a_get.values<ObjectId>().size(), 2);
  EXPECT_THAT(ds_a_get.values<ObjectId>(),
              ElementsAre(ds_a.values<ObjectId>()[0],
                          ds_a.values<ObjectId>()[5]));
}

TEST(DataSliceAccessorsTest, GetAttributeFromSources_SingleSourcePrimitives) {
  constexpr int64_t kSize = 4;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto a_values = arolla::CreateDenseArray<int64_t>(
      std::vector<OptionalValue<int64_t>>{17, std::nullopt, 57, std::nullopt});
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> source,
      DenseSource::CreateReadonly(ds.allocation_ids().ids()[0],
                                  DataSliceImpl::Create(a_values)));

  {
    DataItem ds_a_2_item = GetAttributeFromSources(
        ds.values<ObjectId>()[2].value, {source.get()}, {});
    EXPECT_EQ(ds_a_2_item.dtype(), arolla::GetQType<int64_t>());
    EXPECT_EQ(ds_a_2_item.value<int64_t>(), 57);
  }
  {
    DataItem ds_a_3_item = GetAttributeFromSources(
        ds.values<ObjectId>()[3].value, {source.get()}, {});
    EXPECT_EQ(ds_a_3_item.dtype(), arolla::GetNothingQType());
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                       GetAttributeFromSources(ds, {source.get()}, {}));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAreArray(a_values));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_SingleSourcePrimitivesPartial) {
  constexpr int64_t kSize = 4;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto a_values = arolla::CreateDenseArray<int64_t>(
      std::vector<OptionalValue<int64_t>>{17, std::nullopt, 57, std::nullopt});
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> source,
      DenseSource::CreateReadonly(ds.allocation_ids().ids()[0],
                                  DataSliceImpl::Create(a_values)));

  auto ds_f = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateFullDenseArray<ObjectId>(
          {ds.values<ObjectId>().values[0],
           ds.values<ObjectId>().values[3]}),
      ds.allocation_ids());

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                       GetAttributeFromSources(ds_f, {source.get()}, {}));

  EXPECT_EQ(ds_a_get.size(), 2);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAre(17, std::nullopt));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_SingleSourceObjects_Empty) {
  for (int64_t kSize : {19, 37}) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(kSize + 1);
    auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize + 1);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<const DenseSource> source,
        DenseSource::CreateReadonly(ds.allocation_ids().ids()[0], ds_a));
    auto ds_req = DataSliceImpl::CreateAllMissingObjectDataSlice(kSize);
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                         GetAttributeFromSources(ds_req, {source.get()}, {}));

    EXPECT_EQ(ds_a_get.size(), kSize);
    EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(ds_a_get.allocation_ids(),
                ElementsAre(ds_a.allocation_ids().ids()[0]));

    EXPECT_EQ(ds_a_get.values<ObjectId>().size(), kSize);
    EXPECT_THAT(ds_a_get.values<ObjectId>(),
                ElementsAreArray(
                    std::vector<OptionalValue<ObjectId>>(kSize, std::nullopt)));
  }
}

TEST(DataSliceAccessorsTest, GetAttributeFromSources_EmptyWithAllocationIds) {
  constexpr int64_t kSize = 4;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<const DenseSource> source,
      DenseSource::CreateReadonly(ds.allocation_ids().ids()[0], ds));
  auto ds_empty = DataSliceImpl::CreateEmptyAndUnknownType(kSize);
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_res,
                       GetAttributeFromSources(ds_empty, {source.get()}, {}));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_DenseSparseSourcePrimitivesPartial) {
  constexpr int64_t kSize = 6;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto a_values =
      arolla::CreateDenseArray<int64_t>(std::vector<OptionalValue<int64_t>>{
          17, std::nullopt, 57, 33, std::nullopt, 21});
  auto alloc_id = ds.allocation_ids().ids()[0];
  ASSERT_OK_AND_ASSIGN(
      auto dense_source,
      DenseSource::CreateReadonly(alloc_id, DataSliceImpl::Create(a_values)));
  auto sparse_source = SparseSource(alloc_id);
  sparse_source.Set(alloc_id.ObjectByOffset(2), DataItem());
  sparse_source.Set(alloc_id.ObjectByOffset(3), DataItem(int64_t{99}));

  auto ds_f = DataSliceImpl::ObjectsFromAllocation(alloc_id, kSize);

  ASSERT_OK_AND_ASSIGN(
      DataSliceImpl ds_a_get,
      GetAttributeFromSources(ds_f, {dense_source.get()}, {&sparse_source}));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(
      ds_a_get.values<int64_t>(),
      ElementsAre(17, std::nullopt, std::nullopt, 99, std::nullopt, 21));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_DenseTwoSparseSourcesPrimitivesPartial) {
  constexpr int64_t kSize = 6;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto a_values =
      arolla::CreateDenseArray<int64_t>(std::vector<OptionalValue<int64_t>>{
          17, std::nullopt, 57, 33, std::nullopt, 21});
  auto alloc_id = ds.allocation_ids().ids()[0];
  ASSERT_OK_AND_ASSIGN(
      auto dense_source,
      DenseSource::CreateReadonly(alloc_id, DataSliceImpl::Create(a_values)));
  auto sparse_source_old = SparseSource(alloc_id);
  sparse_source_old.Set(alloc_id.ObjectByOffset(0), DataItem(int64_t{19}));
  sparse_source_old.Set(alloc_id.ObjectByOffset(2), DataItem());
  sparse_source_old.Set(alloc_id.ObjectByOffset(3), DataItem(int64_t{99}));
  auto sparse_source_fresh = SparseSource(alloc_id);
  sparse_source_fresh.Set(alloc_id.ObjectByOffset(0), DataItem(int64_t{37}));
  sparse_source_fresh.Set(alloc_id.ObjectByOffset(2), DataItem(int64_t{47}));

  auto ds_f = DataSliceImpl::ObjectsFromAllocation(alloc_id, kSize);

  ASSERT_OK_AND_ASSIGN(
      DataSliceImpl ds_a_get,
      GetAttributeFromSources(ds_f, {dense_source.get()},
                              {&sparse_source_fresh, &sparse_source_old}));

  EXPECT_EQ(ds_a_get.size(), 6);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(),
              ElementsAre(37, std::nullopt, 47, 99, std::nullopt, 21));
}

TEST(DataSliceAccessorsTest,
     GetAttributeFromSources_TwoSparseSourcesPrimitivesPartial) {
  constexpr int64_t kSize = 6;
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_EQ(ds.allocation_ids().size(), 1);
  auto alloc_id = ds.allocation_ids().ids()[0];
  auto sparse_source_old = SparseSource(alloc_id);
  sparse_source_old.Set(alloc_id.ObjectByOffset(0), DataItem(int64_t{19}));
  sparse_source_old.Set(alloc_id.ObjectByOffset(2), DataItem());
  sparse_source_old.Set(alloc_id.ObjectByOffset(3), DataItem(int64_t{99}));
  sparse_source_old.Set(alloc_id.ObjectByOffset(4), DataItem(int64_t{21}));
  auto sparse_source_fresh = SparseSource(alloc_id);
  sparse_source_fresh.Set(alloc_id.ObjectByOffset(0), DataItem(int64_t{37}));
  sparse_source_fresh.Set(alloc_id.ObjectByOffset(2), DataItem(int64_t{47}));
  sparse_source_fresh.Set(alloc_id.ObjectByOffset(3), DataItem());
  sparse_source_old.Set(alloc_id.ObjectByOffset(5), DataItem(int64_t{25}));

  auto ds_f = DataSliceImpl::ObjectsFromAllocation(alloc_id, kSize);

  ASSERT_OK_AND_ASSIGN(
      DataSliceImpl ds_a_get,
      GetAttributeFromSources(ds_f, {},
                              {&sparse_source_fresh, &sparse_source_old}));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(),
              ElementsAre(37, std::nullopt, 47, std::nullopt, 21, 25));
}

}  // namespace
}  // namespace koladata::internal
