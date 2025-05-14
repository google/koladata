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
#include "koladata/internal/sparse_source.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

using ::arolla::bitmap::Word;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

TEST(SparseSourceTest, MutableObjectAttrSimple) {
  AllocationId alloc = Allocate(3);
  AllocationId attr_alloc = Allocate(3);
  auto ds = std::make_shared<SparseSource>(alloc);
  ASSERT_OK(ds->Set(
      arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0),
                                              alloc.ObjectByOffset(1),
                                              alloc.ObjectByOffset(2)}),
      DataSliceImpl::CreateWithAllocIds(
          AllocationIdSet(attr_alloc),
          arolla::CreateFullDenseArray<ObjectId>(
              {attr_alloc.ObjectByOffset(0), attr_alloc.ObjectByOffset(1),
               attr_alloc.ObjectByOffset(2)}))));

  {
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(ds->Get(alloc.ObjectByOffset(i)),
                DataItem(attr_alloc.ObjectByOffset(i)));
    }
    // object outside of alloc
    EXPECT_EQ(ds->Get(attr_alloc.ObjectByOffset(0)), std::nullopt);

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(0)});

    EXPECT_THAT(ds->Get(objs).values<ObjectId>(),
                ElementsAre(attr_alloc.ObjectByOffset(0), std::nullopt,
                            attr_alloc.ObjectByOffset(2),
                            attr_alloc.ObjectByOffset(0)));
  }

  AllocationId attr_alloc2 = Allocate(3);

  // Reassign 0 and remove 1.
  ASSERT_OK(ds->Set(arolla::CreateFullDenseArray<ObjectId>(
                        {alloc.ObjectByOffset(0), alloc.ObjectByOffset(1)}),
                    DataSliceImpl::CreateWithAllocIds(
                        AllocationIdSet(attr_alloc2),
                        arolla::CreateDenseArray<ObjectId>(
                            {attr_alloc2.ObjectByOffset(1), std::nullopt}))));
  // Reassign 0 again, 1 should remain removed.
  ASSERT_OK(ds->Set(
      arolla::CreateFullDenseArray<ObjectId>({alloc.ObjectByOffset(0)}),
      DataSliceImpl::CreateWithAllocIds(AllocationIdSet(attr_alloc2),
                                        arolla::CreateFullDenseArray<ObjectId>(
                                            {attr_alloc2.ObjectByOffset(0)}))));

  {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)),
              DataItem(attr_alloc2.ObjectByOffset(0)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)), DataItem());
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)),
              DataItem(attr_alloc.ObjectByOffset(2)));

    // object outside of alloc
    EXPECT_EQ(ds->Get(attr_alloc.ObjectByOffset(0)), std::nullopt);

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, attr_alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(1)});

    SliceBuilder slice_bldr(objs.size());
    slice_bldr.ApplyMask(objs.ToMask());
    ds->Get(objs.values.span(), slice_bldr);
    EXPECT_THAT(slice_bldr.types_buffer().ToBitmap(TypesBuffer::kUnset),
                ElementsAre(0b0100));
    EXPECT_THAT(std::move(slice_bldr).Build().values<ObjectId>(),
                ElementsAre(attr_alloc2.ObjectByOffset(0), std::nullopt,
                            std::nullopt, std::nullopt));
  }

  // Reassign previously removed 1.
  ds->Set(alloc.ObjectByOffset(1),
                    DataItem(attr_alloc2.ObjectByOffset(1)));
  // Remove 2.
  ds->Set(alloc.ObjectByOffset(2), DataItem());

  {
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(0)),
              DataItem(attr_alloc2.ObjectByOffset(0)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(1)),
              DataItem(attr_alloc2.ObjectByOffset(1)));
    EXPECT_EQ(ds->Get(alloc.ObjectByOffset(2)), DataItem());

    // object outside of alloc
    EXPECT_EQ(ds->Get(attr_alloc.ObjectByOffset(0)), std::nullopt);

    auto objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{
            alloc.ObjectByOffset(0), std::nullopt, alloc.ObjectByOffset(2),
            alloc.ObjectByOffset(1)});

    EXPECT_THAT(ds->Get(objs).values<ObjectId>(),
                ElementsAre(attr_alloc2.ObjectByOffset(0), std::nullopt,
                            std::nullopt, attr_alloc2.ObjectByOffset(1)));
  }
}

TEST(SparseSourceTest, SetGet) {
  auto ds = std::make_shared<SparseSource>();
  auto object_id1 = AllocateSingleObject();
  auto object_id2 = AllocateSingleObject();
  auto object_id_val2 = Allocate(1027).ObjectByOffset(77);

  for (DataItem value :
       {DataItem(object_id1), DataItem(object_id_val2), DataItem(57),
        DataItem(57.0), DataItem(arolla::Bytes("14"))}) {
    ds->Set(object_id1, value);
    EXPECT_EQ(ds->Get(object_id1), value);
    EXPECT_EQ(ds->Get(object_id2), std::nullopt);
  }

  auto object_id_val3 = Allocate(5027).ObjectByOffset(977);
  auto object_id3 = AllocateSingleObject();
  auto object_id_ignored = Allocate(15027).ObjectByOffset(999);
  ds->Set(object_id3, DataItem());
  ds->Set(object_id1, DataItem(object_id_val3));
  ds->Set(object_id2, DataItem(object_id1));
  ds->Set(object_id_ignored, DataItem(object_id1));

  auto objs = arolla::CreateDenseArray<ObjectId>(
      std::vector<arolla::OptionalValue<ObjectId>>{
          object_id1, std::nullopt, object_id2, object_id3, object_id_ignored});

  std::vector<arolla::OptionalValue<ObjectId>> expected_objs = {
      object_id_val3, std::nullopt, object_id1, std::nullopt, std::nullopt};
  {
    auto data_slice = ds->Get(objs);
    ASSERT_EQ(data_slice.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(data_slice.values<ObjectId>(),
                ElementsAreArray(expected_objs));
    EXPECT_EQ(data_slice.allocation_ids(),
              AllocationIdSet(
                  {AllocationId(object_id1), AllocationId(object_id_val3)}));
  }

  {
    SliceBuilder slice_bldr(objs.size());
    ds->Get(objs.values.span(), slice_bldr);
    EXPECT_THAT(slice_bldr.types_buffer().ToBitmap(TypesBuffer::kUnset),
                ElementsAre(0b10010));
    EXPECT_THAT(std::move(slice_bldr).Build().values<ObjectId>(),
                ElementsAreArray(expected_objs));
  }

  // mixed types
  ds->Set(object_id1, DataItem(57));
  {
    auto data_slice = ds->Get(objs);
    ASSERT_EQ(data_slice.dtype(), arolla::GetNothingQType());
    ASSERT_TRUE(data_slice.is_mixed_dtype());
    EXPECT_EQ(data_slice.allocation_ids(),
              AllocationIdSet({AllocationId(object_id1)}));
    EXPECT_EQ(data_slice[0], DataItem(57));
    EXPECT_EQ(data_slice[2], DataItem(object_id1));
  }

  {
    auto set_objs = arolla::CreateDenseArray<ObjectId>(
        std::vector<arolla::OptionalValue<ObjectId>>{object_id1, std::nullopt,
                                                     object_id2, object_id3,
                                                     object_id_ignored});
    ASSERT_OK(ds->Set(
        set_objs, DataSliceImpl::Create(
                      arolla::CreateDenseArray<ObjectId>(
                          std::vector<arolla::OptionalValue<ObjectId>>{
                              object_id_val3, std::nullopt, std::nullopt,
                              std::nullopt, object_id_val2}),
                      arolla::CreateDenseArray<float>(
                          std::vector<arolla::OptionalValue<float>>{
                              std::nullopt, std::nullopt, std::nullopt, 75.0f,
                              std::nullopt}))));

    auto data_slice = ds->Get(objs);
    ASSERT_EQ(data_slice.dtype(), arolla::GetNothingQType());
    ASSERT_TRUE(data_slice.is_mixed_dtype());
    EXPECT_EQ(data_slice.allocation_ids(),
              AllocationIdSet({AllocationId(object_id_val3)}));
    EXPECT_THAT(data_slice, ElementsAre(object_id_val3, std::nullopt,
                                        std::nullopt, 75.0f, std::nullopt));
  }
}

TEST(SparseSourceTest, SetUnitAttrAndReturnMissingObjectsInternal) {
  auto oth0 = AllocateSingleObject();
  auto oth1 = AllocateSingleObject();
  auto oth2 = Allocate(1027).ObjectByOffset(77);

  AllocationId alloc = Allocate(4);
  auto ds = std::make_shared<SparseSource>(alloc);
  auto a0 = alloc.ObjectByOffset(0);
  auto a1 = alloc.ObjectByOffset(1);
  auto a2 = alloc.ObjectByOffset(2);
  auto a3 = alloc.ObjectByOffset(3);

  std::vector<ObjectId> missing_objects;
  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>({}), missing_objects));
  EXPECT_TRUE(missing_objects.empty());

  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>(
          {a0, a2, a2, std::nullopt, std::nullopt, a0, oth0}),
      missing_objects));
  EXPECT_THAT(missing_objects, ElementsAre(a0, a2));

  missing_objects.clear();
  EXPECT_OK(ds->SetUnitAndUpdateMissingObjects(
      arolla::CreateDenseArray<ObjectId>({oth0, oth1, oth2, a3, a2, a1, a0}),
      missing_objects));
  EXPECT_THAT(missing_objects, ElementsAre(a3, a1));
}

}  // namespace
}  // namespace koladata::internal
