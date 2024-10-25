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
#include "koladata/internal/slice_builder.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

TEST(SliceBuilderTest, TypesBuffer) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.types.push_back(ScalarTypeId<float>());
  b.id_to_typeidx = arolla::CreateBuffer<uint8_t>(
      {1, TypesBuffer::kUnset, 0, TypesBuffer::kRemoved, 1});

  EXPECT_EQ(b.size(), 5);
  EXPECT_EQ(b.type_count(), 2);
  EXPECT_EQ(b.id_to_type(0), arolla::GetQType<float>());
  EXPECT_EQ(b.id_to_type(1), nullptr);
  EXPECT_EQ(b.id_to_type(2), arolla::GetQType<int>());
  EXPECT_EQ(b.id_to_type(3), nullptr);
  EXPECT_EQ(b.id_to_type(4), arolla::GetQType<float>());

  EXPECT_THAT(b.ToBitmap(0), ElementsAre(0b00100));
  EXPECT_THAT(b.ToBitmap(1), ElementsAre(0b10001));
  EXPECT_THAT(b.ToBitmap(TypesBuffer::kRemoved), ElementsAre(0b01000));
  EXPECT_THAT(b.ToPresenceBitmap(), ElementsAre(0b10101));
}

TEST(SliceBuilderTest, Empty) {
  DataSliceImpl ds = SliceBuilder(3).Build();
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(), DataItem()));
}

TEST(SliceBuilderTest, SingleValue) {
  SliceBuilder bldr(3);
  bldr.InsertIfNotSet(1, 5);
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(5), DataItem()));
}

TEST(SliceBuilderTest, IsFinalized) {
  SliceBuilder bldr(3);
  EXPECT_FALSE(bldr.is_finalized());
  bldr.typed<float>().InsertIfNotSet(1, 1.f);
  bldr.InsertIfNotSet(0, 2);
  bldr.typed<float>().InsertIfNotSet(0, 0.f);  // ignored as it is already set
  EXPECT_FALSE(bldr.is_finalized());
  EXPECT_TRUE(bldr.IsSet(0));
  EXPECT_TRUE(bldr.IsSet(1));
  EXPECT_FALSE(bldr.IsSet(2));

  bldr.InsertIfNotSet(2, 3);
  EXPECT_TRUE(bldr.IsSet(2));
  EXPECT_TRUE(bldr.is_finalized());

  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_THAT(ds, ElementsAre(DataItem(2), DataItem(1.0f), DataItem(3)));
}

TEST(SliceBuilderTest, ScalarInsertIfNotSet) {
  SliceBuilder bldr(10);
  bldr.InsertIfNotSet(1, 5);
  bldr.InsertIfNotSet(3, DataItem(5));
  bldr.InsertIfNotSet(9, arolla::Bytes("bytes"));
  bldr.InsertIfNotSet(8, DataItem::View<arolla::Bytes>("bytes_view"));
  bldr.InsertIfNotSet(1, 4);  // ignored as it was already set
  bldr.InsertIfNotSet(2, std::nullopt);
  bldr.InsertIfNotSet(4, DataItem());
  auto typed = bldr.typed<float>();
  typed.InsertIfNotSet(2, 3.0f);  // ignored as it was already set to missing
  typed.InsertIfNotSet(6, 3.0f);

  EXPECT_THAT(bldr.types_buffer().ToBitmap(TypesBuffer::kUnset),
              ElementsAre(0b0010100001));
  EXPECT_THAT(bldr.types_buffer().ToBitmap(TypesBuffer::kRemoved),
              ElementsAre(0b0000010100));
  EXPECT_FALSE(bldr.is_finalized());

  DataSliceImpl ds = std::move(bldr).Build();

  EXPECT_EQ(ds.size(), 10);
  EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(5), DataItem(), DataItem(5),
                              DataItem(), DataItem(), DataItem(3.0f),
                              DataItem(), DataItem(arolla::Bytes("bytes_view")),
                              DataItem(arolla::Bytes("bytes"))));
}

TEST(SliceBuilderTest, SingleType) {
  constexpr int kSize = 100;
  SliceBuilder bldr(kSize);
  for (int i = 0; i < kSize; i += 2) {
    bldr.InsertIfNotSet(i, i);
  }
  for (int i = 0; i < kSize; i += 3) {
    bldr.InsertIfNotSet(i, -i);
  }
  EXPECT_FALSE(bldr.is_finalized());
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int32_t>());
  const arolla::DenseArray<int>& vals = ds.values<int>();
  for (int i = 0; i < kSize; ++i) {
    if (i % 2 == 0) {
      EXPECT_EQ(vals[i], i);
    } else if (i % 3 == 0) {
      EXPECT_EQ(vals[i], -i);
    } else {
      EXPECT_FALSE(vals.present(i));
    }
  }
}

TEST(SliceBuilderTest, AllocationIds) {
  AllocationId alloc_id = Allocate(50);
  for (int type_count = 1; type_count <= 2; ++type_count) {
    SliceBuilder bldr(3);
    bldr.GetMutableAllocationIds().Insert(alloc_id);
    bldr.InsertIfNotSet(0, alloc_id.ObjectByOffset(0));
    if (type_count > 1) {
      // Add an int to have different code path in SliceBuilder::Build.
      bldr.InsertIfNotSet(1, 5);
    }
    DataSliceImpl res = std::move(bldr).Build();
    EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(res.allocation_ids().ids(), ElementsAre(alloc_id));
  }
}

TEST(SliceBuilderTest, ApplyMask) {
  SliceBuilder bldr(5);
  bldr.InsertIfNotSet(0, 5);  // set before ApplyMask, so it will remain
  bldr.InsertIfNotSet(1, 7.5f);
  bldr.ApplyMask(arolla::CreateDenseArray<arolla::Unit>(
      {{}, arolla::kUnit, {}, arolla::kUnit, {}}));
  EXPECT_FALSE(bldr.is_finalized());
  bldr.InsertIfNotSet(0, 7.5f);
  bldr.InsertIfNotSet(1, 5);
  bldr.InsertIfNotSet(1, std::nullopt);
  EXPECT_FALSE(bldr.is_finalized());
  bldr.InsertIfNotSet(2, 41);  // removed by the mask
  bldr.InsertIfNotSet(3, 3);
  bldr.InsertIfNotSet(3, std::nullopt);
  EXPECT_TRUE(bldr.is_finalized());
  EXPECT_THAT(std::move(bldr).Build(),
              ElementsAre(DataItem(5), DataItem(7.5f), DataItem(), DataItem(3),
                          DataItem()));
}

// TODO: Test batch version of InsertIfNotSet.

}  // namespace
}  // namespace koladata::internal
