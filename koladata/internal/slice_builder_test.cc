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
#include <initializer_list>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::arolla::kUnit;

arolla::bitmap::Bitmap CreateBitmap(
    std::initializer_list<arolla::OptionalUnit> data) {
  return arolla::CreateDenseArray<arolla::Unit>(data).bitmap;
}

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

TEST(SliceBuilderTest, ToBitmap64Elements) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.id_to_typeidx = arolla::CreateBuffer<uint8_t>(std::vector<uint8_t>(64, 0));
  EXPECT_THAT(b.ToBitmap(0),
              ElementsAre(~arolla::bitmap::Word{0}, ~arolla::bitmap::Word{0}));
}

TEST(SliceBuilderTest, ToBitmap40Elements) {
  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.types.push_back(ScalarTypeId<float>());
  std::vector<uint8_t> types(40, 0);
  for (int i = 32; i < 40; ++i) types[i] = 1;
  b.id_to_typeidx = arolla::CreateBuffer<uint8_t>(types);
  EXPECT_THAT(b.ToBitmap(0),
              ElementsAre(~arolla::bitmap::Word{0}, arolla::bitmap::Word{0}));
  EXPECT_THAT(b.ToBitmap(1),
              ElementsAre(arolla::bitmap::Word{0}, 0b11111111));
}

TEST(SliceBuilderTest, ToBitmapVarSize) {
  for (int size = 0; size < 100; ++size) {
    TypesBuffer b;
    b.types.push_back(ScalarTypeId<int>());
    b.types.push_back(ScalarTypeId<float>());
    std::vector<uint8_t> types(size);
    for (int i = 0; i < size; ++i) {
      types[i] = i % 2;
    }
    b.id_to_typeidx = arolla::CreateBuffer<uint8_t>(types);
    {
      std::vector<arolla::bitmap::Word> expected0(
          arolla::bitmap::BitmapSize(size));
      for (int i = 0; i < size; i += 2) {
        arolla::SetBit(expected0.data(), i);
      }
      EXPECT_THAT(b.ToBitmap(0), ElementsAreArray(expected0));
    }
    {
      std::vector<arolla::bitmap::Word> expected1(
          arolla::bitmap::BitmapSize(size));
      for (int i = 1; i < size; i += 2) {
        arolla::SetBit(expected1.data(), i);
      }
      EXPECT_THAT(b.ToBitmap(1), ElementsAreArray(expected1));
    }
  }
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
  {
    AllocationId alloc_id = Allocate(50);
    for (int type_count = 1; type_count <= 2; ++type_count) {
      SliceBuilder bldr(3);
      bldr.GetMutableAllocationIds().Insert(alloc_id);
      bldr.InsertIfNotSet(0, alloc_id.ObjectByOffset(0));
      if (type_count > 1) {
        // Add an int to have different code path in SliceBuilder::Build.
        bldr.InsertIfNotSetAndUpdateAllocIds(1, DataItem(5));
      }
      DataSliceImpl res = std::move(bldr).Build();
      EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
      EXPECT_THAT(res.allocation_ids().ids(), ElementsAre(alloc_id));
    }
  }
  {
    ObjectId obj_id = AllocateSingleObject();
    SliceBuilder bldr(3);
    bldr.InsertIfNotSetAndUpdateAllocIds(2, DataItem(obj_id));
    DataSliceImpl res = std::move(bldr).Build();
    EXPECT_TRUE(res.allocation_ids().contains_small_allocation_id());
  }
}

TEST(SliceBuilderTest, ApplyMask) {
  SliceBuilder bldr(5);
  bldr.InsertIfNotSet(0, 5);  // set before ApplyMask, so it will remain
  bldr.InsertIfNotSet(1, 7.5f);
  bldr.ApplyMask(
      arolla::CreateDenseArray<arolla::Unit>({{}, kUnit, {}, kUnit, {}}));
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

TEST(SliceBuilderTest, EmptyArraysViaTypedT) {
  constexpr int kSize = 100;
  SliceBuilder bldr(kSize);
  bldr.typed<int>();
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
}

TEST(SliceBuilderTest, EmptyArraysViaTypedT2Types) {
  constexpr int kSize = 100;
  SliceBuilder bldr(kSize);
  bldr.typed<int>();
  bldr.typed<float>();
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
}

TEST(SliceBuilderTest, EmptyArraysOnTopOfNonEmptyArrayViaTypedT) {
  constexpr int kSize = 3;
  SliceBuilder bldr(kSize);
  bldr.typed<int>();
  bldr.typed<float>();
  bldr.InsertIfNotSet(1, arolla::Text("ok"));
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_single_dtype());
  EXPECT_EQ(ds.dtype(), arolla::GetQType<arolla::Text>());
  EXPECT_THAT(
      ds, ElementsAre(DataItem(), DataItem(arolla::Text("ok")), DataItem()));
  ASSERT_OK(ds.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
    if constexpr (std::is_same_v<T, int> || std::is_same_v<T, float>) {
      return absl::InternalError(absl::StrCat("get an unexpected text array: ",
                                              DataSliceImpl::Create(array)));
    }
    return absl::OkStatus();
  }));
}

TEST(SliceBuilderTest, EmptyArraysOnTopOfNonEmpty2ArrayaViaTypedT) {
  constexpr int kSize = 3;
  SliceBuilder bldr(kSize);
  bldr.typed<int>();
  bldr.typed<float>();
  bldr.InsertIfNotSet(1, arolla::Text("ok"));
  bldr.InsertIfNotSet(2, int64_t{7});
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_mixed_dtype());
  EXPECT_EQ(ds.dtype(), arolla::GetNothingQType());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(arolla::Text("ok")),
                              DataItem(int64_t{7})));
  ASSERT_OK(ds.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
    if constexpr (std::is_same_v<T, int> || std::is_same_v<T, float>) {
      return absl::InternalError(absl::StrCat("get an unexpected text array: ",
                                              DataSliceImpl::Create(array)));
    }
    return absl::OkStatus();
  }));
}

TEST(SliceBuilderTest, SingleBatchInsert) {
  auto buf = arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1});
  SliceBuilder bldr(6);
  bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                           CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                           buf);
  EXPECT_FALSE(bldr.IsSet(0));
  EXPECT_TRUE(bldr.IsSet(1));
  EXPECT_TRUE(bldr.IsSet(2));
  EXPECT_TRUE(bldr.IsSet(3));
  EXPECT_FALSE(bldr.IsSet(4));
  EXPECT_FALSE(bldr.IsSet(5));
  auto ds = std::move(bldr).Build();
  EXPECT_THAT(ds, ElementsAre(std::nullopt, 5, std::nullopt, 3, std::nullopt,
                              std::nullopt));
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int>());
  EXPECT_EQ(ds.values<int>().values.span().begin(), buf.span().begin());
}

TEST(SliceBuilderTest, TwoBatchInsertsDifferentTypes) {
  auto buf_int = arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1});
  auto buf_float =
      arolla::CreateBuffer<float>({1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f});
  SliceBuilder bldr(6);

  bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                           CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                           buf_int);
  bldr.InsertIfNotSet<float>(
      CreateBitmap({{}, {}, {}, kUnit, kUnit, kUnit}), arolla::bitmap::Bitmap(),
      buf_float);

  EXPECT_FALSE(bldr.is_finalized());
  bldr.InsertIfNotSet(0, arolla::Text("abc"));
  EXPECT_TRUE(bldr.is_finalized());

  auto ds = std::move(bldr).Build();
  EXPECT_THAT(ds,
              ElementsAre(arolla::Text("abc"), 5, std::nullopt, 3, 5.5f, 6.5f));

  ds.VisitValues([&]<typename T>(const arolla::DenseArray<T>& v) {
    if constexpr (std::is_same_v<T, int>) {
      EXPECT_EQ(v.values.span().begin(), buf_int.span().begin());
    }
    if constexpr (std::is_same_v<T, float>) {
      EXPECT_EQ(v.values.span().begin(), buf_float.span().begin());
    }
  });
}

TEST(SliceBuilderTest, TwoBatchInsertsSameType) {
  auto buf_int = arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1});
  auto buf_float =
      arolla::CreateBuffer<float>({1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f});
  SliceBuilder bldr(6);

  bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                           CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                           arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1}));
  bldr.InsertIfNotSet<int>(CreateBitmap({{}, {}, {}, kUnit, kUnit, kUnit}),
                           arolla::bitmap::Bitmap(),
                           arolla::CreateBuffer<int>({1, 2, 3, 4, 5, 6}));

  EXPECT_FALSE(bldr.is_finalized());
  bldr.InsertIfNotSet(0, arolla::Text("abc"));
  EXPECT_TRUE(bldr.is_finalized());

  auto ds = std::move(bldr).Build();
  EXPECT_THAT(ds,
              ElementsAre(arolla::Text("abc"), 5, std::nullopt, 3, 5, 6));
}

TEST(SliceBuilderTest, BatchInsertAndScalarInsertSameType) {
  auto buf = arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1});
  {  // insert batch, insert scalar
    SliceBuilder bldr(6);
    bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                             CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                             arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1}));
    bldr.InsertIfNotSet(0, 10);
    auto ds = std::move(bldr).Build();
    EXPECT_EQ(ds.dtype(), arolla::GetQType<int32_t>());
    EXPECT_THAT(
        ds, ElementsAre(10, 5, std::nullopt, 3, std::nullopt, std::nullopt));
  }
  {  // insert scalar, insert batch
    SliceBuilder bldr(6);
    bldr.InsertIfNotSet(0, 10);
    bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                             CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                             arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1}));
    auto ds = std::move(bldr).Build();
    EXPECT_EQ(ds.dtype(), arolla::GetQType<int32_t>());
    EXPECT_THAT(
        ds, ElementsAre(10, 5, std::nullopt, 3, std::nullopt, std::nullopt));
  }
}

TEST(SliceBuilderTest, BatchInsertAndScalarInsertDifferentTypes) {
  auto buf = arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1});
  {  // insert batch, insert scalar
    SliceBuilder bldr(6);
    bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                             CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                             arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1}));
    bldr.InsertIfNotSet(0, 3.14f);
    auto ds = std::move(bldr).Build();
    EXPECT_THAT(
        ds, ElementsAre(3.14f, 5, std::nullopt, 3, std::nullopt, std::nullopt));
  }
  {  // insert scalar, insert batch
    SliceBuilder bldr(6);
    bldr.InsertIfNotSet(0, 3.14f);
    bldr.InsertIfNotSet<int>(CreateBitmap({{}, kUnit, kUnit, kUnit, {}, {}}),
                             CreateBitmap({{}, kUnit, {}, kUnit, {}, kUnit}),
                             arolla::CreateBuffer<int>({6, 5, 4, 3, 2, 1}));
    auto ds = std::move(bldr).Build();
    EXPECT_THAT(
        ds, ElementsAre(3.14f, 5, std::nullopt, 3, std::nullopt, std::nullopt));
  }
}

}  // namespace
}  // namespace koladata::internal
