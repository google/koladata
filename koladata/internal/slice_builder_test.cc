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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types_buffer.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;
using ::arolla::kUnit;

arolla::bitmap::Bitmap CreateBitmap(
    std::initializer_list<arolla::OptionalUnit> data) {
  return arolla::CreateDenseArray<arolla::Unit>(data).bitmap;
}

TEST(SliceBuilderTest, Empty) {
  DataSliceImpl ds = SliceBuilder(3).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(), DataItem()));
  EXPECT_THAT(ds.types_buffer().id_to_typeidx,
              ElementsAre(TypesBuffer::kUnset, TypesBuffer::kUnset,
                          TypesBuffer::kUnset));
}

TEST(SliceBuilderTest, AllRemoved) {
  auto bldr = SliceBuilder(3);
  bldr.InsertIfNotSet(0, DataItem());
  bldr.InsertIfNotSet(1, DataItem());
  bldr.InsertIfNotSet(2, DataItem());
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(), DataItem()));
  EXPECT_THAT(ds.types_buffer().id_to_typeidx,
              ElementsAre(TypesBuffer::kRemoved, TypesBuffer::kRemoved,
                          TypesBuffer::kRemoved));
}

TEST(SliceBuilderTest, AllRemovedViaTypedBuilder) {
  auto bldr = SliceBuilder(3);
  auto typed = bldr.typed<int>();
  typed.InsertIfNotSet(0, std::nullopt);
  typed.InsertIfNotSet(1, std::nullopt);
  typed.InsertIfNotSet(2, std::nullopt);
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(), DataItem()));
  EXPECT_THAT(ds.types_buffer().id_to_typeidx,
              ElementsAre(TypesBuffer::kRemoved, TypesBuffer::kRemoved,
                          TypesBuffer::kRemoved));
}

TEST(SliceBuilderTest, EmptyWithRemoved) {
  auto bldr = SliceBuilder(3);
  bldr.InsertIfNotSet(0, DataItem());
  bldr.InsertIfNotSet(2, DataItem());
  DataSliceImpl ds = std::move(bldr).Build();
  EXPECT_TRUE(ds.is_empty_and_unknown());
  EXPECT_THAT(ds, ElementsAre(DataItem(), DataItem(), DataItem()));
  EXPECT_THAT(ds.types_buffer().id_to_typeidx,
              ElementsAre(TypesBuffer::kRemoved, TypesBuffer::kUnset,
                          TypesBuffer::kRemoved));
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

TEST(SliceBuilderTest, ScalarInsertGuaranteedNotSet) {
  SliceBuilder bldr(10);
  bldr.InsertGuaranteedNotSet(1, 5);
  bldr.InsertGuaranteedNotSet(3, DataItem(5));
  bldr.InsertGuaranteedNotSet(9, arolla::Bytes("bytes"));
  bldr.InsertGuaranteedNotSet(8, DataItem::View<arolla::Bytes>("bytes_view"));
  bldr.InsertGuaranteedNotSet(2, std::nullopt);
  bldr.InsertGuaranteedNotSet(4, DataItem());
  auto typed = bldr.typed<float>();
  typed.InsertGuaranteedNotSet(6, 3.0f);

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
  for (bool use_guaranteed_not_set : {true, false}) {
    SCOPED_TRACE(absl::StrCat("use_guaranteed_not_set: ",
                              use_guaranteed_not_set ? "true" : "false"));
    auto insert_fn = [&](SliceBuilder& bldr, int64_t id, const auto& v) {
      if (use_guaranteed_not_set) {
        bldr.InsertGuaranteedNotSet(id, v);
      } else {
        bldr.InsertIfNotSet(id, v);
      }
    };
    auto insert_and_update_alloc_ids_fn = [&](SliceBuilder& bldr, int64_t id,
                                              const auto& v) {
      if (use_guaranteed_not_set) {
        bldr.InsertGuaranteedNotSetAndUpdateAllocIds(id, v);
      } else {
        bldr.InsertIfNotSetAndUpdateAllocIds(id, v);
      }
    };
    {
      AllocationId alloc_id = Allocate(50);
      for (int type_count = 1; type_count <= 2; ++type_count) {
        SliceBuilder bldr(3);
        bldr.GetMutableAllocationIds().Insert(alloc_id);
        insert_fn(bldr, 0, alloc_id.ObjectByOffset(0));
        if (type_count > 1) {
          // Add an int to have different code path in SliceBuilder::Build.
          insert_and_update_alloc_ids_fn(bldr, 1, DataItem(5));
        }
        DataSliceImpl res = std::move(bldr).Build();
        EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
        EXPECT_THAT(res.allocation_ids().ids(), ElementsAre(alloc_id));
      }
    }
    {
      ObjectId obj_id = AllocateSingleObject();
      SliceBuilder bldr(3);
      insert_and_update_alloc_ids_fn(bldr, 2, DataItem(obj_id));
      DataSliceImpl res = std::move(bldr).Build();
      EXPECT_TRUE(res.allocation_ids().contains_small_allocation_id());
    }
  }
}

TEST(SliceBuilderTest, AllocationIdsIgnoredObject) {
  {
    ObjectId obj_id = Allocate(57).ObjectByOffset(0);
    SliceBuilder bldr(3);
    bldr.InsertIfNotSetAndUpdateAllocIds(2, DataItem());
    // ignored
    bldr.InsertIfNotSetAndUpdateAllocIds(2, DataItem(obj_id));
    DataSliceImpl res = std::move(bldr).Build();
    EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
    EXPECT_TRUE(res.allocation_ids().empty());
  }
  {
    ObjectId obj_id1 = Allocate(57).ObjectByOffset(0);
    ObjectId obj_id2 = Allocate(57).ObjectByOffset(0);
    SliceBuilder bldr(3);
    bldr.InsertIfNotSetAndUpdateAllocIds(2, DataItem(obj_id1));
    // ignored
    bldr.InsertIfNotSetAndUpdateAllocIds(2, DataItem(obj_id2));
    DataSliceImpl res = std::move(bldr).Build();
    EXPECT_FALSE(res.allocation_ids().contains_small_allocation_id());
    EXPECT_THAT(res.allocation_ids(), ElementsAre(AllocationId(obj_id1)));
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
