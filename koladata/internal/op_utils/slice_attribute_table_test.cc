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
#include "koladata/internal/op_utils/slice_attribute_table.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;
using ::arolla::kUnit;
using ::arolla::Text;
using ::arolla::Unit;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(SliceAttributeTableTest, EmptySliceWithAttributes) {
  SliceAttributeTable::Builder builder(/*slice_size=*/0, /*num_attrs=*/2);
  builder.Add("a", DataSliceImpl::CreateEmptyAndUnknownType(0));
  builder.Add("b", DataSliceImpl::CreateEmptyAndUnknownType(0),
              DataSliceImpl::CreateEmptyAndUnknownType(0));
  SliceAttributeTable attrs = std::move(builder).Build();
  (void)attrs;
}

TEST(SliceAttributeTableTest, EmptyAttributes) {
  SliceAttributeTable::Builder builder(/*slice_size=*/2, /*num_attrs=*/0);
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), IsEmpty());
  EXPECT_THAT(attrs.ValuesOf(0), IsEmpty());
  EXPECT_THAT(attrs.NamesOf(1), IsEmpty());
  EXPECT_THAT(attrs.ValuesOf(1), IsEmpty());
}

TEST(SliceAttributeTableTest, SameAttrNames) {
  SliceAttributeTable::Builder builder(/*slice_size=*/2, /*num_attrs=*/2);
  builder.Add("a", DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2})));
  builder.Add("b", DataSliceImpl::Create(
                       CreateDenseArray<float>({std::nullopt, 3.5f})));
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), ElementsAre("a", "b"));
  EXPECT_THAT(attrs.ValuesOf(0), ElementsAre(DataItem(1), DataItem()));
  EXPECT_THAT(attrs.NamesOf(1), ElementsAre("a", "b"));
  EXPECT_THAT(attrs.ValuesOf(1), ElementsAre(DataItem(2), DataItem(3.5f)));
  // The underlying names buffer is shared when all items have the same
  // attribute names.
  EXPECT_EQ(attrs.NamesOf(0).data(), attrs.NamesOf(1).data());
}

TEST(SliceAttributeTableTest, DifferentAttrNames) {
  SliceAttributeTable::Builder builder(/*slice_size=*/2, /*num_attrs=*/3);
  builder.Add(
      "a",
      DataSliceImpl::Create(CreateDenseArray<int32_t>({std::nullopt, 2})),
      DataSliceImpl::Create(CreateDenseArray<Unit>({kUnit, kUnit})));
  builder.Add(
      "b",
      DataSliceImpl::Create(CreateDenseArray<float>({std::nullopt, 3.5f})),
      DataSliceImpl::Create(CreateDenseArray<Unit>({std::nullopt, kUnit})));
  builder.Add(
      "unused",
      DataSliceImpl::CreateEmptyAndUnknownType(2),
      DataSliceImpl::CreateEmptyAndUnknownType(2));
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), ElementsAre("a"));
  EXPECT_THAT(attrs.ValuesOf(0), ElementsAre(DataItem()));

  EXPECT_THAT(attrs.NamesOf(1), ElementsAre("a", "b"));
  EXPECT_THAT(attrs.ValuesOf(1), ElementsAre(DataItem(2), DataItem(3.5f)));
}

TEST(SliceAttributeTableTest, MixedDTypeValuesWithSameAttrNames) {
  SliceBuilder mixed_builder(2);
  mixed_builder.InsertIfNotSetAndUpdateAllocIds(0, DataItem(1));
  mixed_builder.InsertIfNotSetAndUpdateAllocIds(1, DataItem(Text("two")));
  DataSliceImpl mixed_values = std::move(mixed_builder).Build();

  SliceAttributeTable::Builder builder(/*slice_size=*/2, /*num_attrs=*/1);
  builder.Add("a", std::move(mixed_values));
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), ElementsAre("a"));
  EXPECT_THAT(attrs.ValuesOf(0), ElementsAre(DataItem(1)));
  EXPECT_THAT(attrs.NamesOf(1), ElementsAre("a"));
  EXPECT_THAT(attrs.ValuesOf(1), ElementsAre(DataItem(Text("two"))));
}

TEST(SliceAttributeTableTest, MixedDTypeValuesWithDifferentAttrNames) {
  SliceBuilder mixed_builder(2);
  mixed_builder.InsertIfNotSetAndUpdateAllocIds(0, DataItem(1));
  mixed_builder.InsertIfNotSetAndUpdateAllocIds(1, DataItem(Text("two")));
  DataSliceImpl mixed_values = std::move(mixed_builder).Build();

  SliceAttributeTable::Builder builder(/*slice_size=*/2, /*num_attrs=*/2);
  builder.Add("a", std::move(mixed_values));
  builder.Add(
      "b", DataSliceImpl::Create(CreateDenseArray<int32_t>({10, 20})),
      DataSliceImpl::Create(CreateDenseArray<Unit>({kUnit, std::nullopt})));
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), ElementsAre("a", "b"));
  EXPECT_THAT(attrs.ValuesOf(0), ElementsAre(DataItem(1), DataItem(10)));
  EXPECT_THAT(attrs.NamesOf(1), ElementsAre("a"));
  EXPECT_THAT(attrs.ValuesOf(1), ElementsAre(DataItem(Text("two"))));
}

TEST(SliceAttributeTableTest, MixedDTypeHasAttr) {
  // Insert index 2 (int32) before index 0 (Text) so `VisitValues` visits
  // index 2 before index 0.
  SliceBuilder has_a_builder(3);
  has_a_builder.InsertIfNotSetAndUpdateAllocIds(2, DataItem(1));
  has_a_builder.InsertIfNotSetAndUpdateAllocIds(0, DataItem(Text("def")));
  DataSliceImpl has_a = std::move(has_a_builder).Build();

  SliceBuilder has_b_builder(3);
  has_b_builder.InsertIfNotSetAndUpdateAllocIds(0, DataItem(1));
  has_b_builder.InsertIfNotSetAndUpdateAllocIds(1, DataItem(Text("def")));
  DataSliceImpl has_b = std::move(has_b_builder).Build();

  SliceAttributeTable::Builder builder(/*slice_size=*/3, /*num_attrs=*/2);
  builder.Add("a",
              DataSliceImpl::Create(CreateDenseArray<int32_t>({10, 20, 30})),
              std::move(has_a));
  builder.Add("b",
              DataSliceImpl::Create(CreateDenseArray<int32_t>({40, 50, 60})),
              std::move(has_b));
  SliceAttributeTable attrs = std::move(builder).Build();

  EXPECT_THAT(attrs.NamesOf(0), ElementsAre("a", "b"));
  EXPECT_THAT(attrs.ValuesOf(0), ElementsAre(DataItem(10), DataItem(40)));
  EXPECT_THAT(attrs.NamesOf(1), ElementsAre("b"));
  EXPECT_THAT(attrs.ValuesOf(1), ElementsAre(DataItem(50)));
  EXPECT_THAT(attrs.NamesOf(2), ElementsAre("a"));
  EXPECT_THAT(attrs.ValuesOf(2), ElementsAre(DataItem(30)));
}

}  // namespace
}  // namespace koladata::internal
