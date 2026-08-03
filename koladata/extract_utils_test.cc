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
#include "koladata/extract_utils.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata {
namespace {

TEST(ExtractionUtilsTest, ExtractWholeKeepBagTheSame) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSlice::Create(internal::DataItem(),
                                         internal::DataItem(schema::kInt32), db,
                                         DataSlice::Wholeness::kWhole));
  EXPECT_TRUE(ds.IsWhole());
  {
    ASSERT_OK_AND_ASSIGN(auto ds_extracted,
                         extract_utils_internal::Extract(ds));
    EXPECT_EQ(ds.GetBag().get(), ds_extracted.GetBag().get());
    EXPECT_TRUE(ds_extracted.IsWhole());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        auto ds_extracted,
        extract_utils_internal::ExtractWithSchema(ds, ds.GetSchema()));
    EXPECT_EQ(ds.GetBag().get(), ds_extracted.GetBag().get());
    EXPECT_TRUE(ds_extracted.IsWhole());
  }
}

TEST(ExtractionUtilsTest, ExtractNonWholeMarksWhole) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSlice::Create(internal::DataItem(),
                                         internal::DataItem(schema::kInt32), db,
                                         DataSlice::Wholeness::kNotWhole));
  EXPECT_FALSE(ds.IsWhole());
  ASSERT_OK_AND_ASSIGN(auto ds_extracted, extract_utils_internal::Extract(ds));
  EXPECT_NE(ds.GetBag().get(), ds_extracted.GetBag().get());
  EXPECT_TRUE(ds_extracted.IsWhole());
}

TEST(ExtractionUtilsTest, ExtractNoBagEmpty) {
  // Primitive schema
  {
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::Create(internal::DataItem(),
                                   internal::DataItem(schema::kInt32), nullptr,
                                   DataSlice::Wholeness::kWhole));
    ASSERT_OK_AND_ASSIGN(auto ds_extracted,
                         extract_utils_internal::Extract(ds));
    EXPECT_NE(ds_extracted.GetBag(), nullptr);
    EXPECT_TRUE(ds_extracted.IsEmpty());
    EXPECT_EQ(ds_extracted.GetSchemaImpl(), schema::kInt32);
  }
  // Entity schema with bag
  {
    auto db = DataBag::EmptyMutable();
    auto schema_item = internal::AllocateExplicitSchema();
    ASSERT_OK_AND_ASSIGN(auto& db_impl, db->GetMutableImpl());
    ASSERT_OK(db_impl.SetSchemaAttr(internal::DataItem(schema_item), "a",
                                    internal::DataItem(schema::kInt32)));

    ASSERT_OK_AND_ASSIGN(
        auto ds,
        DataSlice::Create(internal::DataItem(), internal::DataItem(schema_item),
                          nullptr, DataSlice::Wholeness::kWhole));

    ASSERT_OK_AND_ASSIGN(auto schema_ds,
                         DataSlice::Create(internal::DataItem(schema_item),
                                           internal::DataItem(schema::kSchema),
                                           db, DataSlice::Wholeness::kWhole));

    ASSERT_OK_AND_ASSIGN(
        auto ds_extracted,
        extract_utils_internal::ExtractWithSchema(ds, schema_ds));
    EXPECT_NE(ds_extracted.GetBag(), nullptr);
    EXPECT_TRUE(ds_extracted.IsEmpty());
    EXPECT_EQ(ds_extracted.GetSchemaImpl(), internal::DataItem(schema_item));

    // Check schema adoption
    auto schema_extracted = ds_extracted.GetSchema();
    EXPECT_NE(schema_extracted.GetBag(), nullptr);
    ASSERT_OK_AND_ASSIGN(auto attr_val, schema_extracted.GetAttr("a"));
    EXPECT_EQ(attr_val.item(), internal::DataItem(schema::kInt32));
  }
}

}  // namespace
}  // namespace koladata
