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
        extract_utils_internal::ExtractWithSchema(ds, ds.GetSchema(), false));
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

}  // namespace
}  // namespace koladata
