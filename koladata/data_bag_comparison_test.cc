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
#include "koladata/data_bag_comparison.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/data_bag.h"

namespace koladata {
namespace {

TEST(DataBagComparisonTest, ExactlyEqual_Empty) {
  auto db1 = DataBag::Empty();
  auto impl_db = internal::DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBag::FromImpl(impl_db);
  EXPECT_TRUE(DataBagComparison::ExactlyEqual(db1, db2));
}

TEST(DataBagComparisonTest, ExactlyEqual_NoFallbacks) {
  auto ds1 = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto ds2 = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto db1_impl = internal::DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db1_impl->SetAttr(ds1, "self", ds1));
  auto db2_impl = internal::DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2_impl->SetAttr(ds1, "self", ds1));
  auto db3_impl = internal::DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db3_impl->SetAttr(ds2, "self", ds2));
  auto db1 = DataBag::FromImpl(db1_impl);
  auto db2 = DataBag::FromImpl(db2_impl);
  auto db3 = DataBag::FromImpl(db3_impl);

  EXPECT_TRUE(DataBagComparison::ExactlyEqual(db1, db2));
  EXPECT_FALSE(DataBagComparison::ExactlyEqual(db1, db3));
}

TEST(DataBagComparisonTest, ExactlyEqual_Fallbacks) {
  auto ds1 = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto ds2 = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto db1_impl = internal::DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db1_impl->SetAttr(ds1, "other", ds2));
  auto db2_impl = internal::DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2_impl->SetAttr(ds2, "other", ds1));

  auto db1 = DataBag::FromImpl(db1_impl);
  auto db2 = DataBag::FromImpl(db2_impl);

  auto db_f1 = DataBag::ImmutableEmptyWithFallbacks({db1});
  auto db_f12 = DataBag::ImmutableEmptyWithFallbacks({db1, db2});
  auto db_f12_copy = DataBag::ImmutableEmptyWithFallbacks({db1, db2});
  auto db_f21 = DataBag::ImmutableEmptyWithFallbacks({db2, db1});
  auto db_ff12 = DataBag::ImmutableEmptyWithFallbacks({db_f1, db2});
  auto db_ff122 = DataBag::ImmutableEmptyWithFallbacks({db_f12, db2});
  auto db_ff212 = DataBag::ImmutableEmptyWithFallbacks({db_f21, db2});

  EXPECT_TRUE(DataBagComparison::ExactlyEqual(db_f12, db_f12_copy));
  EXPECT_FALSE(DataBagComparison::ExactlyEqual(db_f12, db_f21));
  EXPECT_FALSE(DataBagComparison::ExactlyEqual(db_f1, db_f12));
  EXPECT_FALSE(DataBagComparison::ExactlyEqual(db_f1, db_f21));
  EXPECT_TRUE(DataBagComparison::ExactlyEqual(db_ff12, db_ff122));
  EXPECT_FALSE(DataBagComparison::ExactlyEqual(db_ff212, db_ff122));
}


}  // namespace
}  // namespace koladata
