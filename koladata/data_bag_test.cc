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
#include "koladata/data_bag.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/testing/status_matchers_backport.h"

namespace koladata {
namespace {

using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(DataBagTest, Fallbacks) {
  auto db = DataBag::Empty();
  EXPECT_TRUE(db->IsMutable());
  EXPECT_OK(db->GetMutableImpl());
  EXPECT_THAT(db->GetFallbacks(), ElementsAre());
  auto db_fb = DataBag::Empty();

  auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, nullptr, db_fb});
  EXPECT_THAT(new_db->GetFallbacks(), ElementsAre(db, db_fb));
  EXPECT_FALSE(new_db->IsMutable());
  EXPECT_THAT(new_db->GetMutableImpl(),
              ::koladata::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                            HasSubstr("immutable")));
}

TEST(DataBagTest, CommonDataBag_AllEmpty) {
  EXPECT_EQ(DataBag::CommonDataBag({nullptr, nullptr}), nullptr);
  EXPECT_EQ(DataBag::CommonDataBag({}), nullptr);
}

TEST(DataBagTest, CommonDataBag_1_Present) {
  auto db = DataBag::Empty();
  EXPECT_EQ(DataBag::CommonDataBag({nullptr, db, nullptr, db}), db);
}

TEST(DataBagTest, CommonDataBag_NewWithFallbacks) {
  auto db_1 = DataBag::Empty();
  auto db_2 = DataBag::Empty();
  auto new_db =
      DataBag::CommonDataBag({nullptr, db_1, db_1, nullptr, db_2, db_2, db_2});
  EXPECT_THAT(new_db->GetFallbacks(), ElementsAre(db_1, db_2));
  EXPECT_FALSE(new_db->IsMutable());
}

TEST(DataBagTest, FromImpl) {
  auto impl_db = internal::DataBagImpl::CreateEmptyDatabag();
  auto db = DataBag::FromImpl(impl_db);
  EXPECT_TRUE(db->IsMutable());
  EXPECT_EQ(&db->GetImpl(), impl_db.get());
  EXPECT_THAT(db->GetFallbacks(), ElementsAre());
}

TEST(DataBagTest, CollectFlattenFallbacks) {
  auto db = DataBag::Empty();
  {
    FlattenFallbackFinder fbf(*db);
    EXPECT_THAT(fbf.GetFlattenFallbacks(), ElementsAre());
  }

  {
    auto db_fb = DataBag::Empty();
    auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, db_fb});
    FlattenFallbackFinder fbf(*new_db);
    EXPECT_THAT(fbf.GetFlattenFallbacks(),
                ElementsAre(&db->GetImpl(), &db_fb->GetImpl()));
  }

  {
    auto db_fb = DataBag::Empty();
    auto db_fb2 = DataBag::Empty();
    auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, db_fb, db_fb2});
    FlattenFallbackFinder fbf(*new_db);
    EXPECT_THAT(
        fbf.GetFlattenFallbacks(),
        ElementsAre(&db->GetImpl(), &db_fb->GetImpl(), &db_fb2->GetImpl()));
  }

  {  // chain of two
    auto db_fb2 = DataBag::Empty();
    auto db_fb = DataBag::ImmutableEmptyWithFallbacks({db_fb2});
    auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, db_fb});
    FlattenFallbackFinder fbf(*new_db);
    EXPECT_THAT(
        fbf.GetFlattenFallbacks(),
        ElementsAre(&db->GetImpl(), &db_fb->GetImpl(), &db_fb2->GetImpl()));
  }

  {  // diamond
    auto db_fb3 = DataBag::Empty();
    auto db_fb = DataBag::ImmutableEmptyWithFallbacks({db_fb3});
    auto db_fb2 = DataBag::ImmutableEmptyWithFallbacks({db_fb3});
    auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, db_fb, db_fb2});
    FlattenFallbackFinder fbf(*new_db);
    EXPECT_THAT(fbf.GetFlattenFallbacks(),
                ElementsAre(&db->GetImpl(), &db_fb->GetImpl(),
                            &db_fb3->GetImpl(), &db_fb2->GetImpl()));
  }

  {  // exponential
    constexpr int kSteps = 1024;
    auto db = DataBag::Empty();
    auto db2 = DataBag::Empty();
    for (int i = 0; i < kSteps; ++i) {
      auto dbx = DataBag::ImmutableEmptyWithFallbacks({db, db2});
      db2 = DataBag::ImmutableEmptyWithFallbacks({db2, db});
      db = dbx;
    }
    FlattenFallbackFinder fbf(*db);
    EXPECT_EQ(fbf.GetFlattenFallbacks().size(), kSteps * 2);
  }
}

}  // namespace
}  // namespace koladata
