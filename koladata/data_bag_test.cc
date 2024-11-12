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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_bag.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;

TEST(DataBagTest, Fallbacks) {
  auto db = DataBag::Empty();
  EXPECT_TRUE(db->IsMutable());
  EXPECT_OK(db->GetMutableImpl());
  EXPECT_THAT(db->GetFallbacks(), ElementsAre());
  EXPECT_FALSE(db->HasMutableFallbacks());
  auto db_fb = DataBag::Empty();

  auto new_db = DataBag::ImmutableEmptyWithFallbacks({db, nullptr, db_fb});
  EXPECT_THAT(new_db->GetFallbacks(), ElementsAre(db, db_fb));
  EXPECT_FALSE(new_db->IsMutable());
  EXPECT_TRUE(new_db->HasMutableFallbacks());
  EXPECT_THAT(
      new_db->GetMutableImpl(),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("immutable")));
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
  EXPECT_TRUE(new_db->HasMutableFallbacks());
}

TEST(DataBagTest, MutableFallbacks) {
  auto db_1 = DataBag::Empty();
  EXPECT_TRUE(db_1->IsMutable());
  EXPECT_FALSE(db_1->HasMutableFallbacks());

  auto db_2 = DataBag::Empty();
  db_2->UnsafeMakeImmutable();
  EXPECT_FALSE(db_2->IsMutable());
  EXPECT_FALSE(db_2->HasMutableFallbacks());

  auto db_3 = DataBag::ImmutableEmptyWithFallbacks({db_1, db_2});
  EXPECT_FALSE(db_3->IsMutable());
  EXPECT_TRUE(db_3->HasMutableFallbacks());

  auto db_4 = DataBag::ImmutableEmptyWithFallbacks({db_3, db_1});
  EXPECT_FALSE(db_4->IsMutable());
  EXPECT_TRUE(db_4->HasMutableFallbacks());
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

TEST(DataBagTest, MergeInplace) {
  auto db_1 = DataBag::Empty();
  auto db_2 = DataBag::Empty();
  auto db_3 = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto ds_1, EntityCreator::FromAttrs(
          db_1,
          {std::string("a"), std::string("b")},
          {test::DataItem(1), test::DataItem(2)}));
  auto ds_2 = ds_1.WithBag(db_2);
  auto ds_3 = ds_1.WithBag(db_3);
  ASSERT_OK(ds_2.SetAttr("a", test::DataItem(3)));
  ASSERT_OK(ds_3.SetAttr("b", test::DataItem("foo")));

  // Some of the MergeInplace calls below do partial modification before
  // failure, so we recreate it every time.
  auto recreate = [&db_1, &ds_1]() -> absl::Status {
    db_1 = DataBag::Empty();
    ds_1 = ds_1.WithBag(db_1);
    RETURN_IF_ERROR(ds_1.SetAttr("a", test::DataItem(1)));
    return ds_1.SetAttr("b", test::DataItem(2));
  };

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_2, /*overwrite=*/false,
                                 /*allow_data_conflicts=*/false,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting values")));

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_2, /*overwrite=*/true,
                                 /*allow_data_conflicts=*/false,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting values")));

  ASSERT_OK(recreate());
  ASSERT_OK(db_1->MergeInplace(db_2, /*overwrite=*/false,
                               /*allow_data_conflicts=*/true,
                               /*allow_schema_conflicts=*/false));
  EXPECT_THAT(ds_1.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(1, db_1))));

  ASSERT_OK(recreate());
  ASSERT_OK(db_1->MergeInplace(db_2, /*overwrite=*/true,
                               /*allow_data_conflicts=*/true,
                               /*allow_schema_conflicts=*/false));
  EXPECT_THAT(ds_1.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(3, db_1))));
  EXPECT_THAT(ds_1.GetAttr("b"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(2, db_1))));

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_3, /*overwrite=*/false,
                                 /*allow_data_conflicts=*/false,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting values")));

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_3, /*overwrite=*/true,
                                 /*allow_data_conflicts=*/false,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting values")));

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_3, /*overwrite=*/false,
                                 /*allow_data_conflicts=*/true,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting dict values")));

  ASSERT_OK(recreate());
  EXPECT_THAT(db_1->MergeInplace(db_3, /*overwrite=*/true,
                                 /*allow_data_conflicts=*/true,
                                 /*allow_schema_conflicts=*/false),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("conflicting dict values")));

  ASSERT_OK(recreate());
  ASSERT_OK(db_1->MergeInplace(db_3, /*overwrite=*/false,
                               /*allow_data_conflicts=*/true,
                               /*allow_schema_conflicts=*/true));
  EXPECT_THAT(ds_1.GetAttr("b"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(2, db_1))));

  ASSERT_OK(recreate());
  ASSERT_OK(db_1->MergeInplace(db_3, /*overwrite=*/true,
                               /*allow_data_conflicts=*/true,
                               /*allow_schema_conflicts=*/true));
  EXPECT_THAT(ds_1.GetAttr("b"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem("foo", db_1))));
  EXPECT_THAT(ds_1.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(1, db_1))));
}

TEST(DataBagTest, GetBagIdRepr) {
  DataBagPtr db = DataBag::Empty();
  EXPECT_THAT(GetBagIdRepr(db), MatchesRegex(R"regex(\$[0-9a-f]{4})regex"));
}

TEST(DataBagTest, Fork) {
  auto db1 = DataBag::Empty();
  auto ds_a_db1 = test::DataItem(42, db1);
  ASSERT_OK_AND_ASSIGN(
      auto ds1,
      EntityCreator::FromAttrs(db1, {std::string("a")}, {ds_a_db1}));

  ASSERT_OK_AND_ASSIGN(auto db2, db1->Fork());
  auto ds2 = ds1.WithBag(db2);
  auto ds_a_db2 = test::DataItem(42, db2);
  EXPECT_THAT(ds1.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a_db1)));
  EXPECT_THAT(ds2.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a_db2)));

  auto ds_a1 = test::DataItem(43, db1);
  ASSERT_OK(ds1.SetAttr("a", ds_a1));
  EXPECT_THAT(ds1.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a1)));
  EXPECT_THAT(ds2.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a_db2)));

  auto ds_a2 = test::DataItem(44, db2);
  ASSERT_OK(ds2.SetAttr("a", ds_a2));
  EXPECT_THAT(ds1.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a1)));
  EXPECT_THAT(ds2.GetAttr("a"), IsOkAndHolds(IsEquivalentTo(ds_a2)));
}

TEST(DataBagTest, Fork_Immutable) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto immutable_db, db->Fork(/*immutable=*/true));
  const internal::DataBagImpl* immutable_db_impl_ptr = &immutable_db->GetImpl();

  // Check that forking an immutable DataBag doesn't change its DataBagImpl.
  {
    auto forked_db = immutable_db->Fork(true);
    EXPECT_EQ(immutable_db_impl_ptr, &immutable_db->GetImpl());
  }
  {
    auto forked_db = immutable_db->Fork(false);
    EXPECT_EQ(immutable_db_impl_ptr, &immutable_db->GetImpl());
  }
}

TEST(DataBagTest, Fork_Mutable) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto mutable_db, db->Fork(/*immutable=*/false));

  // Check that forking a mutable DataBag *does* change its DataBagImpl.
  {
    const internal::DataBagImpl* mutable_db_impl_ptr = &mutable_db->GetImpl();
    auto forked_db = mutable_db->Fork(true);
    EXPECT_EQ(mutable_db_impl_ptr, &mutable_db->GetImpl());
    // Forking a mutable DataBag is delayed until GetMutableImpl() is called.
    auto _ = mutable_db->GetMutableImpl();
    EXPECT_NE(mutable_db_impl_ptr, &mutable_db->GetImpl());
  }
  {
    const internal::DataBagImpl* mutable_db_impl_ptr = &mutable_db->GetImpl();
    auto forked_db = mutable_db->Fork(false);
    // Forking a mutable DataBag is delayed until GetMutableImpl() is called.
    EXPECT_EQ(mutable_db_impl_ptr, &mutable_db->GetImpl());
    auto _ = mutable_db->GetMutableImpl();
    EXPECT_NE(mutable_db_impl_ptr, &mutable_db->GetImpl());
  }
}

TEST(DataBagTest, MergeFallbacks) {
  auto fallback_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds1,
      EntityCreator::FromAttrs(fallback_db, {std::string("a")},
                               {test::DataItem(42, fallback_db)}));
  auto db = DataBag::ImmutableEmptyWithFallbacks({fallback_db});
  auto ds2 = ds1.WithBag(db);

  ASSERT_OK_AND_ASSIGN(auto db_merged, db->MergeFallbacks());

  // Check that the merged DataBag has the same data as the original one,
  // but no fallbacks.
  auto ds1_merged = ds1.WithBag(db_merged);
  EXPECT_THAT(ds1_merged.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(42, db_merged))));
  ASSERT_EQ(db_merged->GetFallbacks().size(), 0);

  // Check that modifications to the merged DataBag don't affect the original.
  ASSERT_OK(ds1_merged.SetAttr("a", test::DataItem(43, db_merged)));
  EXPECT_THAT(ds2.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(42, db))));

  // Check that modifications to the fallback db don't affect the merged one.
  ASSERT_OK(ds1.SetAttr("a", test::DataItem(44, db_merged)));
  EXPECT_THAT(ds1_merged.GetAttr("a"),
              IsOkAndHolds(IsEquivalentTo(test::DataItem(43, db_merged))));
}

TEST(DataBagTest, Fork_Mutability) {
  {
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(auto db2, db1->Fork());
    EXPECT_TRUE(db2->IsMutable());
  }
  {
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(auto db2, db1->Fork(/*immutable=*/true));
    EXPECT_FALSE(db2->IsMutable());
  }
  {
    auto db1 = DataBag::ImmutableEmptyWithFallbacks({});
    ASSERT_OK_AND_ASSIGN(auto db2, db1->Fork());
    EXPECT_TRUE(db2->IsMutable());
  }
  {
    auto db1 = DataBag::ImmutableEmptyWithFallbacks({});
    ASSERT_OK_AND_ASSIGN(auto db2, db1->Fork(/*immutable=*/true));
    EXPECT_FALSE(db2->IsMutable());
  }
}

TEST(DataBagTest, UnsafeMakeImmutable) {
  DataBag db1;
  EXPECT_TRUE(db1.IsMutable());
  db1.UnsafeMakeImmutable();
  EXPECT_FALSE(db1.IsMutable());
}

}  // namespace
}  // namespace koladata
