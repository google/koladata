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
#include "koladata/internal/dict.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/memory_stats.h"
#include "koladata/internal/missing_value.h"

namespace koladata::internal {

struct DictVectorTestFriend {
  static bool is_map_mode(const DictVector& v) { return v.is_map_mode(); }
};

namespace {

using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Optional;
using ::testing::UnorderedElementsAre;

MATCHER_P(RefWrap, value, "") { return arg.get() == value; }

void AssertKVsAreAligned(const Dict& dict) {
  auto keys = dict.GetKeys();
  auto values = dict.GetValues();
  ASSERT_EQ(keys.size(), values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    EXPECT_THAT(dict.Get(keys[i]), Optional(RefWrap(values[i])));
  }
}

TEST(DictTest, Dict) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(3, /*update_size=*/3);
  (*dicts)[0].Set(DataItem(1), DataItem(7.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(1, DataItem(9)), DataItem(7.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(1), DataItem(9)), DataItem(7.f));
  (*dicts)[0].Set(arolla::Bytes("aaa"), DataItem(arolla::Bytes("bbb")));
  (*dicts)[0].Set(DataItem(1), DataItem(6.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(1, DataItem(9)), DataItem(6.f));

  const DictVector& const_dicts = *dicts;
  ASSERT_NE(const_dicts.Get(0), nullptr);
  EXPECT_THAT(const_dicts.Get(0)->GetKeys(),
              UnorderedElementsAre(1, arolla::Bytes("aaa")));
  EXPECT_THAT(const_dicts.Get(0)->GetValues(),
              UnorderedElementsAre(DataItem(6.f), arolla::Bytes("bbb")));
  AssertKVsAreAligned(*const_dicts.Get(0));
  ASSERT_EQ(const_dicts.Get(1), nullptr);
  ASSERT_EQ(const_dicts.Get(2), nullptr);

  DictVector derived_dicts(dicts, /*update_size=*/3);
  derived_dicts[0].Set(arolla::Bytes("aaa"), DataItem(9));
  derived_dicts[0].Set(arolla::Bytes("2"), DataItem(10));

  EXPECT_THAT(
      derived_dicts[0].GetKeys(),
      UnorderedElementsAre(1, arolla::Bytes("2"), arolla::Bytes("aaa")));
  EXPECT_THAT(derived_dicts[0].GetValues(),
              UnorderedElementsAre(DataItem(6.f), DataItem(10), DataItem(9)));
  AssertKVsAreAligned(derived_dicts[0]);
  EXPECT_THAT(derived_dicts[1].GetKeys(), UnorderedElementsAre());
  EXPECT_THAT(derived_dicts[1].GetValues(), UnorderedElementsAre());
  AssertKVsAreAligned(derived_dicts[1]);
  EXPECT_THAT(derived_dicts[2].GetKeys(), UnorderedElementsAre());
  EXPECT_THAT(derived_dicts[2].GetValues(), UnorderedElementsAre());
  AssertKVsAreAligned(derived_dicts[2]);

  EXPECT_THAT(derived_dicts[0].Get(DataItem(1)), Optional(RefWrap(6.f)));
  EXPECT_EQ(derived_dicts[0].GetOrAssign(DataItem(1), DataItem(7.f)), 6.f);
  EXPECT_THAT(derived_dicts[0].Get(1), Optional(RefWrap(6.f)));
  EXPECT_THAT(derived_dicts[0].Get(arolla::Bytes("aaa")), Optional(RefWrap(9)));
  EXPECT_EQ(derived_dicts[0].GetOrAssign(arolla::Bytes("aaa"), DataItem(1)), 9);
  EXPECT_THAT(derived_dicts[0].Get(arolla::Bytes("aaa")), Optional(RefWrap(9)));
  EXPECT_THAT(derived_dicts[0].Get(arolla::Bytes("2")), Optional(RefWrap(10)));
  EXPECT_EQ(derived_dicts[0].GetOrAssign(arolla::Bytes("2"), DataItem(13)), 10);

  EXPECT_THAT(derived_dicts[1].Get(DataItem(1)), Eq(std::nullopt));
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(1), DataItem(57)),
            DataItem(57));
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(53), DataItem(37.f)), 37.f);
  derived_dicts[1].Set(DataItem(53), DataItem());
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(53), DataItem(39.f)),
            DataItem());
  EXPECT_THAT(derived_dicts[1].Get(DataItem()), Eq(std::nullopt));
  EXPECT_THAT(derived_dicts[1].Get(MissingValue{}), Eq(std::nullopt));

  derived_dicts[0].Clear();
  EXPECT_THAT(
      derived_dicts[0].GetKeys(),
      UnorderedElementsAre(1, arolla::Bytes("aaa"), arolla::Bytes("2")));
  EXPECT_THAT(derived_dicts[0].GetValues(),
              UnorderedElementsAre(DataItem(), DataItem(), DataItem()));
  AssertKVsAreAligned(derived_dicts[0]);
  EXPECT_THAT(derived_dicts[0].Get(DataItem(1)), Optional(RefWrap(DataItem())));
  EXPECT_THAT(derived_dicts[0].Get(DataItem(1.f)), Eq(std::nullopt));

  EXPECT_THAT(const_dicts.Get(0)->GetKeys(),
              UnorderedElementsAre(1, arolla::Bytes("aaa")));
  EXPECT_THAT(const_dicts.Get(0)->GetValues(),
              UnorderedElementsAre(DataItem(6.f), arolla::Bytes("bbb")));
  AssertKVsAreAligned(*const_dicts.Get(0));
  EXPECT_THAT(const_dicts.Get(0)->Get(DataItem(1)), Optional(RefWrap(6.f)));
  EXPECT_THAT(const_dicts.Get(0)->Get(arolla::Bytes("2")), Eq(std::nullopt));
  EXPECT_THAT(const_dicts.Get(0)->Get(arolla::Bytes("aaa")),
              Optional(RefWrap(arolla::Bytes("bbb"))));

  using BytesItemView = DataItem::View<arolla::Bytes>;
  EXPECT_THAT(const_dicts.Get(0)->Get(BytesItemView{"aaa"}),
              Optional(RefWrap(BytesItemView{"bbb"})));

  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(13), DataItem(7.f)), 7.f);
  (*dicts)[0].Set(DataItem(13), DataItem());
  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(13), DataItem(9.f)), DataItem());
}

TEST(DictTest, OverrideWithEmptyNoParent) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& dict = (*dicts)[0];
  dict.Set(DataItem(1), DataItem(5.f));
  dict.Set(DataItem(1), DataItem());
  EXPECT_THAT(dict.Get(1), Optional(RefWrap(DataItem())));
}

TEST(DictTest, OverrideWithEmptyWithParent) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& parent_dict = (*dicts)[0];
  parent_dict.Set(DataItem(1), DataItem(5.f));
  parent_dict.Set(DataItem(2), DataItem(7.f));

  DictVector derived_dicts(dicts, /*update_size=*/1);
  auto& derived_dict = derived_dicts[0];
  derived_dict.Set(1, DataItem());
  EXPECT_THAT(derived_dict.Get(1), Optional(RefWrap(DataItem())));
  EXPECT_THAT(derived_dict.Get(2), Optional(RefWrap(7.f)));

  derived_dict.Set(1, DataItem(9.f));
  EXPECT_THAT(derived_dict.Get(1), Optional(RefWrap(9.f)));
}

TEST(DictTest, GetOrAssignWithEmptyNoParent) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& dict = (*dicts)[0];
  EXPECT_EQ(dict.GetOrAssign(DataItem(1), DataItem()), DataItem());
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(1));
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
  EXPECT_THAT(dict.Get(1), Optional(RefWrap(DataItem())));

  // `2` is not assigned because the dict already has an empty value for this
  // key.
  EXPECT_EQ(dict.GetOrAssign(DataItem(1), DataItem(2)), DataItem());

  dict.Set(DataItem(1), DataItem(5.f));
  EXPECT_EQ(dict.GetOrAssign(DataItem(1), DataItem()), DataItem(5.f));
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(DataItem(1)));
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
}

TEST(DictTest, GetOrAssignWithEmptyWithParent) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& parent_dict = (*dicts)[0];
  parent_dict.Set(DataItem(1), DataItem(5.f));
  parent_dict.Set(DataItem(2), DataItem(7.f));

  DictVector derived_dicts(dicts, /*update_size=*/1);
  auto& derived_dict = derived_dicts[0];
  EXPECT_THAT(derived_dict.GetKeys(),
              UnorderedElementsAre(DataItem(1), DataItem(2)));
  EXPECT_EQ(derived_dict.GetSizeNoFallbacks(), 2);
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(7), DataItem()), DataItem());
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(5.f));
  // repeat to be sure we do not override
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(5.f));
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(2), DataItem()), DataItem(7.f));
  EXPECT_THAT(derived_dict.GetKeys(),
              UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(7)));
  EXPECT_EQ(derived_dict.GetSizeNoFallbacks(), 3);

  parent_dict.Set(DataItem(1), DataItem(9.f));
  parent_dict.Set(DataItem(3), DataItem(2.f));
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(9.f));
  // repeat to be sure we do not override
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(9.f));
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(3), DataItem()), DataItem(2.f));
  // repeat to be sure we do not override
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(3), DataItem()), DataItem(2.f));
  EXPECT_THAT(
      derived_dict.GetKeys(),
      UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(3), DataItem(7)));
  EXPECT_EQ(derived_dict.GetSizeNoFallbacks(), 4);
}

TEST(DictTest, DerivedDictExtra) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& dict = (*dicts)[0];
  dict.Set(arolla::Text("a"), DataItem(7.f));
  dict.Set(1, DataItem(8));

  std::shared_ptr<DictVector> derived_dicts =
      std::make_shared<DictVector>(std::move(dicts), /*update_size=*/1);
  auto& derived_dict = (*derived_dicts)[0];
  derived_dict.Set(1, DataItem(9));
  derived_dict.Set(2, DataItem(10));

  EXPECT_THAT(derived_dict.GetKeys(),
              UnorderedElementsAre(1, 2, arolla::Text("a")));
  EXPECT_THAT(derived_dict.GetValues(),
              UnorderedElementsAre(DataItem(9), DataItem(10), DataItem(7.f)));
  AssertKVsAreAligned(derived_dict);

  EXPECT_THAT(derived_dict.Get(arolla::Text("a")), Optional(RefWrap(7.f)));
  EXPECT_THAT(derived_dict.Get(1), Optional(RefWrap(9)));
  EXPECT_THAT(derived_dict.Get(2), Optional(RefWrap(10)));

  std::shared_ptr<DictVector> derived_dicts2 =
      std::make_shared<DictVector>(std::move(derived_dicts), /*update_size=*/1);
  auto& derived_dict2 = (*derived_dicts2)[0];
  derived_dict2.Set(0, DataItem(5));
  derived_dict2.Set(2, DataItem(7));

  EXPECT_THAT(derived_dict2.GetKeys(),
              UnorderedElementsAre(0, 1, 2, arolla::Text("a")));
  EXPECT_THAT(derived_dict2.GetValues(),
              UnorderedElementsAre(DataItem(5), DataItem(9), DataItem(7),
                                   DataItem(7.f)));
  AssertKVsAreAligned(derived_dict2);

  EXPECT_THAT(derived_dict2.Get(arolla::Text("a")), Optional(RefWrap(7.f)));
  EXPECT_THAT(derived_dict2.Get(0), Optional(RefWrap(5)));
  EXPECT_THAT(derived_dict2.Get(1), Optional(RefWrap(9)));
  EXPECT_THAT(derived_dict2.Get(2), Optional(RefWrap(7)));

  derived_dict2.Clear();
  EXPECT_THAT(derived_dict2.GetKeys(),
              UnorderedElementsAre(0, 1, 2, arolla::Text("a")));
  EXPECT_THAT(
      derived_dict2.GetValues(),
      UnorderedElementsAre(DataItem(), DataItem(), DataItem(), DataItem()));
  AssertKVsAreAligned(derived_dict2);
  EXPECT_THAT(derived_dict2.Get(arolla::Text("a")),
              Optional(RefWrap(DataItem())));
  EXPECT_THAT(derived_dict2.Get(0), Optional(RefWrap(DataItem())));
  EXPECT_THAT(derived_dict2.Get(1), Optional(RefWrap(DataItem())));
  EXPECT_THAT(derived_dict2.Get(2), Optional(RefWrap(DataItem())));
}

TEST(DictTest, DerivedDictSingle) {
  std::shared_ptr<Dict> parent_dict = std::make_shared<Dict>();
  parent_dict->Set(arolla::Text("a"), DataItem(7.f));
  parent_dict->Set(1, DataItem(8));

  DictVector derived_dicts(4, parent_dict, /*update_size=*/4);
  parent_dict.reset();  // verify ownership
  {
    auto& derived_dict = derived_dicts[0];
    derived_dict.Set(1, DataItem(9));
    derived_dict.Set(2, DataItem(10));

    EXPECT_THAT(derived_dict.GetKeys(),
                UnorderedElementsAre(1, 2, arolla::Text("a")));

    EXPECT_THAT(derived_dict.Get(arolla::Text("a")), Optional(RefWrap(7.f)));
    EXPECT_THAT(derived_dict.Get(1), Optional(RefWrap(9)));
    EXPECT_THAT(derived_dict.Get(2), Optional(RefWrap(10)));
  }
  {
    auto& derived_dict = derived_dicts[1];

    EXPECT_THAT(derived_dict.GetKeys(),
                UnorderedElementsAre(1, arolla::Text("a")));

    EXPECT_THAT(derived_dict.Get(arolla::Text("a")), Optional(RefWrap(7.f)));
    EXPECT_THAT(derived_dict.Get(1), Optional(RefWrap(8)));
  }
}

TEST(DictTest, GetKeysAndValuesWithFallback) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& dict = (*dicts)[0];
  dict.Set(arolla::Text("a"), DataItem(7.f));
  dict.Set(1, DataItem(8));

  DictVector derived_dicts(dicts, /*update_size=*/1);
  auto& derived_dict = derived_dicts[0];
  derived_dict.Set(1, DataItem(9));
  derived_dict.Set(2, DataItem(10));

  std::shared_ptr<DictVector> fb_dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& fb_dict = (*fb_dicts)[0];
  fb_dict.Set(arolla::Text("b"), DataItem(7.f));
  fb_dict.Set(1, DataItem(2));

  EXPECT_THAT(derived_dict.GetKeys({&fb_dict}),
              UnorderedElementsAre(1, 2, arolla::Text("a"), arolla::Text("b")));
  EXPECT_THAT(derived_dict.GetValues({&fb_dict}),
              UnorderedElementsAre(DataItem(9), DataItem(10), DataItem(7.f),
                                   DataItem(7.f)));
  AssertKVsAreAligned(derived_dict);

  DictVector fb_derived_dicts(fb_dicts, /*update_size=*/1);
  auto& fb_derived_dict = fb_derived_dicts[0];
  fb_derived_dict.Set(3, DataItem(7));
  fb_derived_dict.Set(2, DataItem(0));

  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));
  EXPECT_THAT(derived_dict.GetValues({&fb_derived_dict}),
              UnorderedElementsAre(DataItem(9), DataItem(10), DataItem(7),
                                   DataItem(7.f), DataItem(7.f)));
  AssertKVsAreAligned(derived_dict);

  derived_dict.Set(2, DataItem());
  // 2 is still in fallback, but value in parent dict is removed
  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));
  EXPECT_THAT(derived_dict.GetValues({&fb_derived_dict}),
              UnorderedElementsAre(DataItem(9), DataItem(), DataItem(7),
                                   DataItem(7.f), DataItem(7.f)));
  AssertKVsAreAligned(derived_dict);

  derived_dict.Set(1, DataItem());
  // 1 is still in fallback, but value in parent dict is removed
  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));
  EXPECT_THAT(derived_dict.GetValues({&fb_derived_dict}),
              UnorderedElementsAre(DataItem(), DataItem(), DataItem(7),
                                   DataItem(7.f), DataItem(7.f)));
  AssertKVsAreAligned(derived_dict);
}

TEST(DictTest, GetKeysWithFallbackEmptyMain) {
  std::shared_ptr<DictVector> dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& dict = (*dicts)[0];
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre());

  std::shared_ptr<DictVector> fb_dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& fb_dict = (*fb_dicts)[0];
  fb_dict.Set(1, DataItem(2));

  EXPECT_THAT(dict.GetKeys({&fb_dict}), UnorderedElementsAre(1));

  std::shared_ptr<DictVector> fb2_dicts =
      std::make_shared<DictVector>(1, /*update_size=*/1);
  auto& fb2_dict = (*fb2_dicts)[0];

  EXPECT_THAT(dict.GetKeys({&fb2_dict}), UnorderedElementsAre());
  EXPECT_THAT(dict.GetKeys({&fb2_dict, &fb_dict}), UnorderedElementsAre(1));
  EXPECT_THAT(dict.GetKeys({&fb_dict, &fb2_dict}), UnorderedElementsAre(1));
}

TEST(DictTest, IntegerKeyTypes) {
  Dict dict;
  dict.Set(int{1}, DataItem(1));
  dict.Set(int64_t{1}, DataItem(2));
  EXPECT_THAT(dict.Get(int{1}), Optional(RefWrap(2)));
  EXPECT_THAT(dict.Get(int64_t{1}), Optional(RefWrap(2)));
}

TEST(DictTest, UnsupportedKeyTypes) {
  Dict dict;
  dict.Set(DataItem(), DataItem(1));
  dict.Set(MissingValue{}, DataItem(2));
  dict.Set(1.0f, DataItem(3));
  dict.Set(1.0, DataItem(4));
  dict.Set(DataItem(1.0f), DataItem(5));
  dict.Set(DataItem(1.0), DataItem(6));

  EXPECT_EQ(dict.GetSizeNoFallbacks(), 0);
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre());
  EXPECT_THAT(dict.GetValues(), UnorderedElementsAre());

  EXPECT_THAT(dict.Get(DataItem()), Eq(std::nullopt));
  EXPECT_THAT(dict.Get(MissingValue{}), Eq(std::nullopt));
  EXPECT_THAT(dict.Get(1.0f), Eq(std::nullopt));
  EXPECT_THAT(dict.Get(1.0), Eq(std::nullopt));
  EXPECT_THAT(dict.Get(DataItem(1.0f)), Eq(std::nullopt));
  EXPECT_THAT(dict.Get(DataItem(1.0)), Eq(std::nullopt));

  EXPECT_EQ(dict.GetOrAssign(DataItem(), DataItem(10)), DataItem());
  EXPECT_EQ(dict.GetOrAssign(1.0f, DataItem(10)), DataItem());
  EXPECT_EQ(dict.GetOrAssign(1.0, DataItem(10)), DataItem());
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 0);
}

TEST(DictTest, GetKeysOnMissing) {
  Dict dict;
  dict.Set(int64_t{1}, DataItem());
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(1));
  EXPECT_THAT(dict.GetValues(), UnorderedElementsAre(DataItem()));
  AssertKVsAreAligned(dict);
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
  dict.Set(int64_t{1}, DataItem(3));
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(1));
  EXPECT_THAT(dict.GetValues(), UnorderedElementsAre(DataItem(3)));
  AssertKVsAreAligned(dict);
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
}

TEST(DictTest, GetSortedKeyValues) {
  Dict dict;
  dict.Set(int64_t{1}, DataItem(5));
  dict.Set(int64_t{3}, DataItem(4));
  dict.Set(int64_t{2}, DataItem(3));
  EXPECT_THAT(dict.GetSortedKeys(), ElementsAre(1, 2, 3));
  EXPECT_THAT(dict.GetSortedByKeyValues(), ElementsAre(5, 3, 4));
}

TEST(DictTest, GetKeysWithUpdatesFromParent) {
  {
    SCOPED_TRACE("parent is the same dict");
    Dict dict;
    dict.Set(DataItem(1), DataItem(10));
    EXPECT_THAT(dict.GetModifiedKeys(&dict), UnorderedElementsAre());
  }

  {
    SCOPED_TRACE("parent is direct parent");
    std::shared_ptr<DictVector> dicts =
        std::make_shared<DictVector>(1, /*update_size=*/1);
    auto& parent_dict = (*dicts)[0];
    parent_dict.Set(DataItem(1), DataItem(10));
    parent_dict.Set(DataItem(2), DataItem(20));

    DictVector derived_dicts(dicts, /*update_size=*/1);
    auto& derived_dict = derived_dicts[0];
    derived_dict.Set(DataItem(2), DataItem(22));  // update
    derived_dict.Set(DataItem(3), DataItem(30));  // new

    EXPECT_THAT(derived_dict.GetModifiedKeys(&parent_dict),
                UnorderedElementsAre(DataItem(2), DataItem(3)));
  }

  {
    SCOPED_TRACE("parent is completely irrelevant");
    std::shared_ptr<DictVector> dicts =
        std::make_shared<DictVector>(1, /*update_size=*/1);
    auto& parent_dict = (*dicts)[0];
    parent_dict.Set(DataItem(1), DataItem(10));

    DictVector derived_dicts(dicts, /*update_size=*/1);
    auto& derived_dict = derived_dicts[0];
    derived_dict.Set(DataItem(2), DataItem(20));

    Dict irrelevant_dict;
    irrelevant_dict.Set(DataItem(3), DataItem(30));

    EXPECT_THAT(derived_dict.GetModifiedKeys(&irrelevant_dict),
                UnorderedElementsAre(DataItem(1), DataItem(2)));
    EXPECT_THAT(derived_dict.GetModifiedKeys(nullptr),
                UnorderedElementsAre(DataItem(1), DataItem(2)));
  }

  {
    SCOPED_TRACE("parent is indirect ancestor of the dict");
    std::shared_ptr<DictVector> dicts =
        std::make_shared<DictVector>(1, /*update_size=*/1);
    auto& grand_parent = (*dicts)[0];
    grand_parent.Set(DataItem(1), DataItem(10));

    std::shared_ptr<DictVector> dicts2 =
        std::make_shared<DictVector>(dicts, /*update_size=*/1);
    auto& parent_dict = (*dicts2)[0];
    parent_dict.Set(DataItem(2), DataItem(20));

    DictVector derived_dicts(dicts2, /*update_size=*/1);
    auto& derived_dict = derived_dicts[0];
    derived_dict.Set(DataItem(3), DataItem(30));

    EXPECT_THAT(derived_dict.GetModifiedKeys(&grand_parent),
                UnorderedElementsAre(DataItem(2), DataItem(3)));
  }
}

TEST(DictTest, DictVectorSmallCapacity) {
  auto vec = std::make_shared<DictVector>(4, /*update_size=*/1);
  ASSERT_EQ(vec->size(), 4);
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_EQ(vec->Get(2), nullptr);

  vec->GetMutable(2).Set(1, DataItem(7));
  EXPECT_NE(vec->Get(2), nullptr);
  EXPECT_THAT(vec->Get(2)->Get(1), Optional(RefWrap(7)));
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));

  auto derived_vec = std::make_shared<DictVector>(vec, /*update_size=*/1);
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*derived_vec));
  derived_vec->GetMutable(1).Set(1, DataItem(5));

  EXPECT_EQ(derived_vec->Get(0), nullptr);
  EXPECT_EQ(derived_vec->Get(1)->GetSizeNoFallbacks(), 1);
  EXPECT_EQ(derived_vec->Get(2)->GetSizeNoFallbacks(), 1);

  EXPECT_EQ(vec->Get(0), derived_vec->Get(0));  // unset dict is nullptr
  EXPECT_NE(vec->Get(1), derived_vec->Get(1));
  EXPECT_NE(vec->Get(2), nullptr);
  EXPECT_NE(derived_vec->Get(2), nullptr);
  EXPECT_THAT(derived_vec->Get(2)->Get(1), Optional(RefWrap(7)));

  derived_vec->GetMutable(2).Set(2, DataItem(9));
  EXPECT_THAT(vec->Get(2)->GetKeys(), UnorderedElementsAre(1));
  EXPECT_THAT(derived_vec->Get(2)->GetKeys(), UnorderedElementsAre(1, 2));
}

TEST(DictTest, DictVectorMapMode) {
  // Capacity 10: 30% of 10 is 3. Up to 3 elements in flat_hash_map.
  auto vec = std::make_shared<DictVector>(10, /*update_size=*/1);
  ASSERT_EQ(vec->size(), 10);
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_EQ(vec->Get(5), nullptr);

  // 1st element (10%)
  vec->GetMutable(1).Set(1, DataItem(100));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(vec->Get(1)->Get(1), Optional(RefWrap(100)));
  EXPECT_EQ(vec->Get(0), nullptr);

  // 2nd element (20%)
  vec->GetMutable(4).Set(1, DataItem(400));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(vec->Get(4)->Get(1), Optional(RefWrap(400)));

  // 3rd element (30%)
  vec->GetMutable(7).Set(1, DataItem(700));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(vec->Get(7)->Get(1), Optional(RefWrap(700)));

  // 4th element (40% > 30%) -> triggers transition to array
  vec->GetMutable(9).Set(1, DataItem(900));
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));

  // Verify all elements after transition to array
  EXPECT_EQ(vec->Get(0), nullptr);
  EXPECT_THAT(vec->Get(1)->Get(1), Optional(RefWrap(100)));
  EXPECT_EQ(vec->Get(2), nullptr);
  EXPECT_EQ(vec->Get(3), nullptr);
  EXPECT_THAT(vec->Get(4)->Get(1), Optional(RefWrap(400)));
  EXPECT_EQ(vec->Get(5), nullptr);
  EXPECT_EQ(vec->Get(6), nullptr);
  EXPECT_THAT(vec->Get(7)->Get(1), Optional(RefWrap(700)));
  EXPECT_EQ(vec->Get(8), nullptr);
  EXPECT_THAT(vec->Get(9)->Get(1), Optional(RefWrap(900)));
}

TEST(DictTest, DictVectorDerivedMap) {
  auto parent = std::make_shared<DictVector>(10, /*update_size=*/1);
  parent->GetMutable(2).Set(1, DataItem(20));
  parent->GetMutable(5).Set(1, DataItem(50));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*parent));

  auto derived = std::make_shared<DictVector>(parent, /*update_size=*/1);
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));

  // Read unmodified from parent
  EXPECT_EQ(derived->Get(0), nullptr);
  EXPECT_EQ(derived->Get(2), parent->Get(2));
  EXPECT_THAT(derived->Get(2)->Get(1), Optional(RefWrap(20)));
  EXPECT_EQ(derived->Get(5), parent->Get(5));
  EXPECT_THAT(derived->Get(5)->Get(1), Optional(RefWrap(50)));

  // Modify 1 element in derived (copy-on-write from parent)
  derived->GetMutable(2).Set(2, DataItem(21));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
  EXPECT_NE(derived->Get(2), parent->Get(2));
  EXPECT_THAT(parent->Get(2)->GetKeys(), UnorderedElementsAre(1));
  EXPECT_THAT(derived->Get(2)->GetKeys(), UnorderedElementsAre(1, 2));
  EXPECT_THAT(derived->Get(2)->Get(1), Optional(RefWrap(20)));
  EXPECT_THAT(derived->Get(2)->Get(2), Optional(RefWrap(21)));

  // Modify a previously unset element in derived
  derived->GetMutable(0).Set(1, DataItem(1));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
  EXPECT_THAT(derived->Get(0)->Get(1), Optional(RefWrap(1)));
  EXPECT_EQ(parent->Get(0), nullptr);

  // Modify 3rd element in derived (3/10 = 30%)
  derived->GetMutable(3).Set(1, DataItem(30));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));

  // Modify 4th element in derived (4/10 = 40% > 30%) -> triggers transition to
  // array
  derived->GetMutable(4).Set(1, DataItem(40));
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*derived));

  // Verify all elements in derived after array transition
  EXPECT_THAT(derived->Get(0)->Get(1), Optional(RefWrap(1)));
  EXPECT_THAT(derived->Get(2)->Get(1), Optional(RefWrap(20)));
  EXPECT_THAT(derived->Get(2)->Get(2), Optional(RefWrap(21)));
  EXPECT_THAT(derived->Get(3)->Get(1), Optional(RefWrap(30)));
  EXPECT_THAT(derived->Get(4)->Get(1), Optional(RefWrap(40)));
  EXPECT_NE(derived->Get(5), nullptr);
  EXPECT_THAT(derived->Get(5)->Get(1), Optional(RefWrap(50)));
  EXPECT_THAT(derived->Get(5)->GetKeys(), UnorderedElementsAre(1));
  EXPECT_EQ(derived->Get(1), nullptr);
  EXPECT_EQ(derived->Get(6), nullptr);

  // Parent should remain unaffected
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*parent));
  EXPECT_EQ(parent->Get(0), nullptr);
  EXPECT_THAT(parent->Get(2)->GetKeys(), UnorderedElementsAre(1));
  EXPECT_THAT(parent->Get(5)->GetKeys(), UnorderedElementsAre(1));
}

TEST(DictTest, DictVectorCapacityFive) {
  // Capacity 5: 30% of 5 is 1.5. 1 element (20%) fits in map mode.
  auto vec = std::make_shared<DictVector>(5, /*update_size=*/1);
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));

  vec->GetMutable(1).Set(1, DataItem(10));
  EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(vec->Get(1)->Get(1), Optional(RefWrap(10)));

  // 2nd element (40% > 30%) -> converts to array
  vec->GetMutable(2).Set(1, DataItem(20));
  EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));
  EXPECT_THAT(vec->Get(1)->Get(1), Optional(RefWrap(10)));
  EXPECT_THAT(vec->Get(2)->Get(1), Optional(RefWrap(20)));
  EXPECT_EQ(vec->Get(0), nullptr);
  EXPECT_EQ(vec->Get(3), nullptr);
  EXPECT_EQ(vec->Get(4), nullptr);
}

TEST(DictTest, DictVectorEmptyMemoryStats) {
  // Map mode
  {
    DictVector vec(10, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(vec));
    MemoryStatsEntry stats;
    vec.AppendMemoryStats(stats);
    EXPECT_EQ(stats.shallow_size, sizeof(DictVector));
    EXPECT_EQ(stats.strings_size, 0);
  }
  // Array mode
  {
    const int kSize = 2;
    DictVector vec(kSize, /*update_size=*/1);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(vec));
    MemoryStatsEntry stats;
    vec.AppendMemoryStats(stats);
    EXPECT_EQ(stats.shallow_size,
              sizeof(DictVector) + kSize * sizeof(Dict));
    EXPECT_EQ(stats.strings_size, 0);
  }
}

TEST(DictTest, DictVectorUpdateSize) {
  // Capacity 10, update_size = 5 (> 30% of 10): directly in Array mode.
  {
    auto vec = std::make_shared<DictVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));
    vec->GetMutable(2).Set(1, DataItem(20));
    EXPECT_THAT(vec->Get(2)->Get(1), Optional(RefWrap(20)));
  }

  // Capacity 10, update_size = 2 (<= 30% of 10): in Map mode.
  {
    auto vec = std::make_shared<DictVector>(10, /*update_size=*/2);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
    vec->GetMutable(2).Set(1, DataItem(20));
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));
    EXPECT_THAT(vec->Get(2)->Get(1), Optional(RefWrap(20)));
  }

  // Derived vector with large update_size -> directly in Array mode.
  {
    auto parent = std::make_shared<DictVector>(10, /*update_size=*/1);
    parent->GetMutable(1).Set(1, DataItem(10));
    auto derived = std::make_shared<DictVector>(parent, /*update_size=*/4);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(derived->Get(1)->Get(1), Optional(RefWrap(10)));
    derived->GetMutable(2).Set(1, DataItem(20));
    EXPECT_THAT(derived->Get(2)->Get(1), Optional(RefWrap(20)));
    EXPECT_THAT(derived->Get(1)->Get(1), Optional(RefWrap(10)));
  }

  // Derived vector with small update_size -> in Map mode.
  {
    auto parent = std::make_shared<DictVector>(10, /*update_size=*/1);
    parent->GetMutable(1).Set(1, DataItem(10));
    auto derived = std::make_shared<DictVector>(parent, /*update_size=*/2);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(derived->Get(1)->Get(1), Optional(RefWrap(10)));
    derived->GetMutable(2).Set(1, DataItem(20));
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
    EXPECT_THAT(derived->Get(2)->Get(1), Optional(RefWrap(20)));
  }
}

TEST(DictTest, DictVectorForEachDict) {
  // 1. Map mode (sparse, no parent)
  {
    auto vec = std::make_shared<DictVector>(10, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));

    // Empty vector: callback not called.
    int count = 0;
    EXPECT_OK(vec->ForEachDict([&](size_t idx, const Dict& dict) {
      ++count;
      return absl::OkStatus();
    }));
    EXPECT_EQ(count, 0);

    vec->GetMutable(2).Set(1, DataItem(20));
    vec->GetMutable(7).Set(2, DataItem(70));

    std::vector<size_t> visited_indices;
    EXPECT_OK(vec->ForEachDict([&](size_t idx, const Dict& dict) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(20)));
      } else if (idx == 7) {
        EXPECT_THAT(dict.Get(2), Optional(RefWrap(70)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 7));
  }

  // 2. Array mode
  {
    auto vec = std::make_shared<DictVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*vec));

    vec->GetMutable(1).Set(1, DataItem(10));
    vec->GetMutable(4).Set(2, DataItem(40));

    std::vector<size_t> visited_indices;
    EXPECT_OK(vec->ForEachDict([&](size_t idx, const Dict& dict) {
      visited_indices.push_back(idx);
      if (idx == 1) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(10)));
      } else if (idx == 4) {
        EXPECT_THAT(dict.Get(2), Optional(RefWrap(40)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(1, 4));
  }

  // 3. Derived with parent (both Map mode)
  {
    auto parent = std::make_shared<DictVector>(10, /*update_size=*/1);
    parent->GetMutable(2).Set(1, DataItem(20));
    parent->GetMutable(5).Set(1, DataItem(50));

    auto derived = std::make_shared<DictVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Set(2, DataItem(51));
    derived->GetMutable(8).Set(3, DataItem(80));

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachDict([&](size_t idx, const Dict& dict) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(20)));
      } else if (idx == 5) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(50)));
        EXPECT_THAT(dict.Get(2), Optional(RefWrap(51)));
      } else if (idx == 8) {
        EXPECT_THAT(dict.Get(3), Optional(RefWrap(80)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5, 8));
  }

  // 4. Derived with parent (Array mode)
  {
    auto parent = std::make_shared<DictVector>(10, /*update_size=*/1);
    parent->GetMutable(2).Set(1, DataItem(20));
    parent->GetMutable(5).Set(1, DataItem(50));

    auto derived = std::make_shared<DictVector>(parent, /*update_size=*/5);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Set(2, DataItem(51));

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachDict([&](size_t idx, const Dict& dict) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(20)));
      } else if (idx == 5) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(50)));
        EXPECT_THAT(dict.Get(2), Optional(RefWrap(51)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5));
  }

  // 5. Derived in Map mode with parent in Array mode
  {
    auto parent = std::make_shared<DictVector>(10, /*update_size=*/5);
    EXPECT_FALSE(DictVectorTestFriend::is_map_mode(*parent));
    parent->GetMutable(2).Set(1, DataItem(20));
    parent->GetMutable(5).Set(1, DataItem(50));

    // Case 5a: derived has empty map (no modifications yet)
    auto derived_empty =
        std::make_shared<DictVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived_empty));
    {
      std::vector<size_t> visited_indices;
      EXPECT_OK(derived_empty->ForEachDict([&](size_t idx, const Dict& dict) {
        visited_indices.push_back(idx);
        if (idx == 2) {
          EXPECT_THAT(dict.Get(1), Optional(RefWrap(20)));
        } else if (idx == 5) {
          EXPECT_THAT(dict.Get(1), Optional(RefWrap(50)));
        }
        return absl::OkStatus();
      }));
      EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5));
    }

    // Case 5b: derived has non-empty map (overriding index 5, adding index 8)
    auto derived = std::make_shared<DictVector>(parent, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(5).Set(2, DataItem(51));
    derived->GetMutable(8).Set(3, DataItem(80));

    std::vector<size_t> visited_indices;
    EXPECT_OK(derived->ForEachDict([&](size_t idx, const Dict& dict) {
      visited_indices.push_back(idx);
      if (idx == 2) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(20)));
      } else if (idx == 5) {
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(50)));
        EXPECT_THAT(dict.Get(2), Optional(RefWrap(51)));
      } else if (idx == 8) {
        EXPECT_THAT(dict.Get(3), Optional(RefWrap(80)));
      }
      return absl::OkStatus();
    }));
    EXPECT_THAT(visited_indices, UnorderedElementsAre(2, 5, 8));
  }

  // 6. DictVector with single_parent_dict (Map mode)
  {
    auto single_parent = std::make_shared<Dict>();
    single_parent->Set(1, DataItem(10));

    auto vec =
        std::make_shared<DictVector>(10, single_parent, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));

    // Case 6a: unmodified vector with single_parent_dict.
    // Every index inherits from single_parent.
    {
      std::vector<size_t> visited_indices;
      EXPECT_OK(vec->ForEachDict([&](size_t idx, const Dict& dict) {
        visited_indices.push_back(idx);
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(10)));
        return absl::OkStatus();
      }));
      EXPECT_THAT(visited_indices, ElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    // Case 6b: with modifications in the vector map.
    vec->GetMutable(2).Set(2, DataItem(20));
    vec->GetMutable(7).Set(3, DataItem(70));
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*vec));

    {
      std::vector<size_t> visited_indices;
      EXPECT_OK(vec->ForEachDict([&](size_t idx, const Dict& dict) {
        visited_indices.push_back(idx);
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(10)));
        if (idx == 2) {
          EXPECT_THAT(dict.Get(2), Optional(RefWrap(20)));
        } else if (idx == 7) {
          EXPECT_THAT(dict.Get(3), Optional(RefWrap(70)));
        }
        return absl::OkStatus();
      }));
      EXPECT_THAT(visited_indices, ElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    // Case 6c: derived vector in Map mode with parent having single_parent_dict
    // (covers seen != nullptr branch).
    auto derived = std::make_shared<DictVector>(vec, /*update_size=*/1);
    EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*derived));
    derived->GetMutable(2).Set(4, DataItem(25));
    derived->GetMutable(5).Set(5, DataItem(55));

    {
      std::vector<size_t> visited_indices;
      EXPECT_OK(derived->ForEachDict([&](size_t idx, const Dict& dict) {
        visited_indices.push_back(idx);
        EXPECT_THAT(dict.Get(1), Optional(RefWrap(10)));
        if (idx == 2) {
          EXPECT_THAT(dict.Get(2), Optional(RefWrap(20)));
          EXPECT_THAT(dict.Get(4), Optional(RefWrap(25)));
        } else if (idx == 5) {
          EXPECT_THAT(dict.Get(5), Optional(RefWrap(55)));
        } else if (idx == 7) {
          EXPECT_THAT(dict.Get(3), Optional(RefWrap(70)));
        }
        return absl::OkStatus();
      }));
      EXPECT_THAT(visited_indices,
                  UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    // Case 6d: empty single_parent_dict means single_parent_dict_ptr_ is null.
    {
      auto empty_parent = std::make_shared<Dict>();
      auto empty_vec =
          std::make_shared<DictVector>(10, empty_parent, /*update_size=*/1);
      EXPECT_TRUE(DictVectorTestFriend::is_map_mode(*empty_vec));
      int count = 0;
      EXPECT_OK(empty_vec->ForEachDict([&](size_t idx, const Dict& dict) {
        ++count;
        return absl::OkStatus();
      }));
      EXPECT_EQ(count, 0);
    }
  }

  // 7. Callback returning absl::Status
  {
    auto vec = std::make_shared<DictVector>(10, /*update_size=*/1);
    vec->GetMutable(1).Set(1, DataItem(10));
    vec->GetMutable(2).Set(1, DataItem(20));
    vec->GetMutable(3).Set(1, DataItem(30));

    // Success case
    int count = 0;
    EXPECT_OK(
        vec->ForEachDict([&](size_t idx, const Dict& dict) -> absl::Status {
          ++count;
          return absl::OkStatus();
        }));
    EXPECT_EQ(count, 3);

    // Early termination on error
    count = 0;
    auto status =
        vec->ForEachDict([&](size_t idx, const Dict& dict) -> absl::Status {
          ++count;
          return absl::InternalError("stop");
        });
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal, "stop"));
    EXPECT_EQ(count, 1);
  }
}

}  // namespace
}  // namespace koladata::internal
