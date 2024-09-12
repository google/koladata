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
#include "koladata/internal/dict.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/missing_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::testing::UnorderedElementsAre;

TEST(DictTest, Dict) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(3);
  (*dicts)[0].Set(DataItem(1), DataItem(7.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(1, DataItem(9)), DataItem(7.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(1), DataItem(9)), DataItem(7.f));
  (*dicts)[0].Set(arolla::Bytes("aaa"), DataItem(arolla::Bytes("bbb")));
  (*dicts)[0].Set(DataItem(1), DataItem(6.f));
  EXPECT_EQ((*dicts)[0].GetOrAssign(1, DataItem(9)), DataItem(6.f));

  const DictVector& const_dicts = *dicts;
  EXPECT_THAT(const_dicts[0].GetKeys(),
              UnorderedElementsAre(1, arolla::Bytes("aaa")));
  EXPECT_THAT(const_dicts[1].GetKeys(), UnorderedElementsAre());
  EXPECT_THAT(const_dicts[2].GetKeys(), UnorderedElementsAre());

  DictVector derived_dicts(dicts);
  derived_dicts[0].Set(arolla::Bytes("aaa"), DataItem(9));
  derived_dicts[0].Set(arolla::Bytes("2"), DataItem(10));

  EXPECT_THAT(
      derived_dicts[0].GetKeys(),
      UnorderedElementsAre(1, arolla::Bytes("2"), arolla::Bytes("aaa")));
  EXPECT_THAT(derived_dicts[1].GetKeys(), UnorderedElementsAre());
  EXPECT_THAT(derived_dicts[2].GetKeys(), UnorderedElementsAre());

  EXPECT_EQ(derived_dicts[0].Get(DataItem(1)), 6.f);
  EXPECT_EQ(derived_dicts[0].GetOrAssign(DataItem(1), DataItem(7.f)), 6.f);
  EXPECT_EQ(derived_dicts[0].Get(1), 6.f);
  EXPECT_EQ(derived_dicts[0].Get(1, DataItem::Hash()(1)), 6.f);
  EXPECT_EQ(derived_dicts[0].Get(arolla::Bytes("aaa")), 9);
  EXPECT_EQ(derived_dicts[0].GetOrAssign(arolla::Bytes("aaa"), DataItem(1)), 9);
  EXPECT_EQ(derived_dicts[0].Get(
                arolla::Bytes("aaa"),
                DataItem::Hash()(DataItem::View<arolla::Bytes>("aaa"))),
            9);
  EXPECT_EQ(derived_dicts[0].Get(arolla::Bytes("2")), 10);
  EXPECT_EQ(derived_dicts[0].GetOrAssign(arolla::Bytes("2"), DataItem(13)), 10);

  EXPECT_EQ(derived_dicts[1].Get(DataItem(1)), DataItem());
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(1), DataItem(57)),
            DataItem(57));
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(53), DataItem(37.f)), 37.f);
  derived_dicts[1].Set(DataItem(53), DataItem());
  EXPECT_EQ(derived_dicts[1].GetOrAssign(DataItem(53), DataItem(39.f)), 39.f);
  EXPECT_EQ(derived_dicts[1].Get(DataItem()), DataItem());
  EXPECT_EQ(derived_dicts[1].Get(MissingValue{}), DataItem());

  derived_dicts[0].Clear();
  EXPECT_THAT(derived_dicts[0].GetKeys(), UnorderedElementsAre());
  EXPECT_EQ(derived_dicts[0].Get(DataItem(1)), DataItem());
  EXPECT_EQ(derived_dicts[0].Get(DataItem(1.f)), DataItem());

  EXPECT_THAT(const_dicts[0].GetKeys(),
              UnorderedElementsAre(1, arolla::Bytes("aaa")));
  EXPECT_EQ(const_dicts[0].Get(DataItem(1)), 6.f);
  EXPECT_EQ(const_dicts[0].Get(arolla::Bytes("2")), DataItem());
  EXPECT_EQ(const_dicts[0].Get(arolla::Bytes("aaa")), arolla::Bytes("bbb"));

  using BytesItemView = DataItem::View<arolla::Bytes>;
  EXPECT_EQ(const_dicts[0].Get(BytesItemView{"aaa"}), BytesItemView{"bbb"});

  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(13), DataItem(7.f)), 7.f);
  (*dicts)[0].Set(DataItem(13), DataItem());
  EXPECT_EQ((*dicts)[0].GetOrAssign(DataItem(13), DataItem(9.f)), 9.f);
}

TEST(DictTest, OverrideWithEmptyNoParent) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& dict = (*dicts)[0];
  dict.Set(DataItem(1), DataItem(5.f));
  dict.Set(DataItem(1), DataItem());
  EXPECT_EQ(dict.Get(1), DataItem());
}

TEST(DictTest, OverrideWithEmptyWithParent) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& parent_dict = (*dicts)[0];
  parent_dict.Set(DataItem(1), DataItem(5.f));
  parent_dict.Set(DataItem(2), DataItem(7.f));

  DictVector derived_dicts(dicts);
  auto& derived_dict = derived_dicts[0];
  derived_dict.Set(1, DataItem());
  EXPECT_EQ(derived_dict.Get(1), DataItem());
  EXPECT_EQ(derived_dict.Get(2), 7.f);

  derived_dict.Set(1, DataItem(9.f));
  EXPECT_EQ(derived_dict.Get(1), 9.f);
}

TEST(DictTest, GetOrAssignWithEmptyNoParent) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& dict = (*dicts)[0];
  EXPECT_EQ(dict.GetOrAssign(DataItem(1), DataItem()), DataItem());
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre());
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 0);

  dict.Set(DataItem(1), DataItem(5.f));
  EXPECT_EQ(dict.GetOrAssign(DataItem(1), DataItem()), DataItem(5.f));
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(DataItem(1)));
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
}

TEST(DictTest, GetOrAssignWithEmptyWithParent) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& parent_dict = (*dicts)[0];
  parent_dict.Set(DataItem(1), DataItem(5.f));
  parent_dict.Set(DataItem(2), DataItem(7.f));

  DictVector derived_dicts(dicts);
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
              UnorderedElementsAre(DataItem(1), DataItem(2)));
  EXPECT_EQ(derived_dict.GetSizeNoFallbacks(), 2);

  parent_dict.Set(DataItem(1), DataItem(9.f));
  parent_dict.Set(DataItem(3), DataItem(2.f));
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(9.f));
  // repeat to be sure we do not override
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(1), DataItem()), DataItem(9.f));
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(3), DataItem()), DataItem(2.f));
  // repeat to be sure we do not override
  EXPECT_EQ(derived_dict.GetOrAssign(DataItem(3), DataItem()), DataItem(2.f));
  EXPECT_THAT(derived_dict.GetKeys(),
              UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(3)));
  EXPECT_EQ(derived_dict.GetSizeNoFallbacks(), 3);
}

TEST(DictTest, DerivedDictExtra) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& dict = (*dicts)[0];
  dict.Set(arolla::Text("a"), DataItem(7.f));
  dict.Set(1, DataItem(8));

  std::shared_ptr<DictVector> derived_dicts =
      std::make_shared<DictVector>(std::move(dicts));
  auto& derived_dict = (*derived_dicts)[0];
  derived_dict.Set(1, DataItem(9));
  derived_dict.Set(2, DataItem(10));

  EXPECT_THAT(derived_dict.GetKeys(),
              UnorderedElementsAre(1, 2, arolla::Text("a")));

  EXPECT_EQ(derived_dict.Get(arolla::Text("a")), 7.f);
  EXPECT_EQ(derived_dict.Get(1), 9);
  EXPECT_EQ(derived_dict.Get(2), 10);

  std::shared_ptr<DictVector> derived_dicts2 =
      std::make_shared<DictVector>(std::move(derived_dicts));
  auto& derived_dict2 = (*derived_dicts2)[0];
  derived_dict2.Set(0, DataItem(5));
  derived_dict2.Set(2, DataItem(7));

  EXPECT_THAT(derived_dict2.GetKeys(),
              UnorderedElementsAre(0, 1, 2, arolla::Text("a")));

  EXPECT_EQ(derived_dict2.Get(arolla::Text("a")), 7.f);
  EXPECT_EQ(derived_dict2.Get(0), 5);
  EXPECT_EQ(derived_dict2.Get(1), 9);
  EXPECT_EQ(derived_dict2.Get(2), 7);

  derived_dict2.Clear();
  EXPECT_THAT(derived_dict2.GetKeys(), UnorderedElementsAre());
  EXPECT_EQ(derived_dict2.Get(arolla::Text("a")), DataItem());
  EXPECT_EQ(derived_dict2.Get(0), DataItem());
  EXPECT_EQ(derived_dict2.Get(1), DataItem());
  EXPECT_EQ(derived_dict2.Get(2), DataItem());
}

TEST(DictTest, DerivedDictSingle) {
  std::shared_ptr<Dict> parent_dict = std::make_shared<Dict>();
  parent_dict->Set(arolla::Text("a"), DataItem(7.f));
  parent_dict->Set(1, DataItem(8));

  DictVector derived_dicts(4, parent_dict);
  parent_dict.reset();  // verify ownership
  {
    auto& derived_dict = derived_dicts[0];
    derived_dict.Set(1, DataItem(9));
    derived_dict.Set(2, DataItem(10));

    EXPECT_THAT(derived_dict.GetKeys(),
                UnorderedElementsAre(1, 2, arolla::Text("a")));

    EXPECT_EQ(derived_dict.Get(arolla::Text("a")), 7.f);
    EXPECT_EQ(derived_dict.Get(1), 9);
    EXPECT_EQ(derived_dict.Get(2), 10);
  }
  {
    auto& derived_dict = derived_dicts[1];

    EXPECT_THAT(derived_dict.GetKeys(),
                UnorderedElementsAre(1, arolla::Text("a")));

    EXPECT_EQ(derived_dict.Get(arolla::Text("a")), 7.f);
    EXPECT_EQ(derived_dict.Get(1), 8);
  }
}

TEST(DictTest, GetKeysWithFallback) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& dict = (*dicts)[0];
  dict.Set(arolla::Text("a"), DataItem(7.f));
  dict.Set(1, DataItem(8));

  DictVector derived_dicts(dicts);
  auto& derived_dict = derived_dicts[0];
  derived_dict.Set(1, DataItem(9));
  derived_dict.Set(2, DataItem(10));

  std::shared_ptr<DictVector> fb_dicts = std::make_shared<DictVector>(1);
  auto& fb_dict = (*fb_dicts)[0];
  fb_dict.Set(arolla::Text("b"), DataItem(7.f));
  fb_dict.Set(1, DataItem(2));

  EXPECT_THAT(derived_dict.GetKeys({&fb_dict}),
              UnorderedElementsAre(1, 2, arolla::Text("a"), arolla::Text("b")));

  DictVector fb_derived_dicts(fb_dicts);
  auto& fb_derived_dict = fb_derived_dicts[0];
  fb_derived_dict.Set(3, DataItem(7));
  fb_derived_dict.Set(2, DataItem(0));

  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));

  derived_dict.Set(2, DataItem());
  // 2 is still in fallback
  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));

  derived_dict.Set(1, DataItem());
  // 1 is still in fallback, value in parent dict must be ignored
  EXPECT_THAT(
      derived_dict.GetKeys({&fb_derived_dict}),
      UnorderedElementsAre(1, 2, 3, arolla::Text("a"), arolla::Text("b")));
}

TEST(DictTest, GetKeysWithFallbackEmptyMain) {
  std::shared_ptr<DictVector> dicts = std::make_shared<DictVector>(1);
  auto& dict = (*dicts)[0];
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre());

  std::shared_ptr<DictVector> fb_dicts = std::make_shared<DictVector>(1);
  auto& fb_dict = (*fb_dicts)[0];
  fb_dict.Set(1, DataItem(2));

  EXPECT_THAT(dict.GetKeys({&fb_dict}), UnorderedElementsAre(1));

  std::shared_ptr<DictVector> fb2_dicts = std::make_shared<DictVector>(1);
  auto& fb2_dict = (*fb2_dicts)[0];

  EXPECT_THAT(dict.GetKeys({&fb2_dict}), UnorderedElementsAre());
  EXPECT_THAT(dict.GetKeys({&fb2_dict, &fb_dict}), UnorderedElementsAre(1));
  EXPECT_THAT(dict.GetKeys({&fb_dict, &fb2_dict}), UnorderedElementsAre(1));
}

TEST(DictTest, IntegerKeyTypes) {
  Dict dict;
  dict.Set(int{1}, DataItem(1));
  dict.Set(int64_t{1}, DataItem(2));
  EXPECT_EQ(dict.Get(int{1}), 2);
  EXPECT_EQ(dict.Get(int64_t{1}), 2);
}

TEST(DictTest, GetKeysOnMissing) {
  Dict dict;
  dict.Set(int64_t{1}, DataItem());
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre());
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 0);
  dict.Set(int64_t{1}, DataItem(3));
  EXPECT_THAT(dict.GetKeys(), UnorderedElementsAre(1));
  EXPECT_EQ(dict.GetSizeNoFallbacks(), 1);
}

}  // namespace
}  // namespace koladata::internal
