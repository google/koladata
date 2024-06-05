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
#include "koladata/internal/testing/matchers.h"

#include <cstdint>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"

namespace koladata::internal {

using ::koladata::internal::testing::DataItemWith;
using ::koladata::internal::testing::IsEquivalentTo;
using ::koladata::internal::testing::MissingDataItem;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::Not;
using ::testing::StringMatchResultListener;

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(MatchersTest, DataItemWith) {
  EXPECT_THAT(DataItem(57), DataItemWith<int>(57));
  EXPECT_THAT(DataItem(57), DataItemWith<int>(Eq(57)));
  EXPECT_THAT(DataItem(57), DataItemWith<int>(Gt(50)));
  EXPECT_THAT(DataItem(int64_t{57}), DataItemWith<int64_t>(57));
  EXPECT_THAT(DataItem(), Not(DataItemWith<int64_t>(57)));

  auto m = DataItemWith<int32_t>(57);
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m),
              Eq("stores value of type `int` that is equal to 57"));
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m, /*negation=*/true),
              Eq("doesn't store a value of type `int` or stores a value that "
                 "isn't equal to 57"));
  EXPECT_THAT(Explain(m, DataItem(57.5)),
              Eq("stores a value with dtype FLOAT64 which does not match C++ "
                 "type `int`"));
  EXPECT_THAT(Explain(m, DataItem(58)), Eq("the value is 58"));
}

TEST(MatchersTest, MissingDataItem) {
  EXPECT_THAT(DataItem(), MissingDataItem());
  EXPECT_THAT(DataItem(57), Not(MissingDataItem()));

  auto m = MissingDataItem();
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m), Eq("is missing"));
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m, /*negation=*/true),
              Eq("is not missing"));
  EXPECT_THAT(Explain(m, DataItem(57)), Eq("is not missing, contains 57"));
}

TEST(MatchersTest, IsEquivalentTo_DataItem) {
  EXPECT_THAT(DataItem(57), IsEquivalentTo(DataItem(57)));
  EXPECT_THAT(DataItem(57), Not(IsEquivalentTo(DataItem(arolla::Text{"foo"}))));

  auto m = IsEquivalentTo(DataItem(57));
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m),
              Eq("is equivalent to 57"));
  EXPECT_THAT(::testing::DescribeMatcher<DataItem>(m, /*negation=*/true),
              Eq("is not equivalent to 57"));
  EXPECT_THAT(Explain(m, DataItem(57)), Eq("which is equivalent"));
  EXPECT_THAT(Explain(m, DataItem(arolla::Text{"foo"})),
              Eq("which is not equivalent"));
}

TEST(MatchersTest, IsEquivalentTo_DataSliceImpl) {
  auto ds1 =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<int>({1, 2, 3}));
  auto ds2 = DataSliceImpl::Create(arolla::CreateFullDenseArray<arolla::Text>(
      {arolla::Text{"foo"}, arolla::Text{"bar"}}));

  EXPECT_THAT(ds1, IsEquivalentTo(ds1));
  EXPECT_THAT(ds1, Not(IsEquivalentTo(ds2)));

  auto m = IsEquivalentTo(ds1);
  EXPECT_THAT(::testing::DescribeMatcher<DataSliceImpl>(m),
              Eq("is equivalent to [1, 2, 3]"));
  EXPECT_THAT(::testing::DescribeMatcher<DataSliceImpl>(m, /*negation=*/true),
              Eq("is not equivalent to [1, 2, 3]"));
  EXPECT_THAT(Explain(m, ds1), Eq("which is equivalent"));
  EXPECT_THAT(Explain(m, ds2), Eq("which is not equivalent"));
}

}  // namespace koladata::internal
