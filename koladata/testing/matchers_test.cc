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
#include "koladata/testing/matchers.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"

namespace koladata {
namespace {

using ::koladata::testing::IsEquivalentTo;
using ::testing::MatchesRegex;
using ::testing::Not;
using ::testing::StringMatchResultListener;

template <typename MatcherType, typename Value>
std::string Explain(const MatcherType& m, const Value& x) {
  StringMatchResultListener listener;
  ExplainMatchResult(m, x, &listener);
  return listener.str();
}

TEST(MatchersTest, IsEquivalentTo) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto objects = internal::DataSliceImpl::AllocateEmptyObjects(shape.size());
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::Create(objects, shape, internal::DataItem(schema::kAny)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_2,
      DataSlice::Create(objects, shape, internal::DataItem(schema::kObject)));

  EXPECT_THAT(ds_1, IsEquivalentTo(ds_1));
  EXPECT_THAT(ds_1, Not(IsEquivalentTo(ds_2)));

  auto m = IsEquivalentTo(ds_1);
  // The DataSlice repr should be detailed enough for debugging, and should
  // avoid overly simplified representations.
  EXPECT_THAT(
      ::testing::DescribeMatcher<DataSlice>(m),
      MatchesRegex(R"(is equivalent to DataSlice\(.*schema.*shape.*\))"));
  EXPECT_THAT(
      ::testing::DescribeMatcher<DataSlice>(m, /*negation=*/true),
      MatchesRegex(R"(is not equivalent to DataSlice\(.*schema.*shape.*\))"));
  EXPECT_THAT(
      Explain(m, ds_1),
      MatchesRegex(R"(DataSlice\(.*schema.*shape.*\) which is equivalent)"));
  EXPECT_THAT(Explain(m, ds_2),
              MatchesRegex(
                  R"(DataSlice\(.*schema.*shape.*\) which is not equivalent)"));
}

TEST(MatchersTest, Printing) {
  // NOTE: go/gunitadvanced#teaching-googletest-how-to-print-your-values.
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto objects = internal::DataSliceImpl::AllocateEmptyObjects(shape.size());
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::Create(objects, shape, internal::DataItem(schema::kAny)));
  EXPECT_THAT(::testing::PrintToString(ds_1),
              MatchesRegex(R"(DataSlice\(.*schema.*shape.*\))"));
}

}  // namespace
}  // namespace koladata
