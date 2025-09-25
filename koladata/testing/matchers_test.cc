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
#include "koladata/testing/matchers.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"

namespace koladata {
namespace {

using ::koladata::testing::IsEquivalentTo;
using ::koladata::testing::IsDeepEquivalentTo;
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
  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::Create(objects, shape, internal::DataItem(explicit_schema)));
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

TEST(MatchersTest, IsDeepEquivalentTo) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto objects = internal::DataSliceImpl::AllocateEmptyObjects(shape.size());
  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::Create(objects, shape, internal::DataItem(explicit_schema)));
  ASSERT_OK_AND_ASSIGN(
      auto itemid_slice,
      DataSlice::Create(objects, shape, internal::DataItem(schema::kItemId)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_2,
      ObjectCreator::FromAttrs(DataBag::EmptyMutable(), {}, {}, itemid_slice));

  EXPECT_THAT(ds_1, IsDeepEquivalentTo(ds_1));
  EXPECT_THAT(ds_1, IsDeepEquivalentTo(ds_2));
  EXPECT_THAT(ds_1, Not(IsDeepEquivalentTo(ds_2, {.schemas_equality = true})));
  EXPECT_THAT(ds_1, IsDeepEquivalentTo(ds_1, {.schemas_equality = true}));

  auto m = IsDeepEquivalentTo(ds_1);
  auto m_strict = IsDeepEquivalentTo(ds_1, {.schemas_equality = true});
  // The DataSlice repr should be detailed enough for debugging, and should
  // avoid overly simplified representations.
  EXPECT_THAT(
      ::testing::DescribeMatcher<DataSlice>(m),
      MatchesRegex(R"(is deep equivalent to DataSlice\(.*schema.*shape.*\))"));
  EXPECT_THAT(
      ::testing::DescribeMatcher<DataSlice>(m, /*negation=*/true),
      MatchesRegex(
          R"(is not deep equivalent to DataSlice\(.*schema.*shape.*\))"));
  EXPECT_THAT(
      Explain(m, ds_1),
      MatchesRegex(R"(DataSlice\(.*schema.*shape.*\) which is equivalent)"));
  EXPECT_THAT(
      Explain(m_strict, ds_2),
      MatchesRegex(
          R"regex(Expected: is equal to DataSlice\(.*\)
Actual: DataSlice\(.*\), with difference:(.|\n)*
modified schema:
expected\.S\[0\]:
DataItem\(ENTITY\(\), schema: SCHEMA.*\)
-> actual\.S\[0\]:
DataItem\(OBJECT, schema: SCHEMA.*\)(.|\n)*)regex"));
}

TEST(MatchersTest, Printing) {
  // NOTE: go/gunitadvanced#teaching-googletest-how-to-print-your-values.
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto objects = internal::DataSliceImpl::AllocateEmptyObjects(shape.size());
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::Create(objects, shape, internal::DataItem(schema::kObject)));
  EXPECT_THAT(::testing::PrintToString(ds_1),
              MatchesRegex(R"(DataSlice\(.*schema.*shape.*\))"));
}

}  // namespace
}  // namespace koladata
