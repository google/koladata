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
#include "koladata/data_bag_repr.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"

namespace koladata {
namespace {

using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::OptionalValue;
using ::koladata::internal::ObjectId;
using ::koladata::testing::IsOkAndHolds;
using ::testing::MatchesRegex;

absl::StatusOr<DenseArrayEdge> EdgeFromSplitPoints(
    absl::Span<const OptionalValue<int64_t>> split_points) {
  return DenseArrayEdge::FromSplitPoints(
      CreateDenseArray<int64_t>(split_points));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_Entities) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");
  ASSERT_OK(EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}:0\.a => 1(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}:0\.b => b(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}:0\.a => INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}:0\.b => TEXT(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_Objects) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");
  ASSERT_OK(ObjectCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}:0\.__schema__ => k[0-9a-f]{32}:0(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}:0\.a => 1(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}:0\.b => b(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*k[0-9a-f]{32}:0\.a => INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*k[0-9a-f]{32}:0\.b => TEXT(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_Dicts) {
  DataBagPtr bag = DataBag::Empty();

  ObjectId dict_id = internal::AllocateSingleDict();
  DataSlice data_slice = test::DataItem(dict_id, schema::kAny, bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<int>({1, 4});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(
          AllOf(MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
                MatchesRegex(
                    R"regex((.|\n)*\$[0-9a-f]{32}:0\['a'\] => 1(.|\n)*)regex"),
                MatchesRegex(
                    R"regex((.|\n)*\$[0-9a-f]{32}:0\['x'\] => 4(.|\n)*)regex"),
                MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_List) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK(CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                             test::Schema(schema::kAny)));
  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}:0\[:\] => \[1, 2, 3\](.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_FallbackBags) {
  auto fallback_db1 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds1, EntityCreator::FromAttrs(fallback_db1, {"a"},
                                         {test::DataItem(42, fallback_db1)}));

  auto fallback_db2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds2, EntityCreator::FromAttrs(fallback_db2, {"b"},
                                         {test::DataItem(123, fallback_db2)}));

  auto db = DataBag::ImmutableEmptyWithFallbacks({fallback_db1, fallback_db2});
  auto ds3 = ds1.WithDb(db);

  EXPECT_THAT(
      DataBagToStr(ds3.GetDb()),
      IsOkAndHolds(MatchesRegex(
          R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*SchemaBag:(.|\n)*2 fallback DataBag\(s\):(.|\n)*  fallback #0 \$[0-9a-f]{4}:(.|\n)*  DataBag:(.|\n)*  \$[0-9a-f]{32}:0\.a => 42(.|\n)*  SchemaBag:(.|\n)*  \$[0-9a-f]{32}:0\.a => INT32(.|\n)*  fallback #1 \$[0-9a-f]{4}:(.|\n)*  DataBag:(.|\n)*  \$[0-9a-f]{32}:0\.b => 123(.|\n)*  SchemaBag:(.|\n)*  \$[0-9a-f]{32}:0\.b => INT32(.|\n)*)regex")));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_DuplicatedFallbackBags) {
  auto fallback_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds1, EntityCreator::FromAttrs(
                                     fallback_db, {"a"}, {test::DataItem(42)}));
  auto db = DataBag::ImmutableEmptyWithFallbacks({fallback_db, fallback_db});
  EXPECT_THAT(
      DataBagToStr(db),
      IsOkAndHolds(MatchesRegex(
          R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*SchemaBag:(.|\n)*2 fallback DataBag\(s\):(.|\n)*  fallback #0 \$[0-9a-f]{4}:(.|\n)*  DataBag:(.|\n)*  \$[0-9a-f]{32}:0\.a => 42(.|\n)*  SchemaBag:(.|\n)*  fallback #1 duplicated, see db with id: \$[0-9a-f]{4})regex")));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_ListSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 4}));
  internal::DataSliceImpl ds =
      internal::DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3, 4}));
  ASSERT_OK_AND_ASSIGN(
      auto ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));
  ASSERT_OK_AND_ASSIGN(DataSlice nested_list,
                       DataSlice::Create(std::move(ds), std::move(ds_shape),
                                         internal::DataItem(schema::kInt32)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(nested_list),
                       /*schema=*/std::nullopt, /*item_schema=*/std::nullopt));

  ASSERT_OK(EntityCreator::FromAttrs(bag, {"a"}, {data_slice}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(MatchesRegex(
          R"regex((\n|.)*\$[0-9a-f]{32}:0\.a => list<list<INT32>>(\n|.)*)regex")));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_DictSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice dict,
      CreateDictShaped(bag, JaggedDenseArrayShape::Empty(), test::DataItem(114),
                       test::DataItem(514)));
  ASSERT_OK_AND_ASSIGN(DataSlice nested_dict,
                       CreateDictShaped(bag, JaggedDenseArrayShape::Empty(),
                                        test::DataItem(114), dict));
  ASSERT_OK(EntityCreator::FromAttrs(bag, {"dudulu"}, {nested_dict}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(MatchesRegex(
          R"regex((\n|.)*\$[0-9a-f]{32}:0\.dudulu => k[0-9a-f]{32}:0\[dict<INT32, k[0-9a-f]{32}:0\[dict<INT32, INT32>\]>\](\n|.)*)regex")));
}

TEST(DataBagReprTest, TestDataBagStringRepresentation_SchemaCycle) {
  DataBagPtr bag = DataBag::Empty();
  DataSlice key_item = test::DataItem(114);

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, JaggedDenseArrayShape::Empty(),
                                        key_item, test::DataItem(514)));

  DataSlice schema = dict.GetSchema();
  ASSERT_OK(schema.SetAttr(schema::kDictValuesSchemaAttr, dict.GetSchema()));
  ASSERT_OK(dict.SetInDict(key_item, dict));

  ASSERT_OK(EntityCreator::FromAttrs(bag, {"dudulu"}, {dict}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(MatchesRegex(
          R"regex((\n|.)*\$[0-9a-f]{32}:0\.dudulu => k[0-9a-f]{32}:0\[dict<INT32, k[0-9a-f]{32}:0\[dict<INT32, k[0-9a-f]{32}:0\[dict<INT32, k[0-9a-f]{32}:0\[dict<INT32, k[0-9a-f]{32}:0\[dict<INT32, \.\.\.>\]>\]>\]>\]>\](\n|.)*)regex")));
}

TEST(DataBagReprTest, TestDataBagStatistics_Dict) {
  DataBagPtr bag = DataBag::Empty();

  JaggedDenseArrayShape shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(
      DataSlice dicts,
      CreateDictShaped(bag, shape, /*keys=*/test::DataSlice<int>({1, 2, 3}),
                       /*values=*/test::DataSlice<int64_t>({57, 58, 59})));

  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 3 values in 3 attrs, plus 2 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*<dict value>: 1 values(.|\n)*<dict value>: 1 values(.|\n)*<dict value>: 1 values(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Use db\.contents_repr\(\) to see the actual values\.)regex"))));
}

TEST(DataBagReprTest, TestDataBagStatistics_TwoDicts) {
  DataBagPtr bag = DataBag::Empty();

  JaggedDenseArrayShape shape = DataSlice::JaggedShape::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice dict1,
      CreateDictShaped(bag, shape, /*keys=*/test::DataSlice<int>({1, 3}),
                       /*values=*/test::DataSlice<int64_t>({2, 4})));

  ASSERT_OK_AND_ASSIGN(
      DataSlice dict2,
      CreateDictShaped(bag, shape, /*keys=*/test::DataSlice<int>({3, 5}),
                       /*values=*/test::DataSlice<int64_t>({5, 6})));

  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 4 values in 3 attrs, plus 2 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*<dict value>: 2 values(.|\n)*<dict value>: 1 values(.|\n)*<dict value>: 1 values(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*Use db\.contents_repr\(\) to see the actual values\.)regex"))));
}

TEST(DataBagReprTest, TestDataBagStatistics_Entity) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataSlice<int>({1, std::nullopt});
  DataSlice value_2 = test::DataSlice<int>({2, 3});
  ASSERT_OK(EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 3 values in 2 attrs, plus 2 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*b: 2 values(.|\n)*a: 1 values(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStatistics_Object) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataSlice<int>({1, std::nullopt, std::nullopt});
  DataSlice value_2 = test::DataSlice<int>({2, 3, std::nullopt});
  ASSERT_OK(ObjectCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 6 values in 3 attrs, plus 6 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*<object schemas>: 3 values(.|\n)*b: 2 values(.|\n)*a: 1 values(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStatistics_NestedList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 4}));
  internal::DataSliceImpl ds =
      internal::DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3, 4}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShape ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));
  ASSERT_OK_AND_ASSIGN(DataSlice nested_list,
                       DataSlice::Create(std::move(ds), std::move(ds_shape),
                                         internal::DataItem(schema::kAny)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(nested_list),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));

  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 6 values in 1 attrs, plus 2 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*<list items>: 6 values(.|\n)*)regex"))));
}

TEST(DataBagReprTest, TestDataBagStatistics_List) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  EXPECT_THAT(
      DataBagStatistics(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(
              R"regex(DataBag \$[0-9a-f]{4} with 3 values in 1 attrs, plus 1 schema values and 0 fallbacks\. Top attrs:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*<list items>: 3 values(.|\n)*)regex"))));
}
}  // namespace
}  // namespace koladata
