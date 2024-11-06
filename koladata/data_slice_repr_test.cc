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
#include "koladata/data_slice_repr.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status_matchers.h"
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
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::OptionalValue;
using ::koladata::internal::ObjectId;
using ::testing::MatchesRegex;
using ::testing::StrEq;

absl::StatusOr<DenseArrayEdge> EdgeFromSplitPoints(
    absl::Span<const OptionalValue<int64_t>> split_points) {
  return DenseArrayEdge::FromSplitPoints(
      CreateDenseArray<int64_t>(split_points));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_Primitives) {
  DataSlice item = test::DataItem(1);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds(StrEq("1")));

  DataSlice item2 = test::DataItem(arolla::kUnit);
  EXPECT_THAT(DataSliceToStr(item2), IsOkAndHolds(StrEq("present")));

  DataSlice item3 = test::DataItem(std::nullopt, schema::kMask);
  EXPECT_THAT(DataSliceToStr(item3), IsOkAndHolds(StrEq("missing")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Dict) {
  DataBagPtr bag = DataBag::Empty();
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, schema::kAny, bag);
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds(StrEq("Dict{}")));

  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<int>({1, 4});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(MatchesRegex(
                  R"regexp(Dict\{('x'=4, 'a'=1|'a'=1, 'x'=4)\})regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Dict_Text) {
  DataBagPtr bag = DataBag::Empty();
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, schema::kAny, bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<arolla::Text>({"a", "4"});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(
      DataSliceToStr(data_slice),
      IsOkAndHolds(MatchesRegex(
          R"regexp(Dict\{('x'='4', 'a'='a'|'a'='a', 'x'='4')\})regexp")));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_NestedDict) {
  DataBagPtr bag = DataBag::Empty();
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice dict = test::DataItem(dict_id, schema::kAny, bag);
  DataSlice key = test::DataSlice<arolla::Text>({"a"});
  DataSlice value = test::DataSlice<int>({1});
  ASSERT_OK(dict.SetInDict(key, value));

  ObjectId second_dict_id = internal::AllocateSingleDict();
  DataSlice second_key = test::DataSlice<arolla::Text>({"x"});
  DataSlice second_dict = test::DataItem(second_dict_id, schema::kAny, bag);
  ASSERT_OK(second_dict.SetInDict(second_key, dict));

  EXPECT_THAT(DataSliceToStr(second_dict),
              IsOkAndHolds("Dict{'x'=Dict{'a'=1}}"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_List) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));
  EXPECT_THAT(DataSliceToStr(empty_list), IsOkAndHolds(StrEq("List[]")));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds(StrEq("List[1, 2, 3]")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_List_Text) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<arolla::Text>({"a", "b", "c"}),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List['a', 'b', 'c']")));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_NestedList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 1, 3}));
  internal::DataSliceImpl ds =
      internal::DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
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
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List[List[1], List[2, 3]]")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_DictInList) {
  DataBagPtr bag = DataBag::Empty();

  internal::DataSliceImpl dict_slice_impl =
      internal::DataSliceImpl::ObjectsFromAllocation(internal::AllocateDicts(3),
                                                     3);
  ASSERT_OK_AND_ASSIGN(
      DataSlice dict_slice,
      DataSlice::Create(std::move(dict_slice_impl),
                        JaggedDenseArrayShape::FlatFromSize(3),
                        internal::DataItem(schema::kAny), bag));

  DataSlice keys = test::DataItem("a");
  DataSlice values = test::DataItem(1);
  ASSERT_OK(dict_slice.SetInDict(keys, values));

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 1, 3}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShape ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(dict_slice), /*schema=*/std::nullopt,
                       test::Schema(schema::kAny)));
  EXPECT_THAT(
      DataSliceToStr(data_slice),
      IsOkAndHolds(StrEq("List[Dict{'a'=1}, Dict{'a'=1}, Dict{'a'=1}]")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_ObjectsInList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List[Obj(a=1), Obj(a=2)]")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_EntitiesInList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List[Entity(a=1), Entity(a=2)]")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Object) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_obj,
                       ObjectCreator::FromAttrs(bag, {}, {}));
  EXPECT_THAT(
      DataSliceToStr(empty_obj),
      IsOkAndHolds(MatchesRegex(R"regex(Obj\(\):\$[0-9a-zA-Z]{22})regex")));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice obj, ObjectCreator::FromAttrs(
                                          bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(obj), IsOkAndHolds("Obj(a=1, b='b')"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Entity) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_entity,
                       EntityCreator::FromAttrs(bag, {}, {}));
  EXPECT_THAT(
      DataSliceToStr(empty_entity),
      IsOkAndHolds(MatchesRegex(R"regex(Entity\(\):\$[0-9a-zA-Z]{22})regex")));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(entity), IsOkAndHolds("Entity(a=1, b='b')"));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_ListSchema) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice list_item = test::DataItem(1);

  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list_item));

  EXPECT_THAT(DataSliceToStr(list.GetSchema()),
              IsOkAndHolds(StrEq("LIST[INT32]")));

  ASSERT_OK_AND_ASSIGN(
      DataSlice list_of_list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list));

  EXPECT_THAT(DataSliceToStr(list_of_list.GetSchema()),
              IsOkAndHolds(StrEq("LIST[LIST[INT32]]")));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_DictSchema) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  EXPECT_THAT(DataSliceToStr(dict.GetSchema()),
              IsOkAndHolds(StrEq("DICT{INT32, STRING}")));

  DataSlice second_key_item = test::DataItem("foo");
  DataSlice second_value_item =
      test::DataItem(internal::AllocateSingleObject());
  ASSERT_OK_AND_ASSIGN(DataSlice second_dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        second_key_item, second_value_item));

  ASSERT_OK_AND_ASSIGN(DataSlice nested_dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        dict, second_dict));

  EXPECT_THAT(
      DataSliceToStr(nested_dict.GetSchema()),
      IsOkAndHolds(StrEq("DICT{DICT{INT32, STRING}, DICT{STRING, OBJECT}}")));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_ExplicitSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_entity,
                       EntityCreator::FromAttrs(bag, {}, {}));
  EXPECT_THAT(DataSliceToStr(empty_entity.GetSchema()),
              IsOkAndHolds("SCHEMA()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(entity.GetSchema()),
              IsOkAndHolds("SCHEMA(a=INT32, b=STRING)"));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_ExplicitSchema_Nested) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");
  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {dict}));
  EXPECT_THAT(DataSliceToStr(entity.GetSchema()),
              IsOkAndHolds("SCHEMA(a=DICT{INT32, STRING})"));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_ImplicitSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_obj,
                       ObjectCreator::FromAttrs(bag, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto schema, empty_obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds("IMPLICIT_SCHEMA()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice obj, ObjectCreator::FromAttrs(
                                          bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(DataSliceToStr(obj.GetSchema()), IsOkAndHolds("OBJECT"));
  ASSERT_OK_AND_ASSIGN(schema, obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("IMPLICIT_SCHEMA(a=INT32, b=STRING)"));
}

TEST(DataSliceReprTest, TestItemStringRepresentation_NoBag) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  entity = entity.WithBag(/*db=*/nullptr);

  EXPECT_THAT(DataSliceToStr(entity),
              IsOkAndHolds(MatchesRegex(R"regex(\$[0-9a-zA-Z]{22})regex")));
}

TEST(DataSliceReprTest, TestItemStringReprWithFallbackDB) {
  DataSlice ds_a = test::DataItem(1);

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds, EntityCreator::FromAttrs(db, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kAny)));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  DataBagPtr db2 = DataBag::Empty();
  ds = ds.WithBag(db2);
  ASSERT_OK(ds.GetSchema().SetAttr("b", test::Schema(schema::kAny)));
  DataSlice ds_b = test::DataItem(2);
  ASSERT_OK(ds.SetAttr("b", ds_b));

  db = DataBag::ImmutableEmptyWithFallbacks({db, db2});
  ds = ds.WithBag(db);

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("Entity(a=1, b=2)"));
  EXPECT_THAT(DataSliceToStr(ds.GetSchema()),
              IsOkAndHolds(("SCHEMA(a=ANY, b=ANY)")));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_Primitives) {
  DataSlice ds = test::DataSlice<int>({1, 2, 3});

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[1, 2, 3]"));

  DataSlice ds2 = test::DataSlice<int>({1, std::nullopt, 3});
  EXPECT_THAT(DataSliceToStr(ds2), IsOkAndHolds("[1, None, 3]"));

  DataSlice ds3 = test::DataSlice<arolla::Unit>(
      {arolla::kUnit, std::nullopt, arolla::kUnit});
  EXPECT_THAT(DataSliceToStr(ds3), IsOkAndHolds("[present, missing, present]"));

  DataSlice ds4 = test::MixedDataSlice<int, arolla::Unit>(
      {1, std::nullopt, std::nullopt},
      {std::nullopt, arolla::kUnit, std::nullopt});
  EXPECT_THAT(DataSliceToStr(ds4), IsOkAndHolds("[1, present, None]"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_EntitySlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       EntityCreator::FromAttrs(db, {"a"}, {values}));
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Entity:\$[0-9a-zA-Z]{22}, Entity:\$[0-9a-zA-Z]{22}\])regex")));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_ObjectSlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       ObjectCreator::FromAttrs(db, {"a"}, {values}));
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Obj:\$[0-9a-zA-Z]{22}, Obj:\$[0-9a-zA-Z]{22}\])regex")));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_ListSlices) {
  ObjectId list_id_1 = internal::AllocateSingleList();
  ObjectId list_id_2 = internal::AllocateSingleList();
  DataSlice ds = test::DataSlice<ObjectId>({list_id_1, list_id_2});

  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[List:\$[0-9a-zA-Z]{22}, List:\$[0-9a-zA-Z]{22}\])regex")));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_DictSlices) {
  ObjectId dict_id_1 = internal::AllocateSingleDict();
  ObjectId dict_id_2 = internal::AllocateSingleDict();

  DataSlice ds = test::DataSlice<ObjectId>({dict_id_1, dict_id_2});
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Dict:\$[0-9a-zA-Z]{22}, Dict:\$[0-9a-zA-Z]{22}\])regex")));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_TwoDimensionsSlice) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 3}));
  ASSERT_OK_AND_ASSIGN(
      auto ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  DataSlice ds = test::DataSlice<int>({1, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[1, 2], [3]]"));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_ThreeDimensionsSlice) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 3}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge3, EdgeFromSplitPoints({0, 2, 3, 4}));
  ASSERT_OK_AND_ASSIGN(auto ds_shape, JaggedDenseArrayShape::FromEdges(
                                          {std::move(edge1), std::move(edge2),
                                           std::move(edge3)}));

  DataSlice ds =
      test::DataSlice<int>({1, std::nullopt, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[[1, None], [2]], [[3]]]"));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_SameChildSizeEdges) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 5}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge3,
                       EdgeFromSplitPoints({0, 1, 2, 3, 4, 5}));
  ASSERT_OK_AND_ASSIGN(auto ds_shape, JaggedDenseArrayShape::FromEdges(
                                          {std::move(edge1), std::move(edge2),
                                           std::move(edge3)}));

  EXPECT_THAT(DataSliceToStr(
                  test::DataSlice<int>({1, 2, 3, 4, 5}, std::move(ds_shape))),
              IsOkAndHolds("[[[1], [2]], [[3], [4], [5]]]"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_SplitLines) {
  std::vector<arolla::OptionalValue<int64_t>> input;
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      input.emplace_back(j);
    }
  }

  std::vector<arolla::OptionalValue<int64_t>> edge2_input;
  for (int i = 0; i <= 10; ++i) {
    edge2_input.emplace_back(i * 10);
  }

  arolla::DenseArray<int64_t> edge2_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(edge2_input));

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 10}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(std::move(edge2_array)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       DataSlice::CreateWithSchemaFromData(
                           internal::DataSliceImpl::Create(std::move(ds_array)),
                           std::move(ds_shape)));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds(R"([
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
])"));
}

TEST(DataSliceReprTest, TestStringRepresentation_NoFollow) {
  DataBagPtr bag = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"y"}, {test::DataItem<int>(1)}));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_entity, NoFollow(entity));

  EXPECT_THAT(DataSliceToStr(nofollow_entity),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Nofollow\(Entity:\$[0-9a-zA-Z]{22}\))regex")));

  EXPECT_THAT(
      DataSliceToStr(nofollow_entity.GetSchema()),
      IsOkAndHolds(MatchesRegex(R"regex(NOFOLLOW\(\$[0-9a-zA-Z]{22}\))regex")));

  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"y"}, {test::DataItem<int>(1)}));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_obj, NoFollow(obj));

  EXPECT_THAT(DataSliceToStr(nofollow_obj),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Nofollow\(Entity:\$[0-9a-zA-Z]{22}\))regex")));
  EXPECT_THAT(DataSliceToStr(nofollow_obj.GetSchema()),
              IsOkAndHolds("NOFOLLOW(OBJECT)"));

  ASSERT_OK_AND_ASSIGN(
      DataSlice dict,
      CreateDictShaped(bag, DataSlice::JaggedShape::Empty(), test::DataItem(1),
                       test::DataItem("value")));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_dict, NoFollow(dict));
  EXPECT_THAT(DataSliceToStr(nofollow_dict),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Nofollow\(Entity:\$[0-9a-zA-Z]{22}\))regex")));
  EXPECT_THAT(
      DataSliceToStr(nofollow_dict.GetSchema()),
      IsOkAndHolds(MatchesRegex(R"regex(NOFOLLOW\(#[0-9a-zA-Z]{22}\))regex")));

  ASSERT_OK_AND_ASSIGN(DataSlice list,
                       CreateListShaped(bag, DataSlice::JaggedShape::Empty(),
                                        test::DataItem(1)));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_list, NoFollow(list));
  EXPECT_THAT(DataSliceToStr(nofollow_list),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Nofollow\(Entity:\$[0-9a-zA-Z]{22}\))regex")));
  EXPECT_THAT(
      DataSliceToStr(nofollow_list.GetSchema()),
      IsOkAndHolds(MatchesRegex(R"regex(NOFOLLOW\(#[0-9a-zA-Z]{22}\))regex")));
}

TEST(DataSliceReprTest, TestStringRepresentation_ShowDtypeOnAnyAndObject) {
  DataSlice item = test::DataItem<int64_t>(1, schema::kObject);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("int64{1}"));

  item = test::DataItem<int64_t>(1, schema::kAny);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("int64{1}"));

  item = test::DataItem<int64_t>(1, schema::kInt64);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("1"));

  item = test::DataItem<double>(double{1.234}, schema::kObject);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("float64{1.234}"));

  item = test::DataItem<double>(double{1.234}, schema::kAny);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("float64{1.234}"));

  item = test::DataItem<double>(double{1.234}, schema::kFloat64);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("1.234"));

  DataSlice slice = test::DataSlice<int64_t>({1, 2, 3}, schema::kObject);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[int64{1}, int64{2}, int64{3}]"));

  slice = test::DataSlice<int64_t>({1, 2, 3}, schema::kAny);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[int64{1}, int64{2}, int64{3}]"));

  slice = test::DataSlice<int64_t>({1, 2, 3}, schema::kInt64);
  EXPECT_THAT(DataSliceToStr(slice), IsOkAndHolds("[1, 2, 3]"));

  slice = test::DataSlice<double>({double{1.234}, double{1.234}, double{1.234}},
                                  schema::kObject);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[float64{1.234}, float64{1.234}, float64{1.234}]"));

  slice = test::DataSlice<double>({double{1.234}, double{1.234}, double{1.234}},
                                  schema::kAny);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[float64{1.234}, float64{1.234}, float64{1.234}]"));

  slice = test::DataSlice<double>({double{1.234}, double{1.234}, double{1.234}},
                                  schema::kFloat64);
  EXPECT_THAT(DataSliceToStr(slice), IsOkAndHolds("[1.234, 1.234, 1.234]"));

  slice = test::MixedDataSlice<double, int64_t>({double{1.234}, std::nullopt},
                                                {std::nullopt, int64_t{123}},
                                                schema::kObject);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[float64{1.234}, int64{123}]"));

  slice = test::MixedDataSlice<double, int64_t>({double{1.234}, std::nullopt},
                                                {std::nullopt, int64_t{123}},
                                                schema::kAny);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[float64{1.234}, int64{123}]"));
}

TEST(DataSliceReprTest, CycleInDict) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));
  // Set dict value schema to self.
  DataSlice schema = dict.GetSchema();
  ASSERT_OK(schema.SetAttr(schema::kDictValuesSchemaAttr, dict.GetSchema()));

  EXPECT_THAT(
      DataSliceToStr(schema),
      IsOkAndHolds(MatchesRegex(
          R"regex(DICT\{INT32, DICT\{INT32, DICT\{INT32, DICT\{INT32, DICT\{INT32, #[0-9a-zA-Z]{22}\}\}\}\}\})regex")));

  // Set dict value to self.
  ASSERT_OK(dict.SetInDict(key_item, dict));

  EXPECT_THAT(
      DataSliceToStr(dict),
      IsOkAndHolds(MatchesRegex(
          R"regex(Dict\{1=Dict\{1=Dict\{1=Dict\{1=Dict\{1=\$[0-9a-zA-Z]{22}\}\}\}\}\})regex")));

  EXPECT_THAT(DataSliceToStr(dict, {.depth = 2}),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Dict\{1=Dict\{1=\$[0-9a-zA-Z]{22}\}\})regex")));
}

TEST(DataSliceReprTest, CycleInList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice list,
                       CreateListShaped(bag, DataSlice::JaggedShape::Empty(),
                                        test::DataItem(1)));

  DataSlice schema = list.GetSchema();
  ASSERT_OK(schema.SetAttr(schema::kListItemsSchemaAttr, list.GetSchema()));

  EXPECT_THAT(
      DataSliceToStr(schema),
      IsOkAndHolds(MatchesRegex(
          R"regex(LIST\[LIST\[LIST\[LIST\[LIST\[#[0-9a-zA-Z]{22}\]\]\]\]\])regex")));

  ASSERT_OK(list.SetInList(test::DataItem(0), list));

  EXPECT_THAT(
      DataSliceToStr(list),
      IsOkAndHolds(MatchesRegex(
          R"regex(List\[List\[List\[List\[List\[\$[0-9a-zA-Z]{22}\]\]\]\]\])regex")));
}

TEST(DataSliceReprTest, CycleInEntity) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));
  DataSlice schema = entity.GetSchema();

  ASSERT_OK(schema.SetAttr("a", schema));

  EXPECT_THAT(
      DataSliceToStr(schema),
      IsOkAndHolds(MatchesRegex(
          R"regex(SCHEMA\(a=SCHEMA\(a=SCHEMA\(a=SCHEMA\(a=SCHEMA\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));

  ASSERT_OK(entity.SetAttr("a", entity));
  EXPECT_THAT(
      DataSliceToStr(entity),
      IsOkAndHolds(MatchesRegex(
          R"regex(Entity\(a=Entity\(a=Entity\(a=Entity\(a=Entity\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));
}

TEST(DataSliceReprTest, CycleInObject) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a"}, {value_1}));
  ASSERT_OK_AND_ASSIGN(DataSlice schema, obj.GetAttr(schema::kSchemaAttr));

  ASSERT_OK(schema.SetAttr("a", schema));

  EXPECT_THAT(
      DataSliceToStr(schema),
      IsOkAndHolds(MatchesRegex(
          R"regex(IMPLICIT_SCHEMA\(a=IMPLICIT_SCHEMA\(a=IMPLICIT_SCHEMA\(a=IMPLICIT_SCHEMA\(a=IMPLICIT_SCHEMA\(a=#[0-9a-zA-Z]{22}\)\)\)\)\))regex")));

  ASSERT_OK(obj.SetAttr("a", obj));
  EXPECT_THAT(
      DataSliceToStr(obj),
      IsOkAndHolds(MatchesRegex(
          R"regex(Obj\(a=Obj\(a=Obj\(a=Obj\(a=Obj\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));
}

TEST(DataSliceReprTest, DictExceedReprItemLimit) {
  DataBagPtr bag = DataBag::Empty();
  DataSlice key_item = test::DataSlice<int>({1, 2, 3, 4, 5, 6});
  DataSlice value_item = test::DataSlice<int>({1, 2, 3, 4, 5, 6});

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));
  EXPECT_THAT(DataSliceToStr(dict, {.item_limit = 5}),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Dict\{([0-9]=[0-9], ){5}\.\.\.\})regex")));
}

TEST(DataSliceReprTest, ListExceedReprItemLimit) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice list_item = test::DataSlice<int>({1, 2, 3, 4, 5, 6});

  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list_item));

  EXPECT_THAT(DataSliceToStr(list, {.item_limit = 5}),
              IsOkAndHolds(StrEq("List[1, 2, 3, 4, 5, ...]")));
}

TEST(DataSliceReprTest, NestedListExceedReprItemLimit) {
  DataBagPtr bag = DataBag::Empty();

  std::vector<arolla::OptionalValue<int64_t>> input;
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      input.emplace_back(j);
    }
  }

  std::vector<arolla::OptionalValue<int64_t>> edge2_input;
  for (int i = 0; i <= 10; ++i) {
    edge2_input.emplace_back(i * 10);
  }

  arolla::DenseArray<int64_t> edge2_array =
      arolla::CreateDenseArray<int64_t>(edge2_input);

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 10}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2,
                       DenseArrayEdge::FromSplitPoints(std::move(edge2_array)));
  ASSERT_OK_AND_ASSIGN(
      auto ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       DataSlice::CreateWithSchemaFromData(
                           internal::DataSliceImpl::Create(std::move(ds_array)),
                           std::move(ds_shape)));
  ASSERT_OK_AND_ASSIGN(DataSlice list, CreateNestedList(bag, ds));
  EXPECT_THAT(DataSliceToStr(list, {.item_limit = 5}),
              IsOkAndHolds(StrEq(R"(List[
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  ...,
])")));
}

TEST(DataSliceReprTest, ObjEntityExceedReprItemLimit) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"a", "b", "c", "d", "e", "f"},
                               std::vector<DataSlice>(6, test::DataItem(1))));

  EXPECT_THAT(DataSliceToStr(obj, {.item_limit = 5}),
              IsOkAndHolds(StrEq("Obj(a=1, b=1, c=1, d=1, e=1, ...)")));

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b", "c", "d", "e", "f"},
                               std::vector<DataSlice>(6, test::DataItem(1))));

  EXPECT_THAT(DataSliceToStr(entity, {.item_limit = 5}),
              IsOkAndHolds(StrEq("Entity(a=1, b=1, c=1, d=1, e=1, ...)")));
}

TEST(DataSliceReprTest, FormatHtml) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds(StrEq("List[1, 2, 3]")));

  std::vector<DataSlice> attr_values = {test::DataItem(1), data_slice};
  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"a", "b"}, attr_values));

  EXPECT_THAT(DataSliceToStr(obj, {.format_html = true}),
            IsOkAndHolds(StrEq(
                "Obj(<span class=\"attr \">a</span>=1, "
                "<span class=\"attr clickable\">b</span>=List[1, 2, 3])")));
}


}  // namespace
}  // namespace koladata
