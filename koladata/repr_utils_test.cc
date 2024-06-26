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
#include "koladata/repr_utils.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/testing/equals_proto.h"
#include "arolla/util/text.h"

namespace koladata {
namespace {

using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::JaggedDenseArrayShapePtr;
using ::arolla::OptionalValue;
using ::arolla::testing::EqualsProto;
using ::koladata::internal::Error;
using ::koladata::internal::ObjectId;
using ::koladata::testing::IsOkAndHolds;
using ::testing::MatchesRegex;
using ::testing::StrEq;

absl::StatusOr<DenseArrayEdge> EdgeFromSplitPoints(
    absl::Span<const OptionalValue<int64_t>> split_points) {
  return DenseArrayEdge::FromSplitPoints(
      CreateDenseArray<int64_t>(split_points));
}

TEST(ReprUtilTest, TestItemStringRepresentation_Int) {
  DataSlice data_slice = test::DataItem(1);
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds(StrEq("1")));
}

TEST(ReprUtilTest, TestDataItemStringRepresentation_Dict) {
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

TEST(ReprUtilTest, TestItemStringRepresentation_NestedDict) {
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

TEST(ReprUtilTest, TestDataItemStringRepresentation_List) {
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

TEST(ReprUtilTest, TestItemStringRepresentation_NestedList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 1, 3}));
  internal::DataSliceImpl ds =
      internal::DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShapePtr ds_shape,
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

TEST(ReprUtilTest, TestDataItemStringRepresentation_DictInList) {
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
      JaggedDenseArrayShapePtr ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(dict_slice), /*schema=*/std::nullopt,
                       test::Schema(schema::kAny)));
  EXPECT_THAT(
      DataSliceToStr(data_slice),
      IsOkAndHolds(StrEq("List[Dict{'a'=1}, Dict{'a'=1}, Dict{'a'=1}]")));
}

TEST(ReprUtilTest, TestDataItemStringRepresentation_ObjectsInList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj, ObjectCreator()(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List[Obj(a=1), Obj(a=2)]")));
}

TEST(ReprUtilTest, TestDataItemStringRepresentation_EntitiesInList) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kAny)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj, EntityCreator()(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(StrEq("List[Entity(a=1), Entity(a=2)]")));
}

TEST(ReprUtilTest, TestDataItemStringRepresentation_Object) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_obj, ObjectCreator()(bag, {}, {}));
  EXPECT_THAT(
      DataSliceToStr(empty_obj),
      IsOkAndHolds(MatchesRegex(R"regex(Obj\(\):\$[a-f0-9]{32})regex")));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator()(bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(obj), IsOkAndHolds("Obj(a=1, b='b')"));
}

TEST(ReprUtilTest, TestDataItemStringRepresentation_Entity) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_entity, EntityCreator()(bag, {}, {}));
  EXPECT_THAT(
      DataSliceToStr(empty_entity),
      IsOkAndHolds(MatchesRegex(R"regex(Entity\(\):\$[a-f0-9]{32})regex")));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator()(bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(entity), IsOkAndHolds("Entity(a=1, b='b')"));
}

TEST(ReprUtilTest, TestItemStringRepresentation_ListSchema) {
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

TEST(ReprUtilTest, TestItemStringRepresentation_DictSchema) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  EXPECT_THAT(DataSliceToStr(dict.GetSchema()),
              IsOkAndHolds(StrEq("DICT{INT32, TEXT}")));

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
      IsOkAndHolds(StrEq("DICT{DICT{INT32, TEXT}, DICT{TEXT, OBJECT}}")));
}

TEST(ReprUtilTest, TestItemStringRepresentation_ExplicitSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_entity, EntityCreator()(bag, {}, {}));
  EXPECT_THAT(DataSliceToStr(empty_entity.GetSchema()),
              IsOkAndHolds("SCHEMA()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator()(bag, {"a", "b"}, {value_1, value_2}));
  EXPECT_THAT(DataSliceToStr(entity.GetSchema()),
              IsOkAndHolds("SCHEMA(a=INT32, b=TEXT)"));
}

TEST(ReprUtilTest, TestItemStringRepresentation_ExplicitSchema_Nested) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");
  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  ASSERT_OK_AND_ASSIGN(DataSlice entity, EntityCreator()(bag, {"a"}, {dict}));
  EXPECT_THAT(DataSliceToStr(entity.GetSchema()),
              IsOkAndHolds("SCHEMA(a=DICT{INT32, TEXT})"));
}

TEST(ReprUtilTest, TestItemStringRepresentation_ImplicitSchema) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_obj, ObjectCreator()(bag, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto schema, empty_obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds("IMPLICIT_SCHEMA()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator()(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(DataSliceToStr(obj.GetSchema()), IsOkAndHolds("OBJECT"));
  ASSERT_OK_AND_ASSIGN(schema, obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("IMPLICIT_SCHEMA(a=INT32, b=TEXT)"));
}

TEST(ReprUtilTest, TestItemStringReprWithFallbackDB) {
  DataSlice ds_a = test::DataItem(1);

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds, EntityCreator()(db, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kAny)));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  DataBagPtr db2 = DataBag::Empty();
  ds = ds.WithDb(db2);
  ASSERT_OK(ds.GetSchema().SetAttr("b", test::Schema(schema::kAny)));
  DataSlice ds_b = test::DataItem(2);
  ASSERT_OK(ds.SetAttr("b", ds_b));

  db = DataBag::ImmutableEmptyWithFallbacks({db, db2});
  ds = ds.WithDb(db);

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("Entity(a=1, b=2)"));
  EXPECT_THAT(DataSliceToStr(ds.GetSchema()),
              IsOkAndHolds(("SCHEMA(a=ANY, b=ANY)")));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_SimpleSlice) {
  DataSlice ds = test::DataSlice<int>({1, 2, 3});

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[1, 2, 3]"));

  DataSlice ds2 = test::DataSlice<int>({1, std::nullopt, 3});
  EXPECT_THAT(DataSliceToStr(ds2), IsOkAndHolds("[1, None, 3]"));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_EntitySlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds, EntityCreator()(db, {"a"}, {values}));
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Entity:\$[a-f0-9]{32}, Entity:\$[a-f0-9]{32}\])regex")));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_ObjectSlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice ds, ObjectCreator()(db, {"a"}, {values}));
  EXPECT_THAT(DataSliceToStr(ds),
              IsOkAndHolds(MatchesRegex(
                  R"regex(\[Obj:\$[a-f0-9]{32}, Obj:\$[a-f0-9]{32}\])regex")));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_ListSlices) {
  ObjectId list_id_1 = internal::AllocateSingleList();
  ObjectId list_id_2 = internal::AllocateSingleList();
  DataSlice ds = test::DataSlice<ObjectId>({list_id_1, list_id_2});

  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[List:\$[a-f0-9]{32}, List:\$[a-f0-9]{32}\])regex")));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_DictSlices) {
  ObjectId dict_id_1 = internal::AllocateSingleDict();
  ObjectId dict_id_2 = internal::AllocateSingleDict();

  DataSlice ds = test::DataSlice<ObjectId>({dict_id_1, dict_id_2});
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Dict:\$[a-f0-9]{32}, Dict:\$[a-f0-9]{32}\])regex")));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_TwoDimensionsSlice) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 3}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShapePtr ds_shape,
      JaggedDenseArrayShape::FromEdges({std::move(edge1), std::move(edge2)}));

  DataSlice ds = test::DataSlice<int>({1, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[1, 2], [3]]"));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_ThreeDimensionsSlice) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 3}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge3, EdgeFromSplitPoints({0, 2, 3, 4}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShapePtr ds_shape,
      JaggedDenseArrayShape::FromEdges(
          {std::move(edge1), std::move(edge2), std::move(edge3)}));

  DataSlice ds =
      test::DataSlice<int>({1, std::nullopt, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[[1, None], [2]], [[3]]]"));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_SameChildSizeEdges) {
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge1, EdgeFromSplitPoints({0, 2}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge2, EdgeFromSplitPoints({0, 2, 5}));
  ASSERT_OK_AND_ASSIGN(DenseArrayEdge edge3,
                       EdgeFromSplitPoints({0, 1, 2, 3, 4, 5}));
  ASSERT_OK_AND_ASSIGN(
      JaggedDenseArrayShapePtr ds_shape,
      JaggedDenseArrayShape::FromEdges(
          {std::move(edge1), std::move(edge2), std::move(edge3)}));

  EXPECT_THAT(DataSliceToStr(
                  test::DataSlice<int>({1, 2, 3, 4, 5}, std::move(ds_shape))),
              IsOkAndHolds("[[[1], [2]], [[3], [4], [5]]]"));
}

TEST(ReprUtilTest, TestDataSliceImplStringRepresentation_SplitLines) {
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
      JaggedDenseArrayShapePtr ds_shape,
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

TEST(ReprUtilTest, TestDataBagStringRepresentation_Entities) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");
  ASSERT_OK(EntityCreator()(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\.a => 1(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\.b => b(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\.a => INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}\.b => TEXT(.|\n)*)regex"))));
}

TEST(ReprUtilTest, TestDataBagStringRepresentation_Objects) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");
  ASSERT_OK(ObjectCreator()(bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}\.__schema__ => k[0-9a-f]{32}(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\.a => 1(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\.b => b(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*k[0-9a-f]{32}\.a => INT32(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*k[0-9a-f]{32}\.b => TEXT(.|\n)*)regex"))));
}

TEST(ReprUtilTest, TestDataBagStringRepresentation_Dicts) {
  DataBagPtr bag = DataBag::Empty();

  ObjectId dict_id = internal::AllocateSingleDict();
  DataSlice data_slice = test::DataItem(dict_id, schema::kAny, bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<int>({1, 4});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\['a'\] => 1(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*\$[0-9a-f]{32}\['x'\] => 4(.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"))));
}

TEST(ReprUtilTest, TestDataBagStringRepresentation_List) {
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK(CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                             test::Schema(schema::kAny)));
  EXPECT_THAT(
      DataBagToStr(bag),
      IsOkAndHolds(AllOf(
          MatchesRegex(R"regex(DataBag \$[0-9a-f]{4}:(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*\$[0-9a-f]{32}\[:\] => \[1, 2, 3\](.|\n)*)regex"),
          MatchesRegex(R"regex((.|\n)*SchemaBag:(.|\n)*)regex"))));
}

TEST(ReprUtilTest, TestAssembleError) {
  DataBagPtr bag = DataBag::Empty();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator()(bag, {"a", "b"}, {value_1, value_2}));
  schema::DType dtype = schema::GetDType<int>();

  Error error;
  internal::NoCommonSchema* no_common_schema = error.mutable_no_common_schema();
  *no_common_schema->mutable_common_schema() =
      internal::EncodeSchema(entity.GetSchemaImpl());
  *no_common_schema->mutable_conflicting_schema() =
      internal::EncodeSchema(internal::DataItem(dtype));

  internal::ObjectId schema_obj =
      entity.GetSchemaImpl().value<internal::ObjectId>();

  absl::Status status = AssembleErrorMessage(
      internal::WithErrorPayload(absl::InvalidArgumentError("error"), error),
      {bag});
  std::optional<Error> payload = internal::GetErrorPayload(status);
  EXPECT_THAT(payload->cause(),
              EqualsProto(absl::StrFormat(
                  R"pb(no_common_schema {
                         common_schema { object_id { hi: %d lo: %d } }
                         conflicting_schema { dtype: %d }
                       })pb",
                  schema_obj.InternalHigh64(), schema_obj.InternalLow64(),
                  dtype.type_id())));
  EXPECT_THAT(
      payload->error_message(),
      AllOf(
          MatchesRegex(
              R"regex((.|\n)*cannot find a common schema for provided schemas(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the common schema\(s\) [0-9a-f]{32}: SCHEMA\(a=INT32, b=TEXT\)(.|\n)*)regex"),
          MatchesRegex(
              R"regex((.|\n)*the first conflicting schema INT32: INT32(.|\n)*)regex")));
}

TEST(ReprUtilTest, TestAssembleErrorNotHandlingOkStatus) {
  EXPECT_TRUE(AssembleErrorMessage(absl::OkStatus(), {}).ok());
}

}  // namespace
}  // namespace koladata
