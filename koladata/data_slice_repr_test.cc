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
#include "koladata/data_slice_repr.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/functor.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/object_factories.h"
#include "koladata/signature.h"
#include "koladata/test_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::CreateDenseArray;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::OptionalValue;
using ::koladata::functor::CppSignatureToKodaSignature;
using ::koladata::functor::CreateFunctor;
using ::koladata::functor::Signature;
using ::koladata::internal::ObjectId;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Primitives) {
  DataSlice item = test::DataItem(1);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("1"));

  DataSlice item2 = test::DataItem(arolla::kUnit);
  EXPECT_THAT(DataSliceToStr(item2), IsOkAndHolds("present"));

  DataSlice item3 = test::DataItem(std::nullopt, schema::kMask);
  EXPECT_THAT(DataSliceToStr(item3), IsOkAndHolds("missing"));

  DataSlice item4 = test::DataItem(arolla::Text("foo"));
  EXPECT_THAT(DataSliceToStr(item4), IsOkAndHolds("'foo'"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(item4.item(), item4.GetSchemaImpl(), item4.GetBag()),
      IsOkAndHolds("'foo'"));
  EXPECT_THAT(DataItemToStr(item4.item(), /*schema=*/internal::DataItem(),
                            /*db=*/nullptr),
              IsOkAndHolds("'foo'"));
  // With ReprOptions.
  EXPECT_THAT(DataItemToStr(item4.item(), item4.GetSchemaImpl(), item4.GetBag(),
                            {.strip_quotes = true}),
              IsOkAndHolds("foo"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Dict) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds("Dict{}"));

  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<int>({1, 4});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(MatchesRegex(
                  R"regexp(Dict\{('x'=4, 'a'=1|'a'=1, 'x'=4)\})regexp")));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds(MatchesRegex(
                  R"regexp(Dict\{('x'=4, 'a'=1|'a'=1, 'x'=4)\})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds(MatchesRegex(
                  R"regexp(Dict\{('x'=4, 'a'=1|'a'=1, 'x'=4)\})regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Dict_Text) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kString)));
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<arolla::Text>({"a", "4"});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(
      DataSliceToStr(data_slice),
      IsOkAndHolds(MatchesRegex(
          R"regexp(Dict\{('x'='4', 'a'='a'|'a'='a', 'x'='4')\})regexp")));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                    data_slice.GetBag()),
      IsOkAndHolds(MatchesRegex(
          R"regexp(Dict\{('x'='4', 'a'='a'|'a'='a', 'x'='4')\})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(
      DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                    data_slice.GetBag()),
      IsOkAndHolds(MatchesRegex(
          R"regexp(Dict\{('x'='4', 'a'='a'|'a'='a', 'x'='4')\})regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_NestedDict) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice dict = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice key = test::DataSlice<arolla::Text>({"a"});
  DataSlice value = test::DataSlice<int>({1});
  ASSERT_OK(dict.SetInDict(key, value));

  ASSERT_OK_AND_ASSIGN(
      auto second_dict_schema,
      CreateDictSchema(bag, test::Schema(schema::kString), dict_schema));
  ObjectId second_dict_id = internal::AllocateSingleDict();
  DataSlice second_key = test::DataSlice<arolla::Text>({"x"});
  DataSlice second_dict =
      test::DataItem(second_dict_id, second_dict_schema.item(), bag);
  ASSERT_OK(second_dict.SetInDict(second_key, dict));

  EXPECT_THAT(DataSliceToStr(second_dict),
              IsOkAndHolds("Dict{'x'=Dict{'a'=1}}"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(second_dict.item(), second_dict.GetSchemaImpl(),
                            second_dict.GetBag()),
              IsOkAndHolds("Dict{'x'=Dict{'a'=1}}"));
  EXPECT_THAT(DataItemToStr(second_dict.item(), second_dict.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(second_dict.item(), second_dict.GetSchemaImpl(),
                            second_dict.GetBag()),
              IsOkAndHolds("Dict{'x'=Dict{'a'=1}}"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_DictMultiline) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();

  std::string large_key0(50, 'a');
  std::string large_key1(50, 'x');
  DataSlice keys =
      test::DataSlice<arolla::Text>({large_key0.c_str(), large_key1.c_str()});
  DataSlice values = test::DataSlice<int>({1, 1});

  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  ASSERT_OK(data_slice.SetInDict(keys, values));

  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds(MatchesRegex(absl::StrFormat(
                  "Dict\\{(\n  '%s'=1,\n  '%s'=1|\n  '%s'=1,\n  '%s'=1),\n\\}",
                  large_key0, large_key1, large_key1, large_key0))));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds(MatchesRegex(absl::StrFormat(
                  "Dict\\{(\n  '%s'=1,\n  '%s'=1|\n  '%s'=1,\n  '%s'=1),\n\\}",
                  large_key0, large_key1, large_key1, large_key0))));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds(MatchesRegex(absl::StrFormat(
                  "Dict\\{(\n  '%s'=1,\n  '%s'=1|\n  '%s'=1,\n  '%s'=1),\n\\}",
                  large_key0, large_key1, large_key1, large_key0))));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_List) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kObject)));
  EXPECT_THAT(DataSliceToStr(empty_list), IsOkAndHolds("List[]"));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds("List[1, 2, 3]"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[1, 2, 3]"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[1, 2, 3]"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_List_Text) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kObject)));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<arolla::Text>({"a", "b", "c"}),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));
  EXPECT_THAT(DataSliceToStr(data_slice), IsOkAndHolds("List['a', 'b', 'c']"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List['a', 'b', 'c']"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds("List['a', 'b', 'c']"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_NestedList) {
  DataBagPtr bag = DataBag::EmptyMutable();

  internal::DataSliceImpl ds =
      internal::DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 2}, {0, 1, 3}});
  ASSERT_OK_AND_ASSIGN(DataSlice nested_list,
                       DataSlice::Create(std::move(ds), std::move(ds_shape),
                                         internal::DataItem(schema::kObject)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(nested_list),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds("List[List[1], List[2, 3]]"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[List[1], List[2, 3]]"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[List[1], List[2, 3]]"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_DictInList) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));

  internal::DataSliceImpl dict_slice_impl =
      internal::DataSliceImpl::ObjectsFromAllocation(internal::AllocateDicts(3),
                                                     3);
  ASSERT_OK_AND_ASSIGN(DataSlice dict_slice,
                       DataSlice::Create(std::move(dict_slice_impl),
                                         JaggedDenseArrayShape::FlatFromSize(3),
                                         dict_schema.item(), bag));

  DataSlice keys = test::DataItem("a");
  DataSlice values = test::DataItem(1);
  ASSERT_OK(dict_slice.SetInDict(keys, values));

  JaggedDenseArrayShape ds_shape =
      test::ShapeFromSplitPoints({{0, 2}, {0, 1, 3}});

  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, std::move(dict_slice), /*schema=*/std::nullopt,
                       test::Schema(schema::kObject)));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds("List[Dict{'a'=1}, Dict{'a'=1}, Dict{'a'=1}]"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[Dict{'a'=1}, Dict{'a'=1}, Dict{'a'=1}]"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[Dict{'a'=1}, Dict{'a'=1}, Dict{'a'=1}]"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_ObjectsInList) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kObject)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds("List[Obj(a=1), Obj(a=2)]"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[Obj(a=1), Obj(a=2)]"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(
      DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                    data_slice.GetBag()),
      IsOkAndHolds(MatchesRegex(
          R"regexp(List\[Entity\(\):\$[0-9a-zA-Z]{22}, Entity\(\):\$[0-9a-zA-Z]{22}\])regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_EntitiesInList) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kObject)));

  DataSlice value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));

  ASSERT_OK_AND_ASSIGN(DataSlice data_slice, CreateNestedList(bag, obj));
  EXPECT_THAT(DataSliceToStr(data_slice),
              IsOkAndHolds("List[Entity(a=1), Entity(a=2)]"));

  // Low level api.
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            data_slice.GetBag()),
              IsOkAndHolds("List[Entity(a=1), Entity(a=2)]"));
  EXPECT_THAT(DataItemToStr(data_slice.item(), data_slice.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(
      DataItemToStr(data_slice.item(), /*schema=*/internal::DataItem(),
                    data_slice.GetBag()),
      IsOkAndHolds(MatchesRegex(
          R"regexp(List\[Entity\(\):\$[0-9a-zA-Z]{22}, Entity\(\):\$[0-9a-zA-Z]{22}\])regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Object) {
  DataBagPtr bag = DataBag::EmptyMutable();

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

  // Low level api.
  EXPECT_THAT(DataItemToStr(obj.item(), obj.GetSchemaImpl(), obj.GetBag()),
              IsOkAndHolds("Obj(a=1, b='b')"));
  EXPECT_THAT(DataItemToStr(obj.item(), obj.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(
      DataItemToStr(obj.item(), /*schema=*/internal::DataItem(), obj.GetBag()),
      IsOkAndHolds(
          MatchesRegex(R"regexp(Entity\(\):\$[0-9a-zA-Z]{22})regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Entity) {
  DataBagPtr bag = DataBag::EmptyMutable();

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

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(entity.item(), entity.GetSchemaImpl(), entity.GetBag()),
      IsOkAndHolds("Entity(a=1, b='b')"));
  EXPECT_THAT(DataItemToStr(entity.item(), entity.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(entity.item(), /*schema=*/internal::DataItem(),
                            entity.GetBag()),
              IsOkAndHolds(
                  MatchesRegex(R"regexp(Entity\(\):\$[0-9a-zA-Z]{22})regexp")));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_ListSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice list_item = test::DataItem(1);

  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list_item));

  EXPECT_THAT(DataSliceToStr(list.GetSchema()), IsOkAndHolds("LIST[INT32]"));

  ASSERT_OK_AND_ASSIGN(
      DataSlice list_of_list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list));

  const auto& schema = list_of_list.GetSchema();
  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds("LIST[LIST[INT32]]"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("LIST[LIST[INT32]]"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\#[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("LIST[LIST[INT32]]"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_DictSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  EXPECT_THAT(DataSliceToStr(dict.GetSchema()),
              IsOkAndHolds("DICT{INT32, STRING}"));

  DataSlice second_key_item = test::DataItem("foo");
  DataSlice second_value_item =
      test::DataItem(internal::AllocateSingleObject());
  ASSERT_OK_AND_ASSIGN(DataSlice second_dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        second_key_item, second_value_item));

  ASSERT_OK_AND_ASSIGN(DataSlice nested_dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        dict, second_dict));

  const auto& schema = nested_dict.GetSchema();
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("DICT{DICT{INT32, STRING}, DICT{STRING, OBJECT}}"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("DICT{DICT{INT32, STRING}, DICT{STRING, OBJECT}}"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\#[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("DICT{DICT{INT32, STRING}, DICT{STRING, OBJECT}}"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_ExplicitSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_entity,
                       EntityCreator::FromAttrs(bag, {}, {}));
  EXPECT_THAT(DataSliceToStr(empty_entity.GetSchema()),
              IsOkAndHolds("ENTITY()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));
  const auto& schema = entity.GetSchema();
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("ENTITY(a=INT32, b=STRING)"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("ENTITY(a=INT32, b=STRING)"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("ENTITY(a=INT32, b=STRING)"));
}

TEST(DataSliceReprTest,
     TestDataItemStringRepresentation_ExplicitSchema_Nested) {
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");
  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));

  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {dict}));
  const auto& schema = entity.GetSchema();
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("ENTITY(a=DICT{INT32, STRING})"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("ENTITY(a=DICT{INT32, STRING})"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\$[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("ENTITY(a=DICT{INT32, STRING})"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_ImplicitSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(DataSlice empty_obj,
                       ObjectCreator::FromAttrs(bag, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto schema, empty_obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds("IMPLICIT_ENTITY()"));

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(DataSlice obj, ObjectCreator::FromAttrs(
                                          bag, {"a", "b"}, {value_1, value_2}));

  EXPECT_THAT(DataSliceToStr(obj.GetSchema()), IsOkAndHolds("OBJECT"));
  ASSERT_OK_AND_ASSIGN(schema, obj.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds("IMPLICIT_ENTITY(a=INT32, b=STRING)"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("IMPLICIT_ENTITY(a=INT32, b=STRING)"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\#[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("IMPLICIT_ENTITY(a=INT32, b=STRING)"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_SchemaName) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateNamedSchema(bag, "foo", {"a", "b"},
                                         {test::Schema(schema::kInt64),
                                          test::Schema(schema::kString)}));
  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds("foo(a=INT64, b=STRING)"));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(schema.item(), schema.GetSchemaImpl(), schema.GetBag()),
      IsOkAndHolds("foo(a=INT64, b=STRING)"));
  EXPECT_THAT(DataItemToStr(schema.item(), schema.GetSchemaImpl(),
                            /*db=*/nullptr),
              IsOkAndHolds(MatchesRegex(R"regexp(\#[0-9a-zA-Z]{22})regexp")));
  EXPECT_THAT(DataItemToStr(schema.item(), /*schema=*/internal::DataItem(),
                            schema.GetBag()),
              IsOkAndHolds("foo(a=INT64, b=STRING)"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_Functor) {
  auto create_input = [](absl::string_view name) {
    return arolla::expr::CallOp("koda_internal.input",
                                {arolla::expr::Literal(arolla::Text("I")),
                                 arolla::expr::Literal(arolla::Text(name))});
  };

  auto create_variable = [](absl::string_view name) {
    return arolla::expr::CallOp("koda_internal.input",
                                {arolla::expr::Literal(arolla::Text("V")),
                                 arolla::expr::Literal(arolla::Text(name))});
  };

  auto wrap_expr = [](absl::StatusOr<arolla::expr::ExprNodePtr> expr_or_error)
      -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto expr, expr_or_error);
    return test::DataItem(arolla::expr::ExprQuote(std::move(expr)));
  };

  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      auto returns_expr,
      wrap_expr(arolla::expr::CallOp(
          "math.multiply", {create_input("a"), create_variable("a")})));
  ASSERT_OK_AND_ASSIGN(auto var_a_expr, wrap_expr(create_input("b")));
  auto slice_57 = test::DataItem(57);
  ASSERT_OK_AND_ASSIGN(
      auto my_obj,
      ObjectCreator::FromAttrs(DataBag::EmptyMutable(), {"a"}, {slice_57}));

  {
    // Test unnamed functor.
    ASSERT_OK_AND_ASSIGN(
        auto fn, CreateFunctor(returns_expr, koda_signature,
                              {"a", "my_obj"},
                              {var_a_expr, my_obj}));
    EXPECT_THAT(
        DataSliceToStr(fn),
        IsOkAndHolds(
            "Functor[a](a=I.b, my_obj=Obj(a=57), returns=I.a * V.a)"));
  }

  {
    // Test named functor.
    ASSERT_OK_AND_ASSIGN(
        auto fn, CreateFunctor(returns_expr, koda_signature,
                               {"a", "my_obj", functor::kQualnameAttrName,
                                functor::kModuleAttrName},
                               {var_a_expr, my_obj, test::DataItem("foo.bar"),
                                test::DataItem("module.name")}));
    EXPECT_THAT(
        DataSliceToStr(fn),
        IsOkAndHolds(
            "Functor foo.bar[a](a=I.b, my_obj=Obj(a=57), returns=I.a * V.a)"));
  }

  {
    // Test non-text name
    ASSERT_OK_AND_ASSIGN(
        auto fn, CreateFunctor(returns_expr, koda_signature,
                               {"a", "my_obj", functor::kQualnameAttrName,
                                functor::kModuleAttrName},
                               {var_a_expr, my_obj, test::DataItem(57),
                                test::DataItem("module.name")}));
    EXPECT_THAT(
        DataSliceToStr(fn),
        IsOkAndHolds(
            "Functor[a](a=I.b, my_obj=Obj(a=57), returns=I.a * V.a)"));
  }
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_SchemaMetadata) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateSchema(bag, {"a", "b"},
                                    {test::Schema(schema::kInt64),
                                     test::Schema(schema::kString)}));
  ASSERT_OK_AND_ASSIGN(DataSlice metadata, CreateMetadata(bag, schema, {}, {}));
  ASSERT_OK(metadata.SetAttr("foo", test::DataItem(1)));
  EXPECT_THAT(DataSliceToStr(schema),
              IsOkAndHolds(
                  "ENTITY(a=INT64, b=STRING, __schema_metadata__=Obj(foo=1))"));
}

TEST(DataSliceReprTest, TestDataItemStringRepresentation_NoBag) {
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice value_1 = test::DataItem(1);
  DataSlice value_2 = test::DataItem("b");

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b"}, {value_1, value_2}));

  entity = entity.WithBag(/*db=*/nullptr);

  EXPECT_THAT(DataSliceToStr(entity),
              IsOkAndHolds(MatchesRegex(R"regex(\$[0-9a-zA-Z]{22})regex")));
  EXPECT_THAT(DataSliceToStr(entity.GetSchema()),
              IsOkAndHolds(MatchesRegex(R"regex(\$[0-9a-zA-Z]{22})regex")));

  // Low level api.
  EXPECT_THAT(
      DataItemToStr(entity.item(), entity.GetSchemaImpl(), entity.GetBag()),
      IsOkAndHolds(MatchesRegex(R"regex(\$[0-9a-zA-Z]{22})regex")));
  EXPECT_THAT(
      DataItemToStr(entity.GetSchemaImpl(), entity.GetSchema().GetSchemaImpl(),
                    entity.GetBag()),
      IsOkAndHolds(MatchesRegex(R"regex(\$[0-9a-zA-Z]{22})regex")));
}

TEST(DataSliceReprTest, TestDataItemStringReprWithFallbackDB) {
  DataSlice ds_a = test::DataItem(1);

  DataBagPtr db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice ds, EntityCreator::FromAttrs(db, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kObject)));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  DataBagPtr db2 = DataBag::EmptyMutable();
  ds = ds.WithBag(db2);
  ASSERT_OK(ds.GetSchema().SetAttr("b", test::Schema(schema::kObject)));
  DataSlice ds_b = test::DataItem(2);
  ASSERT_OK(ds.SetAttr("b", ds_b));

  ASSERT_OK_AND_ASSIGN(
      db, DataBag::ImmutableEmptyWithFallbacks({db->Freeze(), db2->Freeze()}));
  ds = ds.WithBag(db);

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("Entity(a=1, b=2)"));
  EXPECT_THAT(DataSliceToStr(ds.GetSchema()),
              IsOkAndHolds(("ENTITY(a=OBJECT, b=OBJECT)")));

  // Low level api.
  EXPECT_THAT(DataItemToStr(ds.item(), ds.GetSchemaImpl(), ds.GetBag()),
              IsOkAndHolds("Entity(a=1, b=2)"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_Primitives) {
  DataSlice ds0 = test::DataSlice<int>({});
  EXPECT_THAT(DataSliceToStr(ds0), IsOkAndHolds("[]"));

  DataSlice ds1 = test::DataSlice<int>({1, 2, 3});
  EXPECT_THAT(DataSliceToStr(ds1), IsOkAndHolds("[1, 2, 3]"));

  DataSlice ds2 = test::DataSlice<int>({1, std::nullopt, 3});
  EXPECT_THAT(DataSliceToStr(ds2), IsOkAndHolds("[1, None, 3]"));

  DataSlice ds3 = test::DataSlice<arolla::Unit>(
      {arolla::kUnit, std::nullopt, arolla::kUnit});
  EXPECT_THAT(DataSliceToStr(ds3), IsOkAndHolds("[present, missing, present]"));

  DataSlice ds4 = test::MixedDataSlice<int, arolla::Unit>(
      {1, std::nullopt, std::nullopt},
      {std::nullopt, arolla::kUnit, std::nullopt});
  EXPECT_THAT(DataSliceToStr(ds4), IsOkAndHolds("[1, present, None]"));

  DataSlice ds5 = test::DataSlice<int>({1, 2, 3});
  EXPECT_THAT(DataSliceToStr(ds5, {.item_limit = 0}), IsOkAndHolds("[...]"));

  DataSlice ds6 = test::DataSlice<arolla::Text>({"ab", "cd", "ef"});
  EXPECT_THAT(DataSliceToStr(ds6), IsOkAndHolds("['ab', 'cd', 'ef']"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_EntitySlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       EntityCreator::FromAttrs(db, {"a"}, {values}));
  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(MatchesRegex(
          R"regex(\[Entity:\$[0-9a-zA-Z]{22}, Entity:\$[0-9a-zA-Z]{22}\])regex")));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_ObjectSlices) {
  DataSlice values = test::DataSlice<int>({1, 2});

  DataBagPtr db = DataBag::EmptyMutable();
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
  auto ds_shape = test::ShapeFromSplitPoints({{0, 2}, {0, 2, 3}});

  DataSlice ds = test::DataSlice<int>({1, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[1, 2], [3]]"));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_ThreeDimensionsSlice) {
  auto ds_shape = test::ShapeFromSplitPoints({{0, 2}, {0, 2, 3}, {0, 2, 3, 4}});

  DataSlice ds =
      test::DataSlice<int>({1, std::nullopt, 2, 3}, std::move(ds_shape));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds("[[[1, None], [2]], [[3]]]"));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_SameChildSizeEdges) {
  auto ds_shape =
      test::ShapeFromSplitPoints({{0, 2}, {0, 2, 5}, {0, 1, 2, 3, 4, 5}});

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

  std::vector<int64_t> edge2_split_points;
  for (int i = 0; i <= 10; ++i) {
    edge2_split_points.emplace_back(i * 10);
  }
  auto ds_shape = test::ShapeFromSplitPoints({{0, 10}, edge2_split_points});

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(
      DataSlice ds,
      DataSlice::CreatePrimitive(std::move(ds_array), std::move(ds_shape)));

  EXPECT_THAT(DataSliceToStr(ds), IsOkAndHolds(R"([
  [0, 1, 2, 3, 4, ...],
  [0, 1, 2, 3, 4, ...],
  [0, 1, 2, 3, 4, ...],
  [0, 1, 2, 3, 4, ...],
  [...],
  ...,
])"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_ApplyLimit) {
  std::vector<arolla::OptionalValue<int64_t>> input;
  std::vector<int64_t> edge2_split_points;
  edge2_split_points.emplace_back(0);
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < (i + 4); ++j) {
      input.emplace_back(j);
    }
    edge2_split_points.emplace_back(edge2_split_points.back() + i + 4);
  }

  auto ds_shape = test::ShapeFromSplitPoints({{0, 10}, edge2_split_points});

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(
      DataSlice ds,
      DataSlice::CreatePrimitive(std::move(ds_array), std::move(ds_shape)));

  EXPECT_THAT(DataSliceToStr(ds),
              IsOkAndHolds("[[0, 1, 2, 3], [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, "
                           "...], [0, 1, 2, 3, 4, ...], [0, ...], ...]"));
}

TEST(DataSliceReprTest,
     TestDataSliceImplStringRepresentation_NotExceedingTotalItemLimit) {
  std::vector<arolla::OptionalValue<int64_t>> input;
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 6; ++j) {
      input.emplace_back(j);
    }
  }

  std::vector<int64_t> edge2_split_points;
  for (int i = 0; i <= 3; ++i) {
    edge2_split_points.emplace_back(i * 6);
  }

  auto ds_shape = test::ShapeFromSplitPoints({{0, 3}, edge2_split_points});

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(
      DataSlice ds,
      DataSlice::CreatePrimitive(std::move(ds_array), std::move(ds_shape)));

  EXPECT_THAT(
      DataSliceToStr(ds),
      IsOkAndHolds(
          "[[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5]]"));
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_ItemId) {
  {
    // Dict with ITEMID schema.
    ObjectId dict_id_1 = internal::AllocateSingleDict();
    ObjectId dict_id_2 = internal::AllocateSingleDict();

    DataSlice ds =
        test::DataSlice<ObjectId>({dict_id_1, dict_id_2}, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(ds),
        IsOkAndHolds(MatchesRegex(
            R"regex(\[Dict:\$[0-9a-zA-Z]{22}, Dict:\$[0-9a-zA-Z]{22}\])regex")));
  }
  {
    // List with ITEMID schema.
    ObjectId list_id_1 = internal::AllocateSingleList();
    ObjectId list_id_2 = internal::AllocateSingleList();
    DataSlice ds =
        test::DataSlice<ObjectId>({list_id_1, list_id_2}, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(ds),
        IsOkAndHolds(MatchesRegex(
            R"regex(\[List:\$[0-9a-zA-Z]{22}, List:\$[0-9a-zA-Z]{22}\])regex")));
  }
  {
    // Schema with ITEMID schema.
    ObjectId schema_id_1 = internal::AllocateExplicitSchema();
    ObjectId schema_id_2 = internal::AllocateExplicitSchema();
    DataSlice ds =
        test::DataSlice<ObjectId>({schema_id_1, schema_id_2}, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(ds),
        IsOkAndHolds(MatchesRegex(
            R"regex(\[Schema:\$[0-9a-zA-Z]{22}, Schema:\$[0-9a-zA-Z]{22}\])regex")));
  }
  {
    // Entity with ITEMID schema.
    ObjectId entity_id_1 = internal::AllocateSingleObject();
    ObjectId entity_id_2 = internal::AllocateSingleObject();
    DataSlice ds =
        test::DataSlice<ObjectId>({entity_id_1, entity_id_2}, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(ds),
        IsOkAndHolds(MatchesRegex(
            R"regex(\[Entity:\$[0-9a-zA-Z]{22}, Entity:\$[0-9a-zA-Z]{22}\])regex")));
  }
}

TEST(DataSliceReprTest, TestDataSliceImplStringRepresentation_NoFollowSchema) {
  DataSlice ds = test::DataSlice<ObjectId>(
      {ObjectId::NoFollowObjectSchemaId(), ObjectId::NoFollowObjectSchemaId()},
      schema::kSchema);
  EXPECT_THAT(DataSliceToStr(ds),
              IsOkAndHolds("[NOFOLLOW(OBJECT), NOFOLLOW(OBJECT)]"));
}

TEST(DataSliceReprTest, TestStringRepresentation_NoFollow) {
  DataBagPtr bag = DataBag::EmptyMutable();
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
                  R"regex(Nofollow\(Dict:\$[0-9a-zA-Z]{22}\))regex")));
  EXPECT_THAT(
      DataSliceToStr(nofollow_dict.GetSchema()),
      IsOkAndHolds(MatchesRegex(R"regex(NOFOLLOW\(#[0-9a-zA-Z]{22}\))regex")));

  ASSERT_OK_AND_ASSIGN(DataSlice list,
                       CreateListShaped(bag, DataSlice::JaggedShape::Empty(),
                                        test::DataItem(1)));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_list, NoFollow(list));
  EXPECT_THAT(DataSliceToStr(nofollow_list),
              IsOkAndHolds(MatchesRegex(
                  R"regex(Nofollow\(List:\$[0-9a-zA-Z]{22}\))regex")));
  EXPECT_THAT(
      DataSliceToStr(nofollow_list.GetSchema()),
      IsOkAndHolds(MatchesRegex(R"regex(NOFOLLOW\(#[0-9a-zA-Z]{22}\))regex")));
}

TEST(DataSliceReprTest, TestStringRepresentation_ShowDtypeOnObject) {
  DataSlice item = test::DataItem<int64_t>(1, schema::kObject);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("int64{1}"));

  item = test::DataItem<int64_t>(1, schema::kInt64);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("1"));

  item = test::DataItem<double>(double{1.234}, schema::kObject);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("float64{1.234}"));

  item = test::DataItem<double>(double{1.234}, schema::kFloat64);
  EXPECT_THAT(DataSliceToStr(item), IsOkAndHolds("1.234"));

  DataSlice slice = test::DataSlice<int64_t>({1, 2, 3}, schema::kObject);
  EXPECT_THAT(DataSliceToStr(slice),
              IsOkAndHolds("[int64{1}, int64{2}, int64{3}]"));

  slice = test::DataSlice<int64_t>({1, 2, 3}, schema::kInt64);
  EXPECT_THAT(DataSliceToStr(slice), IsOkAndHolds("[1, 2, 3]"));

  slice = test::DataSlice<double>({double{1.234}, double{1.234}, double{1.234}},
                                  schema::kObject);
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
}

TEST(DataSliceReprTest, TestStringRepresentation_ItemId) {
  {
    ObjectId item_id = internal::AllocateSingleObject();
    DataSlice item = test::DataItem<ObjectId>(item_id, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(item),
        IsOkAndHolds(MatchesRegex(R"regex(Entity:\$[0-9a-zA-Z]{22})regex")));
  }
  {
    ObjectId dict_id = internal::AllocateSingleDict();
    DataSlice item = test::DataItem<ObjectId>(dict_id, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(item),
        IsOkAndHolds(MatchesRegex(R"regex(Dict:\$[0-9a-zA-Z]{22})regex")));
  }
  {
    ObjectId list_id = internal::AllocateSingleList();
    DataSlice item = test::DataItem<ObjectId>(list_id, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(item),
        IsOkAndHolds(MatchesRegex(R"regex(List:\$[0-9a-zA-Z]{22})regex")));
  }
  {
    ObjectId entity_id = internal::AllocateExplicitSchema();
    DataSlice item = test::DataItem<ObjectId>(entity_id, schema::kItemId);
    EXPECT_THAT(
        DataSliceToStr(item),
        IsOkAndHolds(MatchesRegex(R"regex(Schema:\$[0-9a-zA-Z]{22})regex")));
  }
}

TEST(DataSliceReprTest, CycleInDict) {
  DataBagPtr bag = DataBag::EmptyMutable();

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
  DataBagPtr bag = DataBag::EmptyMutable();

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
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));
  DataSlice schema = entity.GetSchema();

  ASSERT_OK(schema.SetAttr("a", schema));

  EXPECT_THAT(
      DataSliceToStr(schema),
      IsOkAndHolds(MatchesRegex(
          R"regex(ENTITY\(a=ENTITY\(a=ENTITY\(a=ENTITY\(a=ENTITY\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));

  ASSERT_OK(entity.SetAttr("a", entity));
  EXPECT_THAT(
      DataSliceToStr(entity),
      IsOkAndHolds(MatchesRegex(
          R"regex(Entity\(a=Entity\(a=Entity\(a=Entity\(a=Entity\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));
}

TEST(DataSliceReprTest, CycleInObject) {
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a"}, {value_1}));
  ASSERT_OK_AND_ASSIGN(DataSlice schema, obj.GetAttr(schema::kSchemaAttr));

  ASSERT_OK(schema.SetAttr("a", schema));

  EXPECT_THAT(DataSliceToStr(schema), IsOkAndHolds(MatchesRegex(
                                          R"regex(IMPLICIT_ENTITY\(
  a=IMPLICIT_ENTITY\(a=IMPLICIT_ENTITY\(a=IMPLICIT_ENTITY\(a=IMPLICIT_ENTITY\(a=#[0-9a-zA-Z]{22}\)\)\)\),
\))regex")));

  ASSERT_OK(obj.SetAttr("a", obj));
  EXPECT_THAT(
      DataSliceToStr(obj),
      IsOkAndHolds(MatchesRegex(
          R"regex(Obj\(a=Obj\(a=Obj\(a=Obj\(a=Obj\(a=\$[0-9a-zA-Z]{22}\)\)\)\)\))regex")));
}

TEST(DataSliceReprTest, DictExceedReprItemLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();
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
  DataBagPtr bag = DataBag::EmptyMutable();

  DataSlice list_item = test::DataSlice<int>({1, 2, 3, 4, 5, 6});

  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list_item));

  EXPECT_THAT(DataSliceToStr(list, {.item_limit = 5}),
              IsOkAndHolds("List[1, 2, 3, 4, 5, ...]"));
}

TEST(DataSliceReprTest, NestedListExceedReprItemLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();

  std::vector<arolla::OptionalValue<int64_t>> input;
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      input.emplace_back(j);
    }
  }

  std::vector<int64_t> edge2_split_points;
  for (int i = 0; i <= 10; ++i) {
    edge2_split_points.emplace_back(i * 10);
  }
  auto ds_shape = test::ShapeFromSplitPoints({{0, 10}, edge2_split_points});

  arolla::DenseArray<int64_t> ds_array =
      arolla::CreateDenseArray<int64_t>(absl::MakeSpan(input));
  ASSERT_OK_AND_ASSIGN(
      DataSlice ds,
      DataSlice::CreatePrimitive(std::move(ds_array), std::move(ds_shape)));
  ASSERT_OK_AND_ASSIGN(DataSlice list, CreateNestedList(bag, ds));
  EXPECT_THAT(DataSliceToStr(list, {.item_limit = 5}), IsOkAndHolds(R"(List[
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  List[0, 1, 2, 3, 4, ...],
  ...,
])"));
}

TEST(DataSliceReprTest, ObjEntityExceedReprItemLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"a", "b", "c", "d", "e", "f"},
                               std::vector<DataSlice>(6, test::DataItem(1))));

  EXPECT_THAT(DataSliceToStr(obj, {.item_limit = 5}),
              IsOkAndHolds("Obj(a=1, b=1, c=1, d=1, e=1, ...)"));

  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"a", "b", "c", "d", "e", "f"},
                               std::vector<DataSlice>(6, test::DataItem(1))));

  EXPECT_THAT(DataSliceToStr(entity, {.item_limit = 5}),
              IsOkAndHolds("Entity(a=1, b=1, c=1, d=1, e=1, ...)"));
}

TEST(DataSliceReprTest, ObjEntityMultiline) {
  DataBagPtr bag = DataBag::EmptyMutable();

  std::string large_attr0(50, 'a');
  std::string large_attr1(50, 'x');
  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {large_attr0, large_attr1},
                               std::vector<DataSlice>(2, test::DataItem(1))));

  EXPECT_THAT(DataSliceToStr(obj, {.item_limit = 5}),
              IsOkAndHolds(absl::StrFormat("Obj(\n  %s=1,\n  %s=1,\n)",
                                           large_attr0, large_attr1)));
}

TEST(DataSliceReprTest, FormatHtml_AttrSpan) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(bag, test::DataSlice<int>({}),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));

  std::vector<DataSlice> attr_values = {test::DataItem(1), data_slice};
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       ObjectCreator::FromAttrs(bag, {"a", "b"}, attr_values));

  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(obj, {.format_html = true}));
  EXPECT_THAT(result, HasSubstr("<span class=\"attr\">a</span>=1"));
  EXPECT_THAT(result, HasSubstr("<span class=\"attr\">b</span>=List[]"));

  // Sanity check that the low level api works.
  ASSERT_OK_AND_ASSIGN(
      result, DataItemToStr(obj.item(), obj.GetSchemaImpl(), obj.GetBag(),
                            {.format_html = true}));
  EXPECT_THAT(result, HasSubstr("<span class=\"attr\">a</span>=1"));
  EXPECT_THAT(result, HasSubstr("<span class=\"attr\">b</span>=List[]"));
}

TEST(DataSliceReprTest, FormatHtml_SliceIndices) {
  auto ds_shape = test::ShapeFromSplitPoints({{0, 2}, {0, 2, 3}});

  DataSlice ds = test::DataSlice<int>({1, 2, 3}, std::move(ds_shape));

  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(ds, {.format_html = true}));
  EXPECT_EQ(
      result,
      "[<span slice-index=\"0\">["
      "<span slice-index=\"0\">1</span>, <span slice-index=\"1\">2</span>"
      "]</span>, "
      "<span slice-index=\"1\">[<span slice-index=\"0\">3</span>]</span>]");
}

TEST(DataSliceReprTest, FormatHtml_ListIndices) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(DataSlice list_item,
                       CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                                        empty_list.GetSchema()));
  ASSERT_OK_AND_ASSIGN(
      DataSlice ds,
      CreateNestedList(bag,
                       test::DataSlice<ObjectId>(
                           {empty_list.item().value<ObjectId>(),
                            list_item.item().value<ObjectId>()},
                           DataSlice::JaggedShape::FlatFromSize(2),
                           list_item.GetSchema().item().value<ObjectId>())));
  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(ds, {.format_html = true}));
  EXPECT_EQ(result,
            "List["
            "<span list-index=\"0\">List[]</span>, "
            "<span list-index=\"1\">List["
            "<span list-index=\"0\">1</span>, "
            "<span list-index=\"1\">2</span>, "
            "<span list-index=\"2\">3</span>"
            "]</span>]");
}

TEST(DataSliceReprTest, FormatHtml_ListSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      DataSlice list_item,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));
  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(list_item.GetSchema(), {.format_html = true}));
  EXPECT_EQ(result, "LIST[<span item-schema=\"\">OBJECT</span>]");
}

TEST(DataSliceReprTest, FormatHtml_ObjEntity) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"a<>", "b\"&"},
                               std::vector<DataSlice>(2, test::DataItem(1))));
  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(obj, {.format_html = true}));
  EXPECT_EQ(
      result,
      "Obj("
      "<span schema-attr=\"a&lt;&gt;\"><span class=\"attr\">a&lt;&gt;"
      "</span>=1</span>, "
      "<span schema-attr=\"b&quot;&amp;\"><span class=\"attr\">b&quot;&amp;"
      "</span>=1</span>)");
}

TEST(DataSliceReprTest, FormatHtml_ObjEntity_NoMultiLineForKeyNearLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();
  std::string lots_of_amps(80, '&');
  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {lots_of_amps},
                               std::vector<DataSlice>(1, test::DataItem(1))));
  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(obj, {.format_html = true}));
  EXPECT_THAT(result, Not(HasSubstr("\n")));
}

TEST(DataSliceReprTest, FormatHtml_Dict) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"\n\t<>&\""});
  DataSlice values = test::DataSlice<int>({1});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(data_slice, {.format_html = true}));
  EXPECT_EQ(result,
            "Dict{"
            "<span dict-key-index=\"0\">'\\n\\t&lt;&gt;&amp;&quot;'</span>="
            "<span dict-value-index=\"0\">1</span>}");
}

TEST(DataSliceReprTest, FormatHtml_DictLongStrings) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kString)));
  ObjectId dict_id = internal::AllocateSingleDict();

  std::string long_string_a(20, 'a');
  std::string long_string_b(20, 'b');
  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice keys = test::DataSlice<arolla::Text>({long_string_a.c_str()});
  DataSlice values = test::DataSlice<arolla::Text>({long_string_b.c_str()});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(data_slice,
                     {.format_html = true, .unbounded_type_max_len = 10}));
  EXPECT_THAT(result, HasSubstr("a'<span class=\"truncated\">...</span>'"));
  EXPECT_THAT(result, HasSubstr("b'<span class=\"truncated\">...</span>'"));
}

TEST(DataSliceReprTest, FormatHtml_TruncationEscaping) {
  // Sanity check that we don't accidentally match `'...'` already in the
  // string.
  {
    // DataSlice
    DataSlice slice = test::DataSlice<arolla::Text>({"'abc'...'def'"});
    ASSERT_OK_AND_ASSIGN(
        auto result, DataSliceToStr(slice, {.format_html = true,
                                            .unbounded_type_max_len = 100}));
    EXPECT_THAT(result, HasSubstr("\\'abc\\'...\\'def\\'"));
    ASSERT_OK_AND_ASSIGN(result,
                         DataSliceToStr(slice, {.strip_quotes = true,
                                                .format_html = true,
                                                .unbounded_type_max_len = 100,
                                                .show_attributes = false}));
    EXPECT_THAT(result, HasSubstr("\\'abc\\'...\\'def\\'"));
  }
  {
    // DataItem
    DataSlice item = test::DataItem(arolla::Text("'abc'...'def'"));
    ASSERT_OK_AND_ASSIGN(auto result,
                         DataSliceToStr(item, {.format_html = true,
                                               .unbounded_type_max_len = 100}));
    EXPECT_THAT(result, HasSubstr("\\'abc\\'...\\'def\\'"));
    ASSERT_OK_AND_ASSIGN(result,
                         DataSliceToStr(item, {.strip_quotes = true,
                                               .format_html = true,
                                               .unbounded_type_max_len = 100,
                                               .show_attributes = false}));
    // TODO: Do not parse `'...'` as truncated.
    // EXPECT_THAT(result, HasSubstr("\\'abc\\'...\\'def\\'"));
  }
}

TEST(DataSliceReprTest, FormatHtml_DictObjectIdKey) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_key_schema,
                       CreateDictSchema(bag, test::Schema(schema::kInt32),
                                        test::Schema(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(
      auto dict_schema,
      CreateDictSchema(bag, dict_key_schema, test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();

  ObjectId dict_as_key = internal::AllocateSingleDict();
  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice values = test::DataSlice<int>({1});

  ASSERT_OK(data_slice.SetInDict(
      test::DataSlice<ObjectId>({dict_as_key},
                                dict_key_schema.item().value<ObjectId>(), bag),
      values));
  EXPECT_THAT(DataSliceToStr(data_slice, {.format_html = true}),
              IsOkAndHolds(absl::StrFormat(
                  "Dict{"
                  "<span dict-key-index=\"0\"><span class=\"object-id\">"
                  "%s</span></span>="
                  "<span dict-value-index=\"0\">1</span>}",
                  ObjectIdStr(dict_as_key))));
}

TEST(DataSliceReprTest, FormatHtml_Dict_NoMultiLineForKeyNearLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  std::string lots_of_amps(80, '&');
  ObjectId dict_id = internal::AllocateSingleDict();
  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice keys = test::DataSlice<arolla::Text>({lots_of_amps.c_str()});
  DataSlice values = test::DataSlice<int>({1});
  ASSERT_OK(data_slice.SetInDict(keys, values));
  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(data_slice, {.format_html = true}));
  EXPECT_THAT(result, Not(HasSubstr("\n")));
}

TEST(DataSliceReprTest, FormatHtml_DictSchema) {
  DataBagPtr bag = DataBag::EmptyMutable();
  auto dict_schema = *CreateDictSchema(bag, test::Schema(schema::kInt32),
                                       test::Schema(schema::kInt32));
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      test::DataSlice<ObjectId>({internal::AllocateSingleDict()}, bag)
          .WithSchema(dict_schema));
  DataSlice keys = test::DataSlice<int>({0});
  DataSlice values = test::DataSlice<int>({1});
  ASSERT_OK(data_slice.SetInDict(keys, values));
  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(data_slice.GetSchema(), {.format_html = true}));
  EXPECT_EQ(result,
            "DICT{<span key-schema=\"\">INT32</span>, "
            "<span value-schema=\"\">INT32</span>}");
}

TEST(DataSliceReprTest, FormatHtml_ByteValues) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kInt32),
                                        test::Schema(schema::kBytes)));
  ObjectId dict_id = internal::AllocateSingleDict();

  DataSlice data_slice = test::DataItem(dict_id, dict_schema.item(), bag);
  char bytes[] = {16, 127, '<', 0};
  DataSlice keys = test::DataSlice<int>({1});
  DataSlice values = test::DataSlice<arolla::Bytes>({bytes});
  ASSERT_OK(data_slice.SetInDict(keys, values));

  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(data_slice, {.format_html = true}));
  EXPECT_EQ(
      result,
      R"RAW(Dict{<span dict-key-index="0">1</span>=<span dict-value-index="0">b'\x10\x7f&lt;'</span>})RAW");
}

TEST(DataSliceReprTest, FormatHtml_ObjectId_Slice) {
  ObjectId list_id_1 = internal::AllocateSingleList();
  DataSlice ds = test::DataSlice<ObjectId>({list_id_1});

  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(ds, {.format_html = true}));
  EXPECT_THAT(result, HasSubstr("object-id"));
}

TEST(DataSliceReprTest, FormatHtml_ObjectId_MaxDepth) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(DataSlice empty_list,
                       CreateEmptyList(bag, /*schema=*/std::nullopt,
                                       test::Schema(schema::kObject)));
  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(empty_list, {.depth = 0, .format_html = true}));
  EXPECT_THAT(result, HasSubstr("object-id"));
}

TEST(DataSliceReprTest, FormatHtml_ObjectId_NoBag) {
  DataBagPtr bag = DataBag::EmptyMutable();
  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));

  entity = entity.WithBag(/*db=*/nullptr);
  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(entity, {.depth = 100, .format_html = true}));
  EXPECT_THAT(result, HasSubstr("object-id"));
}

TEST(DataSliceReprTest, FormatHtml_ObjectId_NoFollow) {
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      DataSlice entity,
      EntityCreator::FromAttrs(bag, {"y"}, {test::DataItem<int>(1)}));
  ASSERT_OK_AND_ASSIGN(DataSlice nofollow_entity, NoFollow(entity));

  ASSERT_OK_AND_ASSIGN(
      std::string result,
      DataSliceToStr(nofollow_entity, {.depth = 100, .format_html = true}));
  // NoFollow entities shows normal text.
  EXPECT_THAT(result, Not(HasSubstr("object-id")));
}

TEST(DataSliceReprTest, FormatHtml_ObjectId_ItemId_NoObjctIdHtmlAttr) {
  DataBagPtr bag = DataBag::EmptyMutable();
  DataSlice value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(bag, {"a"}, {value_1}));
  DataSlice item = test::DataItem<ObjectId>(entity.item().value<ObjectId>(),
                                            schema::kItemId);
  ASSERT_OK_AND_ASSIGN(std::string result,
                       DataSliceToStr(item, {.format_html = true}));
  EXPECT_THAT(result, Not(HasSubstr("object-id")));
}

TEST(DataSliceReprTest, UnboundedTypeMaxLength) {
  // We only need to test two cases here because other call sites of
  // DataItemRepr do not need this param. In those other call sites, we know
  // the DataItem does not contain an unbounded type.
  DataBagPtr bag = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kString)));
  ObjectId dict_id = internal::AllocateSingleDict();

  std::string large_str(50, 'a');
  DataSlice dict = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice key = test::DataSlice<arolla::Text>({large_str.c_str()});
  DataSlice value = test::DataSlice<arolla::Text>({large_str.c_str()});
  ASSERT_OK(dict.SetInDict(key, value));

  EXPECT_THAT(DataSliceToStr(key, {.unbounded_type_max_len = 3}),
              IsOkAndHolds("['aa'...'aa']"));
  EXPECT_THAT(DataSliceToStr(dict, {.unbounded_type_max_len = 3}),
              IsOkAndHolds("Dict{'aa'...'aa'='aa'...'aa'}"));
}

TEST(DataSliceReprTest, FormatHtml_ItemLimit) {
  DataBagPtr bag = DataBag::EmptyMutable();

  // DataSlice case
  EXPECT_THAT(DataSliceToStr(test::DataSlice<int>({1, 2, 3}),
                             {
                                 .item_limit = 1,
                                 .format_html = true,
                             }),
              IsOkAndHolds(HasSubstr("<span class=\"limited\">...</span>")));

  // List case
  DataSlice list_item = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateListShaped(bag, DataSlice::JaggedShape::Empty(), list_item));
  EXPECT_THAT(DataSliceToStr(list, {.item_limit = 1, .format_html = true}),
              IsOkAndHolds(HasSubstr("<span class=\"limited\">...</span>")));

  // Dict case
  ASSERT_OK_AND_ASSIGN(auto dict_schema,
                       CreateDictSchema(bag, test::Schema(schema::kString),
                                        test::Schema(schema::kInt32)));
  ObjectId dict_id = internal::AllocateSingleDict();
  DataSlice dict = test::DataItem(dict_id, dict_schema.item(), bag);
  DataSlice keys = test::DataSlice<arolla::Text>({"a", "x"});
  DataSlice values = test::DataSlice<int>({1, 4});
  ASSERT_OK(dict.SetInDict(keys, values));
  EXPECT_THAT(DataSliceToStr(dict, {.item_limit = 1, .format_html = true}),
              IsOkAndHolds(HasSubstr("<span class=\"limited\">...</span>")));

  // Object case
  ASSERT_OK_AND_ASSIGN(
      DataSlice obj,
      ObjectCreator::FromAttrs(bag, {"a", "b"},
                               std::vector<DataSlice>(2, test::DataItem(1))));
  EXPECT_THAT(DataSliceToStr(obj, {.item_limit = 1, .format_html = true}),
              IsOkAndHolds(HasSubstr("<span class=\"limited\">...</span>")));
}

TEST(DataSliceReprTest, DataSliceRepr) {
  // NOTE: More extensive repr tests are done in Python.
  auto db = DataBag::EmptyMutable();
  {
    // DataSlice repr without db.
    auto ds = test::DataSlice<int>({1});
    EXPECT_THAT(DataSliceRepr(ds),
                Eq("DataSlice([1], schema: INT32, shape: JaggedShape(1))"));
  }
  {
    // DataSlice repr with db.
    auto ds = test::DataSlice<int>({1}, schema::kInt32, db);
    std::string expected_repr = absl::StrFormat(
        "DataSlice([1], schema: INT32, shape: JaggedShape(1), bag_id: $%s)",
        arolla::TypedValue::FromValue(db).GetFingerprint().AsString().substr(
            32 - 4));
    EXPECT_THAT(DataSliceRepr(ds), Eq(expected_repr));
  }
  {
    // DataItem repr without db.
    auto ds = test::DataItem(1);
    EXPECT_THAT(DataSliceRepr(ds), Eq("DataItem(1, schema: INT32)"));
  }
  {
    // DataItem repr with db.
    auto ds = test::DataItem(1, db);
    std::string expected_repr = absl::StrFormat(
        "DataItem(1, schema: INT32, bag_id: $%s)",
        arolla::TypedValue::FromValue(db).GetFingerprint().AsString().substr(
            32 - 4));
    EXPECT_THAT(DataSliceRepr(ds), Eq(expected_repr));
  }
}

TEST(DataSliceReprTest, DataSliceRepr_ShowAttribute) {
  auto db = DataBag::EmptyMutable();

  auto value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(db, {"a"}, {value_1}));
  ASSERT_OK_AND_ASSIGN(DataSlice entity_2,
                       EntityCreator::FromAttrs(db, {"b"}, {entity}));

  EXPECT_THAT(
      DataSliceRepr(entity_2, {.show_attributes = true,
                               .show_databag_id = false,
                               .show_shape = false}),
      Eq("DataSlice([Entity(b=Entity(a=1)), Entity(b=Entity(a=2))], schema: "
         "ENTITY(b=ENTITY(a=INT32)))"));
}

TEST(DataSliceReprTest, DataSliceRepr_NoSchemaDataBagOrShape) {
  auto db = DataBag::EmptyMutable();
  auto value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(db, {"a"}, {value_1}));
  ASSERT_OK_AND_ASSIGN(DataSlice entity_2,
                       EntityCreator::FromAttrs(db, {"b"}, {entity}));
  EXPECT_THAT(DataSliceRepr(entity_2,
                            {
                                .show_attributes = true,
                                .show_databag_id = false,
                                .show_shape = false,
                                .show_schema = false,
                                .show_present_count = false,
                            }),
              Eq("[Entity(b=Entity(a=1)), Entity(b=Entity(a=2))]"));
}

TEST(DataSliceReprTest, DataSliceRepr_ShowPresentCount) {
  {
    // DataSlice - default behavior.
    auto slice = test::DataSlice<int>({1, 2, std::nullopt});
    EXPECT_THAT(DataSliceRepr(slice, {.show_shape = false,
                                      .show_schema = false}),
                Eq("DataSlice([1, 2, None])"));
  }
  {
    // DataSlice - enabled.
    auto slice = test::DataSlice<int>({1, 2, std::nullopt});
    EXPECT_THAT(DataSliceRepr(slice, {.show_shape = false,
                                      .show_schema = false,
                                      .show_present_count = true}),
                Eq("DataSlice([1, 2, None], present: 2/3)"));
  }
  {
    // DataSlice - disabled.
    auto slice = test::DataSlice<int>({1, 2, std::nullopt});
    EXPECT_THAT(DataSliceRepr(slice, {.show_shape = false,
                                      .show_schema = false,
                                      .show_present_count = false}),
                Eq("DataSlice([1, 2, None])"));
  }
  {
    // DataSlice - with truncation.
    auto slice = test::DataSlice<int>({1, 2, std::nullopt});
    EXPECT_THAT(DataSliceRepr(slice, {.item_limit = 2,
                                      .show_shape = false,
                                      .show_schema = false,
                                      .show_present_count = true}),
                Eq("DataSlice([1, 2, ...], present: 2/3)"));
  }
  {
    // DataItem - always disabled.
    auto slice = test::DataItem<int>(1);
    EXPECT_THAT(DataSliceRepr(slice, {.show_shape = false,
                                      .show_schema = false,
                                      .show_present_count = true}),
                Eq("DataItem(1)"));
  }
}

TEST(DataSliceReprTest, DataSliceRepr_OnlyShowAttrNamesOnLargeEntityDataSlice) {
  {
    // Entities with entity schema.
    auto db = DataBag::EmptyMutable();
    auto value = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    ASSERT_OK_AND_ASSIGN(DataSlice entity_1,
                         EntityCreator::FromAttrs(db, {"a"}, {value}));
    ASSERT_OK_AND_ASSIGN(
        DataSlice entity_2,
        EntityCreator::FromAttrs(db, {"a", "b"}, {value, entity_1}));
    EXPECT_THAT(
        DataSliceRepr(entity_2, {.item_limit = 2,
                                 .item_limit_per_dimension = 2,
                                 .show_attributes = true,
                                 .show_databag_id = false,
                                 .show_shape = false,
                                 .show_present_count = false}),
        MatchesRegex(
            R"regexp(DataSlice\(attrs: \[a, b\], schema: ENTITY\(a=INT32, b=\$[0-9a-zA-Z]{22}\)\))regexp"));
  }
  {
    // Entities with OBJECT schema.
    auto db = DataBag::EmptyMutable();
    auto value_1 = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    ASSERT_OK_AND_ASSIGN(
        DataSlice obj,
        ObjectCreator::FromAttrs(db, {"a", "b"}, {value_1, value_1}));
    EXPECT_THAT(DataSliceRepr(obj, {.item_limit = 2,
                                    .item_limit_per_dimension = 2,
                                    .show_attributes = true,
                                    .show_databag_id = false,
                                    .show_shape = false,
                                    .show_present_count = false}),
                Eq("DataSlice(attrs: [a, b], schema: OBJECT)"));
  }
}

TEST(DataSliceReprTest, DataSliceRepr_DoNotShowAttrNamesOnLargeDataSlice) {
  {
    // Primirive DataSlice.
    auto db = DataBag::EmptyMutable();
    auto ints = test::DataSlice<int>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    EXPECT_THAT(DataSliceRepr(ints, {.item_limit = 2,
                                     .item_limit_per_dimension = 2,
                                     .show_attributes = true,
                                     .show_databag_id = false,
                                     .show_shape = false,
                                     .show_present_count = false}),
                Eq("DataSlice([1, 2, ...], schema: INT32)"));
  }
  {
    // Mixed OBJECT DataSlice with entities and primitives.
    auto db = DataBag::EmptyMutable();
    auto value_1 = test::DataSlice<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto obj, ObjectCreator::FromAttrs(db, {"a", "b"}, {value_1, value_1}));
    auto ids = obj.slice().values<ObjectId>();
    auto mixed_ds = test::MixedDataSlice<int, ObjectId>(
                        {42, std::nullopt, std::nullopt, std::nullopt},
                        {std::nullopt, ids[0], ids[1], ids[2]})
                        .WithBag(db);
    EXPECT_THAT(DataSliceRepr(mixed_ds, {.item_limit = 2,
                                         .item_limit_per_dimension = 2,
                                         .show_attributes = true,
                                         .show_databag_id = false,
                                         .show_shape = false,
                                         .show_present_count = false}),
                Eq("DataSlice([42, Obj(a=1, b=1), ...], schema: OBJECT)"));
  }
}

TEST(DataSliceReprTest, DataSliceRepr_WithNamedSchema) {
  auto db = DataBag::EmptyMutable();

  ASSERT_OK_AND_ASSIGN(
      DataSlice schema,
      CreateNamedSchema(db, "foo", {"a"}, {test::Schema(schema::kInt64)}));
  auto value_1 = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(db, {"a"}, {value_1}, schema));

  EXPECT_THAT(
      DataSliceRepr(entity, {.show_attributes = true,
                             .show_databag_id = false,
                             .show_shape = false,
                             .show_present_count = false}),
      Eq("DataSlice([Entity(a=1), Entity(a=2)], schema: foo(a=INT64))"));
}

TEST(DataSliceReprTest, DataSliceRepr_ShowItemId) {
  auto db = DataBag::EmptyMutable();
  auto value_1 = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(DataSlice entity,
                       EntityCreator::FromAttrs(db, {"a"}, {value_1}));
  EXPECT_THAT(
      DataSliceRepr(entity,
                    {
                        .show_attributes = true,
                        .show_databag_id = false,
                        .show_shape = false,
                        .show_item_id = true,
                        .show_present_count = false,
                    }),
      MatchesRegex(
          R"regexp(DataItem\(Entity\(a=1\), schema: ENTITY\(a=INT32\), item_id: Entity:\$[0-9a-zA-Z]{22}\))regexp"));
}

TEST(DataSliceReprTest, DataSliceRepr_ShowItemIdList) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      DataSlice data_slice,
      CreateNestedList(db, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kObject)));
  EXPECT_THAT(
      DataSliceRepr(data_slice,
                    {
                        .show_attributes = true,
                        .show_databag_id = false,
                        .show_shape = false,
                        .show_item_id = true,
                    }),
      MatchesRegex(
          R"regexp(DataItem\(List\[1, 2, 3\], schema: LIST\[OBJECT\], item_id: List:\$[0-9a-zA-Z]{22}\))regexp"));
}

TEST(DataSliceReprTest, SchemaToStr) {
  EXPECT_THAT(SchemaToStr(test::Schema(schema::kObject)), Eq("OBJECT"));
  EXPECT_THAT(SchemaToStr(test::Schema(schema::kInt32)), Eq("INT32"));
  EXPECT_THAT(SchemaToStr(*CreateListSchema(DataBag::EmptyMutable(),
                                            test::Schema(schema::kInt32))),
              Eq("LIST[INT32]"));
}

}  // namespace
}  // namespace koladata
