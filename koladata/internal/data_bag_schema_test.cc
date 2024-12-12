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
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::internal::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::Not;
using ::testing::UnorderedElementsAre;

const internal::DataItem& GetAnySchema() {
  static const absl::NoDestructor<internal::DataItem> kAnySchema(schema::kAny);
  return *kAnySchema;
}

const internal::DataItem& GetIntSchema() {
  static const absl::NoDestructor<internal::DataItem> kIntSchema(
      schema::kInt32);
  return *kIntSchema;
}

const internal::DataItem& GetFloatSchema() {
  static const absl::NoDestructor<internal::DataItem> kFloatSchema(
      schema::kFloat32);
  return *kFloatSchema;
}

AllocationId GenerateImplicitSchemas(size_t size) {
  return AllocationId(
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          Allocate(size).ObjectByOffset(0),
          arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()));
}

TEST(DataBagTest, EmptySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_item = DataItem(AllocateExplicitSchema());

  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema_slice = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));


  ASSERT_OK_AND_ASSIGN(auto schema_attrs, db->GetSchemaAttrs(schema_item));
  EXPECT_THAT(schema_attrs, ElementsAre());
  EXPECT_TRUE(schema_attrs.is_empty_and_unknown());

  EXPECT_THAT(db->GetSchemaAttr(schema_item, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrAllowMissing(schema_item, "a"),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema_slice, "a"),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(DataSliceImpl::CreateEmptyAndUnknownType(3),
                                    "a"),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(schema_attrs,
                       db->GetSchemaAttrs(schema_item, {fb_db.get()}));
  EXPECT_THAT(schema_attrs, ElementsAre());
  EXPECT_TRUE(schema_attrs.is_empty_and_unknown());
  EXPECT_THAT(db->GetSchemaAttr(schema_item, "a", {fb_db.get()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrAllowMissing(schema_item, "a", {fb_db.get()}),
              IsOkAndHolds(DataItem()));

  EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a", {fb_db.get()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema_slice, "a", {fb_db.get()}),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, GetSetSchemaAttr_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  // Setting on an empty item is a no-op.
  ASSERT_OK(db->SetSchemaAttr(DataItem(), "a", GetFloatSchema()));
  EXPECT_THAT(db->GetSchemaAttr(DataItem(), "a"), IsOkAndHolds(DataItem()));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttrs(schema),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"))));

  ASSERT_OK(db->SetSchemaAttr(schema, "b", GetFloatSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(GetFloatSchema()));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"))));

  auto schema_2 = DataItem(AllocateExplicitSchema());
  ASSERT_OK(db->SetSchemaAttr(schema, "c", schema_2));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_2));
  EXPECT_THAT(db->GetSchemaAttrs(schema),
              IsOkAndHolds(UnorderedElementsAre(
                  arolla::Text("a"), arolla::Text("b"), arolla::Text("c"))));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(fb_db->SetSchemaAttr(schema, "d", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c", {fb_db.get()}),
              IsOkAndHolds(schema_2));
  EXPECT_THAT(db->GetSchemaAttr(schema, "d", {fb_db.get()}),
              IsOkAndHolds(GetIntSchema()));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema, {fb_db.get()}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                        arolla::Text("c"), arolla::Text("d"))));

  EXPECT_THAT(db->GetSchemaAttrs(DataItem(), {}), IsOkAndHolds(ElementsAre()));
}

TEST(DataBagTest, GetSetSchemaAttr_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(6);
  // First item is None.
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    std::nullopt, schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values_a = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, std::nullopt}),
      arolla::CreateDenseArray<ObjectId>({
        std::nullopt, std::nullopt, AllocateExplicitSchema()}));

  // Setting on an empty slice is a no-op.
  ASSERT_OK(db->SetSchemaAttr(DataSliceImpl::CreateEmptyAndUnknownType(2), "a",
                              values_a));
  ASSERT_OK(
      db->SetSchemaAttr(
          DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
              {std::nullopt, std::nullopt})), "a",
      values_a));
  EXPECT_THAT(
      db->GetSchemaAttr(DataSliceImpl::CreateEmptyAndUnknownType(2), "a"),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(2))));
  EXPECT_THAT(
      db->GetSchemaAttr(
          DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
              {std::nullopt, std::nullopt})), "a"),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(2))));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", values_a));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              IsOkAndHolds(
                  ElementsAre(std::nullopt, GetIntSchema(), values_a[2])));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(ElementsAre(arolla::Text("a"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(ElementsAre(arolla::Text("a"))));

  // Setting a single item on a schema slice.
  ASSERT_OK(db->SetSchemaAttr(schema, "b", GetFloatSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"),
              IsOkAndHolds(ElementsAre(std::nullopt, GetFloatSchema(),
                                       GetFloatSchema())));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"))));

  auto schema_2 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(3), schemas_alloc.ObjectByOffset(4),
    schemas_alloc.ObjectByOffset(5)
  }));
  ASSERT_OK(db->SetSchemaAttr(schema, "c", schema_2));
  EXPECT_THAT(
      db->GetSchemaAttr(schema, "c"),
      IsOkAndHolds(ElementsAre(std::nullopt, schema_2[1], schema_2[2])));
  EXPECT_THAT(db->GetSchemaAttrs(schema[1]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c"))));
  EXPECT_THAT(db->GetSchemaAttrs(schema[2]),
              IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c"))));

  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(fb_db->SetSchemaAttr(schema, "d",
                                 DataSliceImpl::Create(3, GetIntSchema())));
  EXPECT_THAT(
      db->GetSchemaAttr(schema, "c", {fb_db.get()}),
      IsOkAndHolds(ElementsAre(std::nullopt, schema_2[1], schema_2[2])));
  EXPECT_THAT(db->GetSchemaAttr(schema, "d", {fb_db.get()}),
              IsOkAndHolds(
                  ElementsAre(std::nullopt, GetIntSchema(), GetIntSchema())));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema[1], {fb_db.get()}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                        arolla::Text("c"), arolla::Text("d"))));
  EXPECT_THAT(
      db->GetSchemaAttrs(schema[1], {}),
      IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"),
                                        arolla::Text("b"),
                                        arolla::Text("c"))));
}

TEST(DataBagTest, SetSchemaAttrOverwrite_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetAnySchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetAnySchema()));

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a")));
}

TEST(DataBagTest, SetSchemaAttrOverwrite_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, schema::kFloat32}),
      arolla::CreateDenseArray<ObjectId>({
        AllocateExplicitSchema(), std::nullopt, std::nullopt}));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  ASSERT_OK_AND_ASSIGN(auto schema_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(schema_get_a, IsEquivalentTo(values));

  ASSERT_OK(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, GetAnySchema())));
  ASSERT_OK_AND_ASSIGN(schema_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(schema_get_a,
              ElementsAre(GetAnySchema(), GetAnySchema(), GetAnySchema()));

  for (int i = 0; i < schema.size(); ++i) {
    EXPECT_THAT(db->GetSchemaAttrs(schema[i]),
                IsOkAndHolds(UnorderedElementsAre(arolla::Text("a"))));
  }
}

TEST(DataBagTest, SetSchemaAttrErrors_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());
  auto dict = DataItem(AllocateSingleDict());

  // Disallow updating schema through a non-schema API:
  EXPECT_THAT(db->SetSchemaAttr(dict, "a", GetIntSchema()),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("schema expected")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: None")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(42)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(AllocateSingleDict())),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: ")));

  EXPECT_THAT(
      db->GetSchemaAttr(GetAnySchema(), "any_attr"),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("cannot get or set attributes on schema: ANY")));
  EXPECT_THAT(
      db->SetSchemaAttr(GetIntSchema(), "any_attr", GetIntSchema()),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               HasSubstr("cannot get or set attributes on schema: INT32")));
}

TEST(DataBagTest, SetSchemaAttrErrors_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto schema_with_const_schemas = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, std::nullopt, schema::kInt32}),
      arolla::CreateDenseArray<ObjectId>({
        schemas_alloc.ObjectByOffset(0), std::nullopt, std::nullopt}));
  auto dict = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    AllocateSingleDict(), AllocateSingleDict(), AllocateSingleDict()}));

  auto valid_values = DataSliceImpl::Create(3, GetIntSchema());

  // Disallow updating schema through a non-schema API:
  EXPECT_THAT(
      db->SetSchemaAttr(dict, "a", valid_values),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(dict, "any_attr", GetIntSchema()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, DataItem(42))),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "cannot set a non-schema slice [42, 42, 42] as a schema attribute"));

  EXPECT_THAT(db->SetSchemaAttr(schema, "a", DataItem(42)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes "
                                 "of schemas, got: 42")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::AllocateEmptyObjects(3)),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          MatchesRegex(
              R"(cannot set a non-schema slice \[.*, .*, .*\] as a schema attribute)")));

  // Mixed with some non schemas.
  auto mixed_with_non_schemas = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, std::nullopt, schema::kInt32}),
      arolla::CreateDenseArray<int>({42, std::nullopt, std::nullopt}));
  EXPECT_THAT(db->SetSchemaAttr(schema, "a", mixed_with_non_schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot set a non-schema slice [42, None, INT32] as a "
                       "schema attribute"));

  // Basically a no-op.
  ASSERT_OK(
      db->SetSchemaAttr(schema, "a", DataSliceImpl::Create(3, DataItem())));

  EXPECT_THAT(
      db->GetSchemaAttr(schema_with_const_schemas, "any_attr"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot get schema attribute of a non-schema slice [")));

  EXPECT_THAT(
      db->SetSchemaAttr(schema_with_const_schemas, "any_attr", GetIntSchema()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("cannot set schema attribute of a non-schema slice [")));

  // Result has less items than schema.
  auto values = DataSliceImpl::Create(arolla::CreateDenseArray<schema::DType>({
    schema::kInt32, std::nullopt, std::nullopt}));
  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      db->GetSchemaAttrAllowMissing(schema, "a"),
      IsOkAndHolds(ElementsAre(schema::kInt32, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, DelSchemaAttr_Item) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateExplicitSchema());

  ASSERT_OK(db->SetSchemaAttr(schema, "a", GetIntSchema()));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(GetIntSchema()));

  EXPECT_THAT(db->DelSchemaAttr(schema, "b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));

  ASSERT_OK(db->DelSchemaAttr(schema, "a"));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(db->GetSchemaAttrs(schema), IsOkAndHolds(ElementsAre()));

  // Deleting on an empty object is a no-op.
  ASSERT_OK(db->DelSchemaAttr(DataItem(), "a"));
}

TEST(DataBagTest, DelSchemaAttr_Slice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schemas_alloc = GenerateImplicitSchemas(3);
  auto schema = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({
    schemas_alloc.ObjectByOffset(0), schemas_alloc.ObjectByOffset(1),
    schemas_alloc.ObjectByOffset(2)
  }));
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<schema::DType>({
        std::nullopt, schema::kInt32, schema::kFloat32}),
      arolla::CreateDenseArray<ObjectId>({
        AllocateExplicitSchema(), std::nullopt, std::nullopt}));

  ASSERT_OK(db->SetSchemaAttr(schema, "a", values));
  ASSERT_OK_AND_ASSIGN(auto ds_get_a, db->GetSchemaAttr(schema, "a"));
  EXPECT_THAT(ds_get_a, IsEquivalentTo(values));

  EXPECT_THAT(db->DelSchemaAttr(schema, "b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));

  ASSERT_OK(db->DelSchemaAttr(schema, "a"));
  EXPECT_THAT(db->GetSchemaAttr(schema, "a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  for (int i = 0; i < schema.size(); ++i) {
    EXPECT_THAT(db->GetSchemaAttrs(schema[i]), IsOkAndHolds(ElementsAre()));
  }

  // Deleting on an empty slice is a no-op.
  ASSERT_OK(
      db->DelSchemaAttr(DataSliceImpl::CreateEmptyAndUnknownType(2), "a"));
}

TEST(DataBagTest, CreateExplicitSchemaFromFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs{"a", "b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };

  ASSERT_OK_AND_ASSIGN(auto schema,
                       db->CreateExplicitSchemaFromFields(attrs, items));
  // Explicit Schema item is returned.
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsExplicitSchema());

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                            arolla::Text("b"),
                                            arolla::Text("c")));

  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(int_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(float_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_s));

  schema_s = DataItem(42);
  std::vector<std::reference_wrapper<const DataItem>> items_error{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };
  EXPECT_THAT(db->CreateExplicitSchemaFromFields(attrs, items_error),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));
}

TEST(DataBagTest, CreateUuSchemaFromFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs{"a", "b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };

  ASSERT_OK_AND_ASSIGN(auto schema,
                       db->CreateUuSchemaFromFields("", attrs, items));
  // Explicit UuSchema item is returned.
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsExplicitSchema());
  EXPECT_TRUE(schema.value<internal::ObjectId>().IsUuid());

  ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema));
  EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                            arolla::Text("b"),
                                            arolla::Text("c")));

  EXPECT_THAT(db->GetSchemaAttr(schema, "a"), IsOkAndHolds(int_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "b"), IsOkAndHolds(float_s));
  EXPECT_THAT(db->GetSchemaAttr(schema, "c"), IsOkAndHolds(schema_s));

  ASSERT_OK_AND_ASSIGN(auto seeded_schema,
                       db->CreateUuSchemaFromFields("seed", attrs, items));
  EXPECT_THAT(schema, Not(IsEquivalentTo(seeded_schema)));

  ASSERT_OK_AND_ASSIGN(
      auto shuffled_schema,
      db->CreateUuSchemaFromFields(
          "", {"a", "c", "b"},
          {std::cref(int_s), std::cref(schema_s), std::cref(float_s)}));
  EXPECT_THAT(schema, IsEquivalentTo(shuffled_schema));

  schema_s = DataItem(42);
  std::vector<std::reference_wrapper<const DataItem>> items_error{
    std::cref(int_s), std::cref(float_s), std::cref(schema_s)
  };
  EXPECT_THAT(db->CreateUuSchemaFromFields("", attrs, items_error),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));
}

TEST(DataBagTest, SetSchemaFields) {
  auto db = internal::DataBagImpl::CreateEmptyDatabag();

  std::vector<absl::string_view> attrs_1{"a", "b"};
  std::vector<absl::string_view> attrs_2{"b", "c"};
  auto int_s = GetIntSchema();
  auto float_s = GetFloatSchema();
  auto schema_s = DataItem(AllocateExplicitSchema());
  std::vector<std::reference_wrapper<const DataItem>> items_1{
    std::cref(int_s), std::cref(float_s)};
  std::vector<std::reference_wrapper<const DataItem>> items_2{
    std::cref(schema_s), std::cref(float_s)};

  {
    // DataItem.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    ASSERT_OK(db->SetSchemaFields<DataItem>(schema_item, attrs_1, items_1));
    ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_item));
    EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                              arolla::Text("b")));

    ASSERT_OK(db->SetSchemaFields<DataItem>(schema_item, attrs_2, items_2));
    ASSERT_OK_AND_ASSIGN(ds_impl, db->GetSchemaAttrs(schema_item));
    EXPECT_THAT(ds_impl,
                UnorderedElementsAre(arolla::Text("a"), arolla::Text("b"),
                                     arolla::Text("c")));

    EXPECT_THAT(db->GetSchemaAttr(schema_item, "a"), IsOkAndHolds(int_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "b"), IsOkAndHolds(schema_s));
    EXPECT_THAT(db->GetSchemaAttr(schema_item, "c"), IsOkAndHolds(float_s));
  }
  {
    // DataSliceImpl - Schema slice is returned.
    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));

    ASSERT_OK(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_1, items_1));
    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b")));
    }

    ASSERT_OK(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_2, items_2));
    for (int i = 0; i < schema_slice.size(); ++i) {
      ASSERT_OK_AND_ASSIGN(auto ds_impl, db->GetSchemaAttrs(schema_slice[i]));
      EXPECT_THAT(ds_impl, UnorderedElementsAre(arolla::Text("a"),
                                                arolla::Text("b"),
                                                arolla::Text("c")));
    }

    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "a"),
                IsOkAndHolds(ElementsAre(int_s, int_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "b"),
                IsOkAndHolds(ElementsAre(schema_s, schema_s)));
    EXPECT_THAT(db->GetSchemaAttr(schema_slice, "c"),
                IsOkAndHolds(ElementsAre(float_s, float_s)));
  }
  {
    // Empty item / slice.
    ASSERT_OK(db->SetSchemaFields(DataItem(), attrs_1, items_1));
    ASSERT_OK(db->SetSchemaFields(DataSliceImpl::CreateEmptyAndUnknownType(2),
                                  attrs_1, items_1));
  }
  {
    // Error.
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataItem(AllocateSingleObject()), schema::kImplicitSchemaSeed));
    schema_s = DataItem(42);

    EXPECT_THAT(
        db->SetSchemaFields<DataItem>(schema_item, attrs_1,
                                      {std::cref(int_s), std::cref(schema_s)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));

    EXPECT_THAT(
        db->SetSchemaFields<DataSliceImpl>(
            DataSliceImpl::AllocateEmptyObjects(2), attrs_1,
            {std::cref(int_s), std::cref(float_s)}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("cannot set schema attribute of a non-schema slice")));

    ASSERT_OK_AND_ASSIGN(
        auto schema_slice,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            DataSliceImpl::AllocateEmptyObjects(2),
            schema::kImplicitSchemaSeed));

    EXPECT_THAT(
        db->SetSchemaFields<DataSliceImpl>(
            schema_slice, attrs_1, {std::cref(int_s), std::cref(schema_s)}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only schemas can be assigned as attributes of "
                           "schemas, got: 42")));
  }
}

}  // namespace
}  // namespace koladata::internal
