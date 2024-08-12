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
#include "koladata/data_slice.h"

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/repr_token_eq.h"
#include "arolla/util/text.h"

namespace koladata {
namespace {

using ObjectId = ::koladata::internal::ObjectId;

using ::arolla::CreateDenseArray;
using ::arolla::CreateFullDenseArray;
using ::arolla::GetQType;
using ::arolla::TypedValue;
using ::arolla::testing::ReprTokenEq;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::testing::DataItemWith;
using ::koladata::schema::DType;
using ::koladata::testing::IsEquivalentTo;
using ::koladata::testing::IsOk;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::AllOf;
using ::testing::ContainsRegex;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;
using ::testing::Property;

internal::DataItem kAnySchema(schema::kAny);

DataSlice::JaggedShape::Edge CreateEdge(
    std::initializer_list<int64_t> split_points) {
  return *DataSlice::JaggedShape::Edge::FromSplitPoints(
      CreateFullDenseArray(std::vector<int64_t>(split_points)));
}

ObjectId GenerateImplicitSchema() {
  return internal::CreateUuidWithMainObject<
      internal::ObjectId::kUuidImplicitSchemaFlag>(
      internal::AllocateSingleObject(),
      arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish());
}

TEST(DataSliceTest, Create_DataSliceImpl) {
  auto db = DataBag::Empty();
  auto int_schema = DataItem(schema::kInt32);
  auto ds_impl = DataSliceImpl::Create(CreateFullDenseArray<int>({1, 2, 3}));
  {
    // DataSliceImpl -> holds DataSlice.
    auto shape = DataSlice::JaggedShape::FlatFromSize(3);
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::Create(ds_impl, shape, int_schema, db));
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_THAT(ds.slice(), IsEquivalentTo(ds_impl));
  }
  {
    // DataSliceImpl -> holds DataItem.
    auto shape = DataSlice::JaggedShape::Empty();
    auto ds_impl_single_item =
        DataSliceImpl::Create(CreateFullDenseArray<int>({1}));
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::Create(ds_impl_single_item, shape, int_schema, db));
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_THAT(ds.item(), IsEquivalentTo(ds_impl[0]));
  }
  {
    // DataSliceImpl failure.
    auto shape = DataSlice::JaggedShape::FlatFromSize(2);
    EXPECT_THAT(
        DataSlice::Create(ds_impl, shape, int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=2 != items_size=3")));
  }
  {
    // StatusOr<DataSliceImpl> success.
    auto shape = DataSlice::JaggedShape::FlatFromSize(3);
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::Create(ds_impl, shape, int_schema, db));
    ASSERT_OK_AND_ASSIGN(
        auto ds_from_status_or,
        DataSlice::Create(absl::StatusOr<DataSliceImpl>(ds_impl), shape,
                          int_schema, db));
    EXPECT_THAT(ds, IsEquivalentTo(ds_from_status_or));
  }
  {
    // StatusOr<DataSliceImpl> return same status.
    auto shape = DataSlice::JaggedShape::FlatFromSize(3);
    EXPECT_THAT(
        DataSlice::Create(absl::StatusOr<DataSliceImpl>(absl::Status(
                              absl::StatusCode::kInvalidArgument, "foo")),
                          shape, int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument, "foo"));
  }
  {
    // StatusOr<DataSliceImpl> shape incompatibility.
    auto shape = DataSlice::JaggedShape::FlatFromSize(2);
    EXPECT_THAT(
        DataSlice::Create(absl::StatusOr<DataSliceImpl>(ds_impl), shape,
                          int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=2 != items_size=3")));
  }
}

TEST(DataSliceTest, Create_DataItem) {
  auto db = DataBag::Empty();
  auto int_schema = DataItem(schema::kInt32);
  auto ds_impl = DataSliceImpl::Create(CreateFullDenseArray<int>({1}));
  auto data_item = ds_impl[0];
  {
    // DataItem with explicit scalar shape -> holds DataItem.
    auto shape = DataSlice::JaggedShape::Empty();
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::Create(data_item, shape, int_schema, db));
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_THAT(ds.item(), IsEquivalentTo(data_item));
  }
  {
    // DataItem with implicit shape -> holds DataItem.
    ASSERT_OK_AND_ASSIGN(auto ds, DataSlice::Create(data_item, int_schema, db));
    auto shape = DataSlice::JaggedShape::Empty();
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_THAT(ds.item(), IsEquivalentTo(data_item));
  }
  {
    // DataItem with explicit non-scalar shape -> holds DataSliceImpl.
    auto edge = CreateEdge({0, 1});
    ASSERT_OK_AND_ASSIGN(auto shape,
                         DataSlice::JaggedShape::FromEdges({edge, edge, edge}));
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::Create(data_item, shape, int_schema, db));
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_THAT(ds.slice(), IsEquivalentTo(ds_impl));
  }
  {
    // DataItem with incompatible non-scalar shape -> failure.
    auto shape = DataSlice::JaggedShape::FlatFromSize(2);
    EXPECT_THAT(
        DataSlice::Create(data_item, shape, int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=2 != items_size=1")));
  }
  {
    // StatusOr<DataItem> success.
    auto shape = DataSlice::JaggedShape::Empty();
    ASSERT_OK_AND_ASSIGN(auto ds,
                         DataSlice::Create(data_item, shape, int_schema, db));
    ASSERT_OK_AND_ASSIGN(
        auto ds_from_status_or,
        DataSlice::Create(absl::StatusOr<internal::DataItem>(data_item), shape,
                          int_schema, db));
    EXPECT_THAT(ds, IsEquivalentTo(ds_from_status_or));
  }
  {
    // StatusOr<DataItem> return same status.
    auto shape = DataSlice::JaggedShape::Empty();
    EXPECT_THAT(
        DataSlice::Create(absl::StatusOr<internal::DataItem>(absl::Status(
                              absl::StatusCode::kInvalidArgument, "foo")),
                          shape, int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument, "foo"));
  }
  {
    // StatusOr<DataItem> shape incompatibility.
    auto shape = DataSlice::JaggedShape::FlatFromSize(2);
    EXPECT_THAT(
        DataSlice::Create(absl::StatusOr<internal::DataItem>(data_item), shape,
                          int_schema, db),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=2 != items_size=1")));
  }
}

TEST(DataSliceTest, CreateWithSchemaFromData) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  {
    auto ds_impl = DataSliceImpl::Create(CreateFullDenseArray<int>({1, 2, 3}));
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::CreateWithSchemaFromData(ds_impl, shape, db));
    EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds.GetDb(), db);
    EXPECT_EQ(ds.GetSchemaImpl(), internal::DataItem(schema::kInt32));
  }
  {
    auto ds_impl = DataSliceImpl::Create(
        CreateDenseArray<DType>({schema::kInt32, std::nullopt, schema::kAny}));
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::CreateWithSchemaFromData(ds_impl, shape, db));

    EXPECT_EQ(ds.GetSchemaImpl(), schema::kSchema);
  }
  {
    auto ds_impl = DataSliceImpl::Create(
        CreateDenseArray<int>({1, std::nullopt, std::nullopt}),
        CreateDenseArray<float>({std::nullopt, std::nullopt, 3.14}));
    EXPECT_THAT(DataSlice::CreateWithSchemaFromData(ds_impl, shape, db),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("creating a DataSlice without passing schema"
                                   " is supported only for primitive types "
                                   "where all items are the same")));
  }
  {
    auto ds_impl = DataSliceImpl::AllocateEmptyObjects(3);
    EXPECT_THAT(DataSlice::CreateWithSchemaFromData(ds_impl, shape, db),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("creating a DataSlice without passing schema"
                                   " is supported only for primitive types "
                                   "where all items are the same")));
  }
}

TEST(DataSliceTest, ForkDb) {
  auto db = DataBag::Empty();
  auto ds_a = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::FromAttrs(db, {"a"}, {ds_a}));
  auto immutable_db = *db->Fork(/*immutable=*/true);
  auto immutable_ds = ds.WithDb(immutable_db);

  EXPECT_THAT(immutable_ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));

  EXPECT_THAT(immutable_ds.SetAttr("a", ds_a),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DataBag is immutable")));

  ASSERT_OK_AND_ASSIGN(auto forked_ds, immutable_ds.ForkDb());
  ASSERT_OK(forked_ds.SetAttr("a", test::DataSlice<int>({42, 37})));
  EXPECT_THAT(forked_ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(42, 37))));
  // The update is not reflected in the old DataBag.
  EXPECT_THAT(immutable_ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));
  EXPECT_THAT(ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));
}

TEST(DataSliceTest, Freeze) {
  auto db = DataBag::Empty();
  auto ds_a = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::FromAttrs(db, {"a"}, {ds_a}));
  ASSERT_TRUE(ds.GetDb()->IsMutable());

  ASSERT_OK_AND_ASSIGN(auto frozen_ds, ds.Freeze());
  EXPECT_THAT(frozen_ds.SetAttr("a", ds_a),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DataBag is immutable")));
}

TEST(DataSliceTest, ForkErrors) {
  auto db = DataBag::Empty();
  auto ds_a = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::FromAttrs(db, {"a"}, {ds_a}));

  ds = ds.WithDb(DataBag::ImmutableEmptyWithFallbacks({db}));
  EXPECT_THAT(ds.ForkDb(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("forking with fallbacks is not supported")));
  EXPECT_THAT(ds.Freeze(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("forking with fallbacks is not supported")));
}

TEST(DataSliceTest, IsEquivalentTo) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto objects = DataSliceImpl::AllocateEmptyObjects(shape.size());
  ASSERT_OK_AND_ASSIGN(auto ds_1,
                       DataSlice::Create(objects, shape, kAnySchema));
  // Same DataSlice instance.
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_1));

  ASSERT_OK_AND_ASSIGN(auto ds_2,
                       DataSlice::Create(objects, shape, kAnySchema));
  // Different, but equal instances.
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_2));
  // Different and same DataBags.
  auto db = DataBag::Empty();
  EXPECT_FALSE(ds_1.IsEquivalentTo(ds_2.WithDb(db)));
  EXPECT_TRUE(ds_1.WithDb(db).IsEquivalentTo(ds_2.WithDb(db)));

  // Different schema.
  auto schema_schema = test::Schema(schema::kSchema);
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_2));
  ASSERT_OK_AND_ASSIGN(ds_2, ds_2.WithSchema(schema_schema));
  EXPECT_FALSE(ds_1.IsEquivalentTo(ds_2));

  // Different items.
  ds_2 = test::AllocateDataSlice(shape, schema::kAny);
  EXPECT_FALSE(ds_1.IsEquivalentTo(ds_2));

  // Same items in different order - slices are equivalent.
  auto mix1_int_float =
      DataSliceImpl::Create(CreateDenseArray<int>({0, std::nullopt}),
                            CreateDenseArray<float>({std::nullopt, 0}));
  auto mix2_int_float =
      DataSliceImpl::Create(CreateDenseArray<float>({std::nullopt, 0}),
                            CreateDenseArray<int>({0, std::nullopt}));
  ASSERT_OK_AND_ASSIGN(ds_1,
                       DataSlice::Create(mix1_int_float, shape, kAnySchema));
  ASSERT_OK_AND_ASSIGN(ds_2,
                       DataSlice::Create(mix2_int_float, shape, kAnySchema));
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_2));

  // Same shape, but not same shape object.
  auto same_shape_different_ptr = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(
      ds_2,
      DataSlice::Create(mix2_int_float, same_shape_different_ptr, kAnySchema));
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_2));

  // Broadcasted shapes.
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 2});
  ASSERT_OK_AND_ASSIGN(auto shape_non_flat,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  ASSERT_OK_AND_ASSIGN(ds_2, BroadcastToShape(ds_2, shape_non_flat));
  EXPECT_FALSE(ds_1.IsEquivalentTo(ds_2));
  ASSERT_OK_AND_ASSIGN(ds_1, BroadcastToShape(ds_1, shape_non_flat));
  EXPECT_TRUE(ds_1.IsEquivalentTo(ds_2));
}

TEST(DataSliceTest, ImplOwnsValue) {
  auto ds_impl = DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, DataSlice::Create(ds_impl, shape, kAnySchema));
  EXPECT_TRUE(ds.impl_owns_value());

  ds_impl = DataSliceImpl::Create(
      CreateDenseArray<int>({1, std::nullopt, std::nullopt}),
      CreateDenseArray<float>({std::nullopt, std::nullopt, 2.71}));
  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(ds_impl, shape, kAnySchema));
  EXPECT_TRUE(ds.impl_owns_value());

  DataSliceImpl::Builder bldr(3);
  ASSERT_OK_AND_ASSIGN(
      ds, DataSlice::Create(std::move(bldr).Build(), shape, kAnySchema));
  EXPECT_FALSE(ds.impl_owns_value());

  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(DataItem(42), kAnySchema));
  EXPECT_TRUE(ds.impl_owns_value());

  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(DataItem(), kAnySchema));
  EXPECT_FALSE(ds.impl_owns_value());
}

TEST(DataSliceTest, IsEntitySchema) {
  auto db = DataBag::Empty();
  auto int_s = test::Schema(schema::kInt32);
  auto entity_schema = *CreateEntitySchema(db, {"a"}, {int_s});
  EXPECT_TRUE(entity_schema.IsEntitySchema());
  EXPECT_FALSE(test::DataSlice<schema::DType>({schema::kAny, schema::kInt32})
                   .IsEntitySchema());
  EXPECT_FALSE(test::Schema(schema::kAny).IsEntitySchema());
  EXPECT_FALSE(test::DataItem(42).IsEntitySchema());
}

TEST(DataSliceTest, IsListSchema) {
  auto db = DataBag::Empty();
  auto int_s = test::Schema(schema::kInt32);
  auto list_schema = test::Schema(*CreateListSchema(db, int_s), db);
  EXPECT_TRUE(list_schema.IsListSchema());
  ASSERT_OK(list_schema.SetAttr("some_attr", test::Schema(schema::kText)));
  EXPECT_TRUE(list_schema.IsListSchema());
  EXPECT_FALSE(list_schema.WithDb(nullptr).IsListSchema());
  auto entity_schema = *CreateEntitySchema(db, {"a"}, {int_s});
  EXPECT_FALSE(entity_schema.IsListSchema());
  EXPECT_FALSE(test::DataSlice<schema::DType>({schema::kAny, schema::kInt32})
                   .IsListSchema());
  EXPECT_FALSE(test::Schema(schema::kAny).IsListSchema());
  EXPECT_FALSE(test::DataItem(42).IsListSchema());
}

TEST(DataSliceTest, IsDictSchema) {
  auto db = DataBag::Empty();
  auto int_s = test::Schema(schema::kInt32);
  auto dict_schema = test::Schema(*CreateDictSchema(db, int_s, int_s), db);
  EXPECT_TRUE(dict_schema.IsDictSchema());
  ASSERT_OK(dict_schema.SetAttr("some_attr", test::Schema(schema::kText)));
  EXPECT_TRUE(dict_schema.IsDictSchema());
  EXPECT_FALSE(dict_schema.WithDb(nullptr).IsDictSchema());
  auto entity_schema = *CreateEntitySchema(db, {"a"}, {int_s});
  EXPECT_FALSE(entity_schema.IsDictSchema());
  entity_schema = *CreateEntitySchema(db, {"__keys__"}, {int_s});
  EXPECT_FALSE(entity_schema.IsDictSchema());
  EXPECT_FALSE(test::DataSlice<schema::DType>({schema::kAny, schema::kInt32})
                   .IsDictSchema());
  EXPECT_FALSE(test::Schema(schema::kAny).IsDictSchema());
  EXPECT_FALSE(test::DataItem(42).IsDictSchema());
}

TEST(DataSliceTest, VerifyIsSchema) {
  EXPECT_THAT(test::Schema(schema::kAny).VerifyIsSchema(), IsOk());

  EXPECT_THAT(
      test::DataItem(1).VerifyIsSchema(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("schema's schema must be SCHEMA, got: INT32")));
  EXPECT_THAT(test::DataSlice<DType>({schema::kInt32, schema::kFloat32})
                  .VerifyIsSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("schema can only be 0-rank schema slice, "
                                 "got: rank(1)")));
  {
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::Create(internal::DataItem(),
                                   internal::DataItem(schema::kSchema)));
    EXPECT_THAT(ds.VerifyIsSchema(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("schema must contain either a DType or "
                                   "valid schema ItemId, got None")));
  }
}

TEST(DataSliceTest, VerifyIsPrimitiveSchema) {
  EXPECT_THAT(test::Schema(schema::kInt32).VerifyIsPrimitiveSchema(), IsOk());

  EXPECT_THAT(
      test::DataItem(1).VerifyIsPrimitiveSchema(),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("primitive schema's schema must be SCHEMA, got: INT32")));
  EXPECT_THAT(
      test::DataSlice<DType>({schema::kInt32, schema::kFloat32})
          .VerifyIsPrimitiveSchema(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("primitive schema can only be 0-rank schema slice, "
                         "got: rank(1)")));
  {
    ASSERT_OK_AND_ASSIGN(
        auto ds, DataSlice::Create(internal::DataItem(),
                                   internal::DataItem(schema::kSchema)));
    EXPECT_THAT(ds.VerifyIsPrimitiveSchema(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("primitive schema must contain a primitive "
                                   "DType, got None")));
  }
}

TEST(DataSliceTest, VerifyIsListSchema) {
  auto db = DataBag::Empty();
  EXPECT_THAT(
      test::Schema(*CreateListSchema(db, test::Schema(schema::kInt32)), db)
          .VerifyIsListSchema(),
      IsOk());

  EXPECT_THAT(test::Schema(schema::kAny).VerifyIsListSchema(), IsOk());

  EXPECT_THAT(test::Schema(schema::kInt32).VerifyIsListSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected List schema, got INT32")));

  EXPECT_THAT(test::DataItem(42).VerifyIsListSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("must be SCHEMA, got: INT32")));
}

TEST(DataSliceTest, VerifyIsDictSchema) {
  auto db = DataBag::Empty();
  EXPECT_THAT(test::Schema(*CreateDictSchema(db, test::Schema(schema::kInt32),
                                             test::Schema(schema::kFloat32)),
                           db)
                  .VerifyIsDictSchema(),
              IsOk());

  EXPECT_THAT(test::Schema(schema::kAny).VerifyIsDictSchema(), IsOk());

  EXPECT_THAT(test::Schema(schema::kObject).VerifyIsDictSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected Dict schema, got OBJECT")));

  EXPECT_THAT(test::DataItem(42).VerifyIsDictSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("must be SCHEMA, got: INT32")));
}

// NOTE: This is also a test for all DataSlice::VerifySchemaConsistency
// functionality that is used in other places as well, but as an invariant (i.e.
// DCHECK).
TEST(DataSliceTest, VerifySchemaConsistency_WithGetSchema) {
  auto any_schema = test::Schema(schema::kAny);
  auto object_schema = test::Schema(schema::kObject);
  auto type_schema = test::Schema(schema::kSchema);
  auto none_schema = test::Schema(schema::kNone);
  EXPECT_THAT(any_schema.GetSchema(), IsEquivalentTo(type_schema));
  EXPECT_THAT(type_schema.GetSchema(), IsEquivalentTo(type_schema));
  EXPECT_THAT(type_schema.GetSchema().GetSchema(), IsEquivalentTo(type_schema));
  {
    // Schema slice consistency - multidim schema.
    auto ds = test::DataSlice<DType>(
        {schema::kInt32, std::nullopt, schema::kFloat32});

    // Any schema can be assigned to a schema slice.
    ASSERT_OK_AND_ASSIGN(auto ds_any_schema, ds.WithSchema(any_schema));
    EXPECT_THAT(ds_any_schema.GetSchema(), IsEquivalentTo(any_schema));
    EXPECT_THAT(ds_any_schema.slice(), IsEquivalentTo(ds.slice()));

    // Schema schema can be assigned to a schema slice.
    ASSERT_OK_AND_ASSIGN(auto ds_schema, ds.WithSchema(type_schema));
    EXPECT_THAT(ds_schema.GetSchema(), IsEquivalentTo(type_schema));
    EXPECT_THAT(ds_schema.slice(), IsEquivalentTo(ds.slice()));

    // ds_1 is a schema, but multidim schema.
    EXPECT_THAT(ds.WithSchema(ds_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("schema can only be 0-rank schema slice, "
                                   "got: rank(1)")));

    ASSERT_OK_AND_ASSIGN(auto any_any, any_schema.WithSchema(any_schema));
    EXPECT_THAT(
        ds.WithSchema(any_any),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("schema's schema must be SCHEMA, got: ANY")));
  }
  {
    // Entity slice consistency.
    auto entity_schema = test::Schema(internal::AllocateExplicitSchema());
    auto ds = test::AllocateDataSlice(3, schema::kAny);

    ASSERT_OK_AND_ASSIGN(auto res, ds.WithSchema(entity_schema));
    EXPECT_THAT(res.GetSchema(), IsEquivalentTo(entity_schema));

    auto db1 = DataBag::Empty();
    auto db2 = DataBag::Empty();
    auto schema_with_db = entity_schema.WithDb(db1);
    EXPECT_THAT(ds.WithSchema(schema_with_db),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("with_schema does not accept schemas with "
                                   "different DataBag attached")));
    EXPECT_THAT(ds.WithDb(db2).WithSchema(schema_with_db),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("with_schema does not accept schemas with "
                                   "different DataBag attached")));

    ASSERT_OK_AND_ASSIGN(res, ds.WithDb(db1).WithSchema(schema_with_db));
    EXPECT_THAT(res.GetSchema(), IsEquivalentTo(schema_with_db));

    auto int_schema = test::Schema(schema::kInt32);
    EXPECT_THAT(ds.WithSchema(int_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("INT32 schema can only be assigned to a "
                                   "DataSlice that contains only primitives of "
                                   "INT32")));

    ds = test::DataSlice<int>({1, 2, 3});
    EXPECT_THAT(ds.WithSchema(entity_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("DataSlice with an Entity schema must hold "
                                   "Entities or Objects")));

    auto itemid_schema = test::Schema(schema::kItemId);
    EXPECT_THAT(ds.WithSchema(itemid_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("ITEMID schema requires DataSlice to hold "
                                   "object ids.")));
  }
  {
    // Primitive slice consistency.
    auto int_schema = test::Schema(schema::kInt32);
    auto values = CreateDenseArray<int>({1, std::nullopt, 5});
    auto ds = test::DataSlice<int>({1, std::nullopt, 5});
    ASSERT_OK_AND_ASSIGN(auto int_ds, ds.WithSchema(int_schema));
    EXPECT_THAT(int_ds.GetSchema(), IsEquivalentTo(int_schema));

    // INT32 schema with a db.
    ASSERT_OK_AND_ASSIGN(int_ds,
                         ds.WithSchema(int_schema.WithDb(DataBag::Empty())));
    EXPECT_THAT(int_ds.GetSchema(), IsEquivalentTo(int_schema));

    auto float_schema = test::Schema(schema::kFloat32);
    EXPECT_THAT(ds.WithSchema(float_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("FLOAT32 schema can only be assigned to a "
                                   "DataSlice that contains only primitives of "
                                   "FLOAT32")));

    auto mixed_ds = test::MixedDataSlice<float, int>(
        {std::nullopt, 3.14, std::nullopt}, {1, std::nullopt, std::nullopt});
    EXPECT_THAT(mixed_ds.WithSchema(int_schema),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("INT32 schema can only be assigned to a "
                                   "DataSlice that contains only primitives of "
                                   "INT32")));

    // Mixed with `kd.ANY`.
    ASSERT_OK_AND_ASSIGN(mixed_ds, mixed_ds.WithSchema(any_schema));
    EXPECT_THAT(mixed_ds.GetSchema(), IsEquivalentTo(any_schema));

    // Mixed with `kd.OBJECT`.
    ASSERT_OK_AND_ASSIGN(mixed_ds, mixed_ds.WithSchema(object_schema));
    EXPECT_THAT(mixed_ds.GetSchema(), IsEquivalentTo(object_schema));

    // NONE cannot be used for non-empty slices.
    EXPECT_THAT(
        mixed_ds.WithSchema(none_schema),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "NONE schema requires DataSlice to be empty and unknown"));

    // Schema slice can have only a SCHEMA schema.
    EXPECT_THAT(
        int_ds.WithSchema(type_schema),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("a non-schema item cannot be present in a schema "
                           "DataSlice")));
  }
  {
    // All-empty slice.
    auto ds = test::EmptyDataSlice(3, schema::kAny);
    ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(type_schema));
    EXPECT_THAT(ds.GetSchema(), IsEquivalentTo(type_schema));
    ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(object_schema));
    EXPECT_THAT(ds.GetSchema(), IsEquivalentTo(object_schema));
    auto int_schema = test::Schema(schema::kInt32);
    ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(int_schema));
    EXPECT_THAT(ds.GetSchema(), IsEquivalentTo(int_schema));
    ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(none_schema));
    EXPECT_THAT(ds.GetSchema(), IsEquivalentTo(none_schema));
  }
  {
    // Allows dtype == NOTHING in VerifySchemaConsistency when schema slice
    // contains both DTypes and SchemaIds.
    auto schema_schema = test::Schema(schema::kSchema);
    auto ds = test::MixedDataSlice<DType, ObjectId>(
        {schema::kInt32, std::nullopt},
        {std::nullopt, internal::AllocateExplicitSchema()}, schema::kAny);
    EXPECT_OK(ds.WithSchema(schema_schema));
  }
}

// Only test differences between SetSchema and WithSchema. More extensive tests
// are in VerifySchemaConsistency_WithGetSchema.
TEST(DataSliceTest, SetSchema) {
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  auto entity_schema1 =
      test::Schema(internal::AllocateExplicitSchema()).WithDb(db1);
  auto entity_schema2 =
      test::Schema(internal::AllocateExplicitSchema()).WithDb(db2);
  auto schema_without_db = test::Schema(internal::AllocateExplicitSchema());
  auto entity_ds = test::AllocateDataSlice(3, schema::kAny);
  auto int_schema = test::Schema(schema::kInt32);
  auto primitive_ds = test::DataSlice<int>({1, std::nullopt, 5});

  ASSERT_OK_AND_ASSIGN(auto res, primitive_ds.SetSchema(int_schema));
  EXPECT_THAT(res.GetSchema(), IsEquivalentTo(int_schema));

  ASSERT_OK_AND_ASSIGN(res, entity_ds.SetSchema(schema_without_db));
  EXPECT_THAT(res.GetSchema(), IsEquivalentTo(schema_without_db));

  EXPECT_THAT(entity_ds.SetSchema(entity_schema1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot set an Entity schema on a DataSlice "
                                 "without a DataBag.")));

  entity_ds = entity_ds.WithDb(db1);
  ASSERT_OK_AND_ASSIGN(res, entity_ds.SetSchema(entity_schema1));
  EXPECT_THAT(res.GetSchema(), IsEquivalentTo(entity_schema1));

  ASSERT_OK_AND_ASSIGN(res, entity_ds.SetSchema(entity_schema2));
  EXPECT_THAT(res.GetSchema().WithDb(nullptr),
              IsEquivalentTo(entity_schema2.WithDb(nullptr)));

  ASSERT_OK_AND_ASSIGN(res, entity_ds.SetSchema(schema_without_db));
  EXPECT_THAT(res.GetSchema().WithDb(nullptr),
              IsEquivalentTo(schema_without_db));
}

TEST(DataSliceTest, GetAttrNames_Entity) {
  auto db = DataBag::Empty();
  auto a = test::DataSlice<int>({1});
  auto b = test::DataSlice<arolla::Text>({"a"});
  auto c = test::DataSlice<float>({3.14});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      EntityCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b", "c")));
  // Test the DataItem codepath.
  ASSERT_OK_AND_ASSIGN(ds, ds.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_EQ(ds.GetShape().rank(), 0);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b", "c")));
}

TEST(DataSliceTest, GetAttrNames_Object) {
  auto db = DataBag::Empty();
  auto a = test::DataSlice<int>({1});
  auto b = test::DataSlice<arolla::Text>({"a"});
  auto c = test::DataSlice<float>({3.14});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      ObjectCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b", "c")));
  // Test the DataItem codepath.
  ASSERT_OK_AND_ASSIGN(ds, ds.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_EQ(ds.GetShape().rank(), 0);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b", "c")));
}

TEST(DataSliceTest, GetAttrNames_Object_AttrsAtIntersection) {
  auto db = DataBag::Empty();
  auto a = test::DataItem(1);
  auto b = test::DataItem("a");
  auto c = test::DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(
      auto object_1,
      ObjectCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_2,
      ObjectCreator::FromAttrs(db, {"a", "b", "d"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_3,
      ObjectCreator::FromAttrs(db, {"c", "b", "a"}, {a, b, c}));
  auto ds = test::DataSlice<ObjectId>({object_1.item().value<ObjectId>(),
                                       object_2.item().value<ObjectId>(),
                                       object_3.item().value<ObjectId>()})
                .WithDb(db);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b")));
}

TEST(DataSliceTest, GetAttrNames_Object_EmptyIntersection) {
  auto db = DataBag::Empty();
  auto a = test::DataItem(1);
  auto b = test::DataItem("a");
  auto c = test::DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(
      auto object_1,
      ObjectCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_2,
      ObjectCreator::FromAttrs(db, {"x", "y", "d"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_3,
      ObjectCreator::FromAttrs(db, {"c", "b", "a"}, {a, b, c}));
  auto ds = test::DataSlice<ObjectId>({object_1.item().value<ObjectId>(),
                                       object_2.item().value<ObjectId>(),
                                       object_3.item().value<ObjectId>()})
                .WithDb(db);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre()));
}

TEST(DataSliceTest, GetAttrNames_NoFollow) {
  auto db = DataBag::Empty();
  auto a = test::DataSlice<int>({1});
  auto b = test::DataSlice<arolla::Text>({"a"});
  auto c = test::DataSlice<float>({3.14});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      EntityCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(ds, NoFollow(ds));
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre()));
  // Test the DataItem codepath.
  ASSERT_OK_AND_ASSIGN(ds, ds.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_EQ(ds.GetShape().rank(), 0);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre()));
}

TEST(DataSliceTest, GetAttrNames_Primitives) {
  auto ds = test::DataSlice<int>({1});
  EXPECT_THAT(ds.GetAttrNames(), StatusIs(absl::StatusCode::kInvalidArgument,
                                          HasSubstr("without a DataBag")));
  EXPECT_THAT(ds.WithDb(DataBag::Empty()).GetAttrNames(),
              IsOkAndHolds(ElementsAre()));
}

TEST(DataSliceTest, GetAttrNames_MixedObjectAndPrimitive) {
  auto db = DataBag::Empty();
  auto a = test::DataItem(1);
  auto b = test::DataItem("a");
  auto c = test::DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(
      auto object,
      ObjectCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  auto ds =
      test::MixedDataSlice<int, ObjectId>(
          {42, std::nullopt}, {std::nullopt, object.item().value<ObjectId>()})
          .WithDb(db);
  EXPECT_THAT(ds.GetAttrNames(), IsOkAndHolds(ElementsAre("a", "b", "c")));
}

TEST(DataSliceTest, GetAttrNames_SchemaItem) {
  auto db = DataBag::Empty();
  auto a = test::DataSlice<int>({1});
  auto b = test::DataSlice<arolla::Text>({"a"});
  auto c = test::DataSlice<float>({3.14});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      EntityCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  auto schema_ds = ds.GetSchema();
  EXPECT_THAT(schema_ds.GetAttrNames(),
              IsOkAndHolds(ElementsAre("a", "b", "c")));
  EXPECT_THAT(test::Schema(schema::kInt32, db).GetAttrNames(),
              IsOkAndHolds(ElementsAre()));
}

TEST(DataSliceTest, GetAttrNames_SchemaSlice) {
  auto db = DataBag::Empty();
  auto a = test::DataItem(1);
  auto b = test::DataItem("a");
  auto c = test::DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(
      auto object_1,
      ObjectCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_2,
      ObjectCreator::FromAttrs(db, {"b", "y", "d"}, {a, b, c}));
  ASSERT_OK_AND_ASSIGN(
      auto object_3,
      ObjectCreator::FromAttrs(db, {"c", "b", "a"}, {a, b, c}));
  auto ds = test::DataSlice<ObjectId>({object_1.item().value<ObjectId>(),
                                       object_2.item().value<ObjectId>(),
                                       object_3.item().value<ObjectId>()})
                .WithDb(db);
  ASSERT_OK_AND_ASSIGN(auto schema_ds, ds.GetAttr(schema::kSchemaAttr));
  ASSERT_EQ(schema_ds.GetSchemaImpl(), schema::kSchema);
  EXPECT_THAT(schema_ds.GetAttrNames(), IsOkAndHolds(ElementsAre("b")));
  EXPECT_THAT(
      test::DataSlice<schema::DType>({schema::kInt32, schema::kText}, db)
      .GetAttrNames(),
      IsOkAndHolds(ElementsAre()));
}

TEST(DataSliceTest, GetAttrNames_SchemaSliceMixed) {
  auto db = DataBag::Empty();
  auto a = test::DataItem(1);
  auto b = test::DataItem("a");
  auto c = test::DataItem(3.14);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      EntityCreator::FromAttrs(db, {"a", "b", "c"}, {a, b, c}));
  auto schema_ds = test::MixedDataSlice<schema::DType, ObjectId>(
                       {schema::kInt32, std::nullopt},
                       {std::nullopt, ds.GetSchemaImpl().value<ObjectId>()})
                       .WithDb(db);
  ASSERT_OK_AND_ASSIGN(schema_ds,
                       schema_ds.WithSchema(test::Schema(schema::kSchema)));
  EXPECT_THAT(schema_ds.GetAttrNames(),
              IsOkAndHolds(ElementsAre("a", "b", "c")));
}

TEST(DataSliceTest, GetNoFollowedSchema) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::FromAttrs(db, {}, {}));
  EXPECT_THAT(ds.GetSchema().GetNoFollowedSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("a nofollow schema is required")));
  ASSERT_OK_AND_ASSIGN(auto nofollow, NoFollow(ds));
  ASSERT_TRUE(nofollow.GetSchemaImpl().holds_value<internal::ObjectId>());
  EXPECT_TRUE(nofollow.GetSchemaImpl().value<internal::ObjectId>()
              .IsNoFollowSchema());
  EXPECT_THAT(nofollow.GetNoFollowedSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("must be SCHEMA")));
  EXPECT_THAT(nofollow.GetSchema().GetNoFollowedSchema(),
              IsOkAndHolds(Property(&DataSlice::item, schema::kObject)));
}

TEST(DataSliceTest, DbRef) {
  auto ds_obj = test::AllocateDataSlice(3, schema::kAny);
  EXPECT_EQ(ds_obj.GetDb(), nullptr);

  auto db = DataBag::Empty();
  ds_obj = ds_obj.WithDb(db);
  EXPECT_EQ(ds_obj.GetDb(), db);
}

TEST(DataSliceTest, DataSliceQType) {
  auto ds_obj = test::AllocateDataSlice(3, schema::kAny);
  EXPECT_EQ(TypedValue::FromValue(ds_obj).GetType(), GetQType<DataSlice>());
}

TEST(DataSliceTest, PyQValueSpecializationKey) {
  auto ds_obj = test::AllocateDataSlice(3, schema::kAny);
  EXPECT_EQ(ds_obj.py_qvalue_specialization_key(),
            kDataSliceQValueSpecializationKey);

  auto shape = DataSlice::JaggedShape::Empty();
  ds_obj = test::AllocateDataSlice(shape, schema::kAny);
  EXPECT_EQ(ds_obj.py_qvalue_specialization_key(),
            kDataItemQValueSpecializationKey);

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(ds_obj, CreateEmptyList(db, /*schema=*/std::nullopt,
                                               test::Schema(schema::kAny)));
  EXPECT_EQ(ds_obj.py_qvalue_specialization_key(),
            kListItemQValueSpecializationKey);

  ASSERT_OK_AND_ASSIGN(
      ds_obj, CreateDictShaped(
                  db, DataSlice::JaggedShape::Empty(), /*keys=*/std::nullopt,
                  /*values=*/std::nullopt, /*schema=*/std::nullopt,
                  /*key_schema=*/std::nullopt, /*value_schema=*/std::nullopt));
  EXPECT_EQ(ds_obj.py_qvalue_specialization_key(),
            kDictItemQValueSpecializationKey);
}

TEST(DataSliceTest, Fingerprint) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_1_1 = CreateEdge({0, 2, 3});
  auto edge_1_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape_1,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_1_1}));
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_1_2}));
  auto ds_impl = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSlice::Create(ds_impl, shape_1, kAnySchema));
  ASSERT_OK_AND_ASSIGN(auto ds_eq,
                       DataSlice::Create(ds_impl, shape_1, kAnySchema));

  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(ds).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_eq).Finish());

  // Different DataSliceImpl.
  auto ds_impl_diff = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK_AND_ASSIGN(auto ds_diff_impl,
                       DataSlice::Create(ds_impl_diff, shape_1, kAnySchema));
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_diff_impl).Finish());

  // Different shape.
  ASSERT_OK_AND_ASSIGN(auto ds_diff_shape,
                       DataSlice::Create(ds_impl, shape_2, kAnySchema));
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_diff_shape).Finish());

  // Different schema.
  ASSERT_OK_AND_ASSIGN(auto schema_schema,
                       DataSlice::Create(internal::DataItem(schema::kSchema),
                                         internal::DataItem(schema::kSchema)));
  ASSERT_OK_AND_ASSIGN(auto ds_diff_schema, ds.WithSchema(schema_schema));
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_diff_schema).Finish());

  // Different DataBag.
  auto db = DataBag::Empty();
  auto ds_diff_db = ds.WithDb(db);
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(ds).Finish(),
            arolla::FingerprintHasher("salt").Combine(ds_diff_db).Finish());
}

TEST(DataSliceTest, FromToArray) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto ds = test::DataSlice<int>({1, 2, 3});
  EXPECT_EQ(ds.size(), 3);
  EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds.dtype(), GetQType<int>());
  EXPECT_EQ(ds.slice().allocation_ids().size(), 0);
  EXPECT_THAT(ds.slice(), ElementsAre(1, 2, 3));

  auto ds_obj = test::AllocateDataSlice(3, schema::kAny);
  EXPECT_EQ(ds_obj.dtype(), GetQType<ObjectId>());
  EXPECT_THAT(ds_obj.GetShape(), IsEquivalentTo(shape));
}

TEST(DataSliceTest, BroadcastToShape) {
  {
    // Same shape.
    auto shape = DataSlice::JaggedShape::FlatFromSize(3);
    auto ds = test::DataSlice<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(auto res_ds, BroadcastToShape(ds, shape));
    EXPECT_THAT(res_ds.GetShape(), IsEquivalentTo(shape));
  }
  {
    // Incompatible shape.
    auto shape = DataSlice::JaggedShape::FlatFromSize(4);
    auto ds = test::DataSlice<int>({1, 2, 3});
    EXPECT_THAT(
        BroadcastToShape(ds, shape),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr(absl::StrFormat(
                     "DataSlice with shape=%s cannot be expanded to shape=%s",
                     arolla::Repr(ds.GetShape()), arolla::Repr(shape)))));
  }
  {
    // Actual expansion. More extensive tests are in:
    // //koladata/internal/op_utils/expand_test.cc
    auto edge_1 = CreateEdge({0, 3});
    auto edge_2 = CreateEdge({0, 2, 4, 6});
    ASSERT_OK_AND_ASSIGN(auto shape,
                         DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
    auto ds_1 = test::DataSlice<int>({1, 2, 3});

    ASSERT_OK_AND_ASSIGN(auto res_ds, BroadcastToShape(ds_1, shape));
    EXPECT_THAT(res_ds.GetShape(), IsEquivalentTo(shape));
    EXPECT_THAT(res_ds.slice(), ElementsAre(1, 1, 2, 2, 3, 3));
  }
}

TEST(DataSliceTest, EmbedSchema_Primitive) {
  // Item
  auto value_item = test::DataItem(42);
  ASSERT_OK_AND_ASSIGN(auto embedded_item, value_item.EmbedSchema());
  EXPECT_EQ(embedded_item.GetSchemaImpl(), schema::kObject);
  EXPECT_EQ(embedded_item.item(), 42);

  // Slice
  auto value_slice = test::DataSlice<int>({1, 2});
  ASSERT_OK_AND_ASSIGN(auto embedded_slice, value_slice.EmbedSchema());
  EXPECT_EQ(embedded_slice.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(embedded_slice.slice(), ElementsAre(1, 2));
}

TEST(DataSliceTest, EmbedSchema_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto implicit_schema = GenerateImplicitSchema();

  // Item
  auto value_item =
      test::DataItem(internal::AllocateSingleObject(), explicit_schema, db);
  ASSERT_OK_AND_ASSIGN(auto embedded_item, value_item.EmbedSchema());
  EXPECT_EQ(embedded_item.GetSchemaImpl(), schema::kObject);
  EXPECT_EQ(embedded_item.item(), value_item.item());
  ASSERT_OK_AND_ASSIGN(auto schema_attr,
                       embedded_item.GetAttr(schema::kSchemaAttr));
  EXPECT_EQ(schema_attr.item(), explicit_schema);

  // Slice
  auto value_slice = test::AllocateDataSlice(2, implicit_schema, db);
  ASSERT_OK_AND_ASSIGN(auto embedded_slice, value_slice.EmbedSchema());
  EXPECT_EQ(embedded_slice.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(embedded_slice.slice(), IsEquivalentTo(value_slice.slice()));
  ASSERT_OK_AND_ASSIGN(schema_attr,
                       embedded_slice.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(schema_attr.slice(),
              ElementsAre(implicit_schema, implicit_schema));
}

TEST(DataSliceTest, EmbedSchema_ObjectNoDataBag) {
  auto value_item = test::DataItem(internal::AllocateSingleObject(),
                                   internal::AllocateExplicitSchema());
  EXPECT_THAT(value_item.EmbedSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot embed object schema without a mutable DataBag"));
}

TEST(DataSliceTest, EmbedSchema_MixedNotAllowed) {
  auto values = test::MixedDataSlice<int, ObjectId>(
      {1, std::nullopt}, {std::nullopt, internal::AllocateSingleObject()},
      schema::kAny);
  EXPECT_THAT(values.EmbedSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "schema embedding is only supported for primitive and "
                       "entity schemas, got ANY"));
}

TEST(DataSliceTest, EmbedSchema_Object_Errors) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto db_impl, db->GetMutableImpl());
  auto explicit_schema_1 = internal::AllocateExplicitSchema();
  auto explicit_schema_2 = internal::AllocateExplicitSchema();

  // Item
  auto item = DataItem(internal::AllocateSingleObject());
  ASSERT_OK(db_impl.get().SetAttr(item, schema::kSchemaAttr,
                                  DataItem(explicit_schema_1)));
  auto value_item =
      test::DataItem(internal::AllocateSingleObject(), explicit_schema_1, db);
  // Check that it is okay to embed the same schema.
  ASSERT_OK(value_item.EmbedSchema(false));

  value_item = test::DataItem(value_item.item(), explicit_schema_2, db);
  EXPECT_THAT(
      value_item.EmbedSchema(false),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(
                   "existing schema .* differs from the provided schema .*")));

  // Slice
  auto value_slice = test::AllocateDataSlice(2, explicit_schema_2, db);
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[0], schema::kSchemaAttr,
                                  DataItem(explicit_schema_2)));  // The same
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[1], schema::kSchemaAttr,
                                  DataItem(schema::kObject)));  // Different

  EXPECT_THAT(
      value_slice.EmbedSchema(false),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(
                   "existing schemas .* differ from the provided schema .*")));

  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[1], schema::kSchemaAttr,
                                  DataItem(schema::kObject)));  // Different
  EXPECT_THAT(
      value_slice.EmbedSchema(false),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(
                   "existing schemas .* differ from the provided schema .*")));

  // Conflict and missing
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[0], schema::kSchemaAttr,
                                  DataItem(explicit_schema_1)));  // Different
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[1], schema::kSchemaAttr,
                                  DataItem()));  // Empty
  EXPECT_THAT(
      value_slice.EmbedSchema(false),
      StatusIs(absl::StatusCode::kInvalidArgument,
               MatchesRegex(
                   "existing schemas .* differ from the provided schema .*")));
}

TEST(DataSliceTest, EmbedSchema_Object_Overwrite) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto db_impl, db->GetMutableImpl());
  auto explicit_schema_1 = internal::AllocateExplicitSchema();
  auto explicit_schema_2 = internal::AllocateExplicitSchema();

  // Item
  auto item = DataItem(internal::AllocateSingleObject());
  ASSERT_OK(db_impl.get().SetAttr(item, schema::kSchemaAttr,
                                  DataItem(explicit_schema_1)));
  auto value_item =
      test::DataItem(internal::AllocateSingleObject(), explicit_schema_1, db);
  // Check that it is okay to embed the same schema.
  ASSERT_OK(value_item.EmbedSchema(true));

  value_item = test::DataItem(value_item.item(), explicit_schema_2, db);
  ASSERT_OK_AND_ASSIGN(auto embedded_item, value_item.EmbedSchema(true));
  ASSERT_OK_AND_ASSIGN(auto schema_attr,
                       embedded_item.GetAttr(schema::kSchemaAttr));
  EXPECT_EQ(schema_attr.item(), explicit_schema_2);

  // Slice
  auto slice = internal::DataSliceImpl::AllocateEmptyObjects(2);
  auto value_slice = test::AllocateDataSlice(2, explicit_schema_2, db);
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[0], schema::kSchemaAttr,
                                  DataItem(explicit_schema_2)));  // The same
  ASSERT_OK(db_impl.get().SetAttr(value_slice.slice()[1], schema::kSchemaAttr,
                                  DataItem(schema::kObject)));  // Different

  ASSERT_OK_AND_ASSIGN(auto embedded_slice, value_slice.EmbedSchema(true));
  ASSERT_OK_AND_ASSIGN(schema_attr,
                       embedded_slice.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(schema_attr.slice(),
              ElementsAre(explicit_schema_2, explicit_schema_2));
}

TEST(DataSliceTest, EmbedSchema_NotAllowed_On_NoFollowSlice) {
  auto db = DataBag::Empty();
  auto ds = test::AllocateDataSlice(3, internal::AllocateExplicitSchema(), db);
  ASSERT_OK_AND_ASSIGN(auto nofollow_ds, NoFollow(ds));
  EXPECT_THAT(nofollow_ds.EmbedSchema(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "schema must not be a NoFollow schema"));
}

TEST(DataSliceTest, ContainsOnlyLists_Empty) {
  // schema INT64
  EXPECT_FALSE(test::DataSlice<int64_t>({}).ContainsOnlyLists());
  EXPECT_FALSE(test::DataSlice<int64_t>({std::nullopt}).ContainsOnlyLists());

  // schema ANY (type-erased INT64)
  EXPECT_TRUE(test::DataSlice<int64_t>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<int64_t>({std::nullopt})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyLists());

  // schema ANY (type-erased ObjectId)
  EXPECT_TRUE(test::DataSlice<ObjectId>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<ObjectId>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyLists());

  // schema OBJECT
  EXPECT_TRUE(test::DataSlice<ObjectId>({}).ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<ObjectId>({std::nullopt}).ContainsOnlyLists());

  // schema LIST[INT32]
  auto db = DataBag::Empty();
  auto list_schema =
      test::Schema(*CreateListSchema(db, test::Schema(schema::kInt32)), db);

  EXPECT_TRUE(test::DataSlice<ObjectId>({}, db)
                  .WithSchema(list_schema)
                  .value()
                  .ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<ObjectId>({std::nullopt}, db)
                  .WithSchema(list_schema)
                  .value()
                  .ContainsOnlyLists());
}

TEST(DataSliceTest, ContainsOnlyLists_NonEmpty) {
  auto db = DataBag::Empty();
  auto list_schema =
      test::Schema(*CreateListSchema(db, test::Schema(schema::kInt32)), db);

  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleList()}, db)
                  .WithSchema(list_schema)
                  ->ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleList()}, db)
                  .WithSchema(test::DataItem(schema::kAny))
                  ->ContainsOnlyLists());
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleList()}, db)
                  .WithSchema(test::DataItem(schema::kObject))
                  ->ContainsOnlyLists());

  // Note: behavior if list_schema is used with non-list values is unspecified.
  // Do not rely on the following line's implications in other code.
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                  .WithSchema(list_schema)
                  ->ContainsOnlyLists());

  EXPECT_FALSE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                   .WithSchema(test::DataItem(schema::kAny))
                   ->ContainsOnlyLists());
  EXPECT_FALSE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                   .WithSchema(test::DataItem(schema::kObject))
                   ->ContainsOnlyLists());
}

TEST(DataSliceTest, ContainsOnlyDicts_Empty) {
  // schema INT64
  EXPECT_FALSE(test::DataSlice<int64_t>({}).ContainsOnlyDicts());
  EXPECT_FALSE(test::DataSlice<int64_t>({std::nullopt}).ContainsOnlyDicts());

  // schema ANY (type-erased INT64)
  EXPECT_TRUE(test::DataSlice<int64_t>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<int64_t>({std::nullopt})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyDicts());

  // schema ANY (type-erased ObjectId)
  EXPECT_TRUE(test::DataSlice<ObjectId>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<ObjectId>({})
                  .WithSchema(test::DataItem(schema::kAny))
                  .value()
                  .ContainsOnlyDicts());

  // schema OBJECT
  EXPECT_TRUE(test::DataSlice<ObjectId>({}).ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<ObjectId>({std::nullopt}).ContainsOnlyDicts());

  // schema DICT{INT32, INT32}
  auto db = DataBag::Empty();
  auto dict_schema =
      test::Schema(*CreateDictSchema(db, test::Schema(schema::kInt32),
                                     test::Schema(schema::kInt32)),
                   db);

  EXPECT_TRUE(test::DataSlice<ObjectId>({}, db)
                  .WithSchema(dict_schema)
                  .value()
                  .ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<ObjectId>({std::nullopt}, db)
                  .WithSchema(dict_schema)
                  .value()
                  .ContainsOnlyDicts());
}

TEST(DataSliceTest, ContainsOnlyDicts_NonEmpty) {
  auto db = DataBag::Empty();
  auto dict_schema =
      test::Schema(*CreateDictSchema(db, test::Schema(schema::kInt32),
                                     test::Schema(schema::kInt32)),
                   db);

  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleDict()}, db)
                  .WithSchema(dict_schema)
                  ->ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleDict()}, db)
                  .WithSchema(test::DataItem(schema::kAny))
                  ->ContainsOnlyDicts());
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleDict()}, db)
                  .WithSchema(test::DataItem(schema::kObject))
                  ->ContainsOnlyDicts());

  // Note: behavior if dict_schema is used with non-dict values is unspecified.
  // Do not rely on the following line's implications in other code.
  EXPECT_TRUE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                  .WithSchema(dict_schema)
                  ->ContainsOnlyDicts());

  EXPECT_FALSE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                   .WithSchema(test::DataItem(schema::kAny))
                   ->ContainsOnlyDicts());
  EXPECT_FALSE(test::DataSlice<ObjectId>({internal::AllocateSingleObject()}, db)
                   .WithSchema(test::DataItem(schema::kObject))
                   ->ContainsOnlyDicts());
}

TEST(DataSliceTest, SetGetPrimitiveAttributes_EntityCreator) {
  auto ds_primitive = test::DataSlice<int64_t>({1, 2, 3});

  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  // Setting attributes on DataSlices with Explicit Schema requires schema attr
  // to be already present.
  ASSERT_OK(ds.GetSchema().SetAttr("a", ds_primitive.GetSchema()));
  ASSERT_OK(ds.SetAttr("a", ds_primitive));

  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_primitive_get.size(), 3);
  EXPECT_THAT(ds_primitive_get.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds_primitive_get.dtype(), GetQType<int64_t>());
  EXPECT_EQ(ds_primitive_get.slice().allocation_ids().size(), 0);
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(1, 2, 3));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt64);

  // implicit int32 -> int64 casting is allowed.
  auto ds_int32_primitive = test::DataSlice<int>({4, 5, 6});
  ASSERT_OK(ds.SetAttr("a", ds_int32_primitive));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(4, 5, 6));
}

TEST(DataSliceTest, SetGetPrimitiveAttributes_ObjectCreator) {
  auto ds_primitive = test::DataSlice<int64_t>({1, 2, 3});

  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.SetAttr("a", ds_primitive));
  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_primitive_get.size(), 3);
  EXPECT_THAT(ds_primitive_get.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds_primitive_get.dtype(), GetQType<int64_t>());
  EXPECT_EQ(ds_primitive_get.slice().allocation_ids().size(), 0);
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(1, 2, 3));
  // Setting an attribute updates schema.
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto schema, ds.GetAttr(schema::kSchemaAttr));
  ASSERT_OK_AND_ASSIGN(auto schema_a, schema.GetAttr("a"));
  for (const auto& schema_a_item : schema_a.slice()) {
    EXPECT_EQ(schema_a_item, schema::kInt64);
  }
}

TEST(DataSliceTest, SetGetAttr_FromEmptyItem_EntityCreator) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto entity_schema,
      CreateEntitySchema(db, {"a"}, {test::Schema(schema::kInt32)}));

  auto ds = test::DataItem(internal::MissingValue(), entity_schema.item(), db);
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::DataItem(internal::MissingValue(), schema::kInt32, db))));

  ASSERT_OK(ds.SetAttr("a", test::DataItem(42)));
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::DataItem(internal::MissingValue(), schema::kInt32, db))));
}

TEST(DataSliceTest, SetGetAttr_FromEmptySlice_EntityCreator) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto entity_schema,
      CreateEntitySchema(db, {"a"}, {test::Schema(schema::kInt32)}));

  auto ds = test::EmptyDataSlice(DataSlice::JaggedShape::FlatFromSize(3),
                                 entity_schema.item(), db);
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::EmptyDataSlice(ds.GetShape(), schema::kInt32, db))));

  ASSERT_OK(ds.SetAttr("a", test::DataItem(42)));
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::EmptyDataSlice(ds.GetShape(), schema::kInt32, db))));
}

TEST(DataSliceTest, SetGetAttr_FromEmptyItem_ObjectCreator) {
  auto db = DataBag::Empty();
  auto ds = test::DataItem(internal::MissingValue(), schema::kObject, db);
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::DataItem(internal::MissingValue(), schema::kObject, db))));

  ASSERT_OK(ds.SetAttr("a", test::DataItem(42)));
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::DataItem(internal::MissingValue(), schema::kObject, db))));
  EXPECT_THAT(
      ds.GetAttrWithDefault("a", test::DataItem(42)),
      IsOkAndHolds(IsEquivalentTo(
          test::DataItem(internal::MissingValue(), schema::kInt32, db))));
}

TEST(DataSliceTest, SetGetAttr_FromEmptySlice_ObjectCreator) {
  auto db = DataBag::Empty();
  auto ds = test::EmptyDataSlice(DataSlice::JaggedShape::FlatFromSize(3),
                                 schema::kObject, db);
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::EmptyDataSlice(ds.GetShape(), schema::kObject, db))));

  ASSERT_OK(ds.SetAttr("a", test::DataItem(42)));
  EXPECT_THAT(
      ds.GetAttr("a"),
      IsOkAndHolds(IsEquivalentTo(
          test::EmptyDataSlice(ds.GetShape(), schema::kObject, db))));
  EXPECT_THAT(
      ds.GetAttrWithDefault("a", test::DataItem(42)),
      IsOkAndHolds(IsEquivalentTo(
          test::EmptyDataSlice(ds.GetShape(), schema::kInt32, db))));
}

TEST(DataSliceTest, ObjectMissingSchemaAttr) {
  arolla::InitArolla();
  auto ds_a = test::DataSlice<int>({1, 2, 3});
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::FromAttrs(db, {"a"}, {ds_a}));

  arolla::DenseArray<internal::ObjectId> array =
      ds.slice().values<internal::ObjectId>();

  auto ds_2 = ds.WithDb(DataBag::Empty());
  absl::StatusOr<DataSlice> result = ds_2.GetAttr("a");

  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  std::optional<internal::Error> error =
      internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  result = ds_2.GetAttrWithDefault("a", test::DataItem(42));
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  result = ds_2.SetAttr("a", test::DataSlice<int>({1, 1, 1}));
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  result = ds_2.DelAttr("a");
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());
}

TEST(DataSliceTest, ObjectMissingSchemaAttr_Primitive) {
  DataSlice ds_a = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(DataSlice ds,
                       ds_a.WithSchema(test::Schema(schema::kObject)));
  DataSlice obj = ds.WithDb(DataBag::Empty());

  absl::StatusOr<DataSlice> result = obj.GetAttr("a");
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  std::optional<internal::Error> error =
      internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());
}

TEST(DataSliceTest, ObjectMissingSchemaAttr_List) {
  arolla::InitArolla();
  DataBagPtr bag = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      DataSlice list,
      CreateNestedList(bag, test::DataSlice<int>({1, 2, 3}),
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       list.WithSchema(test::Schema(schema::kObject)));

  absl::StatusOr<DataSlice> result = obj.ExplodeList(0, std::nullopt);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  std::optional<internal::Error> error =
      internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  absl::Status status = obj.SetInList(test::DataItem(1), test::DataItem(1));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  status = obj.AppendToList(test::DataItem(1));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  status = obj.ReplaceInList(0, 1, test::DataSlice<int>({1}));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  status = obj.RemoveInList(test::DataItem(1));
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  status = obj.RemoveInList(0, std::nullopt);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  result = obj.PopFromList(test::DataItem(1));
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());
}

TEST(DataSliceTest, ObjectMissingSchemaAttr_Dict) {
  arolla::InitArolla();
  DataBagPtr bag = DataBag::Empty();

  DataSlice key_item = test::DataItem(1);
  DataSlice value_item = test::DataItem("value");

  ASSERT_OK_AND_ASSIGN(DataSlice dict,
                       CreateDictShaped(bag, DataSlice::JaggedShape::Empty(),
                                        key_item, value_item));
  ASSERT_OK_AND_ASSIGN(DataSlice obj,
                       dict.WithSchema(test::Schema(schema::kObject)));

  absl::StatusOr<DataSlice> result = obj.GetFromDict(key_item);
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  std::optional<internal::Error> error =
      internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  result = obj.GetDictKeys();
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(result.status());
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());

  absl::Status status = obj.SetInDict(key_item, value_item);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("missing __schema__ attribute")));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_missing_object_schema());
}

TEST(DataSliceTest, SetAttr_OnItemIdNotAllowed) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  auto ds_object_id = test::DataSlice<ObjectId>({
    internal::AllocateSingleObject(), internal::AllocateSingleObject(),
    internal::AllocateSingleObject()}, db);
  ASSERT_OK_AND_ASSIGN(ds_object_id,
                       ds_object_id.WithSchema(test::Schema(schema::kItemId)));
  EXPECT_THAT(ds_object_id.SetAttr("a", ds_primitive),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("setting attributes on ITEMID slices is not "
                                 "allowed")));
}

TEST(DataSliceTest, SetAttr_ObjectWithExplicitSchema_Incompatible) {
  arolla::InitArolla();
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds_1, EntityCreator::FromAttrs(db, {"a"}, {test::DataItem(1)}));
  ASSERT_OK_AND_ASSIGN(ds_1, ds_1.EmbedSchema());
  ASSERT_OK_AND_ASSIGN(
      auto ds_2, EntityCreator::FromAttrs(db, {"a"}, {test::DataItem(1.0)}));
  ASSERT_OK_AND_ASSIGN(ds_2, ds_2.EmbedSchema());

  ObjectId obj_id_1 = ds_1.item().value<ObjectId>();
  ObjectId obj_id_2 = ds_2.item().value<ObjectId>();
  auto ds_object_id = test::DataSlice<ObjectId>({obj_id_1, obj_id_2}, db);

  absl::Status status = ds_object_id.SetAttr("a", test::DataItem(1));
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("two different types: FLOAT64 and INT32")));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_incompatible_schema());
  EXPECT_THAT(
      error->error_message(),
      HasSubstr("explicitly override schema of 'a' in the Object schema."));
}

TEST(DataSliceTest, SetAttrWithUpdateSchema_EntityCreator) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds,
                       EntityCreator::FromAttrs(db, {"a"}, {ds_primitive}));

  // Explicit casting from int64 -> int32 is not allowed - only implicit.
  auto ds_int64_primitive = test::DataSlice<int64_t>({12, 42, 97});
  EXPECT_THAT(
      ds.SetAttr("a", ds_int64_primitive),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("INT32"), HasSubstr("INT64"))));
  ASSERT_OK(ds.SetAttrWithUpdateSchema("a", ds_int64_primitive));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(12, 42, 97));
}

TEST(DataSliceTest, SetAttrWithUpdateSchema_ObjectCreator) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"a"}, {ds_primitive}));

  // SetAttrWithUpdateSchema works the same way as SetAttr on objects with
  // implicit schema.
  auto ds_int64_primitive = test::DataSlice<int64_t>({12, 42, 97});
  ASSERT_OK(ds.SetAttrWithUpdateSchema("a", ds_int64_primitive));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(12, 42, 97));
}

TEST(DataSliceTest, SetAttrWithUpdateSchema_ObjectsWithExplicitSchema) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds_e,
                       EntityCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  ASSERT_OK(ds.SetAttr(schema::kSchemaAttr, ds_e.GetSchema()));

  auto ds_int64_primitive = test::DataSlice<int64_t>({12, 42, 97});
  EXPECT_THAT(
      ds.SetAttr("a", ds_int64_primitive),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("INT32"), HasSubstr("INT64"))));
  ASSERT_OK(ds.SetAttrWithUpdateSchema("a", ds_int64_primitive));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(12, 42, 97));
}

TEST(DataSliceTest, SetMultipleAttrs_Entity) {
  auto db = DataBag::Empty();
  auto ds_a = test::DataItem(1);
  auto ds_b = test::DataItem("a");
  ASSERT_OK_AND_ASSIGN(auto ds,
                       EntityCreator::FromAttrs(db, {"a", "b"}, {ds_a, ds_b}));

  ds_a = test::DataItem(42);
  ds_b = test::DataItem("abc");
  ASSERT_OK(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}));
  EXPECT_THAT(ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::item, Eq(42))));
  EXPECT_THAT(
      ds.GetAttr("b"),
      IsOkAndHolds(Property(&DataSlice::item, Eq(arolla::Text("abc")))));
}

TEST(DataSliceTest, SetMultipleAttrs_Object) {
  auto db = DataBag::Empty();
  auto ds_a = test::DataItem(1);
  auto ds_b = test::DataItem("a");
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"a", "b"}, {ds_a, ds_b}));

  ds_a = test::DataItem(42);
  ds_b = test::DataItem("abc");
  ASSERT_OK(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}));
  EXPECT_THAT(ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::item, Eq(42))));
  EXPECT_THAT(
      ds.GetAttr("b"),
      IsOkAndHolds(Property(&DataSlice::item, Eq(arolla::Text("abc")))));
}

TEST(DataSliceTest, SetMultipleAttrs_UpdateSchema_Entity) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::FromAttrs(db, {}, {}));
  auto ds_a = test::DataItem(42);
  auto ds_b = test::DataItem("abc");
  EXPECT_THAT(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing on the schema")));

  ASSERT_OK(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}, /*update_schema=*/true));
  EXPECT_THAT(ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::item, Eq(42))));
  EXPECT_THAT(
      ds.GetAttr("b"),
      IsOkAndHolds(Property(&DataSlice::item, Eq(arolla::Text("abc")))));
}

TEST(DataSliceTest, SetMultipleAttrs_UpdateSchema_Object) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::FromAttrs(db, {}, {}));
  ASSERT_OK(ds.SetAttr(schema::kSchemaAttr,
                       test::Schema(internal::AllocateExplicitSchema())));
  auto ds_a = test::DataItem(42);
  auto ds_b = test::DataItem("abc");
  EXPECT_THAT(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing on the schema")));

  ASSERT_OK(ds.SetAttrs({"a", "b"}, {ds_a, ds_b}, /*update_schema=*/true));
  EXPECT_THAT(ds.GetAttr("a"),
              IsOkAndHolds(Property(&DataSlice::item, Eq(42))));
  EXPECT_THAT(
      ds.GetAttr("b"),
      IsOkAndHolds(Property(&DataSlice::item, Eq(arolla::Text("abc")))));
}

TEST(DataSliceTest, SetGetSchemaSlice) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto db = DataBag::Empty();
  auto fallback_db = DataBag::Empty();

  auto schema_ds = test::DataSlice<ObjectId>(
      {internal::AllocateExplicitSchema(), internal::AllocateExplicitSchema()},
      schema::kSchema, db);

  auto schema_a_part_1 = test::DataSlice<schema::DType>(
      {schema::kInt32, std::nullopt}, schema::kSchema);
  ASSERT_OK(schema_ds.SetAttr("a", schema_a_part_1));

  auto schema_a_part_2 = test::DataSlice<schema::DType>(
      {std::nullopt, schema::kFloat32}, schema::kSchema);
  ASSERT_OK(schema_ds.WithDb(fallback_db).SetAttr("a", schema_a_part_2));

  schema_ds =
      schema_ds.WithDb(DataBag::ImmutableEmptyWithFallbacks({db, fallback_db}));
  ASSERT_OK_AND_ASSIGN(auto schema_a_get, schema_ds.GetAttr("a"));
  EXPECT_EQ(schema_a_get.GetSchemaImpl(), schema::kSchema);
  EXPECT_THAT(schema_a_get.slice(),
              ElementsAre(schema::kInt32, schema::kFloat32));

  ASSERT_OK_AND_ASSIGN(schema_ds,
                       schema_ds.WithSchema(test::Schema(schema::kAny)));
  ASSERT_OK_AND_ASSIGN(schema_a_get, schema_ds.GetAttr("a"));
  EXPECT_EQ(schema_a_get.GetSchemaImpl(), schema::kAny);
  EXPECT_THAT(schema_a_get.slice(), ElementsAre(DataItem(), DataItem()));
}

TEST(DataSliceTest, SetGetObjectAttributesSameDb) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto db = DataBag::Empty();

  for (auto other_db : {db, std::shared_ptr<DataBag>(nullptr)}) {
    auto ds_a = test::AllocateDataSlice(shape.size(), schema::kAny, other_db);

    ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
    ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kAny)));
    ASSERT_OK(ds.SetAttr("a", ds_a));
    ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
    EXPECT_EQ(ds_a_get.GetDb(), db);
    EXPECT_EQ(ds_a_get.size(), shape.size());
    EXPECT_THAT(ds_a_get.GetShape(), IsEquivalentTo(shape));
    EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());
    EXPECT_THAT(ds_a_get.slice().allocation_ids(),
                ElementsAreArray(ds_a.slice().allocation_ids()));
    EXPECT_THAT(ds_a_get.slice(), ElementsAreArray(ds_a.slice()));
    EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kAny);
    EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());
  }
}

TEST(DataSliceTest, SetGetObjectAttributesOtherDb_EntityCreator) {
  auto db = DataBag::Empty();
  auto db2 = DataBag::Empty();

  auto ds_a = test::AllocateDataSlice(3, schema::kAny, db2);
  auto ds_x = test::AllocateDataSlice(3, schema::kAny);
  ASSERT_OK(ds_a.SetAttr("x", ds_x));

  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kAny)));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(ds_x);
  adoption_queue.Add(ds_a);
  ASSERT_OK(adoption_queue.AdoptInto(*ds.GetDb()));

  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetDb(), db);
  EXPECT_THAT(ds_a_get.slice(), ElementsAreArray(ds_a.slice()));

  ASSERT_OK_AND_ASSIGN(auto ds_x_get, ds_a_get.GetAttr("x"));
  EXPECT_EQ(ds_x_get.GetDb(), db);
  EXPECT_THAT(ds_x_get.slice(), ElementsAreArray(ds_x.slice()));
  // Merging copy schemas.
  EXPECT_EQ(ds_x_get.GetSchemaImpl(), schema::kAny);
  EXPECT_EQ(ds_x_get.dtype(), GetQType<ObjectId>());
}

TEST(DataSliceTest, SetGetObjectAttributesOtherDbConflict) {
  auto db = DataBag::Empty();
  auto db2 = DataBag::Empty();

  auto ds_a = test::AllocateDataSlice(3, schema::kAny, db2);
  auto ds_a_conflict = test::DataItem(ds_a.slice()[1], schema::kAny, db);
  auto ds_x = test::AllocateDataSlice(3, schema::kAny);
  ASSERT_OK(ds_a.SetAttr("x", ds_x));
  // Adding a conflict as self reference.
  ASSERT_OK(ds_a_conflict.SetAttr("x", ds_a_conflict));

  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(ds_a);
  EXPECT_THAT(
      adoption_queue.AdoptInto(*ds.GetDb()),
      StatusIs(absl::StatusCode::kFailedPrecondition, HasSubstr("conflict")));
}

TEST(DataSliceTest, SetGetObjectAttributesWithFallback) {
  auto ds_a = test::AllocateDataSlice(3, schema::kAny);

  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", test::Schema(schema::kAny)));
  ASSERT_OK(ds.SetAttr("a", ds_a));
  db = DataBag::ImmutableEmptyWithFallbacks({db});
  ds = ds.WithDb(db);

  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.size(), shape.size());
  EXPECT_THAT(ds_a_get.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.slice().allocation_ids(),
              ElementsAreArray(ds_a.slice().allocation_ids()));
  EXPECT_THAT(ds_a_get.slice(), ElementsAreArray(ds_a.slice()));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kAny);
  EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());
}

TEST(DataSliceTest, SetGetObjectAttributesWithOtherDbWithFallback) {
  auto ds_a = test::AllocateDataSlice(3, schema::kAny);
  auto ds_b = test::AllocateDataSlice(3, schema::kAny);

  auto db1 = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds_x, EntityCreator::Shaped(db1, shape, {}, {}));
  ASSERT_OK(ds_x.GetSchema().SetAttr("a", ds_a.GetSchema()));
  ASSERT_OK(ds_x.SetAttr("a", ds_a));
  ASSERT_OK(ds_x.GetSchema().SetAttr("b", ds_a.GetSchema()));
  ASSERT_OK(ds_x.SetAttr("b", ds_a));
  db1 = DataBag::ImmutableEmptyWithFallbacks({db1});
  // overwrite fallback
  ASSERT_OK(ds_x.GetSchema().SetAttr("b", ds_b.GetSchema()));
  ASSERT_OK(ds_x.SetAttr("b", ds_b));
  ASSERT_OK(ds_x.GetSchema().SetAttr("b", ds_b.GetSchema()));

  auto db2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds2, EntityCreator::Shaped(db2, shape, {}, {}));
  ASSERT_OK(ds2.GetSchema().SetAttr("x", ds_x.GetSchema()));
  ASSERT_OK(ds2.SetAttr("x", ds_x));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(ds_x);
  ASSERT_OK(adoption_queue.AdoptInto(*db2));

  ASSERT_OK_AND_ASSIGN(auto ds_x_get, ds2.GetAttr("x"));
  EXPECT_THAT(ds_x_get.slice(), ElementsAreArray(ds_x.slice()));
  ds_x = DataSlice();  // not supposed to be used after
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds_x_get.GetAttr("a"));
  EXPECT_THAT(ds_a_get.slice(), ElementsAreArray(ds_a.slice()));
  // Setting an attribute updates schema.
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kAny);
  EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());

  ASSERT_OK_AND_ASSIGN(auto ds_b_get, ds_x_get.GetAttr("b"));
  EXPECT_THAT(ds_b_get.slice(), ElementsAreArray(ds_b.slice()));
  // Setting an attribute updates schema.
  EXPECT_EQ(ds_b_get.GetSchemaImpl(), schema::kAny);
  EXPECT_EQ(ds_b_get.dtype(), GetQType<ObjectId>());
}

TEST(DataSliceTest, SetGetObjectAttributes_ObjectCreator) {
  auto ds_b = test::DataSlice<int>({1, 2, 3});
  auto db1 = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds_a, ObjectCreator::Shaped(db1, shape, {}, {}));
  ASSERT_OK(ds_a.SetAttr("b", ds_b));

  auto db2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db2, shape, {}, {}));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(ds_a);
  ASSERT_OK(adoption_queue.AdoptInto(*db2));

  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.size(), shape.size());
  EXPECT_THAT(ds_a_get.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.slice().allocation_ids(),
              ElementsAreArray(ds_a.slice().allocation_ids()));
  EXPECT_EQ(ds_a_get.dtype(), GetQType<ObjectId>());

  // Schema "a".
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kObject);
  ASSERT_OK_AND_ASSIGN(auto ds_a_schema, ds_a_get.GetAttr(schema::kSchemaAttr));
  EXPECT_TRUE(ds_a_schema.slice()[0].value<ObjectId>().IsImplicitSchema());
  EXPECT_TRUE(ds_a_schema.slice()[1].value<ObjectId>().IsImplicitSchema());
  EXPECT_TRUE(ds_a_schema.slice()[2].value<ObjectId>().IsImplicitSchema());

  // Attribute "b" of slice "a" and its schema.
  ASSERT_OK_AND_ASSIGN(auto ds_b_schema, ds_a_schema.GetAttr("b"));
  EXPECT_EQ(ds_b_schema.slice()[0], schema::kInt32);
  EXPECT_EQ(ds_b_schema.slice()[1], schema::kInt32);
  EXPECT_EQ(ds_b_schema.slice()[2], schema::kInt32);
  ASSERT_OK_AND_ASSIGN(auto ds_b_get, ds_a_get.GetAttr("b"));
  EXPECT_EQ(ds_b_get.GetSchemaImpl(), schema::kInt32);
}

TEST(DataSliceTest, OverwriteSchemaAndAttributes_ObjectCreator) {
  auto ds_a = test::DataSlice<int>({1, 2, 3});
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.SetAttr("a", ds_a));

  // Overwriting overwrites schema, too.
  auto ds_a_prim = test::DataSlice<int64_t>({4, 5, 6});
  ASSERT_OK(ds.SetAttr("a", ds_a_prim));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(4, 5, 6));

  // Deeper tests for GetObjSchemaAttr.
  // Overwrite INT32 with None - erase schema attribute for only 1 schema in
  // __schema__.
  ASSERT_OK_AND_ASSIGN(auto ds_schema, ds.GetAttr(schema::kSchemaAttr));
  ASSERT_OK_AND_ASSIGN(internal::DataBagImpl & db_mutable_impl,
                       ds_schema.GetDb()->GetMutableImpl());
  ASSERT_OK(db_mutable_impl.DelSchemaAttr(ds_schema.slice()[1], "a"));
  auto ds_1 = test::DataItem(ds.slice()[1], ds.GetSchemaImpl(), db);
  EXPECT_THAT(ds_1.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));

  // Overwrite schema even when some schema items is missing (1st).
  ASSERT_OK_AND_ASSIGN(ds_a_prim,
                       ds_a_prim.WithSchema(test::Schema(schema::kAny)));
  ASSERT_OK(ds.SetAttr("a", ds_a_prim));
  ASSERT_OK_AND_ASSIGN(ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kAny);
  EXPECT_THAT(ds_a_get.slice(), ElementsAre(4, 5, 6));
}

TEST(DataSliceTest, SetAttr_NoFollowSchema_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));

  ASSERT_OK_AND_ASSIGN(ds, NoFollow(ds));

  EXPECT_THAT(ds.SetAttr("a", test::DataSlice<int>({1, 2, 3})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing")));
}

TEST(DataSliceTest, SetAttr_NoFollowSchema_Object) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));

  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema());
  ASSERT_OK_AND_ASSIGN(auto nofollow_schema,
                       CreateNoFollowSchema(explicit_schema));
  ASSERT_OK(ds.SetAttr(schema::kSchemaAttr, nofollow_schema));

  EXPECT_THAT(ds.SetAttr("a", test::DataSlice<int>({1, 2, 3})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing")));
}

TEST(DataSliceTest, GetNoFollowAttr_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(
      auto ds_attr,
      EntityCreator::FromAttrs(db, {"y"}, {test::DataSlice<int>({1, 2})}));
  ASSERT_OK_AND_ASSIGN(auto nofollow_attr, NoFollow(ds_attr));
  ASSERT_TRUE(nofollow_attr.GetSchemaImpl().value<ObjectId>()
              .IsNoFollowSchema());

  ASSERT_OK_AND_ASSIGN(auto ds,
                       EntityCreator::FromAttrs(db, {"x"}, {nofollow_attr}));
  // Schema stores a NoFollow schema.
  EXPECT_THAT(
      ds.GetSchema().GetAttr("x"),
      IsOkAndHolds(Property(&DataSlice::item,
                            Eq(nofollow_attr.GetSchemaImpl()))));
  ASSERT_OK_AND_ASSIGN(auto unwrapped_nofollow, ds.GetAttr("x"));
  EXPECT_FALSE(unwrapped_nofollow.GetSchemaImpl().value<ObjectId>()
               .IsNoFollowSchema());
  EXPECT_EQ(unwrapped_nofollow.GetSchemaImpl(), ds_attr.GetSchemaImpl());
  EXPECT_THAT(unwrapped_nofollow.GetAttr("y"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));
}

TEST(DataSliceTest, GetNoFollowAttr_Object) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(
      auto ds_attr,
      ObjectCreator::FromAttrs(db, {"y"}, {test::DataSlice<int>({1, 2})}));
  ASSERT_OK_AND_ASSIGN(auto nofollow_attr, NoFollow(ds_attr));
  ASSERT_TRUE(nofollow_attr.GetSchemaImpl().value<ObjectId>()
              .IsNoFollowSchema());

  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"x"}, {nofollow_attr}));
  // Schema stores a NoFollow schema.
  EXPECT_THAT(
      ds.GetAttr(schema::kSchemaAttr)->GetAttr("x"),
      IsOkAndHolds(Property(&DataSlice::slice,
                            ElementsAre(nofollow_attr.GetSchemaImpl(),
                                        nofollow_attr.GetSchemaImpl()))));
  ASSERT_OK_AND_ASSIGN(auto unwrapped_nofollow, ds.GetAttr("x"));
  EXPECT_EQ(unwrapped_nofollow.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(unwrapped_nofollow.GetAttr("y"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));
}

TEST(DataSliceTest, GetNoFollow_ListItems) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(
      auto items,
      EntityCreator::FromAttrs(db, {"y"}, {test::DataSlice<int>({1, 2})}));
  ASSERT_OK_AND_ASSIGN(auto nofollow_items, NoFollow(items));
  ASSERT_TRUE(nofollow_items.GetSchemaImpl().value<ObjectId>()
              .IsNoFollowSchema());
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListsFromLastDimension(db, nofollow_items, /*schema=*/std::nullopt,
                                   /*item_schema=*/std::nullopt));
  // Schema stores a NoFollow schema.
  EXPECT_THAT(
      lists.GetSchema().GetAttr(schema::kListItemsSchemaAttr),
      IsOkAndHolds(Property(&DataSlice::item,
                            Eq(nofollow_items.GetSchemaImpl()))));
  ASSERT_OK_AND_ASSIGN(auto unwrapped_nofollow,
                       lists.ExplodeList(0, std::nullopt));
  EXPECT_FALSE(unwrapped_nofollow.GetSchemaImpl().value<ObjectId>()
               .IsNoFollowSchema());
  EXPECT_THAT(unwrapped_nofollow.GetAttr("y"),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(1, 2))));
}

TEST(DataSliceTest, SetAttr_AutoBroadcasting) {
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});

  auto edge_1 = CreateEdge({0, 3});
  auto edge_2 = CreateEdge({0, 2, 4, 6});
  ASSERT_OK_AND_ASSIGN(auto res_shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, res_shape, {}, {}));
  ASSERT_OK(ds.GetSchema().SetAttr("a", ds_primitive.GetSchema()));
  ASSERT_OK(ds.SetAttr("a", ds_primitive));

  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_primitive_get.size(), ds.size());
  EXPECT_THAT(ds_primitive_get.GetShape(), IsEquivalentTo(res_shape));
  EXPECT_EQ(ds_primitive_get.dtype(), GetQType<int>());
  EXPECT_EQ(ds_primitive_get.slice().allocation_ids().size(), 0);
  EXPECT_THAT(ds_primitive_get.slice().values<int>(),
              ElementsAre(1, 1, 2, 2, 3, 3));
}

TEST(DataSliceTest, SetAttr_BroadcastingError) {
  auto edge_1 = CreateEdge({0, 3});
  auto edge_2 = CreateEdge({0, 2, 4, 6});
  ASSERT_OK_AND_ASSIGN(auto res_shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto ds_primitive = test::DataSlice<int>({1, 2, 3, 4, 5, 6}, res_shape);

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      ObjectCreator::Shaped(
          db, DataSlice::JaggedShape::FlatFromSize(3), {}, {}));
  EXPECT_THAT(ds.SetAttr("a", ds_primitive),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("trying to assign a slice with 2 dim")));
}

TEST(DataSliceTest, SetGetError) {
  auto ds = test::AllocateDataSlice(3, schema::kAny);
  auto ds_a = test::AllocateDataSlice(2, schema::kAny);
  EXPECT_THAT(ds.SetAttr("QQQ", ds_a),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ContainsRegex("without.*DataBag")));
  EXPECT_THAT(ds.GetAttr("a"), StatusIs(absl::StatusCode::kInvalidArgument,
                                        HasSubstr("without a DataBag: a")));

  ds = ds.WithDb(DataBag::Empty());

  auto ds_primitive = test::DataSlice<int>({42, 42}, ds.GetDb());
  EXPECT_THAT(ds_primitive.SetAttr("a", ds_a),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("setting attributes on primitive slices is not"
                                 " allowed")));

  EXPECT_THAT(
      ds_a.WithDb(ds.GetDb()).SetAttr(schema::kSchemaAttr, ds_primitive),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("only schemas can be assigned to the '__schema__' "
                         "attribute, got INT32")));

  // ds has kd.ANY schema, thus missing lookups are allowed.
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.slice().size(), 3);
  // Basically missing.
  EXPECT_EQ(ds_a_get.slice().present_count(), 0);
  EXPECT_FALSE(ds_a_get.impl_owns_value());
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kAny);

  auto allocated_schema = test::Schema(internal::AllocateExplicitSchema());
  ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(allocated_schema));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(
      ds.SetAttr("a", ds_a),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(absl::StrFormat(
              "DataSlice with shape=%s cannot be expanded to shape=%s",
              arolla::Repr(ds_a.GetShape()), arolla::Repr(ds.GetShape())))));
}

TEST(DataSliceTest, MissingAttribute_EntityCreator) {
  auto ds_int32 = test::DataSlice<int>({42, 42, 42});
  auto ds_text = test::DataSlice<arolla::Text>({"abc", "abc", "abc"});

  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  EXPECT_THAT(
      ds.SetAttr("a", ds_int32),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("the attribute 'a' is missing on the schema")));
}

TEST(DataSliceTest, SetGetError_ObjectCreator) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto implicit_schema,
                       objects.GetAttr(schema::kSchemaAttr));
  auto mixed_implicit_explicit_schema = test::DataSlice<ObjectId>(
      {implicit_schema.slice()[0].value<internal::ObjectId>(),
       internal::AllocateExplicitSchema(), internal::AllocateExplicitSchema()},
      schema::kSchema, db);
  ASSERT_OK(
      objects.SetAttr(schema::kSchemaAttr, mixed_implicit_explicit_schema));
  ASSERT_OK_AND_ASSIGN(auto explicit_schema_get,
                       objects.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(explicit_schema_get,
              IsEquivalentTo(mixed_implicit_explicit_schema));

  auto ds_a = test::DataItem("foo");
  EXPECT_THAT(
      objects.SetAttr("a", ds_a),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("the attribute 'a' is missing on the schema")));

  auto float_schema = test::Schema(schema::kFloat32);
  ASSERT_OK(mixed_implicit_explicit_schema.SetAttr("a", float_schema));
  absl::Status status = objects.SetAttr("a", ds_a);
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("FLOAT32"), HasSubstr("TEXT"))));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_incompatible_schema());

  // NOTE: If we overwrote IMPLICIT schemas above (regardless of error on
  // EXPLICIT schemas), this would not raise as we would overwrite EXPLICIT
  // schema too. Tests that IMPLICIT schema overwriting happens after all
  // EXPLICIT schemas have been processed.
  auto object_1 = test::DataItem(objects.slice()[1], db);
  status = object_1.SetAttr("a", ds_a);
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("FLOAT32"), HasSubstr("TEXT"))));
  error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(error->has_incompatible_schema());

  // Implicit schema gets overwritten when there are no errors and attr schema
  // is not the same.
  auto implicit_schema_0 = test::Schema(implicit_schema.slice()[0], db);
  auto object_0 = test::DataItem(objects.slice()[0], db);
  // object_0 has implicit_schema_0 at "__schema__" attr.
  ASSERT_OK(implicit_schema_0.SetAttr("a", float_schema));
  ASSERT_OK(object_0.SetAttr("a", ds_a));
  EXPECT_EQ(implicit_schema_0.GetAttr("a")->item(), schema::kText);
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, object_0.GetAttr("a"));
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kText);
  EXPECT_EQ(ds_a_get.item(), ds_a.item());
}

TEST(DataSliceTest, GetAttrWithDefault_Primitives_EntityCreator) {
  auto ds_primitive = test::DataSlice<int>({1, std::nullopt, 3});
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.SetAttrWithUpdateSchema("a", ds_primitive));

  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get,
                       ds.GetAttrWithDefault("a", test::DataItem(4)));
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(1, 4, 3));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt32);
}

TEST(DataSliceTest, GetAttrWithDefault_ObjectsAndMerging_EntityCreator) {
  auto ds_primitive = test::DataSlice<int>({1, std::nullopt, 3});
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));
  auto object_id_1 = internal::AllocateSingleObject();
  auto object_id_2 = internal::AllocateSingleObject();
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto ds_object = test::DataSlice<ObjectId>(
      {object_id_1, std::nullopt, object_id_2}, explicit_schema, ds.GetDb());
  ASSERT_OK(ds_object.SetAttrWithUpdateSchema("attr", ds_primitive));
  ASSERT_OK(ds.SetAttrWithUpdateSchema("a", ds_object));

  ASSERT_OK_AND_ASSIGN(auto default_val,
                       EntityCreator::FromAttrs(DataBag::Empty(), {}, {}));
  ASSERT_OK_AND_ASSIGN(default_val,
                       default_val.WithSchema(test::Schema(explicit_schema)));
  ASSERT_OK(default_val.SetAttrWithUpdateSchema("attr", test::DataItem(42)));

  ASSERT_OK_AND_ASSIGN(auto ds_object_get,
                       ds.GetAttrWithDefault("a", default_val));
  EXPECT_THAT(ds_object_get.slice(),
              ElementsAre(object_id_1, default_val.item(), object_id_2));
  EXPECT_EQ(ds_object_get.GetSchemaImpl(), explicit_schema);
  // Merging happened.
  ASSERT_OK_AND_ASSIGN(auto attr, ds_object_get.GetAttr("attr"));
  EXPECT_THAT(attr.slice(), ElementsAre(1, 42, 3));
  // With old DataBag.
  ASSERT_OK_AND_ASSIGN(attr, ds_object_get.WithDb(db).GetAttr("attr"));
  EXPECT_THAT(attr.slice(), ElementsAre(1, std::nullopt, 3));
}

TEST(DataSliceTest, GetAttrWithDefault_ResultIsDefault_EntityCreator) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, EntityCreator::Shaped(db, shape, {}, {}));

  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing")));
  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get,
                       ds.GetAttrWithDefault("a", test::DataItem(4)));
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(4, 4, 4));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt32);
}

TEST(DataSliceTest, GetAttrWithDefault_Primitives_ObjectCreator) {
  auto ds_primitive = test::DataSlice<int>({1, std::nullopt, 3});
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(ds.SetAttr("a", ds_primitive));

  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get,
                       ds.GetAttrWithDefault("a", test::DataItem("a")));
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(1, arolla::Text("a"), 3));
  // Conflict INT32 and TEXT => OBJECT.
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kObject);
}

TEST(DataSliceTest, GetAttrWithDefault_ObjectsAndMerging_ObjectCreator) {
  auto ds_primitive = test::DataSlice<int>({1, std::nullopt, 3});
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));
  auto object_id_1 = internal::AllocateSingleObject();
  auto object_id_2 = internal::AllocateSingleObject();
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto ds_object = test::DataSlice<ObjectId>(
      {object_id_1, std::nullopt, object_id_2}, explicit_schema, ds.GetDb());
  ASSERT_OK(ds_object.SetAttrWithUpdateSchema("attr", ds_primitive));
  ASSERT_OK(ds.SetAttr("a", ds_object));

  ASSERT_OK_AND_ASSIGN(auto default_val,
                       EntityCreator::FromAttrs(DataBag::Empty(), {}, {}));
  ASSERT_OK_AND_ASSIGN(default_val,
                       default_val.WithSchema(test::Schema(explicit_schema)));
  ASSERT_OK(default_val.SetAttrWithUpdateSchema("attr", test::DataItem(42)));

  ASSERT_OK_AND_ASSIGN(auto ds_object_get,
                       ds.GetAttrWithDefault("a", default_val));
  EXPECT_THAT(ds_object_get.slice(),
              ElementsAre(object_id_1, default_val.item(), object_id_2));
  EXPECT_EQ(ds_object_get.GetSchemaImpl(), explicit_schema);
  // Merging happened.
  ASSERT_OK_AND_ASSIGN(auto attr, ds_object_get.GetAttr("attr"));
  EXPECT_THAT(attr.slice(), ElementsAre(1, 42, 3));
  // With old DataBag.
  ASSERT_OK_AND_ASSIGN(attr, ds_object_get.WithDb(db).GetAttr("attr"));
  EXPECT_THAT(attr.slice(), ElementsAre(1, std::nullopt, 3));
}

TEST(DataSliceTest, GetAttrWithDefault_ResultIsDefault_ObjectCreator) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::Shaped(db, shape, {}, {}));

  auto default_val = test::DataSlice<arolla::Text>(
      {std::nullopt, "a", std::nullopt});
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("attribute 'a' is missing")));
  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get,
                       ds.GetAttrWithDefault("a", default_val));
  EXPECT_THAT(ds_primitive_get.slice(),
              ElementsAre(std::nullopt, arolla::Text("a"), std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kText);
}

TEST(DataSliceTest, GetAttrWithDefault_SchemaSlice) {
  ASSERT_OK_AND_ASSIGN(auto entity,
                       EntityCreator::FromAttrs(DataBag::Empty(), {}, {}));
  auto entity_schema = entity.GetSchema();
  ASSERT_OK_AND_ASSIGN(
      auto schema_attr,
      entity_schema.GetAttrWithDefault(
          "attr", test::DataItem(DataItem(), schema::kSchema)));
  EXPECT_EQ(schema_attr.item(), DataItem());
  EXPECT_EQ(schema_attr.GetSchemaImpl(), schema::kSchema);
}

TEST(DataSliceTest, GetAttrWithDefault_AnyNoSchema) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto ds = test::DataSlice<ObjectId>(
      {internal::AllocateSingleObject(), internal::AllocateSingleObject(),
       std::nullopt}, explicit_schema, db);
  ASSERT_OK_AND_ASSIGN(auto ds_any, ds.WithSchema(test::Schema(schema::kAny)));
  ASSERT_OK(ds_any.SetAttr("a", test::DataSlice<int>({1, std::nullopt, 3})));

  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get,
                       ds.GetAttrWithDefault("a", test::DataItem(4)));
  EXPECT_THAT(ds_primitive_get.slice(), ElementsAre(1, 4, std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt32);

  ASSERT_OK_AND_ASSIGN(ds_primitive_get,
                       ds_any.GetAttrWithDefault("a", test::DataItem("abc")));
  EXPECT_THAT(ds_primitive_get.slice(),
              ElementsAre(1, arolla::Text("abc"), std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kAny);

  // Missing data, missing schema.
  ASSERT_OK_AND_ASSIGN(
      ds_primitive_get,
      ds.GetAttrWithDefault("missing", test::DataItem("abc")));
  EXPECT_THAT(
      ds_primitive_get.slice(),
      ElementsAre(arolla::Text("abc"), arolla::Text("abc"), std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kText);

  // Schema from getattr is inferred to be INT32 and then combined with TEXT,
  // returns OBJECT.
  ASSERT_OK_AND_ASSIGN(ds_primitive_get,
                       ds.GetAttrWithDefault("a", test::DataItem("abc")));
  EXPECT_THAT(ds_primitive_get.slice(),
              ElementsAre(1, arolla::Text("abc"), std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kObject);

  // Mixed, no schema.
  ASSERT_OK(ds_any.SetAttr("mixed", ds_primitive_get));
  ASSERT_OK_AND_ASSIGN(ds_primitive_get,
                       ds.GetAttrWithDefault("mixed", test::DataItem("abc")));
  EXPECT_THAT(ds_primitive_get.slice(),
              ElementsAre(1, arolla::Text("abc"), std::nullopt));
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kAny);
}

TEST(DataSliceTest, DelAttr_EntityCreator) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      EntityCreator::FromAttrs(db, {"a", "b"}, {ds_primitive, ds_primitive}));
  ASSERT_OK(ds.DelAttr("a"));

  // Empty result.
  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get, ds.GetAttr("a"));
  EXPECT_THAT(ds_primitive_get.GetShape(), IsEquivalentTo(ds.GetShape()));
  EXPECT_EQ(ds_primitive_get.dtype(), GetQType<int>());
  EXPECT_EQ(ds_primitive_get.slice().allocation_ids().size(), 0);
  EXPECT_EQ(ds_primitive_get.slice().present_count(), 0);
  // Deleting an attribute does not touch schema.
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kInt32);

  // Deleting a schema attribute, updates schema.
  ASSERT_OK(ds.GetSchema().GetAttr("b").status());
  ASSERT_OK(ds.GetSchema().DelAttr("b"));
  EXPECT_THAT(ds.GetAttr("b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));
  // Missing attributes cannot be deleted on Entities.
  EXPECT_THAT(ds.DelAttr("c"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'c' is missing")));
  EXPECT_THAT(ds.GetSchema().DelAttr("c"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'c' is missing")));
}

TEST(DataSliceTest, DelAttr_Slice_ObjectCreator) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  // Removes it from Schema as well.
  ASSERT_OK(ds.GetAttr("a").status());
  ASSERT_OK(ds.DelAttr("a"));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  // Missing attributes cannot be deleted on Entities.
  EXPECT_THAT(ds.DelAttr("b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));
}

TEST(DataSliceTest, DelAttr_Item_ObjectCreator) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataItem(42);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       ObjectCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  ASSERT_EQ(ds.GetShape().rank(), 0);
  // Removes it from Schema as well.
  ASSERT_OK(ds.DelAttr("a"));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  // Missing attribute deletion.
  EXPECT_THAT(ds.DelAttr("b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));
}

TEST(DataSliceTest, DelAttr_Object_ExplicitSchema) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds, ObjectCreator::FromAttrs(db, {}, {}));
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kInt32)));
  ASSERT_OK(ds.SetAttr(schema::kSchemaAttr, explicit_schema));
  auto ds_primitive = test::DataItem(42);
  ASSERT_OK(ds.SetAttr("a", ds_primitive));
  // Does NOT remove from Schema.
  ASSERT_OK(ds.DelAttr("a"));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, ds.GetAttr("a"));
  EXPECT_EQ(ds_a_get.item(), DataItem());
  EXPECT_EQ(ds_a_get.GetSchemaImpl(), schema::kInt32);
  // Removing from __schema__ attribute, causes the missing attribute.
  ASSERT_OK(ds.GetAttr(schema::kSchemaAttr)->DelAttr("a"));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(ds.DelAttr("b"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'b' is missing")));
}

TEST(DataSliceTest, DelAttr_Any) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds,
                       EntityCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  ASSERT_OK_AND_ASSIGN(ds, ds.WithSchema(test::Schema(schema::kAny)));
  ASSERT_OK(ds.DelAttr("a"));
  ASSERT_OK_AND_ASSIGN(auto ds_primitive_get, ds.GetAttr("a"));
  EXPECT_THAT(ds_primitive_get.GetShape(), IsEquivalentTo(ds.GetShape()));
  EXPECT_EQ(ds_primitive_get.dtype(), GetQType<int>());
  EXPECT_EQ(ds_primitive_get.slice().allocation_ids().size(), 0);
  EXPECT_EQ(ds_primitive_get.slice().present_count(), 0);
  EXPECT_EQ(ds_primitive_get.GetSchemaImpl(), schema::kAny);
}

TEST(DataSliceTest, DelAttr_MixedImplicitAndExplicitSchema_InObject) {
  auto db = DataBag::Empty();
  auto schema_slice = test::DataSlice<ObjectId>(
      {internal::AllocateExplicitSchema(), GenerateImplicitSchema()},
      schema::kSchema, db);
  ASSERT_OK(schema_slice.SetAttr("a", test::Schema(schema::kInt32)));

  auto ds = test::AllocateDataSlice(2, schema::kObject, db);
  ASSERT_OK(ds.SetAttr(schema::kSchemaAttr, schema_slice));
  ASSERT_OK(ds.SetAttr("a", test::DataSlice<int>({1, 2})));

  ASSERT_OK(ds.DelAttr("a"));
  EXPECT_THAT(ds.GetAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));
  EXPECT_THAT(ds.DelAttr("a"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("the attribute 'a' is missing")));

  // Partially removing attr "a" only for IMPLICIT schemas.
  EXPECT_THAT(db->GetImpl().GetSchemaAttr(schema_slice.slice()[0], "a"),
              IsOkAndHolds(schema::kInt32));
  EXPECT_THAT(
      db->GetImpl().GetSchemaAttrAllowMissing(schema_slice.slice()[1], "a"),
      IsOkAndHolds(DataItem()));
}

TEST(DataSliceTest, DelAttr_NoDataBag) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto ds,
                       EntityCreator::FromAttrs(db, {"a"}, {ds_primitive}));
  ds = ds.WithDb(nullptr);
  EXPECT_THAT(ds.DelAttr("a"), StatusIs(absl::StatusCode::kInvalidArgument,
                                        HasSubstr("without a DataBag")));
}

TEST(DataSliceTest, DelAttr_DisallowedSchema) {
  auto db = DataBag::Empty();
  auto ds_primitive = test::DataSlice<int>({1, 2, 3}, db);
  EXPECT_THAT(
      ds_primitive.DelAttr("c"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Deleting an attribute cannot be done on a DataSlice "
                         "with INT32 schema")));

  auto ds_item_id = test::AllocateDataSlice(3, schema::kItemId, db);
  EXPECT_THAT(
      ds_item_id.DelAttr("c"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Deleting an attribute cannot be done on a DataSlice "
                         "with ITEMID schema")));
}

TEST(DataSliceTest, MixedSchemaSlice_ExplicitSchemaDTypeMatch) {
  auto implicit_schema_id = GenerateImplicitSchema();
  auto schema_a = test::MixedDataSlice<ObjectId, schema::DType>(
      {implicit_schema_id, std::nullopt}, {std::nullopt, schema::kInt32},
      schema::kAny);
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  auto mixed_implicit_explicit_schema = test::DataSlice<ObjectId>(
      {implicit_schema_id, internal::AllocateExplicitSchema()}, schema::kSchema,
      db);
  ASSERT_OK(mixed_implicit_explicit_schema.SetAttr("a", schema_a));
  ASSERT_OK(
      objects.SetAttr(schema::kSchemaAttr, mixed_implicit_explicit_schema));
  // At explicit schema place, there is already INT32, so the error shouldn't be
  // raise even though there is `implicit_schema_id` as an attribute "a" of
  // schema_a slice, but not at the explicit schema place.
  auto ds_a = test::DataSlice<int>({1, 3});
  ASSERT_OK(objects.SetAttr("a", ds_a));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, objects.GetAttr("a"));
  EXPECT_THAT(ds_a_get.WithDb(nullptr), IsEquivalentTo(ds_a));
}

TEST(DataSliceTest, MixedSchemaSlice_ExplicitSchemaObjectIdMatch) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto ds_a, EntityCreator::Shaped(db, shape, {}, {}));
  EXPECT_TRUE(ds_a.GetSchemaImpl().value<ObjectId>().IsExplicitSchema());
  auto schema_a = test::MixedDataSlice<ObjectId, schema::DType>(
      {ds_a.GetSchemaImpl().value<ObjectId>(), std::nullopt},
      {std::nullopt, schema::kInt32}, schema::kAny);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  auto implicit_schema_id = GenerateImplicitSchema();
  auto mixed_implicit_explicit_schema = test::DataSlice<ObjectId>(
      {internal::AllocateExplicitSchema(), implicit_schema_id}, schema::kSchema,
      db);
  ASSERT_OK(mixed_implicit_explicit_schema.SetAttr("a", schema_a));
  ASSERT_OK(
      objects.SetAttr(schema::kSchemaAttr, mixed_implicit_explicit_schema));
  // At explicit schema place, there is already ds_a.GetSchemaImpl(), so the
  // error shouldn't be raised even though there is INT32 as an attribute "a"
  // of schema_a slice, but not at the explicit schema place.
  ASSERT_OK(objects.SetAttr("a", ds_a));
  ASSERT_OK_AND_ASSIGN(auto ds_a_get, objects.GetAttr("a"));
  EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
}

TEST(DataSliceTest, GetFromList) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      DataSlice::Create(
          DataSliceImpl::ObjectsFromAllocation(internal::AllocateLists(2), 2),
          items.GetShape().RemoveDims(1), DataItem(schema::kAny), db));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));
  {
    // Normal GetFromList
    auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                            schema::kInt64);
    ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(42, 42, 13, DataItem(), 13));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  }
  {
    // Normal GetFromList: INT32 -> INT64 casting
    auto indices = test::DataSlice<int>({0, 0, 1, std::nullopt, 1}, shape_2,
                                        schema::kInt32);
    ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(42, 42, 13, DataItem(), 13));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  }
  {
    // Empty DataItem
    auto indices = test::DataItem(internal::DataItem(), schema::kInt32);
    ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(DataItem(), DataItem()));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  }
  {
    // Empty indices
    auto indices = test::DataSlice<int64_t>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        shape_2, schema::kInt64);
    ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(DataItem(), DataItem(), DataItem(),
                                           DataItem(), DataItem()));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));

    indices = test::DataSlice<int>(
        {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        shape_2, schema::kInt32);
    ASSERT_OK_AND_ASSIGN(items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(DataItem(), DataItem(), DataItem(),
                                           DataItem(), DataItem()));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  }
  {
    // Empty indices INT64 / INT32
    auto indices = test::EmptyDataSlice(shape, schema::kInt64);
    ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(DataItem(), DataItem(), DataItem()));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));

    indices = test::EmptyDataSlice(shape, schema::kInt32);
    ASSERT_OK_AND_ASSIGN(items, lists.GetFromList(indices));
    EXPECT_THAT(items.slice(), ElementsAre(DataItem(), DataItem(), DataItem()));
    EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  }
  {
    // Errors - only implicit casting is allowed.
    auto indices = test::EmptyDataSlice(shape, schema::kFloat64);
    EXPECT_THAT(
        lists.GetFromList(indices),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("unsupported implicit cast from FLOAT64 to INT64")));

    indices = test::EmptyDataSlice(shape, schema::kObject);
    EXPECT_THAT(
        lists.GetFromList(indices),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("unsupported implicit cast from OBJECT to INT64")));

    indices = test::EmptyDataSlice(shape, schema::kAny);
    EXPECT_THAT(
        lists.GetFromList(indices),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("unsupported implicit cast from ANY to INT64")));
  }
}

TEST(DataSliceTest, GetFromList_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kInt64)));

  // Note that we assigned int32 items here, but the result is int64 anyway.
  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
  EXPECT_THAT(items.slice(),
              ElementsAre(DataItemWith<int64_t>(42), 42, 13, DataItem(), 13));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kInt64));
}

TEST(DataSliceTest, GetFromList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kAny)));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
  EXPECT_THAT(items.slice(),
              ElementsAre(DataItemWith<int>(42), 42, 13, DataItem(), 13));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
}

TEST(DataSliceTest, GetFromList_ObjectSchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::MixedDataSlice<int, int64_t>(
      {42, std::nullopt, std::nullopt}, {std::nullopt, 12, 13}, shape,
      schema::kObject);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kObject)));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.GetFromList(indices));
  EXPECT_THAT(items.slice(),
              ElementsAre(DataItemWith<int>(42), DataItemWith<int>(42),
                          DataItemWith<int64_t>(13), DataItem(),
                          DataItemWith<int64_t>(13)));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kObject));
}

TEST(DataSliceTest, PopFromList_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kInt64)));

  // Note that we assigned int32 items here, but the result is int64 anyway.
  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.PopFromList(indices));
  EXPECT_THAT(items.slice(), ElementsAre(DataItemWith<int64_t>(42), DataItem(),
                                         13, DataItem(), DataItem()));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kInt64));
  // The only remaining element.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(12))));
}

TEST(DataSliceTest, PopFromList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::MixedDataSlice<int, int64_t>(
      {42, std::nullopt, std::nullopt}, {std::nullopt, 12, 13}, shape,
      schema::kObject);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kAny)));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.PopFromList(indices));
  EXPECT_THAT(items.slice(),
              ElementsAre(DataItemWith<int>(42), DataItem(),
                          DataItemWith<int64_t>(13), DataItem(), DataItem()));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kAny));
  // The only remaining element.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(12))));
}

TEST(DataSliceTest, PopFromList_ObjectSchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::MixedDataSlice<int, int64_t>(
      {42, std::nullopt, std::nullopt}, {std::nullopt, 12, 13}, shape,
      schema::kObject);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kObject)));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, list_items));

  auto edge_2_1 = CreateEdge({0, 2, 5});
  ASSERT_OK_AND_ASSIGN(auto shape_2,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2_1}));

  auto indices = test::DataSlice<int64_t>({0, 0, 1, std::nullopt, 1}, shape_2,
                                          schema::kInt64);
  ASSERT_OK_AND_ASSIGN(auto items, lists.PopFromList(indices));
  EXPECT_THAT(items.slice(),
              ElementsAre(DataItemWith<int>(42), DataItem(),
                          DataItemWith<int64_t>(13), DataItem(), DataItem()));
  EXPECT_THAT(items.GetSchemaImpl(), Eq(schema::kObject));
  // The only remaining element.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(12))));
}

TEST(DataSliceTest, ExplodeList_Int32Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  auto edge_3 = CreateEdge({0, 1, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      auto shape, DataSlice::JaggedShape::FromEdges({edge_1, edge_2, edge_3}));

  auto items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, items.GetShape().RemoveDims(2),
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       test::Schema(schema::kInt32)));
  ASSERT_OK(lists.AppendToList(items));
  ASSERT_OK_AND_ASSIGN(auto exploded_lists, lists.ExplodeList(0, std::nullopt));
  EXPECT_THAT(exploded_lists.slice(), ElementsAre(42, 12, 13));
  EXPECT_THAT(exploded_lists.GetSchemaImpl(), Eq(schema::kInt32));
}

TEST(DataSliceTest, ExplodeList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  auto edge_3 = CreateEdge({0, 1, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      auto shape, DataSlice::JaggedShape::FromEdges({edge_1, edge_2, edge_3}));

  auto items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      DataSlice::Create(
          DataSliceImpl::ObjectsFromAllocation(internal::AllocateLists(3), 3),
          items.GetShape().RemoveDims(2), DataItem(schema::kAny), db));
  ASSERT_OK(lists.AppendToList(items));
  ASSERT_OK_AND_ASSIGN(auto exploded_lists, lists.ExplodeList(0, std::nullopt));
  EXPECT_THAT(exploded_lists.slice(), ElementsAre(42, 12, 13));
  EXPECT_THAT(exploded_lists.GetSchemaImpl(), Eq(schema::kAny));
}

TEST(DataSliceTest, ExplodeList_ObjectSchema) {
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({CreateEdge({0, 3})}));
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto lists,
                       DataSlice::Create(DataSliceImpl::ObjectsFromAllocation(
                                             internal::AllocateLists(3), 3),
                                         shape, DataItem(schema::kObject), db));

  ASSERT_OK_AND_ASSIGN(auto list_int32_schema,
                       CreateListSchema(db, test::Schema(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(auto list_int64_schema,
                       CreateListSchema(db, test::Schema(schema::kInt64)));

  // All the item schemas are INT32.
  ASSERT_OK(
      lists.SetAttr(schema::kSchemaAttr, test::Schema(list_int32_schema)));
  EXPECT_THAT(
      lists.ExplodeList(0, std::nullopt),
      IsOkAndHolds(Property(&DataSlice::GetSchemaImpl, Eq(schema::kInt32))));

  // Item schemas have different types.
  ASSERT_OK(lists.SetAttr(
      schema::kSchemaAttr,
      test::DataSlice<ObjectId>({list_int32_schema.value<ObjectId>(),
                                 list_int64_schema.value<ObjectId>(),
                                 list_int32_schema.value<ObjectId>()},
                                schema::kSchema, db)));
  EXPECT_THAT(
      lists.ExplodeList(0, std::nullopt),
      IsOkAndHolds(Property(&DataSlice::GetSchemaImpl, Eq(schema::kInt64))));
}

TEST(DataSliceTest, ReplaceInList_NoDb) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kInt32)));
  auto values = test::DataSlice<int>({57, 7, -2}, shape, schema::kInt32);
  lists = lists.WithDb(nullptr);
  EXPECT_THAT(
      lists.ReplaceInList(0, std::nullopt, values),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot set items of a list without a DataBag")));
}

TEST(DataSliceTest, ReplaceInList) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  auto edge_3 = CreateEdge({0, 1, 2, 3});
  ASSERT_OK_AND_ASSIGN(
      auto shape, DataSlice::JaggedShape::FromEdges({edge_1, edge_2, edge_3}));

  auto items = test::DataSlice<int>({42, 12, 13}, shape, schema::kInt32);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      DataSlice::Create(
          DataSliceImpl::ObjectsFromAllocation(internal::AllocateLists(3), 3),
          items.GetShape().RemoveDims(2), DataItem(schema::kAny), db));

  ASSERT_OK(lists.ReplaceInList(0, std::nullopt, items));
  ASSERT_OK_AND_ASSIGN(auto res_items, lists.ExplodeList(0, std::nullopt));
  EXPECT_THAT(res_items.slice(), ElementsAre(42, 12, 13));

  auto edge_2_alternative = CreateEdge({0, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto wrong_shape1,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  ASSERT_OK_AND_ASSIGN(
      auto wrong_shape2,
      DataSlice::JaggedShape::FromEdges({edge_1, edge_2_alternative, edge_3}));
  auto incompatible_items1 =
      test::DataSlice<int>({42, 12, 13}, wrong_shape1, schema::kInt32);
  auto incompatible_items2 =
      test::DataSlice<int>({42, 12, 13}, wrong_shape2, schema::kInt32);

  EXPECT_THAT(
      lists.ReplaceInList(0, std::nullopt, incompatible_items1),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("3 dimensions are required")));
  EXPECT_THAT(lists.ReplaceInList(0, std::nullopt, incompatible_items2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot be expanded to")));
}

TEST(DataSliceTest, ReplaceInList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));
  auto initial_values =
      test::DataSlice<int>({0, 0, 0}, shape, schema::kInt32);
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt)->GetSchemaImpl(),
              Eq(schema::kAny));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(0, 0, 0, 0, 0, 0, 0, 0, 0))));

  ASSERT_OK_AND_ASSIGN(auto subshape,
                       DataSlice::JaggedShape::FromEdges(
                           {edge_1, edge_2, CreateEdge({0, 1, 2, 3})}));
  ASSERT_OK(
      lists.ReplaceInList(1, 2,
                          test::DataSlice<arolla::Text>(
                              {"foo", "bar", "baz"}, subshape, schema::kText)));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(
                  Property(&DataSlice::slice,
                           // Mixing types is allowed.
                           ElementsAre(DataItemWith<int>(0),
                                       DataItemWith<arolla::Text>("foo"), 0, 0,
                                       DataItemWith<arolla::Text>("bar"), 0, 0,
                                       DataItemWith<arolla::Text>("baz"), 0))));
}

TEST(DataSliceTest, ReplaceInList_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kInt64)));
  auto initial_values =
      test::DataSlice<int64_t>({0, 0, 0}, shape, schema::kInt64);
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(0, 0, 0, 0, 0, 0, 0, 0, 0))));

  ASSERT_OK_AND_ASSIGN(auto subshape,
                       DataSlice::JaggedShape::FromEdges(
                           {edge_1, edge_2, CreateEdge({0, 1, 2, 3})}));
  ASSERT_OK(lists.ReplaceInList(
      1, 2, test::DataSlice<int>({1, 2, 3}, subshape, schema::kInt32)));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    // All the values are casted to int64.
                                    ElementsAre(DataItemWith<int64_t>(0),
                                                DataItemWith<int64_t>(1), 0, 0,
                                                DataItemWith<int64_t>(2), 0, 0,
                                                DataItemWith<int64_t>(3), 0))));
  absl::Status status = lists.ReplaceInList(
      1, 2, test::DataSlice<float>({5, 6, 7}, subshape, schema::kFloat32));
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("the schema for List Items is incompatible.")));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error);
  EXPECT_TRUE(error->has_incompatible_schema());

  // Lists are not modified.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(DataItemWith<int64_t>(0),
                                                DataItemWith<int64_t>(1), 0, 0,
                                                DataItemWith<int64_t>(2), 0, 0,
                                                DataItemWith<int64_t>(3), 0))));
}

TEST(DataSliceTest, SetInList_NoDb) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kInt32)));
  auto ids = test::DataSlice<int>({0, 2, 57}, shape, schema::kInt32);
  auto values = test::DataSlice<int>({57, 7, -2}, shape, schema::kInt32);
  lists = lists.WithDb(nullptr);
  EXPECT_THAT(lists.SetInList(ids, values),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("cannot set list items without a DataBag")));
}

TEST(DataSliceTest, SetInList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));

  auto initial_values =
      test::DataSlice<int64_t>({0, 0, 0}, shape, schema::kInt64);
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(0, 0, 0, 0, 0, 0, 0, 0, 0))));

  auto ids = test::DataSlice<int>({0, 2, 57}, shape, schema::kInt32);
  auto values = test::DataSlice<int>({57, 7, -2}, shape, schema::kInt32);

  ASSERT_OK(lists.SetInList(ids, values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(
                                        // int32 values remain as int32.
                                        DataItemWith<int>(57),
                                        // And int64 values remain as int64.
                                        DataItemWith<int64_t>(0), 0, 0, 0,
                                        DataItemWith<int>(7), 0, 0, 0))));

  auto text_values = test::DataSlice<arolla::Text>({"foo", "bar", "baz"}, shape,
                                                   schema::kText);
  ASSERT_OK(lists.SetInList(ids, text_values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(
                  &DataSlice::slice,
                  ElementsAre(DataItemWith<arolla::Text>("foo"), 0, 0, 0, 0,
                              DataItemWith<arolla::Text>("bar"), 0, 0, 0))));
}

TEST(DataSliceTest, SetInList_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kInt64)));

  auto initial_values =
      test::DataSlice<int64_t>({0, 0, 0}, shape, schema::kInt64);
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  ASSERT_OK(lists.AppendToList(initial_values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(0, 0, 0, 0, 0, 0, 0, 0, 0))));

  auto ids = test::DataSlice<int>({0, 2, 57}, shape, schema::kInt32);
  auto values = test::DataSlice<int>({57, 7, -2}, shape, schema::kInt32);

  ASSERT_OK(lists.SetInList(ids, values));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(
                                        // Int32 values get casted to Int64.
                                        DataItemWith<int64_t>(57), 0, 0, 0, 0,
                                        DataItemWith<int64_t>(7), 0, 0, 0))));

  // Float32 values are not casted to Int64.
  auto float_values =
      test::DataSlice<float>({42., 43., 44.}, shape, schema::kFloat32);
  absl::Status status = lists.SetInList(ids, float_values);
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("the schema for List Items is incompatible.")));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error);
  EXPECT_TRUE(error->has_incompatible_schema());

  // The lists were not modified.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(57, 0, 0, 0, 0, 7, 0, 0, 0))));
}

TEST(DataSliceTest, AppendToList_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kAny)));

  ASSERT_OK(lists.AppendToList(
      test::DataSlice<int64_t>({1, 2, 3}, shape, schema::kInt64)));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(DataItemWith<int64_t>(1),
                                                DataItemWith<int64_t>(2),
                                                DataItemWith<int64_t>(3)))));

  // Mixing types is allowed.
  ASSERT_OK(lists.AppendToList(
      test::DataSlice<float>({5, 6, 7}, shape, schema::kFloat32)));
  EXPECT_THAT(
      lists.ExplodeList(0, std::nullopt),
      IsOkAndHolds(Property(
          &DataSlice::slice,
          ElementsAre(DataItemWith<int64_t>(1), DataItemWith<float>(5),
                      DataItemWith<int64_t>(2), DataItemWith<float>(6.),
                      DataItemWith<int64_t>(3), DataItemWith<float>(7.)))));
}

TEST(DataSliceTest, AppendToList_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, shape, /*values=*/std::nullopt,
                       /*schema=*/std::nullopt, test::Schema(schema::kInt64)));

  ASSERT_OK(lists.AppendToList(
      test::DataSlice<int>({1, 2, 3}, shape, schema::kInt32)));
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    // Items are casted to int64.
                                    ElementsAre(DataItemWith<int64_t>(1),
                                                DataItemWith<int64_t>(2),
                                                DataItemWith<int64_t>(3)))));

  absl::Status status = lists.AppendToList(
      test::DataSlice<float>({5, 6, 7}, shape, schema::kFloat32));
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("the schema for List Items is incompatible.")));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error);
  EXPECT_TRUE(error->has_incompatible_schema());

  // Lists are not modified.
  EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
              IsOkAndHolds(Property(&DataSlice::slice,
                                    ElementsAre(DataItemWith<int64_t>(1),
                                                DataItemWith<int64_t>(2),
                                                DataItemWith<int64_t>(3)))));
}

TEST(DataSliceTest, AppendToList_DifferentShapes) {
  auto db = DataBag::Empty();

  auto values123 = test::DataSlice<int>({1, 2, 3}, schema::kInt32);

  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto values_shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto values6_78 =
      test::DataSlice<int>({6, 7, 8}, values_shape, schema::kInt32);

  // single list
  {
    ASSERT_OK_AND_ASSIGN(auto list,
                         CreateEmptyList(db, /*schema=*/std::nullopt,
                                         test::Schema(schema::kInt32)));
    auto values123 = test::DataSlice<int>({1, 2, 3}, schema::kInt32);
    ASSERT_OK(list.AppendToList(values123));
    ASSERT_OK(list.AppendToList(values6_78));
    EXPECT_THAT(list.ExplodeList(0, std::nullopt),
                IsOkAndHolds(Property(&DataSlice::slice,
                                      ElementsAre(1, 2, 3, 6, 7, 8))));
  }

  // slice of lists
  {
    auto lists_shape = DataSlice::JaggedShape::FlatFromSize(2);
    ASSERT_OK_AND_ASSIGN(
        auto lists, CreateListShaped(db, lists_shape, /*values=*/std::nullopt,
                                     /*schema=*/std::nullopt,
                                     test::Schema(schema::kInt32)));
    ASSERT_OK(lists.AppendToList(*DataSlice::Create(
        internal::DataItem(1), internal::DataItem(schema::kInt32))));
    ASSERT_OK(lists.AppendToList(values6_78));

    auto expected_edge_2 = CreateEdge({0, 2, 5});
    ASSERT_OK_AND_ASSIGN(auto expected_shape, DataSlice::JaggedShape::FromEdges(
                                                  {edge_1, expected_edge_2}));
    EXPECT_THAT(
        lists.ExplodeList(0, std::nullopt),
        IsOkAndHolds(AllOf(
            Property(&DataSlice::slice, ElementsAre(1, 6, 1, 7, 8)),
            Property(&DataSlice::GetShape, IsEquivalentTo(expected_shape)))));
  }
}

TEST(DataSliceTest, RemoveInList) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));

  auto list_items = test::DataSlice<int>({42, std::nullopt, 15}, shape);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto lists,
      CreateListShaped(db, list_items.GetShape().RemoveDims(1),
                       /*values=*/list_items, /*schema=*/std::nullopt,
                       test::Schema(schema::kInt32)));

  {
    // Entity List - indices API.
    ASSERT_OK_AND_ASSIGN(auto forked_db, db->Fork());
    lists = lists.WithDb(forked_db);
    ASSERT_OK(lists.RemoveInList(test::DataItem(0)));
    EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
                IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(15))));

    EXPECT_THAT(EntityCreator::Shaped(forked_db, lists.GetShape(), {}, {})
                ->RemoveInList(test::DataItem(0)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("attribute '__items__' is missing")));
  }
  {
    // Entity List - slice / range API.
    ASSERT_OK_AND_ASSIGN(auto forked_db, db->Fork());
    lists = lists.WithDb(forked_db);
    ASSERT_OK(lists.RemoveInList(0, 1));
    EXPECT_THAT(lists.ExplodeList(0, std::nullopt),
                IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(15))));

    EXPECT_THAT(EntityCreator::Shaped(forked_db, lists.GetShape(), {}, {})
                ->RemoveInList(0, 1),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("attribute '__items__' is missing")));
  }
  {
    // Object List - indices API.
    ASSERT_OK_AND_ASSIGN(auto forked_db, db->Fork());
    auto obj_lists = lists.WithDb(forked_db);
    ASSERT_OK_AND_ASSIGN(obj_lists, obj_lists.EmbedSchema());
    ASSERT_OK(obj_lists.RemoveInList(test::DataItem(0)));
    EXPECT_THAT(obj_lists.ExplodeList(0, std::nullopt),
                IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(15))));

    obj_lists = test::DataSlice<ObjectId>(
        {lists.slice()[0].value<ObjectId>(), internal::AllocateSingleList()},
        schema::kObject, forked_db);
    EXPECT_THAT(obj_lists.RemoveInList(test::DataItem(0)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("missing __schema__ attribute")));
  }
  {
    // Object List - slice / range API.
    ASSERT_OK_AND_ASSIGN(auto forked_db, db->Fork());
    auto obj_lists = lists.WithDb(forked_db);
    ASSERT_OK_AND_ASSIGN(obj_lists, obj_lists.EmbedSchema());
    ASSERT_OK(obj_lists.RemoveInList(0, 1));
    EXPECT_THAT(obj_lists.ExplodeList(0, std::nullopt),
                IsOkAndHolds(Property(&DataSlice::slice, ElementsAre(15))));

    obj_lists = test::DataSlice<ObjectId>(
        {lists.slice()[0].value<ObjectId>(), internal::AllocateSingleList()},
        schema::kObject, forked_db);
    EXPECT_THAT(obj_lists.RemoveInList(0, 1),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("missing __schema__ attribute")));
  }
}

TEST(DataSliceTest, DictErrors) {
  auto dict = test::DataItem(internal::AllocateSingleObject());
  EXPECT_THAT(dict.GetDictKeys(), StatusIs(absl::StatusCode::kInvalidArgument,
                                           HasSubstr("without a DataBag")));
  EXPECT_THAT(dict.GetFromDict(dict),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("without a DataBag")));
  EXPECT_THAT(dict.SetInDict(dict, dict),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("without a DataBag")));
}

TEST(DataSliceTest, SetInDict_GetFromDict_AnySchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  ASSERT_OK_AND_ASSIGN(auto edge_3,
                       DataSlice::JaggedShape::Edge::FromUniformGroups(3, 1));
  ASSERT_OK_AND_ASSIGN(auto keys_shape, shape.AddDims({edge_3}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto dicts,
      CreateDictShaped(db, shape, /*keys=*/std::nullopt,
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       /*key_schema=*/test::Schema(schema::kAny),
                       /*value_schema=*/test::Schema(schema::kAny)));

  ASSERT_OK(dicts.SetInDict(
      test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
      test::MixedDataSlice<int, arolla::Bytes>(
          {4, 5, std::nullopt}, {std::nullopt, std::nullopt, "six"}, keys_shape,
          schema::kObject)));

  ASSERT_OK_AND_ASSIGN(auto immutable_dicts, dicts.Freeze());

  ASSERT_OK_AND_ASSIGN(auto keys, immutable_dicts.GetDictKeys());
  EXPECT_THAT(keys.slice(), ElementsAre(DataItemWith<int>(1), 2, 3));
  EXPECT_THAT(keys.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(keys.GetSchemaImpl(), Eq(schema::kAny));

  ASSERT_OK_AND_ASSIGN(auto values, immutable_dicts.GetFromDict(keys));
  EXPECT_THAT(values.slice(),
              ElementsAre(DataItemWith<int>(4), DataItemWith<int>(5),
                          DataItemWith<arolla::Bytes>("six")));
  EXPECT_THAT(values.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(values.GetSchemaImpl(), Eq(schema::kAny));

  ASSERT_OK_AND_ASSIGN(auto typed_keys,
                       keys.WithSchema(test::Schema(schema::kInt32)));
  EXPECT_THAT(immutable_dicts.GetFromDict(typed_keys),
              IsOkAndHolds(IsEquivalentTo(values)));

  ASSERT_OK_AND_ASSIGN(auto object_type_keys,
                       keys.WithSchema(test::Schema(schema::kObject)));
  EXPECT_THAT(immutable_dicts.GetFromDict(object_type_keys),
              IsOkAndHolds(IsEquivalentTo(values)));
}

TEST(DataSliceTest, SetInDict_GetFromDict_DataItem_ObjectSchema) {
  ASSERT_OK_AND_ASSIGN(auto shape, DataSlice::JaggedShape::FromEdges({}));
  ASSERT_OK_AND_ASSIGN(auto keys_shape, shape.AddDims({CreateEdge({0, 3})}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto dicts,
      CreateDictShaped(db, shape, /*keys=*/std::nullopt,
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       /*key_schema=*/std::nullopt,
                       /*value_schema=*/std::nullopt));

  ASSERT_OK(dicts.SetInDict(
      test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
      test::MixedDataSlice<int, arolla::Bytes>(
          {4, 5, std::nullopt}, {std::nullopt, std::nullopt, "six"}, keys_shape,
          schema::kObject)));

  ASSERT_OK_AND_ASSIGN(auto immutable_dicts, dicts.Freeze());

  ASSERT_OK_AND_ASSIGN(auto keys, immutable_dicts.GetDictKeys());
  EXPECT_THAT(keys.slice(),
              UnorderedElementsAre(DataItemWith<int>(1), 2, 3));
  EXPECT_THAT(keys.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(keys.GetSchemaImpl(), Eq(schema::kObject));

  ASSERT_OK_AND_ASSIGN(auto values, immutable_dicts.GetFromDict(keys));
  EXPECT_THAT(
      values.slice(),
      UnorderedElementsAre(DataItemWith<int>(4), DataItemWith<int>(5),
                           DataItemWith<arolla::Bytes>("six")));
  EXPECT_THAT(values.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(values.GetSchemaImpl(), Eq(schema::kObject));

  ASSERT_OK_AND_ASSIGN(auto typed_keys,
                       keys.WithSchema(test::Schema(schema::kInt32)));
  EXPECT_THAT(immutable_dicts.GetFromDict(typed_keys),
              IsOkAndHolds(IsEquivalentTo(values)));

  ASSERT_OK_AND_ASSIGN(auto any_type_keys,
                       keys.WithSchema(test::Schema(schema::kAny)));
  absl::Status status = immutable_dicts.GetFromDict(any_type_keys).status();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               "the schema for Dict Keys is incompatible.\n\n"
                               "Expected schema for 'Keys': OBJECT\n"
                               "Assigned schema for 'Keys': ANY"));
  std::optional<internal::Error> error = internal::GetErrorPayload(status);
  ASSERT_TRUE(error);
  EXPECT_TRUE(error->has_incompatible_schema());

  EXPECT_THAT(
      immutable_dicts.SetInDict(
          test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
          test::DataSlice<int>({4, 5, 6}, keys_shape, schema::kObject)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataBag is immutable")));
}

TEST(DataSliceTest, SetInDict_GetFromDict_ObjectSchema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  ASSERT_OK_AND_ASSIGN(auto edge_3,
                       DataSlice::JaggedShape::Edge::FromUniformGroups(3, 1));
  ASSERT_OK_AND_ASSIGN(auto keys_shape, shape.AddDims({edge_3}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto dicts,
      CreateDictShaped(db, shape, /*keys=*/std::nullopt,
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       /*key_schema=*/std::nullopt,
                       /*value_schema=*/std::nullopt));

  ASSERT_OK(dicts.SetInDict(
      test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
      test::MixedDataSlice<int, arolla::Bytes>(
          {4, 5, std::nullopt}, {std::nullopt, std::nullopt, "six"}, keys_shape,
          schema::kObject)));

  ASSERT_OK_AND_ASSIGN(auto immutable_dicts, dicts.Freeze());

  ASSERT_OK_AND_ASSIGN(auto keys, immutable_dicts.GetDictKeys());
  EXPECT_THAT(keys.slice(), ElementsAre(DataItemWith<int>(1), 2, 3));
  EXPECT_THAT(keys.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(keys.GetSchemaImpl(), Eq(schema::kObject));

  ASSERT_OK_AND_ASSIGN(auto values, immutable_dicts.GetFromDict(keys));
  EXPECT_THAT(values.slice(),
              ElementsAre(DataItemWith<int>(4), DataItemWith<int>(5),
                          DataItemWith<arolla::Bytes>("six")));
  EXPECT_THAT(values.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(values.GetSchemaImpl(), Eq(schema::kObject));

  ASSERT_OK_AND_ASSIGN(auto typed_keys,
                       keys.WithSchema(test::Schema(schema::kInt32)));
  EXPECT_THAT(immutable_dicts.GetFromDict(typed_keys),
              IsOkAndHolds(IsEquivalentTo(values)));

  ASSERT_OK_AND_ASSIGN(auto any_type_keys,
                       keys.WithSchema(test::Schema(schema::kAny)));
  EXPECT_THAT(immutable_dicts.GetFromDict(any_type_keys),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for Dict Keys is incompatible.\n\n"
                       "Expected schema for 'Keys': OBJECT\n"
                       "Assigned schema for 'Keys': ANY"));

  EXPECT_THAT(
      immutable_dicts.SetInDict(
          test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
          test::DataSlice<int>({4, 5, 6}, keys_shape, schema::kObject)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataBag is immutable")));
}

TEST(DataSliceTest, SetInDict_GetFromDict_Int64Schema) {
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  ASSERT_OK_AND_ASSIGN(auto edge_3,
                       DataSlice::JaggedShape::Edge::FromUniformGroups(3, 1));
  ASSERT_OK_AND_ASSIGN(auto keys_shape, shape.AddDims({edge_3}));
  auto db = DataBag::Empty();

  ASSERT_OK_AND_ASSIGN(
      auto dicts,
      CreateDictShaped(db, shape, /*keys=*/std::nullopt,
                       /*values=*/std::nullopt, /*schema=*/std::nullopt,
                       /*key_schema=*/test::Schema(schema::kInt64),
                       /*value_schema=*/test::Schema(schema::kInt64)));

  EXPECT_THAT(dicts.SetInDict(
                  test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
                  test::MixedDataSlice<int, arolla::Bytes>(
                      {4, 5, std::nullopt}, {std::nullopt, std::nullopt, "six"},
                      keys_shape, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       // TODO: The error message seems to be
                       // misleading here: the same assignment below passes,
                       // despite having OBJECT schema as well.
                       "the schema for Dict Values is incompatible.\n\n"
                       "Expected schema for 'Values': INT64\n"
                       "Assigned schema for 'Values': OBJECT"));
  EXPECT_THAT(
      dicts.SetInDict(
          test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
          test::DataSlice<float>({4, 5, 6}, keys_shape, schema::kFloat32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "the schema for Dict Values is incompatible.\n\n"
               "Expected schema for 'Values': INT64\n"
               "Assigned schema for 'Values': FLOAT32"));

  EXPECT_THAT(dicts.SetInDict(
                  test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
                  test::DataSlice<int>({4, 5, 6}, keys_shape, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for Dict Values is incompatible.\n\n"
                       "Expected schema for 'Values': INT64\n"
                       "Assigned schema for 'Values': OBJECT"));

  ASSERT_OK(dicts.SetInDict(
      test::DataSlice<int>({1, 2, 3}, keys_shape, schema::kInt32),
      test::DataSlice<int>({4, 5, 6}, keys_shape, schema::kInt32)));

  ASSERT_OK_AND_ASSIGN(auto immutable_dicts, dicts.Freeze());

  ASSERT_OK_AND_ASSIGN(auto keys, immutable_dicts.GetDictKeys());
  EXPECT_THAT(keys.slice(),
              // Keys are casted to int64.
              ElementsAre(DataItemWith<int64_t>(1), DataItemWith<int64_t>(2),
                          DataItemWith<int64_t>(3)));
  EXPECT_THAT(keys.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(keys.GetSchemaImpl(), Eq(schema::kInt64));

  ASSERT_OK_AND_ASSIGN(auto values, immutable_dicts.GetFromDict(keys));
  EXPECT_THAT(values.slice(),
              // Values are casted to int64.
              ElementsAre(DataItemWith<int64_t>(4), DataItemWith<int64_t>(5),
                          DataItemWith<int64_t>(6)));
  EXPECT_THAT(values.GetShape(), IsEquivalentTo(keys_shape));
  EXPECT_THAT(values.GetSchemaImpl(), Eq(schema::kInt64));

  ASSERT_OK_AND_ASSIGN(auto object_type_keys,
                       keys.WithSchema(test::Schema(schema::kObject)));
  EXPECT_THAT(immutable_dicts.GetFromDict(object_type_keys),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for Dict Keys is incompatible.\n\n"
                       "Expected schema for 'Keys': INT64\n"
                       "Assigned schema for 'Keys': OBJECT"));

  ASSERT_OK_AND_ASSIGN(auto any_type_keys,
                       keys.WithSchema(test::Schema(schema::kAny)));
  EXPECT_THAT(immutable_dicts.GetFromDict(any_type_keys),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for Dict Keys is incompatible.\n\n"
                       "Expected schema for 'Keys': INT64\n"
                       "Assigned schema for 'Keys': ANY"));
}

TEST(DataSliceTest, ShouldApplyListOp_DataItem) {
  auto db = DataBag::Empty();
  auto list_items = test::DataSlice<int>({1, 2, 3});
  ASSERT_OK_AND_ASSIGN(auto list, CreateListsFromLastDimension(db, list_items));
  EXPECT_TRUE(list.ShouldApplyListOp());
  EXPECT_TRUE(test::DataItem(internal::DataItem(), list.GetSchemaImpl(), db)
              .ShouldApplyListOp());
  ASSERT_OK_AND_ASSIGN(auto list_any,
                       list.WithSchema(test::Schema(schema::kAny)));
  EXPECT_TRUE(list_any.ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(auto list_embedded, list.EmbedSchema());
  EXPECT_TRUE(list_embedded.ShouldApplyListOp());

  EXPECT_FALSE(test::DataItem(internal::DataItem(), schema::kObject, db)
               .ShouldApplyListOp());
  EXPECT_FALSE(test::DataItem(internal::DataItem(), schema::kAny, db)
               .ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::FromAttrs(db, {}, {}));
  EXPECT_FALSE(entity.ShouldApplyListOp());
  EXPECT_FALSE(test::DataItem(internal::DataItem(), entity.GetSchemaImpl(), db)
               .ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(
      auto dict,
      CreateDictShaped(db, DataSlice::JaggedShape::Empty(),
                       /*keys=*/std::nullopt, /*values=*/std::nullopt));
  EXPECT_FALSE(dict.ShouldApplyListOp());
  EXPECT_FALSE(test::DataItem(internal::DataItem(), dict.GetSchemaImpl(), db)
               .ShouldApplyListOp());
}

TEST(DataSliceTest, ShouldApplyListOp_DataSlice) {
  auto db = DataBag::Empty();
  auto edge_1 = CreateEdge({0, 2});
  auto edge_2 = CreateEdge({0, 1, 3});
  ASSERT_OK_AND_ASSIGN(auto shape,
                       DataSlice::JaggedShape::FromEdges({edge_1, edge_2}));
  auto list_items = test::DataSlice<int>({1, 2, 3}, shape);
  ASSERT_OK_AND_ASSIGN(auto lists,
                       CreateListsFromLastDimension(db, list_items));
  EXPECT_TRUE(lists.ShouldApplyListOp());
  EXPECT_TRUE(test::EmptyDataSlice(lists.GetShape(), lists.GetSchemaImpl(), db)
              .ShouldApplyListOp());
  ASSERT_OK_AND_ASSIGN(auto lists_any,
                       lists.WithSchema(test::Schema(schema::kAny)));
  EXPECT_TRUE(lists_any.ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(auto lists_embedded, lists.EmbedSchema());
  EXPECT_TRUE(lists_embedded.ShouldApplyListOp());

  EXPECT_FALSE(test::EmptyDataSlice(3, schema::kObject, db)
               .ShouldApplyListOp());
  EXPECT_FALSE(test::EmptyDataSlice(3, schema::kAny, db).ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(
      auto entities,
      EntityCreator::Shaped(
          db, DataSlice::JaggedShape::FlatFromSize(3), {}, {}));
  EXPECT_FALSE(entities.ShouldApplyListOp());
  EXPECT_FALSE(
      test::EmptyDataSlice(entities.GetShape(), entities.GetSchemaImpl(), db)
      .ShouldApplyListOp());

  ASSERT_OK_AND_ASSIGN(
      auto dicts,
      CreateDictShaped(db, DataSlice::JaggedShape::FlatFromSize(3),
                       /*keys=*/std::nullopt, /*values=*/std::nullopt));
  EXPECT_FALSE(dicts.ShouldApplyListOp());
  EXPECT_FALSE(test::EmptyDataSlice(dicts.GetShape(), dicts.GetSchemaImpl(), db)
               .ShouldApplyListOp());
}

TEST(DataSliceTest, SchemaSlice) {
  auto x = test::DataItem(42);
  auto a = test::DataItem(3.14);
  auto b = test::DataItem(2.71);

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto y,
      EntityCreator::FromAttrs(
          db, {std::string("a"), std::string("b")}, {a, b}));
  ASSERT_OK_AND_ASSIGN(
      auto o,
      EntityCreator::FromAttrs(
          db, {std::string("x"), std::string("y")}, {x, y}));

  auto schema = o.GetSchema();
  ASSERT_OK_AND_ASSIGN(auto x_schema, schema.GetAttr("x"));
  EXPECT_EQ(x_schema.item(), schema::kInt32);
  ASSERT_OK_AND_ASSIGN(auto y_schema, schema.GetAttr("y"));
  EXPECT_EQ(y_schema.item(), y.GetSchemaImpl());
  ASSERT_OK_AND_ASSIGN(auto a_schema, y_schema.GetAttr("a"));
  EXPECT_EQ(a_schema.item(), schema::kFloat64);
  ASSERT_OK_AND_ASSIGN(auto b_schema, y_schema.GetAttr("b"));
  EXPECT_EQ(b_schema.item(), schema::kFloat64);

  // Getting and Setting attributes on Schema constants is not allowed.
  EXPECT_THAT(x_schema.GetAttr("not_allowed"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("cannot get or set attributes on schema "
                                 "constants: INT32")));
  EXPECT_THAT(x_schema.SetAttr("not_allowed", y_schema),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("cannot get or set attributes on schema "
                                 "constants: INT32")));

  auto any_schema = test::DataItem(schema::kAny, schema::kSchema, db);
  EXPECT_THAT(any_schema.GetAttr("not_allowed"),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("cannot get or set attributes on schema "
                                 "constants: ANY")));
  EXPECT_THAT(any_schema.SetAttr("not_allowed", schema),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("cannot get or set attributes on schema "
                                 "constants: ANY")));

  // Setting a non-schema as a schema attribute.
  EXPECT_THAT(y_schema.SetAttr("non_schema", x),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("only schemas can be assigned as attributes of"
                                 " schemas, got: 42")));

  // Successful setting and getting a schema attribute.
  ASSERT_OK(schema.SetAttr("new_attr", a.GetSchema()));
  ASSERT_OK_AND_ASSIGN(auto new_attr_schema, schema.GetAttr("new_attr"));
  EXPECT_EQ(new_attr_schema.item(), schema::kFloat64);
}

TEST(DataSliceTest, Reshape) {
  {
    // DataSliceImpl -> DataSliceImpl.
    auto ds = test::DataSlice<int>({1});
    auto edge = CreateEdge({0, 1});
    ASSERT_OK_AND_ASSIGN(auto new_shape,
                         DataSlice::JaggedShape::FromEdges({edge, edge, edge}));
    ASSERT_OK_AND_ASSIGN(auto new_ds, ds.Reshape(new_shape));
    EXPECT_THAT(new_ds.slice(), ElementsAre(1));
    EXPECT_THAT(new_ds.GetShape(), IsEquivalentTo(new_shape));
  }
  {
    // DataSliceImpl -> DataItem.
    auto ds = test::DataSlice<int>({1});
    auto new_shape = DataSlice::JaggedShape::Empty();
    ASSERT_OK_AND_ASSIGN(auto new_ds, ds.Reshape(new_shape));
    EXPECT_EQ(new_ds.item(), 1);
    EXPECT_THAT(new_ds.GetShape(), IsEquivalentTo(new_shape));
  }
  {
    // DataItem -> DataSliceImpl.
    auto ds = test::DataSlice<int>({1});
    auto new_shape = DataSlice::JaggedShape::FlatFromSize(1);
    ASSERT_OK_AND_ASSIGN(auto new_ds, ds.Reshape(new_shape));
    EXPECT_THAT(new_ds.slice(), ElementsAre(1));
    EXPECT_THAT(new_ds.GetShape(), IsEquivalentTo(new_shape));
  }
  {
    // DataSliceImpl -> DataSliceImpl incompatible shape.
    auto ds = test::DataSlice<int>({1});
    auto new_shape = DataSlice::JaggedShape::FlatFromSize(3);
    EXPECT_THAT(
        ds.Reshape(new_shape),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=3 != items_size=1")));
  }
  {
    // DataSliceImpl -> DataItem incompatible shape.
    auto ds = test::DataSlice<int>({1, 2, 3});
    auto new_shape = DataSlice::JaggedShape::Empty();
    EXPECT_THAT(
        ds.Reshape(new_shape),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=1 != items_size=3")));
  }
  {
    // DataItem -> DataSliceImpl incompatible shape.
    auto ds = test::DataItem(1);
    auto new_shape = DataSlice::JaggedShape::FlatFromSize(3);
    EXPECT_THAT(
        ds.Reshape(new_shape),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("shape size must be compatible with number of "
                           "items: shape_size=3 != items_size=1")));
  }
}

TEST(DataSliceTest, PresentCount) {
  auto db = DataBag::Empty();
  {
    auto ds = test::DataSlice<int>({1, std::nullopt, 3});
    EXPECT_THAT(ds.present_count(), Eq(2));
  }
  {
    auto ds = test::DataItem(1);
    EXPECT_THAT(ds.present_count(), Eq(1));
  }
  {
    auto ds = test::DataItem(internal::DataItem());
    EXPECT_THAT(ds.present_count(), Eq(0));
  }
}

TEST(DataSliceTest, Repr) {
  // NOTE: More extensive repr tests are done in Python.
  auto db = DataBag::Empty();
  {
    // DataSlice repr without db.
    auto ds = test::DataSlice<int>({1});
    EXPECT_THAT(arolla::GenReprToken(ds),
                ReprTokenEq("DataSlice([1], schema: INT32, "
                            "shape: JaggedShape(1))"));
  }
  {
    // DataSlice repr with db.
    auto ds = test::DataSlice<int>({1}, schema::kInt32, db);
    std::string expected_repr = absl::StrFormat(
        "DataSlice([1], schema: INT32, shape: JaggedShape(1), bag_id: $%s)",
        TypedValue::FromValue(db).GetFingerprint().AsString().substr(32 - 4));
    EXPECT_THAT(arolla::GenReprToken(ds), ReprTokenEq(expected_repr));
  }
  {
    // DataItem repr without db.
    auto ds = test::DataItem(1);
    EXPECT_THAT(arolla::GenReprToken(ds),
                ReprTokenEq("DataItem(1, schema: INT32)"));
  }
  {
    // DataItem repr with db.
    auto ds = test::DataItem(1, db);
    std::string expected_repr = absl::StrFormat(
        "DataItem(1, schema: INT32, bag_id: $%s)",
        TypedValue::FromValue(db).GetFingerprint().AsString().substr(32 - 4));
    EXPECT_THAT(arolla::GenReprToken(ds), ReprTokenEq(expected_repr));
  }
}

TEST(DataSliceCastingTest, ToIn64_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kInt64)));

  auto values_int32 = test::DataSlice<int>({42, 12});
  ASSERT_OK(entity.SetAttr("a", values_int32));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a.slice(), ElementsAre(42l, 12l));

  // Empty INT32
  values_int32 = test::EmptyDataSlice(shape, schema::kInt32);
  ASSERT_OK(entity.SetAttr("a", values_int32));
  ASSERT_OK_AND_ASSIGN(ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a.slice(), ElementsAre(std::nullopt, std::nullopt));
}

TEST(DataSliceCastingTest, ToIn64_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kInt64)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto values_int32 = test::DataSlice<int>({42, 12});
  ASSERT_OK(objects.SetAttr("a", values_int32));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a.slice(), ElementsAre(42l, 12l));

  // Empty INT32
  values_int32 = test::EmptyDataSlice(shape, schema::kInt32);
  ASSERT_OK(objects.SetAttr("a", values_int32));
  ASSERT_OK_AND_ASSIGN(ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a.slice(), ElementsAre(std::nullopt, std::nullopt));
}

TEST(DataSliceCastingTest, ToFloat64_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kFloat64)));

  auto values_float32 = test::DataSlice<float>({3.1, 2.7});
  ASSERT_OK(entity.SetAttr("a", values_float32));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kFloat64);
  EXPECT_THAT(ds_a.slice()[0].value<double>(), DoubleNear(3.1, 0.001));
  EXPECT_THAT(ds_a.slice()[1].value<double>(), DoubleNear(2.7, 0.001));
}

TEST(DataSliceCastingTest, ToFloat64_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kFloat64)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto values_float32 = test::DataSlice<float>({3.1, 2.7});
  ASSERT_OK(objects.SetAttr("a", values_float32));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kFloat64);
  EXPECT_THAT(ds_a.slice()[0].value<double>(), DoubleNear(3.1, 0.001));
  EXPECT_THAT(ds_a.slice()[1].value<double>(), DoubleNear(2.7, 0.001));
}

TEST(DataSliceCastingTest, EmptyToOther_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kText)));

  auto empty_values_any = test::EmptyDataSlice(shape, schema::kAny);
  EXPECT_THAT(entity.SetAttr("a", empty_values_any),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': ANY"));

  auto empty_values_object = test::EmptyDataSlice(shape, schema::kObject);
  EXPECT_THAT(entity.SetAttr("a", empty_values_object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': OBJECT"));
}

TEST(DataSliceCastingTest, EmptyToOther_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kText)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto empty_values_any = test::EmptyDataSlice(shape, schema::kAny);
  EXPECT_THAT(objects.SetAttr("a", empty_values_any),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': ANY"));

  auto empty_values_object = test::EmptyDataSlice(shape, schema::kObject);
  EXPECT_THAT(objects.SetAttr("a", empty_values_object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': OBJECT"));
}

TEST(DataSliceCastingTest, SameUnderlying_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kText)));

  auto values_any_text =
      test::DataSlice<arolla::Text>({"abc", std::nullopt}, schema::kAny);
  EXPECT_THAT(entity.SetAttr("a", values_any_text),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': ANY"));

  auto values_text =
      test::DataSlice<arolla::Text>({"abc", std::nullopt}, schema::kText);
  ASSERT_OK(entity.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kText);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, SameUnderlying_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kText)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto values_any_text =
      test::DataSlice<arolla::Text>({"abc", std::nullopt}, schema::kAny);
  EXPECT_THAT(objects.SetAttr("a", values_any_text),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': TEXT\n"
                       "Assigned schema for 'a': ANY"));

  auto values_text =
      test::DataSlice<arolla::Text>({"abc", std::nullopt}, schema::kText);
  ASSERT_OK(objects.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kText);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, ToAny_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kAny)));

  auto values_text = test::DataSlice<arolla::Text>({"abc", std::nullopt});
  ASSERT_OK(entity.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kAny);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, ToAny_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kAny)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto values_text = test::DataSlice<arolla::Text>({"abc", std::nullopt});
  ASSERT_OK(objects.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kAny);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, IncompatibleSchema_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kText)));

  EXPECT_THAT(
      entity.SetAttr("a", test::DataSlice<int>({12, 42})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("TEXT"), HasSubstr("INT32"))));
}

TEST(DataSliceCastingTest, IncompatibleSchema_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kText)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  EXPECT_THAT(
      objects.SetAttr("a", test::DataSlice<int>({12, 42})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               AllOf(HasSubstr("the schema for attribute 'a' is incompatible"),
                     HasSubstr("TEXT"), HasSubstr("INT32"))));
}

TEST(DataSliceCastingTest, PrimitiveToObject_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kObject)));

  auto values_text = test::DataSlice<arolla::Text>({"abc", std::nullopt});
  ASSERT_OK(entity.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, PrimitiveToObject_Object) {
  auto db = DataBag::Empty();
  auto explicit_schema = test::Schema(internal::AllocateExplicitSchema(), db);
  ASSERT_OK(explicit_schema.SetAttr("a", test::Schema(schema::kObject)));

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, explicit_schema));

  auto values_text = test::DataSlice<arolla::Text>({"abc", std::nullopt});
  ASSERT_OK(objects.SetAttr("a", values_text));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(ds_a.slice(), ElementsAre(arolla::Text("abc"), std::nullopt));
}

TEST(DataSliceCastingTest, SchemaToObject) {
  arolla::InitArolla();
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::FromAttrs(db, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kObject)));

  auto schema_item = test::Schema(schema::kAny);
  EXPECT_THAT(entity.SetAttr("a", test::Schema(schema::kAny)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': OBJECT\n"
                       "Assigned schema for 'a': SCHEMA"));
}

TEST(DataSliceCastingTest, ToObject_EmbedSchema_Entity) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kObject)));

  auto val_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto val_entity,
                       EntityCreator::Shaped(val_db, shape, {}, {}));
  ASSERT_OK(entity.SetAttr("a", val_entity));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kObject);
  ASSERT_OK_AND_ASSIGN(auto ds_a_schema, ds_a.GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(ds_a_schema.slice(), ElementsAre(val_entity.GetSchemaImpl(),
                                               val_entity.GetSchemaImpl()));
  // Trying to embed another schema for the same entity fails.
  ASSERT_OK_AND_ASSIGN(
      val_entity,
      val_entity.WithSchema(test::Schema(internal::AllocateExplicitSchema())));
  EXPECT_THAT(entity.SetAttr("a", val_entity),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat(
                           "the schema for attribute 'a' is incompatible.\n\n"
                           "Expected schema for 'a': OBJECT\n"
                           "Assigned schema for 'a': %v",
                           val_entity.GetSchemaImpl())));
}

TEST(DataSliceCastingTest, ToObject_NoEmbed_Object) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kObject)));

  auto db_2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto val_objects,
                       ObjectCreator::Shaped(db_2, shape, {}, {}));
  ASSERT_OK_AND_ASSIGN(auto val_object_schemas,
                       val_objects.GetAttr(schema::kSchemaAttr));

  ASSERT_OK(entity.SetAttr("a", val_objects));
  ASSERT_OK_AND_ASSIGN(auto ds_a, entity.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kObject);
  ASSERT_OK_AND_ASSIGN(auto ds_a_schema, ds_a.GetAttr(schema::kSchemaAttr));

  // End-to-end of this requires DataBag merging.
  EXPECT_EQ(ds_a_schema.present_count(), 0);

  ASSERT_OK_AND_ASSIGN(ds_a_schema,
                       ds_a.WithDb(db_2).GetAttr(schema::kSchemaAttr));
  EXPECT_THAT(ds_a_schema.slice(), IsEquivalentTo(val_object_schemas.slice()));

  // Assigning an RHS without a DataBag also works.
  auto values_int32_obj = test::DataSlice<internal::ObjectId>(
      {internal::AllocateSingleObject(), internal::AllocateSingleObject()},
      schema::kObject);
  ASSERT_OK(entity.SetAttr("a", values_int32_obj));
  ASSERT_OK_AND_ASSIGN(ds_a, entity.GetAttr("a"));
  EXPECT_THAT(ds_a.slice(), IsEquivalentTo(values_int32_obj.slice()));
}

TEST(DataSliceCastingTest, ToObject_Any_Error) {
  auto db = DataBag::Empty();
  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto entity, EntityCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(entity.GetSchema().SetAttr("a", test::Schema(schema::kObject)));

  auto values_int32_any = test::DataSlice<int>({42, 12}, schema::kAny);
  EXPECT_THAT(entity.SetAttr("a", values_int32_any),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': OBJECT\n"
                       "Assigned schema for 'a': ANY"));
}


TEST(DataSliceCastingTest, Implicit_And_Explicit_CastingAndSchemaUpdate) {
  auto db = DataBag::Empty();
  auto values_int32 = test::DataSlice<int>({42, 12});

  auto explicit_schema = internal::AllocateExplicitSchema();
  ASSERT_OK(test::Schema(explicit_schema, db)
            .SetAttr("a", test::Schema(schema::kInt64)));
  auto implicit_schema = GenerateImplicitSchema();
  ASSERT_OK(test::Schema(implicit_schema, db)
            .SetAttr("a", test::Schema(schema::kInt32)));

  auto schema_slice = test::DataSlice<ObjectId>(
      {explicit_schema, implicit_schema}, schema::kSchema);

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, schema_slice));

  // Now objects have the following:
  // obj_1.__schema__ == explicit_schema
  // obj_2.__schema__ == implicit_schema
  // obj_1.__schema__.a == INT64
  // obj_2.__schema__.a == INT32
  //
  // Setting an INT32 slice on objects, will cause it to be casted to INT64,
  // because of explicit schema and then as part of implicit schema overwrote,
  // obj_2.__schema__.a will become INT64.
  ASSERT_OK_AND_ASSIGN(auto ds_a_object, objects.GetAttr("a"));
  EXPECT_EQ(ds_a_object.GetSchemaImpl(),
            schema::kInt64);  // Before setting attr.

  ASSERT_OK(objects.SetAttr("a", values_int32));
  ASSERT_OK_AND_ASSIGN(auto ds_a, objects.GetAttr("a"));
  EXPECT_EQ(ds_a.GetSchemaImpl(), schema::kInt64);
  EXPECT_THAT(ds_a.slice(), ElementsAre(42l, 12l));

  // Casting does not work on OBJECTs, and requires an explicit call to
  // ToObject.
  values_int32 = test::DataSlice<int>({42, 12}, schema::kObject);
  ASSERT_EQ(values_int32.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(objects.SetAttr("a", values_int32),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "the schema for attribute 'a' is incompatible.\n\n"
                       "Expected schema for 'a': INT64\n"
                       "Assigned schema for 'a': OBJECT"));
}

TEST(DataSliceCastingTest, SchemaAttr_DifferentExplicitSchemas) {
  auto db = DataBag::Empty();

  auto schema_1 = internal::AllocateExplicitSchema();
  ASSERT_OK(test::Schema(schema_1, db)
            .SetAttr("a", test::Schema(schema::kFloat32)));

  auto schema_2 = internal::AllocateExplicitSchema();
  ASSERT_OK(test::Schema(schema_2, db)
            .SetAttr("a", test::Schema(schema::kInt64)));

  auto schema_slice = test::DataSlice<ObjectId>(
      {schema_1, schema_2}, schema::kSchema);

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, schema_slice));

  auto values_float = test::DataSlice<float>({2.71, 3.14});
  EXPECT_THAT(
      objects.SetAttr("a", values_float),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("different types: INT64 and FLOAT32")));
}

// TODO(b/329836487); Explore if this should succeed.
TEST(DataSliceCastingTest, SchemaAttr_DifferentButCompatibleExplicitSchemas) {
  auto db = DataBag::Empty();

  auto schema_1 = internal::AllocateExplicitSchema();
  ASSERT_OK(test::Schema(schema_1, db)
            .SetAttr("a", test::Schema(schema::kInt32)));

  auto schema_2 = internal::AllocateExplicitSchema();
  ASSERT_OK(test::Schema(schema_2, db)
            .SetAttr("a", test::Schema(schema::kInt64)));

  auto schema_slice = test::DataSlice<ObjectId>(
      {schema_1, schema_2}, schema::kSchema);

  auto shape = DataSlice::JaggedShape::FlatFromSize(2);
  ASSERT_OK_AND_ASSIGN(auto objects, ObjectCreator::Shaped(db, shape, {}, {}));
  ASSERT_OK(objects.SetAttr(schema::kSchemaAttr, schema_slice));

  auto values_float = test::DataSlice<float>({2.71, 3.14});
  EXPECT_THAT(
      objects.SetAttr("a", values_float),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("different types: INT64 and INT32")));
}

// More extensive tests are in object_factories_test.cc.
TEST(CastOrUpdateSchema, SimpleTest) {
  auto db = DataBag::Empty();
  auto int_s = test::Schema(schema::kFloat32);
  auto entity_schema = *CreateEntitySchema(db, {"a"}, {int_s});

  ASSERT_OK_AND_ASSIGN(internal::DataBagImpl& db_mutable_impl,
                       db->GetMutableImpl());
  EXPECT_THAT(
      CastOrUpdateSchema(test::DataItem(42), entity_schema.item(), "a",
                         /*update_schema=*/false, db_mutable_impl),
      IsOkAndHolds(IsEquivalentTo(test::DataItem(42.0f))));
}

}  // namespace
}  // namespace koladata
