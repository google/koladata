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
#include "koladata/internal/schema_utils.h"

#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::schema {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using arolla::CreateDenseArray;
using internal::DataItem;
using internal::DataSliceImpl;
using ::testing::Contains;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::NotNull;

TEST(SchemaUtilsTest, DTypeLatticeIsAcyclic) {
  // Sanity check that the lattice is acyclic. This is otherwise not enforced.
  const schema_internal::DTypeLattice& lattice =
      schema_internal::GetDTypeLattice();
  absl::flat_hash_set<schema::DTypeId> visited;
  bool cyclic = false;
  std::function<void(schema::DTypeId)> visit = [&](schema::DTypeId dtype) {
    cyclic |= visited.contains(dtype);
    // Loop detection.
    ASSERT_THAT(visited, Not(Contains(dtype)));
    if (cyclic) {
      return;
    }
    visited.insert(dtype);
    for (schema::DTypeId next_dtype = 0; next_dtype < schema::kNextDTypeId;
         ++next_dtype) {
      if (lattice[dtype][next_dtype]) {
        visit(next_dtype);
      }
    }
    visited.erase(dtype);
  };
  for (schema::DTypeId dtype = 0; dtype < schema::kNextDTypeId; ++dtype) {
    visited.clear();
    visit(dtype);
  }
}


struct CommonDTypeTestCase {
  std::vector<schema::DType> input_dtypes;
  std::optional<schema::DType> expected_dtype;
};

using CommonDTypeTest = ::testing::TestWithParam<CommonDTypeTestCase>;

TEST_P(CommonDTypeTest, CommonDTypePairwise) {
  const CommonDTypeTestCase& test_case = GetParam();
  CommonSchemaAggregator agg;
  for (const auto& dtype : test_case.input_dtypes) {
    agg.Add(dtype);
  }
  EXPECT_THAT(std::move(agg).Get(),
              IsOkAndHolds(DataItem(test_case.expected_dtype)));
}

INSTANTIATE_TEST_SUITE_P(
    CommonDTypeTestInit, CommonDTypeTest, ::testing::ValuesIn([] {
      // Generate test cases for all immediate connections.
      std::vector<CommonDTypeTestCase> test_cases;
      constexpr auto lattice = schema_internal::GetDTypeLattice();
      for (schema::DTypeId dtype_id = 0; dtype_id < schema::kNextDTypeId;
           ++dtype_id) {
        schema::DType dtype = schema::DType::UnsafeFromId(dtype_id);
        test_cases.push_back({{dtype, dtype}, dtype});  // Self loop.
        for (schema::DTypeId descendant_id = 0;
             descendant_id < schema::kNextDTypeId; ++descendant_id) {
          if (!lattice[dtype_id][descendant_id]) {
            continue;
          }
          schema::DType descendant = schema::DType::UnsafeFromId(descendant_id);
          // Commutative - so we test both directions.
          test_cases.push_back({{dtype, descendant}, descendant});
          test_cases.push_back({{descendant, dtype}, descendant});
        }
      }
      // Test additional cases manually.
      //
      // Default output.
      test_cases.push_back({{}, std::nullopt});
      // Single input.
      test_cases.push_back({{schema::kInt32}, schema::kInt32});
      // Non-adjacent types.
      test_cases.push_back(
          {{schema::kInt32, schema::kFloat32}, schema::kFloat32});
      // Multiple types.
      test_cases.push_back({{schema::kInt32, schema::kInt64, schema::kFloat32},
                            schema::kFloat32});
      // "sibling" types.
      test_cases.push_back({{schema::kInt32, kString}, schema::kObject});
      return test_cases;
    }()),
    [](const ::testing::TestParamInfo<CommonDTypeTest::ParamType>& info) {
      return absl::StrCat(absl::StrJoin(info.param.input_dtypes, "_"), "_",
                          info.param.expected_dtype
                              ? absl::StrCat(*info.param.expected_dtype)
                              : "nullopt");
    });

using CommonDTypeBinaryTest = ::testing::TestWithParam<CommonDTypeTestCase>;

TEST_P(CommonDTypeBinaryTest, CommonDTypeBinary) {
  ASSERT_EQ(GetParam().input_dtypes.size(), 2);
  DType lhs = GetParam().input_dtypes[0];
  DType rhs = GetParam().input_dtypes[1];
  // DType inputs.
  EXPECT_THAT(CommonSchema(lhs, rhs),
              IsOkAndHolds(DataItem(GetParam().expected_dtype)));
  // DataItem inputs.
  EXPECT_THAT(CommonSchema(DataItem(lhs), DataItem(rhs)),
              IsOkAndHolds(DataItem(GetParam().expected_dtype)));
  DTypeId common_dtype_id = CommonDType(lhs.type_id(), rhs.type_id());
  if (GetParam().expected_dtype.has_value()) {
    EXPECT_EQ(common_dtype_id, GetParam().expected_dtype->type_id());
  } else {
    EXPECT_EQ(common_dtype_id, schema_internal::kUnknownDType);
  }
}

INSTANTIATE_TEST_SUITE_P(
    CommonDTypeBinaryTestInit, CommonDTypeBinaryTest, ::testing::ValuesIn([] {
      // Generate test cases for all immediate connections.
      std::vector<CommonDTypeTestCase> test_cases;
      constexpr auto lattice = schema_internal::GetDTypeLattice();
      for (schema::DTypeId dtype_id = 0; dtype_id < schema::kNextDTypeId;
           ++dtype_id) {
        schema::DType dtype = schema::DType::UnsafeFromId(dtype_id);
        test_cases.push_back({{dtype, dtype}, dtype});  // Self loop.
        for (schema::DTypeId descendant_id = 0;
             descendant_id < schema::kNextDTypeId; ++descendant_id) {
          if (!lattice[dtype_id][descendant_id]) {
            continue;
          }
          schema::DType descendant = schema::DType::UnsafeFromId(descendant_id);
          // Commutative - so we test both directions.
          test_cases.push_back({{dtype, descendant}, descendant});
          test_cases.push_back({{descendant, dtype}, descendant});
        }
      }
      // "sibling" types.
      test_cases.push_back({{schema::kInt32, kString}, schema::kObject});
      return test_cases;
    }()),
    [](const ::testing::TestParamInfo<CommonDTypeTest::ParamType>& info) {
      return absl::StrCat(absl::StrJoin(info.param.input_dtypes, "_"), "_",
                          info.param.expected_dtype
                              ? absl::StrCat(*info.param.expected_dtype)
                              : "nullopt");
    });

TEST(SchemaUtilsTest, CommonSchemaUnary) {
  {
    // Simple.
    EXPECT_THAT(CommonSchema(DataItem(schema::kInt32)),
                IsOkAndHolds(schema::kInt32));
    EXPECT_THAT(CommonSchema(DataItem(schema::kObject)),
                IsOkAndHolds(schema::kObject));
    auto explicit_schema = DataItem(internal::AllocateExplicitSchema());
    EXPECT_THAT(CommonSchema(DataItem(explicit_schema)),
                IsOkAndHolds(explicit_schema));
    EXPECT_THAT(CommonSchema(DataItem()), IsOkAndHolds(std::nullopt));
  }
  {
    // Not a schema error.
    EXPECT_THAT(CommonSchema(DataItem(1)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected Schema, got: 1"));
  }
}

TEST(SchemaUtilsTest, CommonSchemaBinary) {
  {
    // Empty items.
    EXPECT_THAT(CommonSchema(DataItem(schema::kInt32), DataItem()),
                IsOkAndHolds(schema::kInt32));
    EXPECT_THAT(CommonSchema(DataItem(), DataItem(schema::kInt32)),
                IsOkAndHolds(schema::kInt32));
    EXPECT_THAT(CommonSchema(DataItem(), DataItem()),
                IsOkAndHolds(std::nullopt));
  }
  {
    // Identical entity schemas.
    auto explicit_schema = DataItem(internal::AllocateExplicitSchema());
    EXPECT_THAT(CommonSchema(explicit_schema, explicit_schema),
                IsOkAndHolds(explicit_schema));
  }
  {
    // Entity schema and None.
    auto explicit_schema = DataItem(internal::AllocateExplicitSchema());
    EXPECT_THAT(CommonSchema(explicit_schema, DataItem(kNone)),
                IsOkAndHolds(explicit_schema));
    EXPECT_THAT(CommonSchema(DataItem(kNone), explicit_schema),
                IsOkAndHolds(explicit_schema));
  }
  {
    // Not a schema error.
    EXPECT_THAT(CommonSchema(DataItem(1), DataItem(kString)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected Schema, got: 1"));
    EXPECT_THAT(CommonSchema(DataItem(kString), DataItem(1)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected Schema, got: 1"));
  }
  {
    // No common schema error.
    auto result = CommonSchema(kItemId, kString);
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "no common schema"));
    EXPECT_THAT(
        arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
        NotNull());

    internal::ObjectId obj_id = internal::AllocateExplicitSchema();
    auto explicit_schema = DataItem(obj_id);

    result = CommonSchema(DataItem(kItemId), explicit_schema);
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "no common schema"));
    EXPECT_THAT(
        arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
        NotNull());

    result = CommonSchema(explicit_schema, DataItem(kItemId));
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "no common schema"));
    EXPECT_THAT(
        arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
        NotNull());

    internal::ObjectId obj_id2 = internal::AllocateExplicitSchema();
    result = CommonSchema(explicit_schema, DataItem(obj_id2));
    EXPECT_THAT(result, StatusIs(absl::StatusCode::kInvalidArgument,
                                 "no common schema"));
    EXPECT_THAT(
        arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
        NotNull());
  }
}

TEST(SchemaUtilsTest, CommonSchemaSimple_DTypes) {
  {
    // DTypes.
    CommonSchemaAggregator agg;
    agg.Add(schema::kInt32);
    agg.Add(schema::kInt64);
    EXPECT_THAT(std::move(agg).Get(), IsOkAndHolds(DataItem(schema::kInt64)));
  }
  {
    // DataItems with DTypes.
    CommonSchemaAggregator agg;
    agg.Add(DataItem(schema::kInt32));
    agg.Add(DataItem(schema::kInt64));
    agg.Add(DataItem());
    EXPECT_THAT(std::move(agg).Get(), IsOkAndHolds(DataItem(schema::kInt64)));
  }
}

TEST(SchemaUtilsTest, CommonSchemaSimple_ObjectIds) {
  auto obj = internal::AllocateExplicitSchema();
  {
    // ObjectIds.
    CommonSchemaAggregator agg;
    agg.Add(obj);
    agg.Add(obj);
    EXPECT_THAT(std::move(agg).Get(), IsOkAndHolds(DataItem(obj)));
  }
  {
    // DataItems with ObjectIds.
    CommonSchemaAggregator agg;
    agg.Add(DataItem(obj));
    agg.Add(DataItem(obj));
    agg.Add(DataItem());
    EXPECT_THAT(std::move(agg).Get(), IsOkAndHolds(DataItem(obj)));
  }
}

TEST(SchemaUtilsTest, CommonSchemaWrongTypeInSchemas) {
  CommonSchemaAggregator agg;
  agg.Add(DataItem(42));
  EXPECT_THAT(std::move(agg).Get(), StatusIs(absl::StatusCode::kInvalidArgument,
                                             "expected Schema, got: 42"));
}

TEST(SchemaUtilsTest, CommonSchemaNonTypeInSchemas) {
  CommonSchemaAggregator agg;
  agg.Add(DataItem(internal::AllocateSingleObject()));
  EXPECT_THAT(std::move(agg).Get(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected a schema ObjectId")));
}

TEST(SchemaUtilsTest, CommonSchemaConflict) {
  CommonSchemaAggregator agg;
  internal::ObjectId schema1 = internal::AllocateExplicitSchema();
  internal::ObjectId schema2 = internal::AllocateExplicitSchema();
  agg.Add(DataItem(schema1));
  agg.Add(DataItem(schema2));
  const auto result = std::move(agg).Get();
  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
  EXPECT_THAT(
      arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
      NotNull());
}

TEST(SchemaUtilsTest, CommonSchemaObjectAndPrimitiveNone) {
  CommonSchemaAggregator agg;
  auto explicit_schema = internal::AllocateExplicitSchema();
  agg.Add(explicit_schema);
  agg.Add(schema::kNone);
  EXPECT_THAT(std::move(agg).Get(), IsOkAndHolds(explicit_schema));
}

TEST(SchemaUtilsTest, CommonSchemaObjectAndPrimitiveConflict) {
  internal::ObjectId schema = internal::AllocateExplicitSchema();
  CommonSchemaAggregator agg;
  agg.Add(schema);
  agg.Add(schema::kInt32);
  const auto result = std::move(agg).Get();
  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
  EXPECT_THAT(
      arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
      NotNull());
}

TEST(SchemaUtilsTest, CommonSchemaPrimitiveConflict) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<schema::DType>({schema::kInt32, schema::kItemId}));
  CommonSchemaAggregator agg;
  agg.Add(schema::kInt32);
  agg.Add(schema::kItemId);
  const auto result = std::move(agg).Get();
  EXPECT_THAT(result,
              StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
  EXPECT_THAT(
      arolla::GetPayload<internal::NoCommonSchemaError>(result.status()),
      NotNull());
}

TEST(SchemaUtilsTest, CommonSchema_DataSliceImpl_DTypes) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {schema::kInt32, schema::kInt32, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(DataItem(schema::kInt32)));

  // Behaves the same as CommonSchemaAggregator on DataItems.
  CommonSchemaAggregator agg;
  for (int i = 0; i < schemas.size(); ++i) {
    agg.Add(schemas[i]);
  }
  ASSERT_OK_AND_ASSIGN(auto agg_res, std::move(agg).Get());
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(agg_res));
}

TEST(SchemaUtilsTest, CommonSchema_ObjectId) {
  internal::ObjectId schema = internal::AllocateExplicitSchema();
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<internal::ObjectId>({schema, schema, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(DataItem(schema)));
}

TEST(SchemaUtilsTest, CommonSchema_InvalidInput) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<schema::DType>({schema::kObject, std::nullopt}),
      CreateDenseArray<int>({std::nullopt, 42}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected Schema, got: INT32"));
}

TEST(SchemaUtilsTest, DefaultIfMissing) {
  {
    CommonSchemaAggregator agg;
    EXPECT_THAT(CommonSchemaAggregator().Get(), IsOkAndHolds(DataItem()));
  }
  {
    CommonSchemaAggregator agg;
    agg.Add(DataItem());
    EXPECT_THAT(CommonSchemaAggregator().Get(), IsOkAndHolds(DataItem()));
  }
}


TEST(SchemaUtilsTest, IsImplicitlyCastableTo) {
  {
    // DType -> DType.
    EXPECT_TRUE(IsImplicitlyCastableTo(DataItem(schema::kInt32),
                                       DataItem(schema::kInt32)));
    EXPECT_TRUE(IsImplicitlyCastableTo(DataItem(schema::kInt32),
                                       DataItem(schema::kInt64)));
    EXPECT_TRUE(IsImplicitlyCastableTo(DataItem(schema::kInt32),
                                       DataItem(schema::kObject)));
    EXPECT_FALSE(IsImplicitlyCastableTo(DataItem(schema::kInt64),
                                        DataItem(schema::kInt32)));
    EXPECT_FALSE(IsImplicitlyCastableTo(DataItem(schema::kInt32),
                                        DataItem(schema::kString)));
    EXPECT_FALSE(IsImplicitlyCastableTo(DataItem(schema::kInt32),
                                        DataItem(schema::kItemId)));
  }
  {
    // ObjectId -> ObjectId.
    auto schema1 = DataItem(internal::AllocateExplicitSchema());
    auto schema2 = DataItem(internal::AllocateExplicitSchema());
    EXPECT_TRUE(IsImplicitlyCastableTo(schema1, schema1));
    EXPECT_FALSE(IsImplicitlyCastableTo(schema1, schema2));
    EXPECT_FALSE(IsImplicitlyCastableTo(schema2, schema1));
  }
  {
    // ObjectId -> DType.
    auto schema = DataItem(internal::AllocateExplicitSchema());
    EXPECT_FALSE(IsImplicitlyCastableTo(schema, DataItem(schema::kObject)));
  }
  {
    // DType -> ObjectId.
    auto schema = DataItem(internal::AllocateExplicitSchema());
    // NONE casts to everything.
    EXPECT_TRUE(IsImplicitlyCastableTo(DataItem(schema::kNone), schema));
    EXPECT_FALSE(IsImplicitlyCastableTo(DataItem(schema::kObject), schema));
  }
}

TEST(SchemaUtilsTest, NoFollow_Roundtrip_OBJECT) {
  ASSERT_OK_AND_ASSIGN(auto nofollow,
                       NoFollowSchemaItem(DataItem(schema::kObject)));
  EXPECT_TRUE(nofollow.value<internal::ObjectId>().IsNoFollowSchema());
  EXPECT_THAT(GetNoFollowedSchemaItem(nofollow), IsOkAndHolds(schema::kObject));
}

TEST(SchemaUtilsTest, NoFollow_Roundtrip_ObjectIdSchema) {
  auto schema_id = internal::AllocateExplicitSchema();
  ASSERT_OK_AND_ASSIGN(auto nofollow, NoFollowSchemaItem(DataItem(schema_id)));
  EXPECT_TRUE(nofollow.value<internal::ObjectId>().IsNoFollowSchema());
  EXPECT_THAT(GetNoFollowedSchemaItem(nofollow), IsOkAndHolds(schema_id));
}

TEST(SchemaUtilsTest, NoFollowSchemaItem_Errors) {
  EXPECT_THAT(
      NoFollowSchemaItem(DataItem(internal::AllocateSingleObject())),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("calling nofollow on a non-schema is not allowed")));
  EXPECT_THAT(
      NoFollowSchemaItem(
          *NoFollowSchemaItem(DataItem(internal::AllocateExplicitSchema()))),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("calling nofollow on a nofollow slice is not allowed")));
  EXPECT_THAT(
      NoFollowSchemaItem(DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("calling nofollow on INT32 slice is not allowed")));
  EXPECT_THAT(
      NoFollowSchemaItem(DataItem(42)),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("schema can be either a DType or ObjectId schema")));
}

TEST(SchemaUtilsTest, GetNoFollowedSchemaItem_Errors) {
  EXPECT_THAT(
      GetNoFollowedSchemaItem(DataItem(internal::AllocateSingleObject())),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("a nofollow schema is required in get_nofollowed_schema")));
}

TEST(SchemaUtilsTest, VerifySchemaForItemIds) {
  EXPECT_TRUE(VerifySchemaForItemIds(DataItem(schema::kObject)));
  EXPECT_TRUE(VerifySchemaForItemIds(DataItem(schema::kItemId)));

  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kInt32)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kInt64)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kFloat32)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kFloat64)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kBool)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kBytes)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kString)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kExpr)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kMask)));
}

TEST(SchemaUtilsTest, VerifyDictKeySchema) {
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kInt32)), IsOk());
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kNone)), IsOk());
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kFloat32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("dict keys cannot be FLOAT32")));
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kFloat64)),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kExpr)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SchemaUtilsTest, GetDataSchema) {
  {  // Item.
    EXPECT_EQ(GetDataSchema(DataItem(1)), DataItem(schema::kInt32));
    EXPECT_EQ(GetDataSchema(DataItem()), DataItem(schema::kNone));
    EXPECT_EQ(GetDataSchema(DataItem(internal::AllocateSingleObject())),
              DataItem());
  }
  {
    // Slice.
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create({DataItem(1), DataItem()})),
              DataItem(schema::kInt32));
    EXPECT_EQ(GetDataSchema(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              DataItem(schema::kNone));
    EXPECT_EQ(GetDataSchema(DataItem(internal::AllocateSingleObject())),
              DataItem());
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create(
                  {DataItem(internal::AllocateSingleObject()), DataItem()})),
              DataItem());
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create(
                  {DataItem(1), DataItem(DataItem(1.0f))})),
              DataItem(schema::kFloat32));
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create(
                  {DataItem(1), DataItem(DataItem("foo"))})),
              DataItem(schema::kObject));
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create(
                  {DataItem(schema::kInt32), DataItem(schema::kFloat32)})),
              DataItem(schema::kSchema));
    // No common type between int and DType.
    EXPECT_EQ(GetDataSchema(DataSliceImpl::Create(
                  {DataItem(1), DataItem(schema::kInt32)})),
              DataItem());
  }
  {
    // DataItem: With a provided bag.
    auto bag = internal::DataBagImpl::CreateEmptyDatabag();
    DataItem obj(internal::AllocateSingleObject());
    EXPECT_EQ(GetDataSchema(obj), DataItem());
    EXPECT_EQ(GetDataSchema(obj, &*bag), DataItem());

    // Assign a schema.
    DataItem schema(internal::AllocateExplicitSchema());
    ASSERT_OK(bag->SetAttr(obj, schema::kSchemaAttr, schema));
    EXPECT_EQ(GetDataSchema(obj), DataItem());
    EXPECT_EQ(GetDataSchema(obj, &*bag), DataItem(schema));

    // As a fallback.
    auto bag_2 = internal::DataBagImpl::CreateEmptyDatabag();
    EXPECT_EQ(GetDataSchema(obj, &*bag_2), DataItem());
    EXPECT_EQ(GetDataSchema(obj, &*bag_2, {bag.get()}), DataItem(schema));

    // Ignored fallback.
    DataItem schema_2(internal::AllocateExplicitSchema());
    ASSERT_OK(bag_2->SetAttr(obj, schema::kSchemaAttr, schema_2));
    EXPECT_EQ(GetDataSchema(obj, &*bag_2, {bag.get()}), DataItem(schema_2));
  }
  {
    // DataSlice: With a provided bag.
    auto bag = internal::DataBagImpl::CreateEmptyDatabag();
    auto objs = DataSliceImpl::Create(
        {DataItem(internal::AllocateSingleObject()), DataItem(),
         DataItem(internal::AllocateSingleObject())});
    EXPECT_EQ(GetDataSchema(objs), DataItem());
    EXPECT_EQ(GetDataSchema(objs, &*bag), DataItem());

    // Assign a schema.
    DataItem schema(internal::AllocateExplicitSchema());
    ASSERT_OK(bag->SetAttr(objs, schema::kSchemaAttr,
                           DataSliceImpl::Create({schema, schema, schema})));
    EXPECT_EQ(GetDataSchema(objs), DataItem());
    EXPECT_EQ(GetDataSchema(objs, &*bag), DataItem(schema));

    // As a fallback.
    auto bag_2 = internal::DataBagImpl::CreateEmptyDatabag();
    EXPECT_EQ(GetDataSchema(objs, &*bag_2), DataItem());
    EXPECT_EQ(GetDataSchema(objs, &*bag_2, {bag.get()}), DataItem(schema));

    // Ignored fallback.
    DataItem schema_2(internal::AllocateExplicitSchema());
    ASSERT_OK(
        bag_2->SetAttr(objs, schema::kSchemaAttr,
                       DataSliceImpl::Create({schema_2, schema_2, schema_2})));
    EXPECT_EQ(GetDataSchema(objs, &*bag_2, {bag.get()}), DataItem(schema_2));

    // One missing - ambiguous.
    auto bag_3 = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(
        bag_3->SetAttr(objs, schema::kSchemaAttr,
                       DataSliceImpl::Create({DataItem(), schema, schema})));
    EXPECT_EQ(GetDataSchema(objs, &*bag_3, {}), DataItem());
    // Conflicting - ambiguous.
    ASSERT_OK(
        bag_3->SetAttr(objs, schema::kSchemaAttr,
                       DataSliceImpl::Create({schema_2, schema, schema})));
    EXPECT_EQ(GetDataSchema(objs, &*bag_3, {}), DataItem());
  }
}

}  // namespace
}  // namespace koladata::schema
