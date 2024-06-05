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
#include "koladata/internal/schema_utils.h"

#include <functional>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"

namespace koladata::schema {
namespace {

using ::koladata::testing::IsOk;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::Contains;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::UnorderedElementsAreArray;

using arolla::CreateDenseArray;
using internal::DataItem;
using internal::DataSliceImpl;

TEST(SchemaUtilsTest, DTypeLattice) {
  // Check that the lattice contains all supported dtypes. This ensures that
  // the lattice stays up to date.
  const schema_internal::DTypeLattice& lattice =
      schema_internal::GetDTypeLattice();
  std::vector<schema::DType> lattice_keys;
  for (const auto& [key, _] : lattice) {
    lattice_keys.push_back(key);
  }
  std::vector<schema::DType> expected_dtypes;
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    expected_dtypes.push_back(schema::GetDType<T>());
  });
  EXPECT_THAT(lattice_keys, UnorderedElementsAreArray(expected_dtypes));
}

TEST(SchemaUtilsTest, DTypeLatticeIsAcyclic) {
  // Sanity check that the lattice is acyclic. This is otherwise not enforced.
  const schema_internal::DTypeLattice& lattice =
      schema_internal::GetDTypeLattice();
  absl::flat_hash_set<schema::DType> visited;
  std::function<void(schema::DType)> visit = [&](schema::DType dtype) {
    // Loop detection.
    ASSERT_THAT(visited, Not(Contains(dtype)));
    visited.insert(dtype);
    for (const auto& next_dtype : lattice.at(dtype)) {
      visit(next_dtype);
    }
    visited.erase(dtype);
  };
  for (const auto& [dtype, _] : lattice) {
    visit(dtype);
  }
}

TEST(SchemaUtilsTest, CommonSchemaSimple) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {schema::kInt32, schema::kInt32, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(DataItem(schema::kInt32)));
}

TEST(SchemaUtilsTest, CommonSchemaSimple_InitList) {
  EXPECT_THAT(CommonSchema({DataItem(schema::kInt32), DataItem(),
                            DataItem(schema::kInt32)}),
              IsOkAndHolds(DataItem(schema::kInt32)));
}

struct CommonDTypeTestCase {
  std::vector<schema::DType> input_dtypes;
  schema::DType expected_dtype;
};

using CommonDTypeTest = ::testing::TestWithParam<CommonDTypeTestCase>;

TEST_P(CommonDTypeTest, CommonDTypePairwise) {
  const CommonDTypeTestCase& test_case = GetParam();
  std::vector<DataItem> input_data_items;
  for (const auto& dtype : test_case.input_dtypes) {
    input_data_items.emplace_back(dtype);
  }
  EXPECT_THAT(CommonSchema(input_data_items),
              IsOkAndHolds(DataItem(test_case.expected_dtype)));
}

INSTANTIATE_TEST_SUITE_P(
    CommonDTypeTestInit, CommonDTypeTest, ::testing::ValuesIn([] {
      // Generate test cases for all immediate connections.
      std::vector<CommonDTypeTestCase> test_cases;
      for (const auto& [dtype, descendants] :
           schema_internal::GetDTypeLattice()) {
        test_cases.push_back({{dtype, dtype}, dtype});  // Self loop.
        for (const auto& descendant : descendants) {
          // Commutative - so we test both directions.
          test_cases.push_back({{dtype, descendant}, descendant});
          test_cases.push_back({{descendant, dtype}, descendant});
        }
      }
      // Test additional cases manually.
      //
      // Default output.
      test_cases.push_back({{}, schema::kObject});
      // Single input.
      test_cases.push_back({{schema::kInt32}, schema::kInt32});
      // Non-adjacent types.
      test_cases.push_back(
          {{schema::kInt32, schema::kFloat32}, schema::kFloat32});
      // Multiple types.
      test_cases.push_back({{schema::kInt32, schema::kInt64, schema::kFloat32},
                            schema::kFloat32});
      // "sibling" types.
      test_cases.push_back({{schema::kInt32, kText}, schema::kObject});
      return test_cases;
    }()),
    [](const ::testing::TestParamInfo<CommonDTypeTest::ParamType>& info) {
      return absl::StrCat(absl::StrJoin(info.param.input_dtypes, "_"), "_",
                          info.param.expected_dtype);
    });

TEST(SchemaUtilsTest, CommonSchemaNonZeroSizeEmpty) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(DataItem(schema::kObject)));
}

TEST(SchemaUtilsTest, CommonSchemaObjectIdSchema) {
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto schemas = DataSliceImpl::Create(CreateDenseArray<internal::ObjectId>(
      {std::nullopt, explicit_schema, explicit_schema}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(explicit_schema));
}

TEST(SchemaUtilsTest, CommonSchemaWrongTypeInSchemas) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<schema::DType>({schema::kAny, std::nullopt}),
      CreateDenseArray<int>({std::nullopt, 42}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected Schema, got: INT32"));
}

TEST(SchemaUtilsTest, CommonSchemaNonTypeInSchemas) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<internal::ObjectId>(
      {internal::AllocateExplicitSchema(),
       /*non-schema=*/internal::AllocateSingleObject()}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Schema slice with ObjectId schema can accept "
                                 "only schema ObjectIds")));
}

TEST(SchemaUtilsTest, CommonSchemaConflict) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<internal::ObjectId>(
      {internal::AllocateExplicitSchema(),
       internal::AllocateExplicitSchema()}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
}

TEST(SchemaUtilsTest, CommonSchemaObjectAndPrimitiveNone) {
  auto explicit_schema = internal::AllocateExplicitSchema();
  auto schemas =
      DataSliceImpl::Create(CreateDenseArray<internal::ObjectId>(
                                {std::nullopt, explicit_schema, std::nullopt}),
                            CreateDenseArray<schema::DType>(
                                {schema::kNone, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas), IsOkAndHolds(explicit_schema));
}

TEST(SchemaUtilsTest, CommonSchemaObjectAndPrimitiveConflict) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<internal::ObjectId>(
          {std::nullopt, internal::AllocateExplicitSchema(), std::nullopt}),
      CreateDenseArray<schema::DType>(
          {schema::kInt32, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
}

TEST(SchemaUtilsTest, CommonSchemaPrimitiveConflict) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<schema::DType>({schema::kInt32, schema::kItemId}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common schema")));
}

TEST(SchemaUtilsTest, CommonSchemaPrimitiveAndObjectConflict) {
  auto schemas = DataSliceImpl::Create(
      CreateDenseArray<schema::DType>(
          {schema::kInt32, std::nullopt, std::nullopt}),
      CreateDenseArray<internal::ObjectId>(
          {std::nullopt, internal::AllocateExplicitSchema(), std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("no common schema")));
}

TEST(SchemaUtilsTest, DefaultIfMissing_Object) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas, DataItem(schema::kObject)),
              IsOkAndHolds(DataItem(schema::kObject)));
}

TEST(SchemaUtilsTest, DefaultIfMissing_Empty) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas, DataItem()), IsOkAndHolds(DataItem()));
}

TEST(SchemaUtilsTest, DefaultIfMissing_Any) {
  auto schemas = DataSliceImpl::Create(CreateDenseArray<schema::DType>(
      {std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_THAT(CommonSchema(schemas, DataItem(schema::kAny)),
              IsOkAndHolds(DataItem(schema::kAny)));
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
      NoFollowSchemaItem(DataItem(schema::kAny)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("calling nofollow on ANY slice is not allowed")));
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
  EXPECT_TRUE(VerifySchemaForItemIds(DataItem(schema::kAny)));
  EXPECT_TRUE(VerifySchemaForItemIds(DataItem(schema::kObject)));
  EXPECT_TRUE(VerifySchemaForItemIds(DataItem(schema::kItemId)));

  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kInt32)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kInt64)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kFloat32)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kFloat64)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kBool)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kBytes)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kText)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kExpr)));
  EXPECT_FALSE(VerifySchemaForItemIds(DataItem(schema::kMask)));
}

TEST(SchemaUtilsTest, VerifyDictKeySchema) {
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kInt32)), IsOk());
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kFloat32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("dict keys cannot be FLOAT32")));
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kFloat64)),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kNone)),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(VerifyDictKeySchema(DataItem(schema::kExpr)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

}  // namespace
}  // namespace koladata::schema
