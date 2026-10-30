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
#include "koladata/internal/op_utils/vectorized_traverser_utils.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal::vectorized_traverser_internal {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

DataItem TextItem(absl::string_view text) {
  return DataItem(arolla::Text(text));
}

DataSliceImpl Slice(absl::Span<const DataItem> items) {
  return DataSliceImpl::Create(items);
}

TEST(VectorizedTraverserUtilsTest, ValidatePrevisitTokensValid) {
  auto objs = DataSliceImpl::AllocateEmptyObjects(2);
  EXPECT_THAT(ValidatePrevisitTokens("PrevisitSlice", objs, objs), IsOk());
  EXPECT_THAT(ValidatePrevisitTokens(
                  "PrevisitSlice", objs,
                  DataSliceImpl::CreateEmptyAndUnknownType(objs.size())),
              IsOk());
  EXPECT_THAT(
      ValidatePrevisitTokens("PrevisitSlice", objs,
                             DataSliceImpl::Create(CreateDenseArray<ObjectId>(
                                 {std::nullopt, std::nullopt}))),
      IsOk());
  EXPECT_THAT(ValidatePrevisitTokens("PrevisitSlice", objs,
                                     Slice({objs[0], DataItem()})),
              IsOk());
}

TEST(VectorizedTraverserUtilsTest, ValidatePrevisitTokensSizeMismatch) {
  auto objs = DataSliceImpl::AllocateEmptyObjects(2);
  auto one_token = DataSliceImpl::AllocateEmptyObjects(1);
  EXPECT_THAT(ValidatePrevisitTokens("PrevisitSlice", objs, one_token),
              StatusIs(absl::StatusCode::kInternal,
                       "expected 2 tokens from PrevisitSlice, got 1"));
}

TEST(VectorizedTraverserUtilsTest, ValidatePrevisitTokensInvalidDType) {
  auto objs = DataSliceImpl::AllocateEmptyObjects(2);
  EXPECT_THAT(ValidatePrevisitTokens("PrevisitSlice", objs,
                                     Slice({DataItem(1), DataItem(2)})),
              StatusIs(absl::StatusCode::kInternal,
                       "expected ObjectId tokens from PrevisitSlice, got "
                       "INT32"));
  EXPECT_THAT(ValidatePrevisitTokens("PrevisitObjectSchemas", objs,
                                     Slice({objs[0], DataItem(1)})),
              StatusIs(absl::StatusCode::kInternal,
                       "expected ObjectId tokens from PrevisitObjectSchemas, "
                       "got mixed types"));
}

TEST(VectorizedTraverserUtilsTest, ValidatePrimitiveType) {
  EXPECT_THAT(ValidatePrimitiveType(DataItem(1), DataItem(schema::kInt32)),
              IsOk());
  EXPECT_THAT(ValidatePrimitiveType(DataItem(), DataItem(schema::kInt32)),
              IsOk());
  EXPECT_THAT(ValidatePrimitiveType(TextItem("a"), DataItem(schema::kString)),
              IsOk());

  EXPECT_THAT(
      ValidatePrimitiveType(DataItem(1.5f), DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content has type FLOAT32")));
  EXPECT_THAT(
      ValidatePrimitiveType(DataItem(AllocateSingleObject()),
                            DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content is not a primitive")));
}

TEST(VectorizedTraverserUtilsTest, ValidatePrimitiveTypes) {
  EXPECT_THAT(
      ValidatePrimitiveTypes(DataSliceImpl::CreateEmptyAndUnknownType(2),
                             DataItem(schema::kInt32)),
      IsOk());
  EXPECT_THAT(ValidatePrimitiveTypes(Slice({DataItem(1), DataItem()}),
                                     DataItem(schema::kInt32)),
              IsOk());

  EXPECT_THAT(
      ValidatePrimitiveTypes(Slice({DataItem(1.5f)}), DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content has type FLOAT32")));
  EXPECT_THAT(
      ValidatePrimitiveTypes(DataSliceImpl::AllocateEmptyObjects(1),
                             DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content is not a primitive")));
  EXPECT_THAT(
      ValidatePrimitiveTypes(Slice({DataItem(1), TextItem("a")}),
                             DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content has type STRING")));
  EXPECT_THAT(
      ValidatePrimitiveTypes(
          Slice({DataItem(1), DataItem(AllocateSingleObject())}),
          DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content is not a primitive")));
}

TEST(VectorizedTraverserUtilsTest, ContainsOnlyValidPrimitivesTrue) {
  auto entity_schema = DataItem(AllocateExplicitSchema());
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), DataItem(), TextItem("a")}),
                  Slice({DataItem(schema::kInt32), DataItem(schema::kFloat32),
                         DataItem(schema::kString)})),
              IsOkAndHolds(true));
  // Missing items with non-primitive schemas are ignored.
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), DataItem()}),
                  Slice({DataItem(schema::kInt32), entity_schema})),
              IsOkAndHolds(true));
  // Primitive items with OBJECT schema are valid primitives.
  EXPECT_THAT(ContainsOnlyValidPrimitives(Slice({DataItem(1), TextItem("a")}),
                                          Slice({DataItem(schema::kObject),
                                                 DataItem(schema::kString)})),
              IsOkAndHolds(true));
  EXPECT_THAT(ContainsOnlyValidPrimitives(Slice({DataItem(1), TextItem("a")}),
                                          Slice({DataItem(schema::kObject),
                                                 DataItem(schema::kObject)})),
              IsOkAndHolds(true));
}

TEST(VectorizedTraverserUtilsTest, ContainsOnlyValidPrimitivesFalse) {
  auto obj = DataItem(AllocateSingleObject());
  auto entity_schema = DataItem(AllocateExplicitSchema());
  // Empty slices have no primitive schemas.
  EXPECT_THAT(
      ContainsOnlyValidPrimitives(DataSliceImpl::CreateEmptyAndUnknownType(0),
                                  DataSliceImpl::CreateEmptyAndUnknownType(0)),
      IsOkAndHolds(false));
  // No primitive/DType schemas in `schemas`.
  EXPECT_THAT(ContainsOnlyValidPrimitives(Slice({obj}), Slice({entity_schema})),
              IsOkAndHolds(false));
  // Present item with entity schema, ITEMID, SCHEMA, or missing schema.
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), obj}),
                  Slice({DataItem(schema::kInt32), entity_schema})),
              IsOkAndHolds(false));
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), obj}),
                  Slice({DataItem(schema::kInt32), DataItem(schema::kItemId)})),
              IsOkAndHolds(false));
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), DataItem(schema::kInt32)}),
                  Slice({DataItem(schema::kInt32), DataItem(schema::kSchema)})),
              IsOkAndHolds(false));
  EXPECT_THAT(ContainsOnlyValidPrimitives(
                  Slice({DataItem(1), DataItem(2)}),
                  Slice({DataItem(schema::kInt32), DataItem()})),
              IsOkAndHolds(false));
  // ObjectId item with OBJECT schema is not a primitive.
  EXPECT_THAT(ContainsOnlyValidPrimitives(Slice({DataItem(1), obj}),
                                          Slice({DataItem(schema::kObject),
                                                 DataItem(schema::kObject)})),
              IsOkAndHolds(false));
}

TEST(VectorizedTraverserUtilsTest, ContainsOnlyValidPrimitivesTypeMismatch) {
  auto obj = DataItem(AllocateSingleObject());
  auto entity_schema = DataItem(AllocateExplicitSchema());
  EXPECT_THAT(
      ContainsOnlyValidPrimitives(
          Slice({DataItem(1), TextItem("bad")}),
          Slice({DataItem(schema::kInt32), DataItem(schema::kInt32)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content has type STRING")));
  // Primitive type validation still runs after seeing a non-primitive item.
  EXPECT_THAT(
      ContainsOnlyValidPrimitives(
          Slice({obj, TextItem("bad")}),
          Slice({entity_schema, DataItem(schema::kInt32)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("got a slice with primitive type INT32 while the "
                         "actual content has type STRING")));
}

TEST(VectorizedTraverserUtilsTest, IsTraversableSchema) {
  EXPECT_TRUE(IsTraversableSchema(DataItem(schema::kSchema)));
  EXPECT_TRUE(IsTraversableSchema(DataItem(schema::kObject)));
  EXPECT_TRUE(IsTraversableSchema(DataItem(AllocateExplicitSchema())));

  EXPECT_FALSE(IsTraversableSchema(DataItem(schema::kInt32)));
  EXPECT_FALSE(IsTraversableSchema(DataItem(schema::kString)));
  EXPECT_FALSE(IsTraversableSchema(DataItem(schema::kItemId)));
  EXPECT_FALSE(IsTraversableSchema(DataItem(schema::kNone)));
  EXPECT_FALSE(IsTraversableSchema(DataItem()));
}

TEST(VectorizedTraverserUtilsTest, ChildItems) {
  auto items = Slice({DataItem(10), DataItem(20), DataItem(30)});
  ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 2, 2, 3})));
  EXPECT_THAT(ChildItems(items, edge, 0),
              ElementsAre(DataItem(10), DataItem(20)));
  DataSliceImpl empty_child = ChildItems(items, edge, 1);
  EXPECT_THAT(empty_child, ElementsAre());
  EXPECT_TRUE(empty_child.is_empty_and_unknown());
  EXPECT_THAT(ChildItems(items, edge, 2), ElementsAre(DataItem(30)));

  // Single parent covering the whole `items` slice.
  ASSERT_OK_AND_ASSIGN(auto full_edge, DenseArrayEdge::FromSplitPoints(
                                           CreateDenseArray<int64_t>({0, 3})));
  EXPECT_THAT(ChildItems(items, full_edge, 0),
              ElementsAre(DataItem(10), DataItem(20), DataItem(30)));
}

}  // namespace
}  // namespace koladata::internal::vectorized_traverser_internal
