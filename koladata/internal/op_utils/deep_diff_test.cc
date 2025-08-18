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
#include "koladata/internal/op_utils/deep_diff.h"
#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/testing/matchers.h"

namespace koladata::internal {
namespace {

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;
using ::koladata::internal::testing::DataBagEqual;

using Transition = TraverseHelper::Transition;
using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

class DeepDiffTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, DeepDiffTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(DeepDiffTest, CreateTokenLike) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);

  {
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(DataItem(1)));
    EXPECT_EQ(token, DataItem(1));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(DataItem("1")));
    EXPECT_EQ(token, DataItem("1"));
  }
  {
    DataItem item = AllocateEmptyObjects(1)[0];
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(item));
    EXPECT_TRUE(token.holds_value<ObjectId>());
    EXPECT_TRUE(token.value<ObjectId>().IsEntity());
    EXPECT_NE(token, item);
  }
  {
    DataItem list = AllocateEmptyLists(1)[0];
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(list));
    EXPECT_TRUE(token.holds_value<ObjectId>());
    EXPECT_TRUE(token.value<ObjectId>().IsList());
    EXPECT_NE(token, list);
  }
  {
    DataItem dict = AllocateEmptyDicts(1)[0];
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(dict));
    EXPECT_TRUE(token.holds_value<ObjectId>());
    EXPECT_TRUE(token.value<ObjectId>().IsDict());
    EXPECT_NE(token, dict);
  }
  {
    DataItem schema = AllocateSchema();
    ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(schema));
    EXPECT_TRUE(token.holds_value<ObjectId>());
    EXPECT_TRUE(token.value<ObjectId>().IsEntity());
    EXPECT_NE(token, schema);
    ASSERT_OK_AND_ASSIGN(auto schema_attr,
                         result_db->GetObjSchemaAttr(token));
    EXPECT_TRUE(schema_attr.is_implicit_schema());
  }
}

TEST_P(DeepDiffTest, LhsOnlyAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = AllocateEmptyObjects(1)[0];
  auto value = AllocateEmptyObjects(1)[0];
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(item));
  ASSERT_OK(deep_diff.LhsOnlyAttribute(token,
                                       {.type = TransitionType::kAttributeName,
                                        .value = DataItem(arolla::Text("x"))},
                                       {value, schema}));

  ASSERT_OK_AND_ASSIGN(auto token_schema, result_db->GetObjSchemaAttr(token));
  ASSERT_OK_AND_ASSIGN(auto x, result_db->GetAttr(token, "x"));
  ASSERT_OK_AND_ASSIGN(auto x_schema, result_db->GetObjSchemaAttr(x));
  ASSERT_OK_AND_ASSIGN(auto diff,
                       result_db->GetAttr(x, DeepDiff::kDiffItemAttr));
  ASSERT_OK_AND_ASSIGN(auto diff_schema, result_db->GetObjSchemaAttr(diff));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {token, {{schema::kSchemaAttr, token_schema}, {"x", x}}},
      {x, {{schema::kSchemaAttr, x_schema}, {DeepDiff::kDiffItemAttr, diff}}},
      {diff,
       {{schema::kSchemaAttr, diff_schema}, {DeepDiff::kLhsAttr, value}}},
  };
  TriplesT expected_schema_triples = {
      {token_schema, {{"x", DataItem(schema::kObject)}}},
      {x_schema, {{DeepDiff::kDiffItemAttr, DataItem(schema::kObject)}}},
      {diff_schema, {{DeepDiff::kLhsAttr, schema}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, RhsOnlyAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = AllocateEmptyObjects(1)[0];
  auto value = AllocateEmptyObjects(1)[0];
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(item));
  ASSERT_OK(deep_diff.RhsOnlyAttribute(token,
                                       {.type = TransitionType::kAttributeName,
                                        .value = DataItem(arolla::Text("x"))},
                                       {value, schema}));

  ASSERT_OK_AND_ASSIGN(auto token_schema, result_db->GetObjSchemaAttr(token));
  ASSERT_OK_AND_ASSIGN(auto x, result_db->GetAttr(token, "x"));
  ASSERT_OK_AND_ASSIGN(auto x_schema, result_db->GetObjSchemaAttr(x));
  ASSERT_OK_AND_ASSIGN(auto diff,
                       result_db->GetAttr(x, DeepDiff::kDiffItemAttr));
  ASSERT_OK_AND_ASSIGN(auto diff_schema, result_db->GetObjSchemaAttr(diff));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {token, {{schema::kSchemaAttr, token_schema}, {"x", x}}},
      {x, {{schema::kSchemaAttr, x_schema}, {DeepDiff::kDiffItemAttr, diff}}},
      {diff,
       {{schema::kSchemaAttr, diff_schema}, {DeepDiff::kRhsAttr, value}}},
  };
  TriplesT expected_schema_triples = {
      {token_schema, {{"x", DataItem(schema::kObject)}}},
      {x_schema, {{DeepDiff::kDiffItemAttr, DataItem(schema::kObject)}}},
      {diff_schema, {{DeepDiff::kRhsAttr, schema}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, LhsRhsMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = AllocateEmptyObjects(1)[0];
  auto lhs_value = AllocateEmptyObjects(1)[0];
  auto rhs_value = AllocateEmptyObjects(1)[0];
  auto lhs_schema = AllocateSchema();
  auto rhs_schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(item));
  ASSERT_OK(deep_diff.LhsRhsMismatch(token,
                                     {.type = TransitionType::kAttributeName,
                                      .value = DataItem(arolla::Text("x"))},
                                     {lhs_value, lhs_schema},
                                     {rhs_value, rhs_schema},
                                     /*is_schema_mismatch=*/false));

  ASSERT_OK_AND_ASSIGN(auto token_schema, result_db->GetObjSchemaAttr(token));
  ASSERT_OK_AND_ASSIGN(auto x, result_db->GetAttr(token, "x"));
  ASSERT_OK_AND_ASSIGN(auto x_schema, result_db->GetObjSchemaAttr(x));
  ASSERT_OK_AND_ASSIGN(auto diff,
                       result_db->GetAttr(x, DeepDiff::kDiffItemAttr));
  ASSERT_OK_AND_ASSIGN(auto diff_schema, result_db->GetObjSchemaAttr(diff));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {token, {{schema::kSchemaAttr, token_schema}, {"x", x}}},
      {x, {{schema::kSchemaAttr, x_schema}, {DeepDiff::kDiffItemAttr, diff}}},
      {diff,
       {{schema::kSchemaAttr, diff_schema},
        {DeepDiff::kLhsAttr, lhs_value},
        {DeepDiff::kRhsAttr, rhs_value}}},
  };
  TriplesT expected_schema_triples = {
      {token_schema, {{"x", DataItem(schema::kObject)}}},
      {x_schema, {{DeepDiff::kDiffItemAttr, DataItem(schema::kObject)}}},
      {diff_schema,
       {{DeepDiff::kLhsAttr, lhs_schema}, {DeepDiff::kRhsAttr, rhs_schema}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, LhsRhsMismatchObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = AllocateEmptyObjects(1)[0];
  auto lhs_value = AllocateEmptyObjects(1)[0];
  auto rhs_value = AllocateEmptyObjects(1)[0];
  auto lhs_schema = AllocateSchema();
  auto rhs_schema = DataItem(schema::kObject);
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto token, deep_diff.CreateTokenLike(item));
  ASSERT_OK(deep_diff.LhsRhsMismatch(token,
                                     {.type = TransitionType::kAttributeName,
                                      .value = DataItem(arolla::Text("x"))},
                                     {lhs_value, lhs_schema},
                                     {rhs_value, rhs_schema},
                                     /*is_schema_mismatch=*/false));

  ASSERT_OK_AND_ASSIGN(auto token_schema, result_db->GetObjSchemaAttr(token));
  ASSERT_OK_AND_ASSIGN(auto x, result_db->GetAttr(token, "x"));
  ASSERT_OK_AND_ASSIGN(auto x_schema, result_db->GetObjSchemaAttr(x));
  ASSERT_OK_AND_ASSIGN(auto diff,
                       result_db->GetAttr(x, DeepDiff::kDiffItemAttr));
  ASSERT_OK_AND_ASSIGN(auto diff_schema, result_db->GetObjSchemaAttr(diff));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {token, {{schema::kSchemaAttr, token_schema}, {"x", x}}},
      {x, {{schema::kSchemaAttr, x_schema}, {DeepDiff::kDiffItemAttr, diff}}},
      {diff,
       {{schema::kSchemaAttr, diff_schema},
        {DeepDiff::kLhsAttr, lhs_value},
        {DeepDiff::kRhsAttr, rhs_value}}},
      {rhs_value, {{schema::kSchemaAttr, DataItem(schema::kItemId)}}},
  };
  TriplesT expected_schema_triples = {
      {token_schema, {{"x", DataItem(schema::kObject)}}},
      {x_schema, {{DeepDiff::kDiffItemAttr, DataItem(schema::kObject)}}},
      {diff_schema,
       {{DeepDiff::kLhsAttr, lhs_schema}, {DeepDiff::kRhsAttr, rhs_schema}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, SliceItemMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lhs_value = AllocateEmptyObjects(1)[0];
  auto rhs_value = AllocateEmptyObjects(1)[0];
  auto lhs_schema = AllocateSchema();
  auto rhs_schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto item,
                       deep_diff.SliceItemMismatch(
                           {.type = TransitionType::kSliceItem, .index = 2},
                           {lhs_value, lhs_schema}, {rhs_value, rhs_schema},
                           /*is_schema_mismatch=*/false));
  ASSERT_OK_AND_ASSIGN(auto item_schema, result_db->GetObjSchemaAttr(item));
  ASSERT_OK_AND_ASSIGN(auto diff,
                       result_db->GetAttr(item, DeepDiff::kDiffItemAttr));
  ASSERT_OK_AND_ASSIGN(auto diff_schema, result_db->GetObjSchemaAttr(diff));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {item,
       {{schema::kSchemaAttr, item_schema}, {DeepDiff::kDiffItemAttr, diff}}},
      {diff,
       {{schema::kSchemaAttr, diff_schema},
        {DeepDiff::kLhsAttr, lhs_value},
        {DeepDiff::kRhsAttr, rhs_value}}},
  };
  TriplesT expected_schema_triples = {
      {item_schema, {{DeepDiff::kDiffItemAttr, DataItem(schema::kObject)}}},
      {diff_schema,
       {{DeepDiff::kLhsAttr, lhs_schema}, {DeepDiff::kRhsAttr, rhs_schema}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, SaveTransitionAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto from_item = AllocateEmptyObjects(1)[0];
  auto to_item = AllocateEmptyObjects(1)[0];
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto from_token, deep_diff.CreateTokenLike(from_item));
  ASSERT_OK_AND_ASSIGN(auto to_token, deep_diff.CreateTokenLike(to_item));
  ASSERT_OK(deep_diff.SaveTransition(from_token,
                                     {.type = TransitionType::kAttributeName,
                                      .value = DataItem(arolla::Text("x"))},
                                     to_token));

  ASSERT_OK_AND_ASSIGN(auto from_token_schema,
                       result_db->GetObjSchemaAttr(from_token));
  ASSERT_OK_AND_ASSIGN(auto to_token_schema,
                       result_db->GetObjSchemaAttr(to_token));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {from_token, {{schema::kSchemaAttr, from_token_schema}, {"x", to_token}}},
      {to_token, {{schema::kSchemaAttr, to_token_schema}}},
  };
  TriplesT expected_schema_triples = {
      {from_token_schema, {{"x", DataItem(schema::kObject)}}},
  };
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, SaveTransitionSchemaAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto from_item = AllocateSchema();
  auto to_item = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto from_token, deep_diff.CreateTokenLike(from_item));
  ASSERT_OK_AND_ASSIGN(auto to_token, deep_diff.CreateTokenLike(to_item));
  ASSERT_OK(
      deep_diff.SaveTransition(from_token,
                               {.type = TransitionType::kSchemaAttributeName,
                                .value = DataItem(arolla::Text("x"))},
                               to_token));

  ASSERT_OK_AND_ASSIGN(auto from_token_schema,
                       result_db->GetObjSchemaAttr(from_token));
  ASSERT_OK_AND_ASSIGN(auto to_token_schema,
                       result_db->GetObjSchemaAttr(to_token));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  std::string x_with_prefix = absl::StrCat(DeepDiff::kSchemaAttrPrefix, "x");
  TriplesT expected_data_triples = {
      {from_token,
       {{schema::kSchemaAttr, from_token_schema}, {x_with_prefix, to_token}}},
      {to_token, {{schema::kSchemaAttr, to_token_schema}}},
  };
  TriplesT expected_schema_triples = {
      {from_token_schema, {{x_with_prefix, DataItem(schema::kObject)}}},
  };
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, SaveTransitionDict) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto from_item = AllocateEmptyDicts(1)[0];
  auto to_item = AllocateEmptyObjects(1)[0];
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto from_token, deep_diff.CreateTokenLike(from_item));
  ASSERT_OK_AND_ASSIGN(auto to_token, deep_diff.CreateTokenLike(to_item));
  ASSERT_OK(deep_diff.SaveTransition(from_token,
                                     {.type = TransitionType::kDictValue,
                                      .value = DataItem(arolla::Text("x"))},
                                     to_token));

  ASSERT_OK_AND_ASSIGN(auto from_token_schema,
                       result_db->GetObjSchemaAttr(from_token));
  ASSERT_OK_AND_ASSIGN(auto to_token_schema,
                       result_db->GetObjSchemaAttr(to_token));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(from_token, DataItem(arolla::Text("x")),
                                   to_token));
  TriplesT expected_data_triples = {
      {from_token, {{schema::kSchemaAttr, from_token_schema}}},
      {to_token, {{schema::kSchemaAttr, to_token_schema}}},
  };
  TriplesT expected_schema_triples = {
      {from_token_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kObject)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kObject)}}},
  };
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(DeepDiffTest, SaveTransitionList) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto from_item = AllocateEmptyLists(1)[0];
  auto to_item = AllocateEmptyObjects(1)[0];
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_diff = DeepDiff(result_db);
  ASSERT_OK_AND_ASSIGN(auto from_token, deep_diff.CreateTokenLike(from_item));
  ASSERT_OK_AND_ASSIGN(auto to_token, deep_diff.CreateTokenLike(to_item));
  ASSERT_OK(deep_diff.SaveTransition(from_token,
                                     {.type = TransitionType::kListItem,
                                      .index = 4},
                                     to_token));

  ASSERT_OK_AND_ASSIGN(auto from_token_schema,
                       result_db->GetObjSchemaAttr(from_token));
  ASSERT_OK_AND_ASSIGN(auto to_token_schema,
                       result_db->GetObjSchemaAttr(to_token));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->ExtendList(
      from_token, DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
                      {std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                       to_token.value<ObjectId>()}))));
  TriplesT expected_data_triples = {
      {from_token, {{schema::kSchemaAttr, from_token_schema}}},
      {to_token, {{schema::kSchemaAttr, to_token_schema}}},
  };
  TriplesT expected_schema_triples = {
      {from_token_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kObject)}}},
  };
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

}  // namespace
}  // namespace koladata::internal
