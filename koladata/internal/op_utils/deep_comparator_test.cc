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
#include "koladata/internal/op_utils/deep_comparator.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_clone.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

class EqualEntityPrimitivesComparator : public AbstractComparator {
 public:
  explicit EqualEntityPrimitivesComparator()
      : status_(absl::OkStatus()),
        nodes_count_(),
        parent_transitions_(),
        diffs_() {}

  absl::StatusOr<DataItem> CreateToken(
      TraverseHelper::Transition lhs, TraverseHelper::Transition rhs) override {
    return DataItem(nodes_count_++);
  }
  void UpdateParent(int64_t child, int64_t parent,
                    TraverseHelper::TransitionKey key,
                    bool forced_update = false) {
    if (forced_update) {
      parent_transitions_.insert_or_assign(child, std::make_pair(parent, key));
    } else {
      parent_transitions_.emplace(child, std::make_pair(parent, key));
    }
  }
  absl::Status LhsRhsMatch(DataItem parent, TraverseHelper::TransitionKey key,
                           DataItem child) override {
    UpdateParent(child.value<int64_t>(), parent.value<int64_t>(), key);
    return absl::OkStatus();
  }
  absl::Status LhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition lhs) override {
    diffs_.push_back({token.value<int64_t>(), key});
    return absl::OkStatus();
  }
  absl::Status RhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition rhs) override {
    diffs_.push_back({token.value<int64_t>(), key});
    return absl::OkStatus();
  }
  absl::Status LhsRhsMismatch(DataItem token, TraverseHelper::TransitionKey key,
                              TraverseHelper::Transition lhs,
                              TraverseHelper::Transition rhs) override {
                                diffs_.push_back({token.value<int64_t>(), key});
    return absl::OkStatus();
  }
  int CompareOrder(TraverseHelper::TransitionKey lhs,
                   TraverseHelper::TransitionKey rhs) override {
    if (lhs.type != rhs.type) {
      return lhs.type < rhs.type ? -1 : 1;
    }
    if (lhs.type != TraverseHelper::TransitionType::kAttributeName) {
      // We only order attribute names.
      if (status_.ok()) {
        status_ = absl::InternalError("unexpected transition type");
      }
      return 0;
    }
    auto lhs_attr_name = lhs.value.value<arolla::Text>();
    auto rhs_attr_name = rhs.value.value<arolla::Text>();
    if (lhs_attr_name == rhs_attr_name) {
      return 0;
    }
    return lhs_attr_name < rhs_attr_name ? -1 : 1;
  }
  bool Equal(TraverseHelper::Transition lhs,
             TraverseHelper::Transition rhs) override {
    if (lhs.schema.is_primitive_schema() || rhs.schema.is_primitive_schema()) {
      return lhs.schema.is_primitive_schema() &&
             rhs.schema.is_primitive_schema() && lhs.item == rhs.item;
    }
    return true;
  }
  std::vector<std::string> GetDiffPaths() {
    std::vector<std::string> result;
    for (const auto& [token, key] : diffs_) {
      auto path = GetTransitionKeys(token);
      path.push_back(key);
      result.push_back(TraverseHelper::TransitionKeySequenceToAccessPath(
          absl::MakeSpan(path)));
    }
    return result;
  }
  absl::Status GetStatus() { return status_; }

 private:
  std::vector<TraverseHelper::TransitionKey> GetTransitionKeys(int64_t token) {
    std::vector<TraverseHelper::TransitionKey> result;
    while (parent_transitions_.contains(token)) {
      auto [parent, key] = parent_transitions_[token];
      result.push_back(key);
      token = parent;
    }
    std::reverse(result.begin(), result.end());
    return result;
  }

  absl::Status status_;
  int64_t nodes_count_;
  absl::flat_hash_map<int64_t,
                      std::pair<int64_t, TraverseHelper::TransitionKey>>
      parent_transitions_;
  std::vector<std::pair<int64_t, TraverseHelper::TransitionKey>> diffs_;
};

absl::StatusOr<std::vector<std::string>> DeepCompare(
    const DataSliceImpl& lhs_ds, const DataItem& lhs_schema,
    const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
    const DataSliceImpl& rhs_ds, const DataItem& rhs_schema,
    const DataBagImpl& rhs_databag,
    DataBagImpl::FallbackSpan rhs_fallbacks) {
  auto comparator = std::make_unique<EqualEntityPrimitivesComparator>();
  auto lhs_traverse_helper = TraverseHelper(lhs_databag, lhs_fallbacks);
  auto rhs_traverse_helper = TraverseHelper(rhs_databag, rhs_fallbacks);
  auto compare_op = DeepComparator<EqualEntityPrimitivesComparator>(
      lhs_traverse_helper, rhs_traverse_helper, std::move(comparator));
  ASSIGN_OR_RETURN(auto tokens, compare_op.CompareSlices(lhs_ds, lhs_schema,
                                                         rhs_ds, rhs_schema));
  for (int64_t i = 0; i < tokens.size(); ++i) {
    // In CompareSlices we build a directed spanning forest over the tokens.
    // Here we add a fake root node, and additional transition to all the
    // starting tokens.
    compare_op.Comparator().UpdateParent(
        tokens[i].value<int64_t>(), -1,
        TraverseHelper::TransitionKey(
            {.type = TraverseHelper::TransitionType::kSliceItem, .index = i}),
        /*forced_update=*/true);
  }
  RETURN_IF_ERROR(compare_op.Comparator().GetStatus());
  return compare_op.Comparator().GetDiffPaths();
};

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

class DeepEqualTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, DeepEqualTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(DeepEqualTest, EmptySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();

  ASSERT_OK_AND_ASSIGN(
      auto result,
      DeepCompare(ds, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, ds,
                  schema, *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_THAT(result, ::testing::IsEmpty());
}

TEST_P(DeepEqualTest, LhsRhsMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", a1}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"x", DataItem(0)}}},
                           {b1, {{"x", DataItem(1)}}},
                           {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(auto cloned_ds, DeepCloneOp(cloned_db.get())(
                                            ds, schema_a, *GetMainDb(db),
                                            {GetFallbackDb(db).get()}));
  TriplesT update_triples = {{a1, {{"b", b2}}}};
  SetDataTriples(*db, update_triples);

  ASSERT_OK_AND_ASSIGN(
      auto result,
      DeepCompare(ds, schema_a, *GetMainDb(db), {GetFallbackDb(db).get()},
                  cloned_ds, schema_a, *cloned_db, {}));

  EXPECT_THAT(result, ::testing::ElementsAre(".S[1].b.x"));
}

TEST_P(DeepEqualTest, OneSideTrailingAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", a1}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"x", DataItem(0)}}},
                           {b1, {{"x", DataItem(1)}}},
                           {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_ds, DeepCloneOp(cloned_db.get())(ds, schema_a, *GetMainDb(db),
                                                   {GetFallbackDb(db).get()}));
  TriplesT update_schema_triples = {
      {schema_b, {{"y", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, update_schema_triples);

  {
    // LhsOnly
    ASSERT_OK_AND_ASSIGN(
        auto result,
        DeepCompare(ds, schema_a, *GetMainDb(db), {GetFallbackDb(db).get()},
                    cloned_ds, schema_a, *cloned_db, {}));

    EXPECT_THAT(result, ::testing::UnorderedElementsAre(
                            ".S[0].b.y", ".S[1].b.y", ".S[2].b.y"));
  }
  {
    // RhsOnly
    ASSERT_OK_AND_ASSIGN(
        auto result,
        DeepCompare(cloned_ds, schema_a, *cloned_db, {}, ds, schema_a,
                    *GetMainDb(db), {GetFallbackDb(db).get()}));

    EXPECT_THAT(result, ::testing::UnorderedElementsAre(
                            ".S[0].b.y", ".S[1].b.y", ".S[2].b.y"));
  }
}

TEST_P(DeepEqualTest, OneSideAttribute) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", a1}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"x", DataItem(0)}}},
                           {b1, {{"x", DataItem(1)}}},
                           {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_ds, DeepCloneOp(cloned_db.get())(ds, schema_a, *GetMainDb(db),
                                                   {GetFallbackDb(db).get()}));
  TriplesT update_schema_triples = {
      {schema_a, {{"data", DataItem(schema::kString)}}}};
  SetSchemaTriples(*db, update_schema_triples);

  {
    // LhsOnly
    ASSERT_OK_AND_ASSIGN(
        auto result,
        DeepCompare(ds, schema_a, *GetMainDb(db), {GetFallbackDb(db).get()},
                    cloned_ds, schema_a, *cloned_db, {}));

    EXPECT_THAT(result, ::testing::UnorderedElementsAre(
                            ".S[0].data", ".S[1].data", ".S[2].data"));
  }
  {
    // RhsOnly
    ASSERT_OK_AND_ASSIGN(
        auto result,
        DeepCompare(cloned_ds, schema_a, *cloned_db, {}, ds, schema_a,
                    *GetMainDb(db), {GetFallbackDb(db).get()}));

    EXPECT_THAT(result, ::testing::UnorderedElementsAre(
                            ".S[0].data", ".S[1].data", ".S[2].data"));
  }
}

TEST_P(DeepEqualTest, ObjectVsEntity) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(12);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto c0 = obj_ids[6];
  auto c1 = obj_ids[7];
  auto c2 = obj_ids[8];
  auto d0 = obj_ids[9];
  auto d1 = obj_ids[10];
  auto d2 = obj_ids[11];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto object_ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({c0, c1, c2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  auto schema_c = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", a1}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"x", DataItem(0)}}},
                           {b1, {{"x", DataItem(1)}}},
                           {b2, {{"x", DataItem(2)}}}};
  TriplesT object_data_triples = {
      {c0, {{schema::kSchemaAttr, schema_c}, {"self", c0}, {"b", d0}}},
      {c1, {{schema::kSchemaAttr, schema_c}, {"self", c1}, {"b", d1}}},
      {c2, {{schema::kSchemaAttr, schema_c}, {"self", c2}, {"b", d2}}},
      {d0, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(3)}}},
      {d1, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(4)}}},
      {d2, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(2)}}}};
  TriplesT schema_triples = {
      {schema_c,
       {{"self", DataItem(schema::kObject)}, {"b", DataItem(schema::kObject)}}},
      {schema_a, {{"self", schema_a}, {"b", schema_b}}},
      {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, object_data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(auto cloned_ds, DeepCloneOp(cloned_db.get())(
                                            ds, schema_a, *GetMainDb(db),
                                            {GetFallbackDb(db).get()}));
  TriplesT update_triples = {{a1, {{"b", b2}}}};
  SetDataTriples(*db, update_triples);

  ASSERT_OK_AND_ASSIGN(
      auto result,
      DeepCompare(ds, schema_a, *GetMainDb(db), {GetFallbackDb(db).get()},
                  object_ds, DataItem(schema::kObject), *GetMainDb(db),
                  {GetFallbackDb(db).get()}));

  EXPECT_THAT(result,
              ::testing::UnorderedElementsAre(".S[0].b.x", ".S[1].b.x"));
}

TEST_P(DeepEqualTest, PrimitiveWithObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{"self", a0}, {"b", DataItem(3)}}},
      {a1, {{"self", a1}, {"b", b1}}},
      {a2, {{"self", a2}, {"b", b2}}},
      {b0, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(0)}}},
      {b1, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(1)}}},
      {b2, {{schema::kSchemaAttr, schema_b}, {"x", DataItem(2)}}}};
  TriplesT schema_triples = {
      {schema_a, {{"self", schema_a}, {"b", DataItem(schema::kObject)}}},
      {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(auto cloned_ds, DeepCloneOp(cloned_db.get())(
                                            ds, schema_a, *GetMainDb(db),
                                            {GetFallbackDb(db).get()}));
  ASSERT_OK_AND_ASSIGN(
      auto result,
      DeepCompare(ds, schema_a, *GetMainDb(db), {GetFallbackDb(db).get()},
                  cloned_ds, schema_a, *cloned_db, {}));

  EXPECT_THAT(result, ::testing::IsEmpty());
}

}  // namespace
}  // namespace koladata::internal
