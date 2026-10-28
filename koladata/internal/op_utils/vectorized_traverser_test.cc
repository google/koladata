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
#include "koladata/internal/op_utils/vectorized_traverser.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/abstract_visitor.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/test_utils.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::koladata::internal::testing::deep_op_utils::DeepOpTest;
using ::koladata::internal::testing::deep_op_utils::test_param_values;
using ::testing::HasSubstr;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

constexpr float NaN = std::numeric_limits<float>::quiet_NaN();

using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

class NoOpVisitor : public AbstractVisitor {
 public:
  using ItemWithSchema = std::pair<DataItem, DataItem>;

  explicit NoOpVisitor(std::vector<ItemWithSchema> skip = {},
                       bool check_topological_order = true)
      : skip_(std::move(skip)),
        check_topological_order_(check_topological_order),
        value_item_(DataItem("get value result")) {}

  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    if (!schema.is_schema()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("%v is not a schema", schema));
    }
    SliceBuilder tokens_builder(ds.size());
    std::optional<ObjectId> token;
    for (int64_t i = 0; i < ds.size(); ++i) {
      if (!ds.present(i)) {
        continue;
      }
      DataItem item = ds[i];
      if (std::find(skip_.begin(), skip_.end(), std::make_pair(item, schema)) !=
          skip_.end()) {
        continue;
      }
      previsited_.push_back({item, schema});
      if (item.holds_value<ObjectId>()) {
        if (!token.has_value()) {
          token = item.value<ObjectId>();
        }
        tokens_builder.InsertIfNotSetAndUpdateAllocIds(i, DataItem(*token));
      }
    }
    return std::move(tokens_builder).Build();
  }

  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token) override {
    SliceBuilder tokens_builder(objects.size());
    for (int64_t i = 0; i < objects.size(); ++i) {
      DataItem item = objects[i];
      DataItem schema = schemas[i];
      if (!schema.is_schema()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("%v is not a schema", schema));
      }
      if (std::find(skip_.begin(), skip_.end(), std::make_pair(item, schema)) !=
          skip_.end()) {
        continue;
      }
      previsited_.push_back({item, schema});
      tokens_builder.InsertIfNotSetAndUpdateAllocIds(i, token);
    }
    return std::move(tokens_builder).Build();
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    if (!item.holds_value<ObjectId>()) {
      return value_item_;
    }
    if (!IsPrevisited(item, schema)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "item %v with schema %v is not previsited", item, schema));
    }
    if (check_topological_order_ && schema != schema::kItemId &&
        !IsVisited(item, schema)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "item %v with schema %v is not visited before GetValue", item,
          schema));
    }
    return value_item_;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    DCHECK(list.holds_value<ObjectId>() && list.value<ObjectId>().IsList());
    RETURN_IF_ERROR(RecordVisit(list, schema, is_object_schema));
    list_sizes_[list.value<ObjectId>()] = items.size();
    return CheckValues(items);
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    RETURN_IF_ERROR(RecordVisit(dict, schema, is_object_schema));
    if (keys.size() != values.size()) {
      return absl::InternalError(
          absl::StrFormat("dict %v has keys.size()=%d != values.size()=%d",
                          dict, keys.size(), values.size()));
    }
    dict_sizes_[dict.value<ObjectId>()] = keys.size();
    RETURN_IF_ERROR(CheckValues(keys));
    return CheckValues(values);
  }

  absl::Status VisitObject(const DataItem& object, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_values) override {
    DCHECK(object.holds_value<ObjectId>());
    RETURN_IF_ERROR(RecordVisit(object, schema, is_object_schema));
    return CheckValues(attr_values);
  }

  absl::Status VisitSchema(const DataItem& item, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_schema) override {
    return VisitObject(item, schema, is_object_schema, attr_names, attr_schema);
  }

  absl::Status CheckAllPrevisitedVisited() const {
    for (const auto& [item, schema] : previsited_) {
      if (item.holds_value<ObjectId>() && schema != schema::kItemId &&
          !IsVisited(item, schema)) {
        return absl::InternalError(absl::StrFormat(
            "previsited item %v with schema %v was not visited", item, schema));
      }
    }
    return absl::OkStatus();
  }

  const absl::flat_hash_map<ObjectId, size_t>& list_sizes() const {
    return list_sizes_;
  }

  const absl::flat_hash_map<ObjectId, size_t>& dict_sizes() const {
    return dict_sizes_;
  }

 private:
  bool IsPrevisited(const DataItem& item, const DataItem& schema) const {
    return std::find(previsited_.begin(), previsited_.end(),
                     std::make_pair(item, schema)) != previsited_.end();
  }

  bool IsVisited(const DataItem& item, const DataItem& schema) const {
    return std::find(visited_.begin(), visited_.end(),
                     std::make_pair(item, schema)) != visited_.end();
  }

  absl::Status RecordVisit(const DataItem& item, const DataItem& schema,
                           bool is_object_schema) {
    if (!IsPrevisited(item, schema)) {
      return absl::InternalError(absl::StrFormat(
          "visited item %v with schema %v was not previsited", item, schema));
    }
    if (IsVisited(item, schema)) {
      return absl::InternalError(absl::StrFormat(
          "item %v with schema %v was visited more than once", item, schema));
    }
    bool expected_is_object_schema =
        schema != schema::kSchema &&
        IsPrevisited(item, DataItem(schema::kObject));
    if (is_object_schema != expected_is_object_schema) {
      return absl::InternalError(absl::StrFormat(
          "item %v with schema %v has is_object_schema=%v, expected %v", item,
          schema, is_object_schema, expected_is_object_schema));
    }
    visited_.push_back({item, schema});
    if (is_object_schema) {
      visited_.push_back({item, DataItem(schema::kObject)});
    }
    return absl::OkStatus();
  }

  template <typename ItemsT>
  absl::Status CheckValues(const ItemsT& items) {
    for (size_t i = 0; i < items.size(); ++i) {
      const DataItem& item = items[i];
      if (item.holds_value<ObjectId>() || item.holds_value<schema::DType>()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expected result of GetValue call, got %v", item));
      }
    }
    return absl::OkStatus();
  }

  std::vector<ItemWithSchema> previsited_;
  std::vector<ItemWithSchema> visited_;
  std::vector<ItemWithSchema> skip_;
  absl::flat_hash_map<ObjectId, size_t> list_sizes_;
  absl::flat_hash_map<ObjectId, size_t> dict_sizes_;
  bool check_topological_order_;
  DataItem value_item_;
};

class ObjectVisitor : public AbstractVisitor {
 public:
  explicit ObjectVisitor() = default;

  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    for (int64_t i = 0; i < ds.size(); ++i) {
      if (!ds.present(i) || !ds[i].holds_value<ObjectId>()) {
        continue;
      }
      ObjectId id = ds[i].value<ObjectId>();
      if (schema == schema::kObject) {
        previsited_objects_.insert(id);
      } else if (schema.holds_value<ObjectId>()) {
        if (!previsited_objects_.contains(id)) {
          return absl::InternalError(absl::StrFormat(
              "object %v is previsited with schema %v first", ds[i], schema));
        }
        previsited_with_schema_.insert(id);
      }
    }
    return SameTokenForAllObjects(ds);
  }

  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token) override {
    for (int64_t i = 0; i < objects.size(); ++i) {
      ObjectId id = objects[i].value<ObjectId>();
      if (!previsited_objects_.contains(id)) {
        return absl::InternalError(
            absl::StrFormat("object %v is previsited with schema %v first",
                            objects[i], schemas[i]));
      }
      if (schemas[i].holds_value<ObjectId>()) {
        previsited_with_schema_.insert(id);
      }
    }
    return DataSliceImpl::Create(objects.size(), token);
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    return item;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    return absl::OkStatus();
  }

  absl::Status VisitObject(const DataItem& object, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_values) override {
    return absl::OkStatus();
  }

  absl::Status VisitSchema(const DataItem& item, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_schema) override {
    return absl::OkStatus();
  }

  absl::Status check_previsited_objects_twice() const {
    if (previsited_objects_.size() != previsited_with_schema_.size()) {
      return absl::InternalError("not all objects are previsited twice");
    }
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_set<ObjectId> previsited_objects_;
  absl::flat_hash_set<ObjectId> previsited_with_schema_;
};

class SaveTransitionKeysVisitor : public AbstractVisitor {
 public:
  struct PrevisitRecord {
    DataItem from_token;
    std::optional<AbstractVisitor::TransitionKey> transition_key;
    DataItem item;
    DataItem schema;

    friend bool operator==(const PrevisitRecord&,
                           const PrevisitRecord&) = default;
  };

  explicit SaveTransitionKeysVisitor() = default;

  // Records all previsited items, and returns a distinct token (the item
  // itself) for each ObjectId.
  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    SliceBuilder tokens_builder(ds.size());
    for (int64_t i = 0; i < ds.size(); ++i) {
      if (!ds.present(i)) {
        continue;
      }
      DataItem item = ds[i];
      records_.push_back({parent_token, transition_key, item, schema});
      if (item.holds_value<ObjectId>()) {
        tokens_builder.InsertIfNotSetAndUpdateAllocIds(i, item);
      }
    }
    return std::move(tokens_builder).Build();
  }

  // Records all previsited objects with the kObjectSchema transition, and
  // returns a distinct token (the object itself) for each object.
  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token) override {
    for (int64_t i = 0; i < objects.size(); ++i) {
      records_.push_back({token,
                          TransitionKey{.type = TransitionType::kObjectSchema},
                          objects[i], schemas[i]});
    }
    return objects;
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    return item;
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    return absl::OkStatus();
  }

  absl::Status VisitObject(const DataItem& object, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_values) override {
    return absl::OkStatus();
  }

  absl::Status VisitSchema(const DataItem& item, const DataItem& schema,
                           bool is_object_schema,
                           absl::Span<const absl::string_view> attr_names,
                           absl::Span<const DataItem> attr_schema) override {
    return absl::OkStatus();
  }

  const std::vector<PrevisitRecord>& records() const { return records_; }

 protected:
  std::vector<PrevisitRecord> records_;
};

class VectorizedTraverserTest : public DeepOpTest {
 protected:
  absl::Status TraverseSlice(const DataSliceImpl& ds, const DataItem& schema,
                             const DataBagImplPtr& db,
                             std::shared_ptr<AbstractVisitor> visitor) {
    auto main_db = GetMainDb(db);
    auto fallback_db = GetFallbackDb(db);
    std::vector<const DataBagImpl*> fallbacks = {fallback_db.get()};
    VectorizedTraverser traverser(*main_db, fallbacks, std::move(visitor));
    return traverser.TraverseSlice(ds, schema);
  }

  absl::Status TraverseSlice(const DataSliceImpl& ds, const DataItem& schema,
                             const DataBagImplPtr& db,
                             std::shared_ptr<NoOpVisitor> visitor = nullptr) {
    if (visitor == nullptr) {
      visitor = std::make_shared<NoOpVisitor>();
    }
    RETURN_IF_ERROR(TraverseSlice(
        ds, schema, db, std::static_pointer_cast<AbstractVisitor>(visitor)));
    return visitor->CheckAllPrevisitedVisited();
  }

  absl::Status TraverseSliceCheckObjectPrevisits(const DataSliceImpl& ds,
                                                 const DataItem& schema,
                                                 const DataBagImplPtr& db) {
    auto visitor = std::make_shared<ObjectVisitor>();
    RETURN_IF_ERROR(TraverseSlice(
        ds, schema, db, std::static_pointer_cast<AbstractVisitor>(visitor)));
    return visitor->check_previsited_objects_twice();
  }
};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, VectorizedTraverserTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(VectorizedTraverserTest, PrevisitTransitionKeys) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto objs = AllocateEmptyObjects(2);
  auto a0 = objs[0];
  auto child = objs[1];
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();

  SetSchemaTriples(*db, {{schema, {{"child", child_schema}}},
                         {child_schema, {{"val", DataItem(schema::kInt32)}}}});
  SetDataTriples(*db,
                 {{a0, {{"child", child}}}, {child, {{"val", DataItem(42)}}}});
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a0}));
  ASSERT_OK(TraverseSlice(ds, schema, db, visitor));

  using PrevisitRecord = SaveTransitionKeysVisitor::PrevisitRecord;
  std::vector<PrevisitRecord> expected_records = {
      {.from_token = DataItem(),
       .transition_key = std::nullopt,
       .item = schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = DataItem(),
       .transition_key = TransitionKey{.type = TransitionType::kSliceItem},
       .item = a0,
       .schema = schema},
      {.from_token = a0,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = a0,
       .transition_key =
           TransitionKey{.type = TransitionType::kAttributeName,
                         .value = DataItem(arolla::Text("child"))},
       .item = child,
       .schema = child_schema},
      {.from_token = child,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = child_schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = child_schema,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
      {.from_token = schema,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
      {.from_token = schema,
       .transition_key =
           TransitionKey{.type = TransitionType::kSchemaAttributeName,
                         .value = DataItem(arolla::Text("child"))},
       .item = child_schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = child_schema,
       .transition_key =
           TransitionKey{.type = TransitionType::kSchemaAttributeName,
                         .value = DataItem(arolla::Text("val"))},
       .item = DataItem(schema::kInt32),
       .schema = DataItem(schema::kSchema)},
  };

  EXPECT_THAT(visitor->records(),
              ::testing::UnorderedElementsAreArray(expected_records));
}

TEST_P(VectorizedTraverserTest, ShallowEntitySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
                           {a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
                           {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  EXPECT_OK(TraverseSlice(obj_ids, schema, db));
}

TEST_P(VectorizedTraverserTest, DeepEntitySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", DataItem()}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"self", b0}}},
                           {b1, {{"self", b1}}},
                           {b2, {{"self", b2}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"self", schema_b}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>(
      /*skip=*/std::vector<NoOpVisitor::ItemWithSchema>{},
      /*check_topological_order=*/false);
  EXPECT_OK(TraverseSlice(ds, schema_a, db, visitor));
}

TEST_P(VectorizedTraverserTest, SkipTraversing) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", DataItem()}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"self", b0}}},
                           {b1, {{"self", b1}}},
                           {b2, {{"self", b2}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"self", schema_b}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>(
      std::vector<NoOpVisitor::ItemWithSchema>{{a2, schema_a}},
      /*check_topological_order=*/false);
  EXPECT_OK(TraverseSlice(ds, schema_a, db, visitor));

  ASSERT_OK_AND_ASSIGN(auto value, visitor->GetValue(b0, schema_b));
  EXPECT_EQ(value, DataItem("get value result"));
  EXPECT_THAT(visitor->GetValue(b2, schema_b),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("is not previsited")));
}

TEST_P(VectorizedTraverserTest, ShallowListsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(4);
  auto values =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));
  auto edge = test::EdgeFromSplitPoints({0, 3, 5, 7, 7});
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>();
  EXPECT_OK(TraverseSlice(lists, list_schema, db, visitor));
  EXPECT_THAT(
      visitor->list_sizes(),
      UnorderedElementsAre(Pair(lists[0].value<ObjectId>(), 3),
                           Pair(lists[1].value<ObjectId>(), 2),
                           Pair(lists[2].value<ObjectId>(), 2),
                           Pair(lists[3].value<ObjectId>(), 0)));
}

TEST_P(VectorizedTraverserTest, DeepListsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(4);
  auto sparse_lists = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {lists[0], DataItem(), DataItem(), lists[1], lists[2], lists[3]}));
  auto values = AllocateEmptyObjects(7);
  auto sparse_values = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {values[0], DataItem(), DataItem(), values[1], values[2], values[3],
       values[4], values[5], values[6], DataItem()}));
  auto edge = test::EdgeFromSplitPoints({0, 5, 7, 10, 10});
  ASSERT_OK(db->ExtendLists(lists, sparse_values, edge));
  auto item_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  TriplesT data_triples = {
      {values[0], {{"x", DataItem(1)}}}, {values[1], {{"x", DataItem(2)}}},
      {values[2], {{"x", DataItem(3)}}}, {values[3], {{"x", DataItem(4)}}},
      {values[4], {{"x", DataItem(5)}}}, {values[5], {{"x", DataItem(6)}}},
      {values[6], {{"x", DataItem(7)}}}};
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
      {item_schema, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>();
  EXPECT_OK(TraverseSlice(sparse_lists, list_schema, db, visitor));
  EXPECT_THAT(
      visitor->list_sizes(),
      UnorderedElementsAre(Pair(lists[0].value<ObjectId>(), 5),
                           Pair(lists[1].value<ObjectId>(), 2),
                           Pair(lists[2].value<ObjectId>(), 3),
                           Pair(lists[3].value<ObjectId>(), 0)));
}

TEST_P(VectorizedTraverserTest, ShallowDictsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(4);
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 1, 5, 3, 7}));
  auto values =
      DataSliceImpl::Create(CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>();
  EXPECT_OK(TraverseSlice(dicts, dict_schema, db, visitor));
  EXPECT_THAT(
      visitor->dict_sizes(),
      UnorderedElementsAre(Pair(dicts[0].value<ObjectId>(), 3),
                           Pair(dicts[1].value<ObjectId>(), 2),
                           Pair(dicts[2].value<ObjectId>(), 2),
                           Pair(dicts[3].value<ObjectId>(), 0)));
}

TEST_P(VectorizedTraverserTest, DeepDictsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(4);
  auto sparse_dicts = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], DataItem(), DataItem(), dicts[1], dicts[2], dicts[3]}));
  auto keys = AllocateEmptyObjects(4);
  auto values = AllocateEmptyObjects(7);
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {keys[0], keys[1], keys[2], keys[0], keys[3], keys[2], keys[3]}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys_expanded, values));
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  TriplesT data_triples = {{keys[0], {{"name", DataItem("a")}}},
                           {keys[1], {{"name", DataItem("b")}}},
                           {keys[2], {{"name", DataItem("c")}}},
                           {keys[3], {{"name", DataItem("d")}}},
                           {values[0], {{"x", DataItem(1)}}},
                           {values[1], {{"x", DataItem(2)}}},
                           {values[2], {{"x", DataItem(3)}}},
                           {values[3], {{"x", DataItem(4)}}},
                           {values[4], {{"x", DataItem(5)}}},
                           {values[5], {{"x", DataItem(6)}}},
                           {values[6], {{"x", DataItem(7)}}}};
  TriplesT schema_triples = {{key_schema, {{"name", DataItem(schema::kBytes)}}},
                             {value_schema, {{"x", DataItem(schema::kInt32)}}},
                             {dict_schema,
                              {{schema::kDictKeysSchemaAttr, key_schema},
                               {schema::kDictValuesSchemaAttr, value_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto visitor = std::make_shared<NoOpVisitor>();
  EXPECT_OK(TraverseSlice(sparse_dicts, dict_schema, db, visitor));
  EXPECT_THAT(
      visitor->dict_sizes(),
      UnorderedElementsAre(Pair(dicts[0].value<ObjectId>(), 3),
                           Pair(dicts[1].value<ObjectId>(), 2),
                           Pair(dicts[2].value<ObjectId>(), 2),
                           Pair(dicts[3].value<ObjectId>(), 0)));
}

TEST_P(VectorizedTraverserTest, ObjectsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto dicts = AllocateEmptyDicts(2);
  auto lists = AllocateEmptyLists(2);
  ASSERT_OK(db->SetInDict(dicts[0], DataItem("a"), DataItem(1)));
  ASSERT_OK(db->SetInDict(dicts[1], a2, a3));
  ASSERT_OK(db->ExtendList(
      lists[0], DataSliceImpl::Create(CreateDenseArray<DataItem>({a4, a5}))));
  ASSERT_OK(db->ExtendList(
      lists[1], DataSliceImpl::Create(CreateDenseArray<int32_t>({0, 1, 2}))));
  auto item_schema = AllocateSchema();
  auto key_schema = AllocateSchema();
  auto dict0_schema = AllocateSchema();
  auto dict1_schema = AllocateSchema();
  auto list0_schema = AllocateSchema();
  auto list1_schema = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(2)}}},
      {a2, {{"name", DataItem("k0")}}},
      {a3, {{"x", DataItem(10)}}},
      {a4, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(3)}}},
      {a5, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(4)}}},
      {dicts[0], {{schema::kSchemaAttr, dict0_schema}}},
      {dicts[1], {{schema::kSchemaAttr, dict1_schema}}},
      {lists[0], {{schema::kSchemaAttr, list0_schema}}},
      {lists[1], {{schema::kSchemaAttr, list1_schema}}},
  };
  TriplesT schema_triples = {
      {item_schema, {{"x", DataItem(schema::kInt32)}}},
      {key_schema, {{"name", DataItem(schema::kBytes)}}},
      {dict0_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kBytes)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}},
      {dict1_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, item_schema}}},
      {list0_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
      {list1_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {a0, a1, DataItem(), DataItem(3), DataItem("a"), dicts[0], dicts[1],
       lists[0], lists[1]}));
  EXPECT_OK(TraverseSlice(ds, DataItem(schema::kObject), db));
}

TEST_P(VectorizedTraverserTest, SchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1, {{"x", DataItem(schema::kInt32)}}},
      {s2, {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2}));
  EXPECT_OK(TraverseSlice(ds, DataItem(schema::kSchema), db));
}

TEST_P(VectorizedTraverserTest, SchemaWithMetadata) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = AllocateEmptyObjects(5);
  auto a1 = ds[1];
  auto a4 = ds[4];
  auto schema = AllocateSchema();
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateUuidWithMainObject(schema, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(auto metadata_schema,
                       internal::CreateUuidWithMainObject<
                           internal::ObjectId::kUuidImplicitSchemaFlag>(
                           metadata, schema::kImplicitSchemaSeed));
  TriplesT data_triples = {{a1, {{"x", DataItem(1)}}},
                           {a4, {{"x", DataItem(4)}}},
                           {metadata,
                            {{schema::kSchemaAttr, metadata_schema},
                             {"order", DataItem(arolla::Text("[x]"))}}}};
  TriplesT schema_triples = {
      {schema,
       {{"x", DataItem(schema::kInt32)},
        {schema::kSchemaMetadataAttr, metadata}}},
      {metadata_schema, {{"order", DataItem(schema::kString)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  EXPECT_OK(TraverseSlice(ds, schema, db));
}

TEST_P(VectorizedTraverserTest, SliceWithNamedSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();
  ASSERT_OK_AND_ASSIGN(auto named_schema,
                       db->CreateUuSchemaFromFields(
                           absl::StrCat("__named_schema__", "foo"), {}, {}));
  TriplesT schema_triples = {
      {schema, {{"x", int_dtype}, {"y", int_dtype}}},
      {named_schema,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("foo"))},
        {"x", DataItem(schema)},
        {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", a1}, {"y", DataItem(1)}}},
                           {a1, {{"x", DataItem(2)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a0}));
  EXPECT_OK(TraverseSlice(ds, DataItem(named_schema), db));
}

TEST_P(VectorizedTraverserTest, SchemaWithNameOnly) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  ASSERT_OK_AND_ASSIGN(auto named_schema,
                       db->CreateUuSchemaFromFields(
                           absl::StrCat("__named_schema__", "foo"), {}, {}));
  TriplesT schema_triples = {
      {named_schema,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("foo"))}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a0}));
  EXPECT_OK(TraverseSlice(ds, DataItem(named_schema), db));
}

TEST_P(VectorizedTraverserTest, PrimitiveTypesMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto schema = AllocateSchema();
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}}},
                           {a1, {{"x", DataItem(2.)}}}};
  TriplesT schema_triples = {
      {schema, {{"x", DataItem(schema::kInt32)}}},
  };
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  EXPECT_THAT(
      TraverseSlice(obj_ids, schema, db),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(
              HasSubstr("during traversal, got a slice with primitive type"),
              HasSubstr("while the actual content has type"))));
}

TEST_P(VectorizedTraverserTest, TypesMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto schema = AllocateSchema();
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}}}, {a1, {{"x", a0}}}};
  TriplesT schema_triples = {
      {schema, {{"x", DataItem(schema::kInt32)}}},
  };
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  EXPECT_THAT(
      TraverseSlice(obj_ids, schema, db),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(
              HasSubstr("during traversal, got a slice with primitive type"),
              HasSubstr("while the actual content is not a primitive"))));
}

TEST_P(VectorizedTraverserTest, ObjectsSlicePrevisitedTwice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto dicts = AllocateEmptyDicts(2);
  auto lists = AllocateEmptyLists(2);
  ASSERT_OK(db->SetInDict(dicts[0], DataItem("a"), DataItem(1)));
  ASSERT_OK(db->SetInDict(dicts[1], a2, a3));
  ASSERT_OK(db->ExtendList(
      lists[0], DataSliceImpl::Create(CreateDenseArray<DataItem>({a4, a5}))));
  ASSERT_OK(db->ExtendList(
      lists[1], DataSliceImpl::Create(CreateDenseArray<int32_t>({0, 1, 2}))));
  auto item_schema = AllocateSchema();
  auto key_schema = AllocateSchema();
  auto dict0_schema = AllocateSchema();
  auto dict1_schema = AllocateSchema();
  auto list0_schema = AllocateSchema();
  auto list1_schema = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(2)}}},
      {a2, {{schema::kSchemaAttr, key_schema}, {"name", DataItem("k0")}}},
      {a3, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(10)}}},
      {a4, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(3)}}},
      {a5, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(4)}}},
      {dicts[0], {{schema::kSchemaAttr, dict0_schema}}},
      {dicts[1], {{schema::kSchemaAttr, dict1_schema}}},
      {lists[0], {{schema::kSchemaAttr, list0_schema}}},
      {lists[1], {{schema::kSchemaAttr, list1_schema}}},
  };
  TriplesT schema_triples = {
      {item_schema, {{"x", DataItem(schema::kInt32)}}},
      {key_schema, {{"name", DataItem(schema::kBytes)}}},
      {dict0_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kBytes)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}},
      {dict1_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kObject)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kObject)}}},
      {list0_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kObject)}}},
      {list1_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {a0, a1, DataItem(), DataItem(3), DataItem("a"), dicts[0], dicts[1],
       lists[0], lists[1]}));
  EXPECT_OK(
      TraverseSliceCheckObjectPrevisits(ds, DataItem(schema::kObject), db));
}

TEST_P(VectorizedTraverserTest, MixedSliceWithNaN) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto item_schema = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(NaN)}}},
      {a1,
       {{schema::kSchemaAttr, item_schema},
        {"x", DataItem(static_cast<float>(2))}}},
  };
  TriplesT schema_triples = {
      {item_schema, {{"x", DataItem(schema::kFloat32)}}},
  };
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {a0, a1, DataItem(), DataItem(3), DataItem("a")}));
  EXPECT_OK(
      TraverseSliceCheckObjectPrevisits(ds, DataItem(schema::kObject), db));
}

TEST_P(VectorizedTraverserTest,
       AllowVisitWithAndWithoutObjectSchemaSameActualSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s = AllocateSchema();
  auto parent_entity_schema = AllocateSchema();
  SetSchemaTriples(
      *db, {{s, {{"val", DataItem(schema::kInt32)}}},
            {parent_entity_schema,
             {{"as_entity", s}, {"as_object", DataItem(schema::kObject)}}}});

  auto objs = AllocateEmptyObjects(2);
  auto target = objs[0];
  auto p = objs[1];
  SetDataTriples(*db,
                 {{target, {{schema::kSchemaAttr, s}, {"val", DataItem(100)}}},
                  {p, {{"as_entity", target}, {"as_object", target}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({p}));
  // Both visits (with and without kObject) share the exact same actual schema
  // s. Must succeed!
  EXPECT_OK(TraverseSlice(ds, parent_entity_schema, db));
}

TEST_P(VectorizedTraverserTest, ConflictMultipleSchemasDetected) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  auto parent_schema = AllocateSchema();
  SetSchemaTriples(*db, {{parent_schema, {{"first", s1}, {"second", s2}}}});

  auto objs = AllocateEmptyObjects(2);
  auto shared_obj = objs[0];
  auto p = objs[1];
  // shared_obj is reachable as s1 through 'first', and as s2 through 'second'.
  SetDataTriples(*db, {{p, {{"first", shared_obj}, {"second", shared_obj}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({p}));
  // Conflict must be detected by SchemaTracker!
  EXPECT_THAT(TraverseSlice(ds, parent_schema, db),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));
}

TEST_P(VectorizedTraverserTest, MissingSchemaDetected) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = AllocateEmptyObjects(1);
  // ds[0] has no __schema__ attribute!

  EXPECT_THAT(TraverseSlice(ds, DataItem(schema::kObject), db),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing __schema__ attribute")));
}

TEST_P(VectorizedTraverserTest, PrimitivesAndEmptyInputs) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  // Primitive slice traversal succeeds:
  auto int_ds = DataSliceImpl::Create(CreateDenseArray<int>({1, 2, 3}));
  EXPECT_OK(TraverseSlice(int_ds, DataItem(schema::kInt32), db));

  // Primitive type mismatch returns error:
  EXPECT_THAT(
      TraverseSlice(int_ds, DataItem(schema::kString), db),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("during traversal, got a slice with primitive type "
                         "STRING while the actual content has type INT32")));
  EXPECT_THAT(
      TraverseSlice(AllocateEmptyObjects(1), DataItem(schema::kInt32), db),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("during traversal, got a slice with primitive type "
                         "INT32 while the actual content is not a primitive")));

  // Empty unknown slice with entity schema succeeds:
  auto empty_ds = DataSliceImpl::CreateEmptyAndUnknownType(0);
  auto schema = AllocateSchema();
  EXPECT_OK(TraverseSlice(empty_ds, schema, db));
}

TEST_P(VectorizedTraverserTest, SchemaTraversalPrevisit) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"child", child_schema}}},
                         {child_schema, {{"val", DataItem(schema::kInt32)}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({schema}));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kSchema), db, visitor));

  using PrevisitRecord = SaveTransitionKeysVisitor::PrevisitRecord;
  std::vector<PrevisitRecord> expected_records = {
      {.from_token = DataItem(),
       .transition_key = std::nullopt,
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
      {.from_token = DataItem(),
       .transition_key = TransitionKey{.type = TransitionType::kSliceItem},
       .item = schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = schema,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
      {.from_token = schema,
       .transition_key =
           TransitionKey{.type = TransitionType::kSchemaAttributeName,
                         .value = DataItem(arolla::Text("child"))},
       .item = child_schema,
       .schema = DataItem(schema::kSchema)},
      {.from_token = child_schema,
       .transition_key = TransitionKey{.type = TransitionType::kSchema},
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
      {.from_token = child_schema,
       .transition_key =
           TransitionKey{.type = TransitionType::kSchemaAttributeName,
                         .value = DataItem(arolla::Text("val"))},
       .item = DataItem(schema::kInt32),
       .schema = DataItem(schema::kSchema)},
  };

  EXPECT_THAT(visitor->records(),
              ::testing::UnorderedElementsAreArray(expected_records));
}

class NonObjectIdTokensVisitor : public SaveTransitionKeysVisitor {
 public:
  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    return DataSliceImpl::Create(ds.size(), DataItem(int64_t{1}));
  }
};

TEST_P(VectorizedTraverserTest, NonObjectIdTokensRejected) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"val", DataItem(schema::kInt32)}}}});
  auto ds = AllocateEmptyObjects(1);
  SetDataTriples(*db, {{ds[0], {{"val", DataItem(10)}}}});

  EXPECT_THAT(
      TraverseSlice(ds, schema, db,
                    std::make_shared<NonObjectIdTokensVisitor>()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("expected ObjectId tokens from PrevisitSlice")));
}

class WrongSizeObjectSchemaTokensVisitor : public SaveTransitionKeysVisitor {
 public:
  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token) override {
    return DataSliceImpl::Create(objects.size() + 1, objects[0]);
  }
};

TEST_P(VectorizedTraverserTest, WrongSizeObjectSchemaTokensRejected) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto ds = AllocateEmptyObjects(1);
  SetDataTriples(*db, {{ds[0], {{schema::kSchemaAttr, schema}}}});

  EXPECT_THAT(
      TraverseSlice(ds, DataItem(schema::kObject), db,
                    std::make_shared<WrongSizeObjectSchemaTokensVisitor>()),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("expected 1 tokens from PrevisitObjectSchemas")));
}

// Records all previsited items (as `SaveTransitionKeysVisitor`), and the sizes
// of the previsit calls. Returns the same token for all items, which keeps the
// traversal vectorized.
class VectorizedRecordingVisitor : public SaveTransitionKeysVisitor {
 public:
  struct PrevisitCall {
    std::optional<AbstractVisitor::TransitionKey> transition_key;
    size_t size;

    friend bool operator==(const PrevisitCall&, const PrevisitCall&) = default;
  };

  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    calls_.push_back({transition_key, ds.present_count()});
    RETURN_IF_ERROR(SaveTransitionKeysVisitor::PrevisitSlice(
                        ds, schema, transition_key, parent_token)
                        .status());
    return SameTokenForAllObjects(ds);
  }

  absl::StatusOr<DataSliceImpl> PrevisitObjectSchemas(
      const DataSliceImpl& objects, const DataSliceImpl& schemas,
      const DataItem& token) override {
    calls_.push_back({TransitionKey{.type = TransitionType::kObjectSchema},
                      objects.present_count()});
    RETURN_IF_ERROR(SaveTransitionKeysVisitor::PrevisitObjectSchemas(
                        objects, schemas, token)
                        .status());
    return SameTokenForAllObjects(objects);
  }

  const std::vector<PrevisitCall>& calls() const { return calls_; }

 private:
  std::vector<PrevisitCall> calls_;
};

// A previsited item without the token of its parent.
struct PrevisitedItem {
  std::optional<AbstractVisitor::TransitionKey> transition_key;
  DataItem item;
  DataItem schema;

  friend bool operator==(const PrevisitedItem&,
                         const PrevisitedItem&) = default;
};

std::vector<PrevisitedItem> PrevisitedItems(
    const std::vector<SaveTransitionKeysVisitor::PrevisitRecord>& records) {
  std::vector<PrevisitedItem> result;
  result.reserve(records.size());
  for (const auto& record : records) {
    result.push_back({record.transition_key, record.item, record.schema});
  }
  return result;
}

TransitionKey AttrKey(std::string_view attr_name) {
  return {.type = TransitionType::kAttributeName,
          .value = DataItem(arolla::Text(attr_name))};
}

TransitionKey SchemaAttrKey(std::string_view attr_name) {
  return {.type = TransitionType::kSchemaAttributeName,
          .value = DataItem(arolla::Text(attr_name))};
}

TEST_P(VectorizedTraverserTest, ObjectsWithSchemasInBigAllocation) {
  constexpr int64_t kSize = 4;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto child_schema = AllocateSchema();
  SetSchemaTriples(*db, {{child_schema, {{"val", DataItem(schema::kInt32)}}}});
  AllocationId schema_alloc = AllocateExplicitSchemas(kSize);
  ASSERT_FALSE(schema_alloc.IsSmall());
  auto objs = AllocateEmptyObjects(kSize);
  auto children = AllocateEmptyObjects(kSize);

  std::vector<PrevisitedItem> expected_items;
  for (int64_t i = 0; i < kSize; ++i) {
    DataItem s(schema_alloc.ObjectByOffset(i));
    DataItem o = objs[i];
    DataItem c = children[i];
    SetSchemaTriples(
        *db,
        {{s, {{"child", child_schema}, {"val", DataItem(schema::kInt32)}}}});
    SetDataTriples(*db, {{o,
                          {{schema::kSchemaAttr, s},
                           {"child", c},
                           {"val", DataItem(static_cast<int>(i))}}},
                         {c, {{"val", DataItem(static_cast<int>(i))}}}});
    expected_items.push_back(
        {TransitionKey{.type = TransitionType::kObjectSchema}, o, s});
    expected_items.push_back({TransitionKey{.type = TransitionType::kSchema}, s,
                              DataItem(schema::kSchema)});
    expected_items.push_back({AttrKey("child"), c, child_schema});
    expected_items.push_back(
        {SchemaAttrKey("child"), child_schema, DataItem(schema::kSchema)});
    expected_items.push_back({SchemaAttrKey("val"), DataItem(schema::kInt32),
                              DataItem(schema::kSchema)});
  }

  auto visitor = std::make_shared<VectorizedRecordingVisitor>();
  ASSERT_OK(TraverseSlice(objs, DataItem(schema::kObject), db, visitor));

  EXPECT_THAT(PrevisitedItems(visitor->records()),
              ::testing::IsSupersetOf(expected_items));
  // Objects, their schemas and their children are previsited together.
  EXPECT_THAT(
      visitor->calls(),
      ::testing::IsSupersetOf<VectorizedRecordingVisitor::PrevisitCall>({
          {TransitionKey{.type = TransitionType::kObjectSchema}, kSize},
          {TransitionKey{.type = TransitionType::kSchema}, kSize},
          {AttrKey("child"), kSize},
          {SchemaAttrKey("child"), kSize},
          {SchemaAttrKey("val"), kSize},
      }));
}

TEST_P(VectorizedTraverserTest, SchemasInBigAllocationWithCycle) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId schema_alloc = AllocateExplicitSchemas(kSize);
  ASSERT_FALSE(schema_alloc.IsSmall());
  std::vector<ObjectId> schemas;
  for (int64_t i = 0; i < kSize; ++i) {
    schemas.push_back(schema_alloc.ObjectByOffset(i));
  }
  for (int64_t i = 0; i < kSize; ++i) {
    SetSchemaTriples(*db, {{DataItem(schemas[i]),
                            {{"next", DataItem(schemas[(i + 1) % kSize])}}}});
  }

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<ObjectId>(schemas));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kSchema), db, visitor));

  using PrevisitRecord = SaveTransitionKeysVisitor::PrevisitRecord;
  std::vector<PrevisitRecord> expected_records = {
      {.from_token = DataItem(),
       .transition_key = std::nullopt,
       .item = DataItem(schema::kSchema),
       .schema = DataItem(schema::kSchema)},
  };
  for (int64_t i = 0; i < kSize; ++i) {
    DataItem s(schemas[i]);
    expected_records.push_back(
        {.from_token = DataItem(),
         .transition_key = TransitionKey{.type = TransitionType::kSliceItem},
         .item = s,
         .schema = DataItem(schema::kSchema)});
    expected_records.push_back(
        {.from_token = s,
         .transition_key = TransitionKey{.type = TransitionType::kSchema},
         .item = DataItem(schema::kSchema),
         .schema = DataItem(schema::kSchema)});
    expected_records.push_back({.from_token = s,
                                .transition_key = SchemaAttrKey("next"),
                                .item = DataItem(schemas[(i + 1) % kSize]),
                                .schema = DataItem(schema::kSchema)});
  }
  EXPECT_THAT(visitor->records(),
              ::testing::UnorderedElementsAreArray(expected_records));
}

TEST_P(VectorizedTraverserTest, DictValuesTransitionKeysIncludeKeys) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto value_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  SetSchemaTriples(*db,
                   {{dict_schema,
                     {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
                      {schema::kDictValuesSchemaAttr, value_schema}}}});
  auto dicts = AllocateEmptyDicts(1);
  DataItem dict = dicts[0];
  auto values = AllocateEmptyObjects(2);
  DataItem value_a = values[0];
  DataItem value_b = values[1];
  ASSERT_OK(db->SetInDict(dict, DataItem(arolla::Text("a")), value_a));
  ASSERT_OK(db->SetInDict(dict, DataItem(arolla::Text("b")), value_b));

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  ASSERT_OK(TraverseSlice(dicts, dict_schema, db, visitor));

  using PrevisitRecord = SaveTransitionKeysVisitor::PrevisitRecord;
  EXPECT_THAT(visitor->records(),
              ::testing::IsSupersetOf<PrevisitRecord>({
                  {.from_token = dict,
                   .transition_key =
                       TransitionKey{.type = TransitionType::kDictValue,
                                     .value = DataItem(arolla::Text("a"))},
                   .item = value_a,
                   .schema = value_schema},
                  {.from_token = dict,
                   .transition_key =
                       TransitionKey{.type = TransitionType::kDictValue,
                                     .value = DataItem(arolla::Text("b"))},
                   .item = value_b,
                   .schema = value_schema},
              }));
}

TEST_P(VectorizedTraverserTest, NoFollowEntitySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"child", child_schema}}}});
  DataItem nofollow_schema(
      CreateNoFollowWithMainObject(schema.value<ObjectId>()));
  auto objs = AllocateEmptyObjects(2);
  DataItem obj = objs[0];
  SetDataTriples(*db, {{obj, {{"child", objs[1]}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({obj}));
  ASSERT_OK(TraverseSlice(ds, nofollow_schema, db, visitor));

  // The NoFollow schema is previsited, but neither its attributes nor the
  // attributes of the object are traversed.
  using PrevisitRecord = SaveTransitionKeysVisitor::PrevisitRecord;
  EXPECT_THAT(
      visitor->records(),
      UnorderedElementsAre(
          PrevisitRecord{.from_token = DataItem(),
                         .transition_key = std::nullopt,
                         .item = nofollow_schema,
                         .schema = DataItem(schema::kSchema)},
          PrevisitRecord{.from_token = DataItem(),
                         .transition_key =
                             TransitionKey{.type = TransitionType::kSliceItem},
                         .item = obj,
                         .schema = nofollow_schema},
          PrevisitRecord{
              .from_token = obj,
              .transition_key = TransitionKey{.type = TransitionType::kSchema},
              .item = nofollow_schema,
              .schema = DataItem(schema::kSchema)},
          PrevisitRecord{
              .from_token = nofollow_schema,
              .transition_key = TransitionKey{.type = TransitionType::kSchema},
              .item = DataItem(schema::kSchema),
              .schema = DataItem(schema::kSchema)}));
}

TEST_P(VectorizedTraverserTest, DuplicateRootItems) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"next", schema}}}});
  auto objs = AllocateEmptyObjects(2);
  DataItem a = objs[0];
  DataItem b = objs[1];
  SetDataTriples(*db, {{a, {{"next", b}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a, a}));
  ASSERT_OK(TraverseSlice(ds, schema, db, visitor));

  // `a` is previsited twice in the root slice, but enqueued only once, so its
  // child `b` is previsited only once.
  EXPECT_THAT(
      PrevisitedItems(visitor->records()),
      UnorderedElementsAre(
          PrevisitedItem{std::nullopt, schema, DataItem(schema::kSchema)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSliceItem}, a,
                         schema},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSliceItem}, a,
                         schema},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSchema}, schema,
                         DataItem(schema::kSchema)},
          PrevisitedItem{AttrKey("next"), b, schema},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSchema}, schema,
                         DataItem(schema::kSchema)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSchema},
                         DataItem(schema::kSchema), DataItem(schema::kSchema)},
          PrevisitedItem{SchemaAttrKey("next"), schema,
                         DataItem(schema::kSchema)}));
}

TEST_P(VectorizedTraverserTest, ObjectsWithSameSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s = AllocateSchema();
  SetSchemaTriples(*db, {{s, {{"x", DataItem(schema::kInt32)}}}});

  auto objs = AllocateEmptyObjects(2);
  SetDataTriples(*db,
                 {{objs[0], {{schema::kSchemaAttr, s}, {"x", DataItem(1)}}},
                  {objs[1], {{schema::kSchemaAttr, s}, {"x", DataItem(2)}}}});

  auto visitor = std::make_shared<VectorizedRecordingVisitor>();
  ASSERT_OK(TraverseSlice(objs, DataItem(schema::kObject), db, visitor));

  // Both objects share schema `s`, so `MakeObjectsSlice` creates an entity
  // slice and `s` is previsited once via `kSchema` (not once per object).
  EXPECT_THAT(
      PrevisitedItems(visitor->records()),
      UnorderedElementsAre(
          PrevisitedItem{std::nullopt, DataItem(schema::kObject),
                         DataItem(schema::kSchema)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSliceItem},
                         objs[0], DataItem(schema::kObject)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSliceItem},
                         objs[1], DataItem(schema::kObject)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kObjectSchema},
                         objs[0], s},
          PrevisitedItem{TransitionKey{.type = TransitionType::kObjectSchema},
                         objs[1], s},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSchema}, s,
                         DataItem(schema::kSchema)},
          PrevisitedItem{TransitionKey{.type = TransitionType::kSchema},
                         DataItem(schema::kSchema), DataItem(schema::kSchema)},
          PrevisitedItem{SchemaAttrKey("x"), DataItem(schema::kInt32),
                         DataItem(schema::kSchema)}));
}

TEST_P(VectorizedTraverserTest,
       DuplicateObjectsWithSchemasInBigAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto child_schema = AllocateSchema();
  AllocationId schema_alloc = AllocateExplicitSchemas(4);
  ASSERT_FALSE(schema_alloc.IsSmall());
  DataItem s0(schema_alloc.ObjectByOffset(0));
  DataItem s1(schema_alloc.ObjectByOffset(1));
  SetSchemaTriples(*db, {{s0, {{"child", child_schema}}},
                         {s1, {{"child", child_schema}}}});

  auto objs = AllocateEmptyObjects(4);
  DataItem o0 = objs[0];
  DataItem o1 = objs[1];
  DataItem c0 = objs[2];
  DataItem c1 = objs[3];
  SetDataTriples(*db, {{o0, {{schema::kSchemaAttr, s0}, {"child", c0}}},
                       {o1, {{schema::kSchemaAttr, s1}, {"child", c1}}}});

  auto visitor = std::make_shared<VectorizedRecordingVisitor>();
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({o0, o1, o0}));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kObject), db, visitor));

  // `o0` is deduplicated when enqueued, so `c0` and `c1` are each previsited
  // once.
  EXPECT_THAT(
      visitor->calls(),
      ::testing::Contains(
          VectorizedRecordingVisitor::PrevisitCall{AttrKey("child"), 2}));
}

TEST_P(VectorizedTraverserTest, ReverseTopologicalVisitOrder) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  auto schema_c = AllocateSchema();
  SetSchemaTriples(*db, {{schema_a, {{"b", schema_b}, {"c", schema_c}}},
                         {schema_b, {{"c", schema_c}}},
                         {schema_c, {{"val", DataItem(schema::kInt32)}}}});

  auto objs = AllocateEmptyObjects(3);
  auto a = objs[0];
  auto b = objs[1];
  auto c = objs[2];
  // Triangle DAG: a -> b -> c and a -> c (and similarly for schemas). Both b
  // and c are discovered at depth 1 from a, but c must be visited before b,
  // and b before a (verified by `NoOpVisitor::GetValue`).
  SetDataTriples(*db, {{a, {{"b", b}, {"c", c}}},
                       {b, {{"c", c}}},
                       {c, {{"val", DataItem(42)}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a}));
  EXPECT_OK(TraverseSlice(ds, schema_a, db));
}

TEST_P(VectorizedTraverserTest, CyclicGraphVisit) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto node_schema = AllocateSchema();
  SetSchemaTriples(
      *db, {{node_schema, {{"next", node_schema}, {"self", node_schema}}}});

  auto objs = AllocateEmptyObjects(4);
  auto a = objs[0];
  auto b = objs[1];
  auto c = objs[2];
  auto d = objs[3];
  // `a` and `b` form a 2-cycle; `c` has two self-edges (remaining in-degree 2);
  // `d` is acyclic in the same root slice as `a` (`d -> c`).
  SetDataTriples(*db, {{a, {{"next", b}}},
                       {b, {{"next", a}}},
                       {c, {{"next", c}, {"self", c}}},
                       {d, {{"next", c}}}});

  auto visitor = std::make_shared<NoOpVisitor>(
      /*skip=*/std::vector<NoOpVisitor::ItemWithSchema>{},
      /*check_topological_order=*/false);
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({a, d}));
  EXPECT_OK(TraverseSlice(ds, node_schema, db, visitor));
}

TEST_P(VectorizedTraverserTest, RemovedSchemaAttr) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  SetSchemaTriples(
      *db,
      {{schema,
        {{"a", DataItem(schema::kInt32)}, {"b", DataItem(schema::kInt32)}}}});
  ASSERT_OK(db->DelSchemaAttr(schema, "b"));

  auto ds = AllocateEmptyObjects(1);
  auto obj = ds[0];
  SetDataTriples(
      *db,
      {{obj, {{"a", DataItem(10)}, {"b", DataItem(arolla::Text("deleted"))}}}});

  EXPECT_OK(TraverseSlice(ds, schema, db));
}

TEST_P(VectorizedTraverserTest,
       VisitSchemasInBigAllocationWithDifferentAttributes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId schema_alloc = AllocateExplicitSchemas(4);
  ASSERT_FALSE(schema_alloc.IsSmall());
  DataItem s0(schema_alloc.ObjectByOffset(0));
  DataItem s1(schema_alloc.ObjectByOffset(1));
  SetSchemaTriples(*db, {{s0, {{"a", DataItem(schema::kInt32)}}},
                         {s1, {{"b", DataItem(schema::kString)}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s0, s1}));
  EXPECT_OK(TraverseSlice(ds, DataItem(schema::kSchema), db));
}

TEST_P(VectorizedTraverserTest, VisitObjectsWithSchemasInBigAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto child_schema = AllocateSchema();
  AllocationId schema_alloc = AllocateExplicitSchemas(4);
  ASSERT_FALSE(schema_alloc.IsSmall());
  DataItem s0(schema_alloc.ObjectByOffset(0));
  DataItem s1(schema_alloc.ObjectByOffset(1));
  SetSchemaTriples(*db,
                   {{child_schema, {{"val", DataItem(schema::kInt32)}}},
                    {s0,
                     {{"x", DataItem(schema::kInt32)},
                      {"y", DataItem(schema::kInt32)},
                      {"z", DataItem(schema::kInt32)},
                      {"deleted", DataItem(schema::kInt32)},
                      {schema::kSchemaNameAttr, DataItem(arolla::Text("S0"))}}},
                    {s1,
                     {{"x", DataItem(schema::kInt32)},
                      {"y", child_schema},
                      {"w", DataItem(schema::kString)}}}});
  ASSERT_OK(db->DelSchemaAttr(s0, "deleted"));

  auto objs = AllocateEmptyObjects(3);
  DataItem obj0 = objs[0];
  DataItem obj1 = objs[1];
  DataItem child = objs[2];
  SetDataTriples(*db, {{child, {{"val", DataItem(99)}}},
                       {obj0,
                        {{schema::kSchemaAttr, s0},
                         {"x", DataItem(1)},
                         {"y", DataItem(2)},
                         {"deleted", DataItem(arolla::Text("deleted"))}}},
                       {obj1,
                        {{schema::kSchemaAttr, s1},
                         {"x", DataItem(3)},
                         {"y", child},
                         {"w", DataItem(arolla::Text("hi"))}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({obj0, obj1}));
  EXPECT_OK(TraverseSlice(ds, DataItem(schema::kObject), db));
}

TEST_P(VectorizedTraverserTest, VisitEntitiesMixedIsObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto child_schema = AllocateSchema();
  auto root_schema = AllocateSchema();
  SetSchemaTriples(
      *db, {{child_schema, {{"x", DataItem(schema::kInt32)}}},
            {root_schema,
             {{"a_obj", DataItem(schema::kObject)}, {"b_ent", child_schema}}}});

  auto objs = AllocateEmptyObjects(4);
  DataItem r0 = objs[0];
  DataItem r1 = objs[1];
  DataItem c_obj = objs[2];
  DataItem c_ent = objs[3];
  SetDataTriples(
      *db, {{c_obj, {{schema::kSchemaAttr, child_schema}, {"x", DataItem(1)}}},
            {c_ent, {{"x", DataItem(2)}}},
            {r0, {{"a_obj", c_obj}, {"b_ent", c_obj}}},
            {r1, {{"b_ent", c_ent}}}});

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({r0, r1}));
  EXPECT_OK(TraverseSlice(ds, root_schema, db));
}

TEST_P(VectorizedTraverserTest,
       InvalidPrimitiveTypeInObjectsWithDifferentAttrSchemas) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId schema_alloc = AllocateExplicitSchemas(4);
  ASSERT_FALSE(schema_alloc.IsSmall());
  DataItem s0(schema_alloc.ObjectByOffset(0));
  DataItem s1(schema_alloc.ObjectByOffset(1));
  SetSchemaTriples(*db, {{s0, {{"x", DataItem(schema::kInt32)}}},
                         {s1, {{"x", DataItem(schema::kFloat32)}}}});

  auto objs = AllocateEmptyObjects(2);
  SetDataTriples(
      *db, {{objs[0],
             {{schema::kSchemaAttr, s0}, {"x", DataItem(arolla::Text("bad"))}}},
            {objs[1], {{schema::kSchemaAttr, s1}, {"x", DataItem(1.5f)}}}});

  EXPECT_THAT(
      TraverseSlice(objs, DataItem(schema::kObject), db),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("during traversal, got a slice with primitive type "
                         "INT32 while the actual content has type STRING")));
}

}  // namespace
}  // namespace koladata::internal
