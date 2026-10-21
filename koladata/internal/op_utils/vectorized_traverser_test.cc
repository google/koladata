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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
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

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::koladata::internal::testing::deep_op_utils::DeepOpTest;
using ::koladata::internal::testing::deep_op_utils::test_param_values;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAre;

using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

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

class PruningVisitor : public SaveTransitionKeysVisitor {
 public:
  explicit PruningVisitor(arolla::Text prune_attr)
      : prune_attr_(std::move(prune_attr)) {}

  absl::StatusOr<DataSliceImpl> PrevisitSlice(
      const DataSliceImpl& ds, const DataItem& schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& parent_token) override {
    if (transition_key.has_value() &&
        transition_key->type ==
            TraverseHelper::TransitionType::kAttributeName &&
        transition_key->value.holds_value<arolla::Text>() &&
        transition_key->value.value<arolla::Text>() == prune_attr_) {
      SliceBuilder empty_builder(ds.size());
      return std::move(empty_builder).Build();
    }
    return SaveTransitionKeysVisitor::PrevisitSlice(ds, schema, transition_key,
                                                    parent_token);
  }

 private:
  arolla::Text prune_attr_;
};

class VectorizedTraverserTest : public DeepOpTest {
 protected:
  absl::Status TraverseSlice(
      const DataSliceImpl& ds, const DataItem& schema, const DataBagImplPtr& db,
      std::shared_ptr<AbstractVisitor> visitor =
          std::make_shared<SaveTransitionKeysVisitor>()) {
    auto main_db = GetMainDb(db);
    auto fallback_db = GetFallbackDb(db);
    std::vector<const DataBagImpl*> fallbacks = {fallback_db.get()};
    VectorizedTraverser traverser(*main_db, fallbacks, std::move(visitor));
    return traverser.TraverseSlice(ds, schema);
  }
};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, VectorizedTraverserTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(VectorizedTraverserTest, SimpleEntityChain) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  SetSchemaTriples(
      *db, {{schema, {{"next", schema}, {"val", DataItem(schema::kInt32)}}}});

  auto objs = AllocateEmptyObjects(3);
  auto a0 = objs[0];
  auto a1 = objs[1];
  auto a2 = objs[2];
  SetDataTriples(*db, {{a0, {{"next", a1}, {"val", DataItem(10)}}},
                       {a1, {{"next", a2}, {"val", DataItem(20)}}},
                       {a2, {{"val", DataItem(30)}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0}));

  EXPECT_OK(TraverseSlice(ds, schema, db, visitor));

  bool found_a0 = false, found_a1 = false, found_a2 = false;
  for (const auto& rec : visitor->records()) {
    if (rec.item == a0) found_a0 = true;
    if (rec.item == a1) found_a1 = true;
    if (rec.item == a2) found_a2 = true;
  }
  EXPECT_TRUE(found_a0);
  EXPECT_TRUE(found_a1);
  EXPECT_TRUE(found_a2);
}

TEST_P(VectorizedTraverserTest, DiamondGraph) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"left", schema}, {"right", schema}}}});

  auto objs = AllocateEmptyObjects(4);
  auto a = objs[0];
  auto b = objs[1];
  auto c = objs[2];
  auto d = objs[3];
  // Diamond: a -> b, a -> c; b -> d, c -> d.
  SetDataTriples(*db, {{a, {{"left", b}, {"right", c}}},
                       {b, {{"left", d}}},
                       {c, {{"right", d}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a}));

  EXPECT_OK(TraverseSlice(ds, schema, db, visitor));

  bool found_a = false, found_b = false, found_c = false, found_d = false;
  for (const auto& rec : visitor->records()) {
    if (rec.item == a) found_a = true;
    if (rec.item == b) found_b = true;
    if (rec.item == c) found_c = true;
    if (rec.item == d) found_d = true;
  }
  EXPECT_TRUE(found_a);
  EXPECT_TRUE(found_b);
  EXPECT_TRUE(found_c);
  EXPECT_TRUE(found_d);
}

TEST_P(VectorizedTraverserTest, ObjectSliceGroupedBySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  SetSchemaTriples(*db, {{s1, {{"x", DataItem(schema::kInt32)}}},
                         {s2, {{"y", DataItem(schema::kString)}}}});

  auto objs = AllocateEmptyObjects(2);
  auto o1 = objs[0];
  auto o2 = objs[1];
  SetDataTriples(
      *db,
      {{o1, {{schema::kSchemaAttr, s1}, {"x", DataItem(42)}}},
       {o2,
        {{schema::kSchemaAttr, s2}, {"y", DataItem(arolla::Text("hello"))}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  EXPECT_OK(TraverseSlice(objs, DataItem(schema::kObject), db, visitor));

  bool found_o1 = false, found_o2 = false;
  for (const auto& rec : visitor->records()) {
    if (rec.transition_key.has_value() &&
        rec.transition_key->type == TransitionType::kObjectSchema) {
      if (rec.from_token == o1 && rec.item == o1 && rec.schema == s1) {
        found_o1 = true;
      }
      if (rec.from_token == o2 && rec.item == o2 && rec.schema == s2) {
        found_o2 = true;
      }
    }
  }
  EXPECT_TRUE(found_o1);
  EXPECT_TRUE(found_o2);
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

  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({p}));
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

  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({p}));
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

TEST_P(VectorizedTraverserTest, ListEntityTraversal) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  SetSchemaTriples(
      *db, {{item_schema, {{"val", DataItem(schema::kInt32)}}},
            {list_schema, {{schema::kListItemsSchemaAttr, item_schema}}}});

  auto items = AllocateEmptyObjects(2);
  auto item0 = items[0];
  auto item1 = items[1];
  SetDataTriples(
      *db, {{item0, {{"val", DataItem(1)}}}, {item1, {{"val", DataItem(2)}}}});

  auto lists = AllocateEmptyLists(1);
  auto list_obj = lists[0];
  ASSERT_OK(db->ExtendList(list_obj, items));

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  EXPECT_OK(TraverseSlice(lists, list_schema, db, visitor));

  bool found_item0 = false, found_item1 = false;
  for (const auto& rec : visitor->records()) {
    if (rec.transition_key.has_value() &&
        rec.transition_key->type == TransitionType::kListItem &&
        rec.from_token == list_obj && rec.schema == item_schema) {
      if (rec.item == item0) found_item0 = true;
      if (rec.item == item1) found_item1 = true;
    }
  }
  EXPECT_TRUE(found_item0);
  EXPECT_TRUE(found_item1);
}

TEST_P(VectorizedTraverserTest, DictEntityTraversal) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto val_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  SetSchemaTriples(*db,
                   {{val_schema, {{"val", DataItem(schema::kInt32)}}},
                    {dict_schema,
                     {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
                      {schema::kDictValuesSchemaAttr, val_schema}}}});

  auto vals = AllocateEmptyObjects(1);
  auto val0 = vals[0];
  SetDataTriples(*db, {{val0, {{"val", DataItem(99)}}}});

  auto dicts = AllocateEmptyDicts(1);
  auto dict_obj = dicts[0];
  ASSERT_OK(db->SetInDict(dict_obj, DataItem(arolla::Text("key0")), val0));

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  EXPECT_OK(TraverseSlice(dicts, dict_schema, db, visitor));

  bool found_val0 = false;
  for (const auto& rec : visitor->records()) {
    if (rec.item == val0) found_val0 = true;
  }
  EXPECT_TRUE(found_val0);
}

TEST_P(VectorizedTraverserTest, PrimitivesAndEmptyInputs) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  // Primitive slice traversal succeeds:
  auto int_ds = DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3}));
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

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0}));
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

TEST_P(VectorizedTraverserTest, SchemaTraversalPrevisit) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  SetSchemaTriples(*db, {{schema, {{"child", child_schema}}},
                         {child_schema, {{"val", DataItem(schema::kInt32)}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({schema}));
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

TEST_P(VectorizedTraverserTest, VisitorPruningSubtree) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  SetSchemaTriples(*db,
                   {{schema, {{"keep", child_schema}, {"prune", child_schema}}},
                    {child_schema, {{"val", DataItem(schema::kInt32)}}}});

  auto objs = AllocateEmptyObjects(3);
  auto a = objs[0];
  auto b = objs[1];
  auto c = objs[2];
  SetDataTriples(*db, {{a, {{"keep", b}, {"prune", c}}},
                       {b, {{"val", DataItem(10)}}},
                       {c, {{"val", DataItem(20)}}}});

  auto visitor = std::make_shared<PruningVisitor>(arolla::Text("prune"));
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a}));
  ASSERT_OK(TraverseSlice(ds, schema, db, visitor));

  bool found_b = false, found_c = false;
  for (const auto& rec : visitor->records()) {
    if (rec.item == b) found_b = true;
    if (rec.item == c) found_c = true;
  }
  EXPECT_TRUE(found_b);
  EXPECT_FALSE(found_c);
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

TEST_P(VectorizedTraverserTest, SchemaMetadataIsTraversedAsObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto metadata_schema = AllocateSchema();
  auto metadata = AllocateEmptyObjects(1)[0];
  SetSchemaTriples(*db,
                   {{schema,
                     {{"x", DataItem(schema::kInt32)},
                      {schema::kSchemaMetadataAttr, metadata}}},
                    {metadata_schema, {{"note", DataItem(schema::kString)}}}});
  SetDataTriples(*db, {{metadata, {{schema::kSchemaAttr, metadata_schema}}}});

  auto visitor = std::make_shared<SaveTransitionKeysVisitor>();
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({schema}));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kSchema), db, visitor));

  EXPECT_THAT(PrevisitedItems(visitor->records()),
              ::testing::IsSupersetOf<PrevisitedItem>({
                  {SchemaAttrKey(schema::kSchemaMetadataAttr), metadata,
                   DataItem(schema::kObject)},
                  {TransitionKey{.type = TransitionType::kObjectSchema},
                   metadata, metadata_schema},
              }));
}

TEST_P(VectorizedTraverserTest, ListsAndDictsInObjectsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item_schema = AllocateSchema();

  // Schemas of a list, a dict and an entity in a single big allocation.
  AllocationId schema_alloc = AllocateExplicitSchemas(4);
  ASSERT_FALSE(schema_alloc.IsSmall());
  DataItem list_schema(schema_alloc.ObjectByOffset(0));
  DataItem dict_schema(schema_alloc.ObjectByOffset(1));
  DataItem entity_schema(schema_alloc.ObjectByOffset(2));
  SetSchemaTriples(
      *db, {{item_schema, {{"val", DataItem(schema::kInt32)}}},
            {list_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
            {dict_schema,
             {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
              {schema::kDictValuesSchemaAttr, item_schema}}},
            {entity_schema, {{"x", item_schema}}}});

  DataItem list = AllocateEmptyLists(1)[0];
  DataItem dict = AllocateEmptyDicts(1)[0];
  auto objs = AllocateEmptyObjects(4);
  DataItem entity = objs[0];
  DataItem list_item = objs[1];
  DataItem dict_value = objs[2];
  DataItem entity_attr = objs[3];
  SetDataTriples(
      *db,
      {{list, {{schema::kSchemaAttr, list_schema}}},
       {dict, {{schema::kSchemaAttr, dict_schema}}},
       {entity, {{schema::kSchemaAttr, entity_schema}, {"x", entity_attr}}}});
  ASSERT_OK(db->ExtendList(list, DataSliceImpl::Create(1, list_item)));
  ASSERT_OK(db->SetInDict(dict, DataItem(arolla::Text("k")), dict_value));

  auto visitor = std::make_shared<VectorizedRecordingVisitor>();
  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<DataItem>({list, dict, entity}));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kObject), db, visitor));

  EXPECT_THAT(PrevisitedItems(visitor->records()),
              ::testing::IsSupersetOf<PrevisitedItem>({
                  {TransitionKey{.type = TransitionType::kListItem}, list_item,
                   item_schema},
                  {TransitionKey{.type = TransitionType::kDictValue,
                                 .value = DataItem(arolla::Text("k"))},
                   dict_value, item_schema},
                  {AttrKey("x"), entity_attr, item_schema},
              }));
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
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({obj}));
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
  auto ds = DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a, a}));
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
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({o0, o1, o0}));
  ASSERT_OK(TraverseSlice(ds, DataItem(schema::kObject), db, visitor));

  // `o0` is deduplicated when enqueued, so `c0` and `c1` are each previsited
  // once.
  EXPECT_THAT(
      visitor->calls(),
      ::testing::Contains(
          VectorizedRecordingVisitor::PrevisitCall{AttrKey("child"), 2}));
}

}  // namespace
}  // namespace koladata::internal
