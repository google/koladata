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
#include "koladata/internal/op_utils/vectorized_traverse_helper.h"

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/functional/function_ref.h"
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
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::internal::vectorized_traverser_internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Field;
using ::testing::Matcher;
using ::testing::UnorderedElementsAre;

using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

DataItem AllocateSchema() { return DataItem(AllocateExplicitSchema()); }

DataItem TextItem(absl::string_view text) {
  return DataItem(arolla::Text(text));
}

DataSliceImpl Slice(absl::Span<const DataItem> items) {
  return DataSliceImpl::Create(items);
}

TransitionKey Key(TransitionType type, DataItem value = DataItem()) {
  return TransitionKey{.type = type, .value = std::move(value)};
}

template <typename ValuesMatcher>
Matcher<const Transition&> TransitionIs(std::optional<TransitionKey> key,
                                        ValuesMatcher values,
                                        const DataItem& schema) {
  return AllOf(Field("key", &Transition::key, Eq(key)),
               Field("values", &Transition::values, values),
               Field("schema", &Transition::schema, Eq(schema)));
}

absl::StatusOr<std::vector<Transition>> RootTransitions(
    const DataSliceImpl& ds, const DataItem& schema) {
  std::vector<Transition> transitions;
  RETURN_IF_ERROR(VectorizedTraverseHelper::ForEachRootTransition(
      ds, schema, [&](const Transition& transition) {
        transitions.push_back(transition);
        return absl::OkStatus();
      }));
  return transitions;
}

// Returns the transitions that `for_each_child` passes to its callback.
absl::StatusOr<std::vector<Transition>> CollectTransitions(
    const DataBagImpl& db,
    absl::FunctionRef<absl::Status(VectorizedTraverseHelper&,
                                   TransitionCallback)>
        for_each_child) {
  VectorizedTraverseHelper helper(db, /*fallbacks=*/{});
  std::vector<Transition> transitions;
  RETURN_IF_ERROR(for_each_child(helper, [&](const Transition& transition) {
    transitions.push_back(transition);
    return absl::OkStatus();
  }));
  return transitions;
}

absl::StatusOr<std::vector<Transition>> ChildrenOfSchemas(
    const DataBagImpl& db, const DataSliceImpl& schemas) {
  return CollectTransitions(
      db, [&](VectorizedTraverseHelper& helper, TransitionCallback callback) {
        return helper.ForEachChildOfSchemas(schemas, callback);
      });
}

absl::StatusOr<std::vector<Transition>> ChildrenOfEntities(
    const DataBagImpl& db, const DataSliceImpl& entities,
    const DataItem& schema) {
  return CollectTransitions(
      db, [&](VectorizedTraverseHelper& helper, TransitionCallback callback) {
        return helper.ForEachChildOfEntities(entities, schema, callback);
      });
}

absl::StatusOr<std::vector<Transition>> ChildrenOfObjects(
    const DataBagImpl& db, const DataSliceImpl& objects,
    const DataSliceImpl& object_schemas) {
  return CollectTransitions(
      db, [&](VectorizedTraverseHelper& helper, TransitionCallback callback) {
        return helper.ForEachChildOfObjects(objects, object_schemas, callback);
      });
}

TEST(VectorizedTraverseHelperTest, RootTransitions) {
  auto schema = AllocateSchema();
  auto objs = DataSliceImpl::AllocateEmptyObjects(2);
  EXPECT_THAT(RootTransitions(objs, schema),
              IsOkAndHolds(ElementsAre(
                  TransitionIs(Key(TransitionType::kSliceItem),
                               ElementsAre(objs[0], objs[1]), schema),
                  TransitionIs(std::nullopt, ElementsAre(schema),
                               DataItem(schema::kSchema)))));
}

TEST(VectorizedTraverseHelperTest, EntityAttributes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(schema, "x", DataItem(schema::kInt32)));
  ASSERT_OK(db->SetSchemaAttr(schema, "child", child_schema));
  auto objs = DataSliceImpl::AllocateEmptyObjects(2);
  auto child = DataItem(AllocateSingleObject());
  ASSERT_OK(db->SetAttr(
      objs, "x", DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2}))));
  ASSERT_OK(db->SetAttr(objs[0], "child", child));

  // Primitive attributes are skipped, other attribute values are not filtered.
  EXPECT_THAT(
      ChildrenOfEntities(*db, objs, schema),
      IsOkAndHolds(ElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kAttributeName, TextItem("child")),
                       ElementsAre(child, DataItem()), child_schema))));
}

TEST(VectorizedTraverseHelperTest, NoFollowEntities) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(schema, "child", AllocateSchema()));
  auto nofollow_schema =
      DataItem(CreateNoFollowWithMainObject(schema.value<ObjectId>()));
  auto objs = DataSliceImpl::AllocateEmptyObjects(1);
  ASSERT_OK(db->SetAttr(objs[0], "child", DataItem(AllocateSingleObject())));

  EXPECT_THAT(ChildrenOfEntities(*db, objs, nofollow_schema),
              IsOkAndHolds(ElementsAre(TransitionIs(
                  Key(TransitionType::kSchema), ElementsAre(nofollow_schema),
                  DataItem(schema::kSchema)))));
}

TEST(VectorizedTraverseHelperTest, ListItems) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(list_schema, schema::kListItemsSchemaAttr,
                              item_schema));
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(2), 2);
  auto items = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK(db->ExtendList(lists[0], Slice({items[0], items[1]})));
  ASSERT_OK(db->ExtendList(lists[1], Slice({items[2]})));

  EXPECT_THAT(
      ChildrenOfEntities(*db, lists, list_schema),
      IsOkAndHolds(ElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(list_schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kListItem),
                       ElementsAre(items[0], items[1], items[2]),
                       item_schema))));
}

TEST(VectorizedTraverseHelperTest, ListOfPrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(list_schema, schema::kListItemsSchemaAttr,
                              DataItem(schema::kInt32)));
  auto list = DataItem(AllocateSingleList());
  ASSERT_OK(db->ExtendList(list, Slice({DataItem(1), DataItem(2)})));

  EXPECT_THAT(ChildrenOfEntities(*db, Slice({list}), list_schema),
              IsOkAndHolds(ElementsAre(TransitionIs(
                  Key(TransitionType::kSchema), ElementsAre(list_schema),
                  DataItem(schema::kSchema)))));
}

TEST(VectorizedTraverseHelperTest, DictWithPrimitiveKeys) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto values_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(dict_schema, schema::kDictKeysSchemaAttr,
                              DataItem(schema::kString)));
  ASSERT_OK(db->SetSchemaAttr(dict_schema, schema::kDictValuesSchemaAttr,
                              values_schema));
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(2), 2);
  auto values = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK(db->SetInDict(dicts[0], TextItem("a"), values[0]));
  ASSERT_OK(db->SetInDict(dicts[0], TextItem("b"), values[1]));
  ASSERT_OK(db->SetInDict(dicts[1], TextItem("a"), values[2]));

  // Primitive keys are skipped; values are grouped by key.
  EXPECT_THAT(
      ChildrenOfEntities(*db, dicts, dict_schema),
      IsOkAndHolds(UnorderedElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(dict_schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kDictValue, TextItem("a")),
                       ElementsAre(values[0], values[2]), values_schema),
          TransitionIs(Key(TransitionType::kDictValue, TextItem("b")),
                       ElementsAre(values[1]), values_schema))));
}

TEST(VectorizedTraverseHelperTest, DictWithObjectKeysAndValues) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto keys_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  ASSERT_OK(
      db->SetSchemaAttr(dict_schema, schema::kDictKeysSchemaAttr, keys_schema));
  ASSERT_OK(db->SetSchemaAttr(dict_schema, schema::kDictValuesSchemaAttr,
                              DataItem(schema::kObject)));
  auto dict = DataItem(AllocateSingleDict());
  auto keys = DataSliceImpl::AllocateEmptyObjects(2);
  auto value = DataItem(AllocateSingleObject());
  ASSERT_OK(db->SetInDict(dict, keys[0], value));
  ASSERT_OK(db->SetInDict(dict, keys[1], DataItem(1)));

  // Primitive values in OBJECT dict values are filtered out when grouping by
  // key.
  EXPECT_THAT(
      ChildrenOfEntities(*db, Slice({dict}), dict_schema),
      IsOkAndHolds(ElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(dict_schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kDictKey),
                       UnorderedElementsAre(keys[0], keys[1]), keys_schema),
          TransitionIs(Key(TransitionType::kDictValue, keys[0]),
                       ElementsAre(value), DataItem(schema::kObject)))));
}

TEST(VectorizedTraverseHelperTest, SchemaAttributes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  auto metadata = DataItem(AllocateSingleObject());
  ASSERT_OK(db->SetSchemaAttr(schema, "x", DataItem(schema::kInt32)));
  ASSERT_OK(db->SetSchemaAttr(schema, "child", child_schema));
  ASSERT_OK(
      db->SetSchemaAttr(schema, schema::kSchemaNameAttr, TextItem("name")));
  ASSERT_OK(db->SetSchemaAttr(schema, schema::kSchemaMetadataAttr, metadata));

  // `__schema_name__` is skipped, schema metadata is an object.
  EXPECT_THAT(
      ChildrenOfSchemas(*db, Slice({schema})),
      IsOkAndHolds(UnorderedElementsAre(
          TransitionIs(Key(TransitionType::kSchema),
                       ElementsAre(DataItem(schema::kSchema)),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kSchemaAttributeName, TextItem("x")),
                       ElementsAre(DataItem(schema::kInt32)),
                       DataItem(schema::kSchema)),
          TransitionIs(
              Key(TransitionType::kSchemaAttributeName, TextItem("child")),
              ElementsAre(child_schema), DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kSchemaAttributeName,
                           TextItem(schema::kSchemaMetadataAttr)),
                       ElementsAre(metadata), DataItem(schema::kObject)))));
}

TEST(VectorizedTraverseHelperTest, SchemaAttributesOfBigAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateExplicitSchemas(10);
  auto s0 = DataItem(alloc.ObjectByOffset(0));
  auto s1 = DataItem(alloc.ObjectByOffset(1));
  auto child_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(s0, "x", DataItem(schema::kInt32)));
  ASSERT_OK(db->SetSchemaAttr(s1, "child", child_schema));

  // Attributes of all schemas in the allocation are included.
  EXPECT_THAT(
      ChildrenOfSchemas(*db, Slice({s0, s1})),
      IsOkAndHolds(UnorderedElementsAre(
          TransitionIs(Key(TransitionType::kSchema),
                       ElementsAre(DataItem(schema::kSchema)),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kSchemaAttributeName, TextItem("x")),
                       ElementsAre(DataItem(schema::kInt32), DataItem()),
                       DataItem(schema::kSchema)),
          TransitionIs(
              Key(TransitionType::kSchemaAttributeName, TextItem("child")),
              ElementsAre(DataItem(), child_schema),
              DataItem(schema::kSchema)))));
}

TEST(VectorizedTraverseHelperTest,
     ObjectAttributesAreGroupedByAttributeSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateExplicitSchemas(10);
  auto s0 = DataItem(alloc.ObjectByOffset(0));
  auto s1 = DataItem(alloc.ObjectByOffset(1));
  auto child_schema0 = AllocateSchema();
  auto child_schema1 = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(s0, "x", DataItem(schema::kInt32)));
  ASSERT_OK(db->SetSchemaAttr(s0, "child", child_schema0));
  ASSERT_OK(db->SetSchemaAttr(s1, "child", child_schema1));
  auto objs = DataSliceImpl::AllocateEmptyObjects(3);
  auto children = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK(db->SetAttr(objs[0], "x", DataItem(1)));
  ASSERT_OK(db->SetAttr(objs, "child", children));

  EXPECT_THAT(
      ChildrenOfObjects(*db, objs, Slice({s0, s1, s0})),
      IsOkAndHolds(ElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(s0, s1, s0),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kAttributeName, TextItem("child")),
                       ElementsAre(children[0], children[2]), child_schema0),
          TransitionIs(Key(TransitionType::kAttributeName, TextItem("child")),
                       ElementsAre(children[1]), child_schema1))));
}

TEST(VectorizedTraverseHelperTest, ObjectListsAndDicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateExplicitSchemas(10);
  auto list_schema = DataItem(alloc.ObjectByOffset(0));
  auto dict_schema = DataItem(alloc.ObjectByOffset(1));
  auto item_schema = AllocateSchema();
  auto values_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(list_schema, schema::kListItemsSchemaAttr,
                              item_schema));
  ASSERT_OK(db->SetSchemaAttr(dict_schema, schema::kDictKeysSchemaAttr,
                              DataItem(schema::kString)));
  ASSERT_OK(db->SetSchemaAttr(dict_schema, schema::kDictValuesSchemaAttr,
                              values_schema));
  auto list = DataItem(AllocateSingleList());
  auto dict = DataItem(AllocateSingleDict());
  auto item = DataItem(AllocateSingleObject());
  auto value = DataItem(AllocateSingleObject());
  ASSERT_OK(db->ExtendList(list, Slice({item})));
  ASSERT_OK(db->SetInDict(dict, TextItem("a"), value));

  EXPECT_THAT(
      ChildrenOfObjects(*db, Slice({list, dict}),
                        Slice({list_schema, dict_schema})),
      IsOkAndHolds(ElementsAre(
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(list_schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kListItem), ElementsAre(item),
                       item_schema),
          TransitionIs(Key(TransitionType::kSchema), ElementsAre(dict_schema),
                       DataItem(schema::kSchema)),
          TransitionIs(Key(TransitionType::kDictValue, TextItem("a")),
                       ElementsAre(value), values_schema))));
}

TEST(VectorizedTraverseHelperTest, CallbackErrorIsPropagated) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(schema, "child", AllocateSchema()));
  auto objs = DataSliceImpl::AllocateEmptyObjects(1);
  ASSERT_OK(db->SetAttr(objs[0], "child", DataItem(AllocateSingleObject())));

  VectorizedTraverseHelper helper(*db, /*fallbacks=*/{});
  int calls = 0;
  EXPECT_THAT(
      helper.ForEachChildOfEntities(objs, schema,
                                    [&](const Transition&) {
                                      ++calls;
                                      return absl::InternalError("fail");
                                    }),
      StatusIs(absl::StatusCode::kInternal, "fail"));
  EXPECT_EQ(calls, 1);
}

}  // namespace
}  // namespace koladata::internal::vectorized_traverser_internal
