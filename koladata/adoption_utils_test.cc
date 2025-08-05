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
#include "koladata/adoption_utils.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::internal::testing::DataBagEqual;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAre;

TEST(AdoptionQueueTest, Empty) {
  AdoptionQueue q;
  EXPECT_TRUE(q.empty());
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(nullptr));
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  EXPECT_OK(q.AdoptInto(*db1));
  EXPECT_THAT(db1->GetImpl(), DataBagEqual(db2->GetImpl()));
}

TEST(AdoptionQueueTest, Single) {
  ASSERT_OK_AND_ASSIGN(auto int32_schema,
                       DataSlice::Create(internal::DataItem(schema::kInt32),
                                         internal::DataItem(schema::kSchema)));
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema,
      CreateEntitySchema(schema_db, {"a", "b", "c"},
                         {int32_schema, int32_schema, int32_schema}));

  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  auto db3 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "c", internal::DataItem(3)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice,
      DataSlice::Create(obj, internal::DataItem(schema::kObject), db2));
  ASSERT_OK_AND_ASSIGN(slice, slice.SetSchema(schema));

  AdoptionQueue q;
  q.Add(slice);
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(db2));
  q.Add(nullptr);
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(db2));
  q.Add(db3);
  ASSERT_OK_AND_ASSIGN(auto db_res, q.GetCommonOrMergedDb());
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "a"), IsOkAndHolds(std::nullopt));
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3)));

  ASSERT_OK(q.AdoptInto(*db1));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3)));
}

TEST(AdoptionQueueTest, GetCommonOrMergedDb) {
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateEntitySchema(schema_db, {"a", "b"},
                                          {test::Schema(schema::kInt32),
                                           test::Schema(schema::kInt32)}));

  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice1,
      DataSlice::Create(obj, internal::DataItem(schema::kObject), db1));
  ASSERT_OK_AND_ASSIGN(slice1, slice1.SetSchema(schema));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice2,
      DataSlice::Create(obj, internal::DataItem(schema::kObject), db2));
  ASSERT_OK_AND_ASSIGN(slice2, slice2.SetSchema(schema));

  AdoptionQueue q;
  q.Add(slice1);
  q.Add(slice2);
  q.Add(slice1);
  ASSERT_OK_AND_ASSIGN(DataBagPtr db3, q.GetCommonOrMergedDb());
  EXPECT_FALSE(db3->IsMutable());

  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
}

TEST(AdoptionQueueTest, WithFallbacks) {
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateEntitySchema(schema_db, {"a", "b", "c"},
                                          {test::Schema(schema::kInt32),
                                           test::Schema(schema::kInt32),
                                           test::Schema(schema::kFloat32)}));

  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  auto db3 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));
  ASSERT_OK(  // conflicts with db2
      db3->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(4.0f)));
  ASSERT_OK(
      db3->GetMutableImpl()->get().SetAttr(obj, "c", internal::DataItem(3.0f)));

  DataBagPtr db_with_fallbacks =
      DataBag::ImmutableEmptyWithFallbacks({db2, db3, schema_db});

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice1,
      DataSlice::Create(obj, internal::DataItem(schema::kObject), db1));
  ASSERT_OK_AND_ASSIGN(slice1, slice1.SetSchema(schema));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice2,
      DataSlice::Create(obj, internal::DataItem(schema::kObject),
                        db_with_fallbacks));
  ASSERT_OK_AND_ASSIGN(slice2,
                       slice2.WithSchema(schema.WithBag(db_with_fallbacks)));

  AdoptionQueue q;
  q.Add(slice1);
  q.Add(slice2);
  q.Add(slice1);

  ASSERT_OK(q.AdoptInto(*db1));

  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3.0f)));
}

TEST(AdoptionQueueTest, TestDbVector) {
  auto db1 = DataBag::Empty();
  DataBagPtr db2;
  auto db3 = DataBag::Empty();
  AdoptionQueue q;
  q.Add(db1);
  q.Add(db2);
  q.Add(db3);

  EXPECT_THAT(q.GetBagWithFallbacks()->GetFallbacks(),
              UnorderedElementsAre(db1, db3));
}

TEST(AdoptionQueueTest, Extraction) {
  auto db1 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema1,
      CreateEntitySchema(db1, {"a"}, {test::Schema(schema::kInt32)}));
  auto& db1_impl = db1->GetMutableImpl()->get();
  internal::DataItem obj11(internal::AllocateSingleObject());
  internal::DataItem obj12(internal::AllocateSingleObject());
  internal::DataItem obj13(internal::AllocateSingleObject());
  ASSERT_OK(db1_impl.SetAttr(obj11, "a", internal::DataItem(11)));
  ASSERT_OK(db1_impl.SetAttr(obj12, "a", internal::DataItem(12)));
  ASSERT_OK(db1_impl.SetAttr(obj13, "a", internal::DataItem(13)));

  auto db2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema2,
      CreateEntitySchema(db2, {"a"}, {test::Schema(schema::kInt32)}));
  auto& db2_impl = db2->GetMutableImpl()->get();
  internal::DataItem obj21(internal::AllocateSingleObject());
  internal::DataItem obj22(internal::AllocateSingleObject());
  ASSERT_OK(db2_impl.SetAttr(obj21, "a", internal::DataItem(21)));
  ASSERT_OK(db2_impl.SetAttr(obj22, "a", internal::DataItem(22)));

  ASSERT_OK_AND_ASSIGN(DataSlice slice11,
                       DataSlice::Create(obj11, schema1.item(), db1));
  ASSERT_OK_AND_ASSIGN(DataSlice slice13,
                       DataSlice::Create(obj13, schema1.item(), db1));
  ASSERT_OK_AND_ASSIGN(DataSlice slice21,
                       DataSlice::Create(obj21, schema2.item(), db2));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(slice11);
  adoption_queue.Add(slice21);
  adoption_queue.Add(slice13);

  ASSERT_OK_AND_ASSIGN(DataBagPtr db3, adoption_queue.GetCommonOrMergedDb());

  EXPECT_THAT(db3->GetImpl().GetAttr(obj11, "a"),
              IsOkAndHolds(internal::DataItem(11)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj12, "a"),
              IsOkAndHolds(internal::DataItem()));  // Not extracted.
  EXPECT_THAT(db3->GetImpl().GetAttr(obj13, "a"),
              IsOkAndHolds(internal::DataItem(13)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj21, "a"),
              IsOkAndHolds(internal::DataItem(21)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj22, "a"),
              IsOkAndHolds(internal::DataItem()));  // Not extracted.
}

TEST(AdoptStubTest, Entity) {
  {
    // Holding primitives.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        DataSlice schema,
        CreateEntitySchema(db1, {"a"}, {test::Schema(schema::kInt32)}));
    auto& db1_impl = db1->GetMutableImpl()->get();
    internal::DataItem obj(internal::AllocateSingleObject());
    ASSERT_OK(db1_impl.SetAttr(obj, "a", internal::DataItem(1)));
    DataSlice item = test::DataItem(obj, schema.item(), db1);
    auto db2 = DataBag::Empty();
    // Nothing is adopted.
    ASSERT_OK(AdoptStub(db2, item));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(DataBag::Empty()->GetImpl()));
  }
  {
    // Holding lists.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(DataSlice list_schema,
                         CreateListSchema(db1, test::Schema(schema::kInt32)));
    ASSERT_OK_AND_ASSIGN(DataSlice schema,
                         CreateEntitySchema(db1, {"a"}, {list_schema}));
    auto& db1_impl = db1->GetMutableImpl()->get();
    internal::DataItem obj(internal::AllocateSingleObject());
    ASSERT_OK_AND_ASSIGN(auto list,
                         CreateEmptyList(db1, /*schema=*/list_schema));
    ASSERT_OK(db1_impl.SetAttr(obj, "a", list.item()));
    DataSlice item = test::DataItem(obj, schema.item(), db1);
    auto db2 = DataBag::Empty();
    // Nothing is adopted.
    ASSERT_OK(AdoptStub(db2, item));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(DataBag::Empty()->GetImpl()));
  }
}

TEST(AdoptStubTest, Object) {
  {
    // Holding primitives.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        DataSlice schema,
        CreateEntitySchema(db1, {"a"}, {test::Schema(schema::kInt32)}));
    auto& db1_impl = db1->GetMutableImpl()->get();
    internal::DataItem obj(internal::AllocateSingleObject());
    ASSERT_OK(db1_impl.SetAttr(obj, "a", internal::DataItem(1)));
    DataSlice item = test::DataItem(obj, schema::kObject, db1);
    ASSERT_OK(item.SetAttr(schema::kSchemaAttr, schema));
    auto db2 = DataBag::Empty();
    // The __schema__ is adopted.
    ASSERT_OK(AdoptStub(db2, item));
    auto expected_db = DataBag::Empty();
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetAttr(
        obj, schema::kSchemaAttr, schema.item()));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(expected_db->GetImpl()));
  }
}

TEST(AdoptStubTest, ObjectWithMixedDtype) {
  auto bag1 = DataBag::Empty();
  auto item1 = *EntityCreator::FromAttrs(bag1, {}, {})->EmbedSchema();
  auto item2 = *DataSlice::CreateFromScalar(2).WithBag(bag1).EmbedSchema();
  auto slice = *DataSlice::CreateWithFlatShape(
      internal::DataSliceImpl::Create({item1.item(), item2.item()}),
      item1.GetSchemaImpl(), bag1);

  auto stub_bag = DataBag::Empty();
  ASSERT_OK(AdoptStub(stub_bag, slice));

  EXPECT_TRUE(
      slice.WithBag(stub_bag).GetObjSchema()->WithBag(nullptr).IsEquivalentTo(
          slice.GetObjSchema()->WithBag(nullptr)));
}

TEST(AdoptStubTest, List) {
  {
    // Holding primitives.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(DataSlice schema,
                         CreateListSchema(db1, test::Schema(schema::kInt32)));
    ASSERT_OK_AND_ASSIGN(auto list, CreateEmptyList(db1, /*schema=*/schema));
    auto db2 = DataBag::Empty();
    // The list schema is adopted.
    ASSERT_OK(AdoptStub(db2, list));
    auto expected_db = DataBag::Empty();
    ASSERT_OK(schema.WithBag(expected_db)
                  .SetAttr(schema::kListItemsSchemaAttr,
                           test::Schema(schema::kInt32)));
    // Write a list-item as well for recursion.
    ASSERT_OK(expected_db->GetMutableImpl()->get().ReplaceInList(
        list.item(), internal::DataBagImpl::ListRange(0),
        internal::DataSliceImpl::CreateEmptyAndUnknownType(0)));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(expected_db->GetImpl()));
  }
  {
    // Recursive lists.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(DataSlice list_schema,
                         CreateListSchema(db1, test::Schema(schema::kInt32)));
    ASSERT_OK_AND_ASSIGN(DataSlice nested_list_schema,
                         CreateListSchema(db1, list_schema));

    ASSERT_OK_AND_ASSIGN(auto l1, CreateEmptyList(db1, /*schema=*/list_schema));
    ASSERT_OK_AND_ASSIGN(auto l2, CreateEmptyList(db1, /*schema=*/list_schema));
    auto lists = test::DataSlice<internal::ObjectId>(
        {l1.item().value<internal::ObjectId>(),
         l2.item().value<internal::ObjectId>()},
        list_schema.item().value<internal::ObjectId>(), db1);
    ASSERT_OK_AND_ASSIGN(auto nested_list,
                         CreateNestedList(db1, lists, nested_list_schema));
    auto db2 = DataBag::Empty();
    // The list schema is adopted.
    ASSERT_OK(AdoptStub(db2, nested_list));
    auto expected_db = DataBag::Empty();
    ASSERT_OK(nested_list_schema.WithBag(expected_db)
                  .SetAttr(schema::kListItemsSchemaAttr, list_schema));
    ASSERT_OK(list_schema.WithBag(expected_db)
                  .SetAttr(schema::kListItemsSchemaAttr,
                           test::Schema(schema::kInt32)));
    // Write list-items as well for recursion.
    ASSERT_OK(expected_db->GetMutableImpl()->get().ReplaceInList(
        nested_list.item(), internal::DataBagImpl::ListRange(0),
        lists.slice()));
    ASSERT_OK(expected_db->GetMutableImpl()->get().ReplaceInList(
        l1.item(), internal::DataBagImpl::ListRange(0),
        internal::DataSliceImpl::CreateEmptyAndUnknownType(0)));
    ASSERT_OK(expected_db->GetMutableImpl()->get().ReplaceInList(
        l2.item(), internal::DataBagImpl::ListRange(0),
        internal::DataSliceImpl::CreateEmptyAndUnknownType(0)));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(expected_db->GetImpl()));
  }
}

TEST(AdoptStubTest, Dict) {
  {
    // Holding primitives.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(DataSlice schema,
                         CreateDictSchema(db1, test::Schema(schema::kInt32),
                                          test::Schema(schema::kFloat32)));
    ASSERT_OK_AND_ASSIGN(
        auto dict,
        CreateDictShaped(db1, DataSlice::JaggedShape::Empty(),
                         /*keys=*/std::nullopt, /*values=*/std::nullopt,
                         /*schema=*/schema));
    auto db2 = DataBag::Empty();
    // The dict schema is adopted.
    ASSERT_OK(AdoptStub(db2, dict));
    auto expected_db = DataBag::Empty();
    ASSERT_OK(schema.WithBag(expected_db)
                  .SetAttr(schema::kDictKeysSchemaAttr,
                           test::Schema(schema::kInt32)));
    ASSERT_OK(schema.WithBag(expected_db)
                  .SetAttr(schema::kDictValuesSchemaAttr,
                           test::Schema(schema::kFloat32)));
  }
  {
    // Recursive dicts.
    auto db1 = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(DataSlice schema,
                         CreateDictSchema(db1, test::Schema(schema::kInt32),
                                          test::Schema(schema::kFloat32)));
    ASSERT_OK_AND_ASSIGN(
        DataSlice nested_schema,
        CreateDictSchema(db1, test::Schema(schema::kInt32), schema));

    ASSERT_OK_AND_ASSIGN(
        auto dict,
        CreateDictShaped(db1, DataSlice::JaggedShape::Empty(),
                         /*keys=*/std::nullopt, /*values=*/std::nullopt,
                         /*schema=*/schema));
    ASSERT_OK_AND_ASSIGN(
        auto nested_dict,
        CreateDictShaped(db1, DataSlice::JaggedShape::Empty(),
                         /*keys=*/test::DataItem(1), /*values=*/dict,
                         /*schema=*/nested_schema));

    auto db2 = DataBag::Empty();
    // The dict schemas are adopted.
    ASSERT_OK(AdoptStub(db2, nested_dict));
    auto expected_db = DataBag::Empty();
    // Inner schema.
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetSchemaAttr(
        schema.item(), schema::kDictKeysSchemaAttr,
        internal::DataItem(schema::kInt32)));
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetSchemaAttr(
        schema.item(), schema::kDictValuesSchemaAttr,
        internal::DataItem(schema::kInt32)));
    // Outer schema.
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetSchemaAttr(
        nested_schema.item(), schema::kDictKeysSchemaAttr,
        internal::DataItem(schema::kInt32)));
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetSchemaAttr(
        nested_schema.item(), schema::kDictValuesSchemaAttr, schema.item()));
  }
}

TEST(AdoptStubTest, ImmutableError) {
  auto db1 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateListSchema(db1, test::Schema(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(auto list, CreateEmptyList(db1, /*schema=*/schema));
  ASSERT_OK_AND_ASSIGN(auto db2, DataBag::Empty()->Fork(/*immutable=*/true));
  EXPECT_THAT(AdoptStub(db2, list), StatusIs(absl::StatusCode::kInvalidArgument,
                                             HasSubstr("immutable DataBag")));
}

TEST(AdoptStubTest, EmptyAndUnknown) {
  // Checks that the evaluation halts (and succeeds) for slices that can be
  // interpreted as lists (item.IsList());
  {
    // OBJECT.
    auto db1 = DataBag::Empty();
    DataSlice item = test::DataItem(std::nullopt, schema::kObject, db1);
    auto db2 = DataBag::Empty();
    ASSERT_TRUE(item.IsList());
    // Nothing is adopted.
    ASSERT_OK(AdoptStub(db2, item));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(DataBag::Empty()->GetImpl()));
  }
  {
    // NONE.
    auto db1 = DataBag::Empty();
    DataSlice item = test::DataItem(std::nullopt, schema::kNone, db1);
    auto db2 = DataBag::Empty();
    ASSERT_TRUE(item.IsList());
    // Nothing is adopted.
    ASSERT_OK(AdoptStub(db2, item));
    EXPECT_THAT(db2->GetImpl(), DataBagEqual(DataBag::Empty()->GetImpl()));
  }
}

TEST(WithAdoptedValuesTest, WithAdoptedValues) {
  {
    // No bags.
    ASSERT_OK_AND_ASSIGN(auto db,
                         WithAdoptedValues(nullptr, test::DataItem(1)));
    EXPECT_EQ(db, nullptr);
  }
  {
    // One bag.
    auto db = DataBag::Empty()->Freeze();
    ASSERT_FALSE(db->IsMutable());
    // `db` is present.
    ASSERT_OK_AND_ASSIGN(auto result_bag,
                         WithAdoptedValues(db, test::DataItem(2)));
    EXPECT_EQ(result_bag, db);  // the same bag.
    // Second item has bag.
    ASSERT_OK_AND_ASSIGN(
        result_bag, WithAdoptedValues(nullptr, test::DataItem(2).WithBag(db)));
    EXPECT_EQ(result_bag, db);  // the same bag.
  }
  {
    // Both same bag.
    // One bag.
    auto db = DataBag::Empty()->Freeze();
    ASSERT_FALSE(db->IsMutable());
    ASSERT_OK_AND_ASSIGN(auto result_bag,
                         WithAdoptedValues(db, test::DataItem(2).WithBag(db)));
    EXPECT_EQ(result_bag, db);  // the same bag.
  }
  {
    // Both different bags.
    auto db = DataBag::Empty();
    internal::DataItem unused(internal::AllocateSingleObject());
    ASSERT_OK(db->GetMutableImpl()->get().SetAttr(unused, "unused",
                                                  internal::DataItem(1)));
    ASSERT_OK_AND_ASSIGN(
        auto e1, EntityCreator::FromAttrs(db, {"x"}, {test::DataItem(2)}));

    auto db2 = DataBag::Empty();
    internal::DataItem unused2(internal::AllocateSingleObject());
    ASSERT_OK(db2->GetMutableImpl()->get().SetAttr(unused2, "unused2",
                                                   internal::DataItem(3)));
    ASSERT_OK_AND_ASSIGN(auto e1_itemid,
                         e1.WithSchema(test::Schema(schema::kItemId)));
    ASSERT_OK_AND_ASSIGN(
        auto e2, EntityCreator::FromAttrs(db2, {"x"}, {test::DataItem(4)},
                                          /*schema=*/e1.GetSchema(),
                                          /*overwrite_schema=*/false,
                                          /*itemid=*/e1_itemid));

    ASSERT_OK_AND_ASSIGN(auto result_bag, WithAdoptedValues(db2, e1));

    auto expected_db = DataBag::Empty();
    // `e1` is extracted, so its unused value is removed. But `'x'` is included.
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetAttr(
        e1.item(), "x", internal::DataItem(2)));
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetSchemaAttr(
        e1.GetSchemaImpl(), "x", internal::DataItem(schema::kInt32)));
    // The contents of `db2` is not extracted but `e1` takes precedence.
    ASSERT_OK(expected_db->GetMutableImpl()->get().SetAttr(
        unused2, "unused2", internal::DataItem(3)));

    ASSERT_OK_AND_ASSIGN(auto merged_bag, result_bag->MergeFallbacks());
    EXPECT_THAT(merged_bag->GetImpl(), DataBagEqual(expected_db->GetImpl()));
  }
}

}  // namespace
}  // namespace koladata
