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
#include "koladata/internal/op_utils/schema_tracker.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsSliceBasic) {
  SchemaTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0, obj1}));
  DataItem schema(AllocateExplicitSchema());

  // First visit succeeds.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schema));
  ASSERT_OK_AND_ASSIGN(auto recorded_schemas, tracker.GetSchemas(ds));
  EXPECT_EQ(recorded_schemas[0], schema);
  EXPECT_EQ(recorded_schemas[1], schema);

  // Re-visiting with the same schema succeeds.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schema));

  // Re-visiting with a conflicting schema raises InvalidArgumentError.
  DataItem conflicting_schema(AllocateExplicitSchema());
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(ds, conflicting_schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));
}

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsSliceWithDuplicatesAndNulls) {
  SchemaTracker tracker;
  auto obj_a = AllocateSingleObject();
  auto obj_b = AllocateSingleObject();
  DataItem schema(AllocateExplicitSchema());

  // obj_a appears multiple times, alongside a nullopt.
  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({obj_a, std::nullopt, obj_b, obj_a}));

  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schema));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schema));

  // Conflicting schema on obj_a raises error.
  auto ds_a =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj_a}));
  DataItem conflicting_schema(AllocateExplicitSchema());
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(ds_a, conflicting_schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));
}

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsSchemasSlice) {
  SchemaTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto s0 = AllocateExplicitSchema();
  auto s1 = AllocateExplicitSchema();
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0, obj1}));
  auto schemas =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({s0, s1}));

  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schemas));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds, schemas));

  ASSERT_OK_AND_ASSIGN(auto recorded_schemas, tracker.GetSchemas(ds));
  EXPECT_EQ(recorded_schemas[0], DataItem(s0));
  EXPECT_EQ(recorded_schemas[1], DataItem(s1));

  auto conflicting_schemas =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({s0, s0}));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(ds, conflicting_schemas),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));
}

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsSliceEmptyAndInvalidInputs) {
  SchemaTracker tracker;
  ObjectId schema_id = AllocateExplicitSchema();
  DataItem schema(schema_id);

  // Empty and unknown slice.
  auto empty_ds = DataSliceImpl::CreateEmptyAndUnknownType(0);
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(empty_ds, schema));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(empty_ds, empty_ds));

  // Size mismatch between ds and schemas slice returns error.
  auto obj = AllocateSingleObject();
  auto obj_ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj}));
  auto schemas_2 = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({schema_id, schema_id}));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(obj_ds, schemas_2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("ds and schemas size mismatch: 1 vs 2")));

  // Non-ObjectId slice returns error (both DataItem and DataSliceImpl schemas).
  auto int_ds = DataSliceImpl::Create(arolla::CreateDenseArray<int>({1}));
  auto schemas_1 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({schema_id}));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(int_ds, schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId slice")));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(int_ds, schemas_1),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId slice")));

  // Missing or non-ObjectId schema returns error.
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(obj_ds, DataItem()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId schema")));
  EXPECT_THAT(
      tracker.SetSchemaCheckNoConflicts(obj_ds, DataItem(schema::kObject)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("expected ObjectId schema")));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(obj_ds, int_ds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId schemas slice")));
}

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsFullAllocation) {
  SchemaTracker tracker;
  AllocationId alloc = Allocate(4);
  auto full_ds = DataSliceImpl::ObjectsFromAllocation(alloc, 4);
  DataItem schema(AllocateExplicitSchema());

  // Full allocation uses SetAttrFullAlloc.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(full_ds, schema));

  // Re-visiting with same schema succeeds.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(full_ds, schema));

  // Conflicting schema on a sub-slice of that allocation raises error.
  auto sub_ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({alloc.ObjectByOffset(0)}));
  DataItem conflicting_schema(AllocateExplicitSchema());
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(sub_ds, conflicting_schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));

  // Full allocation with per-object schemas slice also uses SetAttrFullAlloc.
  AllocationId alloc2 = Allocate(2);
  auto full_ds2 = DataSliceImpl::ObjectsFromAllocation(alloc2, 2);
  ObjectId s0 = AllocateExplicitSchema();
  ObjectId s1 = AllocateExplicitSchema();
  auto schemas2 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({s0, s1}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(full_ds2, schemas2));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(full_ds2, schemas2));

  auto conflicting_schemas2 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({s0, s0}));
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(full_ds2, conflicting_schemas2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));
}

TEST(SchemaTrackerTest, NonFullBigAllocations) {
  SchemaTracker tracker;
  DataItem schema(AllocateExplicitSchema());

  // Multiple big allocations in one slice (alloc_ids.size() > 1).
  AllocationId alloc_a = Allocate(2);
  AllocationId alloc_b = Allocate(2);
  auto multi_alloc_ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_a.ObjectByOffset(0), alloc_b.ObjectByOffset(0)}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(multi_alloc_ds, schema));

  // Single big allocation of full capacity size, but with a missing element.
  AllocationId alloc_sparse = Allocate(2);
  auto sparse_ds = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
      {alloc_sparse.ObjectByOffset(0), std::nullopt}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(sparse_ds, schema));

  // Single big allocation combined with a small allocation ID
  // (alloc_ids.contains_small_allocation_id() && alloc_ids.size() == 1).
  auto mixed_small_and_big_ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_a.ObjectByOffset(0), AllocateSingleObject()}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(mixed_small_and_big_ds, schema));

  // Single big allocation of full capacity size, all present, but out of order.
  AllocationId alloc_reordered = Allocate(2);
  auto reordered_ds = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
      {alloc_reordered.ObjectByOffset(1), alloc_reordered.ObjectByOffset(0)}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(reordered_ds, schema));
}

TEST(SchemaTrackerTest, SetSchemaCheckNoConflictsItemBasic) {
  SchemaTracker tracker;
  auto obj = AllocateSingleObject();
  DataItem item(obj);
  DataItem schema(AllocateExplicitSchema());
  DataItem conflicting_schema(AllocateExplicitSchema());

  // First visit succeeds.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(item, schema));

  // Re-visiting with same schema succeeds.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(item, schema));

  // Conflicting schema raises error.
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(item, conflicting_schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("multiple schemas found")));

  // Empty item succeeds (no-op).
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(DataItem(), schema));

  // Non-ObjectId item returns error.
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(DataItem(42), schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId")));

  // Missing schema returns error.
  EXPECT_THAT(tracker.SetSchemaCheckNoConflicts(item, DataItem()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected ObjectId schema")));
}

TEST(SchemaTrackerTest, ObjectSchemaMaskTracking) {
  SchemaTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto obj2 = AllocateSingleObject();
  ObjectId schema_id = AllocateExplicitSchema();
  DataItem schema(schema_id);

  auto ds0 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj0}));
  auto ds1 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj1}));
  auto ds2 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj2}));
  auto schemas2 =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({schema_id}));
  auto ds_all = DataSliceImpl::Create(
      arolla::CreateDenseArray<ObjectId>({obj0, obj1, obj2}));

  // Initially obj0 is visited without object schema, obj1 and obj2 with object
  // schema (via DataItem and DataSliceImpl overloads respectively).
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds0, schema,
                                              /*is_object_schema=*/false));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds1, schema,
                                              /*is_object_schema=*/true));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds2, schemas2,
                                              /*is_object_schema=*/true));

  ASSERT_OK_AND_ASSIGN(auto mask1, tracker.GetObjectSchemaMask(ds_all));
  EXPECT_FALSE(mask1.present(0));
  EXPECT_TRUE(mask1.present(1));
  EXPECT_TRUE(mask1.present(2));

  // Later visiting obj0 with object schema sets its mask bit too.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds0, schema,
                                              /*is_object_schema=*/true));
  ASSERT_OK_AND_ASSIGN(auto mask2, tracker.GetObjectSchemaMask(ds_all));
  EXPECT_TRUE(mask2.present(0));
  EXPECT_TRUE(mask2.present(1));
  EXPECT_TRUE(mask2.present(2));
}

TEST(SchemaTrackerTest, GetSchemas) {
  SchemaTracker tracker;
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto obj_unvisited = AllocateSingleObject();
  auto s0 = AllocateExplicitSchema();
  auto s1 = AllocateExplicitSchema();

  auto query_ds = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
      {obj0, obj1, obj_unvisited, std::nullopt}));

  // Before any schemas are recorded, GetSchemas returns an all-missing slice.
  ASSERT_OK_AND_ASSIGN(auto initial_schemas, tracker.GetSchemas(query_ds));
  EXPECT_EQ(initial_schemas.size(), 4);
  EXPECT_EQ(initial_schemas.present_count(), 0);

  // Set schema for obj0 via DataItem overload and obj1 via DataSliceImpl
  // overload.
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(DataItem(obj0), DataItem(s0)));
  auto ds1 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>({obj1}));
  EXPECT_OK(tracker.SetSchemaCheckNoConflicts(ds1, DataItem(s1)));

  ASSERT_OK_AND_ASSIGN(auto schemas, tracker.GetSchemas(query_ds));
  EXPECT_EQ(schemas.size(), 4);
  EXPECT_EQ(schemas[0], DataItem(s0));
  EXPECT_EQ(schemas[1], DataItem(s1));
  EXPECT_FALSE(schemas[2].has_value());
  EXPECT_FALSE(schemas[3].has_value());
}

}  // namespace
}  // namespace koladata::internal
