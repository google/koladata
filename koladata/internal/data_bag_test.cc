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
#include "koladata/internal/data_bag.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/types_buffer.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::OptionalValue;
using ::koladata::internal::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Ne;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;

TEST(DataBagTest, Empty) {
  auto empty = DataBagImpl::CreateEmptyDatabag();
  EXPECT_TRUE(empty->IsPristine());

  DataBagImpl::ConstDenseSourceArray dense_sources;
  DataBagImpl::ConstSparseSourceArray sparse_sources;
  empty->GetAttributeDataSources(Allocate(1), "a", dense_sources,
                                 sparse_sources);

  EXPECT_THAT(dense_sources, IsEmpty());
  EXPECT_THAT(sparse_sources, IsEmpty());
}

TEST(DataBagTest, PariallyPersistentForkNoModifications) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(3);

  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  // PartiallyPersistentFork shouldn't create linear structure
  // if no modifications happen.
  // Would either timeout or go out of stack without such an optimization.
  for (int i = 0; i < (1 << 20); ++i) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));
    EXPECT_EQ(ds_a_get.size(), 3);
    db = db->PartiallyPersistentFork();
    EXPECT_TRUE(db->IsPristine());
  }
}

TEST(DataBagTest, DeepChainOfForks) {
  int64_t alloc_size = 10;
  int64_t set_size = 100;
  int64_t step = alloc_size / set_size;

  AllocationId alloc = Allocate(alloc_size);
  arolla::DenseArrayBuilder<ObjectId> objects_bldr(set_size);
  for (int64_t i = 0; i < set_size; ++i) {
    objects_bldr.Set(i, alloc.ObjectByOffset(i * step));
  }
  DataSliceImpl objects = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet(alloc), std::move(objects_bldr).Build());
  DataSliceImpl values = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<int32_t>(set_size, 57));

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(objects, "a", values));

  for (int i = 0; i < 1000'000; ++i) {
    auto old_db = db;
    db = db->PartiallyPersistentFork();
    ASSERT_OK(db->SetAttr(objects, "a", values));
    ASSERT_THAT(db, Ne(old_db));
    ASSERT_FALSE(db->IsPristine());
  }
}

TEST(DataBagTest, SetGet) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(DataSliceImpl::CreateEmptyAndUnknownType(3), "a",
                        DataSliceImpl::CreateEmptyAndUnknownType(3)));

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);

  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  DataBagImpl::ConstDenseSourceArray dense_sources;
  DataBagImpl::ConstSparseSourceArray sparse_sources;
  db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a", dense_sources,
                              sparse_sources);
  EXPECT_EQ(dense_sources.size(), 1);
  EXPECT_EQ(sparse_sources.size(), 0);

  ASSERT_OK_AND_ASSIGN(DataItem ds_a_5_item,
                       db->GetAttr(DataItem(ds.values<ObjectId>()[5]), "a"));
  EXPECT_EQ(ds_a_5_item.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds_a_5_item.value<ObjectId>(), ds_a.values<ObjectId>()[5].value);

  ASSERT_OK_AND_ASSIGN(DataItem ds_a_null_item, db->GetAttr(DataItem(), "a"));
  EXPECT_FALSE(ds_a_null_item.has_value());

  EXPECT_THAT(db->GetAttr(DataSliceImpl::CreateEmptyAndUnknownType(3), "a"),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(),
              ElementsAreArray(ds_a.allocation_ids()));
  EXPECT_EQ(ds_a_get.values<ObjectId>().size(), kSize);
  EXPECT_THAT(ds_a_get.values<ObjectId>(),
              ElementsAreArray(ds_a.values<ObjectId>()));
}

TEST(DataBagTest, GetAttrWithRemovedSlice) {
  for (int64_t size : {1, 3, 7, 13, 1023}) {
    SCOPED_TRACE(absl::StrCat("size: ", size));
    auto db = DataBagImpl::CreateEmptyDatabag();
    AllocationId alloc = Allocate(size);
    std::vector<DataItem> objects;
    for (int64_t i = 0; i < size; ++i) {
      objects.push_back(i % 3 == 2 ? DataItem()
                                   : DataItem(alloc.ObjectByOffset(i)));
    }
    auto ds = DataSliceImpl::Create(objects);
    std::vector<DataItem> items;
    for (int64_t i = 0; i < size; ++i) {
      items.push_back(i % 2 == 1 ? DataItem(i) : DataItem());
    }
    auto ds_a = DataSliceImpl::Create(items);

    ASSERT_OK(db->SetAttr(ds, "a", ds_a));

    ASSERT_OK_AND_ASSIGN(
        auto empty_get,
        db->GetAttrWithRemoved(DataSliceImpl::CreateEmptyAndUnknownType(size),
                               "a"));
    EXPECT_EQ(empty_get.size(), size);
    EXPECT_THAT(empty_get.allocation_ids(), IsEmpty());
    EXPECT_EQ(empty_get.types_buffer().size(), 0);

    auto all_objects = DataSliceImpl::ObjectsFromAllocation(alloc, size);
    ASSERT_OK_AND_ASSIGN(auto ds_a_get,
                         db->GetAttrWithRemoved(all_objects, "a"));

    EXPECT_EQ(ds_a_get.size(), size);
    EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
    EXPECT_EQ(ds_a_get.types_buffer().size(), size);
    EXPECT_EQ(ds_a_get.is_empty_and_unknown(), size == 1);
    for (int64_t i = 0; i < size; ++i) {
      bool is_set = i % 3 != 2;
      bool is_removed = i % 2 == 0;
      ASSERT_EQ(ds_a_get[i], is_set && !is_removed ? DataItem(i) : DataItem())
          << i;
      if (size != 1) {
        auto expected_typeidx = is_set
                                    ? (is_removed ? TypesBuffer::kRemoved : 0)
                                    : TypesBuffer::kUnset;
        ASSERT_EQ(ds_a_get.types_buffer().id_to_typeidx[i], expected_typeidx)
            << i;
      }
    }
  }
}

TEST(DataBagTest, GetAttrWithRemovedItem) {
  for (int64_t size : {1, 3, 27, 56, 64, 111}) {
    SCOPED_TRACE(absl::StrCat("size: ", size));
    auto db = DataBagImpl::CreateEmptyDatabag();
    std::vector<DataItem> objects;
    std::vector<ObjectId> object_ids;
    for (int64_t i = 0; i < size; ++i) {
      auto obj_id = size % 7 == 0 ? AllocateSingleObject()
                                  : Allocate(987).ObjectByOffset(i);
      objects.push_back(i % 3 == 2 ? DataItem() : DataItem(obj_id));
      object_ids.push_back(obj_id);
    }
    std::vector<DataItem> items;
    for (int64_t i = 0; i < size; ++i) {
      items.push_back(i % 2 == 1 ? DataItem(i) : DataItem());
    }

    ASSERT_OK(db->SetAttr(DataSliceImpl::Create(objects), "a",
                          DataSliceImpl::Create(items)));

    for (int64_t i = 0; i < size; ++i) {
      SCOPED_TRACE(absl::StrCat("i: ", i));
      ObjectId object_id = object_ids[i];
      std::optional<DataItem> item = db->GetAttrWithRemoved(object_id, "a");
      bool is_set = i % 3 != 2;
      bool is_removed = i % 2 == 0;
      if (is_set) {
        EXPECT_TRUE(item.has_value());
        if (is_removed) {
          EXPECT_FALSE(item->has_value());
        } else {
          EXPECT_TRUE(item->has_value());
          EXPECT_EQ(item.value(), DataItem(i));
        }
      } else {
        EXPECT_FALSE(item.has_value());
      }
    }
  }
}

TEST(DataBagTest, GetObjSchemaAttr) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);

  ASSERT_OK(db->SetAttr(ds, "__schema__", ds_a));
  ASSERT_OK_AND_ASSIGN(
      DataItem ds_a_5_item,
      db->GetObjSchemaAttr(DataItem(ds.values<ObjectId>()[5])));
  EXPECT_EQ(ds_a_5_item.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds_a_5_item.value<ObjectId>(), ds_a.values<ObjectId>()[5].value);

  ASSERT_OK_AND_ASSIGN(DataItem ds_a_null_item,
                       db->GetObjSchemaAttr(DataItem()));
  EXPECT_FALSE(ds_a_null_item.has_value());

  EXPECT_THAT(db->GetObjSchemaAttr(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetObjSchemaAttr(ds));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(),
              ElementsAreArray(ds_a.allocation_ids()));
  EXPECT_EQ(ds_a_get.values<ObjectId>().size(), kSize);
  EXPECT_THAT(ds_a_get.values<ObjectId>(),
              ElementsAreArray(ds_a.values<ObjectId>()));

  auto ds_missing_schema = DataSliceImpl::AllocateEmptyObjects(kSize);

  EXPECT_THAT(
      db->GetObjSchemaAttr(DataItem(ds_missing_schema.values<ObjectId>()[5])),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("missing __schema__ attribute")));
  EXPECT_THAT(db->GetObjSchemaAttr(ds_missing_schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("missing __schema__ attribute")));
}

TEST(DataBagTest, SetSchemaFieldsForEntireAllocation) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto int32 = DataItem(schema::kInt32);
  auto float32 = DataItem(schema::kFloat32);
  // Doesn't fail.
  ASSERT_OK(db->SetSchemaFieldsForEntireAllocation(
      AllocateExplicitSchemas(0), 0, {"a", "b"},
      {std::cref(int32), std::cref(float32)}));

  auto alloc_id = Allocate(kSize);
  auto ds = DataSliceImpl::ObjectsFromAllocation(alloc_id, kSize);
  auto schema_alloc =
      AllocationId(CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          alloc_id.ObjectByOffset(0), arolla::Fingerprint(57)));
  ASSERT_OK(db->SetSchemaFieldsForEntireAllocation(
      schema_alloc, kSize, {"a", "b"}, {std::cref(int32), std::cref(float32)}));

  auto ds_schema = DataSliceImpl::ObjectsFromAllocation(schema_alloc, kSize);
  ASSERT_OK(db->SetAttr(ds, "__schema__", ds_schema));
  auto new_schema_alloc =
      AllocationId(CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          alloc_id.ObjectByOffset(0), arolla::Fingerprint(75)));

  for (DataItem obj : ds) {
    ASSERT_OK_AND_ASSIGN(DataItem schema, db->GetAttr(obj, "__schema__"));
    ASSERT_NE(schema, DataItem()) << obj;
    ASSERT_EQ(schema, DataItem(schema_alloc.ObjectByOffset(
                     obj.value<ObjectId>().Offset())))
        << obj;
    ASSERT_OK_AND_ASSIGN(DataItem a, db->GetSchemaAttr(schema, "a"));
    ASSERT_EQ(a, int32) << obj;
    ASSERT_OK_AND_ASSIGN(DataItem b, db->GetSchemaAttr(schema, "b"));
    ASSERT_EQ(b, float32) << obj;

    // Overwrite to make sure that schemas are independent.
    ASSERT_OK(db->SetSchemaAttr(schema, "a",
                                DataItem(new_schema_alloc.ObjectByOffset(
                                    schema.value<ObjectId>().Offset()))));
  }

  for (DataItem obj : ds) {
    ASSERT_OK_AND_ASSIGN(DataItem schema, db->GetAttr(obj, "__schema__"));
    ASSERT_NE(schema, DataItem()) << obj;
    ASSERT_OK_AND_ASSIGN(DataItem a, db->GetSchemaAttr(schema, "a"));
    ASSERT_NE(a, int32) << obj;
    ASSERT_EQ(a, DataItem(new_schema_alloc.ObjectByOffset(
                     schema.value<ObjectId>().Offset())))
        << obj;
  }

  // Make sure we can overwrite existent schemas.
  ASSERT_OK(db->SetSchemaFieldsForEntireAllocation(
      schema_alloc, kSize, {"a", "b"}, {std::cref(float32), std::cref(int32)}));
  for (DataItem obj : ds) {
    ASSERT_OK_AND_ASSIGN(DataItem schema, db->GetAttr(obj, "__schema__"));
    ASSERT_NE(schema, DataItem()) << obj;
    ASSERT_OK_AND_ASSIGN(DataItem a, db->GetSchemaAttr(schema, "a"));
    ASSERT_EQ(a, float32) << obj;
    ASSERT_OK_AND_ASSIGN(DataItem b, db->GetSchemaAttr(schema, "b"));
    ASSERT_EQ(b, int32) << obj;
  }
}

TEST(DataBagTest, GetSchemaAttrs) {
  {
    // Empty schema.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(DataSliceImpl schema_attrs,
                         db->GetSchemaAttrs(schema));
    EXPECT_TRUE(schema_attrs.is_empty_and_unknown());
  }
  {
    // Single schema allocation.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    ASSERT_OK(db->SetSchemaAttr(schema, "value", DataItem(schema::kInt32)));
    ASSERT_OK(db->SetSchemaAttr(schema, "self", schema));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl schema_attrs,
                         db->GetSchemaAttrs(schema));
    EXPECT_EQ(schema_attrs.dtype(), arolla::GetQType<arolla::Text>());
    EXPECT_THAT(schema_attrs.values<arolla::Text>(),
                UnorderedElementsAre("value", "self"));
  }
  for (size_t size : {kSmallAllocMaxCapacity, kSmallAllocMaxCapacity + 1}) {
    SCOPED_TRACE(absl::StrCat("size: ", size));
    // Multiple schemas allocations.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema_ds = DataSliceImpl::ObjectsFromAllocation(
        AllocateExplicitSchemas(size), size);
    ASSERT_OK(db->SetSchemaAttr(schema_ds, "self", schema_ds));
    for (size_t i = 0; i < size; ++i) {
      ASSERT_OK(db->SetSchemaAttr(schema_ds[i], "a" + std::to_string(i),
                                  DataItem(schema::kInt32)));
    }
    for (size_t i = 0; i < size; ++i) {
      ASSERT_OK_AND_ASSIGN(DataSliceImpl schema_attrs,
                           db->GetSchemaAttrs(schema_ds[i]));
      EXPECT_THAT(
          schema_attrs,
          UnorderedElementsAre(DataItem(arolla::Text("a" + std::to_string(i))),
                               DataItem(arolla::Text("self"))));
    }
  }
}

TEST(DataBagTest, GetSchemaAttrsAsVector) {
  {
    // Empty schema.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(DataSliceImpl schema_attrs,
                         db->GetSchemaAttrs(schema));
    EXPECT_TRUE(schema_attrs.is_empty_and_unknown());
  }
  {
    // Single schema allocation.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    ASSERT_OK(db->SetSchemaAttr(schema, "value", DataItem(schema::kInt32)));
    ASSERT_OK(db->SetSchemaAttr(schema, "self", schema));
    ASSERT_OK_AND_ASSIGN(std::vector<DataItem> schema_attrs,
                         db->GetSchemaAttrsAsVector(schema));
    EXPECT_THAT(schema_attrs,
                UnorderedElementsAre(DataItem::View<arolla::Text>("value"),
                                     DataItem::View<arolla::Text>("self")));
  }
  for (size_t size : {kSmallAllocMaxCapacity, kSmallAllocMaxCapacity + 1}) {
    // Multiple schemas allocations.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema_ds = DataSliceImpl::ObjectsFromAllocation(
        AllocateExplicitSchemas(size), size);
    ASSERT_OK(db->SetSchemaAttr(schema_ds, "self", schema_ds));
    for (size_t i = 0; i < size; ++i) {
      ASSERT_OK(db->SetSchemaAttr(schema_ds[i], "a" + std::to_string(i),
                                  DataItem(schema::kInt32)));
    }
    for (size_t i = 0; i < size; ++i) {
      ASSERT_OK_AND_ASSIGN(std::vector<DataItem> schema_attrs,
                           db->GetSchemaAttrsAsVector(schema_ds[i]));
      EXPECT_THAT(schema_attrs,
                  UnorderedElementsAre(
                      DataItem::View<arolla::Text>("a" + std::to_string(i)),
                      DataItem::View<arolla::Text>("self")));
    }
  }
}

TEST(DataBagTest, GetSchemaAttrsForBigAllocationAsVector) {
  {
    // Big allocation.
    size_t size = kSmallAllocMaxCapacity + 1;
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto alloc = AllocateExplicitSchemas(size);
    auto schema_ds = DataSliceImpl::ObjectsFromAllocation(alloc, size);
    ASSERT_OK(db->SetSchemaAttr(schema_ds, "self", schema_ds));
    for (size_t i = 0; i < size; ++i) {
      if (i % 2 == 0) {
        ASSERT_OK(db->SetSchemaAttr(schema_ds[i], "a_mixed",
                                    DataItem(schema::kInt32)));
        ASSERT_OK(
            db->SetSchemaAttr(schema_ds[i], "b_int", DataItem(schema::kInt32)));
      } else {
        ASSERT_OK(db->SetSchemaAttr(schema_ds[i], "a_mixed",
                                    DataItem(schema::kFloat32)));
        ASSERT_OK(db->SetSchemaAttr(schema_ds[i], "c_float",
                                    DataItem(schema::kFloat32)));
      }
    }
    std::vector<DataItem> schema_attrs =
        db->GetSchemaAttrsForBigAllocationAsVector(alloc);
    EXPECT_THAT(schema_attrs,
                UnorderedElementsAre(DataItem::View<arolla::Text>("self"),
                                     DataItem::View<arolla::Text>("a_mixed"),
                                     DataItem::View<arolla::Text>("b_int"),
                                     DataItem::View<arolla::Text>("c_float")));
  }
}

TEST(DataBagTest, GetSchemaAttrAllowMissing) {
  {
    // Small allocations.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schemas = DataSliceImpl::ObjectsFromAllocation(
        AllocateExplicitSchemas(kSmallAllocMaxCapacity),
        kSmallAllocMaxCapacity);
    auto schema = DataItem(AllocateExplicitSchema());
    auto schema_ds = DataSliceImpl::Create(
        {schemas[0], schema, schemas[1], schemas[1], schema, schema});
    ASSERT_OK(db->SetSchemaAttr(schema_ds, "self", schema_ds));
    ASSERT_OK_AND_ASSIGN(
        DataSliceImpl schema_attrs,
        db->GetSchemaAttrAllowMissing(schema_ds, "self", {}));
    EXPECT_THAT(schema_attrs, IsEquivalentTo(schema_ds));
  }
  {
    // Big allocations.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schemas_a = DataSliceImpl::ObjectsFromAllocation(
        AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1),
        kSmallAllocMaxCapacity + 1);
    auto schemas_b = DataSliceImpl::ObjectsFromAllocation(
        AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1),
        kSmallAllocMaxCapacity + 1);
    auto schema_ds = DataSliceImpl::Create(
        {schemas_a[0], schemas_b[0], schemas_b[1], schemas_a[1], schemas_b[0]});
    ASSERT_OK(db->SetSchemaAttr(schema_ds, "self", schema_ds));
    ASSERT_OK_AND_ASSIGN(
        DataSliceImpl schema_attrs,
        db->GetSchemaAttrAllowMissing(schema_ds, "self", {}));
    EXPECT_THAT(schema_attrs, IsEquivalentTo(schema_ds));
  }
}

TEST(DataBagTest, SetSchemaName) {
  {
    // Valid name.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    auto schema_name = DataItem(arolla::Text("foo"));
    ASSERT_OK(db->SetSchemaAttr(schema, schema::kSchemaNameAttr, schema_name));
    ASSERT_OK_AND_ASSIGN(DataItem assigned_name,
                         db->GetSchemaAttr(schema, schema::kSchemaNameAttr));
    EXPECT_EQ(schema_name, assigned_name);
  }
  {
    // Invalid name;
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    auto schema_name = DataItem(1);
    EXPECT_THAT(db->SetSchemaAttr(schema, schema::kSchemaNameAttr, schema_name),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("only Text can be used as a schema name")));
  }
}

TEST(DataBagTest, SetSchemaMetadata) {
  {
    // Valid metadata.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(
        auto schema_metadata,
        CreateUuidWithMainObject(schema, schema::kMetadataSeed));
    ASSERT_OK(db->SetSchemaAttr(schema, schema::kSchemaMetadataAttr,
                                schema_metadata));
    ASSERT_OK_AND_ASSIGN(
        DataItem assigned_metadata,
        db->GetSchemaAttr(schema, schema::kSchemaMetadataAttr));
    EXPECT_EQ(schema_metadata, assigned_metadata);
  }
  {
    // Invalid metadata;
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto schema = DataItem(AllocateExplicitSchema());
    auto schema_metadata = DataItem(1);
    EXPECT_THAT(
        db->SetSchemaAttr(schema, schema::kSchemaMetadataAttr, schema_metadata),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("only ItemId can be used as a schema metadata")));
  }
}

TEST(DataBagTest, GetAttrPrimitivesErrors) {
  {
    // DataSliceImpl - only primitives.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db_f = DataBagImpl::CreateEmptyDatabag();
    auto ds = DataSliceImpl::Create({internal::DataItem(1)});
    EXPECT_THAT(db->GetAttr(ds, "a", {db_f.get()}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         "getting attributes of primitives is not allowed"));
  }
  {
    // DataSliceImpl - mix.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db_f = DataBagImpl::CreateEmptyDatabag();
    auto ds = DataSliceImpl::Create(
        {DataItem(1), DataItem(internal::AllocateSingleObject())});
    EXPECT_THAT(db->GetAttr(ds, "a", {db_f.get()}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         "getting attributes of primitives is not allowed"));
  }
  {
    // DataItem.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db_f = DataBagImpl::CreateEmptyDatabag();
    EXPECT_THAT(db->GetAttr(internal::DataItem(1), "a", {db_f.get()}),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         "getting attribute of a primitive is not allowed"));
  }
}

TEST(DataBagTest, SetGetWithFallbackObjectId) {
  for (size_t size : {1, 2, 4, 17, 126}) {
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto db_f = DataBagImpl::CreateEmptyDatabag();

    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));

    {  // fallback is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db->GetAttr(ds, "a", {db_f.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
      EXPECT_THAT(ds_a_get.allocation_ids(),
                  ElementsAreArray(ds_a.allocation_ids()));
      EXPECT_THAT(ds_a_get.values<ObjectId>(),
                  ElementsAreArray(ds_a.values<ObjectId>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    {  // main is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db_f->GetAttr(ds, "a", {db.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
      EXPECT_THAT(ds_a_get.allocation_ids(),
                  ElementsAreArray(ds_a.allocation_ids()));
      EXPECT_THAT(ds_a_get.values<ObjectId>(),
                  ElementsAreArray(ds_a.values<ObjectId>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    // General case
    std::vector<arolla::OptionalValue<ObjectId>> main_b_ids(size);
    std::vector<arolla::OptionalValue<ObjectId>> main_b_values(size);
    std::vector<arolla::OptionalValue<ObjectId>> fallback_b_ids(size);
    std::vector<arolla::OptionalValue<ObjectId>> fallback_b_values(size);
    std::vector<arolla::OptionalValue<ObjectId>> merge_b_values(size);
    AllocationId alloc_b1 = Allocate(size);
    AllocationId alloc_b2 = Allocate(size);
    for (size_t i = 0; i < size; ++i) {
      ObjectId id1 = alloc_b1.ObjectByOffset(i);
      ObjectId id2 = alloc_b2.ObjectByOffset(i);
      if (int group = i % 5; group == 0) {
        main_b_ids[i] = ds[i].value<ObjectId>();
        main_b_values[i] = id1;
        merge_b_values[i] = id1;
      } else if (group == 1) {
        fallback_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_values[i] = id2;
        merge_b_values[i] = id2;
      } else if (group == 2) {
        main_b_ids[i] = ds[i].value<ObjectId>();
        main_b_values[i] = id1;
        fallback_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_values[i] = id2;
        merge_b_values[i] = id1;
      } else if (group == 3) {
        // Note: main_b_values contains removed values, fallback not used
        main_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_values[i] = id2;
      }
    }
    auto main_ds_ids_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(main_b_ids));
    auto main_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(main_b_values));
    ASSERT_OK(db->SetAttr(main_ds_ids_b, "b", main_ds_b));
    auto fallback_ds_ids_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(fallback_b_ids));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(fallback_ds_ids_b, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(merge_b_values));

    EXPECT_EQ(ds_b_get.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(ds_b_get.allocation_ids(),
                ElementsAreArray(merge_ds_b.allocation_ids()));
    EXPECT_THAT(ds_b_get.values<ObjectId>(),
                ElementsAreArray(merge_ds_b.values<ObjectId>()));
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetWithFallbackPrimitive) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db_f = DataBagImpl::CreateEmptyDatabag();

  {
    auto ds = DataSliceImpl::AllocateEmptyObjects(3);

    auto ds_a = DataSliceImpl::Create(
        arolla::CreateFullDenseArray<double>({1.0, 2.0, 3.0}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));

    {  // fallback is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db->GetAttr(ds, "a", {db_f.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<double>());
      EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
      EXPECT_THAT(ds_a_get.values<double>(),
                  ElementsAreArray(ds_a.values<double>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }

    {  // main is empty
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get,
                           db_f->GetAttr(ds, "a", {db.get()}));
      EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<double>());
      EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
      EXPECT_THAT(ds_a_get.values<double>(),
                  ElementsAreArray(ds_a.values<double>()));
      EXPECT_THAT(ds_a_get, IsEquivalentTo(ds_a));
    }
  }

  // General case
  for (size_t size : {1, 2, 4, 17, 126}) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    std::vector<arolla::OptionalValue<int64_t>> main_b_values(size);
    std::vector<arolla::OptionalValue<int64_t>> fallback_b_values(size);
    std::vector<arolla::OptionalValue<int64_t>> merge_b_values(size);
    for (size_t i = 0; i < size; ++i) {
      int64_t val1 = i + 1;
      int64_t val2 = -i - 1;
      if (i % 4 == 0) {
        main_b_values[i] = val1;
        merge_b_values[i] = val1;
      } else if (i % 4 == 1) {
        fallback_b_values[i] = val2;
        // Note: main_b_values contains removed values, fallback not used
      } else if (i % 4 == 2) {
        main_b_values[i] = val1;
        fallback_b_values[i] = val2;
        merge_b_values[i] = val1;
      }
    }
    auto main_ds_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(main_b_values));
    ASSERT_OK(db->SetAttr(ds, "b", main_ds_b));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(ds, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(merge_b_values));

    EXPECT_EQ(ds_b_get.dtype(), arolla::GetQType<int64_t>());
    EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_b_get.values<int64_t>(),
                ElementsAreArray(merge_ds_b.values<int64_t>()));
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetManyAllocations) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  // We use big size to test that there is no quadratic complexity in the
  // implementation.
  constexpr int kBigSize = 50239;

  std::vector<DataItem> objs;
  objs.reserve(kBigSize);
  std::vector<DataItem> values;
  values.reserve(kBigSize);

  for (int64_t size : {1, 2, 4, 17, 126, 1034, 5234, kBigSize}) {
    for (int64_t i = static_cast<int64_t>(objs.size()); i < size; ++i) {
      objs.push_back(DataItem(Allocate(37 * size).ObjectByOffset(i)));
      values.push_back(DataItem(i * 17));
    }
    CHECK_EQ(objs.size(), size);
    CHECK_EQ(values.size(), size);
    ASSERT_OK(db->SetAttr(DataSliceImpl::Create(objs), "a",
                          DataSliceImpl::Create(values)));
    ASSERT_OK_AND_ASSIGN(auto ds_a,
                         db->GetAttr(DataSliceImpl::Create(objs), "a"));
    EXPECT_THAT(ds_a, ElementsAreArray(values));
  }
}

TEST(DataBagTest, SetGetWithFallbackPrimitiveMixedType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db_f = DataBagImpl::CreateEmptyDatabag();

  for (size_t size : {1, 2, 4, 17, 126}) {
    auto ds = DataSliceImpl::AllocateEmptyObjects(size);

    std::vector<arolla::OptionalValue<ObjectId>> main_b_ids(size);
    std::vector<arolla::OptionalValue<int64_t>> main_b_values(size);
    std::vector<arolla::OptionalValue<ObjectId>> fallback_b_ids(size);
    std::vector<arolla::OptionalValue<double>> fallback_b_values(size);

    std::vector<arolla::OptionalValue<int64_t>> merge_b_values_int64(size);
    std::vector<arolla::OptionalValue<double>> merge_b_values_double(size);
    for (size_t i = 0; i < size; ++i) {
      int64_t val1 = i + 1;
      double val2 = -i - 1;
      if (i % 4 == 0) {
        main_b_ids[i] = ds[i].value<ObjectId>();
        main_b_values[i] = val1;
        merge_b_values_int64[i] = val1;
      } else if (i % 4 == 1) {
        fallback_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_values[i] = val2;
        merge_b_values_double[i] = val2;
      } else if (i % 4 == 2) {
        main_b_ids[i] = ds[i].value<ObjectId>();
        fallback_b_ids[i] = ds[i].value<ObjectId>();
        main_b_values[i] = val1;
        fallback_b_values[i] = val2;
        merge_b_values_int64[i] = val1;
      }
    }
    auto main_ds_ids_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(main_b_ids));
    auto main_ds_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(main_b_values));
    ASSERT_OK(db->SetAttr(main_ds_ids_b, "b", main_ds_b));
    auto fallback_ds_ids_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<ObjectId>(fallback_b_ids));
    auto fallback_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<double>(fallback_b_values));
    ASSERT_OK(db_f->SetAttr(fallback_ds_ids_b, "b", fallback_ds_b));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get,
                         db->GetAttr(ds, "b", {db_f.get()}));
    auto merge_ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int64_t>(merge_b_values_int64),
        arolla::CreateDenseArray<double>(merge_b_values_double));

    EXPECT_EQ(ds_b_get.is_mixed_dtype(), size > 1);
    EXPECT_EQ(ds_b_get.dtype(), size == 1 ? arolla::GetQType<int64_t>()
                                          : arolla::GetNothingQType())
        << size;
    EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_b_get, IsEquivalentTo(merge_ds_b));
  }
}

TEST(DataBagTest, SetGetEmptyAndUnknownType) {
  constexpr int kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto empty_ds = DataSliceImpl::CreateEmptyAndUnknownType(kSize);
  auto int_ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<int64_t>({2, std::nullopt, 42}));

  ASSERT_OK(db->SetAttr(ds, "attr", empty_ds));
  EXPECT_THAT(db->GetAttr(ds, "attr"),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));

  EXPECT_THAT(db->GetAttr(DataItem(ds[0]), "attr"), IsOkAndHolds(DataItem()));

  ASSERT_OK(db->SetAttr(ds, "attr", int_ds));
  EXPECT_THAT(db->GetAttr(ds, "attr"),
              IsOkAndHolds(ElementsAre(2, DataItem(), 42)));
}

TEST(DataBagTest, SingleSparseSourceToDense) {
  constexpr size_t kAllocSize = 10000;
  auto db = DataBagImpl::CreateEmptyDatabag();

  AllocationId alloc = Allocate(kAllocSize);

  {  // Set 5 of 10000 -> create sparse source
    auto ds = DataSliceImpl::ObjectsFromAllocation(alloc, 5);
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(5, 57));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {  // Set 5000 of 10000 -> convert sparse source to dense source
    SliceBuilder objs_bldr(kAllocSize / 2);
    SliceBuilder values_bldr(kAllocSize / 2);
    for (size_t i = 0; i < kAllocSize / 2; ++i) {
      objs_bldr.InsertIfNotSetAndUpdateAllocIds(
          i, DataItem(alloc.ObjectByOffset(i + kAllocSize / 2)));
      values_bldr.InsertIfNotSetAndUpdateAllocIds(
          i, DataItem(static_cast<int>(i + kAllocSize / 2)));
    }
    ASSERT_OK(db->SetAttr(std::move(objs_bldr).Build(), "a",
                          std::move(values_bldr).Build()));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(
        DataItem item,
        db->GetAttr(DataItem(alloc.ObjectByOffset(kAllocSize - 1)), "a"));
    EXPECT_EQ(item, DataItem(static_cast<int>(kAllocSize - 1)));
  }
}

TEST(DataBagTest, SparseSource) {
  constexpr size_t kAllocSize = 10000;
  auto db = DataBagImpl::CreateEmptyDatabag();

  AllocationId alloc = Allocate(kAllocSize);

  {  // Set 5 of 10000 -> create sparse source
    auto ds = DataSliceImpl::ObjectsFromAllocation(alloc, 5);
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(5, 57));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 1);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem());
  }

  auto db2 = db->PartiallyPersistentFork();

  {  // Sparse source from parent is used.
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 1);
  }

  // Set 1 of 10000 -> create sparse source
  ASSERT_OK(db2->SetAttr(DataItem(alloc.ObjectByOffset(7)), "a", DataItem(43)));

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 0);
    EXPECT_EQ(sparse_sources.size(), 2);
  }

  {  // Set 5000 of 10000 -> create dense source and merge sparse sources
    SliceBuilder objs_bldr(kAllocSize / 2);
    SliceBuilder values_bldr(kAllocSize / 2);
    for (size_t i = 0; i < kAllocSize / 2; ++i) {
      objs_bldr.InsertIfNotSetAndUpdateAllocIds(
          i, DataItem(alloc.ObjectByOffset(i + kAllocSize / 2)));
      values_bldr.InsertIfNotSetAndUpdateAllocIds(
          i, DataItem(static_cast<int>(i + kAllocSize / 2)));
    }
    ASSERT_OK(db2->SetAttr(std::move(objs_bldr).Build(), "a",
                           std::move(values_bldr).Build()));
  }

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db2->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(57));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem(43));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db2->GetAttr(DataItem(alloc.ObjectByOffset(8)), "a"));
    EXPECT_EQ(item, DataItem());
  }
  {
    ASSERT_OK_AND_ASSIGN(
        DataItem item,
        db2->GetAttr(DataItem(alloc.ObjectByOffset(kAllocSize / 2 + 1)), "a"));
    EXPECT_EQ(item, DataItem(static_cast<int>(kAllocSize / 2 + 1)));
  }

  // Set 1 of 10000 -> use existing dense source
  ASSERT_OK(
      db2->SetAttr(DataItem(alloc.ObjectByOffset(3)), "a", DataItem(3.14f)));

  auto db3 = db2->PartiallyPersistentFork();

  {
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db3->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db3->GetAttr(DataItem(alloc.ObjectByOffset(3)), "a"));
    EXPECT_EQ(item, DataItem(3.14f));
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem item,
                         db3->GetAttr(DataItem(alloc.ObjectByOffset(7)), "a"));
    EXPECT_EQ(item, DataItem(43));
  }
}

AllocationIdSet ConcatAllocations(const DataSliceImpl& ds,
                                  const DataSliceImpl& ds2) {
  std::vector<AllocationId> allocation_ids_union;
  allocation_ids_union.insert(allocation_ids_union.end(),
                              ds.allocation_ids().begin(),
                              ds.allocation_ids().end());
  allocation_ids_union.insert(allocation_ids_union.end(),
                              ds2.allocation_ids().begin(),
                              ds2.allocation_ids().end());
  return AllocationIdSet(allocation_ids_union);
}

std::vector<OptionalValue<ObjectId>> ConcatObjects(const DataSliceImpl& ds,
                                                   const DataSliceImpl& ds2) {
  std::vector<OptionalValue<ObjectId>> objects_union;
  objects_union.insert(objects_union.end(), ds.values<ObjectId>().begin(),
                       ds.values<ObjectId>().end());
  objects_union.insert(objects_union.end(), ds2.values<ObjectId>().begin(),
                       ds2.values<ObjectId>().end());
  return objects_union;
}

TEST(DataBagTest, SetGetTwoAllocationIds) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_OK(db->SetAttr(ds, "a", ds_a));

  auto ds2 = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds2_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  ASSERT_OK(db->SetAttr(ds2, "a", ds2_a));

  auto ds_union = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateDenseArray<ObjectId>(ConcatObjects(ds, ds2)),
      ConcatAllocations(ds, ds2));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds_union, "a"));

  auto db2 = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db2->SetAttr(ds_union, "a", ds_a_get));

  for (auto& db_cur : {db.get(), db2.get()}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get_cur,
                         db_cur->GetAttr(ds_union, "a"));
    EXPECT_EQ(ds_a_get_cur.size(), kSize * 2);
    EXPECT_EQ(ds_a_get_cur.dtype(), arolla::GetQType<ObjectId>());
    EXPECT_THAT(ds_a_get_cur.allocation_ids(),
                ElementsAreArray(ConcatAllocations(ds_a, ds2_a)));
    EXPECT_EQ(ds_a_get_cur.values<ObjectId>().size(), kSize * 2);
    EXPECT_THAT(ds_a_get_cur.values<ObjectId>(),
                ElementsAreArray(ConcatObjects(ds_a, ds2_a)));
  }
}

TEST(DataBagTest, SetGetPrimitiveOverrideRemoveViaFork) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);

  for (int i = 0; i < 3; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(
        std::vector{arolla::OptionalValue<int64_t>{i == 0, 1},
                    arolla::OptionalValue<int64_t>{i == 1, 2},
                    arolla::OptionalValue<int64_t>{i == 2, 3}}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
  }

  {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_0_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[0]), "a"));
    EXPECT_FALSE(ds_a_0_item.has_value());
  }
  {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_2_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[2]), "a"));
    ASSERT_EQ(ds_a_2_item.dtype(), arolla::GetQType<int64_t>());
    EXPECT_EQ(ds_a_2_item.value<int64_t>(), 3);
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(),
              ElementsAre(std::nullopt, std::nullopt, 3));

  {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>(
        std::vector{arolla::OptionalValue<int64_t>(std::nullopt),
                    arolla::OptionalValue<int64_t>{5},
                    arolla::OptionalValue<int64_t>{7}}));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
    ASSERT_OK_AND_ASSIGN(ds_a_get, db->GetAttr(ds, "a"));
    EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAre(std::nullopt, 5, 7));
  }
}

TEST(DataBagTest, SetGetMixedTypes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(5);
  auto a0 = obj_ids[0];
  auto s0 = DataItem(AllocateSingleObject());
  auto ds = DataSliceImpl::Create(
      arolla::CreateDenseArray<DataItem>({a0, DataItem(3), s0}));
  auto ds_f = DataSliceImpl::Create(
      arolla::CreateDenseArray<DataItem>({a0, std::nullopt, s0}));
  auto ds_ff =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, s0}));
  ASSERT_OK(db->SetAttr(ds_f, "a", ds));
  ASSERT_OK_AND_ASSIGN(auto result, db->GetAttr(ds_ff, "a"));
  EXPECT_TRUE(result.is_single_dtype());
  EXPECT_OK(
      result.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
        if constexpr (std::is_same_v<T, int32_t>) {
          return absl::InternalError(absl::StrCat(
              "get an unexpected int array: ", DataSliceImpl::Create(array)));
        }
        return absl::OkStatus();
      }));
}

TEST(DataBagTest, SetGetPrimitiveOverrideValueViaFork) {
  constexpr int64_t kSize = 4;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);

  for (int i = 0; i < 4; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_cur_obj = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                ds.values<ObjectId>()[i], ds.values<ObjectId>()[3 - i]}),
        ds.allocation_ids());
    auto ds_a =
        DataSliceImpl::Create(arolla::CreateDenseArray<int64_t>({i, i}));
    ASSERT_OK(db->SetAttr(ds_cur_obj, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<int64_t>());
  EXPECT_THAT(ds_a_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_a_get.values<int64_t>(), ElementsAre(3, 2, 2, 3));
}

TEST(DataBagTest, SetGetObjectsOverrideRemoveViaFork) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  AllocationId a_objects = Allocate(8);

  for (int i = 0; i < 3; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_a = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector{arolla::OptionalValue<ObjectId>{
                            i == 0, a_objects.ObjectByOffset(0)},
                        arolla::OptionalValue<ObjectId>{
                            i == 1, a_objects.ObjectByOffset(1)},
                        arolla::OptionalValue<ObjectId>{
                            i == 2, a_objects.ObjectByOffset(2)}}),
        AllocationIdSet(a_objects));
    ASSERT_OK(db->SetAttr(ds, "a", ds_a));
    DataBagImpl::ConstDenseSourceArray dense_sources;
    DataBagImpl::ConstSparseSourceArray sparse_sources;
    db->GetAttributeDataSources(ds.allocation_ids().ids()[0], "a",
                                dense_sources, sparse_sources);
    EXPECT_EQ(dense_sources.size(), 1);
    EXPECT_EQ(sparse_sources.size(), 0);
  }

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK_AND_ASSIGN(DataItem ds_a_item,
                         db->GetAttr(DataItem(ds.values<ObjectId>()[i]), "a"));
    if (i == 2) {
      EXPECT_EQ(ds_a_item.value<ObjectId>(), a_objects.ObjectByOffset(2)) << i;
    } else {
      EXPECT_FALSE(ds_a_item.holds_value<ObjectId>()) << i;
    }
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(), ElementsAre(a_objects));
  EXPECT_THAT(
      ds_a_get.values<ObjectId>(),
      ElementsAre(std::nullopt, std::nullopt, a_objects.ObjectByOffset(2)));
}

TEST(DataBagTest, SetGetObjectsOverrideNewValueViaFork) {
  constexpr int64_t kSize = 4;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
  AllocationId a_objects = Allocate(8);

  for (int i = 0; i < 4; ++i) {
    db = db->PartiallyPersistentFork();
    auto ds_cur_obj = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                ds.values<ObjectId>()[i], ds.values<ObjectId>()[3 - i]}),
        ds.allocation_ids());
    auto ds_a = DataSliceImpl::CreateObjectsDataSlice(
        arolla::CreateDenseArray<ObjectId>(
            std::vector<arolla::OptionalValue<ObjectId>>{
                a_objects.ObjectByOffset(i), a_objects.ObjectByOffset(i)}),
        AllocationIdSet(a_objects));
    ASSERT_OK(db->SetAttr(ds_cur_obj, "a", ds_a));
  }

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_a_get, db->GetAttr(ds, "a"));

  EXPECT_EQ(ds_a_get.size(), kSize);
  EXPECT_EQ(ds_a_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_a_get.allocation_ids(), ElementsAre(a_objects));
  EXPECT_THAT(
      ds_a_get.values<ObjectId>(),
      ElementsAre(a_objects.ObjectByOffset(3), a_objects.ObjectByOffset(2),
                  a_objects.ObjectByOffset(2), a_objects.ObjectByOffset(3)));
}

TEST(DataBagTest, CreateObjectDataItemGet) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a = DataItem(AllocateSingleObject());
  auto ds_b = DataItem(57);
  auto ds_c = DataItem();
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      db->CreateObjectsFromFields(
          {"a", "b", "c"},
          {std::cref(ds_a), std::cref(ds_b), std::cref(ds_c)}));
  ASSERT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());

  AllocationId alloc(ds.value<ObjectId>());
  EXPECT_TRUE(alloc.IsSmall());

  for (auto [attr, expected_ds] :
       std::vector{std::pair{"a", ds_a}, std::pair{"b", ds_b},
                   std::pair{"c", ds_c}}) {
    ASSERT_OK_AND_ASSIGN(auto ds_get, db->GetAttr(ds, attr));
    EXPECT_EQ(ds_get, expected_ds);
  }
}

TEST(DataBagTest, CreateObjectsGet) {
  for (int64_t size : {1, 2, 13, 1079}) {
    auto db = DataBagImpl::CreateEmptyDatabag();

    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_c = DataSliceImpl::CreateEmptyAndUnknownType(size);
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        db->CreateObjectsFromFields(
            {"a", "b", "c"},
            {std::cref(ds_a), std::cref(ds_b), std::cref(ds_c)}));

    AllocationId alloc(ds.values<ObjectId>()[0].value);

    for (auto [attr, expected_ds, dtype] :
         std::vector{std::tuple{"a", ds_a, arolla::GetQType<ObjectId>()},
                     std::tuple{"b", ds_b, arolla::GetQType<ObjectId>()},
                     std::tuple{"c", ds_c, arolla::GetNothingQType()}}) {
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

      EXPECT_EQ(ds_get.size(), size);
      EXPECT_EQ(ds_get.dtype(), dtype);
      EXPECT_EQ(ds_get.allocation_ids(), expected_ds.allocation_ids());
      EXPECT_EQ(ds_get.size(), size);
      EXPECT_THAT(ds_get, IsEquivalentTo(expected_ds));

      EXPECT_THAT(db->GetAttr(ds[0], attr), IsOkAndHolds(ds_get[0]));
    }

    // Assigning an attribute on a missing (nullptr) dense source.
    ASSERT_OK(db->SetAttr(ds, "c", DataSliceImpl::Create(size, DataItem(42))));
    EXPECT_THAT(db->GetAttr(ds[0], "c"), IsOkAndHolds(42));
  }
}

TEST(DataBagTest, CreateObjectsSetObjects) {
  for (int64_t size : {1, 2, 13, 1079}) {
    auto db = DataBagImpl::CreateEmptyDatabag();

    auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
    auto ds_a = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b = DataSliceImpl::AllocateEmptyObjects(size);
    auto ds_b_new = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK_AND_ASSIGN(auto ds,
                         db->CreateObjectsFromFields(
                             {"a", "b"}, {std::cref(ds_a), std::cref(ds_b)}));
    ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

    AllocationId alloc(ds.values<ObjectId>()[0].value);

    {
      DataBagImpl::ConstDenseSourceArray dense_sources;
      DataBagImpl::ConstSparseSourceArray sparse_sources;
      db->GetAttributeDataSources(alloc, "a", dense_sources, sparse_sources);
      EXPECT_EQ(dense_sources.size(), size <= kSmallAllocMaxCapacity ? 0 : 1);
      EXPECT_EQ(sparse_sources.size(), size <= kSmallAllocMaxCapacity ? 1 : 0);
    }

    for (auto [attr, expected_ds, extra_alloc_ids_ds] :
         std::vector{std::tuple{"a", ds_a, ds_empty},
                     std::tuple{"b", ds_b_new, ds_b}}) {
      ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

      EXPECT_EQ(ds_get.size(), size);
      EXPECT_EQ(ds_get.dtype(), arolla::GetQType<ObjectId>());
      {
        AllocationIdSet expected_set;
        expected_set.Insert(extra_alloc_ids_ds.allocation_ids());
        expected_set.Insert(expected_ds.allocation_ids());
        EXPECT_EQ(ds_get.allocation_ids(), expected_set);
      }
      EXPECT_EQ(ds_get.values<ObjectId>().size(), size);
      EXPECT_THAT(ds_get.values<ObjectId>(),
                  ElementsAreArray(expected_ds.values<ObjectId>()));
    }

    auto ds_b_new2 = DataSliceImpl::AllocateEmptyObjects(size);
    ASSERT_OK(db->SetAttr(ds, "b", ds_b_new2));
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get, db->GetAttr(ds, "b"));
    {
      AllocationIdSet expected_set;
      expected_set.Insert(ds_b.allocation_ids());
      expected_set.Insert(ds_b_new.allocation_ids());
      expected_set.Insert(ds_b_new2.allocation_ids());
      EXPECT_EQ(ds_b_get.allocation_ids(), expected_set);
    }
    EXPECT_EQ(ds_b_get.values<ObjectId>().size(), size);
    EXPECT_THAT(ds_b_get.values<ObjectId>(),
                ElementsAreArray(ds_b_new2.values<ObjectId>()));
  }
}

TEST(DataBagTest, CreatePrimitiveSetPrimitive) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
  auto ds_a =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 75));
  auto ds_b =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 57));
  auto ds_b_new =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 37));
  ASSERT_OK_AND_ASSIGN(
      auto ds, db->CreateObjectsFromFields({"a", "b"},
                                           {std::cref(ds_a), std::cref(ds_b)}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

  for (auto [attr, expected_ds] :
       std::vector{std::tuple{"a", ds_a}, std::tuple{"b", ds_b_new}}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

    EXPECT_EQ(ds_get.size(), kSize);
    EXPECT_EQ(ds_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_THAT(ds_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_get.values<int32_t>(),
                ElementsAreArray(expected_ds.values<int32_t>()));
  }

  auto ds_b_new2 =
      DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(kSize, 93));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new2));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_b_get, db->GetAttr(ds, "b"));
  EXPECT_THAT(ds_b_get.allocation_ids(), IsEmpty());
  EXPECT_THAT(ds_b_get.values<int32_t>(),
              ElementsAreArray(std::vector<int32_t>(kSize, 93)));
}

TEST(DataBagTest, TextAttribute) {
  constexpr int64_t kSize = 13;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_empty = DataSliceImpl::AllocateEmptyObjects(0);
  auto ds_a = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "aaa"));
  auto ds_b = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "bbb"));
  auto ds_b_new = DataSliceImpl::Create(
      arolla::CreateConstDenseArray<arolla::Text>(kSize, "ccc"));
  ASSERT_OK_AND_ASSIGN(
      auto ds, db->CreateObjectsFromFields({"a", "b"},
                                           {std::cref(ds_a), std::cref(ds_b)}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_new));

  for (auto [attr, expected_ds] :
       std::vector{std::tuple{"a", ds_a}, std::tuple{"b", ds_b_new}}) {
    ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, attr));

    EXPECT_EQ(ds_get.size(), kSize);
    EXPECT_EQ(ds_get.dtype(), arolla::GetQType<arolla::Text>());
    EXPECT_THAT(ds_get.allocation_ids(), IsEmpty());
    EXPECT_THAT(ds_get.values<arolla::Text>(),
                ElementsAreArray(expected_ds.values<arolla::Text>()));
  }
}

TEST(DataBagTest, EmptySliceGet) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  constexpr int64_t kSize = 4;
  auto obj1 = AllocateSingleObject();
  auto dsb = SliceBuilder(kSize);
  dsb.GetMutableAllocationIds().Insert(AllocationId(obj1));
  auto ds = std::move(dsb).Build();
  EXPECT_TRUE(ds.allocation_ids().empty());
  EXPECT_FALSE(ds.allocation_ids().contains_small_allocation_id());

  ASSERT_OK_AND_ASSIGN(auto result, db->GetAttr(ds, "a"));
  EXPECT_TRUE(result.is_empty_and_unknown());
}

TEST(DataBagTest, EmptySliceSet) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  constexpr int64_t kSize = 4;
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(kSize);

  ASSERT_OK(db->SetAttr(ds, "a", ds));
}

TEST(DataBagTest, SmallBigAllocMixSetGet) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::Create({
      DataItem(AllocateSingleObject()),
      DataItem(Allocate(8).ObjectByOffset(0)),
      DataItem(AllocateSingleObject()),
      DataItem(Allocate(81).ObjectByOffset(17)),
  });

  ASSERT_OK(db->SetAttr(ds, "a", ds));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, "a"));
  EXPECT_THAT(ds_get, IsEquivalentTo(ds));
}

TEST(DataBagTest, SmallBigAllocMixSetGetTooManyAllocations) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  std::vector<DataItem> objs;
  for (int64_t i = 0; i < 1000; ++i) {
    objs.push_back(i % 10 == 0 ? DataItem(AllocateSingleObject())
                               : DataItem(Allocate(1000).ObjectByOffset(i)));
  }
  auto ds = DataSliceImpl::Create(objs);

  ASSERT_OK(db->SetAttr(ds, "a", ds));
  ASSERT_OK_AND_ASSIGN(DataSliceImpl ds_get, db->GetAttr(ds, "a"));
  EXPECT_THAT(ds_get, IsEquivalentTo(ds));

  // Add more objects to check that we work fine when some elements are
  // missing.
  std::shuffle(objs.begin(), objs.end(), absl::BitGen());
  auto expected_objs = objs;
  for (int64_t i = 0; i < 100; ++i) {
    int64_t insert_idx = (i * 157) % objs.size();
    auto obj = i % 10 == 0 ? DataItem(AllocateSingleObject())
                           : DataItem(Allocate(1000).ObjectByOffset(i));
    objs.insert(objs.begin() + insert_idx, obj);
    expected_objs.insert(expected_objs.begin() + insert_idx, DataItem());
  }
  ds = DataSliceImpl::Create(objs);
  ASSERT_OK_AND_ASSIGN(ds_get, db->GetAttr(ds, "a"));
  EXPECT_THAT(ds_get, ElementsAreArray(expected_objs));
}

TEST(DataBagTest, InternalSetUnitAttrAndReturnMissingObjects) {
  constexpr int64_t kSize = 3;
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds_b = DataSliceImpl::AllocateEmptyObjects(kSize);
  auto ds1 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
      {ds_a.values<ObjectId>()[0], ds_b.values<ObjectId>()[1]}));
  ASSERT_OK_AND_ASSIGN(
      auto ds1_result,
      db->InternalSetUnitAttrAndReturnMissingObjects(ds1, "a"));
  EXPECT_THAT(ds1_result.values<ObjectId>(),
              ElementsAreArray(ds1.values<ObjectId>()));

  auto ds_union = DataSliceImpl::CreateObjectsDataSlice(
      arolla::CreateDenseArray<ObjectId>(ConcatObjects(ds_a, ds_b)),
      ConcatAllocations(ds_a, ds_b));
  ASSERT_OK_AND_ASSIGN(
      auto ds_result,
      db->InternalSetUnitAttrAndReturnMissingObjects(ds_union, "a"));
  EXPECT_THAT(ds_result.allocation_ids(),
              ElementsAreArray(ConcatAllocations(ds_a, ds_b)));
  EXPECT_THAT(ds_result.values<ObjectId>(),
              UnorderedElementsAreArray(
                  {ds_a.values<ObjectId>()[1], ds_a.values<ObjectId>()[2],
                   ds_b.values<ObjectId>()[0], ds_b.values<ObjectId>()[2]}));

  ASSERT_OK_AND_ASSIGN(
      auto ds_oth,
      db->InternalSetUnitAttrAndReturnMissingObjects(ds_union, "oth"));
  EXPECT_THAT(ds_oth.allocation_ids(),
              ElementsAreArray(ConcatAllocations(ds_a, ds_b)));
  EXPECT_THAT(ds_oth.allocation_ids(),
              ElementsAreArray(ConcatAllocations(ds_a, ds_b)));
  EXPECT_THAT(ds_oth.values<ObjectId>(),
              ElementsAreArray(ds_union.values<ObjectId>()));
}

TEST(DataBagTest, InternalSetUnitAttrAndReturnMissingObjectsSparseToDense) {
  for (int64_t kSize : {2, 1000}) {
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto ds = DataSliceImpl::AllocateEmptyObjects(kSize);
    for (int64_t sz : {kSize / 100 + 1, kSize}) {
      auto objs_bldr = arolla::DenseArrayBuilder<ObjectId>(sz);
      objs_bldr.Set(0, AllocateSingleObject());
      auto objs = DataSliceImpl::Create(std::move(objs_bldr).Build());
      ASSERT_OK_AND_ASSIGN(
          auto _, db->InternalSetUnitAttrAndReturnMissingObjects(objs, "a"));
    }
  }
}

TEST(DataBagTest,
     InternalSetUnitAttrAndReturnMissingObjectsManyBigAllocations) {
  constexpr int64_t kSize = 50239;
  auto db = DataBagImpl::CreateEmptyDatabag();
  std::vector<DataItem> objs1;
  std::vector<DataItem> objs2;
  std::vector<DataItem> expected_objs2_missing;
  for (int64_t i = 0; i < kSize; ++i) {
    auto obj = DataItem(Allocate(57).ObjectByOffset(i % 57));
    if (i % 2 == 0) {
      objs1.push_back(obj);
    } else {
      objs2.push_back(obj);
      if (i % 3 == 0) {
        objs1.push_back(obj);
      } else {
        expected_objs2_missing.push_back(obj);
      }
    }
  }
  // We do not use UnorderedElementsAreArray, because test is too slow with it.
  {
    auto ds = DataSliceImpl::Create(objs1);
    ASSERT_OK_AND_ASSIGN(
        auto ds_result,
        db->InternalSetUnitAttrAndReturnMissingObjects(ds, "a"));
    std::vector<DataItem> result_objs(ds_result.begin(), ds_result.end());
    std::sort(result_objs.begin(), result_objs.end(), DataItem::Less());
    std::sort(objs1.begin(), objs1.end(), DataItem::Less());
    EXPECT_THAT(result_objs, ElementsAreArray(objs1));
  }
  {
    auto ds = DataSliceImpl::Create(objs2);
    ASSERT_OK_AND_ASSIGN(
        auto ds_result,
        db->InternalSetUnitAttrAndReturnMissingObjects(ds, "a"));
    std::vector<DataItem> result_objs(ds_result.begin(), ds_result.end());
    std::sort(result_objs.begin(), result_objs.end(), DataItem::Less());
    std::sort(expected_objs2_missing.begin(), expected_objs2_missing.end(),
              DataItem::Less());
    EXPECT_THAT(result_objs,
                ElementsAreArray(expected_objs2_missing));
  }
}

TEST(DataBagTest,
     InternalSetUnitAttrAndReturnMissingObjectsMixSmallAndBigAllocations) {
  constexpr int64_t kSize = 111;
  auto db = DataBagImpl::CreateEmptyDatabag();
  std::vector<DataItem> objs1;
  std::vector<DataItem> objs2;
  std::vector<DataItem> expected_objs2_missing;
  for (int64_t i = 0; i < kSize; ++i) {
    auto obj = i % 5 == 0 ? DataItem(Allocate(57).ObjectByOffset(i % 57))
                          : DataItem(AllocateSingleObject());
    if (i % 2 == 0) {
      objs1.push_back(obj);
    } else {
      objs2.push_back(obj);
      if (i % 3 == 0) {
        objs1.push_back(obj);
      } else {
        expected_objs2_missing.push_back(obj);
      }
    }
  }
  {
    auto ds = DataSliceImpl::Create(objs1);
    EXPECT_THAT(db->InternalSetUnitAttrAndReturnMissingObjects(ds, "a"),
                IsOkAndHolds(UnorderedElementsAreArray(objs1)));
  }
  {
    auto ds = DataSliceImpl::Create(objs2);
    EXPECT_THAT(
        db->InternalSetUnitAttrAndReturnMissingObjects(ds, "a"),
        IsOkAndHolds(UnorderedElementsAreArray(expected_objs2_missing)));
  }
}

TEST(DataBagTest, SetGetDataItem) {
  auto ds_a = DataItem(57.0f);
  for (DataItem ds : {
           CreateUuidFromFields("", {"a"}, {std::cref(ds_a)}),
           DataItem(AllocateSingleObject()),
           DataItem(Allocate(1).ObjectByOffset(0)),
           DataItem(Allocate(2).ObjectByOffset(1)),
           DataItem(Allocate(7777).ObjectByOffset(5555)),
       }) {
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto ds_b = DataItem(75);
    ASSERT_OK(db->SetAttr(ds, "b", ds_b));

    ASSERT_OK_AND_ASSIGN(DataItem ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_EQ(ds_b_get.value<int>(), 75);

    db = db->PartiallyPersistentFork();
    auto ds_c = DataItem(91.0);
    ASSERT_OK(db->SetAttr(ds, "c", ds_c));

    ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
    EXPECT_EQ(ds_b_get.value<int32_t>(), 75);

    ASSERT_OK_AND_ASSIGN(auto ds_c_get, db->GetAttr(ds, "c"));
    ASSERT_EQ(ds_c_get.dtype(), arolla::GetQType<double>());
    EXPECT_EQ(ds_c_get.value<double>(), 91.0);

    ASSERT_OK(db->SetAttr(ds, "b", DataItem(arolla::Text("B"))));
    ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
    ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<arolla::Text>());
    EXPECT_EQ(ds_b_get.value<arolla::Text>(), arolla::Text("B"));
  }
}

TEST(DataBagTest, SetGetDataItemWithFallback) {
  auto ds_a = DataItem(57.0f);
  for (DataItem ds : {
           CreateUuidFromFields("", {"a"}, {std::cref(ds_a)}),
           DataItem(AllocateSingleObject()),
           DataItem(Allocate(1).ObjectByOffset(0)),
           DataItem(Allocate(2).ObjectByOffset(1)),
           DataItem(Allocate(7777).ObjectByOffset(5555)),
       }) {
    auto db_fb = DataBagImpl::CreateEmptyDatabag();
    auto ds_b = DataItem(75);
    ASSERT_OK(db_fb->SetAttr(ds, "b", ds_b));

    auto db = DataBagImpl::CreateEmptyDatabag();

    EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}),
                IsOkAndHolds(DataItem(75)));
    EXPECT_THAT(db->GetAttr(ds, "b"), IsOkAndHolds(DataItem()));

    if (auto obj = ds.value<ObjectId>(); AllocationId(obj).Capacity() > 1) {
      // Check that setting another value in the same allocation doesn't
      // change the result.
      AllocationId alloc(obj);
      DataItem ds_other(
          alloc.ObjectByOffset((obj.Offset() + 1) % alloc.Capacity()));
      ASSERT_OK(db->SetAttr(ds_other, "b", DataItem(arolla::Text("X"))));

      EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}),
                  IsOkAndHolds(DataItem(75)));
      EXPECT_THAT(db->GetAttr(ds, "b"), IsOkAndHolds(DataItem()));
    }

    ASSERT_OK(db->SetAttr(ds, "b", DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b"),
                IsOkAndHolds(DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b", {}),
                IsOkAndHolds(DataItem(arolla::Text("B"))));
    EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}),
                IsOkAndHolds(DataItem(arolla::Text("B"))));

    // Check that if value is REMOVED we don't search it in fallback.
    ASSERT_OK(db->SetAttr(ds, "b", DataItem()));
    EXPECT_THAT(db->GetAttr(ds, "b", {db_fb.get()}), IsOkAndHolds(DataItem()));
    EXPECT_THAT(db->GetAttr(ds, "b"), IsOkAndHolds(DataItem()));
  }
}

TEST(DataBagTest, SetGetDataSliceUUid) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto ds_a =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<int>({13, 19, 57}));
  ASSERT_OK_AND_ASSIGN(auto ds, CreateUuidFromFields(
      "", {"a"}, {std::cref(ds_a)}));

  auto ds_b =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<int>({23, 29, 75}));

  ASSERT_OK(db->SetAttr(ds, "b", ds_b));

  ASSERT_OK_AND_ASSIGN(auto ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(23, 29, 75));

  db = db->PartiallyPersistentFork();
  auto ds_c =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<double>({91, 39, 43}));
  ASSERT_OK(db->SetAttr(ds, "c", ds_c));

  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(23, 29, 75));

  ASSERT_OK_AND_ASSIGN(auto ds_c_get, db->GetAttr(ds, "c"));
  ASSERT_EQ(ds_c_get.dtype(), arolla::GetQType<double>());
  EXPECT_THAT(ds_c_get.values<double>(), ElementsAre(91, 39, 43));

  auto text_array =
      std::vector{arolla::Text("Q"), arolla::Text("B"), arolla::Text("W")};
  auto ds_b_text =
      DataSliceImpl::Create(arolla::CreateFullDenseArray<arolla::Text>(
          text_array.begin(), text_array.end()));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b_text));

  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<arolla::Text>());
  EXPECT_THAT(ds_b_get.values<arolla::Text>(), ElementsAreArray(text_array));

  // remove
  ds_b = DataSliceImpl::Create(arolla::CreateDenseArray<int>(
      std::vector<OptionalValue<int>>{5, std::nullopt, 7}));
  ASSERT_OK(db->SetAttr(ds, "b", ds_b));
  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<int32_t>());
  EXPECT_THAT(ds_b_get.values<int>(), ElementsAre(5, std::nullopt, 7));

  // ObjectId
  AllocationId alloc1 = Allocate(1000);
  AllocationId alloc2 = Allocate(2000);
  AllocationId alloc3 = Allocate(1);
  auto object_array = arolla::CreateFullDenseArray<ObjectId>(
      {alloc1.ObjectByOffset(5), alloc3.ObjectByOffset(0),
       alloc2.ObjectByOffset(9)});
  ds_b = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({alloc1, alloc2, alloc3}), object_array);
  ASSERT_OK(db->SetAttr(ds, "b", ds_b));
  ASSERT_OK_AND_ASSIGN(ds_b_get, db->GetAttr(ds, "b"));
  ASSERT_EQ(ds_b_get.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_THAT(ds_b_get.values<ObjectId>(), ElementsAreArray(object_array));
  EXPECT_THAT(ds_b_get.allocation_ids(), UnorderedElementsAre(alloc1, alloc2));
  EXPECT_TRUE(ds_b_get.allocation_ids().contains_small_allocation_id());
}

// There are more tests for DataBagIndex/DataBagContent in triples_test.cc
TEST(DataBagTest, DataBagIndex) {
  auto ds1 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds2 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds3 = DataSliceImpl::AllocateEmptyObjects(15);

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(ds1, "a", ds2));
  ASSERT_OK(db->SetAttr(ds1, "b", ds2));
  // Add small allocation to the attribute "b".
  ASSERT_OK(db->SetAttr(DataItem(AllocateSingleObject()), "b", DataItem(57)));
  ASSERT_OK(db->SetAttr(ds2, "a", ds3));

  DataBagIndex index = db->CreateIndex();
  EXPECT_TRUE(index.dicts.empty());
  EXPECT_TRUE(index.lists.empty());
  EXPECT_EQ(index.attrs.size(), 2);
  EXPECT_FALSE(index.attrs["a"].with_small_allocs);
  EXPECT_TRUE(index.attrs["b"].with_small_allocs);
  EXPECT_THAT(index.attrs["b"].allocations,
              ElementsAreArray(ds1.allocation_ids().ids()));

  // db.CreateIndex() should return allocation ids in sorted order.
  AllocationId ds1_alloc = ds1.allocation_ids().ids().front();
  AllocationId ds2_alloc = ds2.allocation_ids().ids().front();
  if (ds1_alloc < ds2_alloc) {
    EXPECT_THAT(index.attrs["a"].allocations,
                ElementsAre(ds1_alloc, ds2_alloc));
  } else {
    EXPECT_THAT(index.attrs["a"].allocations,
                ElementsAre(ds1_alloc, ds2_alloc));
  }
}

TEST(DataBagTest, MergeInplace) {
  auto none_databag = DataBagImpl::CreateEmptyDatabag();
  auto big_alloc_databag = DataBagImpl::CreateEmptyDatabag();

  // Set attr values.
  auto big_alloc_ids = DataSliceImpl::AllocateEmptyObjects(3);
  ASSERT_OK(none_databag->SetAttr(big_alloc_ids[2], "x", DataItem()));
  ASSERT_OK(big_alloc_databag->SetAttr(big_alloc_ids[1], "x", DataItem(2)));

  auto res_db = DataBagImpl::CreateEmptyDatabag();
  MergeOptions merge_options;
  ASSERT_OK(res_db->MergeInplace(*none_databag, merge_options));
  ASSERT_OK(res_db->MergeInplace(*big_alloc_databag, merge_options));
}

TEST(DataBagTest, StatisticsAdds) {
  auto stats = DataBagStatistics{
      .attr_values_sizes = {{"a", 1}, {"b", 2}},
      .total_non_empty_lists = 5,
      .total_items_in_lists = 6,
      .total_non_empty_dicts = 7,
      .total_items_in_dicts = 8,
      .total_explicit_schemas = 9,
      .total_explicit_schema_attrs = 10,
      .entity_and_object_count = 11,
  };
  auto other = DataBagStatistics{
      .attr_values_sizes = {{"a", 3}, {"c", 5}},
      .total_non_empty_lists = 11,
      .total_items_in_lists = 10,
      .total_non_empty_dicts = 9,
      .total_items_in_dicts = 8,
      .total_explicit_schemas = 7,
      .total_explicit_schema_attrs = 6,
      .entity_and_object_count = 5,
  };
  stats.Add(other);
  EXPECT_THAT(stats.attr_values_sizes,
              UnorderedElementsAre(Pair("a", 4), Pair("b", 2), Pair("c", 5)));
  EXPECT_EQ(stats.total_non_empty_lists, 16);
  EXPECT_EQ(stats.total_items_in_lists, 16);
  EXPECT_EQ(stats.total_non_empty_dicts, 16);
  EXPECT_EQ(stats.total_items_in_dicts, 16);
  EXPECT_EQ(stats.total_explicit_schemas, 16);
  EXPECT_EQ(stats.total_explicit_schema_attrs, 16);
  EXPECT_EQ(stats.entity_and_object_count, 16);
}

TEST(DataBagTest, GetStatistics_Lists) {
  {
    DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();
    AllocationId alloc_id = AllocateLists(5);
    DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 5);
    // Empty list is not stored in databag.
    ASSERT_OK(db->AppendToList(
        lists, DataSliceImpl::Create(arolla::CreateDenseArray<float>(
                   {std::nullopt, 11.0f, std::nullopt, 2.0f, 3.0f}))));

    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_lists, 5);
    EXPECT_EQ(stats.total_items_in_lists, 5);
  }
  {
    // Small allocs.
    DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();
    for (int i = 0; i < 3; ++i) {
      ObjectId obj_id = AllocateSingleList();
      ASSERT_OK(db->AppendToList(DataItem(obj_id), DataItem(i)));
    }
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_lists, 3);
    EXPECT_EQ(stats.total_items_in_lists, 3);
  }
}

TEST(DataBagTest, GetStatistics_Dicts) {
  {
    DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();
    AllocationId dict_alloc_id = AllocateDicts(5);
    DataSliceImpl dicts =
        DataSliceImpl::ObjectsFromAllocation(dict_alloc_id, 5);
    ASSERT_OK(db->SetInDict(dicts,
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {1, std::nullopt, 3, 4, 5})),
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {5, 6, 7, 8, std::nullopt}))));

    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_dicts, 4);
    EXPECT_EQ(stats.total_items_in_dicts, 4);
  }
  {
    DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();
    AllocationId dict_alloc_id = AllocateDicts(5);
    DataSliceImpl dicts =
        DataSliceImpl::ObjectsFromAllocation(dict_alloc_id, 5);
    ASSERT_OK(db->SetInDict(dicts,
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {1, std::nullopt, 3, 4, 5})),
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {5, 6, 7, 8, std::nullopt}))));
    DataSliceImpl sub_dicts =
        DataSliceImpl::ObjectsFromAllocation(dict_alloc_id, 3);
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_dicts, 4);
    EXPECT_EQ(stats.total_items_in_dicts, 4);

    ASSERT_OK(db->SetInDict(sub_dicts,
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {7, std::nullopt, 8})),
                            DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                                {7, 8, std::nullopt}))));
    ASSERT_OK_AND_ASSIGN(stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_dicts, 4);
    EXPECT_EQ(stats.total_items_in_dicts, 6);
  }
  {
    DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();
    for (int i = 0; i < 3; ++i) {
      ObjectId obj_id = AllocateSingleDict();
      ASSERT_OK(db->SetInDict(DataItem(obj_id), DataItem(i), DataItem(i)));
    }
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_non_empty_dicts, 3);
    EXPECT_EQ(stats.total_items_in_dicts, 3);
  }
}

TEST(DataBagTest, GetStatistics_Schemas) {
  {
    auto db = DataBagImpl::CreateEmptyDatabag();
    DataItem int_s = DataItem(schema::kInt64);
    DataItem float_s = DataItem(schema::kFloat32);
    DataItem int_s2 = DataItem(schema::kInt64);
    std::vector<std::reference_wrapper<const DataItem>> items1{
        std::cref(int_s), std::cref(float_s)};
    ASSERT_OK_AND_ASSIGN(auto schema_item, db->CreateExplicitSchemaFromFields(
                                               {"a", "b"}, items1));
    std::vector<std::reference_wrapper<const DataItem>> items2{
        std::cref(int_s2), schema_item};
    ASSERT_OK_AND_ASSIGN(
        schema_item, db->CreateExplicitSchemaFromFields({"a", "b"}, items2));
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_explicit_schema_attrs, 4);
    EXPECT_EQ(stats.total_explicit_schemas, 2);
  }
  {
    // UU schema
    auto db = DataBagImpl::CreateEmptyDatabag();
    DataItem int_s = DataItem(schema::kInt64);
    DataItem float_s = DataItem(schema::kFloat32);
    ASSERT_OK_AND_ASSIGN(
        auto schema_item,
        db->CreateUuSchemaFromFields("", {"a", "b"},
                                     {std::cref(int_s), std::cref(float_s)}));
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_EQ(stats.total_explicit_schema_attrs, 2);
    EXPECT_EQ(stats.total_explicit_schemas, 1);
  }
}

TEST(DataBagTest, GetStatistics_Attrs) {
  {
    // Small allocs.
    auto db = DataBagImpl::CreateEmptyDatabag();
    DataSliceImpl ds_a =
        DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, std::nullopt}));
    DataSliceImpl ds_b =
        DataSliceImpl::Create(arolla::CreateDenseArray<int>({std::nullopt, 2}));
    ASSERT_OK(db->CreateObjectsFromFields({"a", "b"}, {ds_a, ds_b}));
    DataSliceImpl ds_c =
        DataSliceImpl::Create(arolla::CreateDenseArray<int>({3, 2}));
    ASSERT_OK(db->CreateObjectsFromFields({"a", "c"}, {ds_c, ds_c}));
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_THAT(stats.attr_values_sizes,
                UnorderedElementsAre(Pair("a", 3), Pair("b", 1), Pair("c", 2)));
    EXPECT_EQ(stats.entity_and_object_count, 4);
  }
  {
    // Big allocs.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto values_a =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int32_t>(100, 57));
    auto ds_a = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({std::nullopt, 1, 2, 3, 4}));
    auto ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({2, 3, 4, 5, std::nullopt}));
    ASSERT_OK(db->CreateObjectsFromFields({"a", "b"}, {ds_a, ds_b}));
    ASSERT_OK(db->CreateObjectsFromFields({"a"}, {values_a}));

    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_THAT(stats.attr_values_sizes,
                UnorderedElementsAre(Pair("a", 104), Pair("b", 4)));
    EXPECT_EQ(stats.entity_and_object_count, 105);
  }
  {
    // Combination
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto ds_a = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({std::nullopt, 1, 2, 3, 4}));
    auto ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({2, 3, 4, 5, std::nullopt}));
    ASSERT_OK(db->CreateObjectsFromFields({"a", "b"}, {ds_a, ds_b}));
    ObjectId obj_id = AllocateSingleObject();
    ASSERT_OK(db->SetAttr(DataItem(obj_id), "a", DataItem(57)));
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_THAT(stats.attr_values_sizes,
                UnorderedElementsAre(Pair("a", 5), Pair("b", 4)));
    EXPECT_EQ(stats.entity_and_object_count, 6);
  }
  {
    // Ignore internal attributes.
    auto db = DataBagImpl::CreateEmptyDatabag();
    auto ds_a = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({std::nullopt, 1, 2, 3, 4}));
    auto ds_b = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({2, 3, 4, 5, std::nullopt}));
    ASSERT_OK(db->CreateObjectsFromFields({"__a__", "__b__"}, {ds_a, ds_b}));
    ASSERT_OK_AND_ASSIGN(auto stats, db->GetStatistics());
    EXPECT_THAT(stats.attr_values_sizes, IsEmpty());
  }
}

// Note that this test is testing the implementation of GetApproxTotalSize that
// can be changed. It is fine to change numbers here.
TEST(DataBagTest, GetApproxTotalSize) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_EQ(db->GetApproxTotalSize(), 0);
  db = db->PartiallyPersistentFork();
  EXPECT_EQ(db->GetApproxTotalSize(), 0);

  {
    SCOPED_TRACE("big allocs");
    static constexpr int64_t kBigSize = 37;
    auto db_cur = db->PartiallyPersistentFork();
    auto ds1 = DataSliceImpl::AllocateEmptyObjects(kBigSize);
    auto ds2 = DataSliceImpl::AllocateEmptyObjects(kBigSize);
    ASSERT_OK(db_cur->SetAttr(ds1, "a", ds2));
    int64_t approx_size = db_cur->GetApproxTotalSize();
    // Big allocs are counted not precisely.
    EXPECT_LE(approx_size, kBigSize * 2);
    EXPECT_GE(approx_size, kBigSize);
  }
  {
    SCOPED_TRACE("small allocs");
    auto db_cur = db->PartiallyPersistentFork();
    auto ds1 = DataSliceImpl::AllocateEmptyObjects(1);
    auto ds2 = DataSliceImpl::AllocateEmptyObjects(1);
    for (int i = 0; i < 1000; ++i) {
      ASSERT_OK(db_cur->SetAttr(ds1, absl::StrCat("a", i), ds2));
    }
    // Small allocs are counted more precisely.
    int64_t approx_size = db_cur->GetApproxTotalSize();
    EXPECT_EQ(approx_size, 1000);
  }

  {
    SCOPED_TRACE("lists");
    auto db_cur = db->PartiallyPersistentFork();
    DataSliceImpl lists =
        DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);

    ASSERT_OK(
        db_cur->ExtendList(lists[0], DataSliceImpl::Create({DataItem(1)})));
    ASSERT_OK(db_cur->ExtendList(
        lists[1],
        DataSliceImpl::Create({DataItem(4), DataItem(), DataItem(6)})));
    ASSERT_OK(db_cur->ExtendList(
        lists[2], DataSliceImpl::Create({DataItem(7), DataItem(8)})));
    // List sizes are estimated as length.
    EXPECT_EQ(db_cur->GetApproxTotalSize(), 6);
  }

  {
    SCOPED_TRACE("dicts");
    auto db_cur = db->PartiallyPersistentFork();
    DataSliceImpl dicts =
        DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);

    ASSERT_OK(db_cur->SetInDict(dicts[0], DataItem(1), DataItem(2)));
    ASSERT_OK(db_cur->SetInDict(dicts[1], DataItem(3), DataItem(4)));
    ASSERT_OK(db_cur->SetInDict(dicts[1], DataItem(-3), DataItem(-4)));
    ASSERT_OK(db_cur->SetInDict(dicts[2], DataItem(5), DataItem(6)));
    ASSERT_OK(db_cur->SetInDict(dicts[2], DataItem(7), DataItem(8)));
    ASSERT_OK(db_cur->SetInDict(dicts[2], DataItem(9), DataItem(0)));

    // Dict sizes are estimated as length.
    EXPECT_EQ(db_cur->GetApproxTotalSize(), 6);
  }

  {
    SCOPED_TRACE("forks");
    auto db_parent = db->PartiallyPersistentFork();
    DataSliceImpl dicts =
        DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
    DataSliceImpl lists =
        DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);

    ASSERT_OK(db_parent->SetInDict(dicts[0], DataItem(1), DataItem(2)));
    ASSERT_OK(db_parent->SetInDict(dicts[1], DataItem(3), DataItem(4)));
    auto db_cur = db_parent->PartiallyPersistentFork();
    EXPECT_EQ(db_cur->GetApproxTotalSize(), 2);
    ASSERT_OK(db_cur->AppendToList(lists[0], DataItem(1)));

    // Dict sizes are estimated as length.
    EXPECT_EQ(db_cur->GetApproxTotalSize(), 3);
  }
}

// NOTE(b/343432263): msan regression test to ensure that the DataBagImpl
// destructor does not cause use-of-uninitialized-value issues.
using DataBagMsanTest = ::testing::TestWithParam<DataBagImplPtr>;

TEST_P(DataBagMsanTest, Msan) {
  const auto& db = GetParam();
  ASSERT_NE(db, nullptr);
}

INSTANTIATE_TEST_SUITE_P(
    DataBagMsanTestSuite, DataBagMsanTest,
    ::testing::ValuesIn([]() -> std::vector<DataBagImplPtr> {
      auto db_no_parent = DataBagImpl::CreateEmptyDatabag();

      auto db = DataBagImpl::CreateEmptyDatabag();
      auto ds = DataSliceImpl::AllocateEmptyObjects(3);
      auto ds_a = DataSliceImpl::AllocateEmptyObjects(3);
      EXPECT_OK(db->SetAttr(ds, "a", ds_a));
      auto db_with_parent = db->PartiallyPersistentFork();

      return {{db_no_parent}, {db_with_parent}};
    }()));

}  // namespace
}  // namespace koladata::internal
