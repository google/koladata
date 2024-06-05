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
#include "koladata/internal/uuid_object.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::koladata::internal::testing::IsEquivalentTo;
using ::testing::Not;
using ::arolla::OptionalValue;
using ::arolla::Text;

TEST(UuidTest, CreateUuidFromFields) {
  DataItem x(5);
  DataItem y(7.0f);
  DataItem q = CreateUuidFromFields("", {{"x", x}, {"y", y}});
  ASSERT_EQ(q.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_TRUE(q.value<ObjectId>().IsUuid());
}

TEST(UuidTest, CreateUuidFromFields_Seed) {
  DataItem x(5);
  DataItem y(7.0f);
  DataItem seeded_uuid_1 = CreateUuidFromFields("seed_1", {{"x", x}, {"y", y}});
  DataItem seeded_uuid_2 = CreateUuidFromFields("seed_2",
                                                {{"x", x}, {"y", y}});
  DataItem seeded_uuid_2_again = CreateUuidFromFields(
      "seed_2", {{"x", x}, {"y", y}});

  ASSERT_EQ(seeded_uuid_1.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_TRUE(seeded_uuid_1.value<ObjectId>().IsUuid());
  EXPECT_NE(seeded_uuid_1.value<ObjectId>(), seeded_uuid_2.value<ObjectId>());
  EXPECT_EQ(seeded_uuid_2.value<ObjectId>(),
            seeded_uuid_2_again.value<ObjectId>());
}


TEST(UuidTest, CreateUuidFromFields_MixedTypes) {
  DataItem x(5);
  DataItem y(7.0f);
  DataItem q = CreateUuidFromFields("", {{"x", x}, {"y", y}});

  DataItem a(Text("5"));
  DataItem b(7.0);
  DataItem w = CreateUuidFromFields("", {{"x", a}, {"y", b}});

  auto x_slice = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>(
          std::vector<OptionalValue<int>>{5, std::nullopt}),
      arolla::CreateDenseArray<Text>(
          std::vector<OptionalValue<Text>>{std::nullopt, Text("5")}));
  auto y_slice = DataSliceImpl::Create(
      arolla::CreateDenseArray<float>(
          std::vector<OptionalValue<float>>{7.0f, std::nullopt}),
      arolla::CreateDenseArray<double>(
          std::vector<OptionalValue<double>>{std::nullopt, 7.0}));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl uuid_slice,
                       CreateUuidFromFields("",
                                            {{"x", x_slice}, {"y", y_slice}}));

  EXPECT_EQ(uuid_slice[0], q);
  EXPECT_EQ(uuid_slice[1], w);

  EXPECT_TRUE(uuid_slice.allocation_ids().contains_small_allocation_id());
  EXPECT_TRUE(uuid_slice.allocation_ids().empty());
}

TEST(UuidTest, CreateUuidFromFields_MixedTypes_Seed) {
  DataItem x(5);
  DataItem y(7.0f);
  DataItem q = CreateUuidFromFields("", {{"x", x}, {"y", y}});

  DataItem a(Text("5"));
  DataItem b(7.0);
  DataItem w = CreateUuidFromFields("seed", {{"x", a}, {"y", b}});

  auto x_slice = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>(
          std::vector<OptionalValue<int>>{5, std::nullopt}),
      arolla::CreateDenseArray<Text>(
          std::vector<OptionalValue<Text>>{std::nullopt, Text("5")}));
  auto y_slice = DataSliceImpl::Create(
      arolla::CreateDenseArray<float>(
          std::vector<OptionalValue<float>>{7.0f, std::nullopt}),
      arolla::CreateDenseArray<double>(
          std::vector<OptionalValue<double>>{std::nullopt, 7.0}));

  ASSERT_OK_AND_ASSIGN(DataSliceImpl uuid_slice,
                       CreateUuidFromFields("seed",
                                            {{"x", x_slice}, {"y", y_slice}}));

  EXPECT_NE(uuid_slice[0], q);
  EXPECT_EQ(uuid_slice[1], w);
}

TEST(UuidTest, CreateUuidFromFieldsTypesUsed) {
  {
    DataItem x(5);
    DataItem y(int64_t{5});
    EXPECT_NE(CreateUuidFromFields("", {{"x", x}, {"y", y}}),
              CreateUuidFromFields("", {{"y", x}, {"x", y}}));
  }
  {
    DataItem x(float{0});
    DataItem y(int32_t{0});
    EXPECT_NE(CreateUuidFromFields("", {{"x", x}, {"y", y}}),
              CreateUuidFromFields("", {{"y", x}, {"x", y}}));
  }
  {
    DataItem x(Text("5"));
    DataItem y(arolla::Bytes("5"));
    EXPECT_NE(CreateUuidFromFields("", {{"x", x}, {"y", y}}),
              CreateUuidFromFields("", {{"y", x}, {"x", y}}));
  }
}

TEST(UuidTest, CreateUuidFromFieldsDataItemAllTypesOrderIndependent) {
  DataItem x(1.5f);
  DataItem y(1.5);
  DataItem z(57);
  DataItem q(int64_t{75});
  DataItem w(Text("57"));
  DataItem e(arolla::Bytes("75"));
  DataItem r(AllocateSingleObject());
  DataItem t;
  auto kwargs = absl::flat_hash_map<absl::string_view,
                                    std::reference_wrapper<const DataItem>>{
      {"x", x}, {"y", y}, {"z", z}, {"q", q},
      {"w", w}, {"e", e}, {"r", r}, {"t", t}};
  DataItem item = CreateUuidFromFields("", kwargs);
  ASSERT_EQ(item.dtype(), arolla::GetQType<ObjectId>());
  auto id = item.value<ObjectId>();
  EXPECT_TRUE(id.IsUuid());

  auto kwargs_copies = std::vector{kwargs};

  for (int64_t i = 0; i != 11; ++i) {
    // copy of hash table changes the order
    kwargs_copies.push_back(kwargs_copies.back());
    DataItem item2 = CreateUuidFromFields("", kwargs_copies.back());
    ASSERT_EQ(item2.dtype(), arolla::GetQType<ObjectId>());
    auto id2 = item2.value<ObjectId>();
    EXPECT_TRUE(id2.IsUuid());
    ASSERT_EQ(id2, id);
  }
}

TEST(UuidTest, CreateSchemaUuidFromFields) {
  DataItem x(schema::kInt32);
  DataItem y(schema::kFloat32);
  DataItem z(AllocateExplicitSchema());
  DataItem uuid_1 =
      CreateSchemaUuidFromFields("", {"x", "y", "z"}, {x, y, z});
  DataItem uuid_2 =
      CreateSchemaUuidFromFields("seed", {"x", "y", "z"}, {x, y, z});
  DataItem uuid_3 =
      CreateSchemaUuidFromFields("", {"y", "x", "z"}, {y, x, z});
  DataItem uuid_4 =
      CreateSchemaUuidFromFields("seed", {"y", "x", "z"}, {y, x, z});
  EXPECT_THAT(uuid_1, IsEquivalentTo(uuid_3));
  EXPECT_THAT(uuid_2, IsEquivalentTo(uuid_4));
  EXPECT_THAT(uuid_1, Not(IsEquivalentTo(uuid_2)));
}

TEST(UuidTest, CreateUuidWithMainObject) {
  ASSERT_OK_AND_ASSIGN(DataItem empty,
                       CreateUuidWithMainObject(DataItem(), "x"));
  EXPECT_FALSE(empty.has_value());

  for (ObjectId main_id :
       {AllocateSingleObject(), Allocate(1024).ObjectByOffset(0),
        Allocate(1024).ObjectByOffset(345)}) {
    DataItem main_object(main_id);
    ASSERT_OK_AND_ASSIGN(DataItem x,
                         CreateUuidWithMainObject(main_object, "x"));
    ASSERT_OK_AND_ASSIGN(DataItem y,
                         CreateUuidWithMainObject(main_object, "y"));
    ASSERT_EQ(x.dtype(), arolla::GetQType<ObjectId>());
    auto x_id = x.value<ObjectId>();
    EXPECT_TRUE(x_id.IsUuid());
    EXPECT_EQ(x_id.Offset(), main_id.Offset());
    ASSERT_EQ(y.dtype(), arolla::GetQType<ObjectId>());
    auto y_id = y.value<ObjectId>();
    EXPECT_TRUE(y_id.IsUuid());
    EXPECT_EQ(y_id.Offset(), main_id.Offset());
    EXPECT_NE(x_id, y_id);

    EXPECT_NE(AllocationId(x_id), AllocationId(y_id));
    EXPECT_EQ(AllocationId(x_id).Capacity(), AllocationId(main_id).Capacity());
  }
  for (AllocationId main_alloc_id : {Allocate(1024), Allocate(9753)}) {
    auto main_id1 = main_alloc_id.ObjectByOffset(333);
    DataItem main_object1(main_id1);
    auto main_id2 = main_alloc_id.ObjectByOffset(57);
    DataItem main_object2(main_id2);
    ASSERT_OK_AND_ASSIGN(DataItem x,
                         CreateUuidWithMainObject(main_object1, "x"));
    ASSERT_OK_AND_ASSIGN(DataItem y,
                         CreateUuidWithMainObject(main_object2, "x"));
    ASSERT_EQ(x.dtype(), arolla::GetQType<ObjectId>());
    auto x_id = x.value<ObjectId>();
    EXPECT_TRUE(x_id.IsUuid());
    EXPECT_EQ(x_id.Offset(), main_id1.Offset());
    ASSERT_EQ(y.dtype(), arolla::GetQType<ObjectId>());
    auto y_id = y.value<ObjectId>();
    EXPECT_TRUE(y_id.IsUuid());
    EXPECT_EQ(y_id.Offset(), main_id2.Offset());
    EXPECT_NE(x_id, y_id);

    EXPECT_EQ(AllocationId(x_id), AllocationId(y_id));
    EXPECT_EQ(AllocationId(x_id).Capacity(), AllocationId(main_id1).Capacity());
  }
}

TEST(UuidTest, CreateUuidWithMainObject_ImplicitSchema) {
  ASSERT_OK_AND_ASSIGN(DataItem empty,
                       CreateUuidWithMainObject(DataItem(), "x"));
  EXPECT_FALSE(empty.has_value());

  auto item = DataItem(Allocate(1000).ObjectByOffset(100));
  ASSERT_OK_AND_ASSIGN(
      auto implicit_schema_item,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          item, "implicit_schema"));

  auto implicit_schema = implicit_schema_item.value<ObjectId>();
  EXPECT_TRUE(implicit_schema.IsUuid());
  EXPECT_TRUE(implicit_schema.IsSchema());
  EXPECT_TRUE(implicit_schema.IsImplicitSchema());
  EXPECT_EQ(implicit_schema.Offset(), item.value<ObjectId>().Offset());
  EXPECT_EQ(AllocationId(implicit_schema).Capacity(),
            AllocationId(item.value<ObjectId>()).Capacity());
  EXPECT_TRUE(AllocationId(implicit_schema).IsSchemasAlloc());
}

TEST(UuidTest, CreateUuidWithMainObjectDataSlice) {
  {
    auto alloc1 = Allocate(1043);
    auto alloc2 = Allocate(9043);
    auto main_obj = DataSliceImpl::CreateWithAllocIds(
        AllocationIdSet({alloc1, alloc2}),
        arolla::CreateDenseArray<ObjectId>(std::vector<OptionalValue<ObjectId>>{
            std::nullopt, alloc1.ObjectByOffset(5), std::nullopt,
            alloc1.ObjectByOffset(7), alloc2.ObjectByOffset(13),
            std::nullopt}));
    ASSERT_OK_AND_ASSIGN(auto x, CreateUuidWithMainObject(main_obj, "x"));
    ASSERT_OK_AND_ASSIGN(auto y, CreateUuidWithMainObject(main_obj, "y"));
    for (const auto& [uuid, salt] :
         std::vector{std::pair{&x, "x"}, std::pair{&y, "y"}}) {
      EXPECT_EQ(uuid->dtype(), arolla::GetQType<ObjectId>());
      EXPECT_EQ(uuid->size(), main_obj.size());
      std::vector<AllocationId> expected_allocs;
      for (int64_t i = 0; i < uuid->size(); ++i) {
        ASSERT_OK_AND_ASSIGN(auto uuid_item,
                             CreateUuidWithMainObject(main_obj[i], salt));
        EXPECT_EQ((*uuid)[i], uuid_item);
        if (uuid_item.has_value()) {
          expected_allocs.push_back(AllocationId(uuid_item.value<ObjectId>()));
        }
      }
      EXPECT_EQ(uuid->allocation_ids(), AllocationIdSet(expected_allocs));
    }
  }
  // TODO: add tests for contains_small_alloc once we remove
  // IsUuid in IsSmallAlloc.
}

TEST(UuidTest, CreateUuidWithMainObjectDataSlice_ImplicitSchema) {
  auto alloc1 = Allocate(1043);
  auto alloc2 = Allocate(9043);
  auto main_obj = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({alloc1, alloc2}),
      arolla::CreateDenseArray<ObjectId>(std::vector<OptionalValue<ObjectId>>{
          std::nullopt, alloc1.ObjectByOffset(5), std::nullopt,
          alloc1.ObjectByOffset(7), alloc2.ObjectByOffset(13),
          std::nullopt}));
  ASSERT_OK_AND_ASSIGN(
      auto schema_slice,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(main_obj,
                                                                  "x"));

  EXPECT_EQ(schema_slice.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(schema_slice.allocation_ids().size(), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(schema_slice.allocation_ids().ids()[i].Capacity(),
              main_obj.allocation_ids().ids()[i].Capacity());
    EXPECT_TRUE(schema_slice.allocation_ids().ids()[i].IsSchemasAlloc());
  }

  for (int i = 0; i < schema_slice.size(); ++i) {
    EXPECT_EQ(schema_slice[i].has_value(), main_obj[i].has_value());
    if (!schema_slice[i].has_value()) {
      continue;
    }
    auto schema_obj = schema_slice[i].value<ObjectId>();
    EXPECT_TRUE(schema_obj.IsUuid());
    EXPECT_TRUE(schema_obj.IsSchema());
    EXPECT_TRUE(schema_obj.IsImplicitSchema());
    EXPECT_EQ(schema_obj.Offset(), main_obj[i].value<ObjectId>().Offset());
  }
}

}  // namespace
}  // namespace koladata::internal
