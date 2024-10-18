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
#include "koladata/uuid_utils.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/test_utils.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata {
namespace {

using internal::ObjectId;

using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Not;

TEST(UuidUtilsTest, IsUnspecifiedDataSlice_True) {
  EXPECT_TRUE(IsUnspecifiedDataSlice(UnspecifiedDataSlice()));
}

TEST(UuidUtilsTest, IsUnspecifiedDataSlice_False) {
  auto ds = test::DataItem(42);
  EXPECT_FALSE(IsUnspecifiedDataSlice(ds));

  ds = *CreateUuidFromFields("sentinel", {"__unspecified__"},
                             {test::DataSlice<int64_t>({(1l << 31) - 1})});
  EXPECT_FALSE(IsUnspecifiedDataSlice(ds));

  ds = test::DataSlice<float>({3.14, 2.71}, DataBag::Empty());
  EXPECT_FALSE(IsUnspecifiedDataSlice(ds));
}

TEST(UuidUtilsTest, UnspecifiedDataSlice_DiffMetadata) {
  ObjectId obj_id = internal::CreateUuidObjectWithMetadata(
      internal::StableFingerprintHasher(
          absl::string_view("__unspecified__")).Finish(),
      internal::ObjectId::kUuidFlag);

  EXPECT_THAT(UnspecifiedDataSlice().item(), Not(Eq(obj_id)));
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataSlice) {
  constexpr int64_t kSize = 3;
  auto shape = DataSlice::JaggedShape::FlatFromSize(kSize);

  auto ds_a = test::AllocateDataSlice(3, schema::kObject);
  auto ds_b = test::AllocateDataSlice(3, schema::kObject);

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  // Schema check.
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  // DataSlice checks.
  EXPECT_EQ(ds.size(), kSize);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());

  ASSERT_OK_AND_ASSIGN(
      auto expected,
      internal::CreateUuidFromFields("", {"a", "b"},
                                     {ds_a.slice(), ds_b.slice()}));
  EXPECT_THAT(ds.slice().values<ObjectId>(), ElementsAreArray(
      expected.values<ObjectId>()));

  // Seeded UUIds
  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_1,
      CreateUuidFromFields("seed_1", {"a", "b"}, {ds_a, ds_b}));
  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_2,
      CreateUuidFromFields("seed_2", {"a", "b"}, {ds_a, ds_b}));
  ASSERT_OK_AND_ASSIGN(expected,
                       internal::CreateUuidFromFields(
                           "seed_1", {"a", "b"},
                           {ds_a.slice(), ds_b.slice()}));
  EXPECT_THAT(ds_with_seed_1.slice().values<ObjectId>(), ElementsAreArray(
      expected.values<ObjectId>()));
  EXPECT_THAT(ds_with_seed_1.slice().values<ObjectId>(),
              Not(ElementsAreArray(
                  ds_with_seed_2.slice().values<ObjectId>())));
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataSlice_List) {
  auto ds_a = test::AllocateDataSlice(3, schema::kObject);
  auto ds_b = test::AllocateDataSlice(3, schema::kObject);

  ASSERT_OK_AND_ASSIGN(
      auto ds, CreateListUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  ds.slice().values<ObjectId>().ForEach(
      [&](int64_t id, bool present, ObjectId object_id) {
        EXPECT_TRUE(object_id.IsUuid());
        EXPECT_TRUE(object_id.IsList());
      });
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataSlice_Dict) {
  auto ds_a = test::AllocateDataSlice(3, schema::kObject);
  auto ds_b = test::AllocateDataSlice(3, schema::kObject);

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateDictUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  ds.slice().values<ObjectId>().ForEach(
      [&](int64_t id, bool present, ObjectId object_id) {
        EXPECT_TRUE(object_id.IsUuid());
        EXPECT_TRUE(object_id.IsDict());
      });
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataItem) {
  auto ds_a = test::DataItem(internal::AllocateSingleObject());
  auto ds_b = test::DataItem(42);

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));

  // Schema check.
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  // DataItem checks.
  EXPECT_EQ(ds.size(), 1);
  EXPECT_EQ(ds.GetShape().rank(), 0);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  auto expected = internal::CreateUuidFromFields(
      "", {"a", "b"}, {ds_a.item(), ds_b.item()});
  EXPECT_EQ(ds.item(), expected);

  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_1,
      CreateUuidFromFields("seed_1", {"a", "b"}, {ds_a, ds_b}));
  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_2,
      CreateUuidFromFields("seed_2", {"a", "b"}, {ds_a, ds_b}));

  expected = internal::CreateUuidFromFields("seed_1", {"a", "b"},
                                            {ds_a.item(), ds_b.item()});
  EXPECT_EQ(ds_with_seed_1.item(), expected);
  EXPECT_NE(ds_with_seed_1.item(), ds_with_seed_2.item());
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataItem_List) {
  auto ds_a = test::DataItem(internal::AllocateSingleObject());
  auto ds_b = test::DataItem(42);

  ASSERT_OK_AND_ASSIGN(
      auto ds, CreateListUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_TRUE(ds.item().value<ObjectId>().IsList());
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataItem_Dict) {
  auto ds_a = test::DataItem(internal::AllocateSingleObject());
  auto ds_b = test::DataItem(42);

  ASSERT_OK_AND_ASSIGN(
      auto ds, CreateDictUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_TRUE(ds.item().value<ObjectId>().IsDict());
}

TEST(UuidUtilsTest, CreateUuidFromFields_Empty) {
  ASSERT_OK_AND_ASSIGN(auto ds, CreateUuidFromFields("", {}, {}));
  EXPECT_EQ(ds.size(), 1);

  EXPECT_EQ(ds.GetShape().rank(), 0);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_FALSE(ds.item().value<ObjectId>().IsSchema());
}

}  // namespace
}  // namespace koladata
