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
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/stable_fingerprint.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/test_utils.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata {
namespace {

using internal::ObjectId;

using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::IsEmpty;
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
      internal::StableFingerprintHasher(absl::string_view("__unspecified__"))
          .Finish(),
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

  ASSERT_OK_AND_ASSIGN(auto expected,
                       internal::CreateUuidFromFields(
                           "", {"a", "b"}, {ds_a.slice(), ds_b.slice()}));
  EXPECT_THAT(ds.slice(), ElementsAreArray(expected));

  // Seeded UUIds
  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_1,
      CreateUuidFromFields("seed_1", {"a", "b"}, {ds_a, ds_b}));
  ASSERT_OK_AND_ASSIGN(
      auto ds_with_seed_2,
      CreateUuidFromFields("seed_2", {"a", "b"}, {ds_a, ds_b}));
  ASSERT_OK_AND_ASSIGN(
      expected, internal::CreateUuidFromFields("seed_1", {"a", "b"},
                                               {ds_a.slice(), ds_b.slice()}));
  EXPECT_THAT(ds_with_seed_1.slice(), ElementsAreArray(expected));
  EXPECT_THAT(ds_with_seed_1.slice(),
              Not(ElementsAreArray(ds_with_seed_2.slice())));
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataSlice_List) {
  auto ds_a = test::AllocateDataSlice(3, schema::kObject);
  auto ds_b = test::AllocateDataSlice(3, schema::kObject);

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateListUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
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
  auto expected = internal::CreateUuidFromFields("", {"a", "b"},
                                                 {ds_a.item(), ds_b.item()});
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

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateListUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_TRUE(ds.item().value<ObjectId>().IsList());
}

TEST(UuidUtilsTest, CreateUuidFromFields_DataItem_Dict) {
  auto ds_a = test::DataItem(internal::AllocateSingleObject());
  auto ds_b = test::DataItem(42);

  ASSERT_OK_AND_ASSIGN(auto ds,
                       CreateDictUuidFromFields("", {"a", "b"}, {ds_a, ds_b}));
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

TEST(UuidUtilsTest, CreateUuidFromFields_Empty_List) {
  ASSERT_OK_AND_ASSIGN(auto ds, CreateListUuidFromFields("", {}, {}));
  EXPECT_EQ(ds.size(), 1);

  EXPECT_EQ(ds.GetShape().rank(), 0);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_TRUE(ds.item().value<ObjectId>().IsList());
}

TEST(UuidUtilsTest, CreateUuidFromFields_Empty_Dict) {
  ASSERT_OK_AND_ASSIGN(auto ds, CreateDictUuidFromFields("", {}, {}));
  EXPECT_EQ(ds.size(), 1);

  EXPECT_EQ(ds.GetShape().rank(), 0);
  EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

  EXPECT_TRUE(ds.item().value<ObjectId>().IsUuid());
  EXPECT_TRUE(ds.item().value<ObjectId>().IsDict());
}

absl::flat_hash_set<ObjectId> GetUniqueObjectIds(const DataSlice& ds) {
  CHECK_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
  absl::flat_hash_set<ObjectId> unique_object_ids;
  ds.slice().values<ObjectId>().ForEach(
      [&](int64_t id, bool present, ObjectId object_id) {
        unique_object_ids.insert(object_id);
      });
  return unique_object_ids;
}

template <typename T>
T Intersection(const T& a, const T& b) {
  const T& smaller = a.size() < b.size() ? a : b;
  const T& larger = a.size() < b.size() ? b : a;
  T result;
  for (const auto& element : smaller) {
    if (larger.contains(element)) {
      result.insert(element);
    }
  }
  return result;
}

TEST(UuidUtilsTest, CreateUuidsWithAllocationSize) {
  constexpr absl::string_view kSeeds[] = {"foo", "bar"};
  constexpr int kMinAllocationSize = 7;
  constexpr int kMaxAllocationSize = 20;

  for (absl::string_view seed : kSeeds) {
    for (int size = kMinAllocationSize; size <= kMaxAllocationSize; ++size) {
      ASSERT_OK_AND_ASSIGN(const auto ds,
                           CreateUuidsWithAllocationSize(seed, size));
      EXPECT_EQ(ds.size(), size);

      EXPECT_EQ(ds.GetShape().rank(), 1);
      EXPECT_EQ(ds.dtype(), arolla::GetQType<ObjectId>());
      EXPECT_EQ(ds.GetSchemaImpl(), schema::kItemId);

      const absl::flat_hash_set<ObjectId> object_ids = GetUniqueObjectIds(ds);
      EXPECT_EQ(object_ids.size(), size);  // Each object id is distinct.
      for (const ObjectId& object_id : object_ids) {
        EXPECT_TRUE(object_id.IsUuid());
      }

      for (absl::string_view other_seed : kSeeds) {
        for (int other_size = kMinAllocationSize;
             other_size <= kMaxAllocationSize; ++other_size) {
          ASSERT_OK_AND_ASSIGN(
              const auto other_ds,
              CreateUuidsWithAllocationSize(other_seed, other_size));

          if (other_seed == seed && other_size == size) {
            // Using exactly the same seed and allocation size should yield the
            // same object ids in the same order.
            EXPECT_THAT(other_ds.slice(), ElementsAreArray(ds.slice()));
            continue;
          }

          // A different seed and/or the allocation size should yield completely
          // different object ids.
          const absl::flat_hash_set<ObjectId> other_object_ids =
              GetUniqueObjectIds(other_ds);
          EXPECT_THAT(Intersection(object_ids, other_object_ids), IsEmpty());
        }
      }
    }
  }
}

}  // namespace
}  // namespace koladata
