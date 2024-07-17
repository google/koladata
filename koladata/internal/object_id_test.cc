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
#include "koladata/internal/object_id.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "koladata/internal/stable_fingerprint.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {
namespace {

using ::testing::IsEmpty;
using ::testing::MatchesRegex;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;

TEST(ObjectIdTest, AllocateSingleObjectStress) {
  ObjectId id = AllocateSingleObject();
  EXPECT_TRUE(id.IsSmallAlloc());
  EXPECT_TRUE(AllocationId(id).IsSmall());
  // We copy kIdBitCount constant from AllocateSingleObject to avoid exposing
  // it out of cc-file.
  constexpr int32_t kIdBitCount = 24;
  for (int64_t i = 0; i != (1 << (kIdBitCount + 1)); ++i) {
    ObjectId new_id = AllocateSingleObject();
    EXPECT_TRUE(new_id.IsSmallAlloc());
    EXPECT_TRUE(AllocationId(new_id).IsSmall());
    if (id > new_id) {
      FAIL() << id << " > " << new_id;
    }
    id = new_id;
  }
}

TEST(ObjectIdTest, AllocationIdSmallSize) {
  size_t expected_capacity = 1;
  for (size_t size = 0; size <= 257; ++size) {
    if (size > expected_capacity) expected_capacity *= 2;
    AllocationId alloc_id = Allocate(size);
    EXPECT_EQ(alloc_id.IsSmall(), size <= kSmallAllocMaxCapacity);
    ASSERT_EQ(alloc_id.Capacity(), expected_capacity) << size;
    for (int64_t i = 0; i != 100; ++i) {
      AllocationId alloc_id_new = Allocate(size);
      EXPECT_LT(alloc_id, alloc_id_new) << size << " " << i;
      ASSERT_EQ(alloc_id_new.Capacity(), expected_capacity) << size;
      alloc_id = alloc_id_new;
    }
    std::vector<AllocationId> other_allocs = {
        Allocate(size), Allocate(0), Allocate(size * 2), Allocate(size + 1)};
    for (int64_t i = 0; i < size; ++i) {
      ObjectId obj_id = alloc_id.ObjectByOffset(i);
      ASSERT_TRUE(obj_id.IsAllocated());
      ASSERT_FALSE(obj_id.IsUuid());
      ASSERT_EQ(alloc_id, AllocationId(obj_id)) << size << " " << i;
      ASSERT_EQ(obj_id.Offset(), i) << size << " " << i;
      ASSERT_TRUE(alloc_id.Contains(obj_id)) << size << " " << i;
      for (const AllocationId& other_alloc : other_allocs) {
        ASSERT_FALSE(other_alloc.Contains(obj_id))
            << size << " " << i << " " << other_alloc.Capacity();
      }
    }
  }
}

TEST(ObjectIdTest, AllocationIdNotContains) {
  AllocationId alloc_id = Allocate(1024);
  for (int64_t i = 0; i != 10000; ++i) {
    ASSERT_FALSE(alloc_id.Contains(Allocate(1024).ObjectByOffset(0))) << i;
  }
}

TEST(ObjectIdTest, UuidAndAllocatedFlagsAreExclusive) {
  std::vector uuid_cases{
      CreateUuidObject(arolla::FingerprintHasher("").Combine(42).Finish()),
      CreateUuidExplicitSchema(
          arolla::FingerprintHasher("").Combine(42).Finish()),
      // Uuid implicit schema.
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          AllocateSingleObject(),
          arolla::FingerprintHasher("").Combine(42).Finish()),
      // Uuid nofollow schema.
      CreateNoFollowWithMainObject(CreateUuidExplicitSchema(
          arolla::FingerprintHasher("").Combine(42).Finish())),
      CreateNoFollowWithMainObject(
          CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
              AllocateSingleObject(),
              arolla::FingerprintHasher("").Combine(42).Finish())),
  };
  for (auto uuid_obj : uuid_cases) {
    EXPECT_TRUE(uuid_obj.IsUuid());
    EXPECT_FALSE(uuid_obj.IsAllocated());
    EXPECT_FALSE(uuid_obj.IsList());
    EXPECT_FALSE(uuid_obj.IsDict());
  }
  std::vector alloc_cases{
      AllocateSingleList(),
      AllocateSingleDict(),
      AllocateExplicitSchema(),
  };
  std::vector checks{
      &ObjectId::IsList,
      &ObjectId::IsDict,
      &ObjectId::IsSchema,
  };
  for (int i = 0; i < alloc_cases.size(); ++i) {
    EXPECT_FALSE(alloc_cases[i].IsUuid());
    EXPECT_TRUE((alloc_cases[i].*checks[i])());
    for (int j = 0; j < alloc_cases.size(); ++j) {
      if (i != j) {
        EXPECT_FALSE((alloc_cases[i].*checks[j])());
      }
    }
  }
}

TEST(ObjectIdTest, ListAllocation) {
  EXPECT_FALSE(Allocate(3).IsListsAlloc());
  EXPECT_FALSE(Allocate(3).ObjectByOffset(0).IsList());
  EXPECT_TRUE(AllocateLists(3).IsListsAlloc());
  EXPECT_TRUE(AllocateLists(3).ObjectByOffset(0).IsList());
  EXPECT_FALSE(AllocateSingleObject().IsList());
  EXPECT_TRUE(AllocateSingleList().IsList());
}

TEST(ObjectIdTest, DictAllocation) {
  EXPECT_FALSE(Allocate(3).IsDictsAlloc());
  EXPECT_FALSE(Allocate(3).ObjectByOffset(0).IsDict());
  EXPECT_TRUE(AllocateDicts(3).IsDictsAlloc());
  EXPECT_TRUE(AllocateDicts(3).ObjectByOffset(0).IsDict());
  EXPECT_FALSE(AllocateSingleObject().IsDict());
  EXPECT_TRUE(AllocateSingleDict().IsDict());
}

TEST(ObjectIdTest, ListUuid) {
  ObjectId list_id = CreateUuidObjectWithMetadata(
      arolla::FingerprintHasher("list-uuid").Combine(57).Finish(),
      ObjectId::kListFlag | ObjectId::kUuidFlag);

  EXPECT_TRUE(list_id.IsList());
  EXPECT_TRUE(list_id.IsUuid());
  EXPECT_FALSE(list_id.IsAllocated());
  EXPECT_FALSE(list_id.IsDict());
  EXPECT_FALSE(list_id.IsSchema());
}

TEST(ObjectIdTest, DictUuid) {
  ObjectId dict_id = CreateUuidObjectWithMetadata(
      arolla::FingerprintHasher("dict-uuid").Combine(57).Finish(),
      ObjectId::kDictFlag | ObjectId::kUuidFlag);

  EXPECT_TRUE(dict_id.IsDict());
  EXPECT_TRUE(dict_id.IsUuid());
  EXPECT_FALSE(dict_id.IsAllocated());
  EXPECT_FALSE(dict_id.IsList());
  EXPECT_FALSE(dict_id.IsSchema());
}

TEST(ObjectIdTest, ExplicitSchemaAllocation) {
  EXPECT_FALSE(Allocate(3).IsSchemasAlloc());
  EXPECT_FALSE(Allocate(3).IsExplicitSchemasAlloc());
  EXPECT_FALSE(Allocate(3).ObjectByOffset(0).IsSchema());
  EXPECT_FALSE(AllocateSingleObject().IsSchema());
  EXPECT_TRUE(AllocateExplicitSchema().IsSchema());
  EXPECT_TRUE(AllocateExplicitSchema().IsExplicitSchema());
  EXPECT_TRUE(AllocationId(AllocateExplicitSchema()).IsExplicitSchemasAlloc());
  EXPECT_FALSE(AllocateExplicitSchema().IsImplicitSchema());
  EXPECT_FALSE(AllocateExplicitSchema().IsNoFollowSchema());
}

TEST(ObjectIdTest, SchemaObjects) {
  auto uuid_explicit_schema = CreateUuidExplicitSchema(
      arolla::FingerprintHasher("").Combine(42).Finish());
  // Uuid implicit schema.
  auto uuid_implicit_schema =
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          AllocateSingleObject(),
          arolla::FingerprintHasher("").Combine(42).Finish());
  // nofollow schema.
  auto nofollow_of_implicit_schema = CreateNoFollowWithMainObject(
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          AllocateSingleObject(),
          arolla::FingerprintHasher("").Combine(42).Finish()));
  auto explicit_schema = AllocateExplicitSchema();

  // All are schemas.
  EXPECT_TRUE(uuid_explicit_schema.IsSchema());
  EXPECT_TRUE(uuid_implicit_schema.IsSchema());
  EXPECT_TRUE(nofollow_of_implicit_schema.IsSchema());
  EXPECT_TRUE(explicit_schema.IsSchema());

  // Testing for explicit schemas.
  EXPECT_TRUE(uuid_explicit_schema.IsExplicitSchema());
  EXPECT_TRUE(explicit_schema.IsExplicitSchema());
  EXPECT_FALSE(uuid_implicit_schema.IsExplicitSchema());
  EXPECT_TRUE(nofollow_of_implicit_schema.IsExplicitSchema());

  // Testing for explicit schemas alloc.
  EXPECT_TRUE(AllocationId(uuid_explicit_schema).IsExplicitSchemasAlloc());
  EXPECT_TRUE(AllocationId(explicit_schema).IsExplicitSchemasAlloc());
  EXPECT_FALSE(AllocationId(uuid_implicit_schema).IsExplicitSchemasAlloc());
  EXPECT_TRUE(
      AllocationId(nofollow_of_implicit_schema).IsExplicitSchemasAlloc());

  // Testing for implicit schemas.
  EXPECT_TRUE(uuid_implicit_schema.IsImplicitSchema());
  EXPECT_FALSE(uuid_explicit_schema.IsImplicitSchema());
  EXPECT_FALSE(nofollow_of_implicit_schema.IsImplicitSchema());
  EXPECT_FALSE(explicit_schema.IsImplicitSchema());

  // Testing for nofollow schemas.
  EXPECT_TRUE(nofollow_of_implicit_schema.IsNoFollowSchema());
  EXPECT_FALSE(uuid_explicit_schema.IsNoFollowSchema());
  EXPECT_FALSE(uuid_implicit_schema.IsNoFollowSchema());
  EXPECT_FALSE(explicit_schema.IsNoFollowSchema());
}

TEST(ObjectIdTest, NoFollow_Roundtrip) {
  auto explicit_schema = AllocateExplicitSchema();
  auto uuid_implicit_schema =
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          AllocateSingleObject(),
          arolla::FingerprintHasher("").Combine(42).Finish());
  auto uuid_explicit_schema = CreateUuidExplicitSchema(
      arolla::FingerprintHasher("").Combine(42).Finish());

  auto nofollow = CreateNoFollowWithMainObject(explicit_schema);
  EXPECT_FALSE(explicit_schema.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsSchema());
  EXPECT_TRUE(nofollow.IsExplicitSchema());
  EXPECT_EQ(explicit_schema, GetOriginalFromNoFollow(nofollow));

  nofollow = CreateNoFollowWithMainObject(uuid_implicit_schema);
  EXPECT_FALSE(uuid_implicit_schema.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsSchema());
  EXPECT_TRUE(nofollow.IsExplicitSchema());
  EXPECT_FALSE(nofollow.IsImplicitSchema());
  EXPECT_EQ(uuid_implicit_schema, GetOriginalFromNoFollow(nofollow));

  nofollow = CreateNoFollowWithMainObject(uuid_explicit_schema);
  EXPECT_FALSE(uuid_explicit_schema.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsNoFollowSchema());
  EXPECT_TRUE(nofollow.IsSchema());
  EXPECT_TRUE(nofollow.IsExplicitSchema());
  EXPECT_EQ(uuid_explicit_schema, GetOriginalFromNoFollow(nofollow));
}

TEST(ObjectIdTest, NoFollow_Errors) {
  EXPECT_DEBUG_DEATH(CreateNoFollowWithMainObject(AllocateSingleObject()), "");
  EXPECT_DEBUG_DEATH(CreateNoFollowWithMainObject(AllocateSingleList()), "");
  EXPECT_DEBUG_DEATH(CreateNoFollowWithMainObject(AllocateSingleDict()), "");
  EXPECT_DEBUG_DEATH(CreateNoFollowWithMainObject(CreateUuidObject(
                         arolla::FingerprintHasher("").Combine(57).Finish())),
                     "");

  EXPECT_DEBUG_DEATH(GetOriginalFromNoFollow(AllocateExplicitSchema()), "");
  EXPECT_DEBUG_DEATH(GetOriginalFromNoFollow(AllocateSingleObject()), "");
  EXPECT_DEBUG_DEATH(
      GetOriginalFromNoFollow(ObjectId::NoFollowObjectSchemaId()), "");
}

TEST(ObjectIdTest, NoFollowObjectSchemaId) {
  auto nofollow_object_schema = ObjectId::NoFollowObjectSchemaId();
  EXPECT_TRUE(nofollow_object_schema.IsNoFollowSchema());
  EXPECT_TRUE(nofollow_object_schema.IsExplicitSchema());
  EXPECT_TRUE(nofollow_object_schema.IsSchema());
}

TEST(ObjectIdTest, CreateUuidObject) {
  ObjectId id1 =
      CreateUuidObject(arolla::FingerprintHasher("").Combine(57).Finish());
  EXPECT_FALSE(id1.IsAllocated());
  EXPECT_TRUE(id1.IsUuid());
  std::set<ObjectId> id_set = {id1};
  for (absl::uint128 x = 75;
       x < absl::MakeUint128(std::numeric_limits<uint64_t>::max() / 2, 0);
       x *= 2) {
    ObjectId id2 = CreateUuidObject(
        arolla::FingerprintHasher("")
            .Combine(absl::Uint128Low64(x), absl::Uint128High64(x))
            .Finish());
    ASSERT_FALSE(id2.IsAllocated());
    ASSERT_TRUE(id2.IsUuid());
    ASSERT_NE(id1, id2);
    ASSERT_TRUE(id_set.insert(id2).second) << id2 << " " << id_set.size();
  }

  EXPECT_NE(id1.DebugString(), "");  // not fail

  AllocationId alloc(id1);
  EXPECT_TRUE(alloc.IsSmall());
  EXPECT_EQ(alloc.Capacity(), 1);
  EXPECT_TRUE(alloc.Contains(id1));
}

TEST(ObjectIdTest, CreateUuidObjectWithMainObject) {
  AllocationId alloc = Allocate(1024);
  ObjectId main_id0 = alloc.ObjectByOffset(0);
  ObjectId main_id1 = alloc.ObjectByOffset(1);
  ObjectId id0_a = CreateUuidWithMainObject(
      main_id0, StableFingerprintHasher("a").Combine(alloc).Finish());
  EXPECT_EQ(id0_a.Offset(), 0);
  ObjectId id1_a = CreateUuidWithMainObject(
      main_id1, StableFingerprintHasher("a").Combine(alloc).Finish());
  EXPECT_EQ(id1_a.Offset(), 1);
  ObjectId id1_b = CreateUuidWithMainObject(
      main_id1, StableFingerprintHasher("b").Combine(alloc).Finish());
  EXPECT_EQ(id1_b.Offset(), 1);

  AllocationId alloc_a(id0_a);
  EXPECT_FALSE(alloc_a.IsSmall());
  EXPECT_EQ(alloc_a.Capacity(), alloc.Capacity());
  EXPECT_TRUE(alloc_a.Contains(id0_a));
  EXPECT_TRUE(alloc_a.Contains(id1_a));
  EXPECT_FALSE(alloc_a.Contains(id1_b));
  EXPECT_EQ(alloc_a, AllocationId(id1_a));

  AllocationId alloc_b(id1_b);
  EXPECT_NE(alloc_a, alloc_b);
  EXPECT_FALSE(alloc_b.IsSmall());
  EXPECT_EQ(alloc_b.Capacity(), alloc.Capacity());
  EXPECT_FALSE(alloc_b.Contains(id0_a));
  EXPECT_FALSE(alloc_b.Contains(id1_a));
  EXPECT_TRUE(alloc_b.Contains(id1_b));

  std::set<ObjectId> id_set;
  for (auto id : {id0_a, id1_a, id1_b}) {
    EXPECT_FALSE(id.IsAllocated()) << id;
    EXPECT_TRUE(id.IsUuid()) << id;
    EXPECT_NE(id.DebugString(), "");  // not fail
    id_set.insert(id);
  }
  for (absl::uint128 x = 75;
       x < absl::MakeUint128(std::numeric_limits<uint64_t>::max() / 2, 0);
       x *= 2) {
    for (std::string salt : {"a", "b", "c"}) {
      ObjectId id = CreateUuidWithMainObject(
          main_id0, StableFingerprintHasher(salt)
                        .Combine(absl::Uint128Low64(x), absl::Uint128High64(x))
                        .Finish());
      ASSERT_FALSE(id.IsAllocated());
      ASSERT_TRUE(id.IsUuid());
      ASSERT_NE(id0_a, id);
      ASSERT_NE(id1_a, id);
      ASSERT_NE(id1_b, id);
      ASSERT_TRUE(id_set.insert(id).second) << id << " " << id_set.size();
    }
  }
}

TEST(ObjectIdTest, AllocationIdBigSize) {
  size_t expected_capacity = 1024 * 1024;
  for (size_t size = expected_capacity; size <= (1ull << 40); size *= 2) {
    while (size > expected_capacity) expected_capacity *= 2;
    AllocationId alloc_id = Allocate(size);
    ASSERT_EQ(alloc_id.Capacity(), expected_capacity) << size;
    for (int64_t i = 0; i != 10000; ++i) {
      AllocationId alloc_id_new = Allocate(size);
      ASSERT_LT(alloc_id, alloc_id_new) << size << " " << i;
      ASSERT_EQ(alloc_id_new.Capacity(), expected_capacity) << size;
      alloc_id = alloc_id_new;
    }
    std::vector<AllocationId> other_allocs = {
        Allocate(size), Allocate(0), Allocate(size / 2), Allocate(size - 1)};
    for (int64_t i = 1; i < size; i *= 2) {
      ObjectId obj_id = alloc_id.ObjectByOffset(i);
      ASSERT_EQ(alloc_id, AllocationId(obj_id)) << size << " " << i;
      ASSERT_EQ(obj_id.Offset(), i) << size << " " << i;
      ASSERT_TRUE(alloc_id.Contains(obj_id)) << size << " " << i;
      for (const AllocationId& other_alloc : other_allocs) {
        ASSERT_FALSE(other_alloc.Contains(obj_id)) << size << " " << i;
      }
    }
  }
}

TEST(ObjectIdTest, AllocationIdMultithreading) {
  constexpr size_t kAllocSize = 1ull << 40;
#ifdef NDEBUG
  constexpr int64_t kThreadCount = 3000;
  constexpr int64_t kAllocPerThread = 3000;
#else  /* NDEBUG */
  constexpr int64_t kThreadCount = 1000;
  constexpr int64_t kAllocPerThread = 1000;
#endif /* NDEBUG */

  auto allocate_fn = [](std::vector<AllocationId>* allocs) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    allocs->reserve(kAllocPerThread * 2);
    for (int64_t i = 0; i != kAllocPerThread; ++i) {
      allocs->push_back(Allocate(kAllocSize));
    }
    if (std::adjacent_find(allocs->begin(), allocs->end(),
                           std::greater_equal<AllocationId>()) !=
        allocs->end()) {
      FAIL() << "Allocations are not increasing";
    }
    for (int64_t i = 0; i != kAllocPerThread; ++i) {
      allocs->push_back(AllocationId(AllocateSingleObject()));
    }
    if (std::adjacent_find(allocs->begin() + kAllocPerThread, allocs->end(),
                           std::greater_equal<AllocationId>()) !=
        allocs->end()) {
      FAIL() << "Single object allocations are not increasing";
    }
  };

  std::vector<std::vector<AllocationId>> allocs(kThreadCount);

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int64_t t = 0; t != kThreadCount; ++t) {
    threads.push_back(std::thread(allocate_fn, &allocs[t]));
  }
  // Join all threads.
  for (int64_t t = 0; t != kThreadCount; ++t) {
    threads[t].join();
  }

  std::vector<AllocationId> all_allocs;
  all_allocs.reserve(kThreadCount * kAllocPerThread * 2);
  for (auto& cur_allocs : allocs) {
    ASSERT_EQ(cur_allocs.size(), kAllocPerThread * 2);
    all_allocs.insert(all_allocs.end(), cur_allocs.begin(), cur_allocs.end());
    cur_allocs = {};
  }
  std::sort(all_allocs.begin(), all_allocs.end());
  if (auto it = std::adjacent_find(all_allocs.begin(), all_allocs.end());
      it != all_allocs.end()) {
    FAIL() << "Duplicated allocations: " << *it;
  }
}

TEST(ObjectIdTest, TypedValueQType) {
  AllocationId alloc_id = Allocate(10);
  EXPECT_EQ(arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(0)).GetType(),
            arolla::GetQType<ObjectId>());
  EXPECT_NE(arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(0))
                .GetFingerprint(),
            arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(1))
                .GetFingerprint());
  AllocationId alloc_id2 = Allocate(10);
  EXPECT_NE(arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(0))
                .GetFingerprint(),
            arolla::TypedValue::FromValue(alloc_id2.ObjectByOffset(0))
                .GetFingerprint());
}

// Testing that we show ids in expected order in repr.
TEST(ObjectIdTest, TypedValueRepr) {
  AllocationId alloc_id = Allocate(1024);
  std::string repr0 =
      arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(0)).Repr();
  EXPECT_THAT(repr0, testing::MatchesRegex(R"regexp([a-f0-9]*.000)regexp"));
  std::string repr1 =
      arolla::TypedValue::FromValue(alloc_id.ObjectByOffset(10)).Repr();
  EXPECT_THAT(repr1, testing::MatchesRegex(R"regexp([a-f0-9]*.00a)regexp"));
  std::string repr2 = absl::StrCat(alloc_id.ObjectByOffset(0xff));
  EXPECT_THAT(repr2, testing::MatchesRegex(R"regexp([a-f0-9]*.0ff)regexp"));

  EXPECT_NE(repr0, repr1);
  // All but last digit (in base 16) should be the same.
  EXPECT_EQ(repr0.substr(0, repr0.size() - 1),
            repr1.substr(0, repr1.size() - 1));
}

TEST(ObjectIdTest, AbslHash) {
  AllocationId alloc_id1 = Allocate(10);
  AllocationId alloc_id2 = Allocate(10);
  std::vector cases{
      alloc_id1.ObjectByOffset(0), alloc_id1.ObjectByOffset(2),
      alloc_id1.ObjectByOffset(4), alloc_id2.ObjectByOffset(0),
      alloc_id2.ObjectByOffset(2),
  };

  for (size_t size = 7; size < (1ull << 40); size *= 2) {
    for (int64_t i = 0; i < 16; ++i) {
      AllocationId alloc_id = Allocate(size);
      cases.push_back(alloc_id.ObjectByOffset(0));
      cases.push_back(alloc_id.ObjectByOffset(size / 2));
      cases.push_back(alloc_id.ObjectByOffset(size - 1));
    }
  }

  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
}

TEST(ObjectIdTest, AllocationIdSet) {
  AllocationIdSet ids;
  EXPECT_FALSE(ids.contains_small_allocation_id());
  EXPECT_TRUE(ids.empty());
  EXPECT_EQ(ids.size(), 0);
  EXPECT_THAT(ids, IsEmpty());
  AllocationId alloc_id1 = Allocate(10);
  ids.Insert(alloc_id1);
  EXPECT_FALSE(ids.contains_small_allocation_id());
  EXPECT_FALSE(ids.empty());
  EXPECT_EQ(ids.size(), 1);
  EXPECT_THAT(ids, UnorderedElementsAre(alloc_id1));
  for (int64_t i = 0; i != 10; ++i) {
    EXPECT_FALSE(ids.Insert(alloc_id1));
  }
  EXPECT_THAT(ids, UnorderedElementsAre(alloc_id1));

  AllocationId alloc_id2 = Allocate(27);
  EXPECT_TRUE(ids.Insert(alloc_id2));
  EXPECT_THAT(ids, UnorderedElementsAre(alloc_id1, alloc_id2));

  for (int64_t i = 0; i != 10; ++i) {
    EXPECT_TRUE(ids.Insert(Allocate(i + 17)));
    EXPECT_EQ(ids.size(), i + 3);
  }

  AllocationIdSet ids2;
  ids2.Insert(alloc_id1);
  ids2.Insert(ids);
  EXPECT_EQ(ids2.size(), ids.size());
  EXPECT_THAT(ids2, UnorderedElementsAreArray(ids));
}

TEST(ObjectIdTest, AllocationIdSetAbslHash) {
  AllocationIdSet ids_1(std::vector{Allocate(10), Allocate(10)});
  AllocationIdSet ids_2(std::vector{Allocate(10), Allocate(11)});
  AllocationIdSet ids_3(Allocate(10));
  AllocationIdSet ids_4(/*contains_small_allocation_id=*/true);
  AllocationIdSet ids_5(/*contains_small_allocation_id=*/true);
  ids_5.Insert(Allocate(10));
  AllocationIdSet ids_6;
  ids_6.Insert(Allocate(10));
  std::vector cases{ids_1, ids_2, ids_3, ids_4, ids_5, ids_6};

  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
}

TEST(ObjectIdTest, AllocationIdSetFromSpan) {
  std::vector<AllocationId> allocs;
  for (int64_t i = 0; i != 10; ++i) {
    allocs.push_back(Allocate(7 << i));
  }
  auto expected_allocs = allocs;
  // Add duplicates
  for (const AllocationId& alloc : std::vector(allocs)) {
    allocs.push_back(alloc);
  }
  std::shuffle(allocs.begin(), allocs.end(), absl::BitGen());
  AllocationIdSet id_set(allocs);
  EXPECT_THAT(id_set, UnorderedElementsAreArray(expected_allocs));
  EXPECT_FALSE(id_set.contains_small_allocation_id());
  id_set.Insert(id_set);  // no op
  EXPECT_FALSE(id_set.contains_small_allocation_id());
  EXPECT_THAT(id_set, UnorderedElementsAreArray(expected_allocs));
  allocs.clear();
  for (int64_t i = 0; i != 10; ++i) {
    allocs.push_back(Allocate(7 << i));
    allocs.push_back(allocs.back());  // duplicate
    expected_allocs.push_back(allocs.back());
  }
  std::shuffle(allocs.begin(), allocs.end(), absl::BitGen());
  id_set.Insert(AllocationIdSet(allocs));
  EXPECT_FALSE(id_set.contains_small_allocation_id());
  EXPECT_THAT(id_set, UnorderedElementsAreArray(expected_allocs));
}

TEST(ObjectIdTest, AllocationIdSetWithSmall) {
  {
    auto id_set = AllocationIdSet(/*contains_small_allocation_id=*/true);
    EXPECT_TRUE(id_set.contains_small_allocation_id());
    EXPECT_TRUE(id_set.empty());
  }
  {
    AllocationIdSet id_set;
    ObjectId id =
        CreateUuidObject(arolla::FingerprintHasher("").Combine(57).Finish());
    EXPECT_TRUE(id_set.Insert(AllocationId(id)));
    EXPECT_TRUE(id_set.contains_small_allocation_id());
    EXPECT_TRUE(id_set.empty());
  }
  AllocationIdSet id_set;
  std::vector<AllocationId> allocs;
  for (int64_t i = 0; i != 10; ++i) {
    allocs.push_back(Allocate(7 << i));
    id_set.Insert(allocs.back());
    if (i % 3 == 2) {
      ObjectId id = CreateUuidObject(
          arolla::FingerprintHasher("").Combine(57 * i).Finish());
      EXPECT_EQ(id_set.Insert(AllocationId(id)), i == 2);
      EXPECT_FALSE(
          id_set.Insert(AllocationId(Allocate(kSmallAllocMaxCapacity))));
      EXPECT_FALSE(id_set.Insert(AllocationId(AllocateSingleObject())));
    }
  }
  EXPECT_THAT(id_set, UnorderedElementsAreArray(allocs));
  EXPECT_TRUE(id_set.contains_small_allocation_id());
}

TEST(ObjectIdTest, AllocationIdSetSmallFromSpan) {
  ObjectId uuid =
      CreateUuidObject(arolla::FingerprintHasher("").Combine(57).Finish());
  {
    auto alloc = Allocate(17);
    AllocationIdSet id_set({alloc, AllocationId(uuid)});
    EXPECT_THAT(id_set, UnorderedElementsAre(alloc));
    EXPECT_TRUE(id_set.contains_small_allocation_id());
  }
  {
    std::vector<AllocationId> allocs;
    std::vector<AllocationId> expected_allocs;
    for (int64_t i = 0; i != 10; ++i) {
      allocs.push_back(Allocate(7 << i));
      expected_allocs.push_back(allocs.back());
      if (i % 3 == 2) {
        allocs.push_back(AllocationId(uuid));
      }
    }
    std::shuffle(allocs.begin(), allocs.end(), absl::BitGen());
    AllocationIdSet id_set(allocs);
    EXPECT_THAT(id_set, UnorderedElementsAreArray(expected_allocs));
    EXPECT_TRUE(id_set.contains_small_allocation_id());
  }
}

TEST(ObjectIdTest, AllocationIdSetUnionSmall) {
  ObjectId uuid =
      CreateUuidObject(arolla::FingerprintHasher("").Combine(57).Finish());
  auto alloc1 = Allocate(17);
  auto alloc2 = Allocate(170);
  AllocationIdSet id_set1(alloc1);
  AllocationIdSet id_set2({alloc2, AllocationId(uuid)});
  {
    auto id_set = id_set1;
    EXPECT_FALSE(id_set.contains_small_allocation_id());
    id_set.Insert(id_set2);
    EXPECT_TRUE(id_set.contains_small_allocation_id());
  }
  {
    auto id_set = id_set2;
    EXPECT_TRUE(id_set.contains_small_allocation_id());
    id_set.Insert(id_set1);
    EXPECT_TRUE(id_set.contains_small_allocation_id());
  }
}

TEST(ObjectIdTest, ObjectIdDebugStringFormatBoundaryCondition) {
  EXPECT_THAT(Allocate(0).ObjectByOffset(0).DebugString(),
              MatchesRegex(R"regex([0-9a-f]{32}:0)regex"));

  for (size_t i = 1; i <= 10; i++) {
    AllocationId alloc_id = Allocate(1ull << (i * 4));
    EXPECT_THAT(alloc_id.ObjectByOffset((1ull << (i * 4)) - 1).DebugString(),
                MatchesRegex(absl::StrFormat(
                    R"regex([0-9a-f]{%d}:[f]{%d})regex", (32 - i), i)));
  }
}

TEST(ObjectIdTest, ObjectIdStringFormat) {
  EXPECT_THAT(ObjectIdStr(CreateUuidObject(
                  arolla::FingerprintHasher("").Combine(57).Finish())),
              MatchesRegex(R"regex(k[0-9a-f]{32}:0)regex"));
  EXPECT_THAT(ObjectIdStr(AllocateSingleObject()),
              MatchesRegex(R"regex(\$[0-9a-f]{32}:0)regex"));
}

}  // namespace
}  // namespace koladata::internal
