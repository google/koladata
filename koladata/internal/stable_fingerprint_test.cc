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
#include "koladata/internal/stable_fingerprint.h"

#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"

namespace koladata::internal {
namespace {

using ::arolla::Fingerprint;

template <typename... Ts>
Fingerprint MakeDummyFingerprint(const Ts&... values) {
  return StableFingerprintHasher("dummy-salt").Combine(values...).Finish();
}

TEST(FingerprintTest, Empty) {
  EXPECT_EQ(Fingerprint().AsString(), "00000000000000000000000000000000");
}

TEST(FingerprintTest, TestPrimitives) {
  EXPECT_NE(MakeDummyFingerprint(5), MakeDummyFingerprint(6));
  EXPECT_NE(MakeDummyFingerprint<std::string>("5"),
            MakeDummyFingerprint<std::string>("6"));
}

TEST(FingerprintTest, FloatingPointZero) {
  EXPECT_NE(MakeDummyFingerprint(0.0).PythonHash(),
            MakeDummyFingerprint(-0.0).PythonHash());
  EXPECT_NE(MakeDummyFingerprint(0.f).PythonHash(),
            MakeDummyFingerprint(-0.f).PythonHash());
}

TEST(FingerprintTest, FloatingPointNAN) {
  EXPECT_NE(MakeDummyFingerprint(std::numeric_limits<float>::quiet_NaN())
                .PythonHash(),
            MakeDummyFingerprint(-std::numeric_limits<float>::quiet_NaN())
                .PythonHash());
  EXPECT_NE(MakeDummyFingerprint(std::numeric_limits<double>::quiet_NaN())
                .PythonHash(),
            MakeDummyFingerprint(-std::numeric_limits<double>::quiet_NaN())
                .PythonHash());
}

TEST(FingerprintTest, CombineRawBytes) {
  {
    StableFingerprintHasher h1("dummy-salt");
    StableFingerprintHasher h2("dummy-salt");
    h1.CombineRawBytes("foobar", 6);
    h2.CombineRawBytes("foobar", 6);
    EXPECT_EQ(std::move(h1).Finish(), std::move(h2).Finish());
  }
  {
    StableFingerprintHasher h1("dummy-salt");
    StableFingerprintHasher h2("dummy-salt");
    h1.CombineRawBytes("foobar", 6);
    h2.CombineRawBytes("barfoo", 6);
    EXPECT_NE(std::move(h1).Finish(), std::move(h2).Finish());
  }
}

TEST(FingerprintTest, AllTypes) {
  absl::flat_hash_set<arolla::Fingerprint> fps;
  arolla::meta::foreach_type(supported_types_list(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    StableFingerprintHasher hasher("dummy-salt");
    if constexpr (std::is_same_v<T, ObjectId>) {
      ASSERT_TRUE(
          fps.insert(std::move(hasher).Combine(AllocateSingleObject()).Finish())
              .second);
    } else {
      ASSERT_TRUE(fps.insert(std::move(hasher).Combine(T()).Finish()).second)
          << typeid(T).name();
    }
  });
  {
    StableFingerprintHasher hasher("dummy-salt");
    ASSERT_TRUE(fps.insert(std::move(hasher)
                               .Combine(arolla::Fingerprint{.value = 0})
                               .Finish())
                    .second);
  }
}

TEST(FingerprintTest, StableFingerprintMethod) {
  struct Abc {
    int value = 0;
    void StableFingerprint(StableFingerprintHasher* hasher) const {
      hasher->Combine(value);
    }
  };
  EXPECT_EQ(StableFingerprintHasher("salt").Combine(Abc{12}).Finish(),
            StableFingerprintHasher("salt").Combine(Abc{12}).Finish());
  EXPECT_NE(StableFingerprintHasher("salt").Combine(Abc{12}).Finish(),
            StableFingerprintHasher("salt").Combine(Abc{15}).Finish());
}

}  // namespace
}  // namespace koladata::internal
