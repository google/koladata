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
#include "koladata/lazy_uuid.h"

#include <optional>
#include <thread>  // NOLINT(build/c++11): only used for tests.
#include <vector>

#include "gtest/gtest.h"
#include "absl/synchronization/barrier.h"
#include "arolla/util/fingerprint.h"
#include "koladata/internal/pseudo_random.h"

namespace koladata {
namespace {

TEST(LazyUuidTest, DefaultConstruction) {
  LazyUuid uuid1;
  LazyUuid uuid2;
  // The identifier is generated upon the first access, and then stays
  // the same.
  const arolla::Fingerprint value1 = uuid1.get();
  EXPECT_EQ(uuid1.get(), value1);
  EXPECT_NE(uuid2.get(), value1);
}

TEST(LazyUuidTest, ConstructionFromNullopt) {
  LazyUuid uuid1(std::nullopt);
  LazyUuid uuid2(std::nullopt);
  const arolla::Fingerprint value1 = uuid1.get();
  EXPECT_EQ(uuid1.get(), value1);
  EXPECT_NE(uuid2.get(), value1);
}

TEST(LazyUuidTest, ConstructionFromValue) {
  const arolla::Fingerprint value = internal::PseudoRandomFingerprint();
  LazyUuid uuid(value);
  EXPECT_EQ(uuid.get(), value);
  EXPECT_EQ(uuid.get(), value);
}

TEST(LazyUuidTest, MultiThreaded) {
  constexpr int kNumThreads = 10;
  LazyUuid uuid;
  absl::Barrier barriers(kNumThreads);
  std::vector<std::thread> threads;
  std::vector<arolla::Fingerprint> results(kNumThreads);
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      barriers.Block();
      results[i] = uuid.get();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& result : results) {
    EXPECT_EQ(result, uuid.get());
  }
}

}  // namespace
}  // namespace koladata
