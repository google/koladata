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
#include "koladata/internal/pseudo_random.h"

#include <cstdint>
#include <random>
#include <thread>  // NOLINT(build/c++11): only used for tests.
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;

TEST(PseudoRandomTest, PseudoRandomUint64) {
  EXPECT_NE(PseudoRandomUint64(), PseudoRandomUint64());
}

TEST(PseudoRandomTest, PseudoRandomFingerprint) {
  EXPECT_NE(PseudoRandomFingerprint(), PseudoRandomFingerprint());
}

TEST(PseudoRandomTest, PseudoRandomUint64_MultiThreaded) {
  constexpr int kNumThreads = 10;
  constexpr int kNumValues = 10;
  std::vector<std::vector<uint64_t>> results(kNumThreads);

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&results, i] {
      results[i].reserve(kNumValues);
      for (int j = 0; j < kNumValues; ++j) {
        results[i].push_back(PseudoRandomUint64());
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (int i = 0; i < kNumThreads; ++i) {
    for (int j = i + 1; j < kNumThreads; ++j) {
      EXPECT_NE(results[i], results[j]);
    }
  }
}

TEST(PseudoRandomTest, PseudoRandomEpochIdPtr) {
  auto* ptr = PseudoRandomEpochIdPtr();
  ASSERT_NE(ptr, nullptr);
  EXPECT_NE(*ptr, 0);
  EXPECT_EQ(PseudoRandomEpochIdPtr(), ptr);

  std::vector<std::thread> threads(10);
  for (auto& thread : threads) {
    thread = std::thread([&] { EXPECT_EQ(PseudoRandomEpochIdPtr(), ptr); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(PseudoRandomTest, PseudoRandomEpochId) {
  const uint64_t epoch_id = PseudoRandomEpochId();
  EXPECT_NE(epoch_id, 0);
  EXPECT_EQ(PseudoRandomEpochId(), epoch_id);
  EXPECT_EQ(epoch_id, *PseudoRandomEpochIdPtr());

  std::vector<std::thread> threads(10);
  for (auto& thread : threads) {
    thread = std::thread([&] { EXPECT_EQ(PseudoRandomEpochId(), epoch_id); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(PseudoRandomTest, ReseedPseudoRandomUnimplemented) {
  auto epoch_id = PseudoRandomEpochId();
  EXPECT_THAT(ReseedPseudoRandom(std::seed_seq{0}),
              StatusIs(absl::StatusCode::kUnimplemented));
  EXPECT_EQ(PseudoRandomEpochId(), epoch_id);
}

}  // namespace
}  // namespace koladata::internal
