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
#include "koladata/internal/random.h"

#include <cstdint>
#include <thread>  // NOLINT(build/c++11): only used for tests.
#include <vector>

#include "gtest/gtest.h"

namespace koladata::internal {

uint64_t DeterministicRandomUint64();

namespace {

TEST(RandomTest, MaybeDeterministicRandomUint64) {
  EXPECT_NE(MaybeDeterministicRandomUint64(), MaybeDeterministicRandomUint64());
}

TEST(RandomTest, MaybeDeterministicRandomFingerprint) {
  EXPECT_NE(MaybeDeterministicRandomFingerprint(),
            MaybeDeterministicRandomFingerprint());
}

TEST(RandomTest, MaybeDeterministicRandomUint64_MultiThreaded) {
  constexpr int kNumThreads = 10;
  constexpr int kNumValues = 10;
  std::vector<std::vector<uint64_t>> results(kNumThreads);

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&results, i] {
      results[i].reserve(kNumValues);
      for (int j = 0; j < kNumValues; ++j) {
        results[i].push_back(MaybeDeterministicRandomUint64());
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

TEST(RandomTest, DeterministicRandomUint64_MultiThreaded) {
  constexpr int kNumThreads = 10;
  constexpr int kNumValues = 10;
  std::vector<std::vector<uint64_t>> results(kNumThreads);

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&results, i] {
      results[i].reserve(kNumValues);
      for (int j = 0; j < kNumValues; ++j) {
        results[i].push_back(DeterministicRandomUint64());
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

}  // namespace
}  // namespace koladata::internal
