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
#include "koladata/functor/parallel/asio_executor.h"

#include <cstddef>
#include <memory>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/barrier.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "koladata/functor/parallel/executor.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

TEST(AsioExecutorTest, Basic) {
  constexpr int kNumThreads = 10;
  const auto executor = MakeAsioExecutor(kNumThreads);
  ASSERT_NE(executor, nullptr);

  auto barrier = std::make_shared<absl::Barrier>(kNumThreads + 1);
  for (int i = 0; i < kNumThreads; ++i) {
    ASSERT_OK(executor->Schedule([barrier] { barrier->Block(); }));
  }
  barrier->Block();
}

TEST(AsioExecutorTest, NumberOfThreads) {
  constexpr int kNumThreads = 5;
  constexpr int kNumTasks = 50;
  const auto executor = MakeAsioExecutor(kNumThreads);

  struct SharedState {
    absl::Mutex mutex;
    absl::flat_hash_set<std::thread::id> thread_ids ABSL_GUARDED_BY(mutex);
    std::vector<absl::Notification> notifications{kNumTasks};
  };
  auto shared_state = std::make_shared<SharedState>();
  for (auto& notification : shared_state->notifications) {
    ASSERT_OK(executor->Schedule([&notification, shared_state] {
      absl::SleepFor(absl::Milliseconds(1));
      absl::MutexLock lock(&shared_state->mutex);
      shared_state->thread_ids.insert(std::this_thread::get_id());
      notification.Notify();
    }));
  }
  for (auto& notification : shared_state->notifications) {
    notification.WaitForNotification();
  }
  absl::MutexLock lock(&shared_state->mutex);
  EXPECT_EQ(shared_state->thread_ids.size(), kNumThreads);
}

TEST(AsioExecutorTest, NonBlockingDestructor) {
  auto executor = MakeAsioExecutor();
  auto barrier = std::make_shared<absl::Barrier>(2);
  ASSERT_OK(executor->Schedule([barrier] {
    barrier->Block();
    absl::SleepFor(absl::Milliseconds(100));
  }));
  barrier->Block();
  const auto start = absl::Now();
  executor.reset();
  const auto stop = absl::Now();
  EXPECT_LE(stop - start, absl::Milliseconds(95));
}

}  // namespace
}  // namespace koladata::functor::parallel
