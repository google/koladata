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
#include "koladata/functor/parallel/asio_executor.h"

#include <atomic>
#include <memory>

#include "gtest/gtest.h"
#include "absl/synchronization/barrier.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/get_default_executor.h"

namespace koladata::functor::parallel {
namespace {

TEST(AsioExecutorTest, Basic) {
  constexpr int kNumThreads = 10;
  const auto executor = MakeAsioExecutor(kNumThreads);
  ASSERT_NE(executor, nullptr);

  auto barrier = std::make_shared<absl::Barrier>(kNumThreads + 1);
  for (int i = 0; i < kNumThreads; ++i) {
    executor->Schedule([barrier] { barrier->Block(); });
  }
  barrier->Block();
}

TEST(AsioExecutorTest, NumberOfThreadsUpperBound) {
  constexpr int kNumThreads = 5;
  constexpr int kNumTasks = kNumThreads + 1;
  const auto executor = MakeAsioExecutor(kNumThreads);

  struct SharedState {
    std::atomic<int> started_task_counter = 0;
    absl::Notification notification;
  };
  auto shared_state = std::make_shared<SharedState>();
  for (int i = 0; i < kNumTasks; ++i) {
    executor->Schedule([shared_state] {
      ++shared_state->started_task_counter;
      shared_state->notification.WaitForNotification();
    });
  }
  absl::SleepFor(absl::Milliseconds(20));
  ASSERT_LE(shared_state->started_task_counter, kNumTasks);
  shared_state->notification.Notify();  // Allow the tasks to finish.
}

TEST(AsioExecutorTest, NonBlockingDestructor) {
  GetDefaultExecutor()->Schedule([] {});  // Spin up the default executor.
  auto executor = MakeAsioExecutor();
  auto barrier = std::make_shared<absl::Barrier>(2);
  executor->Schedule([barrier] {
    barrier->Block();
    absl::SleepFor(absl::Milliseconds(100));
  });
  barrier->Block();
  const auto start = absl::Now();
  executor.reset();
  const auto stop = absl::Now();
  EXPECT_LE(stop - start, absl::Milliseconds(50));
}

}  // namespace
}  // namespace koladata::functor::parallel
