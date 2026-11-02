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
#include "koladata/functor/parallel/derived_executor.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/functor/parallel/context_guard.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

class TestExecutor : public Executor {
 public:
  using Executor::Executor;

  void DoSchedule(TaskFn task_fn) noexcept override { std::move(task_fn)(); }

  std::string Repr() const noexcept override { return "test_executor"; }
};

class TestScopeGuard {
 public:
  TestScopeGuard(std::function<void()> on_construct_fn,
                 std::function<void()> on_destruct_fn)
      : on_construct_fn_(std::move(on_construct_fn)),
        on_destruct_fn_(std::move(on_destruct_fn)) {
    on_construct_fn_();
  }

  ~TestScopeGuard() noexcept { on_destruct_fn_(); }

 private:
  std::function<void()> on_construct_fn_;
  std::function<void()> on_destruct_fn_;
};

TEST(DerivedExecutorTest, Repr) {
  auto base_executor = std::make_shared<TestExecutor>();
  auto derived_executor =
      std::make_shared<DerivedExecutor>(base_executor, nullptr);
  EXPECT_EQ(derived_executor->Repr(), "derived_executor[test_executor, 1]");

  auto derived_executor_2 =
      std::make_shared<DerivedExecutor>(derived_executor, nullptr);
  EXPECT_EQ(derived_executor_2->Repr(), "derived_executor[test_executor, 2]");

  auto derived_executor_3 =
      std::make_shared<DerivedExecutor>(derived_executor_2, nullptr);
  EXPECT_EQ(derived_executor_3->Repr(), "derived_executor[test_executor, 3]");
}

TEST(DerivedExecutorTest, ScheduleCallsBaseExecutor) {
  auto base_executor = std::make_shared<TestExecutor>();
  DerivedExecutorPtr derived_executor =
      std::make_shared<DerivedExecutor>(base_executor, nullptr);

  bool task_executed = false;
  derived_executor->Schedule([&]() mutable { task_executed = true; });
  EXPECT_TRUE(task_executed);
}

TEST(DerivedExecutorTest, CurrentExecutor) {
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  auto base_executor = std::make_shared<TestExecutor>();
  auto derived_executor =
      std::make_shared<DerivedExecutor>(base_executor, nullptr);
  bool task_executed = false;
  derived_executor->Schedule([&] {
    task_executed = true;
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(derived_executor));
  });
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_TRUE(task_executed);
}

TEST(DerivedExecutorTest, ExtraContextGuard) {
  std::vector<std::string> log;
  auto base_guard_initializer = [&](ContextGuard& context_guard) {
    context_guard.init<TestScopeGuard>(
        [&] { log.push_back("enter_base_scope"); },
        [&] { log.push_back("leave_base_scope"); });
  };
  auto derived_guard_initializer = [&](ContextGuard& context_guard) {
    context_guard.init<TestScopeGuard>(
        [&] { log.push_back("enter_derived_scope"); },
        [&] { log.push_back("leave_derived_scope"); });
  };
  auto base_executor =
      std::make_shared<TestExecutor>(std::move(base_guard_initializer));
  auto derived_executor = std::make_shared<DerivedExecutor>(
      base_executor, std::move(derived_guard_initializer));
  derived_executor->Schedule([&] { log.push_back("run_task_1"); });
  derived_executor->Schedule([&] { log.push_back("run_task_2"); });
  EXPECT_THAT(log, testing::ElementsAre(
                       "enter_base_scope", "enter_derived_scope", "run_task_1",
                       "leave_derived_scope", "leave_base_scope",
                       "enter_base_scope", "enter_derived_scope", "run_task_2",
                       "leave_derived_scope", "leave_base_scope"));
}

TEST(DerivedExecutorTest, CaptureDestroyedWithCurrentExecutor) {
  auto base_executor = std::make_shared<TestExecutor>();
  bool in_derived_scope = false;
  auto derived_executor = std::make_shared<DerivedExecutor>(
      base_executor, [&](ContextGuard& context_guard) {
        context_guard.init<TestScopeGuard>([&] { in_derived_scope = true; },
                                           [&] { in_derived_scope = false; });
      });
  Executor* executor_on_destruct = nullptr;
  bool is_task_on_destruct = false;
  bool in_derived_scope_on_destruct = false;
  derived_executor->Schedule(
      [guard = absl::Cleanup([&] {
         executor_on_destruct = CurrentExecutorScopeGuard::current_executor();
         is_task_on_destruct = IsExecutorTask();
         in_derived_scope_on_destruct = in_derived_scope;
       })] {});
  EXPECT_EQ(executor_on_destruct, derived_executor.get());
  EXPECT_TRUE(is_task_on_destruct);
  EXPECT_TRUE(in_derived_scope_on_destruct);
  EXPECT_FALSE(in_derived_scope);
}

}  // namespace
}  // namespace koladata::functor::parallel
