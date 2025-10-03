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
#include "koladata/functor/parallel/executor.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/context_guard.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

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

class TestExecutor : public Executor {
 public:
  TestExecutor() noexcept = default;

  explicit TestExecutor(
      absl::AnyInvocable<void(ContextGuard&) const>
          cross_task_scope_guard_creator) noexcept
      : Executor(std::move(cross_task_scope_guard_creator)) {}

  void DoSchedule(TaskFn task_fn) noexcept final { std::move(task_fn)(); }

  std::string Repr() const noexcept final { return "test_executor"; }
};

TEST(ExecutorTest, Nullptr) {
  ExecutorPtr executor = nullptr;
  EXPECT_EQ(arolla::Repr(executor), "executor{nullptr}");
  EXPECT_EQ(arolla::TypedValue::FromValue(executor).GetFingerprint(),
            arolla::TypedValue::FromValue(executor).GetFingerprint());
}

TEST(ExecutorTest, Repr) {
  ExecutorPtr executor = std::make_shared<TestExecutor>();
  EXPECT_EQ(arolla::Repr(executor), "test_executor");
}

TEST(ExecutorTest, Fingerprint) {
  ExecutorPtr executor1 = std::make_shared<TestExecutor>();
  ExecutorPtr executor2 = std::make_shared<TestExecutor>();
  EXPECT_NE(arolla::TypedValue::FromValue(executor1).GetFingerprint(),
            arolla::TypedValue::FromValue(executor2).GetFingerprint());
}

TEST(ExecutorTest, Current) {
  ExecutorPtr executor1 = std::make_shared<TestExecutor>();
  ExecutorPtr executor2 = std::make_shared<TestExecutor>();
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), nullptr);
  bool called = false;
  executor1->Schedule([&] {
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor1));
    EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor1.get());
    executor2->Schedule([&] {
      EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor2));
      EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor2.get());
      called = true;
    });
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor1));
    EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor1.get());
  });
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), nullptr);
  EXPECT_TRUE(called);
}

TEST(ExecutorTest, CurrentExecutorScopeGuard) {
  ExecutorPtr executor1 = std::make_shared<TestExecutor>();
  ExecutorPtr executor2 = std::make_shared<TestExecutor>();
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), nullptr);
  {
    CurrentExecutorScopeGuard guard(executor1);
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor1));
    EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor1.get());
    {
      CurrentExecutorScopeGuard guard(executor2);
      EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor2));
      EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor2.get());
    }
    EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor1));
    EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), executor1.get());
  }
  EXPECT_THAT(CurrentExecutor(), StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(), nullptr);
}

TEST(ExecutorTest, CrossTaskContextGuard) {
  testing::MockFunction<void()> mock_construct;
  testing::MockFunction<void()> mock_destruct;
  ExecutorPtr executor = std::make_shared<TestExecutor>(
      [&mock_construct, &mock_destruct](ContextGuard& context_guard) {
        context_guard.init<TestScopeGuard>(mock_construct.AsStdFunction(),
                                           mock_destruct.AsStdFunction());
      });
  testing::MockFunction<void()> mock_task;
  {
    testing::InSequence seq;
    // First task.
    EXPECT_CALL(mock_construct, Call());
    EXPECT_CALL(mock_task, Call());
    EXPECT_CALL(mock_destruct, Call());

    // Second task.
    EXPECT_CALL(mock_construct, Call());
    EXPECT_CALL(mock_task, Call());
    EXPECT_CALL(mock_destruct, Call());
  }
  executor->Schedule(mock_task.AsStdFunction());
  executor->Schedule(mock_task.AsStdFunction());
}

TEST(ExecutorTest, CrossTaskContextGuard_CurrentExecutorScopeGuard) {
  ExecutorPtr executor =
      std::make_shared<TestExecutor>([&](ContextGuard& context_guard) {
        context_guard.init<TestScopeGuard>(
            [&] { EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor)); },
            [&] { EXPECT_THAT(CurrentExecutor(), IsOkAndHolds(executor)); });
      });
  executor->Schedule([] {});
}

TEST(ExecutorTest, TaskCopying) {
  ExecutorPtr executor = std::make_shared<TestExecutor>();
  std::function<int()> fn = [x = 0]() mutable { return x++; };
  {
    const auto gn = fn;
    executor->Schedule(gn);
    ASSERT_EQ(gn(), 0);
  }
  {
    auto gn = fn;
    executor->Schedule(gn);
    ASSERT_EQ(gn(), 0);
  }
  {
    auto gn = fn;
    executor->Schedule<std::function<int()>&>(gn);
    ASSERT_EQ(gn(), 0);
  }
  {
    auto gn = fn;
    executor->Schedule(std::ref(gn));
    ASSERT_EQ(gn(), 1);
  }
}

TEST(ExecutorTest, IsExecutorTask) {
  ExecutorPtr executor = std::make_shared<TestExecutor>();
  ASSERT_FALSE(IsExecutorTask());
  bool called = false;
  executor->Schedule([&] {
    ASSERT_TRUE(IsExecutorTask());
    called = true;
  });
  ASSERT_FALSE(IsExecutorTask());
  EXPECT_TRUE(called);
}

}  // namespace
}  // namespace koladata::functor::parallel
