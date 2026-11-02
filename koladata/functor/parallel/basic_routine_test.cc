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
#include "koladata/functor/parallel/basic_routine.h"

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/get_default_executor.h"
#include "koladata/functor/parallel/make_executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::StatusIs;
using ::testing::_;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::NiceMock;
using ::testing::Pointer;
using ::testing::Return;

class BasicRoutineHooksWithDestructionNotification : public BasicRoutineHooks {
 public:
  // Note: We're not using the gMock cookbook's destructor mocking approach
  // (https://google.github.io/googletest/gmock_cook_book.html#mocking-destructors)
  // because it has led to false-positive mock object leak detections in our
  // tests.
  //
  // This notification logic is implemented within a base class, ensuring it's
  // only triggered after all mock-related machinery has been deregistered.
  //
  // (NOLINTNEXTLINE: public member is used for testing only)
  const std::shared_ptr<absl::Notification> destroyed =
      std::make_shared<absl::Notification>();

  ~BasicRoutineHooksWithDestructionNotification() override {
    destroyed->Notify();
  }
};

class MockBasicRoutineHooks
    : public BasicRoutineHooksWithDestructionNotification {
 public:
  MockBasicRoutineHooks() {
    ON_CALL(*this, Interrupted).WillByDefault([this] {
      return interrupted_.load();
    });
    ON_CALL(*this, OnCancel).WillByDefault([this](absl::Status&&) {
      interrupted_.store(true);
    });
  }

  MOCK_METHOD(bool, Interrupted, (), (const, override));
  MOCK_METHOD(void, OnCancel, (absl::Status && status), (override));
  MOCK_METHOD(StreamReaderPtr, Start, (), (override));
  MOCK_METHOD(StreamReaderPtr, Resume, (StreamReaderPtr stream), (override));

 private:
  std::atomic<bool> interrupted_ = false;
};

using NiceMockBasicRoutineHooks = NiceMock<MockBasicRoutineHooks>;

TEST(BasicRoutineTest, Basic) {
  auto [stream1, writer1] = MakeStream(arolla::GetQType<int>());
  auto reader1 = stream1->MakeReader();
  auto* reader1_raw_ptr = reader1.get();
  auto [stream2, writer2] = MakeStream(arolla::GetQType<int>());
  auto reader2 = stream2->MakeReader();
  auto* reader2_raw_ptr = reader2.get();

  MockFunction<void(std::string check_point_name)> check;
  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, OnCancel(_)).Times(0);
  {
    InSequence s;
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // First iteration.
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Start()).WillOnce(Return(std::move(reader1)));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // Second iteration.
    EXPECT_CALL(check, Call("stream1_ready"));
    EXPECT_CALL(*routine_hooks, Interrupted())  // The subscription callback.
        .WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Resume(Pointer(reader1_raw_ptr)))
        .WillOnce(Return(std::move(reader2)));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // Third iteration.
    EXPECT_CALL(check, Call("stream2_ready"));
    EXPECT_CALL(*routine_hooks, Interrupted())  // The subscription callback.
        .WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Resume(Pointer(reader2_raw_ptr)))
        .WillOnce(Return(nullptr));
  }
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  check.Call("stream1_ready");
  std::move(*writer1).Close();
  check.Call("stream2_ready");
  std::move(*writer2).Close();
  EXPECT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST(BasicRoutineTest, CancellationContextPropagation) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  std::move(*writer).Close();

  arolla::CancellationContext::ScopeGuard cancellation_scope;
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  {
    InSequence s;
    EXPECT_CALL(*routine_hooks, Start()).WillOnce([&] {
      EXPECT_EQ(arolla::CurrentCancellationContext().get(),
                cancellation_scope.cancellation_context());
      return std::exchange(stream, nullptr)->MakeReader();
    });
    // The stream is already closed, so the subscription callback fires
    // immediately and schedules `Resume()`.
    EXPECT_CALL(*routine_hooks, Resume(_)).WillOnce([&](auto reader) {
      EXPECT_EQ(arolla::CurrentCancellationContext().get(),
                cancellation_scope.cancellation_context());
      cancellation_scope.cancellation_context()->Cancel(
          absl::InvalidArgumentError("Boom!"));
      return reader;
    });
  }
  StartBasicRoutine(executor, std::move(routine_hooks));
  EXPECT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST(BasicRoutineTest, InitiallyCancelled) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  cancellation_scope.cancellation_context()->Cancel(
      absl::InvalidArgumentError("Boom!"));

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, Start()).Times(0);
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));

  StartBasicRoutine(GetDefaultExecutor(), std::move(routine_hooks));
  EXPECT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST(BasicRoutineTest, CancelWhileWaitingOnIdleStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, Start())
      .WillOnce(Return(std::exchange(stream, nullptr)->MakeReader()));
  EXPECT_CALL(*routine_hooks, Resume(_)).Times(0);
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  // The routine is waiting on the idle stream.
  ASSERT_FALSE(destroyed->HasBeenNotified());
  ASSERT_FALSE(writer->Orphaned());

  cancellation_scope.cancellation_context()->Cancel(
      absl::InvalidArgumentError("Boom!"));
  EXPECT_TRUE(destroyed->HasBeenNotified());
  EXPECT_TRUE(writer->Orphaned());
}

TEST(BasicRoutineTest, CancelBeforeWaiting) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  EXPECT_CALL(*routine_hooks, Resume(_)).Times(0);
  {
    InSequence s;
    EXPECT_CALL(*routine_hooks, Interrupted())
        .Times(2)
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*routine_hooks, Start())
        .WillOnce(Return(std::exchange(stream, nullptr)->MakeReader()));
    // Cancel after the routine has started waiting (i.e., after it has set
    // `waiting_for_reader`), but before it subscribes to the reader; this check
    // still reports no interruption.
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce([&] {
      cancellation_scope.cancellation_context()->Cancel(
          absl::InvalidArgumentError("Boom!"));
      return false;
    });
    // `OnCancel()` sees the interruption.
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(true));
  }
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  EXPECT_TRUE(destroyed->HasBeenNotified());
  EXPECT_TRUE(writer->Orphaned());
}

TEST(BasicRoutineTest, CancelBeforeParking) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  EXPECT_CALL(*routine_hooks, Resume(_)).Times(0);
  // Cancel while `Start()` is running, i.e., before the routine starts waiting
  // on the returned reader; the cancellation has nothing to release yet.
  EXPECT_CALL(*routine_hooks, Start()).WillOnce([&] {
    cancellation_scope.cancellation_context()->Cancel(
        absl::InvalidArgumentError("Boom!"));
    return std::exchange(stream, nullptr)->MakeReader();
  });
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  EXPECT_TRUE(destroyed->HasBeenNotified());
  EXPECT_TRUE(writer->Orphaned());
}

TEST(BasicRoutineTest, WakeupBeforeCancellationCallback) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto* cancellation_context = cancellation_scope.cancellation_context();

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, Start())
      .WillOnce(Return(std::exchange(stream, nullptr)->MakeReader()));
  // The routine is resumed (it's up to the hooks to report the interruption),
  // and then waits on the stream again.
  EXPECT_CALL(*routine_hooks, Resume(_)).WillOnce([](StreamReaderPtr reader) {
    while (reader->TryRead().item() != nullptr) {
    }
    return reader;
  });
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  ASSERT_FALSE(destroyed->HasBeenNotified());

  // Note: The cancellation callbacks are invoked in the reverse order of
  // subscription, so this one runs before the routine's callback, and wakes
  // the routine after the context is marked as cancelled.
  auto subscription = cancellation_context->Subscribe(
      [&writer] { writer->Write(arolla::TypedRef::FromValue(1)); });
  cancellation_context->Cancel(absl::InvalidArgumentError("Boom!"));
  EXPECT_TRUE(destroyed->HasBeenNotified());
  EXPECT_TRUE(writer->Orphaned());
}

TEST(BasicRoutineTest, InterruptedOnWakeup) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());

  auto routine_hooks = std::make_unique<NiceMockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, Start())
      .WillOnce(Return(std::exchange(stream, nullptr)->MakeReader()));
  EXPECT_CALL(*routine_hooks, Resume(_)).Times(0);
  EXPECT_CALL(*routine_hooks, OnCancel(_)).Times(0);
  {
    InSequence s;
    // Before scheduling, before `Start()`, and after `Start()`.
    EXPECT_CALL(*routine_hooks, Interrupted())
        .Times(3)
        .WillRepeatedly(Return(false));
    // The subscription callback.
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(true));
  }
  StartBasicRoutine(GetEagerExecutor(), std::move(routine_hooks));
  ASSERT_FALSE(destroyed->HasBeenNotified());

  // The wake-up happens outside of the executor, and the routine stops because
  // it's interrupted.
  writer->Write(arolla::TypedRef::FromValue(1));
  EXPECT_TRUE(destroyed->HasBeenNotified());
  EXPECT_TRUE(writer->Orphaned());
}

TEST(BasicRoutineTest, HooksDestroyedOnExecutor) {
  struct Hooks : NiceMockBasicRoutineHooks {
    Executor* expected_executor = nullptr;
    ~Hooks() override {
      EXPECT_EQ(CurrentExecutorScopeGuard::current_executor(),
                expected_executor);
    }
  };
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  arolla::CancellationContext::ScopeGuard cancellation_scope;

  auto executor = MakeExecutor(1);
  auto routine_hooks = std::make_unique<Hooks>();
  routine_hooks->expected_executor = executor.get();
  auto destroyed = routine_hooks->destroyed;
  absl::Notification started;
  EXPECT_CALL(*routine_hooks, Start()).WillOnce([&] {
    started.Notify();
    return std::exchange(stream, nullptr)->MakeReader();
  });
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  StartBasicRoutine(executor, std::move(routine_hooks));
  ASSERT_TRUE(started.WaitForNotificationWithTimeout(absl::Seconds(1)));
  cancellation_scope.cancellation_context()->Cancel(
      absl::InvalidArgumentError("Boom!"));
  EXPECT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(1)));
  EXPECT_TRUE(writer->Orphaned());
}

// Routine hooks that read the stream until it's closed, count the calls to
// `OnCancel`, and report the interruption after the cancellation.
class DrainStreamHooks final
    : public BasicRoutineHooksWithDestructionNotification {
 public:
  DrainStreamHooks(StreamPtr absl_nonnull stream,
                   std::atomic<int>& on_cancel_count)
      : stream_(std::move(stream)), on_cancel_count_(on_cancel_count) {}

  bool Interrupted() const final {
    return on_cancel_count_.load(std::memory_order_relaxed) > 0;
  }

  void OnCancel(absl::Status&& /*status*/) final {
    on_cancel_count_.fetch_add(1, std::memory_order_relaxed);
  }

  StreamReaderPtr absl_nullable Start() final {
    return Resume(std::exchange(stream_, nullptr)->MakeReader());
  }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull reader) final {
    EXPECT_FALSE(active_.exchange(true));
    auto try_read_result = reader->TryRead();
    while (try_read_result.item() != nullptr) {
      try_read_result = reader->TryRead();
    }
    active_.store(false);
    return try_read_result.empty() ? std::move(reader) : nullptr;
  }

 private:
  StreamPtr absl_nullable stream_;
  std::atomic<int>& on_cancel_count_;
  std::atomic<bool> active_ = false;
};

void RunCancellationRacesWithWrites(const ExecutorPtr absl_nonnull& executor) {
  constexpr int kIterations = 100;
  constexpr int kItemCount = 10;
  // Two threads to race the writes against the cancellation.
  const auto race_executor = MakeExecutor(2);
  for (int i = 0; i < kIterations; ++i) {
    SCOPED_TRACE(absl::StrCat("iteration=", i));
    const bool close_stream = (i % 2 == 1);
    auto [stream, writer] = MakeStream(arolla::GetQType<int>());
    auto cancellation_context = arolla::CancellationContext::Make();
    std::atomic<int> on_cancel_count = 0;
    auto routine_hooks =
        std::make_unique<DrainStreamHooks>(std::move(stream), on_cancel_count);
    auto destroyed = routine_hooks->destroyed;
    {
      arolla::CancellationContext::ScopeGuard cancellation_scope(
          cancellation_context);
      StartBasicRoutine(executor, std::move(routine_hooks));
    }
    absl::Notification go;
    absl::BlockingCounter done(2);
    race_executor->Schedule([&] {
      go.WaitForNotification();
      for (int j = 0; j < kItemCount; ++j) {
        writer->Write(arolla::TypedRef::FromValue(j));
      }
      if (close_stream) {
        std::move(*writer).Close();
      }
      done.DecrementCount();
    });
    race_executor->Schedule([&] {
      go.WaitForNotification();
      cancellation_context->Cancel();
      done.DecrementCount();
    });
    go.Notify();
    done.Wait();
    ASSERT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(10)));
    EXPECT_TRUE(writer->Orphaned());
    if (close_stream) {
      // The routine may complete before the cancellation.
      EXPECT_LE(on_cancel_count.load(), 1);
    } else {
      EXPECT_EQ(on_cancel_count.load(), 1);
    }
  }
}

TEST(BasicRoutineTest, CancellationRacesWithWritesEagerExecutor) {
  RunCancellationRacesWithWrites(GetEagerExecutor());
}

TEST(BasicRoutineTest, CancellationRacesWithWritesMultiThreadedExecutor) {
  RunCancellationRacesWithWrites(MakeExecutor(4));
}

}  // namespace
}  // namespace koladata::functor::parallel
