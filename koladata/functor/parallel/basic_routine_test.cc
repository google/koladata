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

#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/get_default_executor.h"
#include "koladata/functor/parallel/make_executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::StatusIs;
using ::testing::_;
using ::testing::InSequence;
using ::testing::MockFunction;
using ::testing::Pointer;
using ::testing::Return;

class MockBasicRoutineHooks : public BasicRoutineHooks {
 public:
  // Note: We're not using the gMock cookbook's destructor mocking approach
  // (https://google.github.io/googletest/gmock_cook_book.html#mocking-destructors)
  // because it has led to false-positive mock object leak detections in our
  // tests. (NOLINTNEXTLINE: public member is used for testing only)
  const std::shared_ptr<absl::Notification> destroyed =
      std::make_shared<absl::Notification>();

  ~MockBasicRoutineHooks() override { destroyed->Notify(); }

  MOCK_METHOD(bool, Interrupted, (), (const, override));
  MOCK_METHOD(void, OnCancel, (absl::Status && status), (override));
  MOCK_METHOD(StreamReaderPtr, Start, (), (override));
  MOCK_METHOD(StreamReaderPtr, Resume, (StreamReaderPtr stream), (override));
};

TEST(BasicRoutineTest, Basic) {
  auto [stream1, writer1] = MakeStream(arolla::GetQType<int>());
  auto reader1 = stream1->MakeReader();
  auto* reader1_raw_ptr = reader1.get();
  auto [stream2, writer2] = MakeStream(arolla::GetQType<int>());
  auto reader2 = stream2->MakeReader();
  auto* reader2_raw_ptr = reader2.get();

  MockFunction<void(std::string check_point_name)> check;
  auto routine_hooks = std::make_unique<MockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, OnCancel(_)).Times(0);
  {
    InSequence s;
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // First iteration.
    EXPECT_CALL(*routine_hooks, Start()).WillOnce(Return(std::move(reader1)));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // Second iteration.
    EXPECT_CALL(check, Call("stream1_ready"));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    EXPECT_CALL(*routine_hooks, Resume(Pointer(reader1_raw_ptr)))
        .WillOnce(Return(std::move(reader2)));
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // Third iteration.
    EXPECT_CALL(check, Call("stream2_ready"));
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

  auto routine_hooks = std::make_unique<MockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  // Note: There is a potential timing issue when the basic routine
  testing::Mock::AllowLeak(routine_hooks.get());
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")))
      .Times(1);
  {
    InSequence s;
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // First iteration.
    EXPECT_CALL(*routine_hooks, Start()).WillOnce([&] {
      EXPECT_EQ(arolla::CurrentCancellationContext().get(),
                cancellation_scope.cancellation_context());
      return stream->MakeReader();
    });
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
    // Second iteration.
    EXPECT_CALL(*routine_hooks, Interrupted()).WillOnce(Return(false));
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

  auto routine_hooks = std::make_unique<MockBasicRoutineHooks>();
  auto destroyed = routine_hooks->destroyed;
  EXPECT_CALL(*routine_hooks, Start()).Times(0);
  EXPECT_CALL(*routine_hooks,
              OnCancel(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")))
      .Times(1);

  StartBasicRoutine(GetDefaultExecutor(), std::move(routine_hooks));
  EXPECT_TRUE(destroyed->WaitForNotificationWithTimeout(absl::Seconds(1)));
}

}  // namespace
}  // namespace koladata::functor::parallel
