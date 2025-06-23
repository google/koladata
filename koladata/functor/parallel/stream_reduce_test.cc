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
#include "koladata/functor/parallel/stream_reduce.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/get_default_executor.h"
#include "koladata/functor/parallel/make_executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::Pointee;

absl::Status NeverFn(arolla::TypedRef /*acc*/, arolla::TypedRef /*item*/) {
  ADD_FAILURE() << "The functor was unexpectedly called.";
  return absl::FailedPreconditionError("Must never happen!");
}

TEST(StreamReduceTest, Basic) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(GetEagerExecutor(),
                        arolla::TypedValue::FromValue(34.0f), std::move(stream),
                        [](arolla::TypedRef acc, arolla::TypedRef item) {
                          return arolla::TypedValue::FromValue(
                              acc.UnsafeAs<float>() / 2 + item.UnsafeAs<int>());
                        });
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
  writer->Write(arolla::TypedRef::FromValue(17));
  writer->Write(arolla::TypedRef::FromValue(20));
  writer->Write(arolla::TypedRef::FromValue(17));
  writer->Write(arolla::TypedRef::FromValue(10));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_EQ(reader->TryRead().item()->UnsafeAs<float>(),
            (((34 * 0.5 + 17) * 0.5 + 20) * 0.5 + 17) * 0.5 + 10);
  EXPECT_OK(*reader->TryRead().close_status());
}

TEST(StreamReduceTest, InputStreamError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(GetEagerExecutor(), arolla::TypedValue::FromValue(0.0f),
                        std::move(stream),
                        [](arolla::TypedRef acc, arolla::TypedRef item) {
                          return arolla::TypedValue::FromValue(
                              acc.UnsafeAs<float>() / 2 + item.UnsafeAs<int>());
                        });
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamReduceTest, FunctorError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(GetEagerExecutor(), arolla::TypedValue::FromValue(0.0f),
                        std::move(stream),
                        [](arolla::TypedRef acc, arolla::TypedRef item) {
                          return absl::InvalidArgumentError("Boom!");
                        });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamReduceTest, FunctorWrongReturnType) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(
      GetEagerExecutor(), arolla::TypedValue::FromValue(0.0f),
      std::move(stream), [](arolla::TypedRef acc, arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(0.5 * acc.UnsafeAs<float>() +
                                             item.UnsafeAs<int>());
      });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(*reader->TryRead().close_status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "functor returned a value of the wrong type: expected "
                       "FLOAT32, got FLOAT64"));
}

TEST(StreamReduceTest, OrphanedOutputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(GetEagerExecutor(), arolla::TypedValue::FromValue(0.0f),
                        std::move(stream), NeverFn);
  stream.reset();
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
}

TEST(StreamReduceTest, FunctorCancellationContextPropagation) {
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  int count_down = 1;
  stream = StreamReduce(
      executor, arolla::TypedValue::FromValue(0), std::move(stream),
      [&count_down](arolla::TypedRef acc, arolla::TypedRef item) {
        auto cancellation_context = arolla::CurrentCancellationContext();
        EXPECT_NE(cancellation_context, nullptr);
        if (cancellation_context != nullptr && count_down-- == 0) {
          cancellation_context->Cancel(absl::CancelledError("Boom!"));
        }
        return arolla::TypedValue::FromValue(acc.UnsafeAs<int>() +
                                             item.UnsafeAs<int>());
      });
  auto reader = stream->MakeReader();
  auto notification = std::make_shared<absl::Notification>();
  reader->SubscribeOnce([notification] { notification->Notify(); });
  notification->WaitForNotification();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

TEST(StreamReduceTest, DoNotStartWhenCancelled) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  stream = StreamReduce(GetEagerExecutor(), arolla::TypedValue::FromValue(0),
                        std::move(stream), NeverFn);
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

TEST(StreamReduceTest, Stress) {
  constexpr int kItemCount = 1000000;
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamReduce(
      GetDefaultExecutor(), arolla::TypedValue::FromValue(int64_t{0}),
      std::move(stream), [](arolla::TypedRef acc, arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(acc.UnsafeAs<int64_t>() +
                                             item.UnsafeAs<int>());
      });
  for (int i = 0; i < kItemCount; ++i) {
    writer->Write(arolla::TypedRef::FromValue(i));
  }
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int64_t>(),
              static_cast<int64_t>(kItemCount) * (kItemCount - 1) / 2);
  }
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
  }
}

TEST(StreamReduceTest, ExecutorShutdown) {
  struct DummyExecutor final : Executor {
    void Schedule(TaskFn /*task_fn*/) final {}
    std::string Repr() const final { return "dummy_executor"; }
  };
  auto dummy_executor = std::make_shared<DummyExecutor>();
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream =
      StreamReduce(dummy_executor, arolla::TypedValue::FromValue(int64_t{0}),
                   std::move(stream), NeverFn);
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "orphaned")));
}

}  // namespace
}  // namespace koladata::functor::parallel
