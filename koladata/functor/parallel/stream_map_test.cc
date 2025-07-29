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
#include "koladata/functor/parallel/stream_map.h"

#include <memory>
#include <set>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
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
using ::arolla::testing::QValueWith;
using ::testing::Pointee;

TEST(StreamMapTest, Basic) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMap(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<double>());
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(5));
  writer->Write(arolla::TypedRef::FromValue(10));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  // Note: The order of resulting items is predictable because we use eager
  // executor.
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(7.5)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(15.0)));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamMapTest, InputStreamError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMap(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamMapTest, FunctorError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMap(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef) { return absl::InvalidArgumentError("Boom!"); });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamMapTest, FunctorWrongReturnType) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMap(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) { return arolla::TypedValue(item); });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(
      reader->TryRead().close_status(),
      Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                       "functor returned a value of the wrong type: expected "
                       "FLOAT64, got INT32")));
}

TEST(StreamMapTest, OrphanedOutputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  int call_count = 0;
  stream = StreamMap(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [&call_count](arolla::TypedRef item) {
        call_count += 1;
        EXPECT_THAT(item, QValueWith<int>(1));
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  stream.reset();
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  // Note: The algorithm only detects that the stream is orphaned after
  // attempting to write an item, and it will call the functor to compute that
  // item first. It then stops and doesn't call the functor for the second item.
  EXPECT_EQ(call_count, 1);
}

TEST(StreamMapTest, FunctorCancellationContextPropagation) {
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  int count_down = 1;
  stream = StreamMap(
      executor, std::move(stream), arolla::GetQType<double>(),
      [&count_down](arolla::TypedRef item) {
        auto cancellation_context = arolla::CurrentCancellationContext();
        EXPECT_NE(cancellation_context, nullptr);
        if (cancellation_context != nullptr && count_down-- == 0) {
          cancellation_context->Cancel(absl::CancelledError("Boom!"));
        }
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  }
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_THAT(reader->TryRead().close_status(),
                Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
  }
}

TEST(StreamMapTest, DoNotStartWhenCancelled) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMap(GetEagerExecutor(), std::move(stream),
                     arolla::GetQType<double>(), [](arolla::TypedRef item) {
                       ADD_FAILURE();
                       return absl::FailedPreconditionError("Never!");
                     });
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
  std::move(*writer).Close();
}

TEST(StreamMapTest, Stress) {
  constexpr int kItemCount = 1024;
  constexpr int kLayerCount = 256;
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  for (int j = 0; j < kLayerCount; ++j) {
    stream = StreamMap(
        GetDefaultExecutor(), std::move(stream), arolla::GetQType<int>(),
        [](arolla::TypedRef item) {
          return arolla::TypedValue::FromValue(1 + item.UnsafeAs<int>());
        });
  }
  for (int i = 0; i < kItemCount; ++i) {
    writer->Write(arolla::TypedRef::FromValue(i));
  }
  std::move(*writer).Close();
  int item_count = 0;
  auto reader = stream->MakeReader();
  for (;;) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      ASSERT_THAT(item, Pointee(QValueWith<int>(kLayerCount + item_count++)));
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      ASSERT_OK(*status);
      break;
    }
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
  }
  ASSERT_EQ(item_count, kItemCount);
}

TEST(StreamMapTest, ExecutorShutdown) {
  struct DummyExecutor final : Executor {
    void DoSchedule(TaskFn /*task_fn*/) final {}
    std::string Repr() const final { return "dummy_executor"; }
  };
  auto dummy_executor = std::make_shared<DummyExecutor>();
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  writer->Write(arolla::TypedRef::FromValue(0));
  std::move(*writer).Close();
  stream = StreamMap(dummy_executor, std::move(stream), arolla::GetQType<int>(),
                     [](arolla::TypedRef item) {
                       ADD_FAILURE();  // Never happens.
                       return absl::FailedPreconditionError("Never!");
                     });
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "orphaned")));
}

TEST(StreamMapUnorderedTest, Basic) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMapUnordered(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<double>());
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(5));
  writer->Write(arolla::TypedRef::FromValue(10));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  // Note: The order of resulting items is predictable because we use eager
  // executor.
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(7.5)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(15.0)));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamMapUnorderedTest, InputStreamError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMapUnordered(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) {
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamMapUnorderedTest, FunctorError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMapUnordered(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef) { return absl::InvalidArgumentError("Boom!"); });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamMapUnorderedTest, FunctorWrongReturnType) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream = StreamMapUnordered(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [](arolla::TypedRef item) { return arolla::TypedValue(item); });
  writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                               "functor returned a value of the wrong type: "
                               "expected FLOAT64, got INT32")));
}

TEST(StreamMapUnorderedTest, OrphanedOutputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  int call_count = 0;
  stream = StreamMapUnordered(
      GetEagerExecutor(), std::move(stream), arolla::GetQType<double>(),
      [&call_count](arolla::TypedRef item) {
        call_count += 1;
        EXPECT_THAT(item, QValueWith<int>(1));
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  stream.reset();
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  // Note: The algorithm only detects that the stream is orphaned after
  // attempting to write an item, and it will call the functor to compute that
  // item first. It then stops and doesn't call the functor for the second item.
  EXPECT_EQ(call_count, 1);
}

TEST(StreamMapUnorderedTest, FunctorCancellationContextPropagation) {
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  writer->Write(arolla::TypedRef::FromValue(1));
  writer->Write(arolla::TypedRef::FromValue(2));
  std::move(*writer).Close();
  int count_down = 1;
  stream = StreamMapUnordered(
      executor, std::move(stream), arolla::GetQType<double>(),
      [&count_down](arolla::TypedRef item) {
        auto cancellation_context = arolla::CurrentCancellationContext();
        EXPECT_NE(cancellation_context, nullptr);
        if (cancellation_context != nullptr && count_down-- == 0) {
          cancellation_context->Cancel(absl::CancelledError("Boom!"));
        }
        return arolla::TypedValue::FromValue(1.5 * item.UnsafeAs<int>());
      });
  auto reader = stream->MakeReader();
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(1.5)));
  }
  {
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
    EXPECT_THAT(reader->TryRead().close_status(),
                Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
  }
}

TEST(StreamMapUnorderedTest, DoNotStartWhenCancelled) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  stream =
      StreamMapUnordered(GetEagerExecutor(), std::move(stream),
                         arolla::GetQType<double>(), [](arolla::TypedRef item) {
                           ADD_FAILURE();  // Never happens.
                           return absl::FailedPreconditionError("Never!");
                         });
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
  std::move(*writer).Close();
}

TEST(StreamMapUnorderedTest, Stress) {
  constexpr int kItemCount = 1024;
  constexpr int kLayerCount = 256;
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  for (int j = 0; j < kLayerCount; ++j) {
    stream = StreamMapUnordered(
        GetDefaultExecutor(), std::move(stream), arolla::GetQType<int>(),
        [](arolla::TypedRef item) {
          return arolla::TypedValue::FromValue(1 + item.UnsafeAs<int>());
        });
  }
  for (int i = 0; i < kItemCount; ++i) {
    writer->Write(arolla::TypedRef::FromValue(i));
  }
  std::move(*writer).Close();
  std::set<int> result;
  auto reader = stream->MakeReader();
  for (;;) {
    auto try_read_result = reader->TryRead();
    while (auto* item = try_read_result.item()) {
      result.insert(item->UnsafeAs<int>());
      try_read_result = reader->TryRead();
    }
    if (auto* status = try_read_result.close_status()) {
      ASSERT_OK(*status);
      break;
    }
    auto notification = std::make_shared<absl::Notification>();
    reader->SubscribeOnce([notification] { notification->Notify(); });
    notification->WaitForNotification();
  }
  ASSERT_EQ(result.size(), kItemCount);
  ASSERT_EQ(*result.begin(), kLayerCount);
  ASSERT_EQ(*result.rbegin(), kLayerCount + kItemCount - 1);
}

TEST(StreamMapUnorderedTest, ExecutorShutdown) {
  struct DummyExecutor final : Executor {
    void DoSchedule(TaskFn /*task_fn*/) final {}
    std::string Repr() const final { return "dummy_executor"; }
  };
  auto dummy_executor = std::make_shared<DummyExecutor>();
  auto [stream, writer] = MakeStream(arolla::GetQType<int>());
  writer->Write(arolla::TypedRef::FromValue(0));
  std::move(*writer).Close();
  stream =
      StreamMapUnordered(dummy_executor, std::move(stream),
                         arolla::GetQType<int>(), [](arolla::TypedRef item) {
                           ADD_FAILURE();  // Never happens.
                           return absl::FailedPreconditionError("Never!");
                         });
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "orphaned")));
}

}  // namespace
}  // namespace koladata::functor::parallel
