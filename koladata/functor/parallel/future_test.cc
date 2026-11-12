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
#include "koladata/functor/parallel/future.h"

#include <atomic>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/notification.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/make_executor.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::testing::Pointee;

template <class T>
void MarkReinitialized(T&) {}

TEST(FutureTest, Basic) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  EXPECT_EQ(future->value_qtype(), arolla::GetQType<int>());
  EXPECT_EQ(writer.value_qtype(), arolla::GetQType<int>());
  EXPECT_EQ(arolla::Repr(future), "future[INT32]");
  EXPECT_THAT(
      future->GetValueForTesting(),
      StatusIs(absl::StatusCode::kInvalidArgument, "future has no value"));
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  EXPECT_THAT(future->GetValueForTesting(), IsOkAndHolds(QValueWith<int>(1)));
}

TEST(FutureTest, PeekValue) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  EXPECT_FALSE(future->PeekValue().has_value());
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  ASSERT_TRUE(future->PeekValue().has_value());
  EXPECT_THAT(*future->PeekValue(), IsOkAndHolds(QValueWith<int>(1)));
}

TEST(FutureTest, PeekValueError) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  EXPECT_FALSE(future->PeekValue().has_value());
  std::move(writer).SetValue(absl::OutOfRangeError("test error"));
  ASSERT_TRUE(future->PeekValue().has_value());
  EXPECT_THAT(*future->PeekValue(),
              StatusIs(absl::StatusCode::kOutOfRange, "test error"));
}

TEST(FutureTest, FutureValueFingerprint) {
  auto [x, x_writer] = MakeFuture(arolla::GetQType<int>());
  auto [y, y_writer] = MakeFuture(arolla::GetQType<int>());
  auto x_fingerprint = arolla::FingerprintHasher("salt").Combine(x).Finish();
  EXPECT_EQ(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(x).Finish());
  EXPECT_NE(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(y).Finish());
  std::move(x_writer).SetValue(arolla::TypedValue::FromValue(1));
  EXPECT_EQ(x_fingerprint,
            arolla::FingerprintHasher("salt").Combine(x).Finish());
}

TEST(FutureTest, Nullptr) {
  FuturePtr x;
  EXPECT_EQ(arolla::Repr(x), "future{nullptr}");
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(x).Finish(),
            arolla::FingerprintHasher("salt").Combine(x).Finish());
}

TEST(FutureTest, ConsumersWithValue) {
  auto [x, writer] = MakeFuture(arolla::GetQType<int>());
  int calls = 0;
  auto consumer = [&calls](absl::StatusOr<arolla::TypedValue> value) {
    ++calls;
    ASSERT_THAT(value, IsOkAndHolds(QValueWith<int>(1)));
  };
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 0);
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  EXPECT_EQ(calls, 1);
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 2);
}

TEST(FutureTest, ConsumersWithErrorStatus) {
  auto [x, writer] = MakeFuture(arolla::GetQType<int>());
  int calls = 0;
  auto consumer = [&calls](absl::StatusOr<arolla::TypedValue> value) {
    ++calls;
    ASSERT_THAT(value, StatusIs(absl::StatusCode::kOutOfRange, "test error"));
  };
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 0);
  std::move(writer).SetValue(absl::OutOfRangeError("test error"));
  EXPECT_EQ(calls, 1);
  x->AddConsumer(consumer);
  EXPECT_EQ(calls, 2);
}

TEST(FutureTest, SetValueTwice) {
  auto [x, writer] = MakeFuture(arolla::GetQType<int>());
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  MarkReinitialized(writer);  // Silence the use-after-move error for test.
  ASSERT_DEATH(
      { std::move(writer).SetValue(arolla::TypedValue::FromValue(2)); },
      "Trying to set value on a moved-from FutureWriter");
}

TEST(FutureTest, SetValueWrongType) {
  auto [x, writer] = MakeFuture(arolla::GetQType<int>());
  ASSERT_DEATH(
      { std::move(writer).SetValue(arolla::TypedValue::FromValue(1.0)); },
      "value type FLOAT64 does not match future type INT32");
}

TEST(FutureTest, OrphanedWriter) {
  int calls = 0;
  auto consumer = [&calls](absl::StatusOr<arolla::TypedValue> value) {
    ++calls;
    ASSERT_THAT(value, StatusIs(absl::StatusCode::kCancelled, "orphaned"));
  };
  FuturePtr future;
  {
    auto [x, writer] = MakeFuture(arolla::GetQType<int>());
    x->AddConsumer(consumer);
    future = x;
    EXPECT_EQ(calls, 0);
    // Writer gets destroyed here.
  }
  EXPECT_EQ(calls, 1);
  EXPECT_THAT(future->GetValueForTesting(),
              StatusIs(absl::StatusCode::kCancelled, "orphaned"));
}

TEST(FutureTest, GetValueForTestingOnError) {
  auto [x, writer] = MakeFuture(arolla::GetQType<int>());
  std::move(writer).SetValue(absl::OutOfRangeError("test error"));
  EXPECT_THAT(x->GetValueForTesting(),
              StatusIs(absl::StatusCode::kOutOfRange, "test error"));
}

TEST(FutureTest, MakeReaderBasic) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  auto stream_reader_1 = future->MakeReader();
  EXPECT_TRUE(stream_reader_1->TryRead().empty());
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  EXPECT_THAT(stream_reader_1->TryRead().item(), Pointee(QValueWith<int>(1)));
  EXPECT_THAT(stream_reader_1->TryRead().close_status(), Pointee(IsOk()));
  auto stream_reader_2 = future->MakeReader();
  EXPECT_THAT(stream_reader_2->TryRead().item(), Pointee(QValueWith<int>(1)));
  EXPECT_THAT(stream_reader_2->TryRead().close_status(), Pointee(IsOk()));
}

TEST(FutureTest, MakeReaderSetError) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  auto stream_reader = future->MakeReader();
  std::move(writer).SetValue(absl::InvalidArgumentError("Boom!"));
  ASSERT_THAT(stream_reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
  ASSERT_THAT(stream_reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(FutureTest, MakeReaderSubscription) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  auto stream_reader = future->MakeReader();
  {
    int callback_count = 0;
    stream_reader->SubscribeOnce([&callback_count] { ++callback_count; });
    stream_reader->SubscribeOnce([&callback_count] { ++callback_count; });
    ASSERT_EQ(callback_count, 0);
    std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
    ASSERT_EQ(callback_count, 2);
  }
  {
    bool callback_done = false;
    stream_reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_TRUE(callback_done);
    ASSERT_THAT(stream_reader->TryRead().item(), Pointee(QValueWith<int>(1)));
  }
  {
    bool callback_done = false;
    stream_reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_TRUE(callback_done);
    ASSERT_THAT(stream_reader->TryRead().close_status(), Pointee(IsOk()));
  }
}

TEST(FutureTest, MakeReaderSubscriptionCallbackTriggeredOnWriterDestructor) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  auto stream_reader = future->MakeReader();
  bool callback_done = false;
  stream_reader->SubscribeOnce([&callback_done] { callback_done = true; });
  ASSERT_FALSE(callback_done);
  {
    auto tmp = std::move(writer);
  }
  ASSERT_TRUE(callback_done);
  ASSERT_THAT(stream_reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "orphaned")));
}

TEST(FutureTest, MakeReaderDestructionReleasesSubscriptions) {
  auto [future, writer] = MakeFuture(arolla::GetQType<int>());
  auto stream_reader = future->MakeReader();
  auto callback_resource = std::make_shared<int>();
  std::weak_ptr<int> weak_callback_resource = callback_resource;
  bool callback_done = false;
  stream_reader->SubscribeOnce(
      [&callback_done, callback_resource = std::move(callback_resource)] {
        callback_done = true;
      });
  ASSERT_FALSE(weak_callback_resource.expired());
  stream_reader.reset();
  // The callback is released, even though the future has no value yet.
  EXPECT_TRUE(weak_callback_resource.expired());
  std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
  EXPECT_FALSE(callback_done);
}

TEST(FutureTest, MakeReaderConcurrentSubscriptions) {
  constexpr int kIterations = 100;
  constexpr int kSubscriberCount = 8;
  const auto executor = MakeExecutor(4);
  for (int i = 0; i < kIterations; ++i) {
    SCOPED_TRACE(absl::StrCat("iteration=", i));
    auto [future, writer] = MakeFuture(arolla::GetQType<int>());
    auto stream_reader = future->MakeReader();
    std::atomic<int> callback_count = 0;
    absl::Notification go;
    absl::BlockingCounter done(kSubscriberCount + 1);
    for (int j = 0; j < kSubscriberCount; ++j) {
      executor->Schedule([&] {
        go.WaitForNotification();
        stream_reader->SubscribeOnce([&callback_count] { ++callback_count; });
        done.DecrementCount();
      });
    }
    executor->Schedule([&] {
      go.WaitForNotification();
      std::move(writer).SetValue(arolla::TypedValue::FromValue(1));
      done.DecrementCount();
    });
    go.Notify();
    done.Wait();
    EXPECT_EQ(callback_count.load(), kSubscriberCount);
  }
}

}  // namespace
}  // namespace koladata::functor::parallel
