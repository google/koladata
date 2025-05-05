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
#include "koladata/functor/parallel/stream.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "koladata/functor/parallel/default_executor.h"
#include "koladata/functor/parallel/executor.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::GetQType;
using ::arolla::QTypePtr;
using ::arolla::TypedRef;

TEST(StreamTest, Basic) {
  constexpr int kItemCount = 1000;
  auto [stream, writer] = MakeStream(GetQType<int>());
  EXPECT_EQ(stream->value_qtype(), GetQType<int>());
  {
    ASSERT_NE(writer, nullptr);
    for (int i = 0; i < kItemCount; ++i) {
      writer->Write(TypedRef::FromValue(i));
    }
    std::move(*writer).Close();
  }
  {
    auto reader = stream->MakeReader();
    for (int i = 0; i < kItemCount; ++i) {
      EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), i);
    }
    EXPECT_OK(*reader->TryRead().close_status());
  }
}

TEST(StreamTest, BasicWithNonTrivialType) {
  auto [stream, writer] = MakeStream(GetQType<std::string>());
  EXPECT_EQ(stream->value_qtype(), GetQType<std::string>());
  {
    ASSERT_NE(writer, nullptr);
    for (int i = 0; i < 10; ++i) {
      writer->Write(TypedRef::FromValue(absl::StrCat(i)));
    }
    std::move(*writer).Close();
  }
  {
    auto reader = stream->MakeReader();
    for (int i = 0; i < 10; ++i) {
      EXPECT_EQ(reader->TryRead().item()->UnsafeAs<std::string>(),
                absl::StrCat(i));
    }
    EXPECT_OK(*reader->TryRead().close_status());
  }
}

TEST(StreamTest, TryCloseAfterClose) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  writer->TryClose(absl::InvalidArgumentError("Boom!"));
  writer->TryClose(absl::InvalidArgumentError("Kaboom!"));
  {
    auto reader = stream->MakeReader();
    EXPECT_THAT(*reader->TryRead().close_status(),
                StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
  }
}

TEST(StreamTest, TryWriteAfterClose) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  ASSERT_TRUE(writer->TryWrite(TypedRef::FromValue(1)));
  writer->TryClose(absl::InvalidArgumentError("Boom!"));
  ASSERT_FALSE(writer->TryWrite(TypedRef::FromValue(2)));
  {
    auto reader = stream->MakeReader();
    EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), 1);
    EXPECT_THAT(*reader->TryRead().close_status(),
                StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
  }
}

TEST(StreamTest, Subscription) {
  auto [stream, writer] = MakeStream(GetQType<int>(), 10);
  EXPECT_EQ(stream->value_qtype(), GetQType<int>());
  auto reader = stream->MakeReader();
  {
    ASSERT_TRUE(reader->TryRead().empty());
  }
  {
    bool callback_done = false;
    reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_FALSE(callback_done);
    writer->Write(TypedRef::FromValue(1));
    ASSERT_TRUE(callback_done);
  }
  {
    bool callback_done = false;
    reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_TRUE(callback_done);
  }
  {
    ASSERT_FALSE(reader->TryRead().empty());
  }
  {
    bool callback_done = false;
    reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_FALSE(callback_done);
    std::move(*writer).Close();
    ASSERT_TRUE(callback_done);
  }
  {
    bool callback_done = false;
    reader->SubscribeOnce([&callback_done] { callback_done = true; });
    ASSERT_TRUE(callback_done);
  }
}

TEST(StreamTest, SubscriptionCallbackTriggeredonStreamWriterDestructor) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  auto reader = stream->MakeReader();
  bool callback_done = false;
  reader->SubscribeOnce([&callback_done] { callback_done = true; });
  ASSERT_FALSE(callback_done);
  writer.reset();
  ASSERT_TRUE(callback_done);
}

TEST(StreamTest, CloseWithError) {
  auto [stream, writer] = MakeStream(GetQType<int>(), 10);
  auto reader = stream->MakeReader();
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  ASSERT_THAT(*reader->TryRead().close_status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(StreamTest, OrphanStreamWriter) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  ASSERT_FALSE(writer->Orphaned());
  writer->Write(TypedRef::FromValue(0));
  ASSERT_TRUE(writer->TryWrite(TypedRef::FromValue(1)));
  stream.reset();
  ASSERT_TRUE(writer->Orphaned());
  writer->Write(TypedRef::FromValue(2));
  ASSERT_FALSE(writer->TryWrite(TypedRef::FromValue(3)));
  std::move(*writer).Close();
  // No crash.
}

TEST(StreamTest, OrphanedStreamWriterDestructor) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  ASSERT_FALSE(writer->Orphaned());
  stream.reset();
  ASSERT_TRUE(writer->Orphaned());
  writer.reset();  // No crash.
}

TEST(StreamTest, StreamWriterCloseInDestructor) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  writer.reset();
  ASSERT_THAT(*stream->MakeReader()->TryRead().close_status(),
              StatusIs(absl::StatusCode::kCancelled, "orphaned"));
}

TEST(StreamTest, DemoFilter) {
  class Filter {
   public:
    Filter(ExecutorPtr executor, int factor, StreamReaderPtr reader,
           StreamWriterPtr writer)
        : executor_(std::move(executor)),
          factor_(factor),
          reader_(std::move(reader)),
          writer_(std::move(writer)) {}

    void operator()() && {
      if (writer_ == nullptr) {
        return;  // There are no consumers for the stream.
      }
      for (;;) {
        auto try_read_result = reader_->TryRead();
        if (auto* item = try_read_result.item()) {
          if (item->UnsafeAs<int>() % factor_ != 0) {
            writer_->Write(*item);
          }
        } else if (auto* status = try_read_result.close_status()) {
          std::move(*writer_).Close(std::move(*status));
          break;
        } else {
          reader_->SubscribeOnce([self = std::move(*this)]() mutable {
            ASSERT_OK(self.executor_->Schedule(std::move(self)));
          });
          break;
        }
      }
    }

   private:
    ExecutorPtr executor_;
    int factor_;
    StreamReaderPtr reader_;
    StreamWriterPtr writer_;
  };

  const auto executor = GetDefaultExecutor();
  const auto apply_filter = [&](ExecutorPtr executor, int factor,
                                StreamReaderPtr reader) {
    auto [stream, writer] = MakeStream(GetQType<int>());
    Filter(std::move(executor), factor, std::move(reader), std::move(writer))();
    return stream->MakeReader();
  };

  auto [stream, writer] = MakeStream(GetQType<int>());
  auto reader = stream->MakeReader();

  // Setup filters.
  for (int factor : {2, 3, 5, 7}) {
    reader = apply_filter(executor, factor, std::move(reader));
  }

  // Produce items.
  for (int i = 2; i < 100; ++i) {
    writer->Write(TypedRef::FromValue(i));
  };
  std::move(*writer).Close(absl::CancelledError("stop"));

  // Consume items.
  std::vector<int> values;
  for (;;) {
    auto try_read_result = reader->TryRead();
    if (auto* item = try_read_result.item()) {
      values.push_back(item->UnsafeAs<int>());
    } else if (auto* status = try_read_result.close_status()) {
      ASSERT_THAT(*status, StatusIs(absl::StatusCode::kCancelled, "stop"));
      break;
    }
  }
  ASSERT_THAT(values,
              testing::ElementsAre(11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
                                   53, 59, 61, 67, 71, 73, 79, 83, 89, 97));
}

class StresstestConsumer
    : public std::enable_shared_from_this<StresstestConsumer> {
 public:
  explicit StresstestConsumer(ExecutorPtr executor, StreamReaderPtr reader,
                              int end)
      : executor_(std::move(executor)), reader_(std::move(reader)), end_(end) {}

  void Start() {
    ASSERT_OK(executor_->Schedule(
        [self = shared_from_this()] { self->DoProcessing(); }));
  }

  void Wait() { done_.WaitForNotification(); }

 private:
  void DoProcessing() {
    for (;;) {
      auto try_read_result = reader_->TryRead();
      if (auto* item = try_read_result.item()) {
        ASSERT_EQ(item->UnsafeAs<int>(), next_++);
        ASSERT_LE(next_, end_);
      } else if (auto* status = try_read_result.close_status()) {
        ASSERT_OK(*status);
        ASSERT_EQ(next_, end_);
        done_.Notify();
        break;
      } else {
        reader_->SubscribeOnce([self = shared_from_this()]() mutable {
          ASSERT_OK(self->executor_->Schedule(
              [self = std::move(self)] { self->DoProcessing(); }));
        });
        break;
      }
    }
  }

  const ExecutorPtr executor_;
  const StreamReaderPtr reader_;
  const int end_;
  int next_ = 0;
  absl::Notification done_;
};

TEST(StreamTest, StressLongStream) {
  constexpr int kItemCount = 1'000'000;

  const auto executor = GetDefaultExecutor();
  auto [stream, writer] = MakeStream(GetQType<int>());

  // Start consumer.
  auto consumer = std::make_shared<StresstestConsumer>(
      executor, stream->MakeReader(), kItemCount);
  consumer->Start();

  // Produce items.
  for (int i = 0; i < kItemCount; ++i) {
    writer->Write(TypedRef::FromValue(i));
  }
  std::move(*writer).Close();

  // Wait for consumer.
  consumer->Wait();
}

TEST(StreamTest, StressConcurrentRead) {
  constexpr int kItemCount = 1'000;
  constexpr int kConsumerCount = 1'000;

  const auto executor = GetDefaultExecutor();
  auto [stream, writer] = MakeStream(GetQType<int>());

  // Start consumers.
  std::vector<std::shared_ptr<StresstestConsumer>> consumers(kConsumerCount);
  for (auto& consumer : consumers) {
    consumer = std::make_shared<StresstestConsumer>(
        executor, stream->MakeReader(), kItemCount);
    consumer->Start();
  }

  // Produce items.
  for (int i = 0; i < kItemCount; ++i) {
    writer->Write(TypedRef::FromValue(i));
  };
  std::move(*writer).Close();

  // Wait for consumers.
  for (auto& consumer : consumers) {
    consumer->Wait();
  }
}

TEST(StreamTest, PanicWhenWriteWrongType) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  ASSERT_DEATH(
      { writer->Write(TypedRef::FromValue(0.0)); },
      "expected a value of type INT32, got FLOAT64");
  ASSERT_DEATH(
      { (void)writer->TryWrite(TypedRef::FromValue(0.0)); },
      "expected a value of type INT32, got FLOAT64");
}

TEST(StreamTest, PanicWhenWriteToClosedStream) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::move(*writer).Close();
  ASSERT_DEATH(
      { writer->Write(TypedRef::FromValue(0)); }, "writing to a closed stream");
}

TEST(StreamTest, PanicWhenCloseClosedStream) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::move(*writer).Close();
  ASSERT_DEATH({ std::move(*writer).Close(); }, "closing a closed stream");
}

}  // namespace
}  // namespace koladata::functor::parallel
