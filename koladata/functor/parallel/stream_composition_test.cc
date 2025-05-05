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
#include "koladata/functor/parallel/stream_composition.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/barrier.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/notification.h"
#include "koladata/functor/parallel/default_executor.h"
#include "koladata/functor/parallel/stream.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::GetQType;
using ::arolla::TypedRef;

TEST(StreamInterleaveTest, Basic) {
  constexpr int kItemCount = 1000;
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::vector<StreamWriterPtr> writers;
  {
    StreamInterleave interleave_helper(std::move(writer));
    for (int i = 0; i < 3; ++i) {
      auto [s, w] = MakeStream(GetQType<int>());
      writers.emplace_back(std::move(w));
      interleave_helper.Add(s);
    }
  }
  for (int i = 0; i < kItemCount; ++i) {
    writers[i % 3]->Write(TypedRef::FromValue(i));
  }
  for (auto& w : writers) {
    std::move(*w).Close();
  }

  auto reader = stream->MakeReader();
  for (int i = 0; i < kItemCount; ++i) {
    EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), i);
  }
  EXPECT_OK(*reader->TryRead().close_status());
}

TEST(StreamInterleaveTest, StreamInterleaveIsAlive) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  StreamInterleave interleave_helper(std::move(writer));
  auto reader = stream->MakeReader();
  EXPECT_TRUE(reader->TryRead().empty());
  {
    auto tmp = std::move(interleave_helper);
    (void)tmp;
  }
  EXPECT_OK(*reader->TryRead().close_status());
}

TEST(StreamInterleaveTest, CloseWithError) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::vector<StreamWriterPtr> writers;
  {
    StreamInterleave interleave_helper(std::move(writer));
    for (int i = 0; i < 3; ++i) {
      auto [s, w] = MakeStream(GetQType<int>());
      writers.emplace_back(std::move(w));
      interleave_helper.Add(s);
    }
  }
  std::move(*writers[0]).Close();
  std::move(*writers[1]).Close(absl::InvalidArgumentError("Boom!"));
  writers[2]->Write(TypedRef::FromValue(0));
  std::move(*writers[2]).Close();

  auto reader = stream->MakeReader();
  EXPECT_THAT(*reader->TryRead().close_status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(StreamInterleaveTest, MultithreadedInterleaving) {
  constexpr int kItemCount = 1000;
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::vector<StreamWriterPtr> writers;
  {
    StreamInterleave interleave_helper(std::move(writer));
    for (int i = 0; i < 5; ++i) {
      auto [s, w] = MakeStream(GetQType<int>());
      writers.emplace_back(std::move(w));
      interleave_helper.Add(s);
    }
  }
  auto executor = GetDefaultExecutor();
  auto notification = std::make_shared<absl::Notification>();
  std::atomic<int> write_count = 0;
  for (int i = 0; i < kItemCount; ++i) {
    ASSERT_OK(executor->Schedule([&write_count, &writers, i, notification] {
      writers[i % writers.size()]->Write(TypedRef::FromValue(i));
      if (++write_count == kItemCount) {
        notification->Notify();
      }
    }));
  }
  notification->WaitForNotification();
  for (auto& w : writers) {
    std::move(*w).Close();
  }
  auto reader = stream->MakeReader();
  std::vector<int> result;
  for (int i = 0; i < kItemCount; ++i) {
    result.push_back(reader->TryRead().item()->UnsafeAs<int>());
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  ASSERT_EQ(result.size(), kItemCount);
  ASSERT_EQ(result.front(), 0);
}

TEST(StreamInterleaveTest, DynamicAddInputs) {
  auto [first_stream, first_writer] = MakeStream(GetQType<int>());
  first_writer->Write(TypedRef::FromValue(0));
  std::move(*first_writer).Close();
  auto [second_stream, second_writer] = MakeStream(GetQType<int>());
  second_writer->Write(TypedRef::FromValue(1));
  std::move(*second_writer).Close();
  auto [stream, writer] = MakeStream(GetQType<int>());
  auto reader = stream->MakeReader();
  {
    StreamInterleave interleave_helper(std::move(writer));
    reader->SubscribeOnce([&] { interleave_helper.Add(second_stream); });
    interleave_helper.Add(first_stream);
  }
  EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), 0);
  EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), 1);
  EXPECT_OK(*reader->TryRead().close_status());
}

TEST(StreamChainTest, Basic) {
  auto [stream1, stream1_writer] = MakeStream(GetQType<int>());
  auto [stream2, stream2_writer] = MakeStream(GetQType<int>());
  auto [stream3, stream3_writer] = MakeStream(GetQType<int>());
  auto [chained_stream, chained_writer] = MakeStream(GetQType<int>());
  {
    StreamChain chain_helper(std::move(chained_writer));
    chain_helper.Add(stream1);
    chain_helper.Add(stream2);
    chain_helper.Add(stream3);
  }
  stream1_writer->Write(TypedRef::FromValue(0));
  stream2_writer->Write(TypedRef::FromValue(2));
  stream1_writer->Write(TypedRef::FromValue(1));
  stream2_writer->Write(TypedRef::FromValue(3));
  stream3_writer->Write(TypedRef::FromValue(4));
  std::move(*stream2_writer).Close();
  auto stream_reader = chained_stream->MakeReader();
  EXPECT_EQ(stream_reader->TryRead().item()->UnsafeAs<int>(), 0);
  EXPECT_EQ(stream_reader->TryRead().item()->UnsafeAs<int>(), 1);
  EXPECT_TRUE(stream_reader->TryRead().empty());
  std::move(*stream1_writer).Close();
  EXPECT_EQ(stream_reader->TryRead().item()->UnsafeAs<int>(), 2);
  EXPECT_EQ(stream_reader->TryRead().item()->UnsafeAs<int>(), 3);
  EXPECT_EQ(stream_reader->TryRead().item()->UnsafeAs<int>(), 4);
  EXPECT_TRUE(stream_reader->TryRead().empty());
  std::move(*stream3_writer).Close();
  EXPECT_OK(*stream_reader->TryRead().close_status());
}

TEST(StreamChainTest, StreamChainIsAlive) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  StreamChain chain_helper(std::move(writer));
  auto reader = stream->MakeReader();
  EXPECT_TRUE(reader->TryRead().empty());
  {
    auto tmp = std::move(chain_helper);
    (void)tmp;
  }
  EXPECT_OK(*reader->TryRead().close_status());
}

TEST(StreamChainTest, CloseWithError) {
  auto [stream, writer] = MakeStream(GetQType<int>());
  std::vector<StreamWriterPtr> writers;
  {
    StreamChain chain_helper(std::move(writer));
    for (int i = 0; i < 3; ++i) {
      auto [s, w] = MakeStream(GetQType<int>());
      writers.emplace_back(std::move(w));
      chain_helper.Add(s);
    }
  }
  std::move(*writers[0]).Close();
  std::move(*writers[1]).Close(absl::InvalidArgumentError("Boom!"));
  writers[2]->Write(TypedRef::FromValue(0));
  std::move(*writers[2]).Close();

  auto reader = stream->MakeReader();
  EXPECT_THAT(*reader->TryRead().close_status(),
              StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(StreamChainTest, MultithreadedChaining) {
  absl::BitGen rng;

  // We try to create situations where a new input is added at the same time
  // as an item is written to an existing input or when an existing input is
  // closed, to try to trigger race conditions if they exist.
  constexpr int kStreamCount = 1000;
  constexpr int kMaxParallelism = 10;
  int max_time = kStreamCount + kMaxParallelism;
  // We have streams appear and disappear roughly in sequence, but with
  // some random noise.
  std::vector<int> stream_start_at(kStreamCount);
  std::vector<int> stream_finish_at(kStreamCount);
  std::vector<std::vector<int>> streams_to_start(max_time);
  std::vector<std::vector<int>> streams_to_finish(max_time);
  std::vector<int> expected_result;
  for (int i = 0; i < kStreamCount; ++i) {
    int start_at = absl::Uniform(rng, i, i + kMaxParallelism);
    int finish_at = absl::Uniform(rng, start_at, i + kMaxParallelism) + 1;
    streams_to_start[start_at].push_back(i);
    streams_to_finish[finish_at].push_back(i);
    stream_start_at[i] = start_at;
    stream_finish_at[i] = finish_at;
    for (int time = start_at; time < finish_at; ++time) {
      expected_result.push_back(time);
    }
  }
  std::vector<std::pair<StreamPtr, StreamWriterPtr>> streams;
  streams.reserve(kStreamCount);
  for (int i = 0; i < kStreamCount; ++i) {
    streams.push_back(MakeStream(GetQType<int>()));
  }

  std::vector<absl::Barrier*> barriers;
  absl::BlockingCounter counter(kStreamCount);
  std::set<int> alive_streams;
  std::vector<int> max_alive_at(max_time);
  int max_alive = 0;
  for (int time = 0; time < max_time; ++time) {
    for (int stream_id : streams_to_start[time]) {
      alive_streams.insert(stream_id);
      max_alive = std::max(max_alive, stream_id);
    }
    barriers.push_back(new absl::Barrier(alive_streams.size() + 1));
    for (int stream_id : streams_to_finish[time]) {
      alive_streams.erase(stream_id);
    }
    max_alive_at[time] = max_alive;
  }
  ASSERT_TRUE(alive_streams.empty());

  auto executor = GetDefaultExecutor();
  for (int i = 0; i < kStreamCount; ++i) {
    const auto& [_, writer] = streams[i];
    int start_at = stream_start_at[i];
    int finish_at = stream_finish_at[i];
    ASSERT_OK(executor->Schedule(
        [&writer, &barriers, &counter, start_at, finish_at]() {
          for (int time = start_at; time <= finish_at; ++time) {
            if (barriers[time]->Block()) delete barriers[time];
            if (time == finish_at) {
              std::move(*writer).Close();
            } else {
              writer->Write(TypedRef::FromValue(time));
            }
          }
          counter.DecrementCount();
        }));
  }

  auto [output_stream, output_writer] = MakeStream(GetQType<int>());
  auto chain_helper = std::make_unique<StreamChain>(std::move(output_writer));
  int added_up_to = 0;
  for (int time = 0; time < max_time; ++time) {
    while (added_up_to <= max_alive_at[time]) {
      chain_helper->Add(streams[added_up_to].first);
      ++added_up_to;
      if (added_up_to == kStreamCount) {
        chain_helper.reset();
      }
    }
    if (barriers[time]->Block()) delete barriers[time];
  }
  ASSERT_EQ(added_up_to, kStreamCount);
  counter.Wait();

  auto output_reader = output_stream->MakeReader();
  std::vector<int> result;
  for (;;) {
    if (auto* item = output_reader->TryRead().item()) {
      ASSERT_OK_AND_ASSIGN(int value, item->As<int>());
      result.push_back(value);
    } else {
      break;
    }
  }
  EXPECT_EQ(result, expected_result);
}

TEST(StreamChainTest, DynamicAddInputs) {
  auto [first_stream, first_writer] = MakeStream(GetQType<int>());
  first_writer->Write(TypedRef::FromValue(0));
  std::move(*first_writer).Close();
  auto [second_stream, second_writer] = MakeStream(GetQType<int>());
  second_writer->Write(TypedRef::FromValue(1));
  std::move(*second_writer).Close();
  auto [stream, writer] = MakeStream(GetQType<int>());
  auto reader = stream->MakeReader();
  {
    StreamChain chain_helper(std::move(writer));
    reader->SubscribeOnce([&] { chain_helper.Add(second_stream); });
    chain_helper.Add(first_stream);
  }
  EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), 0);
  EXPECT_EQ(reader->TryRead().item()->UnsafeAs<int>(), 1);
  EXPECT_OK(*reader->TryRead().close_status());
}

}  // namespace
}  // namespace koladata::functor::parallel
