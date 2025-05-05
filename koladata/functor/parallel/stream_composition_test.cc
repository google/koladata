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
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
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

}  // namespace
}  // namespace koladata::functor::parallel
