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
#include "koladata/functor/parallel/stream_while_loop.h"

#include <optional>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata::functor::parallel {
namespace {

using ::testing::ElementsAre;

// Note: More comprehensive tests are in
// py/koladata/operators/tests/koda_internal_parallel_stream_while_loop_returns_test.py
TEST(StreamWhileLoopReturnsTest, Basic) {
  constexpr int kN = 5;
  StreamWhileLoopFunctor condition_fn =
      [](absl::Span<const arolla::TypedRef> args,
         absl::Span<const std::string> kwnames) {
        EXPECT_EQ(args.size(), 2);
        EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
        EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
        EXPECT_THAT(kwnames, ElementsAre("n", "returns"));
        if (args[0].UnsafeAs<int>() < kN) {
          return arolla::TypedValue::FromValue(
              test::DataItem(arolla::kUnit, schema::kMask));
        } else {
          return arolla::TypedValue::FromValue(
              test::DataItem(std::nullopt, schema::kMask));
        }
      };
  StreamWhileLoopFunctor body_fn = [](absl::Span<const arolla::TypedRef> args,
                                      absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "returns"));
    return arolla::MakeNamedTuple(
        {
            "n",
            "returns",
        },
        {
            arolla::TypedRef::FromValue(args[0].UnsafeAs<int>() + 1),
            arolla::TypedRef::FromValue(args[1].UnsafeAs<float>() +
                                        args[0].UnsafeAs<int>()),
        });
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamWhileLoopReturns(
          GetEagerExecutor(), std::move(condition_fn), std::move(body_fn),
          arolla::TypedRef::FromValue(0.5f),
          arolla::MakeNamedTuple({"n"}, {arolla::TypedRef::FromValue(1)})
              ->AsRef()));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 0.5 + 1 + 2 + 3 + 4);
  EXPECT_OK(*reader->TryRead().close_status());
}

// Note: More comprehensive tests are in
// py/koladata/operators/tests/koda_internal_parallel_stream_while_loop_yields_chained_test.py
TEST(StreamWhileLoopYieldsChainedTest, Basic) {
  constexpr int kN = 3;
  StreamWhileLoopFunctor condition_fn =
      [](absl::Span<const arolla::TypedRef> args,
         absl::Span<const std::string> kwnames) {
        EXPECT_EQ(args.size(), 2);
        EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
        EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
        EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
        if (args[0].UnsafeAs<int>() < kN) {
          return arolla::TypedValue::FromValue(
              test::DataItem(arolla::kUnit, schema::kMask));
        } else {
          return arolla::TypedValue::FromValue(
              test::DataItem(std::nullopt, schema::kMask));
        }
      };
  StreamWhileLoopFunctor body_fn = [](absl::Span<const arolla::TypedRef> args,
                                      absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
    auto [stream, writer] = MakeStream(arolla::GetQType<float>());
    writer->Write(arolla::TypedRef::FromValue(args[1].UnsafeAs<float>()));
    writer->Write(arolla::TypedRef::FromValue(
        static_cast<float>(args[0].UnsafeAs<int>())));
    std::move(*writer).Close();
    return arolla::MakeNamedTuple(
        {
            "n",
            "acc",
            "yields",
        },
        {
            arolla::TypedRef::FromValue(args[0].UnsafeAs<int>() + 1),
            arolla::TypedRef::FromValue(args[0].UnsafeAs<int>() +
                                        args[1].UnsafeAs<float>()),
            MakeStreamQValue(std::move(stream)).AsRef(),
        });
  };
  auto [initial_yields_stream, initial_yields_writer] =
      MakeStream(arolla::GetQType<float>());
  initial_yields_writer->Write(arolla::TypedRef::FromValue(0.0f));
  std::move(*initial_yields_writer).Close();
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamWhileLoopYieldsChained(GetEagerExecutor(), std::move(condition_fn),
                                   std::move(body_fn), initial_yields_stream,
                                   arolla::MakeNamedTuple(
                                       {
                                           "n",
                                           "acc",
                                       },
                                       {
                                           arolla::TypedRef::FromValue(1),
                                           arolla::TypedRef::FromValue(0.5f),
                                       })
                                       ->AsRef()));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 0.0f);  // initial
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 0.5f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 1.0f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 1.5f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 2.0f);
  EXPECT_OK(*reader->TryRead().close_status());
}

// Note: More comprehensive tests are in
// py/koladata/operators/tests/koda_internal_parallel_stream_while_loop_yields_interleaved_test.py
TEST(StreamWhileLoopYieldsInterleavedTest, Basic) {
  constexpr int kN = 3;
  StreamWhileLoopFunctor condition_fn =
      [](absl::Span<const arolla::TypedRef> args,
         absl::Span<const std::string> kwnames) {
        EXPECT_EQ(args.size(), 2);
        EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
        EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
        EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
        if (args[0].UnsafeAs<int>() < kN) {
          return arolla::TypedValue::FromValue(
              test::DataItem(arolla::kUnit, schema::kMask));
        } else {
          return arolla::TypedValue::FromValue(
              test::DataItem(std::nullopt, schema::kMask));
        }
      };
  StreamWhileLoopFunctor body_fn = [](absl::Span<const arolla::TypedRef> args,
                                      absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
    auto [stream, writer] = MakeStream(arolla::GetQType<float>());
    writer->Write(arolla::TypedRef::FromValue(args[1].UnsafeAs<float>()));
    writer->Write(arolla::TypedRef::FromValue(
        static_cast<float>(args[0].UnsafeAs<int>())));
    std::move(*writer).Close();
    return arolla::MakeNamedTuple(
        {
            "n",
            "acc",
            "yields_interleaved",
        },
        {
            arolla::TypedRef::FromValue(args[0].UnsafeAs<int>() + 1),
            arolla::TypedRef::FromValue(args[0].UnsafeAs<int>() +
                                        args[1].UnsafeAs<float>()),
            MakeStreamQValue(std::move(stream)).AsRef(),
        });
  };
  auto [initial_yields_stream, initial_yields_writer] =
      MakeStream(arolla::GetQType<float>());
  initial_yields_writer->Write(arolla::TypedRef::FromValue(0.0f));
  std::move(*initial_yields_writer).Close();
  ASSERT_OK_AND_ASSIGN(StreamPtr stream,
                       StreamWhileLoopYieldsInterleaved(
                           GetEagerExecutor(), std::move(condition_fn),
                           std::move(body_fn), initial_yields_stream,
                           arolla::MakeNamedTuple(
                               {
                                   "n",
                                   "acc",
                               },
                               {
                                   arolla::TypedRef::FromValue(1),
                                   arolla::TypedRef::FromValue(0.5f),
                               })
                               ->AsRef()));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
  // Note: The order of items is deterministic due to the use of an eager
  // executor.
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 0.0f);  // initial
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 0.5f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 1.0f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 1.5f);
  EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), 2.0f);
  EXPECT_OK(*reader->TryRead().close_status());
}

}  // namespace
}  // namespace koladata::functor::parallel
