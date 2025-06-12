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
// Note: More comprehensive tests are in
// py/koladata/operators/tests/koda_internal_parallel_stream_for_test.py

#include "koladata/functor/parallel/stream_for.h"

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
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata::functor::parallel {
namespace {

using ::testing::ElementsAre;

TEST(StreamForReturnsTest, Basic) {
  constexpr int kN = 3;
  constexpr auto body_fn = [](absl::Span<const arolla::TypedRef> args,
                              absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 3);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[2].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "returns"));
    return arolla::MakeNamedTuple(
        {
            "n",
            "returns",
        },
        {
            arolla::TypedRef::FromValue(args[1].UnsafeAs<int>() + 1),
            arolla::TypedRef::FromValue(args[2].UnsafeAs<float>() +
                                        args[0].UnsafeAs<int>()),
        });
  };
  constexpr auto finalize_fn = [](absl::Span<const arolla::TypedRef> args,
                                  absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "returns"));
    return arolla::MakeNamedTuple(
        {"returns"}, {arolla::TypedRef::FromValue(-args[1].UnsafeAs<float>())});
  };
  constexpr auto condition_fn = [](absl::Span<const arolla::TypedRef> args,
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
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<int>());
    ASSERT_OK_AND_ASSIGN(
        stream,
        StreamForReturns(
            GetEagerExecutor(), std::move(stream), body_fn, finalize_fn,
            condition_fn, arolla::TypedRef::FromValue(0.5f),
            arolla::MakeNamedTuple({"n"}, {arolla::TypedRef::FromValue(0)})
                ->AsRef()));
    EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
    for (int x = 1; x < 10; ++x) {
      writer->Write(arolla::TypedRef::FromValue(x));
    }
    std::move(*writer).Close();
    auto reader = stream->MakeReader();
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), (0.5 + 1 + 2 + 3));
    EXPECT_OK(*reader->TryRead().close_status());
  }
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<int>());
    ASSERT_OK_AND_ASSIGN(
        stream,
        StreamForReturns(
            GetEagerExecutor(), std::move(stream), body_fn, finalize_fn,
            condition_fn, arolla::TypedRef::FromValue(0.5f),
            arolla::MakeNamedTuple({"n"}, {arolla::TypedRef::FromValue(0)})
                ->AsRef()));
    EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
    for (int x = 1; x < 3; ++x) {
      writer->Write(arolla::TypedRef::FromValue(x));
    }
    std::move(*writer).Close();
    auto reader = stream->MakeReader();
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(), -(0.5 + 1 + 2));
    EXPECT_OK(*reader->TryRead().close_status());
  }
}

TEST(StreamForYieldsTest, Basic) {
  constexpr int kN = 4;
  constexpr auto body_fn = [](absl::Span<const arolla::TypedRef> args,
                              absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 3);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[2].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
    return arolla::MakeNamedTuple(
        {
            "n",
            "acc",
            "yields",
        },
        {
            arolla::TypedRef::FromValue(args[1].UnsafeAs<int>() + 1),
            arolla::TypedRef::FromValue(args[2].UnsafeAs<float>() +
                                        args[0].UnsafeAs<int>()),
            args[2],
        });
  };
  constexpr auto finalize_fn = [](absl::Span<const arolla::TypedRef> args,
                                  absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<int>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<float>());
    EXPECT_THAT(kwnames, ElementsAre("n", "acc"));
    return arolla::MakeNamedTuple(
        {
            "yields",
        },
        {
            arolla::TypedRef::FromValue(-args[1].UnsafeAs<float>()),
        });
  };
  constexpr auto condition_fn = [](absl::Span<const arolla::TypedRef> args,
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
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<int>());
    ASSERT_OK_AND_ASSIGN(
        stream, StreamForYields(GetEagerExecutor(), std::move(stream), body_fn,
                                finalize_fn, condition_fn, "yields",
                                arolla::TypedRef::FromValue(-1.0f),
                                arolla::MakeNamedTuple(
                                    {
                                        "n",
                                        "acc",
                                    },
                                    {
                                        arolla::TypedRef::FromValue(0),
                                        arolla::TypedRef::FromValue(0.5f),
                                    })
                                    ->AsRef()));
    EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
    for (int x = 1; x < 10; ++x) {
      writer->Write(arolla::TypedRef::FromValue(x));
    }
    std::move(*writer).Close();
    auto reader = stream->MakeReader();
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (-1.0f));  // initial `yields`
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f));  // 1st iteration
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f + 1.0f));  // 2nd iteration
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f + 1.0f + 2.0f));  // 3rd iteration
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f + 1.0f + 2.0f + 3.0f));  // 4th iteration
    // negative condition -> no finalizer
    EXPECT_OK(*reader->TryRead().close_status());
  }
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<int>());
    ASSERT_OK_AND_ASSIGN(
        stream, StreamForYields(GetEagerExecutor(), std::move(stream), body_fn,
                                finalize_fn, condition_fn, "yields",
                                arolla::TypedRef::FromValue(-1.0f),
                                arolla::MakeNamedTuple(
                                    {
                                        "n",
                                        "acc",
                                    },
                                    {
                                        arolla::TypedRef::FromValue(0),
                                        arolla::TypedRef::FromValue(0.5f),
                                    })
                                    ->AsRef()));
    EXPECT_EQ(stream->value_qtype(), arolla::GetQType<float>());
    for (int x = 1; x < 3; ++x) {
      writer->Write(arolla::TypedRef::FromValue(x));
    }
    std::move(*writer).Close();
    auto reader = stream->MakeReader();
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (-1.0f));  // initial `yields`
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f));  // 1st iteration
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                (0.5f + 1.0f));  // 2nd iteration
    EXPECT_THAT(reader->TryRead().item()->UnsafeAs<float>(),
                -(0.5f + 1.0f + 2.0f));  // <EOS> -> finalizer
    EXPECT_OK(*reader->TryRead().close_status());
  }
}

}  // namespace
}  // namespace koladata::functor::parallel
