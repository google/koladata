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
#include "koladata/functor/parallel/stream_loop.h"

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

// Note: More comprehensive tests are in
// py/koladata/operators/tests/koda_internal_parallel_stream_while_loop_returns_test.py
TEST(StreamWhileLoopReturnsTest, Basic) {
  constexpr int kN = 5;
  StreamLoopFunctor condition_fn = [](absl::Span<const arolla::TypedRef> args,
                                      absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<float>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<int>());
    EXPECT_THAT(kwnames, ElementsAre("returns", "n"));
    if (args[1].UnsafeAs<int>() < kN) {
      return arolla::TypedValue::FromValue(
          test::DataItem(arolla::kUnit, schema::kMask));
    }
    return arolla::TypedValue::FromValue(
        test::DataItem(std::nullopt, schema::kMask));
  };
  StreamLoopFunctor body_fn = [](absl::Span<const arolla::TypedRef> args,
                                 absl::Span<const std::string> kwnames) {
    EXPECT_EQ(args.size(), 2);
    EXPECT_EQ(args[0].GetType(), arolla::GetQType<float>());
    EXPECT_EQ(args[1].GetType(), arolla::GetQType<int>());
    EXPECT_THAT(kwnames, ElementsAre("returns", "n"));
    return arolla::MakeNamedTuple(
        {
            "returns",
            "n",
        },
        {
            arolla::TypedRef::FromValue(args[0].UnsafeAs<float>() +
                                        args[1].UnsafeAs<int>()),
            arolla::TypedRef::FromValue(args[1].UnsafeAs<int>() + 1),
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

}  // namespace
}  // namespace koladata::functor::parallel
