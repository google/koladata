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
#include "koladata/functor/parallel/stream_reduce_stack_or_concat.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/cancellation.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;

using ::arolla::testing::QValueWith;
using ::koladata::testing::IsEquivalentTo;
using ::testing::Pointee;

TEST(StreamReduceConcatTest, BasicNDim1) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(GetEagerExecutor(), 1,
                                 test::DataSlice<int>({0}), std::move(stream)));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({1})));
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({2})));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<DataSlice>(
                  IsEquivalentTo(test::DataSlice<int>({0, 1, 2})))));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamReduceConcatTest, BasicNDim2) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(
                  GetEagerExecutor(), 2,
                  test::DataSlice<int>({0}, test::ShapeFromSizes({{1}, {1}})),
                  std::move(stream)));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(
      test::DataSlice<int>({1}, test::ShapeFromSizes({{1}, {1}}))));
  writer->Write(arolla::TypedRef::FromValue(
      test::DataSlice<int>({2}, test::ShapeFromSizes({{1}, {1}}))));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<DataSlice>(IsEquivalentTo(test::DataSlice<int>(
                  {0, 1, 2}, test::ShapeFromSizes({{3}, {1, 1, 1}}))))));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamReduceConcatTest, InvalidNDim) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  std::move(*writer).Close();
  EXPECT_THAT(StreamReduceConcat(GetEagerExecutor(), 0,
                                 DataSlice::CreateFromScalar(0), stream),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid ndim=0 for concat"));
  EXPECT_THAT(StreamReduceConcat(GetEagerExecutor(), 1,
                                 DataSlice::CreateFromScalar(0), stream),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid ndim=1 for rank=0 concat"));
}

TEST(StreamReduceConcatTest, IncompatibleRanks) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(
                  GetEagerExecutor(), 1,
                  test::DataSlice<int>({0}, test::ShapeFromSizes({{1}, {1}})),
                  std::move(stream)));
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({})));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  "all input slices must have the same rank, got 2 and 1")));
}

TEST(StreamReduceConcatTest, IncompatibleShapes) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream,
      StreamReduceConcat(GetEagerExecutor(), 1,
                         test::DataSlice<int>(
                             {0, 1, 2}, test::ShapeFromSizes({{3}, {1, 1, 1}})),
                         std::move(stream)));
  writer->Write(arolla::TypedRef::FromValue(
      test::DataSlice<int>({}, test::ShapeFromSizes({{0}, {}}))));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(
      reader->TryRead().close_status(),
      Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                       "concat/stack requires all inputs to have the same "
                       "shape prefix before the concatenation dimension")));
}

TEST(StreamReduceConcatTest, InvalidInputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<float>());
  std::move(*writer).Close();
  EXPECT_THAT(
      StreamReduceConcat(GetEagerExecutor(), 1, test::DataSlice<int>({}),
                         stream),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "input stream has value_qtype=FLOAT32, expected DATA_SLICE"));
}

TEST(StreamReduceConcatTest, InputStreamError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(GetEagerExecutor(), 1,
                                 test::DataSlice<int>({}), std::move(stream)));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamReduceConcatTest, OrphanedOutputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(GetEagerExecutor(), 1,
                                 test::DataSlice<int>({}), std::move(stream)));
  EXPECT_TRUE(
      writer->TryWrite(arolla::TypedRef::FromValue(test::DataSlice<int>({1}))));
  stream.reset();
  // Note: The implementation detects that the resulting stream is
  // orphaned only after one extra iteration."
  (void)writer->TryWrite(
      arolla::TypedRef::FromValue(test::DataSlice<int>({1})));
  EXPECT_FALSE(
      writer->TryWrite(arolla::TypedRef::FromValue(test::DataSlice<int>({1}))));
}

TEST(StreamReduceConcatTest, Cancellation) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceConcat(GetEagerExecutor(), 1,
                                 test::DataSlice<int>({}), std::move(stream)));
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

TEST(StreamReduceStackTest, BasicNDim0) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(stream, StreamReduceStack(GetEagerExecutor(), 0,
                                                 DataSlice::CreateFromScalar(0),
                                                 std::move(stream)));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(DataSlice::CreateFromScalar(1)));
  writer->Write(arolla::TypedRef::FromValue(DataSlice::CreateFromScalar(2)));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<DataSlice>(
                  IsEquivalentTo(test::DataSlice<int>({0, 1, 2})))));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamReduceStackTest, BasicNDim1) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceStack(GetEagerExecutor(), 1,
                                test::DataSlice<int>({0}), std::move(stream)));
  EXPECT_EQ(stream->value_qtype(), arolla::GetQType<DataSlice>());
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({1})));
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({2})));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<DataSlice>(IsEquivalentTo(test::DataSlice<int>(
                  {0, 1, 2}, test::ShapeFromSizes({{3}, {1, 1, 1}}))))));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamReduceStackTest, InvalidNDim) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  std::move(*writer).Close();
  EXPECT_THAT(StreamReduceStack(GetEagerExecutor(), -1,
                                DataSlice::CreateFromScalar(0), stream),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid ndim=-1 for stack"));
  EXPECT_THAT(StreamReduceStack(GetEagerExecutor(), 1,
                                DataSlice::CreateFromScalar(0), stream),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid ndim=1 for rank=0 stack"));
}

TEST(StreamReduceStackTest, IncompatibleRanks) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(stream, StreamReduceStack(GetEagerExecutor(), 0,
                                                 DataSlice::CreateFromScalar(0),
                                                 std::move(stream)));
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({})));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  "all input slices must have the same rank, got 0 and 1")));
}

TEST(StreamReduceStackTest, IncompatibleShapes) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream,
      StreamReduceStack(GetEagerExecutor(), 0, test::DataSlice<int>({0, 1, 2}),
                        std::move(stream)));
  writer->Write(arolla::TypedRef::FromValue(test::DataSlice<int>({})));
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(
      reader->TryRead().close_status(),
      Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                       "concat/stack requires all inputs to have the same "
                       "shape prefix before the concatenation dimension")));
}

TEST(StreamReduceStackTest, InvalidInputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<float>());
  std::move(*writer).Close();
  EXPECT_THAT(
      StreamReduceStack(GetEagerExecutor(), 0, DataSlice::CreateFromScalar(0),
                        stream),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "input stream has value_qtype=FLOAT32, expected DATA_SLICE"));
}

TEST(StreamReduceStackTest, InputStreamError) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  std::move(*writer).Close(absl::InvalidArgumentError("Boom!"));
  ASSERT_OK_AND_ASSIGN(stream, StreamReduceStack(GetEagerExecutor(), 0,
                                                 DataSlice::CreateFromScalar(0),
                                                 std::move(stream)));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamReduceStackTest, OrphanedOutputStream) {
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(
      stream, StreamReduceStack(GetEagerExecutor(), 0, test::DataSlice<int>({}),
                                std::move(stream)));
  EXPECT_TRUE(
      writer->TryWrite(arolla::TypedRef::FromValue(test::DataSlice<int>({1}))));
  stream.reset();
  // Note: The implementation detects that the resulting stream is
  // orphaned only after one extra iteration."
  (void)writer->TryWrite(
      arolla::TypedRef::FromValue(test::DataSlice<int>({1})));
  EXPECT_FALSE(
      writer->TryWrite(arolla::TypedRef::FromValue(test::DataSlice<int>({1}))));
}

TEST(StreamReduceStackTest, Cancellation) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [stream, writer] = MakeStream(arolla::GetQType<DataSlice>());
  ASSERT_OK_AND_ASSIGN(stream, StreamReduceStack(GetEagerExecutor(), 0,
                                                 DataSlice::CreateFromScalar(0),
                                                 std::move(stream)));
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

}  // namespace
}  // namespace koladata::functor::parallel
