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
#include "koladata/functor/parallel/stream_call.h"

#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "arolla/derived_qtype/labeled_qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/make_executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::testing::ElementsAre;
using ::testing::Pointee;

TEST(StreamCallTest, Basic) {
  auto functor = [](absl::Span<const arolla::TypedRef> args) {
    EXPECT_THAT(args, ElementsAre(QValueWith<int>(2), QValueWith<double>(3.5)));
    return arolla::TypedValue::FromValue(std::string("result"));
  };
  auto [arg1, arg1_writer] = MakeStream(arolla::GetQType<double>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<std::string>(),
                 {arolla::TypedRef::FromValue(2),
                  MakeStreamCallAwaitArg(MakeStreamQValueRef(arg1))}));
  EXPECT_THAT(stream->value_qtype(), arolla::GetQType<std::string>());
  arg1_writer->Write(arolla::TypedRef::FromValue(3.5));
  std::move(*arg1_writer).Close();
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<std::string>("result")));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamCallTest, FunctorReturnsStream) {
  auto functor = [](absl::Span<const arolla::TypedRef> args) {
    EXPECT_THAT(args, ElementsAre(QValueWith<int>(2), QValueWith<double>(3.5)));
    auto [result, result_writer] = MakeStream(arolla::GetQType<std::string>());
    result_writer->Write(arolla::TypedRef::FromValue(std::string("item_0")));
    result_writer->Write(arolla::TypedRef::FromValue(std::string("item_1")));
    std::move(*result_writer).Close();
    return MakeStreamQValue(std::move(result));
  };
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(
          GetEagerExecutor(), functor, arolla::GetQType<std::string>(),
          {arolla::TypedRef::FromValue(2), arolla::TypedRef::FromValue(3.5)}));
  EXPECT_THAT(stream->value_qtype(), arolla::GetQType<std::string>());
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<std::string>("item_0")));
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<std::string>("item_1")));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamCallTest, FunctorError) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    return absl::InvalidArgumentError("Boom!");
  };
  ASSERT_OK_AND_ASSIGN(auto stream, StreamCall(GetEagerExecutor(), functor,
                                               arolla::GetQType<int>(), {}));
  EXPECT_THAT(stream->value_qtype(), arolla::GetQType<int>());
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamCallTest, FunctorWrongReturnType) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    return arolla::TypedValue::FromValue(1.5);
  };
  ASSERT_OK_AND_ASSIGN(auto stream, StreamCall(GetEagerExecutor(), functor,
                                               arolla::GetQType<int>(), {}));
  EXPECT_THAT(stream->value_qtype(), arolla::GetQType<int>());
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                               "expected the functor to return INT32 or "
                               "STREAM[INT32], got FLOAT64")));
}

TEST(StreamCallTest, ErrorInAwaitedArgBeforeValue) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  std::move(*arg_writer).Close(absl::InvalidArgumentError("Boom!"));
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamCallTest, ErrorInAwaitedArgAfterValue) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  arg_writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*arg_writer).Close(absl::InvalidArgumentError("Boom!"));
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamCallTest, EmptyAwaitedArg) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  std::move(*arg_writer).Close();
  EXPECT_THAT(
      stream->MakeReader()->TryRead().close_status(),
      Pointee(StatusIs(
          absl::StatusCode::kInvalidArgument,
          "expected a stream with a single item, got an empty stream")));
}

TEST(StreamCallTest, AwaitedArgWithMultipleItems) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  arg_writer->Write(arolla::TypedRef::FromValue(1));
  arg_writer->Write(arolla::TypedRef::FromValue(2));
  EXPECT_THAT(stream->MakeReader()->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument,
                               "expected a stream with a single item, got a "
                               "stream with multiple items")));
}

TEST(StreamCallTest, AwaitedNonStreamArg) {
  auto functor = [](absl::Span<const arolla::TypedRef> args) {
    EXPECT_THAT(args, ElementsAre(QValueWith<int>(2)));
    return arolla::TypedValue::FromValue(3.0);
  };
  ASSERT_OK_AND_ASSIGN(auto arg, arolla::TypedValue::FromValueWithQType(
                                     2, arolla::GetLabeledQType(
                                            arolla::GetQType<int>(), "AWAIT")));
  ASSERT_OK_AND_ASSIGN(auto stream,
                       StreamCall(GetEagerExecutor(), functor,
                                  arolla::GetQType<double>(), {arg.AsRef()}));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<double>(3.0)));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamCallTest, NonAwaitedStreamArg) {
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<double>());
  auto functor = [&arg](absl::Span<const arolla::TypedRef> args) {
    EXPECT_EQ(args.size(), 1);
    EXPECT_EQ(args[0].GetType(), GetStreamQType<double>());
    EXPECT_EQ(args[0].UnsafeAs<StreamPtr>(), arg);
    return arolla::TypedValue::FromValue(1);
  };
  ASSERT_OK_AND_ASSIGN(auto stream, StreamCall(GetEagerExecutor(), functor,
                                               arolla::GetQType<int>(),
                                               {MakeStreamQValueRef(arg)}));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(1)));
  EXPECT_THAT(reader->TryRead().close_status(), Pointee(IsOk()));
}

TEST(StreamCallTest, FunctorCancellationContextPropagation) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    auto cancellation_context = arolla::CurrentCancellationContext();
    EXPECT_NE(cancellation_context, nullptr);
    if (cancellation_context != nullptr) {
      cancellation_context->Cancel(absl::CancelledError("Boom!"));
    }
    return arolla::TypedValue::FromValue(1);
  };
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  ASSERT_OK_AND_ASSIGN(
      auto stream, StreamCall(executor, functor, arolla::GetQType<int>(), {}));
  auto reader = stream->MakeReader();
  auto notification = std::make_shared<absl::Notification>();
  reader->SubscribeOnce([notification] { notification->Notify(); });
  EXPECT_TRUE(notification->WaitForNotificationWithTimeout(absl::Seconds(1.0)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

TEST(StreamCallTest, CancelledBeforeFunctor) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  // Note: Use a single-threaded executor to run computations on a different
  // thread, while still having predictable behaviour.
  auto executor = MakeExecutor(1);
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(executor, functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  auto reader = stream->MakeReader();
  EXPECT_TRUE(reader->TryRead().empty());
  arg_writer->Write(arolla::TypedRef::FromValue(1));
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  std::move(*arg_writer).Close();
  auto notification = std::make_shared<absl::Notification>();
  reader->SubscribeOnce([notification] { notification->Notify(); });
  EXPECT_TRUE(notification->WaitForNotificationWithTimeout(absl::Seconds(1.0)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

TEST(StreamCallTest, OrphanedBeforeFunctor) {
  auto functor = [](absl::Span<const arolla::TypedRef> /*args*/) {
    ADD_FAILURE();
    return absl::FailedPreconditionError("Never!");
  };
  auto [arg, arg_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      auto stream,
      StreamCall(GetEagerExecutor(), functor, arolla::GetQType<int>(),
                 {MakeStreamCallAwaitArg(MakeStreamQValueRef(arg))}));
  stream.reset();
  arg_writer->Write(arolla::TypedRef::FromValue(1));
  std::move(*arg_writer).Close();
}

TEST(MakeStreamCallAwaitArg, Basic) {
  {
    auto [stream, writer] = MakeStream(arolla::GetQType<double>());
    auto awaited_stream = MakeStreamCallAwaitArg(MakeStreamQValueRef(stream));
    EXPECT_EQ(awaited_stream.GetType(),
              arolla::GetLabeledQType(GetStreamQType<double>(), "AWAIT"));
    EXPECT_EQ(awaited_stream.GetRawPointer(), &stream);
  }
  {
    auto int_value = 1;
    auto awaited_int =
        MakeStreamCallAwaitArg(arolla::TypedRef::FromValue(int_value));
    EXPECT_EQ(awaited_int.GetType(), arolla::GetQType<int>());
    EXPECT_EQ(awaited_int.GetRawPointer(), &int_value);
  }
}

}  // namespace
}  // namespace koladata::functor::parallel
