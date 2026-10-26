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
#include "koladata/contrib/cascade.h"

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "absl/types/span.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/cancellation.h"
#include "koladata/functor/parallel/eager_executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"

namespace koladata::contrib {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::MakeEmptyNamedTuple;
using ::arolla::testing::QValueWith;
using ::koladata::functor::parallel::GetEagerExecutor;
using ::koladata::functor::parallel::MakeStream;
using ::koladata::functor::parallel::MakeStreamQValueRef;
using ::koladata::functor::parallel::StreamPtr;
using ::koladata::functor::parallel::StreamReaderPtr;
using ::koladata::functor::parallel::StreamWriterPtr;
using ::testing::ElementsAre;
using ::testing::Pointee;

// The initial value of the `yields` variable, which is the first item of
// the resulting stream.
constexpr int kInitialYields = -1;

// Returns a closed stream with the given items.
StreamPtr MakeIntStream(absl::Span<const int> items,
                        absl::Status status = absl::OkStatus()) {
  auto [stream, writer] = MakeStream(arolla::GetQType<int>(), items.size());
  for (int item : items) {
    writer->Write(arolla::TypedRef::FromValue(item));
  }
  std::move(*writer).Close(std::move(status));
  return stream;
}

arolla::TypedValue EmptyState() { return MakeEmptyNamedTuple(); }

// Reads all items from the stream, waiting until the stream is closed.
absl::StatusOr<std::vector<int>> ReadAllInts(const StreamPtr& stream) {
  std::vector<int> result;
  StreamReaderPtr reader = stream->MakeReader();
  for (;;) {
    auto try_read_result = reader->TryRead();
    if (auto* item = try_read_result.item()) {
      result.push_back(item->UnsafeAs<int>());
      continue;
    }
    if (auto* status = try_read_result.close_status()) {
      if (!status->ok()) {
        return std::move(*status);
      }
      return result;
    }
    absl::Notification notification;
    reader->SubscribeOnce([&] { notification.Notify(); });
    notification.WaitForNotification();
  }
}

// step(batch) -> namedtuple(
//     yields=1000 * len(batch) + sum(batch),
//     cascades=[n - 1 for n in batch if n > 0],
// )
absl::StatusOr<arolla::TypedValue> BatchCountDownStep(
    absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) {
  EXPECT_EQ(args.size(), kwnames.size() + 1);
  ASSIGN_OR_RETURN(std::vector<int> batch,
                   ReadAllInts(args[0].UnsafeAs<StreamPtr>()));
  int yields = 1000 * batch.size();
  std::vector<int> cascades;
  for (int n : batch) {
    yields += n;
    if (n > 0) {
      cascades.push_back(n - 1);
    }
  }
  StreamPtr cascades_stream = MakeIntStream(cascades);
  return arolla::MakeNamedTuple({"yields", "cascades"},
                                {arolla::TypedRef::FromValue(yields),
                                 MakeStreamQValueRef(cascades_stream)});
}

TEST(StreamBatchedCascadeTest, CountDown) {
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(GetEagerExecutor(), MakeIntStream({3, 1}),
                           BatchCountDownStep, "yields",
                           arolla::TypedRef::FromValue(kInitialYields),
                           EmptyState().AsRef()));
  // Batches: [3, 1], [2, 0], [1], [0].
  EXPECT_THAT(
      ReadAllInts(stream),
      IsOkAndHolds(ElementsAre(kInitialYields, 2004, 2002, 1001, 1000)));
}

TEST(StreamBatchedCascadeTest, NonTrivialBatches) {
  std::vector<std::vector<int>> batches;
  auto step_fn = [&batches](absl::Span<const arolla::TypedRef> args,
                            absl::Span<const std::string> /*kwnames*/)
      -> absl::StatusOr<arolla::TypedValue> {
    ASSIGN_OR_RETURN(std::vector<int> batch,
                     ReadAllInts(args[0].UnsafeAs<StreamPtr>()));
    std::vector<int> cascades;
    for (int x : batch) {
      if (x < 100) {
        cascades.push_back(10 * x + 1);
        cascades.push_back(10 * x + 2);
      }
    }
    batches.push_back(std::move(batch));
    if (cascades.empty()) {
      return arolla::MakeEmptyNamedTuple();
    }
    return arolla::MakeNamedTuple(
        {"cascades"}, {MakeStreamQValueRef(MakeIntStream(cascades))});
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1, 2}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(ReadAllInts(stream), IsOkAndHolds(ElementsAre(kInitialYields)));
  EXPECT_THAT(batches,
              ElementsAre(ElementsAre(1, 2), ElementsAre(11, 12, 21, 22),
                          ElementsAre(111, 112, 121, 122, 211, 212, 221, 222)));
}

TEST(StreamBatchedCascadeTest, State) {
  auto step_fn = [](absl::Span<const arolla::TypedRef> args,
                    absl::Span<const std::string> kwnames)
      -> absl::StatusOr<arolla::TypedValue> {
    EXPECT_THAT(kwnames, ElementsAre("total"));
    ASSIGN_OR_RETURN(std::vector<int> batch,
                     ReadAllInts(args[0].UnsafeAs<StreamPtr>()));
    int total = args[1].UnsafeAs<int>();
    for (int n : batch) {
      total += n;
    }
    return arolla::MakeNamedTuple({"total", "yields"},
                                  {arolla::TypedRef::FromValue(total),
                                   arolla::TypedRef::FromValue(total)});
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1, 2, 3}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields),
          arolla::MakeNamedTuple({"total"}, {arolla::TypedRef::FromValue(10)})
              ->AsRef()));
  EXPECT_THAT(ReadAllInts(stream),
              IsOkAndHolds(ElementsAre(kInitialYields, 16)));
}

TEST(StreamBatchedCascadeTest, EmptyInput) {
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({}), BatchCountDownStep, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(ReadAllInts(stream), IsOkAndHolds(ElementsAre(kInitialYields)));
}

TEST(StreamBatchedCascadeTest, PendingSources) {
  // NOTE: With the eager executor, the routine runs synchronously as soon as
  // any of the sources changes its state.
  auto [input_stream, input_writer] = MakeStream(arolla::GetQType<int>());
  input_writer->Write(arolla::TypedRef::FromValue(1));
  input_writer->Write(arolla::TypedRef::FromValue(2));
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), input_stream, BatchCountDownStep, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  auto reader = stream->MakeReader();
  // Batches: [1, 2], [0, 1], [0].
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<int>(kInitialYields)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(2003)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(2001)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(1000)));
  EXPECT_TRUE(reader->TryRead().empty());
  input_writer->Write(arolla::TypedRef::FromValue(5));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(1005)));
  std::move(*input_writer).Close();
  EXPECT_THAT(ReadAllInts(stream),
              IsOkAndHolds(ElementsAre(kInitialYields, 2003, 2001, 1000, 1005,
                                       1004, 1003, 1002, 1001, 1000)));
}

TEST(StreamBatchedCascadeTest, InputStreamError) {
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(),
          MakeIntStream({0, 0}, absl::InvalidArgumentError("Boom!")),
          BatchCountDownStep, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<int>(kInitialYields)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kInvalidArgument, "Boom!")));
}

TEST(StreamBatchedCascadeTest, StepFunctorError) {
  auto step_fn = [](absl::Span<const arolla::TypedRef> /*args*/,
                    absl::Span<const std::string> /*kwnames*/)
      -> absl::StatusOr<arolla::TypedValue> {
    return absl::InvalidArgumentError("Boom!");
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(ReadAllInts(stream),
              StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(StreamBatchedCascadeTest, ReservedStateNames) {
  for (const char* name : {"yields", "cascades"}) {
    SCOPED_TRACE(name);
    EXPECT_THAT(
        StreamBatchedCascade(
            GetEagerExecutor(), MakeIntStream({}), BatchCountDownStep, "yields",
            arolla::TypedRef::FromValue(kInitialYields),
            arolla::MakeNamedTuple({name}, {arolla::TypedRef::FromValue(0)})
                ->AsRef()),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrCat("`", name,
                              "` cannot be used as a state variable name")));
  }
}

TEST(StreamBatchedCascadeTest, CascadesAsYieldsParamName) {
  EXPECT_THAT(StreamBatchedCascade(GetEagerExecutor(), MakeIntStream({}),
                                   BatchCountDownStep, "cascades",
                                   arolla::TypedRef::FromValue(kInitialYields),
                                   EmptyState().AsRef()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "`cascades` cannot be used as the yields parameter "
                       "name"));
}

TEST(StreamBatchedCascadeTest, NonNamedTupleState) {
  EXPECT_THAT(
      StreamBatchedCascade(GetEagerExecutor(), MakeIntStream({}),
                           BatchCountDownStep, "yields",
                           arolla::TypedRef::FromValue(kInitialYields),
                           arolla::TypedRef::FromValue(0)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected the initial state to be a namedtuple, got INT32"));
}

TEST(StreamBatchedCascadeTest, StepFunctorUnexpectedVariable) {
  auto step_fn = [](absl::Span<const arolla::TypedRef> /*args*/,
                    absl::Span<const std::string> /*kwnames*/) {
    return arolla::MakeNamedTuple({"foo"}, {arolla::TypedRef::FromValue(0)});
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(
      ReadAllInts(stream),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "unexpected variable 'foo'; the step functor must return a "
               "namedtuple with a subset of initial variables, 'yields', and "
               "'cascades'"));
}

TEST(StreamBatchedCascadeTest, StepFunctorWrongCascadesType) {
  auto step_fn = [](absl::Span<const arolla::TypedRef> /*args*/,
                    absl::Span<const std::string> /*kwnames*/) {
    auto [cascades, writer] = MakeStream(arolla::GetQType<float>());
    std::move(*writer).Close();
    return arolla::MakeNamedTuple({"cascades"},
                                  {MakeStreamQValueRef(cascades)});
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(
      ReadAllInts(stream),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "variable 'cascades' has type STREAM[INT32], but the provided "
               "value has type STREAM[FLOAT32]; the step functor must return a "
               "namedtuple with a subset of initial variables, 'yields', and "
               "'cascades'"));
}

TEST(StreamBatchedCascadeTest, CascadeStreamError) {
  auto step_fn = [](absl::Span<const arolla::TypedRef> /*args*/,
                    absl::Span<const std::string> /*kwnames*/) {
    StreamPtr cascades = MakeIntStream({}, absl::InvalidArgumentError("Boom!"));
    return arolla::MakeNamedTuple({"cascades"},
                                  {MakeStreamQValueRef(cascades)});
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), MakeIntStream({1}), step_fn, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  EXPECT_THAT(ReadAllInts(stream),
              StatusIs(absl::StatusCode::kInvalidArgument, "Boom!"));
}

TEST(StreamBatchedCascadeTest, OrphanedOutputStream) {
  auto [input_stream, input_writer] = MakeStream(arolla::GetQType<int>());
  int call_count = 0;
  auto step_fn = [&call_count](absl::Span<const arolla::TypedRef> /*args*/,
                               absl::Span<const std::string> /*kwnames*/) {
    call_count += 1;
    return MakeEmptyNamedTuple();
  };
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(GetEagerExecutor(), input_stream, step_fn, "yields",
                           arolla::TypedRef::FromValue(kInitialYields),
                           EmptyState().AsRef()));
  input_writer->Write(arolla::TypedRef::FromValue(1));
  EXPECT_EQ(call_count, 1);
  stream.reset();
  input_writer->Write(arolla::TypedRef::FromValue(2));
  input_writer->Write(arolla::TypedRef::FromValue(3));
  std::move(*input_writer).Close();
  EXPECT_EQ(call_count, 1);
}

TEST(StreamBatchedCascadeTest, SeveralReadySources) {
  // The first three steps return the open cascade streams `a`, `b`, and `c`;
  // the fourth step writes to all of them, so they become ready at once. Then
  // the routine reads `a` until it's exhausted, `b` until it has no more items,
  // and `c`, collecting all the items into a single batch.
  StreamPtr a, b, c;
  StreamWriterPtr a_writer, b_writer, c_writer;
  std::tie(a, a_writer) = MakeStream(arolla::GetQType<int>());
  std::tie(b, b_writer) = MakeStream(arolla::GetQType<int>());
  std::tie(c, c_writer) = MakeStream(arolla::GetQType<int>());
  std::vector<std::vector<int>> batches;
  auto step_fn = [&](absl::Span<const arolla::TypedRef> args,
                     absl::Span<const std::string> /*kwnames*/)
      -> absl::StatusOr<arolla::TypedValue> {
    ASSIGN_OR_RETURN(std::vector<int> batch,
                     ReadAllInts(args[0].UnsafeAs<StreamPtr>()));
    batches.push_back(std::move(batch));
    switch (batches.size()) {
      case 1:
        return arolla::MakeNamedTuple({"cascades"}, {MakeStreamQValueRef(a)});
      case 2:
        return arolla::MakeNamedTuple({"cascades"}, {MakeStreamQValueRef(b)});
      case 3:
        return arolla::MakeNamedTuple({"cascades"}, {MakeStreamQValueRef(c)});
      case 4:
        a_writer->Write(arolla::TypedRef::FromValue(10));
        std::move(*a_writer).Close();
        b_writer->Write(arolla::TypedRef::FromValue(20));
        c_writer->Write(arolla::TypedRef::FromValue(30));
        break;
    }
    return MakeEmptyNamedTuple();
  };
  auto [input_stream, input_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(GetEagerExecutor(), input_stream, step_fn, "yields",
                           arolla::TypedRef::FromValue(kInitialYields),
                           EmptyState().AsRef()));
  for (int i = 1; i <= 4; ++i) {
    input_writer->Write(arolla::TypedRef::FromValue(i));
  }
  EXPECT_THAT(batches,
              ElementsAre(ElementsAre(1), ElementsAre(2), ElementsAre(3),
                          ElementsAre(4), ElementsAre(10, 20, 30)));
  std::move(*input_writer).Close();
  std::move(*b_writer).Close();
  std::move(*c_writer).Close();
  EXPECT_THAT(ReadAllInts(stream), IsOkAndHolds(ElementsAre(kInitialYields)));
}

TEST(StreamBatchedCascadeTest, Cancellation) {
  arolla::CancellationContext::ScopeGuard cancellation_scope;
  auto [input_stream, input_writer] = MakeStream(arolla::GetQType<int>());
  ASSERT_OK_AND_ASSIGN(
      StreamPtr stream,
      StreamBatchedCascade(
          GetEagerExecutor(), input_stream, BatchCountDownStep, "yields",
          arolla::TypedRef::FromValue(kInitialYields), EmptyState().AsRef()));
  // Batches: [1], [0].
  input_writer->Write(arolla::TypedRef::FromValue(1));
  cancellation_scope.cancellation_context()->Cancel(
      absl::CancelledError("Boom!"));
  auto reader = stream->MakeReader();
  EXPECT_THAT(reader->TryRead().item(),
              Pointee(QValueWith<int>(kInitialYields)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(1001)));
  EXPECT_THAT(reader->TryRead().item(), Pointee(QValueWith<int>(1000)));
  EXPECT_THAT(reader->TryRead().close_status(),
              Pointee(StatusIs(absl::StatusCode::kCancelled, "Boom!")));
}

}  // namespace
}  // namespace koladata::contrib
