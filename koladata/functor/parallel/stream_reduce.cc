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
#include "koladata/functor/parallel/stream_reduce.h"

#include <cstddef>
#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

using Functor = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
    arolla::TypedRef, arolla::TypedRef) const>;

class StreamReduceHooks final : public BasicRoutineHooks {
 public:
  StreamReduceHooks(StreamWriterPtr /*absl_nonnull*/ writer,
                    arolla::TypedValue initial_value,
                    StreamPtr /*absl_nonnull*/ input_stream, Functor functor)
      : writer_(std::move(writer)),
        input_stream_(std::move(input_stream)),
        functor_(std::move(functor)),
        value_(std::move(initial_value)) {
    DCHECK_EQ(value_.GetType(), writer_->value_qtype());
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr /*absl_nullable*/ Start() final {
    return Resume(input_stream_->MakeReader());
  }

  StreamReaderPtr /*absl_nullable*/ Resume(  // clang-format hint
      StreamReaderPtr /*absl_nonnull*/ reader) final {
    auto try_read_result = reader->TryRead();
    while (arolla::TypedRef* item = try_read_result.item()) {
      if (Interrupted()) {
        return nullptr;
      }
      ASSIGN_OR_RETURN(value_, functor_(value_.AsRef(), *item),
                       OnError(std::move(_)));
      if (value_.GetType() != writer_->value_qtype()) {
        return OnError(absl::InvalidArgumentError(absl::StrFormat(
            "functor returned a value of the wrong type: expected %s, got %s",
            writer_->value_qtype()->name(), value_.GetType()->name())));
      }
      try_read_result = reader->TryRead();
    }
    if (absl::Status* status = try_read_result.close_status()) {
      if (!status->ok() || writer_->TryWrite(value_.AsRef())) {
        writer_->TryClose(std::move(*status));
      }
      return nullptr;
    }
    return reader;
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    writer_->TryClose(std::move(status));
    return nullptr;
  }

  const StreamWriterPtr /*absl_nonnull*/ writer_;
  const StreamPtr /*absl_nonnull*/ input_stream_;
  const Functor functor_;
  arolla::TypedValue value_;
};

}  // namespace

StreamPtr /*absl_nonnull*/ StreamReduce(ExecutorPtr /*absl_nonnull*/ executor,
                                    arolla::TypedValue initial_value,
                                    StreamPtr /*absl_nonnull*/ input_stream,
                                    Functor functor) {
  DCHECK(functor != nullptr);
  auto [result, writer] = MakeStream(initial_value.GetType(), 1);
  StartBasicRoutine(std::move(executor),
                    std::make_unique<StreamReduceHooks>(
                        std::move(writer), std::move(initial_value),
                        std::move(input_stream), std::move(functor)));
  return std::move(result);
}

}  // namespace koladata::functor::parallel
