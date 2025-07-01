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

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/derived_qtype/labeled_qtype.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

constexpr absl::string_view kAwaitLabel = "AWAIT";

using Functor = absl::AnyInvocable<  // clang-format hint
    absl::StatusOr<arolla::TypedValue>(
        absl::Span<const arolla::TypedRef> args) &&>;

class StreamCallHooks final : public BasicRoutineHooks {
 public:
  StreamCallHooks(StreamWriterPtr absl_nonnull writer,
                  Functor /*nonnull*/ functor,
                  absl::Span<const arolla::TypedRef> args)
      : return_value_type_(writer->value_qtype()),
        functor_(std::move(functor)),
        tuple_args_(arolla::MakeTuple(args)),
        args_size_(args.size()),
        composition_(std::move(writer)) {
    args_.reserve(args_size_);
  }

  bool Interrupted() const final { return composition_.Orphaned(); }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr absl_nullable Start() final { return Run(); }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull reader) final {
    ASSIGN_OR_RETURN(reader, ProcessAwaitedArg(std::move(reader)),
                     OnError(std::move(_)));
    if (reader != nullptr) {  // NOLINT: Can be null after ProcessAwaitedArg().
      return reader;
    }
    return Run();
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    composition_.AddError(std::move(status));
    return nullptr;
  }

  StreamReaderPtr absl_nullable Run() {
    while (args_.size() < args_size_) {
      if (Interrupted()) {
        return nullptr;
      }
      arolla::TypedRef arg = tuple_args_.GetField(args_.size());
      if (arolla::GetQTypeLabel(arg.GetType()) != kAwaitLabel) {
        args_.push_back(arg);
        continue;
      }
      arg = arolla::DecayDerivedQValue(arg);
      if (!IsStreamQType(arg.GetType())) {
        args_.push_back(arg);
        continue;
      }
      awaited_arg_index_ = args_.size();
      ASSIGN_OR_RETURN(
          auto reader,
          ProcessAwaitedArg(arg.UnsafeAs<StreamPtr>()->MakeReader()),
          OnError(std::move(_)));
      if (reader != nullptr) {
        return reader;
      }
    }
    if (Interrupted()) {
      return nullptr;
    }
    ASSIGN_OR_RETURN(auto result, std::move(functor_)(args_),
                     OnError(std::move(_)));
    if (result.GetType() == return_value_type_) {
      composition_.AddItem(result.AsRef());
      return nullptr;
    }
    if (result.GetType() == GetStreamQType(return_value_type_)) {
      composition_.Add(result.UnsafeAs<StreamPtr>());
      return nullptr;
    }
    return OnError(absl::InvalidArgumentError(absl::StrFormat(
        "expected the functor to return %s or %s, got %s",
        return_value_type_->name(), GetStreamQType(return_value_type_)->name(),
        result.GetType()->name())));
  }

  // Handles a single "awaited" argument. The stream is expected to yield
  // exactly one item; however, it may involve multiple waitings.
  absl::StatusOr<StreamReaderPtr absl_nullable> ProcessAwaitedArg(
      StreamReaderPtr absl_nonnull reader) {
    // TODO: In the case of an error, could we identify the parameter in
    // the error message somehow?
    auto try_read_result = reader->TryRead();
    if (awaited_arg_index_ == args_.size()) {
      if (auto* item = try_read_result.item()) {
        args_.push_back(*item);
        try_read_result = reader->TryRead();
      } else if (auto* status = try_read_result.close_status()) {
        if (status->ok()) {
          return absl::InvalidArgumentError(
              "expected a stream with a single item, got an empty stream");
        }
        return std::move(*status);
      } else {
        return reader;
      }
    }
    if (try_read_result.item() != nullptr) {
      return absl::InvalidArgumentError(
          "expected a stream with a single item, got a stream with "
          "multiple items");
    } else if (auto* status = try_read_result.close_status()) {
      if (status->ok()) {
        return nullptr;
      }
      return std::move(*status);
    } else {
      return reader;
    }
  }

  const arolla::QTypePtr absl_nonnull return_value_type_;
  Functor /*nonnull*/ functor_;
  const arolla::TypedValue tuple_args_;
  const size_t args_size_;
  size_t awaited_arg_index_;
  std::vector<arolla::TypedRef> args_;
  StreamInterleave composition_;
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamCall(
    ExecutorPtr absl_nonnull executor, Functor /*nonnull*/ functor,
    arolla::QTypePtr absl_nonnull return_value_qtype,
    absl::Span<const arolla::TypedRef> args) {
  DCHECK(functor != nullptr);
  auto [result, writer] = MakeStream(return_value_qtype, 1);
  StartBasicRoutine(std::move(executor),
                    std::make_unique<StreamCallHooks>(
                        std::move(writer), std::move(functor), args));
  return std::move(result);
}

arolla::TypedRef MakeStreamCallAwaitArg(arolla::TypedRef arg) {
  if (!IsStreamQType(arg.GetType())) {
    return arg;
  }
  return arolla::UnsafeDowncastDerivedQValue(
      arolla::GetLabeledQType(arg.GetType(), kAwaitLabel), arg);
}

}  // namespace koladata::functor::parallel
