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

#include <cstddef>
#include <list>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_loop_internal.h"
#include "koladata/functor/parallel/stream_qtype.h"

namespace koladata::contrib {
namespace {

using ::koladata::functor::parallel::BasicRoutineHooks;
using ::koladata::functor::parallel::ExecutorPtr;
using ::koladata::functor::parallel::MakeStream;
using ::koladata::functor::parallel::MakeStreamQValue;
using ::koladata::functor::parallel::MakeStreamQValueRef;
using ::koladata::functor::parallel::StartBasicRoutine;
using ::koladata::functor::parallel::StreamPtr;
using ::koladata::functor::parallel::StreamReader;
using ::koladata::functor::parallel::StreamReaderPtr;
using ::koladata::functor::parallel::StreamWriterPtr;
using ::koladata::functor::parallel::stream_loop_internal::Vars;

constexpr absl::string_view kCascades = "cascades";

// Interleaves the items from a dynamic set of streams.
//
// TODO: Try using `koladata::functor::parallel::StreamInterleave`
// here, to simplify the implementation.
class InterleaveMixer : public std::enable_shared_from_this<InterleaveMixer> {
  struct PrivateConstructorTag {};

 public:
  // Factory function.
  static std::shared_ptr<InterleaveMixer> absl_nonnull Make() {
    return std::make_shared<InterleaveMixer>(PrivateConstructorTag());
  }

  // Private constructor.
  explicit InterleaveMixer(PrivateConstructorTag) {
    auto [ready_event_stream, ready_event_writer] =
        MakeStream(arolla::GetNothingQType(), /*initial_capacity=*/0);
    ready_event_stream_ = std::move(ready_event_stream);
    ready_event_writer_ = std::move(ready_event_writer);
  }

  // Returns the next item from the input streams. Returns an empty result if
  // no reader is ready, absl::OkStatus() if there are no more input streams
  // left, or an error if a reader fails (the error is returned by all
  // subsequent calls).
  //
  // NOTE: The returned item stays valid only until the next call. This method
  // is not thread-safe.
  StreamReader::TryReadResult TryRead() {
    if (active_reader_ == nullptr) [[unlikely]] {
      absl::MutexLock lock(mutex_);
      if (!ready_readers_.empty()) {
        active_reader_ = ready_readers_.front().get();
      } else if (!waiting_readers_.empty()) {
        return {};
      } else {
        return {absl::OkStatus()};
      }
    }

    for (;;) {
      DCHECK(active_reader_ != nullptr);

      auto try_read_result = active_reader_->TryRead();
      if (try_read_result.item() != nullptr) {
        return try_read_result;
      }

      if (auto* status = try_read_result.close_status()) {
        if (!status->ok()) {
          return try_read_result;
        }
        // `active_reader_` is fully exhausted, remove it from the list, and
        // consider the next ready reader.
        absl::MutexLock lock(mutex_);
        ready_readers_.pop_front();
        if (!ready_readers_.empty()) {
          active_reader_ = ready_readers_.front().get();
          continue;
        } else if (!waiting_readers_.empty()) {
          ResetReadyEvent();
          active_reader_ = nullptr;
          return {};
        } else {
          active_reader_ = nullptr;
          return {absl::OkStatus()};
        }
      }

      // The active reader has no new items.
      DCHECK(try_read_result.empty());
      ReaderList::iterator waiting_it;
      const auto guard = absl::Cleanup([&] { SubscribeForReady(waiting_it); });
      absl::MutexLock lock(mutex_);
      waiting_it = ready_readers_.begin();
      waiting_readers_.splice(waiting_readers_.end(), ready_readers_,
                              waiting_it);
      if (ready_readers_.empty()) {
        ResetReadyEvent();
        active_reader_ = nullptr;
        return {};
      } else {
        active_reader_ = ready_readers_.front().get();
      }
    }
  }

  // Returns a reader that becomes ready (i.e. gets closed) once one of the
  // waiting readers becomes ready (has a new item or gets closed). Intended to
  // be used after `TryRead()` returns an empty result.
  StreamReaderPtr absl_nonnull MakeReadyEvent() {
    absl::MutexLock lock(mutex_);
    return ready_event_stream_->MakeReader();
  }

  // Adds an input stream reader.
  void AddStreamReader(StreamReaderPtr absl_nonnull reader) {
    ReaderList::iterator waiting_it;
    const auto guard = absl::Cleanup([&] { SubscribeForReady(waiting_it); });
    {
      absl::MutexLock lock(mutex_);
      if (ready_readers_.empty() && waiting_readers_.empty()) {
        // Previously, there was no event to wait for, but now there is one.
        ResetReadyEvent();
      }
      waiting_it =
          waiting_readers_.insert(waiting_readers_.end(), std::move(reader));
    }
  }

 private:
  using ReaderList = std::list<StreamReaderPtr>;

  void ResetReadyEvent() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    if (ready_event_writer_ == nullptr) {
      std::tie(ready_event_stream_, ready_event_writer_) =
          MakeStream(arolla::GetNothingQType(), /*initial_capacity=*/0);
    }
  }

  void SubscribeForReady(ReaderList::iterator waiting_it)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    (**waiting_it).SubscribeOnce([weak_self = weak_from_this(), waiting_it] {
      if (auto self = weak_self.lock()) {
        StreamWriterPtr wake_writer;
        // NOTE: `cleanup` is declared before `lock`, so that the event gets
        // closed without holding the lock; closing the event may
        // synchronously resume the consumer.
        auto cleanup = absl::Cleanup([&] {
          if (wake_writer != nullptr) {
            wake_writer->TryClose(absl::OkStatus());
          }
        });
        absl::MutexLock lock(self->mutex_);
        self->ready_readers_.splice(self->ready_readers_.end(),
                                    self->waiting_readers_, waiting_it);
        wake_writer = std::move(self->ready_event_writer_);
      }
    });
  }

  absl::Mutex mutex_;
  ReaderList ready_readers_ ABSL_GUARDED_BY(mutex_);
  ReaderList waiting_readers_ ABSL_GUARDED_BY(mutex_);

  StreamPtr absl_nonnull ready_event_stream_ ABSL_GUARDED_BY(mutex_);
  StreamWriterPtr absl_nullable ready_event_writer_ ABSL_GUARDED_BY(mutex_);

  // If non-null, points to `ready_readers_.front()`.
  //
  // This field does not require synchronization because:
  //  * it is only accessed in `TryRead()`, which disallows concurrency;
  //  * only `TryRead()` removes items from `ready_readers_`;
  //  * while other threads can concurrently append items to `ready_readers_`,
  //    `std::list` preserves the order of the front items.
  StreamReader* absl_nullable active_reader_ = nullptr;
};

class StreamBatchedCascadeHooks final : public BasicRoutineHooks {
 public:
  StreamBatchedCascadeHooks(StreamReaderPtr absl_nonnull input_reader,
                            StreamWriterPtr absl_nonnull writer,
                            StreamCascadeFunctor absl_nonnull step_functor,
                            Vars vars)
      : writer_(std::move(writer)),
        step_functor_(std::move(step_functor)),
        vars_(std::move(vars)),
        initial_yields_(vars_.values().rbegin()[1]),
        initial_cascades_(vars_.values().rbegin()[0]) {
    DCHECK_GE(vars_.kwnames().size(), size_t{2});
    DCHECK_EQ(vars_.values().size(), vars_.kwnames().size() + 1);
    writer_->Write(initial_yields_);
    mixer_->AddStreamReader(std::move(input_reader));
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final {
    writer_->TryClose(std::move(status));
  }

  StreamReaderPtr absl_nullable Start() final { return Run(); }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull /*reader*/) final {
    // NOTE: The reader comes from `InterleaveMixer::MakeReadyEvent()`, and it
    // only indicates that some sources have become ready.
    return Run();
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    writer_->TryClose(std::move(status));
    return nullptr;
  }

  auto OnError() {
    return [this](absl::Status&& status) { return OnError(std::move(status)); };
  }

  // Takes all the items that are ready and passes them to the step functor at
  // once, as a (closed) stream.
  StreamReaderPtr absl_nullable Run() {
    StreamPtr batch;
    StreamWriterPtr batch_writer;
    while (!Interrupted()) {
      auto try_read_result = mixer_->TryRead();
      if (auto* item = try_read_result.item()) {
        // The `item` stays valid until the next `TryRead()` call, so we copy
        // it to the batch right away.
        if (batch_writer == nullptr) {
          std::tie(batch, batch_writer) = MakeStream(item->GetType());
        }
        batch_writer->Write(*item);
        continue;
      }
      if (auto* status = try_read_result.close_status()) {
        if (!status->ok() || batch_writer == nullptr) {
          writer_->TryClose(std::move(*status));
          return nullptr;  // done
        }
      }
      if (batch_writer == nullptr) {
        return mixer_->MakeReadyEvent();  // nothing to read yet
      }
      std::move(*batch_writer).Close();
      batch_writer = nullptr;
      DCHECK(batch != nullptr);
      RETURN_IF_ERROR(CallStepFunctor(std::move(batch))).With(OnError());
    }
    return nullptr;
  }

  absl::Status CallStepFunctor(StreamPtr absl_nonnull batch) {
    // The trailing yields and `cascades` variables are not passed to the step
    // functor.
    const size_t arg_count = vars_.values().size() - 2;
    const size_t kwarg_count = vars_.kwnames().size() - 2;
    vars_.mutable_values().front() = MakeStreamQValueRef(batch);
    absl::StatusOr<arolla::TypedValue> update =
        step_functor_(vars_.values().subspan(0, arg_count),
                      vars_.kwnames().subspan(0, kwarg_count));
    // Don't retain the reference to the batch, so that it can be released.
    vars_.mutable_values().front() = arolla::GetUnspecifiedQValue().AsRef();
    RETURN_IF_ERROR(update.status());
    RETURN_IF_ERROR(vars_.Update(*std::move(update)))
        << "the step functor must return a namedtuple with a subset of initial "
           "variables, '"
        << vars_.kwnames()[kwarg_count] << "', and 'cascades'";

    // NOTE: We use the initial values of the yields and `cascades` variables as
    // sentinels. This is safe because `vars_` owns the initial values,
    // preventing their addresses from being reused.
    //
    // Write the yields to the resulting stream.
    arolla::TypedRef& yields = vars_.mutable_values().rbegin()[1];
    if (yields.GetRawPointer() != initial_yields_.GetRawPointer()) {
      (void)writer_->TryWrite(yields);
      yields = initial_yields_;
    }
    // Add the cascades to the input streams.
    arolla::TypedRef& cascades = vars_.mutable_values().rbegin()[0];
    if (cascades.GetRawPointer() != initial_cascades_.GetRawPointer()) {
      mixer_->AddStreamReader(cascades.UnsafeAs<StreamPtr>()->MakeReader());
      cascades = initial_cascades_;
    }
    return absl::OkStatus();
  }

  const StreamWriterPtr absl_nonnull writer_;
  const StreamCascadeFunctor absl_nonnull step_functor_;
  Vars vars_;
  const arolla::TypedRef initial_yields_;
  const arolla::TypedRef initial_cascades_;
  const std::shared_ptr<InterleaveMixer> absl_nonnull mixer_ =
      InterleaveMixer::Make();
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamBatchedCascade(
    ExecutorPtr absl_nonnull executor,
    const StreamPtr absl_nonnull& input_stream,
    StreamCascadeFunctor absl_nonnull step_functor,
    absl::string_view yields_param_name, arolla::TypedRef initial_yields,
    arolla::TypedRef initial_state) {
  DCHECK(step_functor != nullptr);
  if (yields_param_name == kCascades) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "`%s` cannot be used as the yields parameter name", kCascades));
  }
  if (!arolla::IsNamedTupleQType(initial_state.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected the initial state to be a namedtuple, got %s",
                        initial_state.GetType()->name()));
  }
  for (absl::string_view reserved_name : {yields_param_name, kCascades}) {
    if (arolla::GetFieldIndexByName(initial_state.GetType(), reserved_name)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "`%s` cannot be used as a state variable name", reserved_name));
    }
  }
  const auto field_names = arolla::GetFieldNames(initial_state.GetType());
  std::vector<std::string> var_names;
  var_names.reserve(field_names.size() + 2);
  var_names.insert(var_names.end(), field_names.begin(), field_names.end());
  var_names.emplace_back(yields_param_name);
  var_names.emplace_back(kCascades);
  // An empty stream, used as the initial (sentinel) value of the `cascades`
  // variable.
  auto [empty_stream, empty_stream_writer] =
      MakeStream(input_stream->value_qtype());
  std::move(*empty_stream_writer).Close();
  const arolla::TypedValue initial_cascades =
      MakeStreamQValue(std::move(empty_stream));
  std::vector<arolla::TypedRef> initial_var_values;
  initial_var_values.reserve(1 + field_names.size() + 2);
  // A placeholder value for the batch.
  initial_var_values.emplace_back(arolla::GetUnspecifiedQValue().AsRef());
  for (size_t i = 0; i < field_names.size(); ++i) {
    initial_var_values.emplace_back(initial_state.GetField(i));
  }
  initial_var_values.emplace_back(initial_yields);
  initial_var_values.emplace_back(initial_cascades.AsRef());
  auto [result, writer] = MakeStream(initial_yields.GetType());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamBatchedCascadeHooks>(
          input_stream->MakeReader(), std::move(writer),
          std::move(step_functor),
          Vars(std::move(initial_var_values), std::move(var_names))));
  return std::move(result);
}

}  // namespace koladata::contrib
