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
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
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
using ::koladata::functor::parallel::StreamReaderPtr;
using ::koladata::functor::parallel::StreamWriterPtr;
using ::koladata::functor::parallel::stream_loop_internal::Vars;

constexpr absl::string_view kCascades = "cascades";

// Interleaves the items from a dynamic set of streams into batches.
class InterleaveMixer : public std::enable_shared_from_this<InterleaveMixer> {
 public:
  struct NextBatchResult {
    StreamPtr absl_nullable batch;
    StreamReaderPtr absl_nullable ready_event;
  };

  explicit InterleaveMixer(size_t max_active_cascades)
      : max_active_cascades_(max_active_cascades == 0
                                 ? std::numeric_limits<size_t>::max()
                                 : max_active_cascades) {}

  // Returns:
  //  * `{batch, nullptr}` if a batch of items is ready to be scheduled;
  //  * `{nullptr, ready_event}` if cascades are still being read, where
  //    `ready_event` becomes ready (i.e. gets closed) once a batch is ready to
  //    be scheduled or all cascades are fully read;
  //  * `{nullptr, nullptr}` if all cascades are fully read;
  //  * an error if any cascade stream fails.
  absl::StatusOr<NextBatchResult> NextBatch() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(mutex_);
    if (!error_.ok()) {
      return error_;
    }
    if (batch_writer_ != nullptr && active_cascades_ < max_active_cascades_) {
      std::move(*batch_writer_).Close();
      batch_writer_ = nullptr;
      return NextBatchResult{.batch = std::move(batch_)};
    }
    if (active_cascades_ == 0) {
      return NextBatchResult{};
    }
    DCHECK(ready_event_writer_ == nullptr);
    StreamPtr ready_event_stream;
    std::tie(ready_event_stream, ready_event_writer_) =
        MakeStream(arolla::GetNothingQType(), /*initial_capacity=*/0);
    return NextBatchResult{.ready_event = ready_event_stream->MakeReader()};
  }

  // Adds an input stream reader.
  void AddReader(StreamReaderPtr absl_nonnull reader)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    ProcessInput(std::move(reader), /*add_reader=*/true);
  }

 private:
  // Returns a writer that should be released to set the ready event.
  [[nodiscard]] StreamWriterPtr absl_nullable AcquireNotifierIfReady()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    if (
        // an error occurred
        !error_.ok() ||
        // all cascades are fully read
        active_cascades_ == 0 ||
        // a non-empty batch is ready
        (batch_writer_ != nullptr && active_cascades_ < max_active_cascades_)) {
      return std::move(ready_event_writer_);
    }
    return nullptr;
  }

  void ProcessInput(StreamReaderPtr absl_nonnull reader, bool add_reader)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    StreamWriterPtr event_notifier;  // released outside of the lock
    {
      absl::MutexLock lock(mutex_);
      if (!error_.ok()) {
        return;
      }
      if (add_reader) {
        ++active_cascades_;
      }
      auto try_read_result = reader->TryRead();
      if (auto* item = try_read_result.item(); item != nullptr) {
        if (batch_writer_ == nullptr) {
          std::tie(batch_, batch_writer_) = MakeStream(item->GetType());
          DCHECK(batch_writer_ != nullptr);
        }
        do {
          batch_writer_->Write(*item);
          try_read_result = reader->TryRead();
        } while ((item = try_read_result.item()) != nullptr);
      }
      if (auto* status = try_read_result.close_status()) {
        --active_cascades_;
        if (!status->ok()) {
          error_ = std::move(*status);
        }
        event_notifier = AcquireNotifierIfReady();
        return;
      }
      event_notifier = AcquireNotifierIfReady();
    }
    auto* reader_ptr = reader.get();
    reader_ptr->SubscribeOnce(
        [weak_self = weak_from_this(), reader = std::move(reader)]() mutable {
          if (auto self = weak_self.lock()) {
            self->ProcessInput(std::move(reader), /*add_reader=*/false);
          }
        });
  }

  const size_t max_active_cascades_;
  absl::Mutex mutex_;
  size_t active_cascades_ ABSL_GUARDED_BY(mutex_) = 0;
  absl::Status error_ ABSL_GUARDED_BY(mutex_);
  StreamPtr absl_nullable batch_ ABSL_GUARDED_BY(mutex_);
  StreamWriterPtr absl_nullable batch_writer_ ABSL_GUARDED_BY(mutex_);
  StreamWriterPtr absl_nullable ready_event_writer_ ABSL_GUARDED_BY(mutex_);
};

class StreamBatchedCascadeHooks final : public BasicRoutineHooks {
 public:
  StreamBatchedCascadeHooks(StreamReaderPtr absl_nonnull input_reader,
                            StreamWriterPtr absl_nonnull writer,
                            StreamCascadeFunctor absl_nonnull step_functor,
                            Vars vars, size_t max_active_cascades)
      : writer_(std::move(writer)),
        step_functor_(std::move(step_functor)),
        vars_(std::move(vars)),
        initial_yields_(vars_.values().rbegin()[1]),
        initial_cascades_(vars_.values().rbegin()[0]),
        mixer_(std::make_shared<InterleaveMixer>(max_active_cascades)) {
    DCHECK_GE(vars_.kwnames().size(), size_t{2});
    DCHECK_EQ(vars_.values().size(), vars_.kwnames().size() + 1);
    writer_->Write(initial_yields_);
    mixer_->AddReader(std::move(input_reader));
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final {
    writer_->TryClose(std::move(status));
  }

  StreamReaderPtr absl_nullable Start() final { return Run(); }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull /*reader*/) final {
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

  // Takes batches of ready items from the mixer and passes them to the step
  // functor.
  StreamReaderPtr absl_nullable Run() {
    while (!Interrupted()) {
      ASSIGN_OR_RETURN((auto [batch, ready_event]), mixer_->NextBatch(),
                       OnError(std::move(_)));
      if (batch != nullptr) {
        RETURN_IF_ERROR(CallStepFunctor(std::move(batch))).With(OnError());
        continue;
      }
      if (ready_event == nullptr) {
        writer_->TryClose(absl::OkStatus());
      }
      return std::move(ready_event);
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
      mixer_->AddReader(cascades.UnsafeAs<StreamPtr>()->MakeReader());
      cascades = initial_cascades_;
    }
    return absl::OkStatus();
  }

  const StreamWriterPtr absl_nonnull writer_;
  const StreamCascadeFunctor absl_nonnull step_functor_;
  Vars vars_;
  const arolla::TypedRef initial_yields_;
  const arolla::TypedRef initial_cascades_;
  const std::shared_ptr<InterleaveMixer> absl_nonnull mixer_;
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamBatchedCascade(
    ExecutorPtr absl_nonnull executor,
    const StreamPtr absl_nonnull& input_stream,
    StreamCascadeFunctor absl_nonnull step_functor,
    absl::string_view yields_param_name, arolla::TypedRef initial_yields,
    arolla::TypedRef initial_state, size_t max_active_cascades) {
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
          Vars(std::move(initial_var_values), std::move(var_names)),
          max_active_cascades));
  return std::move(result);
}

}  // namespace koladata::contrib
