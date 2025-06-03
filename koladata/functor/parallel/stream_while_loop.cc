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

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/basic_routine.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_composition.h"
#include "koladata/functor/parallel/stream_loop_internal.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

constexpr absl::string_view kReturns = "returns";
constexpr absl::string_view kYieldsChained = "yields";
constexpr absl::string_view kYieldsInterleaved = "yields_interleaved";

using ::koladata::functor::parallel::stream_loop_internal::ParsedLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::ParseLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::
    ParseLoopConditionStream;
using ::koladata::functor::parallel::stream_loop_internal::Vars;

// A generic implementation of while loop hooks, which further specialized using
// a traits class.
template <class Traits>
class StreamWhileLoopHooks final : public BasicRoutineHooks {
 public:
  template <typename... Args>
  explicit StreamWhileLoopHooks(Args&&... args)
      : traits_(std::forward<Args>(args)...) {}

  bool Interrupted() const final { return traits_.Interrupted(); }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr /*absl_nullable*/ Start() final {
    ASSIGN_OR_RETURN(auto parsed_condition, CallLoopCondition(),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

  StreamReaderPtr /*absl_nullable*/ Resume(  // clang-format hint
      StreamReaderPtr /*absl_nonnull*/ condition) final {
    ASSIGN_OR_RETURN(auto parsed_condition,
                     ParseLoopConditionStream(std::move(condition)),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    traits_.OnError(std::move(status));
    return nullptr;
  }

  auto OnError() {
    return [this](absl::Status&& status) { return OnError(std::move(status)); };
  }

  StreamReaderPtr /*absl_nullable*/ Run(ParsedLoopCondition parsed_condition) {
    while (parsed_condition.value) {
      RETURN_IF_ERROR(CallLoopBody()).With(OnError());
      ASSIGN_OR_RETURN(parsed_condition, CallLoopCondition(),
                       OnError(std::move(_)));
    }
    if (parsed_condition.reader != nullptr) {
      return std::move(parsed_condition.reader);
    }
    traits_.OnSuccess();
    return nullptr;
  }

  absl::StatusOr<ParsedLoopCondition> CallLoopCondition() {
    if (Interrupted()) {
      return ParsedLoopCondition{false};
    }
    ASSIGN_OR_RETURN(auto condition, traits_.CallLoopCondition());
    return ParseLoopCondition(condition.AsRef());
  }

  absl::Status CallLoopBody() {
    if (Interrupted()) {
      return absl::OkStatus();
    }
    return traits_.CallLoopBody();
  }

 private:
  Traits traits_;
};

// Traits for the `StreamWhileLoopReturns` operator.
class StreamWhileLoopReturnsTraits {
 public:
  StreamWhileLoopReturnsTraits(
      StreamWriterPtr /*absl_nonnull*/ writer,
      StreamWhileLoopFunctor /*nonnull*/ condition_functor,
      StreamWhileLoopFunctor /*nonnull*/ body_functor, Vars vars)
      : writer_(std::move(writer)),
        condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)) {}

  bool Interrupted() const { return writer_->Orphaned(); }

  void OnError(absl::Status&& status) { writer_->TryClose(std::move(status)); }

  absl::StatusOr<arolla::TypedValue> CallLoopCondition() {
    return condition_functor_(vars_.values(), vars_.kwnames());
  }

  absl::Status CallLoopBody() {
    ASSIGN_OR_RETURN(auto update,
                     body_functor_(vars_.values(), vars_.kwnames()));
    RETURN_IF_ERROR(vars_.Update(std::move(update)))
        << "the body functor must return a namedtuple with "
        << "a subset of initial variables";
    return absl::OkStatus();
  }

  void OnSuccess() {
    DCHECK(!vars_.values().empty());
    if (writer_->TryWrite(vars_.values().back())) {
      writer_->TryClose(absl::OkStatus());
    }
  }

 private:
  const StreamWriterPtr /*absl_nonnull*/ writer_;
  const StreamWhileLoopFunctor /*nonnull*/ condition_functor_;
  const StreamWhileLoopFunctor /*nonnull*/ body_functor_;
  Vars vars_;
};

}  // namespace

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopReturns(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamWhileLoopFunctor /*nonnull*/ condition_functor,
    StreamWhileLoopFunctor /*nonnull*/ body_functor,
    arolla::TypedRef initial_state_returns, arolla::TypedRef initial_state) {
  DCHECK(condition_functor != nullptr);
  DCHECK(body_functor != nullptr);
  if (!arolla::IsNamedTupleQType(initial_state.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected the initial state to be a namedtuple, got %s",
                        initial_state.GetType()->name()));
  }
  if (arolla::GetFieldIndexByName(initial_state.GetType(), kReturns)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the state includes variable `%s` multiple times", kReturns));
  }
  const auto field_names = arolla::GetFieldNames(initial_state.GetType());
  std::vector<std::string> var_names;
  var_names.reserve(1 + field_names.size());
  var_names.insert(var_names.end(), field_names.begin(), field_names.end());
  var_names.emplace_back(kReturns);
  std::vector<arolla::TypedRef> initial_var_values;
  initial_var_values.reserve(1 + field_names.size());
  for (size_t i = 0; i < field_names.size(); ++i) {
    initial_var_values.emplace_back(initial_state.GetField(i));
  }
  initial_var_values.emplace_back(initial_state_returns);
  auto [result, writer] =
      MakeStream(initial_state_returns.GetType(), /*initial_capacity=*/1);
  auto loop_hooks =
      std::make_unique<StreamWhileLoopHooks<StreamWhileLoopReturnsTraits>>(
          std::move(writer), std::move(condition_functor),
          std::move(body_functor),
          Vars(std::move(initial_var_values), std::move(var_names)));
  StartBasicRoutine(std::move(executor), std::move(loop_hooks));
  return std::move(result);
}

namespace {

// Traits for the `StreamWhileLoopYieldsChained` and
// `StreamWhileLoopYieldsInterleaved` operators.
template <class StreamComposition>
class StreamWhileLoopYieldsComposedTraits {
 public:
  StreamWhileLoopYieldsComposedTraits(
      StreamWriterPtr /*absl_nonnull*/ writer,
      StreamWhileLoopFunctor /*nonnull*/ condition_functor,
      StreamWhileLoopFunctor /*nonnull*/ body_functor, Vars vars)
      : composition_(std::move(writer)),
        condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)),
        yields_ref_(vars_.mutable_values().back()),
        initial_yields_(yields_ref_) {
    DCHECK(!vars_.kwnames().empty());
    DCHECK(!vars_.values().empty());
    DCHECK(IsStreamQType(initial_yields_.GetType()));
    composition_.Add(initial_yields_.UnsafeAs<StreamPtr>());
  }

  ~StreamWhileLoopYieldsComposedTraits() {
    if (!finished_.test(std::memory_order_relaxed)) {
      composition_.AddError(absl::CancelledError("orphaned"));
    }
  }

  bool Interrupted() const { return finished_.test(std::memory_order_relaxed); }

  void OnError(absl::Status&& status) {
    if (!finished_.test_and_set(std::memory_order_relaxed)) {
      composition_.AddError(std::move(status));
    }
  }

  absl::StatusOr<arolla::TypedValue> CallLoopCondition() {
    return condition_functor_(
        vars_.values().subspan(0, vars_.values().size() - 1),
        vars_.kwnames().subspan(0, vars_.kwnames().size() - 1));
  }

  absl::Status CallLoopBody() {
    ASSIGN_OR_RETURN(
        auto update,
        body_functor_(vars_.values().subspan(0, vars_.values().size() - 1),
                      vars_.kwnames().subspan(0, vars_.kwnames().size() - 1)));
    RETURN_IF_ERROR(vars_.Update(std::move(update)))
        << "the body functor must return a namedtuple with "
        << "a subset of initial variables and '" << vars_.kwnames().back()
        << "'";
    // We use the initial "yields" value as a sentinel. This is safe because
    // `vars_` owns the initial value, preventing its address from being reused.
    if (yields_ref_.GetRawPointer() != initial_yields_.GetRawPointer()) {
      composition_.Add(yields_ref_.UnsafeAs<StreamPtr>());
      // Restore the initial "yields" value. This allows us to determine if it
      // was updated next time.
      yields_ref_ = initial_yields_;
    }
    return absl::OkStatus();
  }

  void OnSuccess() { finished_.test_and_set(std::memory_order_relaxed); }

 private:
  // Use an atomic variable to track if the resulting composition has been
  // finished (either due to an error or because the loop has finished its
  // computation).
  std::atomic_flag finished_ = ATOMIC_FLAG_INIT;
  StreamComposition composition_;
  const StreamWhileLoopFunctor /*nonnull*/ condition_functor_;
  const StreamWhileLoopFunctor /*nonnull*/ body_functor_;
  Vars vars_;
  arolla::TypedRef& yields_ref_;
  const arolla::TypedRef initial_yields_;
};

template <class StreamComposition>
absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopYieldsComposed(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamWhileLoopFunctor /*nonnull*/ condition_functor,
    StreamWhileLoopFunctor /*nonnull*/ body_functor,
    arolla::TypedRef initial_state,
    const StreamPtr /*absl_nonnull*/& initial_yields,
    absl::string_view yields_param_name) {
  DCHECK(condition_functor != nullptr);
  DCHECK(body_functor != nullptr);
  if (!arolla::IsNamedTupleQType(initial_state.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected the initial state to be a namedtuple, got %s",
                        initial_state.GetType()->name()));
  }
  if (arolla::GetFieldIndexByName(initial_state.GetType(), yields_param_name)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the state includes variable `%s` multiple times", yields_param_name));
  }
  const auto field_names = arolla::GetFieldNames(initial_state.GetType());
  std::vector<std::string> var_names;
  var_names.reserve(1 + field_names.size());
  var_names.insert(var_names.end(), field_names.begin(), field_names.end());
  var_names.emplace_back(yields_param_name);
  std::vector<arolla::TypedRef> initial_var_values;
  initial_var_values.reserve(1 + field_names.size());
  for (size_t i = 0; i < field_names.size(); ++i) {
    initial_var_values.emplace_back(initial_state.GetField(i));
  }
  initial_var_values.emplace_back(arolla::TypedRef::UnsafeFromRawPointer(
      GetStreamQType(initial_yields->value_qtype()), &initial_yields));
  auto [result, writer] = MakeStream(initial_yields->value_qtype());
  auto loop_hooks = std::make_unique<StreamWhileLoopHooks<
      StreamWhileLoopYieldsComposedTraits<StreamComposition>>>(
      std::move(writer), std::move(condition_functor), std::move(body_functor),
      Vars(std::move(initial_var_values), std::move(var_names)));
  StartBasicRoutine(std::move(executor), std::move(loop_hooks));
  return std::move(result);
}

}  // namespace

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopYieldsChained(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamWhileLoopFunctor /*nonnull*/ condition_functor,
    StreamWhileLoopFunctor /*nonnull*/ body_functor,
    const StreamPtr /*absl_nonnull*/& initial_yields,
    arolla::TypedRef initial_state) {
  return StreamWhileLoopYieldsComposed<StreamChain>(
      std::move(executor), std::move(condition_functor),
      std::move(body_functor), std::move(initial_state), initial_yields,
      kYieldsChained);
}

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopYieldsInterleaved(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamWhileLoopFunctor /*nonnull*/ condition_functor,
    StreamWhileLoopFunctor /*nonnull*/ body_functor,
    const StreamPtr /*absl_nonnull*/& initial_yields_interleaved,
    arolla::TypedRef initial_state) {
  return StreamWhileLoopYieldsComposed<StreamInterleave>(
      std::move(executor), std::move(condition_functor),
      std::move(body_functor), std::move(initial_state),
      initial_yields_interleaved, kYieldsInterleaved);
}

}  // namespace koladata::functor::parallel
