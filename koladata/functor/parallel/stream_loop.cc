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

using ::koladata::functor::parallel::stream_loop_internal::ParsedLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::ParseLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::
    ParseLoopConditionStream;
using ::koladata::functor::parallel::stream_loop_internal::Vars;

constexpr absl::string_view kReturns = "returns";
constexpr absl::string_view kYieldsChained = "yields";

class StreamWhileLoopReturnsHooks final : public BasicRoutineHooks {
 public:
  StreamWhileLoopReturnsHooks(StreamWriterPtr /*absl_nonnull*/ writer,
                              StreamLoopFunctor /*nonnull*/ condition_functor,
                              StreamLoopFunctor /*nonnull*/ body_functor,
                              Vars vars)
      : writer_(std::move(writer)),
        condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)) {
    DCHECK(!vars_.kwnames().empty() && vars_.kwnames().back() == kReturns);
  }

  bool Interrupted() const final { return writer_->Orphaned(); }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr /*absl_nullable*/ Start() final {
    ASSIGN_OR_RETURN(auto parsed_condition, CallLoopCondition(),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

  StreamReaderPtr /*absl_nullable*/ Resume(StreamReaderPtr
                                       /*absl_nonnull*/ condition) final {
    ASSIGN_OR_RETURN(auto parsed_condition,
                     ParseLoopConditionStream(std::move(condition)),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    writer_->TryClose(std::move(status));
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
    if (!Interrupted() && writer_->TryWrite(vars_.values().back())) {
      writer_->TryClose(absl::OkStatus());
    }
    return nullptr;
  }

  absl::StatusOr<ParsedLoopCondition> CallLoopCondition() {
    if (Interrupted()) {
      return ParsedLoopCondition{false};
    }
    ASSIGN_OR_RETURN(auto condition,
                     condition_functor_(vars_.values(), vars_.kwnames()));
    return ParseLoopCondition(condition.AsRef());
  }

  absl::Status CallLoopBody() {
    if (Interrupted()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto update,
                     body_functor_(vars_.values(), vars_.kwnames()));
    RETURN_IF_ERROR(vars_.Update(std::move(update)))
        << "the body functor must return a namedtuple with "
        << "a subset of initial variables";
    return absl::OkStatus();
  }

 private:
  const StreamWriterPtr /*absl_nonnull*/ writer_;
  const StreamLoopFunctor /*nonnull*/ condition_functor_;
  const StreamLoopFunctor /*nonnull*/ body_functor_;
  Vars vars_;
};

}  // namespace

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopReturns(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamLoopFunctor /*nonnull*/ condition_functor,
    StreamLoopFunctor /*nonnull*/ body_functor,
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
  auto loop_hooks = std::make_unique<StreamWhileLoopReturnsHooks>(
      std::move(writer), std::move(condition_functor), std::move(body_functor),
      Vars(std::move(initial_var_values), std::move(var_names)));
  StartBasicRoutine(std::move(executor), std::move(loop_hooks));
  return std::move(result);
}

namespace {

class StreamWhileLoopYieldsChainedHooks final : public BasicRoutineHooks {
 public:
  StreamWhileLoopYieldsChainedHooks(
      StreamWriterPtr /*absl_nonnull*/ writer,
      StreamLoopFunctor /*nonnull*/ condition_functor,
      StreamLoopFunctor /*nonnull*/ body_functor, Vars vars)
      : condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)),
        yield_sentinel_(vars_.values().back()),
        chain_(std::move(writer)) {
    // We add the initial "yields" value to the resulting chain and then
    // repurpose it as a sentinel. This is safe because `vars_` owns the initial
    // value, preventing its address from being reused.
    DCHECK(!vars_.kwnames().empty() &&
           vars_.kwnames().back() == kYieldsChained);
    DCHECK(IsStreamQType(yield_sentinel_.GetType()));
    chain_.Add(yield_sentinel_.UnsafeAs<StreamPtr>());
  }

  ~StreamWhileLoopYieldsChainedHooks() final {
    if (!chain_finished_.test(std::memory_order_relaxed)) {
      chain_.AddError(absl::CancelledError("orphaned"));
    }
  }

  bool Interrupted() const final {
    return chain_finished_.test(std::memory_order_relaxed);
  }

  void OnCancel(absl::Status&& status) final { OnError(std::move(status)); }

  StreamReaderPtr /*absl_nullable*/ Start() final {
    ASSIGN_OR_RETURN(auto parsed_condition, CallLoopCondition(),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

  StreamReaderPtr /*absl_nullable*/ Resume(StreamReaderPtr
                                       /*absl_nonnull*/ condition) final {
    ASSIGN_OR_RETURN(auto parsed_condition,
                     ParseLoopConditionStream(std::move(condition)),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

 private:
  std::nullptr_t OnError(absl::Status&& status) {
    if (!chain_finished_.test_and_set(std::memory_order_relaxed)) {
      chain_.AddError(std::move(status));
    }
    return nullptr;
  }

  StreamReaderPtr /*absl_nullable*/ Run(ParsedLoopCondition parsed_condition) {
    while (parsed_condition.value) {
      RETURN_IF_ERROR(CallLoopBody()).With([this](absl::Status status) {
        return OnError(std::move(status));
      });
      ASSIGN_OR_RETURN(parsed_condition, CallLoopCondition(),
                       OnError(std::move(_)));
    }
    if (parsed_condition.reader != nullptr) {
      return std::move(parsed_condition.reader);
    }
    chain_finished_.test_and_set(std::memory_order_relaxed);
    return nullptr;
  }

  absl::StatusOr<ParsedLoopCondition> CallLoopCondition() {
    if (Interrupted()) {
      return ParsedLoopCondition{false};
    }
    ASSIGN_OR_RETURN(
        auto condition,
        condition_functor_(
            vars_.values().subspan(0, vars_.values().size() - 1),
            vars_.kwnames().subspan(0, vars_.kwnames().size() - 1)));
    return ParseLoopCondition(condition.AsRef());
  }

  absl::Status CallLoopBody() {
    if (Interrupted()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(
        auto update,
        body_functor_(vars_.values().subspan(0, vars_.values().size() - 1),
                      vars_.kwnames().subspan(0, vars_.kwnames().size() - 1)));
    RETURN_IF_ERROR(vars_.Update(std::move(update)))
        << "the body functor must return a namedtuple with "
        << "a subset of initial variables or 'yields'";
    MaybeYield();
    return absl::OkStatus();
  }

  void MaybeYield() {
    DCHECK(!vars_.kwnames().empty() &&
           vars_.kwnames().back() == kYieldsChained);
    auto yields = vars_.values().back();
    if (yields.GetRawPointer() == yield_sentinel_.GetRawPointer()) {
      return;  // No new value.
    }
    vars_.mutable_values().back() = yield_sentinel_;
    // Overwrite the value. This allows us to determine if it was updated next
    // time.
    if (!Interrupted()) {
      chain_.Add(yields.UnsafeAs<StreamPtr>());
    }
  }

 private:
  const StreamLoopFunctor /*nonnull*/ condition_functor_;
  const StreamLoopFunctor /*nonnull*/ body_functor_;
  Vars vars_;
  // A sentinel value indicating no new 'yields' value.
  const arolla::TypedRef yield_sentinel_;

  // Use an atomic variable to track if the resulting chain has been finished
  // (either due to an error or because the loop has finished its computation).
  std::atomic_flag chain_finished_ = ATOMIC_FLAG_INIT;
  StreamChain chain_;
};

}  // namespace

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopYieldsChained(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamLoopFunctor /*nonnull*/ condition_functor,
    StreamLoopFunctor /*nonnull*/ body_functor,
    const StreamPtr /*absl_nonnull*/& initial_yields,
    arolla::TypedRef initial_state) {
  DCHECK(condition_functor != nullptr);
  DCHECK(body_functor != nullptr);
  if (!arolla::IsNamedTupleQType(initial_state.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected the initial state to be a namedtuple, got %s",
                        initial_state.GetType()->name()));
  }
  if (arolla::GetFieldIndexByName(initial_state.GetType(), kYieldsChained)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the state includes variable `%s` multiple times", kYieldsChained));
  }
  const auto field_names = arolla::GetFieldNames(initial_state.GetType());
  std::vector<std::string> var_names;
  var_names.reserve(1 + field_names.size());
  var_names.insert(var_names.end(), field_names.begin(), field_names.end());
  var_names.emplace_back(kYieldsChained);
  std::vector<arolla::TypedRef> initial_var_values;
  initial_var_values.reserve(1 + field_names.size());
  for (size_t i = 0; i < field_names.size(); ++i) {
    initial_var_values.emplace_back(initial_state.GetField(i));
  }
  initial_var_values.emplace_back(arolla::TypedRef::UnsafeFromRawPointer(
      GetStreamQType(initial_yields->value_qtype()), &initial_yields));
  auto [result, writer] = MakeStream(initial_yields->value_qtype());
  auto loop_hooks = std::make_unique<StreamWhileLoopYieldsChainedHooks>(
      std::move(writer), std::move(condition_functor), std::move(body_functor),
      Vars(std::move(initial_var_values), std::move(var_names)));
  StartBasicRoutine(std::move(executor), std::move(loop_hooks));
  return std::move(result);
}

}  // namespace koladata::functor::parallel
