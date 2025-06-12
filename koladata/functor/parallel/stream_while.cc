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
#include "koladata/functor/parallel/stream_while.h"

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
#include "koladata/functor/parallel/stream_loop_internal.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {
namespace {

constexpr absl::string_view kReturns = "returns";

using ::koladata::functor::parallel::stream_loop_internal::ParsedLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::ParseLoopCondition;
using ::koladata::functor::parallel::stream_loop_internal::
    ParseLoopConditionStream;
using ::koladata::functor::parallel::stream_loop_internal::Vars;

// A generic implementation of while loop hooks, which further specialized using
// a traits class.
template <class Traits>
class StreamWhileHooks final : public BasicRoutineHooks {
 public:
  template <typename... Args>
  explicit StreamWhileHooks(Args&&... args)
      : traits_(std::forward<Args>(args)...) {}

  bool Interrupted() const final { return traits_.Interrupted(); }

  void OnCancel(absl::Status&& status) final {
    traits_.OnError(std::move(status));
  }

  StreamReaderPtr absl_nullable Start() final {
    ASSIGN_OR_RETURN(auto parsed_condition, CallLoopCondition(),
                     OnError(std::move(_)));
    return Run(std::move(parsed_condition));
  }

  StreamReaderPtr absl_nullable Resume(  // clang-format hint
      StreamReaderPtr absl_nonnull condition) final {
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

  StreamReaderPtr absl_nullable Run(ParsedLoopCondition parsed_condition) {
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
    return traits_.CallLoopCondition();
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

// Traits for the `StreamWhileReturns` operator.
class StreamWhileReturnsTraits {
 public:
  StreamWhileReturnsTraits(StreamWriterPtr absl_nonnull writer,
                           StreamWhileFunctor /*nonnull*/ condition_functor,
                           StreamWhileFunctor /*nonnull*/ body_functor,
                           Vars vars)
      : writer_(std::move(writer)),
        condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)) {
    DCHECK(!vars_.kwnames().empty());
    DCHECK_EQ(vars_.values().size(), vars_.kwnames().size());
  }

  bool Interrupted() const { return writer_->Orphaned(); }

  void OnError(absl::Status&& status) { writer_->TryClose(std::move(status)); }

  absl::StatusOr<ParsedLoopCondition> CallLoopCondition() {
    ASSIGN_OR_RETURN(auto condition,
                     condition_functor_(vars_.values(), vars_.kwnames()));
    return ParseLoopCondition(condition.AsRef());
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
    if (writer_->TryWrite(vars_.values().back())) {
      writer_->TryClose(absl::OkStatus());
    }
  }

 private:
  const StreamWriterPtr absl_nonnull writer_;
  const StreamWhileFunctor /*nonnull*/ condition_functor_;
  const StreamWhileFunctor /*nonnull*/ body_functor_;
  Vars vars_;
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamWhileReturns(
    ExecutorPtr absl_nonnull executor,
    StreamWhileFunctor /*nonnull*/ condition_functor,
    StreamWhileFunctor /*nonnull*/ body_functor,
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
  var_names.reserve(field_names.size() + 1);
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
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamWhileHooks<StreamWhileReturnsTraits>>(
          std::move(writer), std::move(condition_functor),
          std::move(body_functor),
          Vars(std::move(initial_var_values), std::move(var_names))));
  return std::move(result);
}

namespace {

// Traits for the `StreamWhileYields` operator.
class StreamWhileYieldsTraits {
 public:
  StreamWhileYieldsTraits(StreamWriterPtr absl_nonnull writer,
                          StreamWhileFunctor /*nonnull*/ condition_functor,
                          StreamWhileFunctor /*nonnull*/ body_functor,
                          Vars vars)
      : writer_(std::move(writer)),
        condition_functor_(std::move(condition_functor)),
        body_functor_(std::move(body_functor)),
        vars_(std::move(vars)),
        initial_yields_(vars_.values().back()) {
    DCHECK(!vars_.kwnames().empty());
    DCHECK_EQ(vars_.values().size(), vars_.kwnames().size());
    writer_->Write(initial_yields_);
  }

  bool Interrupted() const { return writer_->Orphaned(); }

  void OnError(absl::Status&& status) { writer_->TryClose(std::move(status)); }

  absl::StatusOr<ParsedLoopCondition> CallLoopCondition() {
    ASSIGN_OR_RETURN(
        auto condition,
        condition_functor_(
            vars_.values().subspan(0, vars_.values().size() - 1),
            vars_.kwnames().subspan(0, vars_.kwnames().size() - 1)));
    return ParseLoopCondition(condition.AsRef());
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
    MaybeYield();
    return absl::OkStatus();
  }

  void OnSuccess() { writer_->TryClose(absl::OkStatus()); }

 private:
  void MaybeYield() {
    // We use the initial "yields" value as a sentinel. This is safe because
    // `vars_` owns the initial value, preventing its address from being reused.
    arolla::TypedRef& yields = vars_.mutable_values().back();
    if (yields.GetRawPointer() != initial_yields_.GetRawPointer() &&
        writer_->TryWrite(yields)) {
      // Restore the initial "yields" value. This allows us to determine if it
      // was updated next time.
      yields = initial_yields_;
    }
  }

  const StreamWriterPtr absl_nonnull writer_;
  const StreamWhileFunctor /*nonnull*/ condition_functor_;
  const StreamWhileFunctor /*nonnull*/ body_functor_;
  Vars vars_;
  const arolla::TypedRef initial_yields_;
};

}  // namespace

absl::StatusOr<StreamPtr absl_nonnull> StreamWhileYields(
    ExecutorPtr absl_nonnull executor,
    StreamWhileFunctor /*nonnull*/ condition_functor,
    StreamWhileFunctor /*nonnull*/ body_functor,
    absl::string_view yields_param_name, arolla::TypedRef initial_yields,
    arolla::TypedRef initial_state) {
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
  var_names.reserve(field_names.size() + 1);
  var_names.insert(var_names.end(), field_names.begin(), field_names.end());
  var_names.emplace_back(yields_param_name);
  std::vector<arolla::TypedRef> initial_var_values;
  initial_var_values.reserve(field_names.size() + 1);
  for (size_t i = 0; i < field_names.size(); ++i) {
    initial_var_values.emplace_back(initial_state.GetField(i));
  }
  initial_var_values.emplace_back(initial_yields);
  auto [result, writer] = MakeStream(initial_yields.GetType());
  StartBasicRoutine(
      std::move(executor),
      std::make_unique<StreamWhileHooks<StreamWhileYieldsTraits>>(
          std::move(writer), std::move(condition_functor),
          std::move(body_functor),
          Vars(std::move(initial_var_values), std::move(var_names))));
  return std::move(result);
}

}  // namespace koladata::functor::parallel
