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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_FOR_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_FOR_H_

#include <string>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

using StreamForFunctor = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
    absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) const>;

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamForReturns(
    ExecutorPtr /*absl_nonnull*/ executor,
    const StreamPtr /*absl_nonnull*/& input_stream,
    StreamForFunctor /*nonnull*/ body_functor,
    StreamForFunctor /*nullable*/ finalize_functor,
    StreamForFunctor /*nullable*/ condition_functor,
    arolla::TypedRef initial_state_returns, arolla::TypedRef initial_state);

absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamForYields(
    ExecutorPtr /*absl_nonnull*/ executor,
    const StreamPtr /*absl_nonnull*/& input_stream,
    StreamForFunctor /*nonnull*/ body_functor,
    StreamForFunctor /*nullable*/ finalize_functor,
    StreamForFunctor /*nullable*/ condition_functor,
    absl::string_view yields_param_name, arolla::TypedRef initial_yields,
    arolla::TypedRef initial_state);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_FOR_H_
