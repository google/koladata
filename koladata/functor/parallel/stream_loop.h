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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_LOOP_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_LOOP_H_

#include <string>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

using StreamLoopFunctor = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
    absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) const>;

// Repeatedly applies a body functor while a condition is met. Returns
// a single-item stream with the final value of the `returns` state variable.
//
// Each iteration, the operator passes current state variables (including
// `returns`) as keyword arguments to `condition_functor` and
// `body_functor`. The loop continues if `condition_functor` returns `present`.
// State variables are then updated from `body_functor`'s namedtuple return.
absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopReturns(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamLoopFunctor /*nonnull*/ condition_functor,
    StreamLoopFunctor /*nonnull*/ body_functor,
    arolla::TypedRef initial_state_returns, arolla::TypedRef initial_state);

// Repeatedly applies a body functor while a condition is met. The loop returns
// a stream formed by chaining together any `yields` streams produced (including
// the `initial_yields` stream).
//
// Each iteration, the operator passes current state variables (excluding
// `yields`, which is for output chaining) as keyword arguments to
// `condition_functor` and `body_functor`. The loop continues if
// `condition_functor` returns `present`. State variables are then updated from
// `body_functor`'s namedtuple return value. Additionally, the body functor
// can return a `yields` variable containing a stream, and these individual
// streams are chained together to form the overall result stream.
absl::StatusOr<StreamPtr /*absl_nonnull*/> StreamWhileLoopYieldsChained(
    ExecutorPtr /*absl_nonnull*/ executor,
    StreamLoopFunctor /*nonnull*/ condition_functor,
    StreamLoopFunctor /*nonnull*/ body_functor,
    const StreamPtr /*absl_nonnull*/& initial_yields,
    arolla::TypedRef initial_state);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_LOOP_H_
