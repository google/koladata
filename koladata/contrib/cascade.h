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
#ifndef KOLADATA_CONTRIB_CASCADE_H_
#define KOLADATA_CONTRIB_CASCADE_H_

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

namespace koladata::contrib {

using StreamCascadeFunctor =
    absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
        absl::Span<const arolla::TypedRef> args,
        absl::Span<const std::string> kwnames) const>;

// Processes the items of the input stream in batches, together with the items
// that the step functor feeds back.
//
// The step functor is called sequentially, once per batch, as
// `step_functor(batch, **state)`, where `batch` is a (closed) stream with all
// the items that are ready by the time of the call. It must return a namedtuple
// with new values for a subset of the state variables; the new values must have
// the same types as the initial ones. Additionally, the namedtuple may contain
// the fields:
//
//   * `<yields_param_name>`: a value of the same type as `initial_yields`,
//     which is written to the resulting stream;
//
//   * `cascades`: a stream with the same value type as the input stream; its
//     items are processed by the step functor as well.
//
// The items of the input stream and of the cascade streams are interleaved:
// the items of each stream are processed in their order, but the order between
// different streams is unspecified. The resulting stream starts with
// `initial_yields`, followed by the yields of the calls in the order of the
// calls, and it is closed when the input stream and all cascade streams are
// exhausted.
absl::StatusOr<functor::parallel::StreamPtr absl_nonnull> StreamBatchedCascade(
    functor::parallel::ExecutorPtr absl_nonnull executor,
    const functor::parallel::StreamPtr absl_nonnull& input_stream,
    StreamCascadeFunctor absl_nonnull step_functor,
    absl::string_view yields_param_name, arolla::TypedRef initial_yields,
    arolla::TypedRef initial_state);

}  // namespace koladata::contrib

#endif  // KOLADATA_CONTRIB_CASCADE_H_
