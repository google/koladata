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
#ifndef KOLADATA_FUNCTOR_CALL_OPERATOR_H_
#define KOLADATA_FUNCTOR_CALL_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::functor {

// kd.functor.call operator.
// Calls a given functor with the given arguments.
class CallOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.functor.call_and_update operator.
// Calls a given functor with the given arguments, the functor must
// return a namedtuple, which is then applied as an update to the given
// namedtuple. This operator exists so that we do not have to specify
// return_type_as for the inner call (since the returned namedtuple may
// have a subset of fields of the original namedtuple, potentially in a
// different order).
class CallAndUpdateNamedTupleOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.functor._maybe_call operator.
// If the first argument is a functor, calls it on the second argument.
// Otherwise, returns the first argument.
absl::StatusOr<DataSlice> MaybeCall(arolla::EvaluationContext* ctx,
                                    const DataSlice& maybe_fn,
                                    const DataSlice& arg,
                                    internal::NonDeterministicToken);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_CALL_OPERATOR_H_
