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
#ifndef KOLADATA_FUNCTOR_PARALLEL_FUTURE_OPERATORS_H_
#define KOLADATA_FUNCTOR_PARALLEL_FUTURE_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::functor::parallel {

// koda_internal.parallel.as_future operator.
// Wraps the given value in a future. If it is already a future,
// raises.
class AsFutureOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.get_future_value_for_testing operator.
// Gets the value from the given future for testing purposes. Raises
// if it is not ready yet.
// Real code should not use this operator, but rather apply asynchronous
// operators to the future.
class GetFutureValueForTestingOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.unwrap_future_to_future
// Given a future to a future, returns a future that will get the value of the
// inner future.
class UnwrapFutureToFutureOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.unwrap_future_to_stream
// Given a future to a stream, returns a stream that will get the values of the
// inner stream.
class UnwrapFutureToStreamOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.stream_from_future
// Given a future[X], returns a stream[X] that will either have exactly one
// value if the future does not store an error, or only the error from the
// future.
class StreamFromFutureOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.future_from_single_value_stream
// Given a stream[X] that is expected to have exactly one value, returns a
// future[X] with that value. If the stream has an error, zero values, or
// more than one value, the future will get an error.
class FutureFromSingleValueStreamOperatorFamily
    : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// koda_internal.parallel.future_iterable_from_stream
// Given a stream[X] creates a future[iterable[X]] with the same values.
// This should be used only if subsequent processing requires all values of the
// stream at once.
class FutureIterableFromStreamOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_AS_FUTURE_OPERATOR_H_
