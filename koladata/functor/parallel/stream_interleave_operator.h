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
#ifndef THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_INTERLEAVE_OPERATOR_H_
#define THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_INTERLEAVE_OPERATOR_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::functor::parallel {

// koda_internal.parallel.stream_interleave operator.
class StreamInterleaveOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

}  // namespace koladata::functor::parallel

#endif  // THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_INTERLEAVE_OPERATOR_H_
