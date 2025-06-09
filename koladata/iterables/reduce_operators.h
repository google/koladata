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
#ifndef KOLADATA_ITERABLES_REDUCE_OPERATORS_H_
#define KOLADATA_ITERABLES_REDUCE_OPERATORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::iterables {

// kd.iterables.reduce_concat operator.
// Concatenates (in the kd.concat sense) the initial value with the slices from
// the given iterable. This operator allows to concatenate N slices in O(N)
// instead of O(N^2) via the two-at-a-time kd.functor.reduce operator.
class ReduceConcatOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.iterables.reduce_updated_bag operator.
// Merges (in the kd.updated_bag sense) the initial value with the bags from
// the given iterable. This operator allows to concatenate N slices in O(N)
// instead of O(N^2) via the two-at-a-time kd.functor.reduce operator.
class ReduceUpdatedBagOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

}  // namespace koladata::iterables

#endif  // KOLADATA_ITERABLES_REDUCE_OPERATORS_H_
