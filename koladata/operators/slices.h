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
#ifndef KOLADATA_OPERATORS_SLICES_H_
#define KOLADATA_OPERATORS_SLICES_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kde.slices._inverse_mapping.
absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x);

// kde.slices._ordinal_rank.
absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending);

// kde.slices._dense_rank.
absl::StatusOr<DataSlice> DenseRank(const DataSlice& x,
                                    const DataSlice& descending);

// kde.slices.align.
class AlignOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.slices._collapse.
absl::StatusOr<DataSlice> Collapse(const DataSlice& ds);

// kde.slices._concat_or_stack
absl::StatusOr<DataSlice> ConcatOrStack(
    absl::Span<const DataSlice* const> slices);

// kde.slices.is_empty.
absl::StatusOr<DataSlice> IsEmpty(const DataSlice& obj);

// kde.slices.group_by_indices.
absl::StatusOr<DataSlice> GroupByIndices(
    absl::Span<const DataSlice* const> slices);

// kde.slices.group_by_indices_sorted.
absl::StatusOr<DataSlice> GroupByIndicesSorted(
    absl::Span<const DataSlice* const> slices);

// kde.slices.unique.
absl::StatusOr<DataSlice> Unique(const DataSlice& x, const DataSlice& sort);

// kde.slices.reverse.
absl::StatusOr<DataSlice> Reverse(const DataSlice& obj);

// kde.slices.select.
absl::StatusOr<DataSlice> Select(const DataSlice& ds, const DataSlice& filter,
                                 bool expand_filter);

// kde.slices.inverse_select.
absl::StatusOr<DataSlice> InverseSelect(const DataSlice& ds,
                                        const DataSlice& filter);

// kde.slices.subslice operator.
class SubsliceOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.slices.take operator.
absl::StatusOr<DataSlice> Take(const DataSlice& x, const DataSlice& indices);

// kde.slices.translate.
absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                    const DataSlice& keys_from,
                                    const DataSlice& values_from);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SLICES_H_
