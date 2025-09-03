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
#ifndef KOLADATA_OPERATORS_SLICES_H_
#define KOLADATA_OPERATORS_SLICES_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.slices._inverse_mapping.
absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x);

// kd.slices._ordinal_rank.
absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending);

// kd.slices._dense_rank.
absl::StatusOr<DataSlice> DenseRank(const DataSlice& x,
                                    const DataSlice& descending);

// kd.slices.align.
class AlignOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.slices._collapse.
absl::StatusOr<DataSlice> Collapse(const DataSlice& ds);

// kd.slices._concat_or_stack
absl::StatusOr<DataSlice> ConcatOrStack(
    absl::Span<const DataSlice* const> slices);

// kd.slices.is_empty.
absl::StatusOr<DataSlice> IsEmpty(const DataSlice& obj);

// kd.slices.empty_shaped.
absl::StatusOr<DataSlice> EmptyShaped(const DataSlice::JaggedShape& shape,
                                      const DataSlice& schema);

// kd.slices.get_repr.
absl::StatusOr<DataSlice> GetRepr(const DataSlice& x, const DataSlice& depth,
                                  const DataSlice& item_limit,
                                  const DataSlice& item_limit_per_dimension,
                                  const DataSlice& format_html,
                                  const DataSlice& max_str_len,
                                  const DataSlice& show_attributes,
                                  const DataSlice& show_databag_id,
                                  const DataSlice& show_shape,
                                  const DataSlice& show_schema);

// kd.slices.group_by_indices.
absl::StatusOr<DataSlice> GroupByIndices(
    absl::Span<const DataSlice* const> slices);

// kd.slices.unique.
absl::StatusOr<DataSlice> Unique(const DataSlice& x, const DataSlice& sort);

// kd.slices.reverse.
absl::StatusOr<DataSlice> Reverse(const DataSlice& obj);

// kd.slices.select.
absl::StatusOr<DataSlice> Select(const DataSlice& ds, const DataSlice& filter,
                                 const DataSlice& expand_filter);

// kd.slices.inverse_select.
absl::StatusOr<DataSlice> InverseSelect(const DataSlice& ds,
                                        const DataSlice& filter);

// kd.slices.subslice operator.
class SubsliceOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.slices.take operator.
absl::StatusOr<DataSlice> Take(const DataSlice& x, const DataSlice& indices);

// kd.slices.translate.
absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                    const DataSlice& keys_from,
                                    const DataSlice& values_from);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SLICES_H_
