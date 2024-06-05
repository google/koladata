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
#ifndef KOLADATA_INTERNAL_DATA_SLICE_ACCESSORS_H_
#define KOLADATA_INTERNAL_DATA_SLICE_ACCESSORS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/sparse_source.h"

namespace koladata::internal {

using DenseSourceSpan = absl::Span<const DenseSource* const>;
using SparseSourceSpan = absl::Span<const SparseSource* const>;

// Get values from DataSources.
// `sparse_sources` must be ordered from the most fresh to the oldest. Sparse
// source with lower index will override values of sources with higher indices.
// Values from `sparse_sources` override `dense_sources`.
// All `dense_sources` must correspond to different allocation ids, so the order
// of dense sources is not important.
absl::StatusOr<DataSliceImpl> GetAttributeFromSources(
    const DataSliceImpl& slice, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources);

// Same as above, but for a single ObjectId rather than for DataSliceImpl.
DataItem GetAttributeFromSources(ObjectId id, DenseSourceSpan dense_sources,
                                 SparseSourceSpan sparse_sources);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DATA_SLICE_ACCESSORS_H_
