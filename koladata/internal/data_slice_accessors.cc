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
#include "koladata/internal/data_slice_accessors.h"

#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/sparse_source.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata::internal {

using ::arolla::DenseArray;
using ::arolla::GetQType;

absl::StatusOr<DataSliceImpl> GetAttributeFromSources(
    const DataSliceImpl& slice, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources) {
  if (slice.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(slice.size());
  }
  if (slice.dtype() != GetQType<ObjectId>()) {
    return absl::FailedPreconditionError(
        "Getting attribute from primitive (or mixed) values is not supported");
  }
  const ObjectIdArray& objs = slice.values<ObjectId>();
  if (sparse_sources.empty()) {
    if (dense_sources.size() == 1) {
      bool check_alloc_id =
          slice.allocation_ids().contains_small_allocation_id() ||
          slice.allocation_ids().ids().size() > 1;
      return dense_sources.front()->Get(objs, check_alloc_id);
    } else if (dense_sources.empty()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(slice.size());
    }
  }
  if (dense_sources.empty() && sparse_sources.size() == 1) {
    return sparse_sources.front()->Get(objs);
  }

  SliceBuilder bldr(objs.size());
  bldr.ApplyMask(objs.ToMask());
  absl::Span<const ObjectId> objs_span = objs.values.span();
  // Sparse sources have priority over dense sources.
  for (const SparseSource* source : sparse_sources) {
    if (bldr.is_finalized()) {
      break;
    }
    source->Get(objs_span, bldr);
  }
  for (const DenseSource* source : dense_sources) {
    if (bldr.is_finalized()) {
      break;
    }
    source->Get(objs_span, bldr);
  }
  return std::move(bldr).Build();
}

DataItem GetAttributeFromSources(
    ObjectId id, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources) {
  // There shouldn't be more than one dense source because they override each
  // other.
  DCHECK_LE(dense_sources.size(), 1);

  for (const auto& source : sparse_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return *res;
    }
  }
  for (const auto& source : dense_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return *res;
    }
  }
  return DataItem();
}

}  // namespace koladata::internal
