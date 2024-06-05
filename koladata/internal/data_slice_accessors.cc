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

#include <cstddef>
#include <cstdlib>
#include <cstring>
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
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"

namespace koladata::internal {

namespace {

using ::arolla::DenseArray;
using ::arolla::GetQType;

// Sources are filtered to support given attr and ordered from the most fresh
// to the oldest.
void GetAttributeFromSparseSources(const DenseArray<ObjectId>& objs,
                                   SparseSourceSpan sources,
                                   DataSliceImpl::Builder& bldr) {
  absl::Span<const ObjectId> objs_span = objs.values.span();
  size_t mask_size = arolla::bitmap::BitmapSize(objs_span.size());
  arolla::bitmap::Word* mask = reinterpret_cast<arolla::bitmap::Word*>(
      malloc(mask_size * sizeof(arolla::bitmap::Word)));
  absl::Span<arolla::bitmap::Word> mask_span(mask, mask_size);
  if (objs.bitmap.empty()) {
    memset(mask, 0xff, mask_size * sizeof(arolla::bitmap::Word));
  } else if (objs.bitmap_bit_offset == 0) {
    memcpy(mask, objs.bitmap.span().data(),
           mask_size * sizeof(arolla::bitmap::Word));
  } else {
    for (size_t i = 0; i < mask_size; ++i) {
      mask[i] = arolla::bitmap::GetWordWithOffset(objs.bitmap, i,
                                                  objs.bitmap_bit_offset);
    }
  }

  // TODO: Try to iterate over values in the outer loop
  // and over data sources in the inner loop.
  for (const SparseSource* source : sources) {
    if (arolla::bitmap::AreAllBitsUnset(mask, objs_span.size())) {
      break;
    }
    source->Get(objs_span, bldr, mask_span);
  }
  free(mask);
}

}  // namespace

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

  DataSliceImpl::Builder bldr(objs.size());
  for (const DenseSource* source : dense_sources) {
    source->Get(objs, bldr);
  }
  if (!sparse_sources.empty()) {
    GetAttributeFromSparseSources(objs, sparse_sources, bldr);
  }
  return std::move(bldr).Build();
}

DataItem GetAttributeFromSources(
    ObjectId id, DenseSourceSpan dense_sources,
    SparseSourceSpan sparse_sources) {
  // There shouldn't be more than one dense source because they override each
  // other.
  DCHECK_LE(dense_sources.size(), 1);

  for (auto& source : sparse_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return *res;
    }
  }
  for (auto& source : dense_sources) {
    if (std::optional<DataItem> res = source->Get(id); res.has_value()) {
      return *res;
    }
  }
  return DataItem();
}

}  // namespace koladata::internal
