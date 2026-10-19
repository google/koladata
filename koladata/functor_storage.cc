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
#include "koladata/functor_storage.h"

#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"

namespace koladata::functor {
namespace {

// A marker stored in the DataBag metadata cache for objects that have been
// verified to be functors.
//
// NOTE: Only positive results are cached, to avoid growing the cache when
// `IsFunctor` is called on many non-functor objects (e.g. by `kd.is_fn`).
struct IsFunctorCacheMarker {};

}  // namespace

absl::StatusOr<bool> IsFunctor(const DataSlice& slice) {
  if (!slice.is_item()) {
    return false;
  }
  const auto& bag = slice.GetBag();
  if (bag == nullptr) {
    return false;
  }
  if (slice.item().dtype() != arolla::GetQType<internal::ObjectId>()) {
    return false;
  }
  const auto object_id = slice.item().value<internal::ObjectId>();
  if (bag->GetCachedMetadataOrNull<IsFunctorCacheMarker>(object_id) !=
      nullptr) {
    return true;
  }
  ASSIGN_OR_RETURN(auto returns, slice.HasAttr(kReturnsAttrName));
  if (returns.IsEmpty()) {
    return false;
  }
  ASSIGN_OR_RETURN(auto signature, slice.HasAttr(kSignatureAttrName));
  if (signature.IsEmpty()) {
    return false;
  }
  // Checking mutability upfront avoids unnecessary cache allocation.
  //
  // NOTE: The metadata cache is only populated for immutable bags (without
  // mutable fallbacks), ensuring the cached result never becomes stale.
  if (!bag->IsMutable()) {
    bag->SetCachedMetadata(object_id, IsFunctorCacheMarker{});
  }
  return true;
}

}  // namespace koladata::functor
