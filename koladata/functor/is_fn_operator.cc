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
#include "koladata/functor/is_fn_operator.h"
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_op.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/operators/utils.h"
#include "koladata/functor/functor.h"
#include "koladata/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<DataSlice> IsFn(const DataSlice& x) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(x));
  return ops::AsMask(is_functor);
}

absl::StatusOr<DataSlice> HasFn(const DataSlice& x) {
  if (x.is_item()) {
    return IsFn(x);
  }
  // Returns an empty mask with the same shape as `slice`.
  auto get_empty_mask = [](const DataSlice& slice) {
    return DataSlice::Create(
        internal::DataSliceImpl::CreateEmptyAndUnknownType(slice.size()),
        slice.GetShape(), internal::DataItem(schema::kMask), nullptr);
  };

  // Returns a mask indicating if each item is a functor or not. Requires
  // slice to have a bag and only contain ObjectIds.
  auto has_fn = [](const DataSlice& slice) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto returns, slice.HasAttr(kReturnsAttrName));
    ASSIGN_OR_RETURN(auto signature, slice.HasAttr(kSignatureAttrName));
    return DataSliceOp<internal::PresenceAndOp>()(
        returns, signature, internal::DataItem(schema::kMask), /*db=*/nullptr);
  };

  if (x.GetBag() == nullptr || !(x.GetSchema().IsStructSchema() ||
                                 x.GetSchemaImpl().is_object_schema())) {
    return get_empty_mask(x);
  }
  if (x.dtype() == arolla::GetQType<internal::ObjectId>()) {
    return has_fn(x);
  }

  // Mixed case. The ObjectIds are extracted to allow HasAttr to be called.
  std::optional<arolla::DenseArray<internal::ObjectId>> object_ids;
  x.slice().VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      object_ids = values;
    }
  });
  if (!object_ids.has_value()) {
    return get_empty_mask(x);
  }
  ASSIGN_OR_RETURN(
      auto object_slice,
      DataSlice::Create(internal::DataSliceImpl::Create(*std::move(object_ids)),
                        x.GetShape(), x.GetSchemaImpl(), x.GetBag()));
  return has_fn(object_slice);
}

}  // namespace koladata::functor
