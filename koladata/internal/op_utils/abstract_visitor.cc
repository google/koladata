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
#include "koladata/internal/op_utils/abstract_visitor.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

// Returns the ObjectIds of `ds` (aligned with `ds`), or nullopt if `ds` has no
// ObjectId values.
std::optional<arolla::DenseArray<ObjectId>> GetObjectIds(
    const DataSliceImpl& ds) {
  if (ds.dtype() == arolla::GetQType<ObjectId>()) {
    return ds.values<ObjectId>();
  }
  std::optional<arolla::DenseArray<ObjectId>> objects;
  if (ds.is_mixed_dtype()) {
    ds.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if constexpr (std::is_same_v<T, ObjectId>) {
        objects = array;
      }
    });
  }
  return objects;
}

std::optional<ObjectId> FirstPresentObjectId(
    const arolla::DenseArray<ObjectId>& objects) {
  for (int64_t i = 0; i < objects.size(); ++i) {
    if (objects.present(i)) {
      return objects.values[i];
    }
  }
  return std::nullopt;
}

}  // namespace

absl::StatusOr<bool> AbstractVisitor::Previsit(
    const DataItem& from_item, const DataItem& from_schema,
    const std::optional<TransitionKey>& transition_key, const DataItem& item,
    const DataItem& schema) {
  return true;
}

absl::StatusOr<DataSliceImpl> AbstractVisitor::PrevisitSlice(
    const DataSliceImpl& ds, const DataItem& schema,
    const std::optional<TransitionKey>& transition_key,
    const DataItem& parent_token) {
  return SameTokenForAllObjects(ds);
}

absl::StatusOr<DataSliceImpl> AbstractVisitor::PrevisitObjectSchemas(
    const DataSliceImpl& objects, const DataSliceImpl& schemas,
    const DataItem& token) {
  return DataSliceImpl::Create(objects.size(), token);
}

absl::StatusOr<DataSliceImpl> SameTokenForAllObjects(const DataSliceImpl& ds) {
  std::optional<arolla::DenseArray<ObjectId>> objects = GetObjectIds(ds);
  std::optional<ObjectId> token =
      objects.has_value() ? FirstPresentObjectId(*objects) : std::nullopt;
  if (!token.has_value()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(ds.size());
  }
  // `token` for each present ObjectId.
  arolla::EvaluationContext ctx;
  ASSIGN_OR_RETURN(
      arolla::DenseArray<ObjectId> tokens,
      arolla::DenseArrayPresenceAndOp()(
          &ctx, arolla::CreateConstDenseArray<ObjectId>(ds.size(), *token),
          arolla::DenseArrayHasOp()(*objects)));
  return DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet(AllocationId(*token)), std::move(tokens));
}

}  // namespace koladata::internal
