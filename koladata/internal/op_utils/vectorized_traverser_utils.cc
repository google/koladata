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
#include "koladata/internal/op_utils/vectorized_traverser_utils.h"

#include <cstdint>
#include <type_traits>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal::vectorized_traverser_internal {
namespace {

absl::Status PrimitiveTypeError(schema::DType schema_dtype,
                                arolla::QTypePtr actual_qtype) {
  if (auto actual_dtype = schema::DType::FromQType(actual_qtype);
      actual_dtype.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "during traversal, got a slice with primitive type ", schema_dtype,
        " while the actual content has type ", actual_dtype->name()));
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "during traversal, got a slice with primitive type ", schema_dtype,
      " while the actual content is not a primitive"));
}

}  // namespace

absl::Status ValidatePrevisitTokens(absl::string_view method,
                                    const DataSliceImpl& ds,
                                    const DataSliceImpl& tokens) {
  if (tokens.size() != ds.size()) {
    return absl::InternalError(
        absl::StrFormat("expected %d tokens from %s, got %d", ds.size(), method,
                        tokens.size()));
  }
  if (tokens.present_count() > 0 &&
      tokens.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InternalError(absl::StrFormat(
        "expected ObjectId tokens from %s, got %s", method,
        tokens.is_mixed_dtype() ? "mixed types" : tokens.dtype()->name()));
  }
  return absl::OkStatus();
}

absl::Status ValidatePrimitiveType(const DataItem& item,
                                   const DataItem& schema) {
  DCHECK(schema.is_primitive_schema());
  auto schema_dtype = schema.value<schema::DType>();
  if (!item.has_value() || item.dtype() == schema_dtype.qtype()) {
    return absl::OkStatus();
  }
  return PrimitiveTypeError(schema_dtype, item.dtype());
}

absl::Status ValidatePrimitiveTypes(const DataSliceImpl& ds,
                                    const DataItem& schema) {
  DCHECK(schema.is_primitive_schema());
  auto schema_dtype = schema.value<schema::DType>();
  if (ds.is_empty_and_unknown() || ds.dtype() == schema_dtype.qtype()) {
    return absl::OkStatus();
  }
  arolla::QTypePtr actual_qtype = ds.dtype();
  if (ds.is_mixed_dtype()) {
    ds.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if (arolla::GetQType<T>() != schema_dtype.qtype()) {
        actual_qtype = arolla::GetQType<T>();
      }
    });
  }
  return PrimitiveTypeError(schema_dtype, actual_qtype);
}

absl::StatusOr<bool> ContainsOnlyValidPrimitives(const DataSliceImpl& ds,
                                                 const DataSliceImpl& schemas) {
  DCHECK_EQ(ds.size(), schemas.size());
  if (!schemas.ContainsAnyPrimitives()) {
    return false;
  }
  bool only_primitives = true;
  for (int64_t i = 0; i < ds.size(); ++i) {
    if (!ds.present(i)) {
      continue;
    }
    const DataItem item = ds[i];
    const DataItem schema = schemas[i];
    if (schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item, schema));
    } else if (schema != schema::kObject || item.holds_value<ObjectId>()) {
      only_primitives = false;
    }
  }
  return only_primitives;
}

DataSliceImpl ChildItems(const DataSliceImpl& items,
                         const arolla::DenseArrayEdge& edge, int64_t i) {
  absl::Span<const int64_t> split_points = edge.edge_values().values.span();
  int64_t from = split_points[i];
  int64_t to = split_points[i + 1];
  if (from == to) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  if (from == 0 && to == items.size()) {
    return items;
  }
  return items.SubSlice(from, to - from);
}

}  // namespace koladata::internal::vectorized_traverser_internal
