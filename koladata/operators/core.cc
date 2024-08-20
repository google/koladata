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
#include "koladata/operators/core.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/arolla_bridge.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval("kde.core._add_impl", {x, y});
}

DataSlice NoDb(const DataSlice& ds) { return ds.WithDb(nullptr); }

absl::StatusOr<DataBagPtr> GetDb(const DataSlice& ds) {
  if (auto result = ds.GetDb()) {
    return result;
  }
  return absl::InvalidArgumentError("DataSlice has no associated DataBag");
}

DataSlice WithDb(const DataSlice& ds, const DataBagPtr& db) {
  return ds.WithDb(db);
}

absl::StatusOr<DataSlice> AggAny(const DataSlice& x) {
  return SimpleAggIntoEval("core.any", {x}, internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> AggAll(const DataSlice& x) {
  return SimpleAggIntoEval("core.all", {x}, internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x) {
  return SimpleAggOverEval("array.inverse_mapping", {x});
}

absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending) {
  if (descending.GetShape().rank() != 0 ||
      !descending.item().holds_value<bool>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected `descending` to be a scalar boolean value, got %s",
        arolla::Repr(descending)));
  }
  ASSIGN_OR_RETURN(auto tie_breaker_primitive_schema,
                   GetPrimitiveArollaSchema(tie_breaker));
  if (tie_breaker_primitive_schema.has_value()) {
    return SimpleAggOverEval(
        "array.ordinal_rank", {x, tie_breaker, descending},
        /*output_schema=*/internal::DataItem(schema::kInt64), /*edge_index=*/2);
  } else {
    // `tie_breaker` _must_ be an integral, while the other data can be of other
    // types. We therefore fix the schema of `tie_breaker` to be INT64 to avoid
    // type errors.
    ASSIGN_OR_RETURN(
        auto tie_breaker_int64,
        tie_breaker.WithSchema(internal::DataItem(schema::kInt64)));
    return SimpleAggOverEval(
        "array.ordinal_rank", {x, std::move(tie_breaker_int64), descending},
        /*output_schema=*/internal::DataItem(schema::kInt64), /*edge_index=*/2);
  }
}

}  // namespace koladata::ops
