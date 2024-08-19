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

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/convert_and_eval.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y) {
  return SimplePointwiseEval(
      std::make_shared<arolla::expr::RegisteredOperator>("kde.core._add_impl"),
      {x, y});
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
  return SimpleAggIntoEval(
      std::make_shared<arolla::expr::RegisteredOperator>("core.any"), x,
      internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> AggAll(const DataSlice& x) {
  return SimpleAggIntoEval(
      std::make_shared<arolla::expr::RegisteredOperator>("core.all"), x,
      internal::DataItem(schema::kMask));
}

}  // namespace koladata::ops
