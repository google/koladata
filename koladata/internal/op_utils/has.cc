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
#include "koladata/internal/op_utils/has.h"

#include <optional>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

arolla::DenseArray<arolla::Unit> koladata::internal::PresenceDenseArray(
    const DataSliceImpl& ds) {
  if (ds.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<arolla::Unit>(ds.size());
  }

  arolla::EvaluationContext ctx;
  std::optional<arolla::DenseArray<arolla::Unit>> result = std::nullopt;
  CHECK_OK(ds.VisitValues([&](const auto& array) -> absl::Status {
    auto array_presence = arolla::DenseArrayHasOp()(array);
    if (!result.has_value()) {
      result = std::move(array_presence);
    } else {
      ASSIGN_OR_RETURN(result, arolla::DenseArrayPresenceOrOp()(
                                   &ctx, array_presence, *result));
    }
    return absl::OkStatus();
  }));
  return *result;
}
