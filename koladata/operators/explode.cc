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
#include "koladata/operators/explode.h"

#include <cstdint>
#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Explode(const DataSlice& x, const int64_t ndim) {
  if (ndim == 0) {
    return x;
  }

  DataSlice result = x;
  if (ndim < 0) {
    // Explode until items are no longer lists.
    while (true) {
      if (result.GetSchemaImpl() == schema::kAny) {
        return absl::InvalidArgumentError(
            "cannot fully explode 'x' with ANY schema");
      }

      if (result.GetSchemaImpl() == schema::kObject &&
          result.present_count() == 0) {
        return absl::InvalidArgumentError(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous");
      }

      if (!result.ContainsOnlyLists()) break;
      ASSIGN_OR_RETURN(result, result.ExplodeList(0, std::nullopt));
    }
  } else {
    for (int i = 0; i < ndim; ++i) {
      if (!result.ContainsOnlyLists()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "cannot explode 'x' to have additional %d dimension(s), the "
            "maximum number of additional dimension(s) is %d",
            ndim, i));
      }

      ASSIGN_OR_RETURN(result, result.ExplodeList(0, std::nullopt));
    }
  }
  return result;
}

}  // namespace koladata::ops
