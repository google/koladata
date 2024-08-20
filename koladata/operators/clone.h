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
#ifndef KOLADATA_OPERATORS_CLONE_H_
#define KOLADATA_OPERATORS_CLONE_H_

#include <utility>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/operators/extract.h"
#include "koladata/operators/shallow_clone.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core._clone
inline absl::StatusOr<DataSlice> Clone(const DataSlice& ds,
                                       const DataSlice& schema) {
  const auto& db = ds.GetDb();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  ASSIGN_OR_RETURN(DataSlice shallow_clone, ShallowClone(ds, schema));
  DataSlice shallow_clone_with_fallback = shallow_clone.WithDb(
      DataBag::ImmutableEmptyWithFallbacks({shallow_clone.GetDb(), db}));
  return Extract(std::move(shallow_clone_with_fallback), schema);
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CLONE_H_
