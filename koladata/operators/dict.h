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
#ifndef KOLADATA_OPERATORS_DICT_H_
#define KOLADATA_OPERATORS_DICT_H_

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"

namespace koladata::ops {

// kde.core.dict_size.
inline absl::StatusOr<DataSlice> DictSize(const DataSlice& dicts) {
  const auto& db = dicts.GetDb();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "Not possible to get Dict size without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*db);
  internal::DataItem schema(schema::kInt64);
  return dicts.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    return DataSlice::Create(
        db->GetImpl().GetDictSize(impl, fb_finder.GetFlattenFallbacks()),
        dicts.GetShapePtr(), std::move(schema), /*db=*/nullptr);
  });
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_DICT_H_
