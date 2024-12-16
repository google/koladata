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
#include "koladata/operators/core_dict.h"

#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/object_factories.h"
#include "koladata/uuid_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> DictSize(const DataSlice& dicts) {
  const auto& db = dicts.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "Not possible to get Dict size without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*db);
  internal::DataItem schema(schema::kInt64);
  return dicts.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    return DataSlice::Create(
        db->GetImpl().GetDictSize(impl, fb_finder.GetFlattenFallbacks()),
        dicts.GetShape(), std::move(schema), /*db=*/nullptr);
  });
}

absl::StatusOr<DataBagPtr> DictUpdate(const DataSlice& x, const DataSlice& keys,
                                      const DataSlice& values) {
  if (x.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot update a DataSlice of dicts without a DataBag");
  }
  if (!x.IsDict()) {
    return absl::InvalidArgumentError("expected a DataSlice of dicts");
  }

  DataBagPtr result_db = DataBag::Empty();
  RETURN_IF_ERROR(AdoptStub(result_db, x));
  RETURN_IF_ERROR(x.WithBag(result_db).SetInDict(keys, values));
  result_db->UnsafeMakeImmutable();
  return result_db;
}

absl::StatusOr<DataSlice> DictShaped(
    const DataSlice::JaggedShape& shape, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken) {
  DataBagPtr db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto result,
      CreateDictShaped(
          db, shape,
          IsUnspecifiedDataSlice(keys) ? std::nullopt
                                       : std::make_optional(keys),
          IsUnspecifiedDataSlice(values) ? std::nullopt
                                         : std::make_optional(values),
          IsUnspecifiedDataSlice(schema) ? std::nullopt
                                         : std::make_optional(schema),
          IsUnspecifiedDataSlice(key_schema) ? std::nullopt
                                             : std::make_optional(key_schema),
          IsUnspecifiedDataSlice(value_schema)
              ? std::nullopt
              : std::make_optional(value_schema),
          IsUnspecifiedDataSlice(itemid) ? std::nullopt
                                         : std::make_optional(itemid)));
  db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> DictLike(
    const DataSlice& shape_and_mask_from, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken) {
  DataBagPtr db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto result,
      CreateDictLike(
          db, shape_and_mask_from,
          IsUnspecifiedDataSlice(keys) ? std::nullopt
                                       : std::make_optional(keys),
          IsUnspecifiedDataSlice(values) ? std::nullopt
                                         : std::make_optional(values),
          IsUnspecifiedDataSlice(schema) ? std::nullopt
                                         : std::make_optional(schema),
          IsUnspecifiedDataSlice(key_schema) ? std::nullopt
                                             : std::make_optional(key_schema),
          IsUnspecifiedDataSlice(value_schema)
              ? std::nullopt
              : std::make_optional(value_schema),
          IsUnspecifiedDataSlice(itemid) ? std::nullopt
                                         : std::make_optional(itemid)));
  db->UnsafeMakeImmutable();
  return result;
}

}  // namespace koladata::ops
