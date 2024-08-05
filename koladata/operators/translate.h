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
#ifndef KOLADATA_OPERATORS_TRANSLATE_H_
#define KOLADATA_OPERATORS_TRANSLATE_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/object_factories.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.core.translate.
inline absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                           const DataSlice& keys_from,
                                           const DataSlice& values_from) {
  const auto& from_shape = keys_from.GetShape();
  if (!from_shape.IsEquivalentTo(values_from.GetShape())) {
    return absl::InvalidArgumentError(
        "keys_from and values_from must have the same shape");
  }

  const auto& to_shape = keys_to.GetShape();
  if (to_shape.rank() == 0 || from_shape.rank() == 0) {
    return absl::InvalidArgumentError(
        "keys_to, keys_from and values_from must have at least one dimension");
  }

  const auto shape_without_last_dim = to_shape.RemoveDims(to_shape.rank() - 1);
  if (!from_shape.RemoveDims(from_shape.rank() - 1)
           .IsEquivalentTo(shape_without_last_dim)) {
    return absl::InvalidArgumentError(
        "keys_from and keys_to must have the same dimensions except the last "
        "one");
  }

  if (keys_from.GetSchemaImpl() != keys_to.GetSchemaImpl()) {
    return absl::InvalidArgumentError(
        "keys_from and keys_to must have the same schema");
  }

  auto temp_db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto lookup,
      CreateDictShaped(temp_db, shape_without_last_dim,
                       keys_from.WithDb(nullptr), values_from.WithDb(nullptr)));
  ASSIGN_OR_RETURN(auto unique_keys, lookup.GetDictKeys());
  if (!unique_keys.GetShape().IsEquivalentTo(keys_from.GetShape())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "keys_from must be unique within each group of the last dimension: "
        "original shape %s vs shape after dedup %s. Consider using "
        "translate_group instead.",
        arolla::Repr(keys_from.GetShape()),
        arolla::Repr(unique_keys.GetShape())));
  }
  ASSIGN_OR_RETURN(auto res, lookup.GetFromDict(keys_to));
  return res.WithDb(values_from.GetDb());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_TRANSLATE_H_
