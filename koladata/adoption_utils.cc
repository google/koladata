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
#include "koladata/adoption_utils.h"

#include <optional>
#include <type_traits>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/traceme.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/error_repr_utils.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

absl::Status AdoptionQueue::AdoptInto(DataBag& db) const {
  arolla::profiling::TraceMe traceme("::koladata::AdoptionQueue::AdoptInto");
  absl::flat_hash_set<const DataBag*> visited_bags{&db};
  for (const DataBagPtr& other_db : bags_to_merge_) {
    if (visited_bags.contains(other_db.get())) {
      continue;
    }
    visited_bags.insert(other_db.get());
    RETURN_IF_ERROR(db.MergeInplace(other_db, /*overwrite=*/false,
                                    /*allow_data_conflicts=*/false,
                                    /*allow_schema_conflicts=*/false))
        .With(KodaErrorCausedByMergeConflictError(db.Freeze(), other_db));
  }
  for (const DataSlice& slice : slices_to_merge_) {
    if (visited_bags.contains(slice.GetBag().get())) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice extracted_slice,
                     extract_utils_internal::Extract(slice));
    const auto& extracted_db = extracted_slice.GetBag();
    if (extracted_db == nullptr) {
      continue;
    }
    // NOTE: Consider moving the cancellation check into db.MergeInplace().
    RETURN_IF_ERROR(arolla::CheckCancellation());
    RETURN_IF_ERROR(db.MergeInplace(extracted_db, /*overwrite=*/false,
                                    /*allow_data_conflicts=*/false,
                                    /*allow_schema_conflicts=*/false))
        .With(KodaErrorCausedByMergeConflictError(db.Freeze(), extracted_db));
  }
  return absl::OkStatus();
}

absl::StatusOr<absl_nullable DataBagPtr> AdoptionQueue::GetCommonOrMergedDb()
    const {
  // Check whether all bags (and slices' bags) are the same bag. If so, we
  // return that bag instead of merging.
  bool has_multiple_bags = false;
  const DataBagPtr* single_bag = nullptr;
  for (const DataBagPtr& bag : bags_to_merge_) {
    if (single_bag == nullptr) {
      single_bag = &bag;
    } else if (*single_bag != bag) {
      has_multiple_bags = true;
      break;
    }
  }
  if (!has_multiple_bags) {
    for (const DataSlice& slice : slices_to_merge_) {
      const DataBagPtr& slice_bag = slice.GetBag();
      if (single_bag == nullptr) {
        single_bag = &slice_bag;
      } else if (*single_bag != slice_bag) {
        has_multiple_bags = true;
        break;
      }
    }
  }

  DCHECK_EQ(slices_to_merge_.empty() && bags_to_merge_.empty(),
            single_bag == nullptr);
  if (single_bag == nullptr) {
    return nullptr;
  } else if (!has_multiple_bags) {
    return *single_bag;
  } else {
    DataBagPtr res = DataBag::EmptyMutable();
    RETURN_IF_ERROR(AdoptInto(*res));
    res->UnsafeMakeImmutable();
    return res;
  }
}

absl_nonnull DataBagPtr AdoptionQueue::GetBagWithFallbacks() const {
  // Collect unique DataBags from all Add calls.
  absl::flat_hash_set<const DataBag*> visited_bags;
  std::vector<DataBagPtr> fallbacks;
  fallbacks.reserve(slices_to_merge_.size() + bags_to_merge_.size());
  for (const DataBagPtr& bag : bags_to_merge_) {
    if (bag.get() == nullptr || visited_bags.contains(bag.get())) {
      continue;
    }
    visited_bags.insert(bag.get());
    fallbacks.push_back(bag->Freeze());
  }
  for (const DataSlice& slice : slices_to_merge_) {
    const DataBagPtr& bag = slice.GetBag();
    if (bag.get() == nullptr || visited_bags.contains(bag.get())) {
      continue;
    }
    visited_bags.insert(bag.get());
    fallbacks.push_back(bag->Freeze());
  }

  // Note: ImmutableEmptyWithFallbacks returns an error if fallbacks are
  // mutable. Here we explicitly freeze all fallbacks, so we assume that
  // the error can not happen.
  return *DataBag::ImmutableEmptyWithFallbacks(fallbacks);
}

absl::Status AdoptStub(const DataBagPtr& db, const DataSlice& x) {
  if (x.GetBag() == nullptr) {
    return absl::OkStatus();
  }

  DataSlice slice = x;
  while (true) {
    DataSlice result_slice = slice.WithBag(db);
    DataSlice schema = slice.GetSchema();

    if (slice.IsEmpty() &&
        (schema.item() == schema::kObject || schema.item() == schema::kNone)) {
      // Nothing to do here, but the code below would run infinitely since it
      // looks like a list.
      break;
    }

    if (schema.item() == schema::kObject) {
      if (slice.IsEmpty() ||
          (!slice.impl_has_mixed_dtype() && slice.ContainsAnyPrimitives())) {
        break;  // No object ids left to process.
      }

      // If slice contains a mixture of object ids and primitives, isolate just
      // the object ids so we can successfully set kSchemaAttr.
      if (slice.impl_has_mixed_dtype() && slice.ContainsAnyPrimitives()) {
        DCHECK(!slice.is_item());  // Item cannot have mixed dtype.
        const arolla::DenseArray<internal::ObjectId>* object_ids = nullptr;
        slice.slice().VisitValues(
            [&]<typename T>(const arolla::DenseArray<T>& values) {
              if constexpr (std::is_same_v<T, internal::ObjectId>) {
                object_ids = &values;
              }
            });
        if (object_ids == nullptr) {
          break;  // No object ids left to process.
        }
        ASSIGN_OR_RETURN(
            result_slice,
            DataSlice::Create(internal::DataSliceImpl::Create(*object_ids),
                              slice.GetShape(), schema.item(), db));
      }

      ASSIGN_OR_RETURN(schema, slice.GetObjSchema());
      RETURN_IF_ERROR(result_slice.SetAttr(schema::kSchemaAttr, schema));
    }

    auto copy_schema_attr = [&](absl::string_view attr_name) -> absl::Status {
      ASSIGN_OR_RETURN(const auto& values, schema.GetAttr(attr_name));
      return schema.WithBag(db).SetAttr(attr_name, values);
    };

    if (slice.IsList()) {
      RETURN_IF_ERROR(copy_schema_attr(schema::kListItemsSchemaAttr));
      ASSIGN_OR_RETURN(slice, slice.ExplodeList(0, std::nullopt));
      RETURN_IF_ERROR(result_slice.ReplaceInList(0, std::nullopt, slice));
      continue;  // Stub list items recursively.
    }
    if (slice.IsDict()) {
      RETURN_IF_ERROR(copy_schema_attr(schema::kDictKeysSchemaAttr));
      RETURN_IF_ERROR(copy_schema_attr(schema::kDictValuesSchemaAttr));
    }
    break;
  }
  return absl::OkStatus();
}

absl::StatusOr<absl_nullable DataBagPtr> WithAdoptedValues(
    const absl_nullable DataBagPtr& db, const DataSlice& slice) {
  if (db != nullptr && (db->IsMutable() || db->HasMutableFallbacks())) {
    return absl::InvalidArgumentError(
        "WithAdoptedValues requires the DataBag to be immutable; "
        "either freeze it, or use inplace merge.");
  }
  if (db == nullptr || db == slice.GetBag()) {
    return slice.GetBag();
  } else if (slice.GetBag() == nullptr) {
    return db;
  } else {
    ASSIGN_OR_RETURN(DataSlice extracted_slice,
                     extract_utils_internal::Extract(slice));
    // NOTE: slices's bag should come first to respect its precedence.
    return DataBag::ImmutableEmptyWithFallbacks({extracted_slice.GetBag(), db});
  }
}

}  // namespace koladata
