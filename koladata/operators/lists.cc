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

#include <algorithm>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/object_factories.h"
#include "koladata/uuid_utils.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/qtype/optional_qtype.h"
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
      if (result.GetSchemaImpl() == schema::kNone ||
          result.GetSchemaImpl() == schema::kItemId) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "cannot fully explode 'x' with %v schema", result.GetSchemaImpl()));
      }

      if (result.GetSchemaImpl() == schema::kObject &&
          result.present_count() == 0) {
        return absl::InvalidArgumentError(
            "cannot fully explode 'x' with OBJECT schema and all-missing items,"
            " because the correct number of times to explode is ambiguous");
      }

      if (!result.IsList()) break;
      ASSIGN_OR_RETURN(result, result.ExplodeList(0, std::nullopt));
    }
  } else {
    for (int i = 0; i < ndim; ++i) {
      if (!result.IsList()) {
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

absl::StatusOr<DataSlice> Implode(const DataSlice& x, int64_t ndim,
                                  const DataSlice& itemid,
                                  internal::NonDeterministicToken) {
  auto db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto result,
      Implode(
          db, x, ndim,
          IsUnspecifiedDataSlice(itemid) ? std::nullopt
                                         : std::make_optional(itemid)));
  db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> ListSize(const DataSlice& lists) {
  const auto& db = lists.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "Not possible to get List size without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*db);
  internal::DataItem schema(schema::kInt64);
  return lists.VisitImpl([&]<class T>(
                             const T& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_impl, db->GetImpl().GetListSize(
                                        impl, fb_finder.GetFlattenFallbacks()));
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      return DataSlice::Create(std::move(res_impl), lists.GetShape(),
                               std::move(schema), /*db=*/nullptr);
    } else {
      return DataSlice::Create(
          internal::DataSliceImpl::Create(std::move(res_impl)),
          lists.GetShape(), std::move(schema), /*db=*/nullptr);
    }
  });
}

absl::StatusOr<DataSlice> ListLike(
    const DataSlice& shape_and_mask_from, const DataSlice& items,
    const DataSlice& item_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken) {
  auto db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto result,
      CreateListLike(
          db, shape_and_mask_from,
          IsUnspecifiedDataSlice(items) ? std::nullopt
                                        : std::make_optional(items),
          IsUnspecifiedDataSlice(schema) ? std::nullopt
                                         : std::make_optional(schema),
          IsUnspecifiedDataSlice(item_schema) ? std::nullopt
                                              : std::make_optional(item_schema),
          IsUnspecifiedDataSlice(itemid) ? std::nullopt
                                         : std::make_optional(itemid)));
  db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> ListShaped(
    const DataSlice::JaggedShape& shape, const DataSlice& items,
    const DataSlice& item_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken) {
  auto db = DataBag::Empty();
  ASSIGN_OR_RETURN(
      auto result,
      CreateListShaped(
          db, shape,
          IsUnspecifiedDataSlice(items) ? std::nullopt
                                        : std::make_optional(items),
          IsUnspecifiedDataSlice(schema) ? std::nullopt
                                         : std::make_optional(schema),
          IsUnspecifiedDataSlice(item_schema) ? std::nullopt
                                              : std::make_optional(item_schema),
          IsUnspecifiedDataSlice(itemid) ? std::nullopt
                                         : std::make_optional(itemid)));
  db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> ConcatLists(std::vector<DataSlice> lists) {
  const DataBagPtr db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto result, ConcatLists(db, std::move(lists)));
  db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> ListAppended(const DataSlice& x,
                                       const DataSlice& append,
                                       internal::NonDeterministicToken) {
  if (x.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot update a DataSlice of lists; "
        "the DataSlice is a reference without a Bag");
  }
  if (!x.IsList()) {
    return absl::InvalidArgumentError(
        "expected first argument to be a DataSlice of lists");
  }

  DataBagPtr result_db = DataBag::Empty();
  ASSIGN_OR_RETURN(auto list_items, Explode(x, 1));
  ASSIGN_OR_RETURN(
      auto result,
      CreateListLike(result_db, x, std::move(list_items), x.GetSchema()));
  RETURN_IF_ERROR(result.AppendToList(append));
  result_db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataBagPtr> ListAppendUpdate(const DataSlice& x,
                                            const DataSlice& append) {
  if (x.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot update a DataSlice of lists; "
        "the DataSlice is a reference without a Bag");
  }
  if (!x.IsList()) {
    return absl::InvalidArgumentError(
        "expected first argument to be a DataSlice of lists");
  }
  DataBagPtr result_db = DataBag::Empty();
  // Adopt stub already copies the list elements, so we don't need to copy them
  // ourselves.
  RETURN_IF_ERROR(AdoptStub(result_db, x));
  RETURN_IF_ERROR(x.WithBag(result_db).AppendToList(append));
  result_db->UnsafeMakeImmutable();
  return result_db;
}

}  // namespace koladata::ops
