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
#ifndef KOLADATA_INTERNAL_SCHEMA_UTILS_H_
#define KOLADATA_INTERNAL_SCHEMA_UTILS_H_

#include <array>
#include <bitset>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::schema {

namespace schema_internal {

// Only used during program initialization.
using DTypeLattice = std::array<std::array<bool, kNextDTypeId>, kNextDTypeId>;

// Returns the lattice of DTypes used for CommonSchema resolution.
//
// See go/koda-type-promotion for more details.
//
// Each "row" in the lattice matrix represents a DType, and each "column"
// represents a directly adjacent greater DType.
constexpr DTypeLattice GetDTypeLattice() {
  DTypeLattice lattice = {};
  constexpr auto fill_row = [&](std::initializer_list<DType> adjacent_dtypes) {
    std::array<bool, kNextDTypeId> row = {};
    for (DType adjacent_dtype : adjacent_dtypes) {
      row[adjacent_dtype.type_id()] = true;
    }
    return row;
  };
  lattice[kNone.type_id()] = fill_row(
      {kItemId, kSchema, kInt32, kMask, kBool, kBytes, kString, kExpr});
  lattice[kItemId.type_id()] = fill_row({});
  lattice[kSchema.type_id()] = fill_row({});
  lattice[kInt32.type_id()] = fill_row({kInt64});
  lattice[kInt64.type_id()] = fill_row({kFloat32});
  lattice[kFloat32.type_id()] = fill_row({kFloat64});
  lattice[kFloat64.type_id()] = fill_row({kObject});
  lattice[kMask.type_id()] = fill_row({kObject});
  lattice[kBool.type_id()] = fill_row({kObject});
  lattice[kBytes.type_id()] = fill_row({kObject});
  lattice[kString.type_id()] = fill_row({kObject});
  lattice[kExpr.type_id()] = fill_row({kObject});
  lattice[kObject.type_id()] = fill_row({});
  return lattice;
}

// Returns a matrix where m[i][j] is true iff j is reachable from i.
constexpr DTypeLattice GetReachableDTypes() {
  // Initialize the adjacency matrix.
  schema_internal::DTypeLattice reachable_dtypes =
      schema_internal::GetDTypeLattice();
  for (DTypeId i = 0; i < kNextDTypeId; ++i) {
    reachable_dtypes[i][i] = true;
  }
  // Floyd-Warshall to find all reachable nodes.
  for (DTypeId k = 0; k < kNextDTypeId; ++k) {
    for (DTypeId i = 0; i < kNextDTypeId; ++i) {
      if (!reachable_dtypes[i][k]) {
        continue;
      }
      for (DTypeId j = 0; j < kNextDTypeId; ++j) {
        reachable_dtypes[i][j] |= reachable_dtypes[k][j];
      }
    }
  }
  return reachable_dtypes;
}

// A helper class to aggregate seen dtypes and return the common dtype.
class CommonDTypeAggregator {
 public:
  // Marks the dtype as seen.
  void Add(DType dtype) { seen_dtypes_ |= (Mask{1} << dtype.type_id()); }

  // Returns the common dtype out of the seen dtypes.
  std::optional<DType> Get(absl::Status& status) const;

 private:
  using Mask = uint16_t;
  static_assert(kNextDTypeId <= sizeof(Mask) * 8);
  Mask seen_dtypes_ = 0;
};

}  // namespace schema_internal

// Finds the supremum schema of all seen schemas according to the type promotion
// lattice defined in go/koda-type-promotion.
class CommonSchemaAggregator {
 public:
  // Marks the schema as seen.
  void Add(const internal::DataItem& schema);

  // Marks the dtype as seen.
  void Add(DType dtype) { dtype_agg_.Add(dtype); }

  // Marks the schema_obj as seen.
  void Add(internal::ObjectId schema_obj);

  // Returns the common schema or an appropriate error. If no common schema can
  // be found because no schemas were seen, an empty DataItem is returned.
  absl::StatusOr<internal::DataItem> Get() &&;

 private:
  schema_internal::CommonDTypeAggregator dtype_agg_;
  std::optional<internal::ObjectId> res_object_id_;
  absl::Status status_;
};

// Validates and returns the schema according to the type promotion lattice
// defined in go/koda-type-promotion. If the `schema` is not a schema, an error
// is returned. If `schema` is missing, an empty DataItem is returned.
//
// This is a convenience wrapper around CommonSchemaAggregator on a single
// value.
inline absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataItem& schema) {
  CommonSchemaAggregator agg;
  agg.Add(schema);
  return std::move(agg).Get();
}

// Finds the supremum schema of lhs and rhs according to the type promotion
// lattice defined in go/koda-type-promotion. If common / supremum schema cannot
// be determined, appropriate error is returned. If both lhs and rhs are
// missing, an empty DataItem is returned.
//
// This is a convenience wrapper around CommonSchemaAggregator on two elements.
inline absl::StatusOr<internal::DataItem> CommonSchema(DType lhs, DType rhs) {
  CommonSchemaAggregator agg;
  agg.Add(lhs);
  agg.Add(rhs);
  return std::move(agg).Get();
}
inline absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataItem& lhs, const internal::DataItem& rhs) {
  CommonSchemaAggregator agg;
  agg.Add(lhs);
  agg.Add(rhs);
  return std::move(agg).Get();
}

// Finds the supremum schema of all schemas in `schema_ids` according to the
// type promotion lattice defined in go/koda-type-promotion. If common /
// supremum schema cannot be determined, appropriate error is returned. If
// schema cannot be found, because all `schema_ids` are missing, an empty
// DataItem is returned.
absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids);

// Returns true iff `from_schema` is implicitly castable to `to_schema`.
// _Requires_ both to be schemas.
bool IsImplicitlyCastableTo(const internal::DataItem& from_schema,
                            const internal::DataItem& to_schema);

// Returns a NoFollow schema item that wraps `schema_item`. In case
// `schema_item` is not schema, or it is a schema for which NoFollow is not
// allowed, error is returned. This function is reversible with
// `GetNoFollowedSchemaItem`.
//
// This function is successful on OBJECT and all ObjectId schemas (implicit and
// explicit).
absl::StatusOr<internal::DataItem> NoFollowSchemaItem(
    const internal::DataItem& schema_item);

// Returns original schema item from a NoFollow schema item. Returns an error if
// the input is not a NoFollow schema.
absl::StatusOr<internal::DataItem> GetNoFollowedSchemaItem(
    const internal::DataItem& nofollow_schema_item);

// Returns true if the schema_item are entity, OBJECT or ITEMID.
bool VerifySchemaForItemIds(const internal::DataItem& schema_item);

// Validates that the given schema can be used for dict keys. The caller must
// guarantee that the argument is a schema.
absl::Status VerifyDictKeySchema(const internal::DataItem& schema_item);

// Returns the schema of the underlying data. If the slice holds ObjectIds and
// `db_impl` is provided, the `__schema__` will be extracted. If the schema is
// ambiguous (e.g. the slice holds ObjectIds and `db_impl` is null), or there is
// no common schema of the underlying data an empty internal::DataItem is
// returned.
internal::DataItem GetDataSchema(
    const internal::DataItem& item,
    const internal::DataBagImpl* absl_nullable db_impl = nullptr,
    internal::DataBagImpl::FallbackSpan fallbacks = {});
internal::DataItem GetDataSchema(
    const internal::DataSliceImpl& slice,
    const internal::DataBagImpl* absl_nullable db_impl = nullptr,
    internal::DataBagImpl::FallbackSpan fallbacks = {});

}  // namespace koladata::schema

#endif  // KOLADATA_INTERNAL_SCHEMA_UTILS_H_
