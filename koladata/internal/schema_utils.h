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
#ifndef KOLADATA_INTERNAL_SCHEMA_UTILS_H_
#define KOLADATA_INTERNAL_SCHEMA_UTILS_H_

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::schema {

namespace schema_internal {

// Only used during program initialization.
using DTypeLattice =
    absl::flat_hash_map<schema::DType, std::vector<schema::DType>>;

// Returns the lattice of DTypes used for CommonSchema resolution.
//
// See go/koda-type-promotion for more details.
//
// Exposed mainly for testing.
const DTypeLattice& GetDTypeLattice();

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

constexpr absl::string_view kSchemaAttr = "__schema__";
constexpr absl::string_view kListItemsSchemaAttr = "__items__";
constexpr absl::string_view kDictKeysSchemaAttr = "__keys__";
constexpr absl::string_view kDictValuesSchemaAttr = "__values__";

constexpr absl::string_view kImplicitSchemaSeed = "__implicit_schema__";
constexpr absl::string_view kNoFollowSchemaSeed = "__nofollow_schema__";

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
  // be found because no schemas were seen, `kObject` is returned.
  absl::StatusOr<internal::DataItem> Get() &&;

 private:
  schema_internal::CommonDTypeAggregator dtype_agg_;
  std::optional<internal::ObjectId> res_object_id_;
  absl::Status status_;
};

// Validates and returns the schema according to the type promotion lattice
// defined in go/koda-type-promotion. If the `schema` is not a schema, an error
// is returned. If `schema` is missing, `kObject` is returned.
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
// missing, `kObject` is returned.
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
// schema cannot be found, because all `schema_ids` are missing, `kObject` is
// returned.
absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids);

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

// Returns true if the schema_item are entity, OBJECT, ANY or ITEMID.
bool VerifySchemaForItemIds(const internal::DataItem& schema_item);

// Validates that the given schema can be used for dict keys. The caller must
// guarantee that the argument is a schema.
absl::Status VerifyDictKeySchema(const internal::DataItem& schema_item);

}  // namespace koladata::schema

#endif  // KOLADATA_INTERNAL_SCHEMA_UTILS_H_
