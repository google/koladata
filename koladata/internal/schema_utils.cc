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
#include "koladata/internal/schema_utils.h"

#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::schema {

// Adjacency list representation of the DType lattice.
//
// Each "row" in the lattice represents a DType, and each "column" represents a
// directly adjacent greater DType.
const schema_internal::DTypeLattice& schema_internal::GetDTypeLattice() {
  static const absl::NoDestructor<DTypeLattice> lattice({
      {kNone, {kItemId, kSchema, kInt32, kMask, kBool, kBytes, kText, kExpr}},
      {kItemId, {}},
      {kSchema, {}},
      {kInt32, {kInt64}},
      {kInt64, {kFloat32}},
      {kFloat32, {kFloat64}},
      {kFloat64, {kObject}},
      {kMask, {kObject}},
      {kBool, {kObject}},
      {kBytes, {kObject}},
      {kText, {kObject}},
      {kExpr, {kObject}},
      {kObject, {kAny}},
      {kAny, {}},
  });
  return *lattice;
}

namespace {

constexpr DTypeId kUnknownDType = -1;

// Matrix representation of the DTypeLattice.
class DTypeMatrix {
  using MatrixImpl =
      std::array<std::array<DTypeId, kNextDTypeId>, kNextDTypeId>;

 public:
  // Returns the common dtype of `a` and `b`.
  //
  // Requires the inputs to be in [0, kNextDTypeId). Returns kUnknownDType if no
  // common dtype exists.
  static DTypeId CommonDType(DTypeId a, DTypeId b) {
    DCHECK_GE(a, 0);
    DCHECK_LT(a, kNextDTypeId);
    const auto& dtype_matrix = GetMatrixImpl();
    return dtype_matrix[a][b];
  }

 private:
  // Returns a matrix where m[i][j] is true iff j is reachable from i.
  static std::array<std::bitset<kNextDTypeId>, kNextDTypeId>
  GetReachableDTypes() {
    const auto& lattice = schema_internal::GetDTypeLattice();
    // Initialize the adjacency matrix.
    std::array<std::bitset<kNextDTypeId>, kNextDTypeId> reachable_dtypes;
    for (const auto& [dtype_a, adjacent_dtypes] : lattice) {
      auto dtype_a_int = dtype_a.type_id();
      reachable_dtypes[dtype_a_int][dtype_a_int] = true;
      for (const auto dtype_b : adjacent_dtypes) {
        reachable_dtypes[dtype_a_int][dtype_b.type_id()] = true;
      }
    }
    // Floyd-Warshall to find all reachable nodes.
    for (DTypeId k = 0; k < kNextDTypeId; ++k) {
      for (DTypeId i = 0; i < kNextDTypeId; ++i) {
        if (reachable_dtypes[i][k]) {
          reachable_dtypes[i] |= reachable_dtypes[k];
        }
      }
    }
    return reachable_dtypes;
  }

  // Computes the common dtype matrix.
  //
  // Represented as a 2-dim array of size kNextDTypeId x kNextDTypeId, where the
  // value at index [i, j] is the common dtype of dtype i and dtype j. If
  // no such dtype exists, the value is kUnknownDType.
  //
  // See http://shortn/_icYRr51SOr for a proof of correctness.
  static const MatrixImpl& GetMatrixImpl() {
    static const MatrixImpl matrix = [] {
      const auto reachable_dtypes = GetReachableDTypes();
      auto get_common_dtype = [&reachable_dtypes](DTypeId a, DTypeId b) {
        // Compute the common upper bound.
        auto cub = reachable_dtypes[a] & reachable_dtypes[b];
        size_t cub_count = cub.count();
        if (cub_count == 0) {
          return kUnknownDType;
        }
        // Find the unique least upper bound of the common upper bounds. This is
        // the DType in `cub` where all common upper bounds are reachable from
        // it.
        for (DTypeId i = 0; i < kNextDTypeId; ++i) {
          if (cub[i] && cub_count == reachable_dtypes[i].count()) {
            return i;
          }
        }
        LOG(FATAL) << DType(static_cast<DTypeId>(a)) << " and "
                   << DType(static_cast<DTypeId>(b))
                   << " do not have a unique upper bound DType "
                      "- the DType lattice is malformed";
      };
      MatrixImpl matrix;
      for (DTypeId i = 0; i < kNextDTypeId; ++i) {
        for (DTypeId j = 0; j < kNextDTypeId; ++j) {
          matrix[i][j] = get_common_dtype(i, j);
        }
      }
      return matrix;
    }();
    return matrix;
  }
};

// A helper class to aggregate seen dtypes and return the common dtype.
class CommonDTypeAggregator {
 public:
  // Marks the dtype as seen.
  void Add(DType dtype) { seen_dtypes_ |= (Mask{1} << dtype.type_id()); }

  // Returns the common dtype out of the seen dtypes.
  absl::StatusOr<std::optional<DType>> Get() const {
    if (seen_dtypes_ == 0) {
      return std::nullopt;
    }
    DTypeId res_dtype_id = absl::countr_zero(seen_dtypes_);

    for (auto mask = seen_dtypes_; true;) {
      mask &= (mask - 1);  // remove the least significant bit.
      if (mask == 0) {
        return DType(res_dtype_id);
      }
      DTypeId i = absl::countr_zero(mask);
      res_dtype_id = DTypeMatrix::CommonDType(res_dtype_id, i);
      if (ABSL_PREDICT_FALSE(res_dtype_id == kUnknownDType)) {
        return absl::InvalidArgumentError("no common schema");
      }
    }
  }

 private:
  using Mask = uint16_t;
  static_assert(kNextDTypeId <= sizeof(Mask) * 8);
  Mask seen_dtypes_ = 0;
};

}  // namespace

absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids) {
  return CommonSchema(schema_ids, internal::DataItem(kObject));
}

absl::StatusOr<internal::DataItem> CommonSchema(
    absl::Span<const internal::DataItem> schema_ids) {
  internal::DataSliceImpl::Builder bldr(schema_ids.size());
  int i = 0;
  for (const auto& schema_id : schema_ids) {
    bldr.Insert(i++, schema_id);
  }
  return CommonSchema(std::move(bldr).Build());
}

absl::StatusOr<internal::DataItem> CommonSchema(DType lhs, DType rhs) {
  if (auto common_dtype =
          DTypeMatrix::CommonDType(lhs.type_id(), rhs.type_id());
      common_dtype != kUnknownDType) {
    return internal::DataItem(DType(common_dtype));
  }
  return absl::InvalidArgumentError("no common schema");
}

absl::StatusOr<internal::DataItem> CommonSchema(const internal::DataItem& lhs,
                                                const internal::DataItem& rhs) {
  if (lhs.holds_value<DType>() && rhs.holds_value<DType>()) {
    return CommonSchema(lhs.value<DType>(), rhs.value<DType>());
  }
  if (!lhs.is_schema() || !rhs.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected Schemas, got: %v and %v", lhs, rhs));
  }
  if (lhs == rhs) {
    return lhs;
  }
  // NONE is the only dtype that casts to entity.
  if (lhs == kNone) {
    return rhs;
  }
  if (rhs == kNone) {
    return lhs;
  }
  return absl::InvalidArgumentError("no common schema");
}

// TODO: Add SchemaConflict::NoCommonSchema or something similar to
// improve error messages and make them on-par with prod-koladata.
absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids,
    internal::DataItem default_if_missing) {
  CommonDTypeAggregator dtype_agg;
  std::optional<internal::ObjectId> res_object_id;

  RETURN_IF_ERROR(schema_ids.VisitValues(absl::Overload(
      [&](const arolla::DenseArray<DType>& array) {
        array.ForEachPresent(
            [&](int64_t id, DType dtype) { dtype_agg.Add(dtype); });
        return absl::OkStatus();
      },
      [&](const arolla::DenseArray<internal::ObjectId>& array) {
        absl::Status maybe_error = absl::OkStatus();
        array.ForEachPresent([&](int64_t id, internal::ObjectId schema_obj) {
          if (!maybe_error.ok()) {
            return;
          }
          if (!schema_obj.IsSchema()) {
            maybe_error = absl::InvalidArgumentError(
                "Schema slice with ObjectId schema can accept only schema "
                "ObjectIds");
            return;
          }
          if (!res_object_id) {
            res_object_id = schema_obj;
            return;
          }
          if (*res_object_id != schema_obj) {
            maybe_error = absl::InvalidArgumentError("no common schema");
          }
        });
        return maybe_error;
      },
      [&](const auto& array) {
        using ValT = typename std::decay_t<decltype(array)>::base_type;
        return absl::InvalidArgumentError(
            absl::StrCat("expected Schema, got: ", GetDType<ValT>()));
      })));
  ASSIGN_OR_RETURN(std::optional<DType> res_dtype, dtype_agg.Get());
  if (!res_dtype && !res_object_id) {
    return default_if_missing;
  }
  // NONE is the only dtype that casts to entity.
  if ((!res_dtype || res_dtype == kNone) && res_object_id) {
    return internal::DataItem(*res_object_id);
  }
  if (!res_object_id) {
    return internal::DataItem(*res_dtype);
  }
  return absl::InvalidArgumentError("no common schema");
}

absl::StatusOr<internal::DataItem> NoFollowSchemaItem(
    const internal::DataItem& schema_item) {
  if (schema_item.holds_value<DType>()) {
    if (schema_item.value<DType>() != kObject) {
      // Raises on ANY, primitives and ITEMID.
      return absl::InvalidArgumentError(absl::StrFormat(
          "calling nofollow on %v slice is not allowed", schema_item));
    }
    // NOTE: NoFollow of OBJECT schema has a reserved mask in ObjectId's
    // metadata.
    return internal::DataItem(internal::ObjectId::NoFollowObjectSchemaId());
  }
  if (!schema_item.holds_value<internal::ObjectId>()) {
    return absl::InternalError(
        "schema can be either a DType or ObjectId schema");
  }
  if (!schema_item.value<internal::ObjectId>().IsSchema()) {
    // Raises on non-schemas.
    return absl::InternalError(
        "calling nofollow on a non-schema is not allowed");
  }
  if (schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    // Raises on already NoFollow schema.
    return absl::InvalidArgumentError(
        "calling nofollow on a nofollow slice is not allowed");
  }
  return internal::DataItem(internal::CreateNoFollowWithMainObject(
      schema_item.value<internal::ObjectId>()));
}

absl::StatusOr<internal::DataItem> GetNoFollowedSchemaItem(
    const internal::DataItem& nofollow_schema_item) {
  if (nofollow_schema_item.holds_value<DType>() ||
      !nofollow_schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    return absl::InvalidArgumentError(
        "a nofollow schema is required in get_nofollowed_schema");
  }
  auto schema_id = nofollow_schema_item.value<internal::ObjectId>();
  if (schema_id == internal::ObjectId::NoFollowObjectSchemaId()) {
    return internal::DataItem(kObject);
  }
  return internal::DataItem(internal::GetOriginalFromNoFollow(schema_id));
}

bool VerifySchemaForItemIds(const internal::DataItem& schema_item) {
  if (!schema_item.holds_value<DType>()) {
    return false;
  }
  const DType& dtype = schema_item.value<DType>();
  return dtype == kAny || dtype == kItemId || dtype == kObject;
}

absl::Status VerifyDictKeySchema(const internal::DataItem& schema_item) {
  if (schema_item == schema::kNone || schema_item == schema::kFloat32 ||
      schema_item == schema::kFloat64 || schema_item == schema::kExpr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dict keys cannot be %v", schema_item));
  }
  return absl::OkStatus();
}

}  // namespace koladata::schema
