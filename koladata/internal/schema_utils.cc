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
#include "absl/base/nullability.h"
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
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::schema {

// Adjacency list representation of the DType lattice.
//
// Each "row" in the lattice represents a DType, and each "column" represents a
// directly adjacent greater DType.
const schema_internal::DTypeLattice& schema_internal::GetDTypeLattice() {
  static const absl::NoDestructor<DTypeLattice> lattice({
      {kNone, {kItemId, kSchema, kInt32, kMask, kBool, kBytes, kString, kExpr}},
      {kItemId, {}},
      {kSchema, {}},
      {kInt32, {kInt64}},
      {kInt64, {kFloat32}},
      {kFloat32, {kFloat64}},
      {kFloat64, {kObject}},
      {kMask, {kObject}},
      {kBool, {kObject}},
      {kBytes, {kObject}},
      {kString, {kObject}},
      {kExpr, {kObject}},
      {kObject, {}},
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
        LOG(FATAL) << DType::UnsafeFromId(static_cast<DTypeId>(a)) << " and "
                   << DType::UnsafeFromId(static_cast<DTypeId>(b))
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

}  // namespace

std::optional<DType> schema_internal::CommonDTypeAggregator::Get(
    absl::Status& status) const {
  if (seen_dtypes_ == 0) {
    return std::nullopt;
  }
  DTypeId res_dtype_id = absl::countr_zero(seen_dtypes_);

  for (auto mask = seen_dtypes_; true;) {
    mask &= (mask - 1);  // remove the least significant bit.
    if (mask == 0) {
      return DType::UnsafeFromId(res_dtype_id);
    }
    DTypeId i = absl::countr_zero(mask);
    DTypeId common_dtype_id = DTypeMatrix::CommonDType(res_dtype_id, i);
    if (ABSL_PREDICT_FALSE(common_dtype_id == kUnknownDType)) {
      status = arolla::WithPayload(
          absl::InvalidArgumentError("no common schema"),
          internal::NoCommonSchemaError{
              .common_schema =
                  internal::DataItem(DType::UnsafeFromId(res_dtype_id)),
              .conflicting_schema =
                  internal::DataItem(DType::UnsafeFromId(i))});
      return std::nullopt;
    }
    res_dtype_id = common_dtype_id;
  }
}

void CommonSchemaAggregator::Add(const internal::DataItem& schema) {
  if (schema.holds_value<DType>()) {
    return Add(schema.value<DType>());
  }
  if (schema.holds_value<internal::ObjectId>()) {
    return Add(schema.value<internal::ObjectId>());
  }
  // Allow missing values to pass through.
  if (schema.has_value()) {
    status_ = absl::InvalidArgumentError(
        absl::StrFormat("expected Schema, got: %v", schema));
  }
}

void CommonSchemaAggregator::Add(internal::ObjectId schema_obj) {
  if (!schema_obj.IsSchema()) {
    status_ = absl::InvalidArgumentError(
        absl::StrFormat("expected a schema ObjectId, got: %v", schema_obj));
    return;
  }
  if (!res_object_id_) {
    res_object_id_ = std::move(schema_obj);
    return;
  }
  if (*res_object_id_ != schema_obj) {
    status_ = arolla::WithPayload(
        absl::InvalidArgumentError("no common schema"),
        internal::NoCommonSchemaError{
            .common_schema = internal::DataItem(*res_object_id_),
            .conflicting_schema = internal::DataItem(schema_obj)});
  }
}

absl::StatusOr<internal::DataItem> CommonSchemaAggregator::Get() && {
  std::optional<DType> res_dtype = dtype_agg_.Get(status_);
  if (!status_.ok()) {
    return std::move(status_);
  }
  if (res_dtype.has_value() && !res_object_id_) {
    return internal::DataItem(*res_dtype);
  }
  // NONE is the only dtype that casts to entity.
  if ((!res_dtype || res_dtype == kNone) && res_object_id_.has_value()) {
    return internal::DataItem(*res_object_id_);
  }
  if (!res_object_id_) {
    DCHECK(!res_dtype);
    return internal::DataItem();
  }
  return arolla::WithPayload(
      absl::InvalidArgumentError("no common schema"),
      internal::NoCommonSchemaError{
          .common_schema = internal::DataItem(*res_dtype),
          .conflicting_schema = internal::DataItem(*res_object_id_)});
}

absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids) {
  CommonSchemaAggregator schema_agg;

  RETURN_IF_ERROR(schema_ids.VisitValues(absl::Overload(
      [&](const arolla::DenseArray<DType>& array) {
        array.ForEachPresent(
            [&](int64_t id, DType dtype) { schema_agg.Add(dtype); });
        return absl::OkStatus();
      },
      [&](const arolla::DenseArray<internal::ObjectId>& array) {
        array.ForEachPresent([&](int64_t id, internal::ObjectId schema_obj) {
          schema_agg.Add(schema_obj);
        });
        return absl::OkStatus();
      },
      [&](const auto& array) {
        using ValT = typename std::decay_t<decltype(array)>::base_type;
        return absl::InvalidArgumentError(
            absl::StrCat("expected Schema, got: ", GetDType<ValT>()));
      })));
  return std::move(schema_agg).Get();
}

bool IsImplicitlyCastableTo(const internal::DataItem& from_schema,
                            const internal::DataItem& to_schema) {
  DCHECK(from_schema.is_schema() && to_schema.is_schema());
  if (from_schema.holds_value<DType>() && to_schema.holds_value<DType>()) {
    return DTypeMatrix::CommonDType(from_schema.value<DType>().type_id(),
                                    to_schema.value<DType>().type_id()) ==
           to_schema.value<DType>().type_id();
  }
  if (from_schema.holds_value<internal::ObjectId>() &&
         to_schema.holds_value<internal::ObjectId>()) {
    return from_schema.value<internal::ObjectId>() ==
             to_schema.value<internal::ObjectId>();
  }
  return from_schema.holds_value<DType>() &&
         to_schema.holds_value<internal::ObjectId>() &&
         from_schema.value<DType>() == kNone;
}

absl::StatusOr<internal::DataItem> NoFollowSchemaItem(
    const internal::DataItem& schema_item) {
  if (schema_item.holds_value<DType>()) {
    if (schema_item.value<DType>() != kObject) {
      // Raises on primitives and ITEMID.
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
  return dtype == kItemId || dtype == kObject;
}

absl::Status VerifyDictKeySchema(const internal::DataItem& schema_item) {
  if (schema_item == kFloat32 || schema_item == kFloat64 ||
      schema_item == kExpr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dict keys cannot be %v", schema_item));
  }
  return absl::OkStatus();
}

internal::DataItem GetDataSchema(
    const internal::DataItem& item,
    const internal::DataBagImpl* absl_nullable db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  return item.VisitValue([&]<class T>(const T& value) {
    if constexpr (arolla::meta::contains_v<supported_primitive_dtypes, T>) {
      return internal::DataItem(GetDType<T>());
    } else if constexpr (std::is_same_v<T, DType>) {
      return internal::DataItem(kSchema);
    } else if constexpr (std::is_same_v<T, internal::MissingValue>) {
      return internal::DataItem(kNone);
    } else {
      static_assert(std::is_same_v<T, internal::ObjectId>);
      if (db_impl == nullptr) {
        return internal::DataItem();
      }
      auto schema_attr = db_impl->GetAttr(item, schema::kSchemaAttr, fallbacks);
      return schema_attr.ok() ? *std::move(schema_attr) : internal::DataItem();
    }
  });
}

internal::DataItem GetDataSchema(
    const internal::DataSliceImpl& slice,
    const internal::DataBagImpl* absl_nullable db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  CommonSchemaAggregator schema_agg;
  schema_agg.Add(kNone);  // All missing -> NONE.
  bool is_ambiguous = false;
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (arolla::meta::contains_v<supported_primitive_dtypes, T>) {
      schema_agg.Add(GetDType<T>());
    } else if constexpr (std::is_same_v<T, DType>) {
      schema_agg.Add(kSchema);
    } else {
      static_assert(std::is_same_v<T, internal::ObjectId>);
      if (db_impl == nullptr) {
        is_ambiguous = true;
        return;
      }
      auto schema_attrs =
          db_impl->GetAttr(slice, schema::kSchemaAttr, fallbacks);
      if (!schema_attrs.ok() ||
          slice.present_count() != schema_attrs->present_count()) {
        is_ambiguous = true;
        return;
      }
      // NOTE(b/413664265): this may construct an error that is subsequently
      // discarded. Consider changing this behavior to check if it has a common
      // schema instead with a bool.
      if (auto common_schema = CommonSchema(*schema_attrs);
          common_schema.ok()) {
        schema_agg.Add(*common_schema);
      } else {
        is_ambiguous = true;
      }
    }
  });
  if (is_ambiguous) {
    return internal::DataItem();
  }
  auto result_schema = std::move(schema_agg).Get();
  if (result_schema.ok()) {
    return *std::move(result_schema);
  } else {
    return internal::DataItem();
  }
}

}  // namespace koladata::schema
