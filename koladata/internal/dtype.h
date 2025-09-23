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
#ifndef KOLADATA_INTERNAL_DTYPE_H_
#define KOLADATA_INTERNAL_DTYPE_H_

#include <array>
#include <cstdint>
#include <type_traits>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/stable_fingerprint.h"

namespace koladata::schema {

// Used as schema for DataSlices that can contain ObjectId(s) with various
// schemas, but cannot contain primitives. UUID slices by default have ITEMID
// schemas.
struct ItemIdDType {};

// Used as schema for DataSlices that can contain any value and that actual
// schema is fetched from individual items. ObjectId items contain `__schema__`
// data attribute that contains schema for it.
struct ObjectDType {};

// Used as schema for DataSlices that represent a Schema, i.e. its values are
// schema ObjectId(s) or other DType(s).
struct SchemaDType {};

// When supporting a new type U, update either `supported_primitives_dtypes` or
// `supported_dtype_values` (depending on what kind of type U is being added),
// add it to `GetDTypeId`, and add its name to `DType::kDTypeNames` at
// `TypeId<U>()` position. E.g.:
//   res[TypeId<U>] = "NAME_FOR_U";
using supported_primitive_dtypes =
    arolla::meta::type_list<int64_t, int32_t, float, double, bool, arolla::Unit,
                            arolla::Bytes, arolla::Text,
                            arolla::expr::ExprQuote>;
using supported_dtype_values = arolla::meta::concat_t<
    supported_primitive_dtypes,
    arolla::meta::type_list<ItemIdDType, ObjectDType, SchemaDType,
                            internal::MissingValue>>;

using DTypeId = int8_t;

// GetDTypeId<T>() returns a type id for type T that can be wrapped into DType.
//
// WARNING: The DTypeIds are required to be (long-term) stable, and the internal
// order should never change.
template <typename T>
static constexpr DTypeId GetDTypeId() {
  if constexpr (std::is_same_v<int64_t, T>) {
    return 0;
  } else if constexpr (std::is_same_v<int32_t, T>) {
    return 1;
  } else if constexpr (std::is_same_v<float, T>) {
    return 2;
  } else if constexpr (std::is_same_v<double, T>) {
    return 3;
  } else if constexpr (std::is_same_v<bool, T>) {
    return 4;
  } else if constexpr (std::is_same_v<arolla::Unit, T>) {
    return 5;
  } else if constexpr (std::is_same_v<arolla::Bytes, T>) {
    return 6;
  } else if constexpr (std::is_same_v<arolla::Text, T>) {
    return 7;
  } else if constexpr (std::is_same_v<arolla::expr::ExprQuote, T>) {
    return 8;
  // ANY -> 9 is deprecated.
  } else if constexpr (std::is_same_v<ItemIdDType, T>) {
    return 10;
  } else if constexpr (std::is_same_v<ObjectDType, T>) {
    return 11;
  } else if constexpr (std::is_same_v<SchemaDType, T>) {
    return 12;
  } else if constexpr (std::is_same_v<internal::MissingValue, T>) {
    return 13;
  } else {
    static_assert(sizeof(T) == 0, "unsupported type for GetDTypeId");
  }
}

// Meta function to map DTypeId to the corresponding type.
template <int TypeId>
struct DTypeIdToType {};

#define DECLARE_DTYPE_ID_TO_TYPE(TYPE) \
  template <>                          \
  struct DTypeIdToType<GetDTypeId<TYPE>()> : std::type_identity<TYPE> {};

DECLARE_DTYPE_ID_TO_TYPE(int64_t);
DECLARE_DTYPE_ID_TO_TYPE(int32_t);
DECLARE_DTYPE_ID_TO_TYPE(float);
DECLARE_DTYPE_ID_TO_TYPE(double);
DECLARE_DTYPE_ID_TO_TYPE(bool);
DECLARE_DTYPE_ID_TO_TYPE(arolla::Unit);
DECLARE_DTYPE_ID_TO_TYPE(arolla::Bytes);
DECLARE_DTYPE_ID_TO_TYPE(arolla::Text);
DECLARE_DTYPE_ID_TO_TYPE(arolla::expr::ExprQuote);
DECLARE_DTYPE_ID_TO_TYPE(ItemIdDType);
DECLARE_DTYPE_ID_TO_TYPE(ObjectDType);
DECLARE_DTYPE_ID_TO_TYPE(SchemaDType);
DECLARE_DTYPE_ID_TO_TYPE(internal::MissingValue);

#undef DECLARE_DTYPE_ID_TO_TYPE

// Maximal possible int value DType can be initialized with (exclusive).
inline constexpr int8_t kNextDTypeId = 14;

// Used to represent a terminal value of Schema within DataSlice. The "terminal"
// here means that it has no further attributes and practically means that a
// DataSlice is either:
// * a primitive;
// * mixed, e.g. dtype == OBJECT;
// * is still an object, but schema should be looked-up in data itself.
//
// In other instances, DataSlice's schema will contain an ObjectId, which means
// a pointer to the schema structure in the DataBag.
class DType {
 public:
  constexpr DType() = default;

  // Initializes a new DType with the provided type_id without validation.
  static constexpr DType UnsafeFromId(DTypeId type_id) {
    return DType(type_id);
  }

  // Initializes a new DType with the provided type_id. Validates that the
  // `type_id()` represents a valid DType. Returns an error otherwise.
  static absl::StatusOr<DType> FromId(DTypeId type_id) {
    if (type_id == 9) {
      return absl::InvalidArgumentError(
          "unsupported DType: ANY - deprecated in cl/715818351");
    }
    if (type_id < 0 || type_id >= kNextDTypeId) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unsupported DType.type_id(): %v", type_id));
    }
    return DType(type_id);
  }

  // Creates a DType from QTypePtr `qtype` if it represents a primitive QType
  // (INT32, STRING, etc.).
  static absl::StatusOr<DType> FromQType(arolla::QTypePtr qtype) {
    if (auto type_id_it = qtype_to_type_id().find(qtype);
        type_id_it != qtype_to_type_id().end()) {
      return DType(type_id_it->second);
    }
    return absl::InvalidArgumentError(
        absl::StrCat("unsupported QType: ", qtype->name()));
  }
  constexpr DType(const DType&) = default;
  constexpr DType(DType&&) = default;

  constexpr DType& operator=(const DType&) = default;
  constexpr DType& operator=(DType&&) = default;

  static bool VerifyQTypeSupported(const arolla::QTypePtr qtype) {
    return qtype_to_type_id().contains(qtype);
  }

  arolla::QTypePtr qtype() const {
    return type_id_to_qtype()[type_id()];
  }

  constexpr bool is_primitive() const {
    return kSupportedPrimitiveDTypeIds[type_id()];
  }

  // Returns a unique identifier of a type.
  constexpr DTypeId type_id() const {
    return type_id_;
  }

  friend bool operator==(const DType& lhs, const DType& rhs) {
    return lhs.type_id_ == rhs.type_id_;
  }
  friend bool operator!=(const DType& lhs, const DType& rhs) {
    return !(lhs == rhs);
  }

  constexpr absl::string_view name() const {
    return kDTypeNames[type_id()];
  }

  template <typename Sink>
  friend void AbslStringify(Sink& sink, DType dtype) {
    sink.Append(dtype.name());
  }

  template <typename H>
  friend H AbslHashValue(H h, const DType& type) {
    return H::combine(std::move(h), type.type_id());
  }

  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
    Fingerprint(hasher);
  }

  void StableFingerprint(internal::StableFingerprintHasher* hasher) const {
    Fingerprint(hasher);
  }

  // Used in DataItem::Less. Needed to have deterministic sorting order.
  struct Less {
    bool operator()(const DType& lhs, const DType& rhs) const {
      return lhs.type_id_ < rhs.type_id_;
    }
  };

 private:
  constexpr explicit DType(DTypeId type_id) : type_id_(type_id) {}

  template <class Hasher>
  void Fingerprint(Hasher* hasher) const {
    // NOTE: This must be stable and not rely on QType pointer, but its name.
    // The requirements for stability here are different than for QType in
    // Arolla.
    hasher->Combine(absl::StrCat("::sc\0hema::D\0Type::", type_id()));
  }

  DTypeId type_id_ = GetDTypeId<internal::MissingValue>();

  static constexpr std::array<bool, kNextDTypeId> kSupportedPrimitiveDTypeIds =
      [] {
        std::array<bool, kNextDTypeId> res{false};
        arolla::meta::foreach_type(supported_primitive_dtypes(), [&](auto tpe) {
          using T = typename decltype(tpe)::type;
          res[GetDTypeId<T>()] = true;
        });
        return res;
      }();
  static constexpr std::array<absl::string_view, kNextDTypeId> kDTypeNames =
      [] {
        std::array<absl::string_view, kNextDTypeId> res;
        res[GetDTypeId<int64_t>()] = "INT64";
        res[GetDTypeId<int32_t>()] = "INT32";
        res[GetDTypeId<float>()] = "FLOAT32";
        res[GetDTypeId<double>()] = "FLOAT64";
        res[GetDTypeId<bool>()] = "BOOLEAN";
        res[GetDTypeId<arolla::Unit>()] = "MASK";
        res[GetDTypeId<arolla::Bytes>()] = "BYTES";
        res[GetDTypeId<arolla::Text>()] = "STRING";
        res[GetDTypeId<arolla::expr::ExprQuote>()] = "EXPR";
        res[GetDTypeId<ItemIdDType>()] = "ITEMID";
        res[GetDTypeId<ObjectDType>()] = "OBJECT";
        res[GetDTypeId<SchemaDType>()] = "SCHEMA";
        res[GetDTypeId<internal::MissingValue>()] = "NONE";
        return res;
      }();

  static const std::array<arolla::QTypePtr, kNextDTypeId>& type_id_to_qtype();
  static const absl::flat_hash_map<arolla::QTypePtr, DTypeId>&
      qtype_to_type_id();
};

template <typename T>
constexpr DType GetDType() {
  return DType::UnsafeFromId(GetDTypeId<T>());
}

// Primitive dtypes.
inline constexpr DType kInt32 = GetDType<int>();
inline constexpr DType kInt64 = GetDType<int64_t>();
inline constexpr DType kFloat32 = GetDType<float>();
inline constexpr DType kFloat64 = GetDType<double>();
inline constexpr DType kBool = GetDType<bool>();
inline constexpr DType kMask = GetDType<arolla::Unit>();
inline constexpr DType kBytes = GetDType<arolla::Bytes>();
inline constexpr DType kString = GetDType<arolla::Text>();
inline constexpr DType kExpr = GetDType<arolla::expr::ExprQuote>();

// Special meaning DTypes.
inline constexpr DType kObject = GetDType<schema::ObjectDType>();
inline constexpr DType kSchema = GetDType<schema::SchemaDType>();
inline constexpr DType kItemId = GetDType<schema::ItemIdDType>();
// Kept as non-primitive since one cannot have values of MissingValue.
inline constexpr DType kNone = GetDType<internal::MissingValue>();

}  // namespace koladata::schema

namespace arolla {

template <>
struct ReprTraits<::koladata::schema::DType> {
  ReprToken operator()(const ::koladata::schema::DType& dtype) const {
    return ReprToken(absl::StrCat(dtype));
  }
};

AROLLA_DECLARE_SIMPLE_QTYPE(DTYPE, ::koladata::schema::DType);
AROLLA_DECLARE_OPTIONAL_QTYPE(DTYPE, ::koladata::schema::DType);
AROLLA_DECLARE_DENSE_ARRAY_QTYPE(DTYPE, ::koladata::schema::DType);

}  // namespace arolla

#endif  // KOLADATA_INTERNAL_DTYPE_H_
