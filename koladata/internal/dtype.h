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
#ifndef KOLADATA_INTERNAL_DTYPE_H_
#define KOLADATA_INTERNAL_DTYPE_H_

#include <array>
#include <cstdint>
#include <tuple>
#include <type_traits>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/stable_fingerprint.h"
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

namespace koladata::schema {

// Additional types definitions to augment DTypeTraits.
//
// Used as schema for DataSlices that can contain any value, including ObjectIds
// and mixed primitive values.
struct AnyDType {};

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

// Used as schema for DataSlices that can only contain None values, and that a
// more specific type is not known.
struct NoneDType {};

// When supporting a new type U, update either `supported_primitives_dtypes` or
// `supported_dtype_values` (depending on what kind of type U is being added)
// and add its name to `DType::kDTypeNames` at `TypeId<U>()` position. E.g.:
//   res[TypeId<U>] = "NAME_FOR_U";
using supported_primitive_dtypes =
    arolla::meta::type_list<int64_t, int32_t, float, double, bool, arolla::Unit,
                            arolla::Bytes, arolla::Text,
                            arolla::expr::ExprQuote>;
using supported_dtype_values = arolla::meta::concat_t<
    supported_primitive_dtypes,
    arolla::meta::type_list<AnyDType, ItemIdDType, ObjectDType, SchemaDType,
                            NoneDType>>;

using DTypeId = int8_t;

// GetDTypeId<T>() returns a type id for type T that can be wrapped into DType.
template <typename T, DTypeId index = 0>
static constexpr DTypeId GetDTypeId() {
  using dtypes = supported_dtype_values::tuple;
  static_assert(std::tuple_size_v<dtypes> > index,
                "unsupported type for DType");
  if constexpr (std::is_same_v<std::tuple_element_t<index, dtypes>, T>) {
    return index;
  } else {
    return GetDTypeId<T, index + 1>();
  }
}

// Maximal possible int value DType can be initialized with (exclusive).
constexpr int8_t kNextDTypeId =
    std::tuple_size_v<supported_dtype_values::tuple>;

// Used to represent a terminal value of Schema within DataSlice. The "terminal"
// here means that it has no further attributes and practically means that a
// DataSlice is either:
// * a primitive;
// * mixed, e.g. dtype == ANY;
// * is still an object, but schema should be looked-up in data itself.
//
// In other instances, DataSlice's schema will contain an ObjectId, which means
// a pointer to the schema structure in the DataBag.
class DType {
 public:
  constexpr DType() = default;
  constexpr explicit DType(DTypeId type_id) : type_id_(type_id) {
    DCHECK_GE(type_id, 0);
    DCHECK_LT(type_id, kNextDTypeId);
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
  template <class Hasher>
  void Fingerprint(Hasher* hasher) const {
    // NOTE: This must be stable and not rely on QType pointer, but its name.
    // The requirements for stability here are different than for QType in
    // Arolla.
    hasher->Combine(absl::StrCat("::sc\0hema::D\0Type::", type_id()));
  }

  DTypeId type_id_ = GetDTypeId<AnyDType>();

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
        res[GetDTypeId<AnyDType>()] = "ANY";
        res[GetDTypeId<ItemIdDType>()] = "ITEMID";
        res[GetDTypeId<ObjectDType>()] = "OBJECT";
        res[GetDTypeId<SchemaDType>()] = "SCHEMA";
        res[GetDTypeId<NoneDType>()] = "NONE";
        return res;
      }();

  static const std::array<arolla::QTypePtr, kNextDTypeId>& type_id_to_qtype();
  static const absl::flat_hash_map<arolla::QTypePtr, DTypeId>&
      qtype_to_type_id();
};

template <typename T>
constexpr DType GetDType() {
  return DType(GetDTypeId<T>());
}

// Primitive dtypes.
constexpr DType kInt32 = GetDType<int>();
constexpr DType kInt64 = GetDType<int64_t>();
constexpr DType kFloat32 = GetDType<float>();
constexpr DType kFloat64 = GetDType<double>();
constexpr DType kBool = GetDType<bool>();
constexpr DType kMask = GetDType<arolla::Unit>();
constexpr DType kBytes = GetDType<arolla::Bytes>();
constexpr DType kText = GetDType<arolla::Text>();
constexpr DType kExpr = GetDType<arolla::expr::ExprQuote>();

// Special meaning DTypes.
constexpr DType kAny = GetDType<schema::AnyDType>();
constexpr DType kObject = GetDType<schema::ObjectDType>();
constexpr DType kSchema = GetDType<schema::SchemaDType>();
constexpr DType kItemId = GetDType<schema::ItemIdDType>();
// Kept as non-primitive since it is created from a special DType, and we will
// not encounter such values directly in the data.
constexpr DType kNone = GetDType<schema::NoneDType>();

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
