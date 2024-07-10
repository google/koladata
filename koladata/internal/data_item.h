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
#ifndef KOLADATA_INTERNAL_DATA_ITEM_H_
#define KOLADATA_INTERNAL_DATA_ITEM_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {

class DataItem {
 public:
  DataItem() = default;

  explicit DataItem(std::nullopt_t) : data_(MissingValue()) {}

  template <class T>
  explicit DataItem(T value) : data_(std::move(value)) {}
  template <class T>
  explicit DataItem(std::optional<T> value) {
    if (value) {
      data_ = std::move(*value);
    } else {
      data_ = MissingValue();
    }
  }
  template <class T>
  explicit DataItem(arolla::OptionalValue<T> value) {
    if (value.present) {
      data_ = std::move(value.value);
    } else {
      data_ = MissingValue();
    }
  }

  // Move value out of DataItem. `T` must match the stored data.
  // Usage: std::move(data_item).MoveValue<T>()
  template <typename T>
  T MoveValue() && {
    DCHECK(std::holds_alternative<T>(data_));
    return std::move(*std::get_if<T>(&data_));
  }

  // `DataItem::View` is a wrapper for a view of the DataItem's value that
  // remembers the original type and can be used with `Eq` and `Hash` functors.
  template <typename T>
  struct View {
    using value_type = T;
    using view_type = arolla::view_type_t<T>;

    view_type view;
  };

  template <class T>
  explicit DataItem(View<T> value_view) : data_(T(value_view.view)) {}

  DataItem& operator=(const DataItem& other) & = default;
  DataItem& operator=(const DataItem& other) && = delete;

  // Returns DataItem with specified primitive value.
  // Returns default constructed DataItem in case of unsupported type.
  static absl::StatusOr<DataItem> Create(const arolla::TypedRef& value);
  static absl::StatusOr<DataItem> Create(const arolla::TypedValue& value);

  // Returns data type of the DataItem.
  // `arolla::GetNothingQType()` is returned for missing value.
  arolla::QTypePtr dtype() const;

  // Returns true if the DataItem is not missing.
  bool has_value() const {
    return !std::holds_alternative<MissingValue>(data_);
  }

  // Returns true if the DataItem has value of given type.
  template <typename T>
  bool holds_value() const {
    static_assert(!std::is_same_v<T, MissingValue>);
    return std::holds_alternative<T>(data_);
  }

  bool is_list() const {
    return holds_value<ObjectId>() && value<ObjectId>().IsList();
  }

  bool is_dict() const {
    return holds_value<ObjectId>() && value<ObjectId>().IsDict();
  }

  bool is_schema() const {
    return holds_value<schema::DType>() ||
           (holds_value<ObjectId>() && value<ObjectId>().IsSchema());
  }

  bool is_primitive_schema() const {
    return holds_value<schema::DType>() &&
           value<schema::DType>().is_primitive();
  }

  bool is_entity_schema() const {
    return holds_value<ObjectId>() && value<ObjectId>().IsSchema();
  }

  bool is_implicit_schema() const {
    return holds_value<ObjectId>() && value<ObjectId>().IsImplicitSchema();
  }

  // Returns value of given type. Has no type check. Check the value with
  // `holds_value` first.
  template <class T>
  const T& value() const {
    return std::get<T>(data_);
  }

  // Returns number of present elements in DataItem (can be 0 or 1).
  size_t present_count() const { return has_value() ? 1 : 0; }

  // Returns true iff all present values are lists.
  bool ContainsOnlyLists() const { return !has_value() || is_list(); }

  // Returns true iff all present values are dicts.
  bool ContainsOnlyDicts() const { return !has_value() || is_dict(); }

  bool IsEquivalentTo(const DataItem& other) const {
    return data_ == other.data_;
  }

  template <typename T>
  auto operator==(const T& other) const {
    return Eq()(*this, other);
  }

  template <typename T>
  bool operator!=(const T& other) const {
    return !(*this == other);
  }

  // Returns stable 128 bit Fingerprint.
  arolla::Fingerprint StableFingerprint() const;

  // Returns an Arolla Fingerprint used by DataSlice Fingerprint.
  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const;

  // NOTE: The implementation does not shield special characters which may be
  // interpreted differently in Python.
  std::string DebugString() const;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const DataItem& item) {
    sink.Append(item.DebugString());
  }

  // Call visitor with the present scalar type.
  // If value is not present, called with MissingValue.
  template <class Visitor>
  auto VisitValue(Visitor&& visitor) const {
    return std::visit(visitor, data_);
  }

  struct Eq {
    using is_transparent = void;  // go/totw/144

    template <typename T>
    bool operator()(const DataItem& a, const T& b) const {
      if constexpr (std::is_same_v<T, DataItem>) {
        return std::visit([&](const auto& v) { return EqImpl(a, v); }, b.data_);
      } else if constexpr (std::is_same_v<T, std::nullopt_t>) {
        return !a.has_value();
      } else if constexpr (arolla::meta::is_wrapped_with_v<View, T>) {
        return EqImpl<typename T::view_type, typename T::value_type>(a, b.view);
      } else {
        return EqImpl(a, b);
      }
    }

   private:
    template <typename VT, typename T = VT>
    bool EqImpl(const DataItem& a, const VT& b) const {
      if (std::holds_alternative<T>(a.data_)) {
        return std::get<T>(a.data_) == b;
      }
      return false;
    }
  };

  struct Less {
    using is_transparent = void;  // go/totw/144

    template <typename T>
    bool operator()(const DataItem& a, const T& b) const {
      if constexpr (std::is_same_v<T, DataItem>) {
        return std::visit([&](const auto& v) { return LessImpl(a, v); },
                          b.data_);
      } else if constexpr (std::is_same_v<T, std::nullopt_t>) {
        return false;
      } else if constexpr (arolla::meta::is_wrapped_with_v<View, T>) {
        return LessImpl<typename T::view_type, typename T::value_type>(a,
                                                                       b.view);
      } else {
        return LessImpl(a, b);
      }
    }

   private:
    template <typename VT, typename T = VT>
    bool LessImpl(const DataItem& a, const VT& b) const {
      if (!std::holds_alternative<T>(a.data_)) {
        return a.data_.index() < ScalarTypeId<T>();
      }
      if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
        return std::get<T>(a.data_).expr_fingerprint() < b.expr_fingerprint();
      } else if constexpr (std::is_same_v<T, schema::DType>) {
        return schema::DType::Less()(std::get<T>(a.data_), b);
      } else {
        return std::get<T>(a.data_) < b;
      }
    }
  };

  struct Hash {
    using is_transparent = void;  // go/totw/144

    template <typename T>
    size_t operator()(const T& a) const {
      auto hash_fn = [](const auto& v, size_t index) {
        using VT = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<VT, MissingValue>) {
          return absl::HashOf(index);
        } else if constexpr (std::is_same_v<VT, int32_t>) {
          return absl::HashOf(int64_t{v}, ScalarTypeId<int64_t>());
        } else if constexpr (std::is_same_v<VT, float>) {
          return absl::HashOf(double{v}, ScalarTypeId<double>());
        } else {
          return absl::HashOf(arolla::view_type_t<VT>(v), index);
        }
      };

      if constexpr (std::is_same_v<T, DataItem>) {
        return std::visit(
            [=](const auto& v) { return hash_fn(v, a.data_.index()); },
            a.data_);
      } else if constexpr (arolla::meta::is_wrapped_with_v<View, T>) {
        return hash_fn(a.view, ScalarTypeId<typename T::value_type>());
      } else {
        return hash_fn(a, ScalarTypeId<T>());
      }
    }
  };

  friend std::ostream& operator<<(std::ostream& os, const DataItem& data_item) {
    return os << data_item.DebugString();
  }

 private:
  ScalarVariant data_;
};

// Returns true if the type is sortable.
template <typename T>
constexpr bool IsKodaScalarSortable() {
  return !std::is_same_v<T, DataItem> &&
         !std::is_same_v<T, arolla::expr::ExprQuote> &&
         !std::is_same_v<T, schema::DType> && !std::is_same_v<T, MissingValue>;
}

// Returns true if the type is sortable based on QType.
inline bool IsKodaScalarQTypeSortable(arolla::QTypePtr qtype) {
  return qtype != arolla::GetQType<arolla::expr::ExprQuote>() &&
         qtype != arolla::GetQType<schema::DType>();
}

template <>
inline bool DataItem::Eq::EqImpl<int64_t, int64_t>(const DataItem& a,
                                                   const int64_t& b) const {
  if (std::holds_alternative<int64_t>(a.data_)) {
    return std::get<int64_t>(a.data_) == b;
  }
  if (std::holds_alternative<int32_t>(a.data_)) {
    return std::get<int32_t>(a.data_) == b;
  }
  return false;
}

template <>
inline bool DataItem::Eq::EqImpl<int32_t, int32_t>(const DataItem& a,
                                                   const int32_t& b) const {
  return EqImpl(a, int64_t{b});
}

template <>
inline bool DataItem::Eq::EqImpl<double, double>(const DataItem& a,
                                                 const double& b) const {
  if (std::holds_alternative<double>(a.data_)) {
    return std::get<double>(a.data_) == b;
  }
  if (std::holds_alternative<float>(a.data_)) {
    return std::get<float>(a.data_) == b;
  }
  return false;
}

template <>
inline bool DataItem::Eq::EqImpl<float, float>(const DataItem& a,
                                               const float& b) const {
  return EqImpl(a, double{b});
}

template <>
inline bool DataItem::Less::LessImpl<int64_t, int64_t>(const DataItem& a,
                                                       const int64_t& b) const {
  if (std::holds_alternative<int64_t>(a.data_)) {
    return std::get<int64_t>(a.data_) < b;
  }
  if (std::holds_alternative<int32_t>(a.data_)) {
    return std::get<int32_t>(a.data_) < b;
  }
  return a.data_.index() < ScalarTypeId<int64_t>();
}

template <>
inline bool DataItem::Less::LessImpl<int32_t, int32_t>(const DataItem& a,
                                                       const int32_t& b) const {
  return LessImpl(a, int64_t{b});
}

template <>
inline bool DataItem::Less::LessImpl<double, double>(const DataItem& a,
                                                     const double& b) const {
  if (std::holds_alternative<double>(a.data_)) {
    return std::get<double>(a.data_) < b;
  }
  if (std::holds_alternative<float>(a.data_)) {
    return std::get<float>(a.data_) < b;
  }
  return a.data_.index() < ScalarTypeId<double>();
}

template <>
inline bool DataItem::Less::LessImpl<float, float>(const DataItem& a,
                                                   const float& b) const {
  return LessImpl(a, double{b});
}

// Returns the string representation for the DataItem.
std::string DataItemRepr(const DataItem& item, bool strip_text = false);

}  // namespace koladata::internal

namespace arolla {

AROLLA_DECLARE_REPR(koladata::internal::DataItem);
AROLLA_DECLARE_SIMPLE_QTYPE(INTERNAL_DATA_ITEM, koladata::internal::DataItem);

}  // namespace arolla

#endif  // KOLADATA_INTERNAL_DATA_ITEM_H_
