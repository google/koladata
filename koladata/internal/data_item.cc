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
#include "koladata/internal/data_item.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "unicode/utf8.h"
#include "koladata/internal/expr_quote_utils.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/stable_fingerprint.h"
#include "koladata/internal/types.h"
#include "double-conversion/double-to-string.h"
#include "double-conversion/utils.h"

namespace koladata::internal {

using ::arolla::GetOptionalQType;
using ::arolla::GetQType;
using ::arolla::QTypePtr;

QTypePtr DataItem::dtype() const {
  return std::visit(
      [](const auto& arg) -> arolla::QTypePtr {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, MissingValue>) {
          return arolla::GetNothingQType();
        } else {
          return arolla::GetQType<T>();
        }
      },
      data_);
}

absl::StatusOr<DataItem> DataItem::Create(const arolla::TypedRef& value) {
  QTypePtr dtype = value.GetType();
  std::optional<DataItem> result;
  arolla::meta::foreach_type(supported_primitives_list(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    if (dtype == GetQType<T>()) {
      result = DataItem(value.UnsafeAs<T>());
    } else if (dtype == GetOptionalQType<T>()) {
      auto& optional_value = value.UnsafeAs<arolla::OptionalValue<T>>();
      if (optional_value.present) {
        result = DataItem(optional_value.value);
      } else {
        result = DataItem();
      }
    }
  });
  if (!result.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("DataItem cannot be created from value with type %s",
                        value.GetType()->name()));
  }
  return *std::move(result);
}

absl::StatusOr<DataItem> DataItem::Create(const arolla::TypedValue& value) {
  return Create(value.AsRef());
}

arolla::Fingerprint DataItem::StableFingerprint() const {
  StableFingerprintHasher hasher("data_item");
  std::visit([&hasher](const auto& value) { hasher.Combine(value); }, data_);
  return std::move(hasher).Finish();
}

void DataItem::ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
  std::visit(
      [&hasher, this](const auto& value) {
        if constexpr (!std::is_same_v<std::decay_t<decltype(value)>,
                                      MissingValue>) {
          hasher->Combine(value);
          hasher->Combine(data_.index());
        } else {
          hasher->Combine(data_.index());
        }
      },
      data_);
}

namespace {

struct TruncatedString {
  absl::string_view prefix;
  std::optional<absl::string_view> suffix = std::nullopt;

  std::string JoinAndEscapeText(bool quote) const {
    auto escape = [&](absl::string_view str) {
      // When we keep quotes, generally escape, but unescape double quotes
      // since they are valid in a single quoted string. Although unescaping
      // the double quotes is not necessary, it improves readability.
      return absl::StrReplaceAll(absl::Utf8SafeCHexEscape(str),
                                 {{"\\\"", "\""}});
    };
    if (quote) {
      if (suffix.has_value()) {
        return absl::StrCat("'", escape(prefix), "'...'", escape(*suffix), "'");
      } else {
        return absl::StrCat("'", escape(prefix), "'");
      }
    } else {
      // When we strip quotes, we do not escape to preserve all the of the
      // original content of the string.
      if (suffix.has_value()) {
        return absl::StrCat(prefix, "...", *suffix);
      } else {
        return std::string(prefix);
      }
    }
  }

  std::string JoinAndEscapeBytes() const {
    if (suffix.has_value()) {
      return absl::StrCat("b'", absl::CHexEscape(prefix), "'...'",
                          absl::CHexEscape(*suffix), "'");
    } else {
      return absl::StrCat("b'", absl::CHexEscape(prefix), "'");
    }
  }
};

TruncatedString TruncateMiddle(absl::string_view str, int32_t max_len) {
  if (max_len < 0 || str.length() <= max_len ||
      max_len == std::numeric_limits<int32_t>::max()) {
    return {.prefix = str};
  }
  int32_t half_len = (max_len + 1) / 2;
  size_t prefix_limit = 0;
  U8_FWD_N(str.data(), prefix_limit, str.length(), half_len);
  size_t suffix_limit = str.length();
  U8_BACK_N(reinterpret_cast<const uint8_t*>(str.data()), 0, suffix_limit,
            half_len);
  if (prefix_limit >= suffix_limit) {
    return {.prefix = str};
  } else {
    return {.prefix = str.substr(0, prefix_limit),
            .suffix = str.substr(suffix_limit)};
  }
}

}  // namespace

std::string DataItemRepr(const DataItem& item,
                         const DataItemReprOption& option) {
  return item.VisitValue(
      [&option]<class T>(const T& val) -> std::string {
        if constexpr (std::is_same_v<T, MissingValue>) {
          return option.show_missing ? "missing" : "None";
        } else if constexpr (std::is_same_v<T, arolla::Unit>) {
          return "present";
        } else if constexpr (std::is_same_v<T, arolla::Text>) {
          return TruncateMiddle(absl::string_view(val),
                                option.unbounded_type_max_len)
              .JoinAndEscapeText(!option.strip_quotes);
        } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
          return TruncateMiddle(absl::string_view(val),
                                option.unbounded_type_max_len)
              .JoinAndEscapeBytes();
        } else if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
          return ExprQuoteDebugString(val);
        } else if constexpr (std::is_same_v<T, bool>) {
          return val ? "True" : "False";
        } else if constexpr (std::is_same_v<T, float> ||
                             std::is_same_v<T, double>) {
          static const double_conversion::DoubleToStringConverter converter(
              double_conversion::DoubleToStringConverter::
                      EMIT_TRAILING_ZERO_AFTER_POINT |
                  double_conversion::DoubleToStringConverter::
                      EMIT_TRAILING_DECIMAL_POINT,
              "inf", "nan", 'e', -6, 21, 6, 0);
          char buf[128];
          double_conversion::StringBuilder builder(buf, sizeof(buf));
          converter.ToShortestSingle(val, &builder);
          std::string result = std::string(builder.Finalize());
          if constexpr (std::is_same_v<T, double>) {
            if (option.show_dtype) {
              return absl::StrCat("float64{", result, "}");
            }
          }
          return result;
        } else if constexpr (std::is_same_v<T, int64_t>) {
          return option.show_dtype
              ? absl::StrCat("int64{", val, "}") : absl::StrCat(val);
        } else if constexpr (std::is_same_v<T, ObjectId>) {
          return option.hex_object_ids
              ? absl::StrCat(val) : ObjectIdStr(val);
        } else {
          return absl::StrCat(val);
        }
      });
}

std::string DataItem::DebugString() const {
  return DataItemRepr(*this, {.hex_object_ids = true});
}

}  // namespace koladata::internal

namespace arolla {

ReprToken ReprTraits<::koladata::internal::DataItem>::operator()(
    const ::koladata::internal::DataItem& value) const {
  return ReprToken{value.DebugString()};
}

AROLLA_DEFINE_SIMPLE_QTYPE(INTERNAL_DATA_ITEM, ::koladata::internal::DataItem);

}  // namespace arolla
