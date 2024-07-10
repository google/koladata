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
#include "koladata/data_slice_qtype.h"

#include <string>
#include <type_traits>

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"

namespace arolla {

using ::koladata::DataBagPtr;
using ::koladata::DataSlice;
using ::koladata::DataSliceToStr;
using ::koladata::internal::DataItem;

namespace {

// Returns the string format of the content and schema of the DataSlice.
std::string GetReprInternal(const DataSlice& value) {
  std::string result;
  absl::StatusOr<std::string> item_str = DataSliceToStr(value);
  if (item_str.ok()) {
    absl::StrAppend(&result, item_str.value());
  } else {
    value.VisitImpl(
        [&](const auto& impl) { return absl::StrAppend(&result, impl); });
  }
  absl::StrAppend(&result, ", schema: ");
  absl::StatusOr<std::string> schema_str = DataSliceToStr(value.GetSchema());
  if (schema_str.ok()) {
    absl::StrAppend(&result, schema_str.value());
  } else {
    absl::StrAppend(&result, value.GetSchemaImpl());
  }
  return result;
}

std::string GetItemRepr(const DataSlice& value) {
  std::string result;
  absl::StrAppend(&result, "DataItem(", GetReprInternal(value));
  if (value.GetDb() != nullptr) {
    absl::StrAppend(&result, ", bag_id: ", GetBagIdRepr(value.GetDb()));
  }
  absl::StrAppend(&result, ")");
  return result;
}

std::string GetSliceRepr(const DataSlice& value) {
  std::string result;
  absl::StrAppend(&result, "DataSlice(", GetReprInternal(value),
                  ", shape: ", Repr(value.GetShape()));
  if (value.GetDb() != nullptr) {
    absl::StrAppend(&result, ", bag_id: ", GetBagIdRepr(value.GetDb()));
  }
  absl::StrAppend(&result, ")");
  return result;
}

}  // namespace

QTypePtr QTypeTraits<DataSlice>::type() {
  struct DataSliceQType final : SimpleQType {
    DataSliceQType() : SimpleQType(meta::type<DataSlice>(), "DATA_SLICE") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* source) const final {
      return static_cast<const DataSlice*>(source)
          ->py_qvalue_specialization_key();
    }
  };
  static const Indestructible<DataSliceQType> result;
  return result.get();
}

void FingerprintHasherTraits<DataSlice>::operator()(FingerprintHasher* hasher,
                                                    const DataSlice& ds) const {
  ds.VisitImpl([&]<class T>(const T& impl) { hasher->Combine(impl); });
  hasher->Combine(ds.GetShape());
  hasher->Combine(ds.GetSchemaImpl().StableFingerprint());
  // WARNING: Fingerprint must always be the same, even if the object is
  // mutated, because it is a TypedValue. This means that Fingerprint of `db_`
  // should remain the same, even if its contents change or we should compute
  // it statically.
  if (ds.GetDb() != nullptr) {
    hasher->Combine(ds.GetDb());
  }
}

// TODO:
//   * Implement proper Repr for multi-dim DataSlice.
ReprToken ReprTraits<DataSlice>::operator()(const DataSlice& value) const {
  return value.VisitImpl([&]<class T>(const T&) {
    if constexpr (std::is_same_v<T, DataItem>) {
      return ReprToken{GetItemRepr(value)};
    } else {
      return ReprToken{GetSliceRepr(value)};
    }
  });
}

}  // namespace arolla
