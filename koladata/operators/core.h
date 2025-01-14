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
#ifndef KOLADATA_OPERATORS_CORE_H_
#define KOLADATA_OPERATORS_CORE_H_


#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kde.core._add.
absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y);

// kde.core.no_bag.
DataSlice NoBag(const DataSlice& ds);

// kde.core.ref.
absl::StatusOr<DataSlice> Ref(const DataSlice& ds);

// kde.core.get_bag.
absl::StatusOr<DataBagPtr> GetBag(const DataSlice& ds);

// kde.core.with_bag.
DataSlice WithBag(const DataSlice& ds, const DataBagPtr& db);

// kde.core.with_merged_bag.
absl::StatusOr<DataSlice> WithMergedBag(const DataSlice& ds);

class EnrichedOrUpdatedOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;

 protected:
  virtual bool is_enriched_operator() const = 0;
};

// kde.core.enriched.
class EnrichedOperatorFamily final : public EnrichedOrUpdatedOperatorFamily {
  bool is_enriched_operator() const override { return true; }
};

// kde.core.updated.
class UpdatedOperatorFamily final : public EnrichedOrUpdatedOperatorFamily {
  bool is_enriched_operator() const override { return false; }
};

// kde.core._extract
absl::StatusOr<DataSlice> Extract(const DataSlice& ds, const DataSlice& schema);

// kde.core._get_attr.
absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name);

// kde.core._get_attr_with_default.
absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value);

// kde.core._stub.
absl::StatusOr<DataSlice> Stub(const DataSlice& x, const DataSlice& keep_attrs);

// kde.core.attrs.
class AttrsOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.attr.
absl::StatusOr<DataBagPtr> Attr(const DataSlice& x,
                                const DataSlice& attr_name,
                                const DataSlice& value,
                                const DataSlice& update_schema);

// kde.core.with_attrs.
class WithAttrsOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.with_attr.
absl::StatusOr<DataSlice> WithAttr(const DataSlice& x,
                                   const DataSlice& attr_name,
                                   const DataSlice& value,
                                   const DataSlice& update_schema);

// kde.core._get_item.
inline absl::StatusOr<DataSlice> GetItem(const DataSlice& ds,
                                         const DataSlice& key_or_index) {
  return ds.GetItem(key_or_index);
}

// kde.core.follow.
absl::StatusOr<DataSlice> Follow(const DataSlice& ds);

template <typename T>
T Freeze(const T& x);

// kde.core._databag_freeze.
template <>
DataBagPtr Freeze<DataBagPtr>(const DataBagPtr& x);

// kde.core.freeze_bag.
template <>
DataSlice Freeze<DataSlice>(const DataSlice& x);

// kde.core._new_ids_like
absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     internal::NonDeterministicToken);

// kde.core._clone.
absl::StatusOr<DataSlice> Clone(
    const DataSlice& ds, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken);

// kde.core._shallow_clone
absl::StatusOr<DataSlice> ShallowClone(
    const DataSlice& obj, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken = {});

// kde.core._deep_clone
absl::StatusOr<DataSlice> DeepClone(
    const DataSlice& ds, const DataSlice& schema,
    internal::NonDeterministicToken = {});

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CORE_H_
