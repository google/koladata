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

// kd.core.no_bag.
DataSlice NoBag(const DataSlice& ds);

// kd.core.ref.
absl::StatusOr<DataSlice> Ref(const DataSlice& ds);

// kd.core.get_bag.
absl::StatusOr<DataBagPtr> GetBag(const DataSlice& ds);

// kd.core.with_bag.
DataSlice WithBag(const DataSlice& ds, const DataBagPtr& db);

// kd.core.with_merged_bag.
absl::StatusOr<DataSlice> WithMergedBag(const DataSlice& ds);

class EnrichedOrUpdatedOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;

 protected:
  virtual bool is_enriched_operator() const = 0;
};

// kd.core.enriched.
class EnrichedOperatorFamily final : public EnrichedOrUpdatedOperatorFamily {
  bool is_enriched_operator() const override { return true; }
};

// kd.core.updated.
class UpdatedOperatorFamily final : public EnrichedOrUpdatedOperatorFamily {
  bool is_enriched_operator() const override { return false; }
};

// kd.core._extract
absl::StatusOr<DataSlice> Extract(const DataSlice& ds, const DataSlice& schema);

// kd.core._get_attr.
absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name);

// kd.core._get_attr_with_default.
absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value);

// kd.core.has_attr.
absl::StatusOr<DataSlice> HasAttr(const DataSlice& obj,
                                  const DataSlice& attr_name);

// kd.core._stub.
absl::StatusOr<DataSlice> Stub(const DataSlice& x, const DataSlice& keep_attrs);

// kd.core.attrs.
class AttrsOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.core.attr.
absl::StatusOr<DataBagPtr> Attr(const DataSlice& x,
                                const DataSlice& attr_name,
                                const DataSlice& value,
                                const DataSlice& update_schema);

// kd.core.with_attrs.
class WithAttrsOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.core.with_attr.
absl::StatusOr<DataSlice> WithAttr(const DataSlice& x,
                                   const DataSlice& attr_name,
                                   const DataSlice& value,
                                   const DataSlice& update_schema);

// kd.core._get_item.
inline absl::StatusOr<DataSlice> GetItem(const DataSlice& ds,
                                         const DataSlice& key_or_index) {
  return ds.GetItem(key_or_index);
}

// kd.core.follow.
absl::StatusOr<DataSlice> Follow(const DataSlice& ds);

template <typename T>
T Freeze(const T& x);

// kd.core._databag_freeze.
template <>
DataBagPtr Freeze<DataBagPtr>(const DataBagPtr& x);

// kd.core.freeze_bag.
template <>
DataSlice Freeze<DataSlice>(const DataSlice& x);

// kd.core._new_ids_like
absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     internal::NonDeterministicToken);

// kd.core._clone.
absl::StatusOr<DataSlice> Clone(
    const DataSlice& ds, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken);

// kd.core._shallow_clone
absl::StatusOr<DataSlice> ShallowClone(
    const DataSlice& obj, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken = {});

// kd.core._deep_clone
absl::StatusOr<DataSlice> DeepClone(
    const DataSlice& ds, const DataSlice& schema,
    internal::NonDeterministicToken = {});

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CORE_H_
