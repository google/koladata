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

#include <cstdint>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kde.core._add.
absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y);

// kde.core.no_db.
DataSlice NoDb(const DataSlice& ds);

// kde.core.get_db.
absl::StatusOr<DataBagPtr> GetDb(const DataSlice& ds);

// kde.core.with_db.
DataSlice WithDb(const DataSlice& ds, const DataBagPtr& db);

// kde.core._inverse_mapping.
absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x);

// kde.core._ordinal_rank.
absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending);

// kde.core.align.
class AlignOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core._clone.
absl::StatusOr<DataSlice> Clone(const DataSlice& ds, const DataSlice& schema);

// kde.core._collapse.
absl::StatusOr<DataSlice> Collapse(const DataSlice& ds);

// kde.core._concat_or_stack
class ConcatOrStackOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.dict_size.
absl::StatusOr<DataSlice> DictSize(const DataSlice& dicts);

// kde.core._explode
absl::StatusOr<DataSlice> Explode(const DataSlice& x, int64_t ndim);

// kde.core._extract
absl::StatusOr<DataSlice> Extract(const DataSlice& ds, const DataSlice& schema);

// kde.core._get_attr.
absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name);

// kde.core._get_attr_with_default.
absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value);

// kde.core.group_by_indices.
class GroupByIndicesFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.group_by_indices_sorted.
class GroupByIndicesSortedFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.unique.
absl::StatusOr<DataSlice> Unique(const DataSlice& x, const DataSlice& sort);

// kde.core.itemid_bits
absl::StatusOr<DataSlice> ItemIdBits(const DataSlice& ds,
                                     const DataSlice& last);

// kde.core.list_size.
absl::StatusOr<DataSlice> ListSize(const DataSlice& lists);

// kde.core.get_nofollowed_schema.
absl::StatusOr<DataSlice> GetNoFollowedSchema(const DataSlice& schema_ds);

// kde.core.follow.
absl::StatusOr<DataSlice> Follow(const DataSlice& ds);

// kde.core.reverse.
absl::StatusOr<DataSlice> Reverse(const DataSlice& obj);

// kde.core.select.
absl::StatusOr<DataSlice> Select(const DataSlice& ds, const DataSlice& filter,
                                 bool expand_filter);

// kde.core.reverse_select.
absl::StatusOr<DataSlice> ReverseSelect(const DataSlice& ds,
                                        const DataSlice& filter);

// kde.core._shallow_clone
absl::StatusOr<DataSlice> ShallowClone(const DataSlice& ds,
                                       const DataSlice& schema);

// kde.core.subslice operator.
class SubsliceOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.at operator.
absl::StatusOr<DataSlice> At(const DataSlice& x, const DataSlice& indices);

// kde.core.translate.
absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                    const DataSlice& keys_from,
                                    const DataSlice& values_from);

// kde.schema._uu_schema operator.
// Creates a UuSchema.
class UuSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core._uuid operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments
class UuidOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core._uuobj operator.
// Creates a DataSlice of UuObjects.
class UuObjOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CORE_H_