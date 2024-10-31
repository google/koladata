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

// kde.core.bag.
DataBagPtr Bag(int64_t unused_hidden_seed = 0);

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

class EnrichedOrUpdatedDbOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;

 protected:
  virtual bool is_enriched_operator() const = 0;
};

// kde.core.enriched_bag.
class EnrichedDbOperatorFamily final
    : public EnrichedOrUpdatedDbOperatorFamily {
  bool is_enriched_operator() const override { return true; }
};

// kde.core.updated_bag.
class UpdatedDbOperatorFamily final : public EnrichedOrUpdatedDbOperatorFamily {
  bool is_enriched_operator() const override { return false; }
};

// kde.core._inverse_mapping.
absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x);

// kde.core._ordinal_rank.
absl::StatusOr<DataSlice> OrdinalRank(const DataSlice& x,
                                      const DataSlice& tie_breaker,
                                      const DataSlice& descending);

// kde.core._dense_rank.
absl::StatusOr<DataSlice> DenseRank(const DataSlice& x,
                                    const DataSlice& descending);

// kde.core.align.
class AlignOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core._collapse.
absl::StatusOr<DataSlice> Collapse(const DataSlice& ds);

// kde.core._concat_or_stack
absl::StatusOr<DataSlice> ConcatOrStack(
    absl::Span<const DataSlice* const> slices);

// kde.core.dict_size.
absl::StatusOr<DataSlice> DictSize(const DataSlice& dicts);

// kde.core._extract
absl::StatusOr<DataSlice> Extract(const DataSlice& ds, const DataSlice& schema);

// kde.core.is_empty.
absl::StatusOr<DataSlice> IsEmpty(const DataSlice& obj);

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

// kde.core.with_attrs.
class WithAttrsOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core._get_item.
inline absl::StatusOr<DataSlice> GetItem(const DataSlice& ds,
                                         const DataSlice& key_or_index) {
  return ds.GetItem(key_or_index);
}

// kde.core.get_keys.
inline absl::StatusOr<DataSlice> GetKeys(const DataSlice& dict_ds) {
  return dict_ds.GetDictKeys();
}

// kde.core._get_values.
inline absl::StatusOr<DataSlice> GetValues(const DataSlice& dict_ds) {
  return dict_ds.GetDictValues();
}

// kde.core._get_values_by_keys.
inline absl::StatusOr<DataSlice> GetValuesByKeys(const DataSlice& dict_ds,
                                                 const DataSlice& key_ds) {
  return dict_ds.GetFromDict(key_ds);
}

// kde.core._dict_update
absl::StatusOr<DataBagPtr> DictUpdate(const DataSlice& x, const DataSlice& keys,
                                      const DataSlice& values);

// kde.core.group_by_indices.
absl::StatusOr<DataSlice> GroupByIndices(
    absl::Span<const DataSlice* const> slices);

// kde.core.group_by_indices_sorted.
absl::StatusOr<DataSlice> GroupByIndicesSorted(
    absl::Span<const DataSlice* const> slices);

// kde.core.unique.
absl::StatusOr<DataSlice> Unique(const DataSlice& x, const DataSlice& sort);

// kde.core.encode_itemid
absl::StatusOr<DataSlice> EncodeItemId(const DataSlice& ds);

// kde.core.decode_itemid
absl::StatusOr<DataSlice> DecodeItemId(const DataSlice& ds);

// kde.core.is_dict.
absl::StatusOr<DataSlice> IsDict(const DataSlice& dicts);

// kde.core.get_nofollowed_schema.
absl::StatusOr<DataSlice> GetNoFollowedSchema(const DataSlice& schema_ds);

// kde.core.follow.
absl::StatusOr<DataSlice> Follow(const DataSlice& ds);

template <typename T>
absl::StatusOr<T> Freeze(const T& x);

// kde.core._freeze_bag.
template <>
absl::StatusOr<DataBagPtr> Freeze<DataBagPtr>(const DataBagPtr& x);

// kde.core._freeze_slice.
template <>
absl::StatusOr<DataSlice> Freeze<DataSlice>(const DataSlice& x);

// kde.core.reverse.
absl::StatusOr<DataSlice> Reverse(const DataSlice& obj);

// kde.core.select.
absl::StatusOr<DataSlice> Select(const DataSlice& ds, const DataSlice& filter,
                                 bool expand_filter);

// kde.core.reverse_select.
absl::StatusOr<DataSlice> ReverseSelect(const DataSlice& ds,
                                        const DataSlice& filter);

// kde.core._new_ids_like
absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     int64_t unused_hidden_seed = 0);

// kde.core._clone.
absl::StatusOr<DataSlice> Clone(const DataSlice& ds, const DataSlice& itemid,
                                const DataSlice& schema,
                                int64_t unused_hidden_seed = 0);

// kde.core._shallow_clone
absl::StatusOr<DataSlice> ShallowClone(const DataSlice& obj,
                                       const DataSlice& itemid,
                                       const DataSlice& schema,
                                       int64_t unused_hidden_seed = 0);

// kde.core._deep_clone
absl::StatusOr<DataSlice> DeepClone(const DataSlice& ds,
                                    const DataSlice& schema,
                                    int64_t unused_hidden_seed = 0);

// kde.core._deep_uuid
absl::StatusOr<DataSlice> DeepUuid(const DataSlice& ds,
                                   const DataSlice& schema,
                                   const DataSlice& seed);

// kde.core.subslice operator.
class SubsliceOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.take operator.
absl::StatusOr<DataSlice> Take(const DataSlice& x, const DataSlice& indices);

// kde.core.agg_uuid operator.
absl::StatusOr<DataSlice> AggUuid(const DataSlice& x);

// kde.core.translate.
absl::StatusOr<DataSlice> Translate(const DataSlice& keys_to,
                                    const DataSlice& keys_from,
                                    const DataSlice& values_from);

// kde.core.new.
class NewOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.new_shaped.
class NewShapedOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.new_like.
class NewLikeOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.obj.
class ObjOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.obj_shaped.
class ObjShapedOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.obj_like.
class ObjLikeOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kde.core.uuid operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments
class UuidOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.uuid_for_list operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments, used
// for keying ListItems.
class UuidForListOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.uuid_for_dict operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments, used
// for keying DictItems.
class UuidForDictOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.uuobj operator.
// Creates a DataSlice of UuObjects.
class UuObjOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core.uu operator.
// Creates a DataSlice of UuEntities.
class UuOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.core._dict_shaped operator.
absl::StatusOr<DataSlice> DictShaped(
    const DataSlice::JaggedShape& shape, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid, int64_t unused_hidden_seed);

// kde.core._dict_like operator.
absl::StatusOr<DataSlice> DictLike(
    const DataSlice& shape_and_mask_from, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid, int64_t unused_hidden_seed);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CORE_H_
