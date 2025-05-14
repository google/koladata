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
#ifndef KOLADATA_OPERATORS_SCHEMA_H_
#define KOLADATA_OPERATORS_SCHEMA_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/repr.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/operators/utils.h"

namespace koladata::ops {

// kd.schema.new_schema operator.
// Creates a new allocated schema.
class NewSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.schema.uu_schema operator.
// Creates a UuSchema.
class UuSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.schema.get_primitive_schema.
inline absl::StatusOr<DataSlice> GetPrimitiveSchema(const DataSlice& ds) {
  const auto& schema = ds.GetSchemaImpl();
  if (schema.is_primitive_schema()) {
    return DataSlice::Create(schema, internal::DataItem(schema::kSchema));
  }
  if (schema::DType::VerifyQTypeSupported(ds.dtype())) {
    return DataSlice::Create(
        internal::DataItem(*schema::DType::FromQType(ds.dtype())),
        internal::DataItem(schema::kSchema));
  }
  return DataSlice::Create(internal::DataItem(),
                           internal::DataItem(schema::kSchema));
}

// kd.schema.named_schema operator.
// Creates a named entity schema with its item id derived only from its name.
class NamedSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.schema._internal_maybe_named_schema operator.
// Creates a named entity schema if the given slice is a text DataItem,
// asserts that it is a schema and returns it unchanged otherwise.
absl::StatusOr<DataSlice> InternalMaybeNamedSchema(
    const DataSlice& name_or_schema);

// kd.schema.cast_to operator.
absl::StatusOr<DataSlice> CastTo(const DataSlice& x, const DataSlice& schema);

// kd.schema.cast_to_implicit operator.
absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& x,
                                         const DataSlice& schema);

// kd.schema.cast_to_narrow operator.
absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& x,
                                       const DataSlice& schema);

// kd.schema._unsafe_cast_to operator.
absl::StatusOr<DataSlice> UnsafeCastTo(const DataSlice& x,
                                       const DataSlice& schema);

// kd.schema.list_schema operator.
absl::StatusOr<DataSlice> ListSchema(const DataSlice& item_schema);

// kd.schema.dict_schema operator.
absl::StatusOr<DataSlice> DictSchema(const DataSlice& key_schema,
                                     const DataSlice& value_schema);

// kd.schema.with_schema operator.
inline absl::StatusOr<DataSlice> WithSchema(const DataSlice& ds,
                                            const DataSlice& schema) {
  return ds.WithSchema(schema);
}

// kd.schema.get_schema operator.
inline DataSlice GetSchema(const DataSlice& ds) { return ds.GetSchema(); }

// kd.schema.get_obj_schema operator.
inline absl::StatusOr<DataSlice> GetObjSchema(const DataSlice& ds) {
  return ds.GetObjSchema();
}

// kd.schema.get_item_schema operator.
inline absl::StatusOr<DataSlice> GetItemSchema(const DataSlice& list_schema) {
  if (!list_schema.IsListSchema()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected List schema for get_item_schema, got ",
                     arolla::Repr(list_schema)));
  }
  return list_schema.GetAttr(schema::kListItemsSchemaAttr);
}

// kd.schema.get_key_schema operator.
inline absl::StatusOr<DataSlice> GetKeySchema(const DataSlice& dict_schema) {
  if (!dict_schema.IsDictSchema()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected Dict schema for get_key_schema, got ",
                     arolla::Repr(dict_schema)));
  }
  return dict_schema.GetAttr(schema::kDictKeysSchemaAttr);
}

// kd.schema.get_value_schema operator.
inline absl::StatusOr<DataSlice> GetValueSchema(const DataSlice& dict_schema) {
  if (!dict_schema.IsDictSchema()) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected Dict schema for get_value_schema, got ",
                     arolla::Repr(dict_schema)));
  }
  return dict_schema.GetAttr(schema::kDictValuesSchemaAttr);
}

// kd.schema.is_dict_schema operator.
inline DataSlice IsDictSchema(const DataSlice& schema) {
  return AsMask(schema.IsDictSchema());
}

// kd.schema.is_entity_schema operator.
inline DataSlice IsEntitySchema(const DataSlice& schema) {
  return AsMask(schema.IsEntitySchema());
}

// kd.schema.is_struct_schema operator.
inline DataSlice IsStructSchema(const DataSlice& schema) {
  return AsMask(schema.IsStructSchema());
}

// kd.schema.is_itemid_schema operator.
inline DataSlice IsItemIdSchema(const DataSlice& schema) {
  return AsMask(schema.IsItemIdSchema());
}

// kd.schema.is_list_schema operator.
inline DataSlice IsListSchema(const DataSlice& schema) {
  return AsMask(schema.IsListSchema());
}

// kd.schema.is_primitive_schema operator.
inline DataSlice IsPrimitiveSchema(const DataSlice& schema) {
  return AsMask(schema.IsPrimitiveSchema());
}

// kd.schema._agg_common_schema operator.
absl::StatusOr<DataSlice> AggCommonSchema(const DataSlice& x);

// kd.schema.get_nofollowed_schema.
absl::StatusOr<DataSlice> GetNoFollowedSchema(const DataSlice& schema_ds);

// kd.schema.get_repr.
absl::StatusOr<DataSlice> GetSchemaRepr(const DataSlice& schema);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SCHEMA_H_
