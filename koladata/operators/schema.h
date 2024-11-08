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
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/utils.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kde.schema.new_schema operator.
// Creates a new allocated schema.
class NewSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.schema.uu_schema operator.
// Creates a UuSchema.
class UuSchemaOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.schema.get_primitive_schema.
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

// kde.schema.named_schema operator.
// Creates a named entity schema with its item id derived only from its name.
absl::StatusOr<DataSlice> NamedSchema(const DataSlice& name);

// kde.schema._internal_maybe_named_schema operator.
// Creates a named entity schema if the given slice is a text DataItem,
// asserts that it is a schema and returns it unchanged otherwise.
absl::StatusOr<DataSlice> InternalMaybeNamedSchema(
    const DataSlice& name_or_schema);

// kde.schema.cast_to operator.
absl::StatusOr<DataSlice> CastTo(const DataSlice& x, const DataSlice& schema);

// kde.schema.cast_to_implicit operator.
absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& x,
                                         const DataSlice& schema);

// kde.schema.cast_to_narrow operator.
absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& x,
                                       const DataSlice& schema);

// kde.schema.list_schema operator.
absl::StatusOr<DataSlice> ListSchema(const DataSlice& item_schema);

// kde.schema.dict_schema operator.
absl::StatusOr<DataSlice> DictSchema(const DataSlice& key_schema,
                                     const DataSlice& value_schema);

// kde.schema.with_schema operator.
inline absl::StatusOr<DataSlice> WithSchema(const DataSlice& ds,
                                            const DataSlice& schema) {
  return ds.WithSchema(schema);
}

// kde.schema.get_schema operator.
inline DataSlice GetSchema(const DataSlice& ds) { return ds.GetSchema(); }

// kde.schema.get_obj_schema operator.
inline absl::StatusOr<DataSlice> GetObjSchema(const DataSlice& ds) {
  return ds.GetObjSchema();
}

// kde.schema.get_item_schema operator.
inline absl::StatusOr<DataSlice> GetItemSchema(const DataSlice& list_schema) {
  if (!list_schema.IsListSchema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected List schema for get_item_schema, got %v",
                        list_schema.item()));
  }
  return list_schema.GetAttr(schema::kListItemsSchemaAttr);
}

// kde.schema.get_key_schema operator.
inline absl::StatusOr<DataSlice> GetKeySchema(const DataSlice& dict_schema) {
  if (!dict_schema.IsDictSchema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected Dict schema for get_key_schema, got %v",
                        dict_schema.item()));
  }
  return dict_schema.GetAttr(schema::kDictKeysSchemaAttr);
}

// kde.schema.get_value_schema operator.
inline absl::StatusOr<DataSlice> GetValueSchema(const DataSlice& dict_schema) {
  if (!dict_schema.IsDictSchema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected Dict schema for get_value_schema, got %v",
                        dict_schema.item()));
  }
  return dict_schema.GetAttr(schema::kDictValuesSchemaAttr);
}

// kde.schema.is_any_schema operator.
inline DataSlice IsAnySchema(const DataSlice& schema) {
  return AsMask(schema.IsAnySchema());
}

// kde.schema.is_dict_schema operator.
inline DataSlice IsDictSchema(const DataSlice& schema) {
  return AsMask(schema.IsDictSchema());
}

// kde.schema.is_entity_schema operator.
inline DataSlice IsEntitySchema(const DataSlice& schema) {
  return AsMask(schema.IsEntitySchema());
}

// kde.schema.is_itemid_schema operator.
inline DataSlice IsItemIdSchema(const DataSlice& schema) {
  return AsMask(schema.IsItemIdSchema());
}

// kde.schema.is_list_schema operator.
inline DataSlice IsListSchema(const DataSlice& schema) {
  return AsMask(schema.IsListSchema());
}

// kde.schema.is_primitive_schema operator.
inline DataSlice IsPrimitiveSchema(const DataSlice& schema) {
  return AsMask(schema.IsPrimitiveSchema());
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_SCHEMA_H_
