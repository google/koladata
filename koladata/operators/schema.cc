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
#include "koladata/operators/schema.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/agg_common_schema.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/object_factories.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class NewSchemaOperator : public arolla::QExprOperator {
 public:
  explicit NewSchemaOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.schema.new_schema",
        [named_tuple_slot = input_slots[0],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(auto result, CreateSchema(db, attr_names, values));
          db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

class UuSchemaOperator : public arolla::QExprOperator {
 public:
  explicit UuSchemaOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.schema.uu_schema",
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(auto result,
                           CreateUuSchema(db, seed, attr_names, values));
          db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

class NamedSchemaOperator : public arolla::QExprOperator {
 public:
  explicit NamedSchemaOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.schema.named_schema",
        [name_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view name,
                           GetStringArgument(frame.Get(name_slot), "name"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(auto result,
                           CreateNamedSchema(db, name, attr_names, values));
          db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

absl::StatusOr<DataSlice> WithAdoptedSchema(const DataSlice& x,
                                            const DataSlice& schema) {
  if (schema.IsStructSchema()) {
    ASSIGN_OR_RETURN(auto res_db, WithAdoptedValues(x.GetBag(), schema));
    return x.WithBag(std::move(res_db));
  } else {
    return x;
  }
}

}  // namespace

absl::StatusOr<arolla::OperatorPtr> NewSchemaOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  // input_types[-1] is a _hidden_seed_ argument used for non-determinism.
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[0]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NewSchemaOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> UuSchemaOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[1]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<UuSchemaOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> NamedSchemaOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[1]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NamedSchemaOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> InternalMaybeNamedSchema(
    const DataSlice& name_or_schema) {
  if (name_or_schema.is_item() &&
      name_or_schema.item().holds_value<arolla::Text>()) {
    ASSIGN_OR_RETURN(absl::string_view name,
                     GetStringArgument(name_or_schema, "name"));
    auto db = koladata::DataBag::Empty();
    ASSIGN_OR_RETURN(auto res, CreateNamedSchema(db, name, {}, {}));
    db->UnsafeMakeImmutable();
    return res;
  } else {
    RETURN_IF_ERROR(name_or_schema.VerifyIsSchema());
    return name_or_schema;
  }
}

absl::StatusOr<DataSlice> CastTo(const DataSlice& x, const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item() == schema::kObject &&
      x.GetSchemaImpl().is_struct_schema()) {
    return absl::InvalidArgumentError(
        "entity to object casting is unsupported - consider using `kd.obj(x)` "
        "instead");
  }
  ASSIGN_OR_RETURN(auto x_with_bag, WithAdoptedSchema(x, schema));
  return ::koladata::CastToExplicit(x_with_bag, schema.item());
}

absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& x,
                                         const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(auto x_with_bag, WithAdoptedSchema(x, schema));
  return ::koladata::CastToImplicit(x_with_bag, schema.item());
}

absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& x,
                                       const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(auto x_with_bag, WithAdoptedSchema(x, schema));
  return ::koladata::CastToNarrow(x_with_bag, schema.item());
}

absl::StatusOr<DataSlice> UnsafeCastTo(const DataSlice& x,
                                       const DataSlice& schema) {
  if (schema.IsStructSchema()) {
    return x.WithSchema(schema);
  } else {
    return CastTo(x, schema);
  }
}

absl::StatusOr<DataSlice> ListSchema(const DataSlice& item_schema) {
  auto db = koladata::DataBag::Empty();
  ASSIGN_OR_RETURN(auto list_schema, CreateListSchema(db, item_schema));
  db->UnsafeMakeImmutable();
  return list_schema;
}

absl::StatusOr<DataSlice> DictSchema(const DataSlice& key_schema,
                                     const DataSlice& value_schema) {
  auto db = koladata::DataBag::Empty();
  ASSIGN_OR_RETURN(auto dict_schema,
                   CreateDictSchema(db, key_schema, value_schema));
  db->UnsafeMakeImmutable();
  return dict_schema;
}

absl::StatusOr<DataSlice> AggCommonSchema(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectSchema("x", x));
  if (x.is_item()) {
    return absl::InvalidArgumentError(
        "aggregation is not supported for scalar DataItems");
  }
  const auto& shape = x.GetShape();
  ASSIGN_OR_RETURN(
      auto res, internal::AggCommonSchemaOp(x.slice(), shape.edges().back()));
  return DataSlice::Create(std::move(res), shape.RemoveDims(shape.rank() - 1),
                           internal::DataItem(schema::kSchema), x.GetBag());
}

absl::StatusOr<DataSlice> GetNoFollowedSchema(const DataSlice& schema_ds) {
  return schema_ds.GetNoFollowedSchema();
}

absl::StatusOr<DataSlice> GetSchemaRepr(const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  auto lookup_status = ValidateAttrLookupAllowed(schema, "ignored error");
  if (lookup_status.ok()) {
    ASSIGN_OR_RETURN(auto has_name, schema.HasAttr(schema::kSchemaNameAttr));
    if (!has_name.IsEmpty()) {
      ASSIGN_OR_RETURN(auto name, schema.GetAttr(schema::kSchemaNameAttr));
      return name.WithBag(nullptr);
    }
  }
  ASSIGN_OR_RETURN(auto repr, DataSliceToStr(schema));
  if (schema.item().holds_value<internal::ObjectId>()) {
    repr += absl::StrFormat(
        " with id %s", ObjectIdStr(schema.item().value<internal::ObjectId>()));
  }
  return DataSlice::Create(internal::DataItem(arolla::Text(std::move(repr))),
                           internal::DataItem(schema::kString));
}

}  // namespace koladata::ops
