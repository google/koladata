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
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/utils.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/repr.h"
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
    return arolla::MakeBoundOperator(
        [named_tuple_slot = input_slots[0],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          auto attr_names =
              GetAttrNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(auto result, CreateSchema(db, attr_names, values),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, result.WithDb(std::move(*db).ToImmutable()));
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
    return arolla::MakeBoundOperator(
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& seed_data_slice = frame.Get(seed_slot);
          if (seed_data_slice.GetShape().rank() != 0 ||
              !seed_data_slice.item().holds_value<arolla::Text>()) {
            ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
                "requires seed to be DataItem holding Text, got %s",
                arolla::Repr(seed_data_slice))));
            return;
          }
          auto seed = seed_data_slice.item().value<arolla::Text>();
          auto attr_names = GetAttrNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(auto result,
                           CreateUuSchema(db, seed, attr_names, values),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, result.WithDb(std::move(*db).ToImmutable()));
        });
  }
};

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
      std::make_shared<NewSchemaOperator>(input_types),
      input_types, output_type);
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

absl::StatusOr<DataSlice> CastTo(const DataSlice& x, const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item() == schema::kObject &&
      x.GetSchemaImpl().is_entity_schema()) {
    return absl::InvalidArgumentError(
        "entity to object casting is unsupported - consider using `kd.obj(x)` "
        "instead");
  }
  return ::koladata::CastToExplicit(x, schema.item());
}

absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& x,
                                         const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  return ::koladata::CastToImplicit(x, schema.item());
}

absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& x,
                                       const DataSlice& schema) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  return ::koladata::CastToNarrow(x, schema.item());
}

absl::StatusOr<DataSlice> ListSchema(const DataSlice& item_schema) {
  auto db = koladata::DataBag::Empty();
  ASSIGN_OR_RETURN(auto list_schema, CreateListSchema(db, item_schema));
  return list_schema.WithDb(std::move(*db).ToImmutable());
}

absl::StatusOr<DataSlice> DictSchema(const DataSlice& key_schema,
                                     const DataSlice& value_schema) {
  auto db = koladata::DataBag::Empty();
  ASSIGN_OR_RETURN(auto dict_schema,
                   CreateDictSchema(db, key_schema, value_schema));
  return dict_schema.WithDb(std::move(*db).ToImmutable());
}

}  // namespace koladata::ops
