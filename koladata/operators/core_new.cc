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
#include "koladata/operators/core_new.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/op_utils/new_ids_like.h"
#include "koladata/object_factories.h"
#include "koladata/operators/utils.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class NewOperator final : public arolla::QExprOperator {
 public:
  explicit NewOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [schema_slot = input_slots[1],
         update_schema_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         item_id_slot = input_slots[3],
         named_tuple_slot = input_slots[4],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          std::optional<DataSlice> schema;
          if (schema_slot.GetType() == arolla::GetQType<DataSlice>()) {
            schema = frame.Get(schema_slot.UnsafeToSlot<DataSlice>());
          }
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetAttrNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::Empty();
          ASSIGN_OR_RETURN(
              auto result,
              EntityCreator::FromAttrs(result_db, attr_names, attr_values,
                                       schema, update_schema, item_id),
              ctx->set_status(std::move(_)));
          result_db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
        });
  }
};

class NewShapedOperator : public arolla::QExprOperator {
 public:
  explicit NewShapedOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [shape_slot = input_slots[0].UnsafeToSlot<DataSlice::JaggedShape>(),
         schema_slot = input_slots[1],
         update_schema_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         item_id_slot = input_slots[3],
         named_tuple_slot = input_slots[4],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& shape = frame.Get(shape_slot);
          std::optional<DataSlice> schema;
          if (schema_slot.GetType() == arolla::GetQType<DataSlice>()) {
            schema = frame.Get(schema_slot.UnsafeToSlot<DataSlice>());
          }
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetAttrNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::Empty();
          ASSIGN_OR_RETURN(
              auto result,
              EntityCreator::Shaped(result_db, shape, attr_names, attr_values,
                                    schema, update_schema, item_id),
              ctx->set_status(std::move(_)));
          result_db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
        });
  }
};

class NewLikeOperator : public arolla::QExprOperator {
 public:
  explicit NewLikeOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [shape_and_mask_from_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         schema_slot = input_slots[1],
         update_schema_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         item_id_slot = input_slots[3],
         named_tuple_slot = input_slots[4],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& shape_and_mask_from = frame.Get(shape_and_mask_from_slot);
          std::optional<DataSlice> schema;
          if (schema_slot.GetType() == arolla::GetQType<DataSlice>()) {
            schema = frame.Get(schema_slot.UnsafeToSlot<DataSlice>());
          }
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetAttrNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::Empty();
          ASSIGN_OR_RETURN(
              auto result,
              EntityCreator::Like(result_db, shape_and_mask_from, attr_names,
                                  attr_values, schema, update_schema, item_id),
              ctx->set_status(std::move(_)));
          result.GetBag()->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
        });
  }
};

class UuOperator : public arolla::QExprOperator {
 public:
  explicit UuOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         schema_slot = input_slots[1],
         update_schema_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          std::optional<DataSlice> schema;
          if (schema_slot.GetType() == arolla::GetUnspecifiedQType()) {
            schema = absl::nullopt;
          } else {
            schema = frame.Get(schema_slot.UnsafeToSlot<DataSlice>());
          }
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"),
                           ctx->set_status(std::move(_)));
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          auto attr_names = GetAttrNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::Empty();
          ASSIGN_OR_RETURN(
              auto result,
              CreateUu(db, seed, attr_names, values, schema, update_schema),
              ctx->set_status(std::move(_)));
          db->UnsafeMakeImmutable();
          frame.Set(output_slot, std::move(result));
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> UuOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires `seed` argument to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>() &&
      input_types[1] != arolla::GetUnspecifiedQType()) {
    return absl::InvalidArgumentError(
        "requires `schema` argument to be DataSlice or unspecified");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires `update_schema` argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<UuOperator>(input_types), input_types, output_type);
}

absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     internal::NonDeterministicToken) {
  return ds.VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
      return DataSlice::Create(internal::NewIdsLike(impl), ds.GetShape(),
                               ds.GetSchemaImpl());
    }
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      auto slice_impl = internal::DataSliceImpl::Create(/*size=*/1, impl);
      return DataSlice::Create(internal::NewIdsLike(slice_impl)[0],
                               ds.GetShape(), ds.GetSchemaImpl());
    }
    DCHECK(false);
  });
}

absl::StatusOr<arolla::OperatorPtr> NewOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires schema argument to be DataSlice or unspecified");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires update_schema argument to be DataSlice");
  }
  if (!IsDataSliceOrUnspecified(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[4]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[5]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NewOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> NewShapedOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice::JaggedShape>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be JaggedShape");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires schema argument to be DataSlice or unspecified");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires update_schema argument to be DataSlice");
  }
  if (!IsDataSliceOrUnspecified(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[4]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[5]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NewShapedOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> NewLikeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 6) {
    return absl::InvalidArgumentError("requires exactly 6 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires schema argument to be DataSlice or unspecified");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires update_schema argument to be DataSlice");
  }
  if (!IsDataSliceOrUnspecified(input_types[3])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[4]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[5]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NewLikeOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::ops
