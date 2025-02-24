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
#include "koladata/operators/ids.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/agg_uuid.h"
#include "koladata/internal/op_utils/deep_uuid.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/internal/op_utils/itemid.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/operators/core.h"
#include "koladata/operators/utils.h"
#include "koladata/uuid_utils.h"
#include "arolla/dense_array/qtype/types.h"
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
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class UuidOperator : public arolla::QExprOperator {
 public:
  explicit UuidOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.ids.uuid",
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result, koladata::CreateUuidFromFields(
                                            seed, attr_names, values));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

class UuidForListOperator : public arolla::QExprOperator {
 public:
  explicit UuidForListOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.ids.uuid_for_list",
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result, koladata::CreateListUuidFromFields(
                                            seed, attr_names, values));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

class UuidForDictOperator : public arolla::QExprOperator {
 public:
  explicit UuidForDictOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.ids.uuid_for_dict",
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result, koladata::CreateDictUuidFromFields(
                                            seed, attr_names, values));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

}  // namespace

absl::StatusOr<DataSlice> DeepUuid(arolla::EvaluationContext* ctx,
                                   const DataSlice& ds, const DataSlice& schema,
                                   const DataSlice& seed) {
  absl::Nullable<DataBagPtr> db = ds.GetBag();
  if (db == nullptr) {
    if (schema.IsStructSchema()) {
      return absl::InvalidArgumentError(
          "cannot compute deep_uuid of entity slice without a DataBag");
    }
    db = DataBag::Empty();
  }
  const auto& schema_db = schema.GetBag();
  if (schema_db != nullptr && schema_db != db) {
    ASSIGN_OR_RETURN(auto extracted_ds, Extract(ctx, ds, schema));
    return DeepUuid(ctx, extracted_ds, schema.WithBag(extracted_ds.GetBag()),
                    seed);
  }
  if (seed.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("seed can only be 0-rank schema slice, got: rank(%d)",
                        seed.GetShape().rank()));
  }
  const auto& seed_item = seed.item();
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_item = schema.item();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    internal::DeepUuidOp deep_uuid_op;
    ASSIGN_OR_RETURN(auto result_slice_impl,
                     deep_uuid_op(seed_item, impl, schema_item, db->GetImpl(),
                                  fallbacks_span));
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             internal::DataItem(schema::kItemId));
  });
}

absl::StatusOr<DataSlice> AggUuid(const DataSlice& x) {
  auto rank = x.GetShape().rank();
  if (rank == 0) {
    return absl::InvalidArgumentError("Can't take agg_uuid over a DataItem");
  }
  internal::DataItem schema(schema::kItemId);
  auto shape = x.GetShape();
  ASSIGN_OR_RETURN(auto res, internal::AggUuidOp(x.slice(), shape));
  return DataSlice::Create(std::move(res), shape.RemoveDims(rank - 1),
                           std::move(schema),
                           /*db=*/nullptr);
}

absl::StatusOr<arolla::OperatorPtr> UuidOperatorFamily::DoGetOperator(
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
      std::make_shared<UuidOperator>(input_types), input_types, output_type);
}

absl::StatusOr<arolla::OperatorPtr> UuidForListOperatorFamily::DoGetOperator(
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
      std::make_shared<UuidForListOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> UuidForDictOperatorFamily::DoGetOperator(
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
      std::make_shared<UuidForDictOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> UuidsWithAllocationSize(const DataSlice& seed,
                                                  const DataSlice& size) {
  if (!seed.is_item() || !seed.item().holds_value<arolla::Text>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("requires seed to be DataItem holding a STRING, got %s",
                        arolla::Repr(seed)));
  }
  absl::string_view seed_value = seed.item().value<arolla::Text>();
  if (!size.is_item()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "requires size to be a scalar, got %s", DataSliceRepr(size)));
  }
  absl::StatusOr<DataSlice> casted_size = ToInt64(size);
  if (!casted_size.ok()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "requires size to be castable to int64, got %s", DataSliceRepr(size)));
  }
  const int64_t size_value = casted_size->item().value<int64_t>();
  return koladata::CreateUuidsWithAllocationSize(seed_value, size_value);
}

absl::StatusOr<DataSlice> EncodeItemId(const DataSlice& ds) {
  ASSIGN_OR_RETURN(auto res,
                   DataSliceOp<internal::EncodeItemId>()(
                       ds, ds.GetShape(), internal::DataItem(schema::kString),
                       /*db=*/nullptr),
                   internal::OperatorEvalError(
                       std::move(_), "kd.encode_itemid",
                       absl::StrFormat("only ItemIds can be encoded, got %v",
                                       ds.GetSchemaImpl())));
  return std::move(res);
}

absl::StatusOr<DataSlice> DecodeItemId(const DataSlice& ds) {
  ASSIGN_OR_RETURN(auto res,
                   DataSliceOp<internal::DecodeItemId>()(
                       ds, ds.GetShape(), internal::DataItem(schema::kItemId),
                       /*db=*/nullptr),
                   internal::OperatorEvalError(
                       std::move(_), "kd.decode_itemid",
                       absl::StrFormat("only STRING can be decoded, got %v",
                                       ds.GetSchemaImpl())));
  return std::move(res);
}

}  // namespace koladata::ops
