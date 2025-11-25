// Copyright 2025 Google LLC
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
#include "koladata/operators/objs.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/jagged_shape_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> ConvertWithAdoption(const DataBagPtr& db,
                                              const DataSlice& value) {
  if (value.GetBag() != nullptr && value.GetBag() != db) {
    AdoptionQueue adoption_queue;
    adoption_queue.Add(value);
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));
  }
  ASSIGN_OR_RETURN(auto res, ObjectCreator::ConvertWithoutAdopt(db, value));
  return res.WithBag(db);
}

class ObjOperator final : public arolla::QExprOperator {
 public:
  explicit ObjOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.objs.new",
        [first_arg_slot = input_slots[0], item_id_slot = input_slots[1],
         named_tuple_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          std::optional<DataSlice> first_arg;
          if (first_arg_slot.GetType() == arolla::GetQType<DataSlice>()) {
            first_arg = frame.Get(first_arg_slot.UnsafeToSlot<DataSlice>());
          }
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetFieldNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::EmptyMutable();
          std::optional<DataSlice> result;
          if (first_arg.has_value()) {
            if (item_id.has_value()) {
              return absl::InvalidArgumentError(
                  "`itemid` is not supported when converting to object");
            }
            if (!attr_values.empty()) {
              return absl::InvalidArgumentError(
                  "cannot set extra attributes when converting to object");
            }
            ASSIGN_OR_RETURN(result,
                             ConvertWithAdoption(result_db, *first_arg));
          } else {
            ASSIGN_OR_RETURN(result,
                             ObjectCreator::FromAttrs(result_db, attr_names,
                                                      attr_values, item_id));
          }
          frame.Set(output_slot, result->UnsafeMakeWholeOnImmutableDb());
          return absl::OkStatus();
        });
  }
};

class ObjShapedOperator : public arolla::QExprOperator {
 public:
  explicit ObjShapedOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.objs.shaped",
        [shape_slot = input_slots[0].UnsafeToSlot<DataSlice::JaggedShape>(),
         item_id_slot = input_slots[1], named_tuple_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& shape = frame.Get(shape_slot);
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetFieldNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::EmptyMutable();
          ASSIGN_OR_RETURN(auto result,
                           ObjectCreator::Shaped(result_db, shape, attr_names,
                                                 attr_values, item_id));
          frame.Set(output_slot, result.UnsafeMakeWholeOnImmutableDb());
          return absl::OkStatus();
        });
  }
};

class ObjLikeOperator : public arolla::QExprOperator {
 public:
  explicit ObjLikeOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.objs.like",
        [shape_and_mask_from_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         item_id_slot = input_slots[1], named_tuple_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& shape_and_mask_from = frame.Get(shape_and_mask_from_slot);
          std::optional<DataSlice> item_id;
          if (item_id_slot.GetType() == arolla::GetQType<DataSlice>()) {
            item_id = frame.Get(item_id_slot.UnsafeToSlot<DataSlice>());
          }
          const std::vector<absl::string_view> attr_names =
              GetFieldNames(named_tuple_slot);
          const std::vector<DataSlice> attr_values =
              GetValueDataSlices(named_tuple_slot, frame);
          DataBagPtr result_db = DataBag::EmptyMutable();
          ASSIGN_OR_RETURN(auto result, ObjectCreator::Like(
                                            result_db, shape_and_mask_from,
                                            attr_names, attr_values, item_id));
          frame.Set(output_slot, result.UnsafeMakeWholeOnImmutableDb());
          return absl::OkStatus();
        });
  }
};

class UuObjOperator : public arolla::QExprOperator {
 public:
  explicit UuObjOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.objs.uu",
        [seed_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          ASSIGN_OR_RETURN(absl::string_view seed,
                           GetStringArgument(frame.Get(seed_slot), "seed"));
          auto attr_names = GetFieldNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, frame);
          auto db = koladata::DataBag::EmptyMutable();
          ASSIGN_OR_RETURN(auto result,
                           CreateUuObject(db, seed, attr_names, values));
          frame.Set(output_slot, result.UnsafeMakeWholeOnImmutableDb());
          return absl::OkStatus();
        });
  }
};

absl::StatusOr<arolla::OperatorPtr> ObjOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (!IsDataSliceOrUnspecified(input_types[0])) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice or unspecified");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }

  RETURN_IF_ERROR(VerifyNamedTuple(input_types[2]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ObjOperator>(input_types), input_types, output_type);
}

absl::StatusOr<arolla::OperatorPtr> ObjShapedOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != GetJaggedShapeQType()) {
    return absl::InvalidArgumentError(
        "requires first argument to be JaggedShape");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }

  RETURN_IF_ERROR(VerifyNamedTuple(input_types[2]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ObjShapedOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<arolla::OperatorPtr> ObjLikeOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (!IsDataSliceOrUnspecified(input_types[1])) {
    return absl::InvalidArgumentError(
        "requires itemid argument to be DataSlice or unspecified");
  }

  RETURN_IF_ERROR(VerifyNamedTuple(input_types[2]));
  RETURN_IF_ERROR(VerifyIsNonDeterministicToken(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ObjLikeOperator>(input_types), input_types, output_type);
}

absl::StatusOr<arolla::OperatorPtr> UuObjOperatorFamily::DoGetOperator(
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
      std::make_shared<UuObjOperator>(input_types), input_types, output_type);
}

}  // namespace koladata::ops
