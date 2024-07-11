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
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/convert_and_eval.h"
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
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

class NewSchemaOperator : public arolla::QExprOperator {
 public:
  explicit NewSchemaOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator("kde.schema._new_schema",
                      arolla::QExprOperatorSignature::Get(
                          input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [named_tuple_slot = input_slots[0],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          auto attr_names = GetAttrNames(named_tuple_slot);
          auto values = GetValueDataSlices(named_tuple_slot, attr_names, frame);
          auto db = koladata::DataBag::Empty();
          koladata::AdoptionQueue adoption_queue;
          for (const auto &ds : values) {
            adoption_queue.Add(ds);
          }
          ASSIGN_OR_RETURN(auto result,
                           SchemaCreator()(db, attr_names, values),
                           ctx->set_status(std::move(_)));
          auto status = adoption_queue.AdoptInto(*db);
          if (!status.ok()) {
            ctx->set_status(std::move(status));
            return;
          }
          frame.Set(output_slot, std::move(result));
        });
  }
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr> NewSchemaOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 1) {
    return absl::InvalidArgumentError("requires exactly 1 argument");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[0]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<NewSchemaOperator>(input_types),
      input_types, output_type);
}

absl::StatusOr<DataSlice> CastTo(const DataSlice& x, const DataSlice& schema,
                                 const DataSlice& implicit_cast) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item() == schema::kObject &&
      x.GetSchemaImpl().is_entity_schema()) {
    return absl::InvalidArgumentError(
        "entity to object casting is unsupported - consider using `kd.obj(x)` "
        "instead");
  }
  ASSIGN_OR_RETURN(bool implicit_cast_unwrapped,
                   ToArollaBoolean(implicit_cast));
  return ::koladata::CastTo(x, schema.item(), implicit_cast_unwrapped);
}

}  // namespace koladata::ops
