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
#include "koladata/operators/bags.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/op_utils/qexpr.h"

namespace koladata::ops {

DataBagPtr Bag(internal::NonDeterministicToken) { return DataBag::Empty(); }

absl::StatusOr<DataSlice> IsNullBag(const DataBagPtr& bag) {
  return DataSlice::Create(
      bag == nullptr ?
      internal::DataItem(arolla::kUnit) : internal::DataItem(),
      internal::DataItem(schema::kMask));
}

namespace {

class EnrichedOrUpdatedDbOperator final : public arolla::QExprOperator {
 public:
  EnrichedOrUpdatedDbOperator(absl::Span<const arolla::QTypePtr> input_types,
                              bool is_enriched_operator)
      : arolla::QExprOperator(input_types, arolla::GetQType<DataBagPtr>()),
        is_enriched_operator_(is_enriched_operator) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    return MakeBoundOperator(
        is_enriched_operator_ ? "kd.bags.enriched" : "kd.bags.updated",
        [input_slots = std::vector<arolla::TypedSlot>(input_slots.begin(),
                                                      input_slots.end()),
         output_slot = output_slot.UnsafeToSlot<DataBagPtr>(),
         is_enriched_operator = is_enriched_operator_](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          std::vector<DataBagPtr> db_list(input_slots.size());
          for (size_t i = 0; i < input_slots.size(); ++i) {
            db_list[i] = frame.Get(input_slots[i].UnsafeToSlot<DataBagPtr>());
          }
          if (!is_enriched_operator) {
            std::reverse(db_list.begin(), db_list.end());
          }
          frame.Set(output_slot, DataBag::ImmutableEmptyWithFallbacks(db_list));
          return absl::OkStatus();
        });
  }

  bool is_enriched_operator_;
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
EnrichedOrUpdatedDbOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  for (const auto& db_input_type : input_types) {
    if (db_input_type != arolla::GetQType<DataBagPtr>()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "arguments must be DataBag, but got ", db_input_type->name()));
    }
  }

  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<EnrichedOrUpdatedDbOperator>(input_types,
                                                    is_enriched_operator()),
      input_types, output_type);
}

}  // namespace koladata::ops
