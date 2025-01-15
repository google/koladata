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
#ifndef KOLADATA_OPERATORS_BAGS_H_
#define KOLADATA_OPERATORS_BAGS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/internal/non_deterministic_token.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"

namespace koladata::ops {

// kd.bags.new.
DataBagPtr Bag(internal::NonDeterministicToken);

class EnrichedOrUpdatedDbOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;

 protected:
  virtual bool is_enriched_operator() const = 0;
};

// kd.bags.enriched.
class EnrichedDbOperatorFamily final
    : public EnrichedOrUpdatedDbOperatorFamily {
  bool is_enriched_operator() const override { return true; }
};

// kd.bags.updated.
class UpdatedDbOperatorFamily final : public EnrichedOrUpdatedDbOperatorFamily {
  bool is_enriched_operator() const override { return false; }
};

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_BAGS_H_
