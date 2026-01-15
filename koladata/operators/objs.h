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
#ifndef KOLADATA_OPERATORS_OBJS_H_
#define KOLADATA_OPERATORS_OBJS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.objs.new.
class ObjOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.objs.shaped.
class ObjShapedOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.objs.like.
class ObjLikeOperatorFamily final : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const override;
};

// kd.objs.uu operator.
// Creates a DataSlice of UuObjects.
class UuObjOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// If `slice` does not have a DataBag, creates a new DataBag and adopts `slice`
// into it. Otherwise calls `ObjectCreator::ConvertWithoutAdopt` for it and
// attaches the `db` to the result.
// TODO(b/475760871) Move this to object_factories.
absl::StatusOr<DataSlice> ConvertWithAdoption(const DataBagPtr& db,
                                              const DataSlice& value);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_OBJS_H_
