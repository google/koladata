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
#ifndef KOLADATA_OPERATORS_IDS_H_
#define KOLADATA_OPERATORS_IDS_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.ids._deep_uuid
absl::StatusOr<DataSlice> DeepUuid(const DataSlice& ds, const DataSlice& schema,
                                   const DataSlice& seed);

// kd.ids.agg_uuid operator.
absl::StatusOr<DataSlice> AggUuid(const DataSlice& x);

// kd.ids.uuid operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments
class UuidOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.ids.uuid_for_list operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments, used
// for keying ListItems.
class UuidForListOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.ids.uuid_for_dict operator.
// Creates a DataSlice whose items are Fingerprints identifying arguments, used
// for keying DictItems.
class UuidForDictOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kd.ids.uuids_with_allocation_size operator.
// Creates a DataSlice whose items are uuids that share the same allocation.
absl::StatusOr<DataSlice> UuidsWithAllocationSize(const DataSlice& seed,
                                                  const DataSlice& size);

// kd.ids.encode_itemid
absl::StatusOr<DataSlice> EncodeItemId(const DataSlice& ds);

// kd.ids.decode_itemid
absl::StatusOr<DataSlice> DecodeItemId(const DataSlice& ds);

// kd.ids.is_uuid
absl::StatusOr<DataSlice> IsUuid(const DataSlice& x);

// kd.ids.has_uuid
absl::StatusOr<DataSlice> HasUuid(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_IDS_H_
