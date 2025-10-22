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
#ifndef KOLADATA_OPERATORS_PROTO_H_
#define KOLADATA_OPERATORS_PROTO_H_

#include "absl/status/statusor.h"
#include "arolla/qexpr/eval_context.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/uuid_utils.h"

namespace koladata::ops {

// kd.proto.from_proto_bytes
absl::StatusOr<DataSlice> FromProtoBytes(
    arolla::EvaluationContext* ctx,
    const DataSlice& x, const DataSlice& proto_path,
    const DataSlice& extensions = UnspecifiedDataSlice(),
    const DataSlice& itemids = UnspecifiedDataSlice(),
    const DataSlice& schema = UnspecifiedDataSlice(),
    const DataSlice& on_invalid = UnspecifiedDataSlice(),
    const internal::NonDeterministicToken& = internal::NonDeterministicToken());

// kd.proto.to_proto_bytes
absl::StatusOr<DataSlice> ToProtoBytes(
    const DataSlice& x, const DataSlice& proto_path);

// kd.proto.from_proto_json
absl::StatusOr<DataSlice> FromProtoJson(
    arolla::EvaluationContext* ctx,
    const DataSlice& x, const DataSlice& proto_path,
    const DataSlice& extensions = UnspecifiedDataSlice(),
    const DataSlice& itemids = UnspecifiedDataSlice(),
    const DataSlice& schema = UnspecifiedDataSlice(),
    const DataSlice& on_invalid = UnspecifiedDataSlice(),
    const internal::NonDeterministicToken& = internal::NonDeterministicToken());

// kd.proto.to_proto_json
absl::StatusOr<DataSlice> ToProtoJson(const DataSlice& x,
                                      const DataSlice& proto_path);

// kd.proto.schema_from_proto_path
absl::StatusOr<DataSlice> SchemaFromProtoPath(
    const DataSlice& proto_path,
    const DataSlice& extensions = UnspecifiedDataSlice());

// kd.proto.get_proto_full_name
absl::StatusOr<DataSlice> GetProtoFullName(const DataSlice& x);

// kd.proto.get_proto_field_custom_default
absl::StatusOr<DataSlice> GetProtoFieldCustomDefault(
    const DataSlice& x, const DataSlice& field_name);

// kd.proto.get_proto_attr
absl::StatusOr<DataSlice> GetProtoAttr(const DataSlice& x,
                                       const DataSlice& field_name);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_PROTO_H_
