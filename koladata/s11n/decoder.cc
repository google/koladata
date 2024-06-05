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

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/s11n/codec.proto.h"
#include "koladata/s11n/codec_names.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/base.proto.h"
#include "arolla/serialization_base/decode.h"
#include "arolla/util/init_arolla.h"

namespace koladata::s11n {
namespace {

using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueDecoderResult> DecodeLiteralOperator(
    absl::Span<const arolla::TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 input_value_index, got %d; value=LITERAL_OPERATOR",
        input_values.size()));
  }
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<expr::LiteralOperator>(input_values[0]);
  return arolla::TypedValue::FromValue(std::move(op));
}

absl::StatusOr<ValueDecoderResult> DecodeKodaValue(
    const ValueProto& value_proto,
    absl::Span<const arolla::TypedValue> input_values,
    absl::Span<const arolla::expr::ExprNodePtr> /* input_exprs*/) {
  if (!value_proto.HasExtension(KodaV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& operators_proto =
      value_proto.GetExtension(KodaV1Proto::extension);
  switch (operators_proto.value_case()) {
    case KodaV1Proto::kLiteralOperator:
      return DecodeLiteralOperator(input_values);
    case KodaV1Proto::kDataSliceQtype:
      return arolla::TypedValue::FromValue(arolla::GetQType<DataSlice>());
    case KodaV1Proto::kEllipsisQtype:
      return arolla::TypedValue::FromValue(
          arolla::GetQType<internal::Ellipsis>());
    case KodaV1Proto::kEllipsisValue:
      return arolla::TypedValue::FromValue(internal::Ellipsis{});
    case KodaV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(operators_proto.value_case())));
}

AROLLA_REGISTER_INITIALIZER(
    kRegisterSerializationCodecs, register_serialization_codecs_koda_v1_decoder,
    []() -> absl::Status {
      return arolla::serialization::RegisterValueDecoder(kKodaV1Codec,
                                                         DecodeKodaValue);
    })

}  // namespace
}  // namespace koladata::s11n
