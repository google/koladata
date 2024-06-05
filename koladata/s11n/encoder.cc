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
#include <cstdint>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/s11n/codec.proto.h"
#include "koladata/s11n/codec_names.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/encode.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::s11n {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kKodaV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeLiteralOperator(arolla::TypedRef value,
                                                 Encoder& encoder) {
  // NOTE: We assume value to be a LiteralOperator since we dispatch for it.
  const auto* op =
      arolla::fast_dynamic_downcast_final<const expr::LiteralOperator*>(
          value.UnsafeAs<arolla::expr::ExprOperatorPtr>().get());
  DCHECK_NE(op, nullptr);
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(KodaV1Proto::extension)
      ->set_literal_operator(true);
  ASSIGN_OR_RETURN(
      auto value_index, encoder.EncodeValue(op->value()),
      _ << "during EncodeValue(op->value()); value=LITERAL_OPERATOR");
  value_proto.add_input_value_indices(value_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDataSliceQType(arolla::TypedRef value,
                                                Encoder& encoder) {
  // Note: Safe since this function is only called for QTypes.
  const auto& qtype = value.UnsafeAs<arolla::QTypePtr>();
  if (qtype != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s", kKodaV1Codec,
                        qtype->name()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(KodaV1Proto::extension)
      ->set_data_slice_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDataSlice(arolla::TypedRef value,
                                           Encoder& encoder) {
  if (value.GetType() == arolla::GetQType<arolla::QTypePtr>()) {
    return EncodeDataSliceQType(value, encoder);
  } else if (value.GetType() == arolla::GetQType<DataSlice>()) {
    return absl::UnimplementedError(absl::StrFormat(
        "%s does not support DataSlice value serialization yet", kKodaV1Codec));
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s: %s",
                        kKodaV1Codec, value.GetType()->name(), value.Repr()));
  }
}

absl::StatusOr<ValueProto> EncodeEllipsis(arolla::TypedRef value,
                                          Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* koda_proto = value_proto.MutableExtension(KodaV1Proto::extension);
  if (value.GetType() == arolla::GetQType<arolla::QTypePtr>()) {
    DCHECK_EQ(value.UnsafeAs<arolla::QTypePtr>(),
              arolla::GetQType<internal::Ellipsis>());
    koda_proto->set_ellipsis_qtype(true);
  } else {
    DCHECK_EQ(value.GetType(), arolla::GetQType<internal::Ellipsis>());
    koda_proto->set_ellipsis_value(true);
  }
  return value_proto;
}

AROLLA_REGISTER_INITIALIZER(
    kRegisterSerializationCodecs, register_serialization_codecs_koda_v1_encoder,
    []() -> absl::Status {
      RETURN_IF_ERROR(
          arolla::serialization::RegisterValueEncoderByQValueSpecialisationKey(
              "::koladata::expr::LiteralOperator", EncodeLiteralOperator));
      RETURN_IF_ERROR(arolla::serialization::RegisterValueEncoderByQType(
          arolla::GetQType<DataSlice>(), EncodeDataSlice));
      RETURN_IF_ERROR(arolla::serialization::RegisterValueEncoderByQType(
          arolla::GetQType<internal::Ellipsis>(), EncodeEllipsis));
      return absl::OkStatus();
    })

}  // namespace
}  // namespace koladata::s11n
