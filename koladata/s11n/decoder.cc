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

#include <cstddef>
#include <memory>
#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/internal/object_id.h"
#include "koladata/s11n/codec.pb.h"
#include "koladata/s11n/codec_names.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/decode.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::s11n {
namespace {

using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::TypedValue;

absl::StatusOr<ValueDecoderResult> DecodeLiteralOperator(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 1 input_value_index, got %d; value=LITERAL_OPERATOR",
        input_values.size()));
  }
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<expr::LiteralOperator>(input_values[0]);
  return TypedValue::FromValue(std::move(op));
}

// Note: it removes used values from input_values (i.e. applies subspan to the
// span).
absl::StatusOr<internal::DataItem> DecodeDataItemProto(
    const KodaV1Proto::DataItemProto& item_proto,
    absl::Span<const TypedValue>& input_values) {
  switch (item_proto.value_case()) {
    case KodaV1Proto::DataItemProto::kBoolean:
      return internal::DataItem(item_proto.boolean());
    case KodaV1Proto::DataItemProto::kBytes:
      return internal::DataItem(arolla::Bytes(item_proto.bytes_()));
    case KodaV1Proto::DataItemProto::kDtype:
      return internal::DataItem(schema::DType(item_proto.dtype()));
    case KodaV1Proto::DataItemProto::kExprQuote: {
      if (input_values.empty()) {
        return absl::InvalidArgumentError(
            "required input_value_index pointing to ExprQuote");
      }
      ASSIGN_OR_RETURN(const auto& q,
                       input_values.front().As<arolla::expr::ExprQuote>());
      input_values = input_values.subspan(1);
      return internal::DataItem(q);
    }
    case KodaV1Proto::DataItemProto::kF32:
      return internal::DataItem(item_proto.f32());
    case KodaV1Proto::DataItemProto::kF64:
      return internal::DataItem(item_proto.f64());
    case KodaV1Proto::DataItemProto::kI32:
      return internal::DataItem(item_proto.i32());
    case KodaV1Proto::DataItemProto::kI64:
      return internal::DataItem(item_proto.i64());
    case KodaV1Proto::DataItemProto::kMissing:
      return internal::DataItem();
    case KodaV1Proto::DataItemProto::kObjectId:
      return internal::DataItem(
          internal::ObjectId::UnsafeCreateFromInternalHighLow(
              item_proto.object_id().hi(), item_proto.object_id().lo()));
    case KodaV1Proto::DataItemProto::kText:
      return internal::DataItem(arolla::Text(item_proto.text()));
    case KodaV1Proto::DataItemProto::kUnit:
      return internal::DataItem(arolla::kUnit);
    case KodaV1Proto::DataItemProto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("value not set");
  }
}

absl::StatusOr<ValueDecoderResult> DecodeDataItemValue(
    const KodaV1Proto::DataItemProto& item_proto,
    absl::Span<const TypedValue> input_values) {
  ASSIGN_OR_RETURN(internal::DataItem res,
                   DecodeDataItemProto(item_proto, input_values));
  if (!input_values.empty()) {
    return absl::InvalidArgumentError("got more input_values than expected");
  }
  return TypedValue::FromValue(std::move(res));
}

absl::StatusOr<ValueDecoderResult> DecodeDataSliceImplValue(
    const KodaV1Proto::DataSliceImplProto& slice_proto,
    absl::Span<const TypedValue> input_values) {
  switch (slice_proto.value_case()) {
    case KodaV1Proto::DataSliceImplProto::kDataItemVector: {
      const auto& vec_proto = slice_proto.data_item_vector();
      internal::DataSliceImpl::Builder bldr(vec_proto.values_size());
      for (size_t i = 0; i < vec_proto.values_size(); ++i) {
        ASSIGN_OR_RETURN(
            internal::DataItem item,
            DecodeDataItemProto(vec_proto.values(i), input_values));
        bldr.Set(i, item);
      }
      if (!input_values.empty()) {
        return absl::InvalidArgumentError(
            "got more input_values than expected");
      }
      return TypedValue::FromValue(std::move(bldr).Build());
    }
    case KodaV1Proto::DataSliceImplProto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("value not set");
  }
  ABSL_UNREACHABLE();
}

absl::StatusOr<ValueDecoderResult> DecodeKodaValue(
    const ValueProto& value_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const arolla::expr::ExprNodePtr> /* input_exprs*/) {
  if (!value_proto.HasExtension(KodaV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& koda_proto =
      value_proto.GetExtension(KodaV1Proto::extension);
  switch (koda_proto.value_case()) {
    case KodaV1Proto::kLiteralOperator:
      return DecodeLiteralOperator(input_values);
    case KodaV1Proto::kDataSliceQtype:
      return TypedValue::FromValue(arolla::GetQType<DataSlice>());
    case KodaV1Proto::kEllipsisQtype:
      return TypedValue::FromValue(
          arolla::GetQType<internal::Ellipsis>());
    case KodaV1Proto::kEllipsisValue:
      return TypedValue::FromValue(internal::Ellipsis{});
    case KodaV1Proto::kInternalDataItemValue:
      return DecodeDataItemValue(koda_proto.internal_data_item_value(),
                                 input_values);
    case KodaV1Proto::kDataSliceImplValue:
      return DecodeDataSliceImplValue(koda_proto.data_slice_impl_value(),
                                      input_values);
    case KodaV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(koda_proto.value_case())));
}

AROLLA_REGISTER_INITIALIZER(
    kRegisterSerializationCodecs, register_serialization_codecs_koda_v1_decoder,
    []() -> absl::Status {
      return arolla::serialization::RegisterValueDecoder(kKodaV1Codec,
                                                         DecodeKodaValue);
    })

}  // namespace
}  // namespace koladata::s11n
