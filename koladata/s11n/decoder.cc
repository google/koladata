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
#include "arolla/serialization_base/decoder.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/data_bag.h"
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
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::s11n {
namespace {

using ::arolla::TypedValue;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::RegisterValueDecoder;

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

internal::ObjectId DecodeObjectId(const KodaV1Proto::ObjectIdProto& proto) {
  return internal::ObjectId::UnsafeCreateFromInternalHighLow(proto.hi(),
                                                             proto.lo());
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
      return internal::DataItem(DecodeObjectId(item_proto.object_id()));
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
        bldr.Insert(i, item);
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

// ValueProto.input_value_indices[0]: DataSliceImpl or DataItem
// ValueProto.input_value_indices[1]: JaggedShape
// ValueProto.input_value_indices[2]: Schema (DataItem)
// ValueProto.input_value_indices[3]: (optional) DataBag
absl::StatusOr<ValueDecoderResult> DecodeDataSliceValue(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 3 && input_values.size() != 4) {
    return absl::InvalidArgumentError(
        absl::StrCat("wrong number of input_values in DecodeDataSliceValue: ",
                     input_values.size()));
  }
  ASSIGN_OR_RETURN(auto shape, input_values[1].As<DataSlice::JaggedShape>());
  ASSIGN_OR_RETURN(auto schema, input_values[2].As<internal::DataItem>());
  DataBagPtr db = nullptr;
  if (input_values.size() == 4) {
    ASSIGN_OR_RETURN(db, input_values[3].As<DataBagPtr>());
  }

  auto create_data_slice =
      [&]<class T>(
          std::type_identity<T>) -> absl::StatusOr<ValueDecoderResult> {
    ASSIGN_OR_RETURN(const T& item, input_values[0].As<T>());
    ASSIGN_OR_RETURN(
        auto res, DataSlice::Create(item, std::move(shape), std::move(schema),
                                    std::move(db)));
    return TypedValue::FromValue(std::move(res));
  };

  if (input_values[0].GetType() == arolla::GetQType<internal::DataItem>()) {
    return create_data_slice(std::type_identity<internal::DataItem>());
  } else {
    return create_data_slice(std::type_identity<internal::DataSliceImpl>());
  }
}

template <typename T>
absl::StatusOr<std::reference_wrapper<const T>> GetInputValue(
    absl::Span<const TypedValue> input_values, int index) {
  if (index < 0 || index >= input_values.size()) {
    return absl::InvalidArgumentError("invalid input value index");
  }
  return input_values[index].As<T>();
}

absl::Status DecodeAttrProto(const KodaV1Proto::AttrProto& attr_proto,
                             absl::Span<const TypedValue> input_values,
                             internal::DataBagImpl& db) {
  for (const KodaV1Proto::AttrChunkProto& chunk_proto : attr_proto.chunks()) {
    internal::ObjectId obj = DecodeObjectId(chunk_proto.first_object_id());
    if (chunk_proto.values_subindex() < 0 ||
        chunk_proto.values_subindex() >= input_values.size()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "invalid input value index: ", chunk_proto.values_subindex()));
    }
    const TypedValue& tval = input_values[chunk_proto.values_subindex()];
    if (tval.GetType() == arolla::GetQType<internal::DataItem>()) {
      RETURN_IF_ERROR(db.SetAttr(internal::DataItem(obj), attr_proto.name(),
                                 tval.UnsafeAs<internal::DataItem>()));
    } else {
      ASSIGN_OR_RETURN(const internal::DataSliceImpl& values,
                       tval.As<internal::DataSliceImpl>());
      if (obj.Offset() != 0) {
        return absl::InvalidArgumentError(
            "AttrChunkProto.first_object_id must have offset==0 if the chunk "
            "contains multiple values");
      }
      internal::AllocationId alloc(obj);
      if (alloc.Capacity() <= values.size()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "AttrChunkProto values don't fit into AllocationId of "
            "`first_object_id`: values size is %d, alloc capacity is %d",
            values.size(), alloc.Capacity()));
      }
      auto objects =
          internal::DataSliceImpl::ObjectsFromAllocation(alloc, values.size());
      RETURN_IF_ERROR(db.SetAttr(objects, attr_proto.name(), values));
    }
  }
  return absl::OkStatus();
}

absl::Status DecodeListProto(const KodaV1Proto::ListProto& list_proto,
                             absl::Span<const TypedValue> input_values,
                             internal::DataBagImpl& db) {
  internal::DataItem list_id(DecodeObjectId(list_proto.list_id()));
  ASSIGN_OR_RETURN(const internal::DataSliceImpl& values,
                   GetInputValue<internal::DataSliceImpl>(
                       input_values, list_proto.values_subindex()));
  return db.ExtendList(list_id, values);
}

absl::Status DecodeDictProto(const KodaV1Proto::DictProto& dict_proto,
                             absl::Span<const TypedValue> input_values,
                             internal::DataBagImpl& db) {
  internal::ObjectId dict_id = DecodeObjectId(dict_proto.dict_id());
  ASSIGN_OR_RETURN(internal::DataSliceImpl keys,
                   GetInputValue<internal::DataSliceImpl>(
                       input_values, dict_proto.keys_subindex()));
  ASSIGN_OR_RETURN(internal::DataSliceImpl values,
                   GetInputValue<internal::DataSliceImpl>(
                       input_values, dict_proto.values_subindex()));
  if (dict_id.IsSchema()) {
    internal::DataItem schema(dict_id);
    for (int i = 0; i < keys.size(); ++i) {
      if (!keys[i].holds_value<arolla::Text>()) {
        return absl::InvalidArgumentError("schema key must be arolla::Text");
      }
      RETURN_IF_ERROR(
          db.SetSchemaAttr(schema, keys[i].value<arolla::Text>(), values[i]));
    }
    return absl::OkStatus();
  } else {
    return db.SetInDict(internal::DataSliceImpl::Create(
                            keys.size(), internal::DataItem(dict_id)),
                        std::move(keys), std::move(values));
  }
}

absl::StatusOr<ValueDecoderResult> DecodeDataBagValue(
    const KodaV1Proto::DataBagProto& db_proto,
    absl::Span<const TypedValue> input_values) {
  if (db_proto.fallback_count() > 0) {
    if (db_proto.attrs_size() > 0 || db_proto.lists_size() > 0 ||
        db_proto.lists_size()) {
      return absl::InvalidArgumentError(
          "only empty DataBag can have fallbacks");
    }
    std::vector<DataBagPtr> fallbacks;
    fallbacks.reserve(db_proto.fallback_count());
    for (int i = 0; i < db_proto.fallback_count(); ++i) {
      ASSIGN_OR_RETURN(fallbacks.emplace_back(),
                       input_values[i].As<DataBagPtr>());
    }
    return TypedValue::FromValue(
        DataBag::ImmutableEmptyWithFallbacks(std::move(fallbacks)));
  }
  DataBagPtr db = DataBag::Empty();
  ASSIGN_OR_RETURN(internal::DataBagImpl & impl, db->GetMutableImpl());
  for (const KodaV1Proto::AttrProto& attr_proto : db_proto.attrs()) {
    RETURN_IF_ERROR(DecodeAttrProto(attr_proto, input_values, impl));
  }
  for (const KodaV1Proto::ListProto& list_proto : db_proto.lists()) {
    RETURN_IF_ERROR(DecodeListProto(list_proto, input_values, impl));
  }
  for (const KodaV1Proto::DictProto& dict_proto : db_proto.dicts()) {
    RETURN_IF_ERROR(DecodeDictProto(dict_proto, input_values, impl));
  }
  return TypedValue::FromValue(std::move(db));
}

absl::StatusOr<ValueDecoderResult> DecodeKodaValue(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const arolla::expr::ExprNodePtr> /* input_exprs*/) {
  if (!value_proto.HasExtension(KodaV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& koda_proto = value_proto.GetExtension(KodaV1Proto::extension);
  switch (koda_proto.value_case()) {
    case KodaV1Proto::kLiteralOperator:
      return DecodeLiteralOperator(input_values);
    case KodaV1Proto::kDataSliceQtype:
      return TypedValue::FromValue(arolla::GetQType<DataSlice>());
    case KodaV1Proto::kEllipsisQtype:
      return TypedValue::FromValue(arolla::GetQType<internal::Ellipsis>());
    case KodaV1Proto::kEllipsisValue:
      return TypedValue::FromValue(internal::Ellipsis{});
    case KodaV1Proto::kInternalDataItemValue:
      return DecodeDataItemValue(koda_proto.internal_data_item_value(),
                                 input_values);
    case KodaV1Proto::kDataSliceImplValue:
      return DecodeDataSliceImplValue(koda_proto.data_slice_impl_value(),
                                      input_values);
    case KodaV1Proto::kDataBagQtype:
      return arolla::TypedValue::FromValue(arolla::GetQType<DataBagPtr>());
    case KodaV1Proto::kDataSliceValue:
      return DecodeDataSliceValue(input_values);
    case KodaV1Proto::kDataBagValue:
      return DecodeDataBagValue(koda_proto.data_bag_value(), input_values);
    case KodaV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(koda_proto.value_case())));
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n}, .init_fn = [] {
          return RegisterValueDecoder(kKodaV1Codec, DecodeKodaValue);
        })

}  // namespace
}  // namespace koladata::s11n
