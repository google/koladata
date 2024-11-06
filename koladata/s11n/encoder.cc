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
#include "arolla/serialization_base/encoder.h"

#include <cstdint>
#include <string>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
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
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/s11n/codec.pb.h"
#include "koladata/s11n/codec_names.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::s11n {
namespace {

using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::RegisterValueEncoderByQType;
using ::arolla::serialization_codecs::
    RegisterValueEncoderByQValueSpecialisationKey;

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

absl::StatusOr<ValueProto> EncodeDataBagQType(arolla::TypedRef value,
                                              Encoder& encoder) {
  // Note: Safe since this function is only called for QTypes.
  DCHECK_EQ(value.UnsafeAs<arolla::QTypePtr>(), arolla::GetQType<DataBagPtr>());
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  value_proto.MutableExtension(KodaV1Proto::extension)
      ->set_data_bag_qtype(true);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDataSlice(arolla::TypedRef value,
                                           Encoder& encoder) {
  if (value.GetType() == arolla::GetQType<arolla::QTypePtr>()) {
    return EncodeDataSliceQType(value, encoder);
  }
  if (value.GetType() != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s does not support serialization of %s: %s",
                        kKodaV1Codec, value.GetType()->name(), value.Repr()));
  }
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* koda_proto = value_proto.MutableExtension(KodaV1Proto::extension);
  koda_proto->set_data_slice_value(true);
  const DataSlice& slice = value.UnsafeAs<DataSlice>();
  ASSIGN_OR_RETURN(auto values_index, slice.VisitImpl([&](const auto& v) {
    return encoder.EncodeValue(arolla::TypedValue::FromValue(v));
  }));
  ASSIGN_OR_RETURN(
      auto shape_index,
      encoder.EncodeValue(arolla::TypedValue::FromValue(slice.GetShape())));
  ASSIGN_OR_RETURN(auto schema_index,
                   encoder.EncodeValue(
                       arolla::TypedValue::FromValue(slice.GetSchemaImpl())));
  value_proto.add_input_value_indices(values_index);
  value_proto.add_input_value_indices(shape_index);
  value_proto.add_input_value_indices(schema_index);
  if (slice.GetBag() != nullptr) {
    ASSIGN_OR_RETURN(
        auto db_index,
        encoder.EncodeValue(arolla::TypedValue::FromValue(slice.GetBag())));
    value_proto.add_input_value_indices(db_index);
  }
  return value_proto;
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

void EncodeObjectId(internal::ObjectId id, KodaV1Proto::ObjectIdProto* proto) {
  proto->set_hi(id.InternalHigh64());
  proto->set_lo(id.InternalLow64());
}

absl::Status EncodeAttribute(const std::string& attr_name,
                             const internal::DataBagContent::AttrContent& data,
                             KodaV1Proto::AttrProto& attr_proto,
                             ValueProto& value_proto, Encoder& encoder) {
  attr_proto.set_name(attr_name);
  for (const internal::DataBagContent::AttrItemContent& ic : data.items) {
    KodaV1Proto::AttrChunkProto* chunk_proto = attr_proto.add_chunks();
    EncodeObjectId(ic.object_id, chunk_proto->mutable_first_object_id());
    ASSIGN_OR_RETURN(
        auto value_index,
        encoder.EncodeValue(arolla::TypedValue::FromValue(ic.value)));
    chunk_proto->set_values_subindex(value_proto.input_value_indices_size());
    value_proto.add_input_value_indices(value_index);
  }
  for (const internal::DataBagContent::AttrAllocContent& ac : data.allocs) {
    KodaV1Proto::AttrChunkProto* chunk_proto = attr_proto.add_chunks();
    EncodeObjectId(ac.alloc_id.ObjectByOffset(0),
                   chunk_proto->mutable_first_object_id());
    ASSIGN_OR_RETURN(
        auto values_index,
        encoder.EncodeValue(arolla::TypedValue::FromValue(ac.values)));
    chunk_proto->set_values_subindex(value_proto.input_value_indices_size());
    value_proto.add_input_value_indices(values_index);
  }
  return absl::OkStatus();
}

absl::Status EncodeDict(const internal::DataBagContent::DictContent& data,
                        KodaV1Proto::DictProto& dict_proto,
                        ValueProto& value_proto, Encoder& encoder) {
  EncodeObjectId(data.dict_id, dict_proto.mutable_dict_id());
  ASSIGN_OR_RETURN(auto keys_index,
                   encoder.EncodeValue(arolla::TypedValue::FromValue(
                       internal::DataSliceImpl::Create(data.keys))));
  ASSIGN_OR_RETURN(auto values_index,
                   encoder.EncodeValue(arolla::TypedValue::FromValue(
                       internal::DataSliceImpl::Create(data.values))));
  dict_proto.set_keys_subindex(value_proto.input_value_indices_size());
  dict_proto.set_values_subindex(value_proto.input_value_indices_size() + 1);
  value_proto.add_input_value_indices(keys_index);
  value_proto.add_input_value_indices(values_index);
  return absl::OkStatus();
}

absl::Status EncodeLists(const internal::DataBagContent::ListsContent& lists,
                         KodaV1Proto::DataBagProto& db_proto,
                         ValueProto& value_proto, Encoder& encoder) {
  DCHECK_EQ(lists.lists_to_values_edge.edge_type(),
            arolla::DenseArrayEdge::SPLIT_POINTS);
  absl::Span<const int64_t> splits =
      lists.lists_to_values_edge.edge_values().values.span();
  DCHECK_GT(splits.size(), 0);
  int64_t size = splits.size() - 1;
  for (int64_t i = 0; i < size; ++i) {
    KodaV1Proto::ListProto* list_proto = db_proto.add_lists();
    EncodeObjectId(lists.alloc_id.ObjectByOffset(i),
                   list_proto->mutable_list_id());
    // TODO(b/328742873) Find a way to do it without copying data.
    internal::SliceBuilder bldr(splits[i + 1] - splits[i]);
    for (int64_t j = 0; j < splits[i + 1] - splits[i]; ++j) {
      bldr.InsertIfNotSetAndUpdateAllocIds(j, lists.values[j + splits[i]]);
    }
    ASSIGN_OR_RETURN(auto values_index,
                     encoder.EncodeValue(arolla::TypedValue::FromValue(
                         std::move(bldr).Build())));
    list_proto->set_values_subindex(value_proto.input_value_indices_size());
    value_proto.add_input_value_indices(values_index);
  }
  return absl::OkStatus();
}

absl::StatusOr<ValueProto> EncodeDataBag(arolla::TypedRef value,
                                         Encoder& encoder) {
  if (value.GetType() == arolla::GetQType<arolla::QTypePtr>()) {
    return EncodeDataBagQType(value, encoder);
  }
  if (value.GetType() != arolla::GetQType<DataBagPtr>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "DataBagPtr expected, got %s", value.GetType()->name()));
  }
  const DataBagPtr& db = value.UnsafeAs<DataBagPtr>();

  ASSIGN_OR_RETURN(ValueProto value_proto, GenValueProto(encoder));
  auto* koda_proto = value_proto.MutableExtension(KodaV1Proto::extension);
  KodaV1Proto::DataBagProto* db_proto = koda_proto->mutable_data_bag_value();

  db_proto->set_fallback_count(db->GetFallbacks().size());
  for (const auto& fb : db->GetFallbacks()) {
    ASSIGN_OR_RETURN(auto fb_index,
                     encoder.EncodeValue(arolla::TypedValue::FromValue(fb)));
    value_proto.add_input_value_indices(fb_index);
  }

  ASSIGN_OR_RETURN(internal::DataBagContent content,
                   db->GetImpl().ExtractContent());
  for (const auto& [attr_name, data] : content.attrs) {
    RETURN_IF_ERROR(EncodeAttribute(attr_name, data, *db_proto->add_attrs(),
                                    value_proto, encoder));
  }
  for (const internal::DataBagContent::DictContent& d : content.dicts) {
    RETURN_IF_ERROR(
        EncodeDict(d, *db_proto->add_dicts(), value_proto, encoder));
  }
  for (const internal::DataBagContent::ListsContent& lists : content.lists) {
    RETURN_IF_ERROR(EncodeLists(lists, *db_proto, value_proto, encoder));
  }
  return value_proto;
}

absl::Status FillItemProto(Encoder& encoder, ValueProto& value_proto,
                           KodaV1Proto::DataItemProto& item_proto,
                           const internal::DataItem& item) {
  return item.VisitValue([&]<typename T>(const T& v) -> absl::Status {
    if constexpr (std::is_same_v<T, internal::MissingValue>) {
      item_proto.set_missing(true);
    } else if constexpr (std::is_same_v<T, internal::ObjectId>) {
      auto* id_proto = item_proto.mutable_object_id();
      id_proto->set_hi(v.InternalHigh64());
      id_proto->set_lo(v.InternalLow64());
    } else if constexpr (std::is_same_v<T, int32_t>) {
      item_proto.set_i32(v);
    } else if constexpr (std::is_same_v<T, int64_t>) {
      item_proto.set_i64(v);
    } else if constexpr (std::is_same_v<T, float>) {
      item_proto.set_f32(v);
    } else if constexpr (std::is_same_v<T, double>) {
      item_proto.set_f64(v);
    } else if constexpr (std::is_same_v<T, bool>) {
      item_proto.set_boolean(v);
    } else if constexpr (std::is_same_v<T, arolla::Unit>) {
      item_proto.set_unit(true);
    } else if constexpr (std::is_same_v<T, arolla::Text>) {
      item_proto.set_text(v.view());
    } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
      item_proto.set_bytes_(v);
    } else if constexpr (std::is_same_v<T, schema::DType>) {
      item_proto.set_dtype(v.type_id());
    } else if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
      item_proto.set_expr_quote(true);
      ASSIGN_OR_RETURN(auto index,
                       encoder.EncodeValue(arolla::TypedValue::FromValue(v)));
      value_proto.add_input_value_indices(index);
    } else {
      static_assert(false);
    }
    return absl::OkStatus();
  });
}

absl::StatusOr<ValueProto> EncodeDataItem(arolla::TypedRef value,
                                          Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* koda_proto = value_proto.MutableExtension(KodaV1Proto::extension);
  ASSIGN_OR_RETURN(const internal::DataItem& item,
                   value.As<internal::DataItem>());
  RETURN_IF_ERROR(FillItemProto(encoder, value_proto,
                                *koda_proto->mutable_internal_data_item_value(),
                                item));
  return value_proto;
}

absl::StatusOr<ValueProto> EncodeDataSliceImpl(arolla::TypedRef value,
                                               Encoder& encoder) {
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* koda_proto = value_proto.MutableExtension(KodaV1Proto::extension);
  ASSIGN_OR_RETURN(const internal::DataSliceImpl& slice,
                   value.As<internal::DataSliceImpl>());
  KodaV1Proto::DataSliceImplProto* slice_proto =
      koda_proto->mutable_data_slice_impl_value();
  KodaV1Proto::DataItemVectorProto* vector_proto =
      slice_proto->mutable_data_item_vector();
  for (int64_t i = 0; i < slice.size(); ++i) {
    RETURN_IF_ERROR(FillItemProto(encoder, value_proto,
                                  *vector_proto->add_values(), slice[i]));
  }
  return value_proto;
}

AROLLA_INITIALIZER(
        .reverse_deps = {arolla::initializer_dep::kS11n},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
              "::koladata::expr::LiteralOperator", EncodeLiteralOperator));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              arolla::GetQType<DataSlice>(), EncodeDataSlice));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              arolla::GetQType<internal::Ellipsis>(), EncodeEllipsis));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              arolla::GetQType<DataBagPtr>(), EncodeDataBag));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              arolla::GetQType<internal::DataItem>(), EncodeDataItem));
          RETURN_IF_ERROR(RegisterValueEncoderByQType(
              arolla::GetQType<internal::DataSliceImpl>(),
              EncodeDataSliceImpl));
          return absl::OkStatus();
        })

}  // namespace
}  // namespace koladata::s11n
