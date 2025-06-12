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
#include "arolla/serialization_base/decoder.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/memory/buffer.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/ellipsis.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "koladata/internal/types_buffer.h"
#include "koladata/jagged_shape_qtype.h"
#include "koladata/s11n/codec.pb.h"
#include "koladata/s11n/codec_names.h"
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
      expr::LiteralOperator::MakeLiteralOperator(input_values[0]);
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
    case KodaV1Proto::DataItemProto::kBytesData:
      return internal::DataItem(arolla::Bytes(item_proto.bytes_data()));
    case KodaV1Proto::DataItemProto::kDtype: {
      ASSIGN_OR_RETURN(schema::DType dtype,
                       schema::DType::FromId(item_proto.dtype()));
      return internal::DataItem(dtype);
    }
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

absl::StatusOr<ValueDecoderResult> DecodeDataItemVectorProto(
    const KodaV1Proto::DataItemVectorProto& vec_proto,
    absl::Span<const TypedValue> input_values) {
  internal::SliceBuilder bldr(vec_proto.values_size());
  for (size_t i = 0; i < vec_proto.values_size(); ++i) {
    ASSIGN_OR_RETURN(internal::DataItem item,
                     DecodeDataItemProto(vec_proto.values(i), input_values));
    bldr.InsertIfNotSetAndUpdateAllocIds(i, item);
  }
  if (!input_values.empty()) {
    return absl::InvalidArgumentError("got more input_values than expected");
  }
  return TypedValue::FromValue(std::move(bldr).Build());
}

// Returns a reference to the values field in the DataSliceCompactProto.
// For types that are not stored in the DataSliceCompactProto,
// returns a reference to a fake object that shouldn't be used.
template <typename T>
const auto& GetValuesFromDataSliceCompactProto(
    const KodaV1Proto::DataSliceCompactProto& slice_proto) {
  if constexpr (std::is_same_v<T, internal::ObjectId>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kObjectIdFieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.object_id();
  } else if constexpr (std::is_same_v<T, int32_t>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kI32FieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.i32();
  } else if constexpr (std::is_same_v<T, int64_t>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kI64FieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.i64();
  } else if constexpr (std::is_same_v<T, float>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kF32FieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.f32();
  } else if constexpr (std::is_same_v<T, double>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kF64FieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.f64();
  } else if constexpr (std::is_same_v<T, bool>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kBooleanFieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.boolean();
  } else if constexpr (std::is_same_v<T, arolla::Unit>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kUnitFieldNumber ==
                  internal::ScalarTypeId<T>());
    static std::type_identity<T> res;
    return res;
  } else if constexpr (std::is_same_v<T, arolla::Text>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kTextFieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.text();
  } else if constexpr (std::is_same_v<T, arolla::Bytes>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kBytesDataFieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.bytes_data();
  } else if constexpr (std::is_same_v<T, schema::DType>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kDtypeFieldNumber ==
                  internal::ScalarTypeId<T>());
    return slice_proto.dtype();
  } else if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
    static_assert(KodaV1Proto::DataSliceCompactProto::kExprQuoteFieldNumber ==
                  internal::ScalarTypeId<T>());
    static std::type_identity<T> res;
    return res;
  } else {
    static_assert(sizeof(T) == 0, "unsupported type for DataSliceCompactProto");
  }
  ABSL_UNREACHABLE();
}

absl::StatusOr<ValueDecoderResult> DecodeDataSliceCompactProto(
    const KodaV1Proto::DataSliceCompactProto& slice_proto,
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() < slice_proto.extra_part_count()) {
    return absl::InvalidArgumentError("invalid extra_part_count");
  }
  std::vector<KodaV1Proto::DataSliceCompactProto> extra_parts;
  extra_parts.resize(slice_proto.extra_part_count());
  for (int64_t i = 0; i < slice_proto.extra_part_count(); ++i) {
    ASSIGN_OR_RETURN(
        const std::string& data,
        input_values[input_values.size() - slice_proto.extra_part_count() + i]
            .As<std::string>());
    extra_parts[i].ParseFromString(data);
  }
  input_values = input_values.subspan(
      0, input_values.size() - slice_proto.extra_part_count());
  const std::string& proto_types_buffer = slice_proto.types_buffer();
  internal::SliceBuilder bldr(proto_types_buffer.size());
  uint64_t processed_types = 0;
  static_assert(std::variant_size_v<internal::ScalarVariant> <
                sizeof(uint64_t) * 8);

  auto is_valid_present_type_idx = [](uint8_t idx) {
    return idx > 0 && idx < std::variant_size_v<internal::ScalarVariant>;
  };

  auto get_values_size = [&]<typename T>(
                             std::type_identity<T>,
                             const auto& values) -> absl::StatusOr<size_t> {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      if (values.hi_size() != values.lo_size()) {
        return absl::InvalidArgumentError(
            "DataSliceCompactProto has different number of hi and lo values");
      }
      return values.hi_size();
    } else if constexpr (std::is_same_v<T, arolla::Unit> ||
                         std::is_same_v<T, arolla::expr::ExprQuote>) {
      // For these types we don't store the values in the proto.
      // So we return 0 to indicate that all values are always used.
      // ExprQuote will be read from input_values.
      return 0;
    } else {
      return values.size();
    }
  };

  auto last_object_id =
      internal::ObjectId::UnsafeCreateFromInternalHighLow(0, 0);

  auto process_type = [&]<typename T>(
                          std::type_identity<T>,
                          size_t start_id) -> absl::StatusOr<size_t> {
    auto typed_bldr = bldr.typed<T>();
    const auto* values = &GetValuesFromDataSliceCompactProto<T>(slice_proto);
    ASSIGN_OR_RETURN(size_t part_size,
                     get_values_size(std::type_identity<T>(), *values));
    size_t next_start_id = proto_types_buffer.size();
    size_t last_id = 0;
    size_t part_offset = 0;
    auto part_it = extra_parts.begin();
    auto next_part = [&]() -> absl::Status {
      if (part_it == extra_parts.end()) {
        return absl::InvalidArgumentError(absl::StrCat(
          "DataSliceCompactProto has not enough values for type ",
          arolla::GetQType<T>()->name()));
      }
      part_offset += part_size;
      values = &GetValuesFromDataSliceCompactProto<T>(*part_it++);
      ASSIGN_OR_RETURN(part_size,
                       get_values_size(std::type_identity<T>(), *values));
      return absl::OkStatus();
    };
    for (size_t cur = start_id; cur < proto_types_buffer.size(); ++cur) {
      uint8_t cur_type_idx = static_cast<uint8_t>(proto_types_buffer[cur]);
      if (cur_type_idx == internal::TypesBuffer::kUnset) {
        continue;
      }
      if (cur_type_idx == internal::TypesBuffer::kRemoved) {
        bldr.InsertIfNotSet(cur, std::nullopt);
        continue;
      }
      if (!is_valid_present_type_idx(cur_type_idx)) {
        return absl::InvalidArgumentError(
            absl::StrCat("invalid type index: ", cur_type_idx));
      }
      if (cur_type_idx != internal::ScalarTypeId<T>()) {
        next_start_id = std::min(next_start_id, cur);
        continue;
      }
      if constexpr (std::is_same_v<T, arolla::Unit>) {
        typed_bldr.InsertIfNotSet(cur, arolla::Unit());
      } else if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
        if (input_values.empty()) {
          return absl::InvalidArgumentError(absl::StrCat(
              "DataSliceCompactProto has not enough values for type ",
              arolla::GetQType<T>()->name()));
        }
        ASSIGN_OR_RETURN(arolla::expr::ExprQuote q,
                         input_values[0].As<arolla::expr::ExprQuote>());
        input_values = input_values.subspan(1);
        typed_bldr.InsertIfNotSet(cur, std::move(q));
      } else {
        while (last_id >= part_offset + part_size) {
          RETURN_IF_ERROR(next_part());
        }
        if constexpr (std::is_same_v<T, internal::ObjectId>) {
          auto object_id = internal::ObjectId::UnsafeCreateFromInternalHighLow(
              values->hi(last_id - part_offset) +
                  last_object_id.InternalHigh64(),
              values->lo(last_id - part_offset) +
                  last_object_id.InternalLow64());
          last_object_id = object_id;
          typed_bldr.InsertIfNotSet(cur, object_id);
          bldr.GetMutableAllocationIds().Insert(
              internal::AllocationId(object_id));
        } else if constexpr (std::is_same_v<T, schema::DType>) {
          ASSIGN_OR_RETURN(
              schema::DType dtype,
              schema::DType::FromId((*values)[last_id - part_offset]));
          typed_bldr.InsertIfNotSet(cur, dtype);
        } else {
          typed_bldr.InsertIfNotSet(
              cur, internal::DataItem::View<T>(arolla::view_type_t<T>(
                       (*values)[last_id - part_offset])));
        }
        ++last_id;
      }
    }
    while (part_it != extra_parts.end()) {
      RETURN_IF_ERROR(next_part());
    }
    if (last_id != part_offset + part_size ||
        (std::is_same_v<T, arolla::expr::ExprQuote> && !input_values.empty())) {
      return absl::InvalidArgumentError(
          absl::StrCat("DataSliceCompactProto has unused values for type ",
                       arolla::GetQType<T>()->name()));
    }
    return next_start_id;
  };

  for (size_t i = 0; i < proto_types_buffer.size(); ++i) {
    uint8_t type_idx = static_cast<uint8_t>(proto_types_buffer[i]);
    if (type_idx == internal::TypesBuffer::kUnset) {
      continue;
    }
    if (type_idx == internal::TypesBuffer::kRemoved) {
      bldr.InsertIfNotSet(i, std::nullopt);
      continue;
    }
    if (!is_valid_present_type_idx(type_idx)) {
      return absl::InvalidArgumentError(
          absl::StrCat("invalid type index: ", type_idx));
    }
    uint64_t msk = uint64_t{1} << type_idx;
    if (processed_types & msk) {
      continue;
    }
    processed_types |= msk;

    absl::Status status = absl::OkStatus();

    arolla::meta::foreach_type<internal::supported_types_list>(
        [&](auto type_meta) {
          using T = typename decltype(type_meta)::type;
          if (type_idx != internal::ScalarTypeId<T>()) {
            return;
          }
          absl::StatusOr<size_t> next_i =
              process_type(std::type_identity<T>(), i);
          if (!next_i.ok()) {
            status = next_i.status();
            return;
          }
          i = *next_i - 1;
        });
    RETURN_IF_ERROR(std::move(status));
  }
  {
    absl::Status status = absl::OkStatus();
    arolla::meta::foreach_type<internal::supported_types_list>(
        [&](auto type_meta) {
          using T = typename decltype(type_meta)::type;
          auto type_idx = internal::ScalarTypeId<T>();
          if (processed_types & (uint64_t{1} << type_idx)) {
            return;
          }
          const auto& values =
              GetValuesFromDataSliceCompactProto<T>(slice_proto);
          absl::StatusOr<size_t> values_size =
              get_values_size(std::type_identity<T>(), values);
          if (!values_size.ok()) {
            status = std::move(values_size).status();
            return;
          }
          bool all_values_used = *values_size == 0;
          if constexpr (std::is_same_v<T, arolla::expr::ExprQuote>) {
            all_values_used = input_values.empty();
          }
          if (!all_values_used) {
            status = absl::InvalidArgumentError(absl::StrCat(
                "DataSliceCompactProto has unused values for type ",
                arolla::GetQType<T>()->name()));
          }
        });
    RETURN_IF_ERROR(std::move(status));
  }

  if (!input_values.empty()) {
    return absl::InvalidArgumentError("got more input_values than expected");
  }
  return TypedValue::FromValue(std::move(bldr).Build());
}

absl::StatusOr<ValueDecoderResult> DecodeDataSliceImplValue(
    const KodaV1Proto::DataSliceImplProto& slice_proto,
    absl::Span<const TypedValue> input_values) {
  switch (slice_proto.value_case()) {
    case KodaV1Proto::DataSliceImplProto::kDataItemVector: {
      return DecodeDataItemVectorProto(slice_proto.data_item_vector(),
                                       input_values);
    }
    case KodaV1Proto::DataSliceImplProto::kDataSliceCompact: {
      return DecodeDataSliceCompactProto(slice_proto.data_slice_compact(),
                                         input_values);
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

absl::StatusOr<ValueDecoderResult> DecodeJaggedShapeValue(
    absl::Span<const TypedValue> input_values) {
  if (input_values.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("wrong number of input_values in DecodeJaggedShapeValue: "
                     "expected 1, got ",
                     input_values.size()));
  }
  const TypedValue& base_value = input_values[0];
  if (base_value.GetType() !=
      arolla::GetQType<arolla::JaggedDenseArrayShape>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a JaggedDenseArrayShape, got %s", base_value.Repr()));
  }
  return arolla::UnsafeDowncastDerivedQValue(GetJaggedShapeQType(), base_value);
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
      if (alloc.Capacity() < values.size()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "AttrChunkProto values don't fit into AllocationId of "
            "`first_object_id`: values size is %d, alloc capacity is %d",
            values.size(), alloc.Capacity()));
      }
      internal::DataSliceImpl objects;
      if (values.types_buffer().id_to_typeidx.empty()) {
        objects = internal::DataSliceImpl::ObjectsFromAllocation(alloc,
                                                                 values.size());
      } else {
        auto bitmap = values.types_buffer().ToSetBitmap();
        arolla::Buffer<internal::ObjectId>::Builder objects_builder(
            values.size());
        size_t id = 0;
        arolla::bitmap::Iterate(bitmap, 0, values.size(), [&](bool present) {
          if (present) {
            objects_builder.Set(id, alloc.ObjectByOffset(id));
          }
          ++id;
        });

        objects = internal::DataSliceImpl::CreateWithAllocIds(
            internal::AllocationIdSet(alloc),
            internal::ObjectIdArray{std::move(objects_builder).Build(),
                                    std::move(bitmap)});
      }
      if (alloc.IsSchemasAlloc() &&
          attr_proto.name() != schema::kSchemaNameAttr) {
        RETURN_IF_ERROR(db.SetSchemaAttr(objects, attr_proto.name(), values));
      } else {
        RETURN_IF_ERROR(db.SetAttr(objects, attr_proto.name(), values));
      }
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
    if (dict_id.IsSmallAlloc()) {
      internal::DataItem schema(dict_id);
      for (int i = 0; i < keys.size(); ++i) {
        if (!keys[i].holds_value<arolla::Text>()) {
          return absl::InvalidArgumentError("schema key must be arolla::Text");
        }
        RETURN_IF_ERROR(
            db.SetSchemaAttr(schema, keys[i].value<arolla::Text>(), values[i]));
      }
    }
    return absl::OkStatus();
  } else {
    return db.SetInDict(internal::DataSliceImpl::Create(
                            keys.size(), internal::DataItem(dict_id)),
                        keys, values);
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
        DataBag::ImmutableEmptyWithFallbacks(fallbacks));
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
    case KodaV1Proto::kJaggedShapeValue:
      return DecodeJaggedShapeValue(input_values);
    case KodaV1Proto::kJaggedShapeQtype:
      return TypedValue::FromValue(GetJaggedShapeQType());
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
    case KodaV1Proto::kNonDeterministicTokenQtype:
      return TypedValue::FromValue(
          arolla::GetQType<internal::NonDeterministicToken>());
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
