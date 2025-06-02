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
#include <memory>
#include <string>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/optools.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/jagged_shape_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/allocation.h"
#include "koladata/operators/assertion.h"
#include "koladata/operators/bags.h"
#include "koladata/operators/comparison.h"
#include "koladata/operators/core.h"
#include "koladata/operators/dicts.h"
#include "koladata/operators/entities.h"
#include "koladata/operators/ids.h"
#include "koladata/operators/json.h"
#include "koladata/operators/lists.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/math.h"
#include "koladata/operators/non_deterministic_op.h"
#include "koladata/operators/objs.h"
#include "koladata/operators/predicates.h"
#include "koladata/operators/print.h"
#include "koladata/operators/proto.h"
#include "koladata/operators/schema.h"
#include "koladata/operators/shapes.h"
#include "koladata/operators/slices.h"
#include "koladata/operators/strings.h"

namespace koladata::ops {
namespace {

using ::koladata::internal::ReturnsOperatorEvalError;

template <typename Ret, typename... Args>
auto OperatorMacroImpl(absl::string_view name, Ret (*func)(Args...)) {
  return ReturnsOperatorEvalError(std::string(name), func);
}

template <typename Ret, typename... Args>
auto OperatorMacroImpl(absl::string_view name, Ret (*func)(Args...),
                       absl::string_view display_name) {
  DCHECK_NE(name, display_name) << "remove excessive display_name argument";
  return ReturnsOperatorEvalError(std::string(display_name), func);
}

#define OPERATOR(name, ...) \
  AROLLA_REGISTER_QEXPR_OPERATOR(name, OperatorMacroImpl(name, __VA_ARGS__))

// TODO: Support derived qtypes via automatic casting.
// Use for operators we need to provide explicit signatures for, due to, for
// example, the use of derived qtypes.
#define OPERATOR_WITH_SIGNATURE(name, signature, ...)                        \
  AROLLA_REGISTER_QEXPR_OPERATOR(name, OperatorMacroImpl(name, __VA_ARGS__), \
                                 signature)

#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY,OPERATOR_WITH_SIGNATURE NOLINT
OPERATOR("kd.allocation.new_dictid_like", NewDictIdLike);
OPERATOR_WITH_SIGNATURE(
    "kd.allocation.new_dictid_shaped",
    arolla::QExprOperatorSignature::Get(
        {GetJaggedShapeQType(),
         arolla::GetQType<internal::NonDeterministicToken>()},
        arolla::GetQType<DataSlice>()), NewDictIdShaped);
OPERATOR("kd.allocation.new_itemid_like", NewItemIdLike);
OPERATOR_WITH_SIGNATURE(
    "kd.allocation.new_itemid_shaped",
    arolla::QExprOperatorSignature::Get(
        {GetJaggedShapeQType(),
         arolla::GetQType<internal::NonDeterministicToken>()},
        arolla::GetQType<DataSlice>()), NewItemIdShaped);
OPERATOR("kd.allocation.new_listid_like", NewListIdLike);
OPERATOR_WITH_SIGNATURE(
    "kd.allocation.new_listid_shaped",
    arolla::QExprOperatorSignature::Get(
        {GetJaggedShapeQType(),
         arolla::GetQType<internal::NonDeterministicToken>()},
        arolla::GetQType<DataSlice>()), NewListIdShaped);
//
OPERATOR("kd.assertion.assert_present_scalar", AssertPresentScalar);
OPERATOR("kd.assertion.assert_primitive", AssertPrimitive);
//
OPERATOR_FAMILY("kd.bags.enriched",
                std::make_unique<EnrichedDbOperatorFamily>());
OPERATOR("kd.bags.new", Bag);
OPERATOR_FAMILY("kd.bags.updated",
                std::make_unique<UpdatedDbOperatorFamily>());
//
OPERATOR("kd.comparison.equal", Equal);
OPERATOR("kd.comparison.greater", Greater);
OPERATOR("kd.comparison.greater_equal", GreaterEqual);
OPERATOR("kd.comparison.less", Less);
OPERATOR("kd.comparison.less_equal", LessEqual);
//
OPERATOR("kd.core._clone", Clone, "kd.core.clone");
OPERATOR("kd.core._databag_freeze", Freeze<DataBagPtr>,
         "kd.core.databag_freeze");
OPERATOR("kd.core._deep_clone", DeepClone, "kd.core.deep_clone");
OPERATOR("kd.core._extract", Extract, "kd.core.extract");
OPERATOR("kd.core._get_attr", GetAttr, "kd.core.get_attr");
OPERATOR("kd.core._get_attr_with_default", GetAttrWithDefault,
         "kd.core.get_attr_with_default");
OPERATOR("kd.core._get_item", GetItem, "kd.core.get_item");
OPERATOR("kd.core._get_list_item_by_range", GetListItemByRange,
         "kd.core.get_list_item_by_range");
OPERATOR("kd.core._new_ids_like", NewIdsLike, "kd.core.new_ids_like");
OPERATOR("kd.core._shallow_clone", ShallowClone, "kd.core.shallow_clone");
OPERATOR("kd.core.attr", Attr);
OPERATOR_FAMILY("kd.core.attrs", std::make_unique<AttrsOperatorFamily>());
OPERATOR_FAMILY("kd.core.enriched",
                std::make_unique<EnrichedOperatorFamily>());
OPERATOR("kd.core.follow", Follow);
OPERATOR("kd.core.freeze_bag", Freeze<DataSlice>);
OPERATOR("kd.core.get_attr_names", GetAttrNames);
OPERATOR("kd.core.get_bag", GetBag);
OPERATOR("kd.core.get_metadata", GetMetadata);
OPERATOR("kd.core.has_attr", HasAttr);
OPERATOR("kd.core.has_entity", HasEntity);
OPERATOR("kd.core.has_primitive", HasPrimitive);
OPERATOR("kd.core.is_entity", IsEntity);
OPERATOR("kd.core.is_primitive", IsPrimitive);
OPERATOR("kd.core.no_bag", NoBag);
OPERATOR("kd.core.nofollow", NoFollow);
OPERATOR("kd.core.ref", Ref);
OPERATOR("kd.core.stub", Stub);
OPERATOR_FAMILY("kd.core.updated", std::make_unique<UpdatedOperatorFamily>());
OPERATOR("kd.core.with_attr", WithAttr);
OPERATOR_FAMILY("kd.core.with_attrs",
                std::make_unique<WithAttrsOperatorFamily>());
OPERATOR("kd.core.with_bag", WithBag);
OPERATOR("kd.core.with_merged_bag", WithMergedBag);
OPERATOR_FAMILY("kd.core.with_print",
                std::make_unique<WithPrintOperatorFamily>());
//
OPERATOR("kd.dicts._dict_update", DictUpdate, "kd.dicts.dict_update");
OPERATOR("kd.dicts._get_values", GetValues, "kd.dicts.get_values");
OPERATOR("kd.dicts._get_values_by_keys", GetValuesByKeys,
         "kd.dicts.get_values_by_keys");
OPERATOR("kd.dicts._like", DictLike, "kd.dicts.like");
OPERATOR_WITH_SIGNATURE(
    "kd.dicts._shaped",
    arolla::QExprOperatorSignature::Get(
        {GetJaggedShapeQType(), arolla::GetQType<DataSlice>(),
         arolla::GetQType<DataSlice>(), arolla::GetQType<DataSlice>(),
         arolla::GetQType<DataSlice>(), arolla::GetQType<DataSlice>(),
         arolla::GetQType<DataSlice>(),
         arolla::GetQType<internal::NonDeterministicToken>()},
        arolla::GetQType<DataSlice>()),
    DictShaped, "kd.dicts.shaped");
OPERATOR("kd.dicts.get_keys", GetKeys);
OPERATOR("kd.dicts.has_dict", HasDict);
OPERATOR("kd.dicts.is_dict", IsDict);
OPERATOR("kd.dicts.size", DictSize);
//
OPERATOR_FAMILY("kd.entities._like",
                std::make_unique<NewLikeOperatorFamily>());
OPERATOR_FAMILY("kd.entities._new", std::make_unique<NewOperatorFamily>());
OPERATOR_FAMILY("kd.entities._shaped",
                std::make_unique<NewShapedOperatorFamily>());
OPERATOR_FAMILY("kd.entities._uu", std::make_unique<UuOperatorFamily>());
//
OPERATOR("kd.ids._agg_uuid", AggUuid, "kd.ids.agg_uuid");
OPERATOR("kd.ids._deep_uuid", DeepUuid, "kd.ids.deep_uuid");
OPERATOR("kd.ids.decode_itemid", DecodeItemId);
OPERATOR("kd.ids.encode_itemid", EncodeItemId);
OPERATOR_FAMILY("kd.ids.uuid", std::make_unique<UuidOperatorFamily>());
OPERATOR_FAMILY("kd.ids.uuid_for_dict",
                std::make_unique<UuidForDictOperatorFamily>());
OPERATOR_FAMILY("kd.ids.uuid_for_list",
                std::make_unique<UuidForListOperatorFamily>());
OPERATOR("kd.ids.uuids_with_allocation_size", UuidsWithAllocationSize);
//
OPERATOR("kd.json.from_json", FromJson);
OPERATOR("kd.json.to_json", ToJson);
//
OPERATOR_FAMILY("kd.lists._concat",
                arolla::MakeVariadicInputOperatorFamily(ConcatLists));
OPERATOR("kd.lists._explode", Explode, "kd.lists.explode");
OPERATOR("kd.lists._implode", Implode, "kd.lists.implode");
OPERATOR("kd.lists._like", ListLike, "kd.lists.like");
OPERATOR_WITH_SIGNATURE(
    "kd.lists._shaped",
    arolla::QExprOperatorSignature::Get(
        {GetJaggedShapeQType(), arolla::GetQType<DataSlice>(),
         arolla::GetQType<DataSlice>(), arolla::GetQType<DataSlice>(),
         arolla::GetQType<DataSlice>(),
         arolla::GetQType<internal::NonDeterministicToken>()},
        arolla::GetQType<DataSlice>()),
    ListShaped, "kd.lists.shaped");
OPERATOR("kd.lists.appended_list", ListAppended);
OPERATOR("kd.lists.has_list", HasList);
OPERATOR("kd.lists.is_list", IsList);
OPERATOR("kd.lists.list_append_update", ListAppendUpdate);
OPERATOR("kd.lists.size", ListSize);
//
OPERATOR("kd.masking._agg_all", AggAll, "kd.masking.agg_all");
OPERATOR("kd.masking._agg_any", AggAny, "kd.masking.agg_any");
OPERATOR("kd.masking._has_not", HasNot, "kd.masking.has_not");
OPERATOR("kd.masking.apply_mask", ApplyMask);
OPERATOR("kd.masking.coalesce", Coalesce);
OPERATOR("kd.masking.disjoint_coalesce", DisjointCoalesce);
OPERATOR("kd.masking.has", Has);
//
OPERATOR("kd.math._agg_inverse_cdf", AggInverseCdf, "kd.math.agg_inverse_cdf");
OPERATOR("kd.math._agg_max", AggMax, "kd.math.agg_max");
OPERATOR("kd.math._agg_mean", AggMean, "kd.math.agg_mean");
OPERATOR("kd.math._agg_median", AggMedian, "kd.math.agg_median");
OPERATOR("kd.math._agg_min", AggMin, "kd.math.agg_min");
OPERATOR("kd.math._agg_std", AggStd, "kd.math.agg_std");
OPERATOR("kd.math._agg_sum", AggSum, "kd.math.agg_sum");
OPERATOR("kd.math._agg_var", AggVar, "kd.math.agg_var");
OPERATOR("kd.math._argmax", Argmax, "kd.math.argmax");
OPERATOR("kd.math._argmin", Argmin, "kd.math.argmin");
OPERATOR("kd.math._cdf", Cdf, "kd.math.cdf");
OPERATOR("kd.math._cum_max", CumMax, "kd.math.cum_max");
OPERATOR("kd.math._cum_min", CumMin, "kd.math.cum_min");
OPERATOR("kd.math._cum_sum", CumSum, "kd.math.cum_sum");
OPERATOR("kd.math._softmax", Softmax, "kd.math.softmax");
OPERATOR("kd.math.abs", Abs);
OPERATOR("kd.math.add", Add);
OPERATOR("kd.math.ceil", Ceil);
OPERATOR("kd.math.divide", Divide);
OPERATOR("kd.math.exp", Exp);
OPERATOR("kd.math.floor", Floor);
OPERATOR("kd.math.floordiv", FloorDiv);
OPERATOR("kd.math.is_nan", IsNaN);
OPERATOR("kd.math.log", Log);
OPERATOR("kd.math.log10", Log10);
OPERATOR("kd.math.maximum", Maximum);
OPERATOR("kd.math.minimum", Minimum);
OPERATOR("kd.math.mod", Mod);
OPERATOR("kd.math.multiply", Multiply);
OPERATOR("kd.math.neg", Neg);
OPERATOR("kd.math.pos", Pos);
OPERATOR("kd.math.pow", Pow);
OPERATOR("kd.math.round", Round);
OPERATOR("kd.math.sigmoid", Sigmoid);
OPERATOR("kd.math.sign", Sign);
OPERATOR("kd.math.subtract", Subtract);
//
OPERATOR_FAMILY("kd.objs.like", std::make_unique<ObjLikeOperatorFamily>());
OPERATOR_FAMILY("kd.objs.new", std::make_unique<ObjOperatorFamily>());
OPERATOR_FAMILY("kd.objs.shaped", std::make_unique<ObjShapedOperatorFamily>());
OPERATOR_FAMILY("kd.objs.uu", std::make_unique<UuObjOperatorFamily>());
//
OPERATOR("kd.proto._from_proto_bytes", FromProtoBytes);
OPERATOR("kd.proto._from_proto_json", FromProtoJson);
OPERATOR("kd.proto.schema_from_proto_path", SchemaFromProtoPath);
OPERATOR("kd.proto.to_proto_bytes", ToProtoBytes);
OPERATOR("kd.proto.to_proto_json", ToProtoJson);
//
OPERATOR("kd.schema._agg_common_schema", AggCommonSchema,
         "kd.schema.agg_common_schema");
OPERATOR("kd.schema._internal_maybe_named_schema", InternalMaybeNamedSchema,
         // Don't pass the display name, because it's confusing.
         // TODO: b/374841918 - Use the outer lambda's name instead.
         "");
OPERATOR("kd.schema._unsafe_cast_to", UnsafeCastTo, "kd.schema.unsafe_cast_to");
OPERATOR("kd.schema.cast_to", CastTo);
OPERATOR("kd.schema.cast_to_implicit", CastToImplicit);
OPERATOR("kd.schema.cast_to_narrow", CastToNarrow);
OPERATOR("kd.schema.dict_schema", DictSchema);
OPERATOR("kd.schema.get_item_schema", GetItemSchema);
OPERATOR("kd.schema.get_key_schema", GetKeySchema);
OPERATOR("kd.schema.get_nofollowed_schema", GetNoFollowedSchema);
OPERATOR("kd.schema.get_obj_schema", GetObjSchema);
OPERATOR("kd.schema.get_primitive_schema", GetPrimitiveSchema);
OPERATOR("kd.schema.get_repr", GetSchemaRepr);
OPERATOR("kd.schema.get_schema", GetSchema);
OPERATOR("kd.schema.get_value_schema", GetValueSchema);
OPERATOR("kd.schema.is_dict_schema", IsDictSchema);
OPERATOR("kd.schema.is_entity_schema", IsEntitySchema);
OPERATOR("kd.schema.is_itemid_schema", IsItemIdSchema);
OPERATOR("kd.schema.is_list_schema", IsListSchema);
OPERATOR("kd.schema.is_primitive_schema", IsPrimitiveSchema);
OPERATOR("kd.schema.is_struct_schema", IsStructSchema);
OPERATOR("kd.schema.list_schema", ListSchema);
OPERATOR_FAMILY("kd.schema.named_schema",
                std::make_unique<NamedSchemaOperatorFamily>());
OPERATOR_FAMILY("kd.schema.new_schema",
                std::make_unique<NewSchemaOperatorFamily>());
OPERATOR("kd.schema.nofollow_schema", CreateNoFollowSchema);
OPERATOR_FAMILY("kd.schema.uu_schema",
                std::make_unique<UuSchemaOperatorFamily>());
OPERATOR("kd.schema.with_schema", WithSchema);
//
OPERATOR("kd.shapes._expand_to_shape", ExpandToShape,
         "kd.shapes.expand_to_shape");
OPERATOR_FAMILY("kd.shapes._new_with_size",
                std::make_unique<JaggedShapeCreateWithSizeOperatorFamily>());
OPERATOR_WITH_SIGNATURE(
    "kd.shapes._reshape",
    arolla::QExprOperatorSignature::Get({arolla::GetQType<DataSlice>(),
                                         GetJaggedShapeQType()},
                                        arolla::GetQType<DataSlice>()),
    Reshape, "kd.shapes.reshape");
OPERATOR_WITH_SIGNATURE(
    "kd.shapes.get_shape",
    arolla::QExprOperatorSignature::Get({arolla::GetQType<DataSlice>()},
                                        GetJaggedShapeQType()),
    GetShape);
OPERATOR_FAMILY("kd.shapes.new",
                std::make_unique<JaggedShapeCreateOperatorFamily>());
//
OPERATOR("kd.slices._collapse", Collapse, "kd.slices.collapse");
OPERATOR_FAMILY(
    "kd.slices._concat_or_stack",
    arolla::MakeVariadicInputOperatorFamily(
        // TODO: b/374841918 - The operator is used as a building
        // block for several lambdas, so we cannot choose one public
        // name for its errors. We are still using ReturnsOperatorEvalError in
        // order to turn the errors into KodaError, but the operator name should
        // be attached using a different mechanism.
        ReturnsOperatorEvalError("", ConcatOrStack)));
OPERATOR("kd.slices._dense_rank", DenseRank, "kd.slices.dense_rank");
OPERATOR_FAMILY("kd.slices._group_by_indices",
                arolla::MakeVariadicInputOperatorFamily(
                    ReturnsOperatorEvalError("kd.slices.group_by_indices",
                                             GroupByIndices)));
OPERATOR("kd.slices._inverse_mapping", InverseMapping,
         "kd.slices.inverse_mapping");
OPERATOR("kd.slices._ordinal_rank", OrdinalRank, "kd.slices.ordinal_rank");
OPERATOR_FAMILY("kd.slices.align", std::make_unique<AlignOperatorFamily>());
OPERATOR_WITH_SIGNATURE(
    "kd.slices.empty_shaped",
    arolla::QExprOperatorSignature::Get({GetJaggedShapeQType(),
                                         arolla::GetQType<DataSlice>()},
                                        arolla::GetQType<DataSlice>()),
    EmptyShaped);
OPERATOR("kd.slices.get_repr", GetRepr);
OPERATOR("kd.slices.internal_select_by_slice", Select, "kd.slices.select");
OPERATOR("kd.slices.inverse_select", InverseSelect);
OPERATOR("kd.slices.is_empty", IsEmpty);
OPERATOR("kd.slices.reverse", Reverse);
OPERATOR_FAMILY("kd.slices.subslice",
                std::make_unique<SubsliceOperatorFamily>());
OPERATOR("kd.slices.take", Take);
OPERATOR("kd.slices.translate", Translate);
OPERATOR("kd.slices.unique", Unique);
//
OPERATOR("kd.strings._agg_join", AggJoin, "kd.strings.agg_join");
OPERATOR("kd.strings._decode_base64", DecodeBase64, "kd.strings.decode_base64");
OPERATOR_FAMILY(
    "kd.strings._test_only_format_wrapper",
    arolla::MakeVariadicInputOperatorFamily(ReturnsOperatorEvalError(
        "kd.strings._test_only_format_wrapper", TestOnlyFormatWrapper)));
OPERATOR("kd.strings.contains", Contains);
OPERATOR("kd.strings.count", Count);
OPERATOR("kd.strings.decode", Decode);
OPERATOR("kd.strings.encode", Encode);
OPERATOR("kd.strings.encode_base64", EncodeBase64);
OPERATOR("kd.strings.find", Find);
OPERATOR_FAMILY("kd.strings.format", std::make_unique<FormatOperatorFamily>());
OPERATOR_FAMILY("kd.strings.join",
                arolla::MakeVariadicInputOperatorFamily(
                    ReturnsOperatorEvalError("kd.strings.join", Join)));
OPERATOR("kd.strings.length", Length);
OPERATOR("kd.strings.lower", Lower);
OPERATOR("kd.strings.lstrip", Lstrip);
OPERATOR_FAMILY("kd.strings.printf",
                arolla::MakeVariadicInputOperatorFamily(
                    ReturnsOperatorEvalError("kd.strings.printf", Printf)));
OPERATOR("kd.strings.regex_extract", RegexExtract);
OPERATOR("kd.strings.regex_match", RegexMatch);
OPERATOR("kd.strings.replace", Replace);
OPERATOR("kd.strings.rfind", Rfind);
OPERATOR("kd.strings.rstrip", Rstrip);
OPERATOR("kd.strings.split", Split);
OPERATOR("kd.strings.strip", Strip);
OPERATOR("kd.strings.substr", Substr);
OPERATOR("kd.strings.upper", Upper);
//
OPERATOR("koda_internal.create_metadata", CreateMetadata);
OPERATOR("koda_internal.non_deterministic", NonDeterministicOp);
OPERATOR_FAMILY("koda_internal.non_deterministic_identity",
                std::make_unique<NonDeterministicIdentityOpFamily>());
OPERATOR("koda_internal.to_arolla_boolean", ToArollaScalar<bool>);
OPERATOR("koda_internal.to_arolla_dense_array_int64",
         ToArollaDenseArray<int64_t>);
OPERATOR("koda_internal.to_arolla_dense_array_text",
         ToArollaDenseArray<arolla::Text>);
OPERATOR("koda_internal.to_arolla_dense_array_unit",
         ToArollaDenseArray<arolla::Unit>);
OPERATOR("koda_internal.to_arolla_float64", ToArollaScalar<double>);
OPERATOR("koda_internal.to_arolla_int64", ToArollaScalar<int64_t>);
OPERATOR("koda_internal.to_arolla_optional_unit",
         ToArollaOptionalScalar<arolla::Unit>);
OPERATOR("koda_internal.to_arolla_text", ToArollaScalar<arolla::Text>);
// go/keep-sorted end

}  // namespace
}  // namespace koladata::ops
