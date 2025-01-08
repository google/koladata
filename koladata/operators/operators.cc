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

#include "koladata/arolla_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/allocation.h"
#include "koladata/operators/assertion.h"
#include "koladata/operators/bags.h"
#include "koladata/operators/comparison.h"
#include "koladata/operators/core.h"
#include "koladata/operators/core_new.h"
#include "koladata/operators/core_obj.h"
#include "koladata/operators/dicts.h"
#include "koladata/operators/ids.h"
#include "koladata/operators/json.h"
#include "koladata/operators/lists.h"
#include "koladata/operators/masking.h"
#include "koladata/operators/math.h"
#include "koladata/operators/non_deterministic_op.h"
#include "koladata/operators/predicates.h"
#include "koladata/operators/schema.h"
#include "koladata/operators/shapes.h"
#include "koladata/operators/slices.h"
#include "koladata/operators/strings.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/optools.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::ops {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR("kde.allocation.new_dictid_like", NewDictIdLike);
OPERATOR("kde.allocation.new_dictid_shaped", NewDictIdShaped);
OPERATOR("kde.allocation.new_itemid_like", NewItemIdLike);
OPERATOR("kde.allocation.new_itemid_shaped", NewItemIdShaped);
OPERATOR("kde.allocation.new_listid_like", NewListIdLike);
OPERATOR("kde.allocation.new_listid_shaped", NewListIdShaped);
//
OPERATOR("kde.assertion.assert_ds_has_primitives_of", AssertDsHasPrimitivesOf);
//
OPERATOR_FAMILY("kde.bags.enriched",
                std::make_unique<EnrichedDbOperatorFamily>());
OPERATOR("kde.bags.new", Bag);
OPERATOR_FAMILY("kde.bags.updated",
                std::make_unique<UpdatedDbOperatorFamily>());
//
OPERATOR("kde.comparison.equal", Equal);
OPERATOR("kde.comparison.greater", Greater);
OPERATOR("kde.comparison.greater_equal", GreaterEqual);
OPERATOR("kde.comparison.less", Less);
OPERATOR("kde.comparison.less_equal", LessEqual);
//
OPERATOR("kde.core._clone", Clone);
OPERATOR("kde.core._databag_freeze", Freeze<DataBagPtr>);
OPERATOR("kde.core._deep_clone", DeepClone);
OPERATOR("kde.core._extract", Extract);
OPERATOR("kde.core._get_attr", GetAttr);
OPERATOR("kde.core._get_attr_with_default", GetAttrWithDefault);
OPERATOR("kde.core._get_item", GetItem);
OPERATOR("kde.core._get_list_item_by_range", GetListItemByRange);
OPERATOR_FAMILY("kde.core._new", std::make_unique<NewOperatorFamily>());
OPERATOR("kde.core._new_ids_like", NewIdsLike);
OPERATOR_FAMILY("kde.core._new_like",
                std::make_unique<NewLikeOperatorFamily>());
OPERATOR_FAMILY("kde.core._new_shaped",
                std::make_unique<NewShapedOperatorFamily>());
OPERATOR("kde.core._shallow_clone", ShallowClone);
OPERATOR_FAMILY("kde.core._uu", std::make_unique<UuOperatorFamily>());
OPERATOR("kde.core.add", Add);
OPERATOR("kde.core.attr", Attr);
OPERATOR_FAMILY("kde.core.attrs", std::make_unique<AttrsOperatorFamily>());
OPERATOR_FAMILY("kde.core.enriched",
                std::make_unique<EnrichedOperatorFamily>());
OPERATOR("kde.core.follow", Follow);
OPERATOR("kde.core.freeze_bag", Freeze<DataSlice>);
OPERATOR("kde.core.get_bag", GetBag);
OPERATOR("kde.core.has_entity", HasEntity);
OPERATOR("kde.core.has_primitive", HasPrimitive);
OPERATOR("kde.core.is_entity", IsEntity);
OPERATOR("kde.core.is_primitive", IsPrimitive);
OPERATOR("kde.core.no_bag", NoBag);
OPERATOR("kde.core.nofollow", NoFollow);
OPERATOR_FAMILY("kde.core.obj", std::make_unique<ObjOperatorFamily>());
OPERATOR_FAMILY("kde.core.obj_like", std::make_unique<ObjLikeOperatorFamily>());
OPERATOR_FAMILY("kde.core.obj_shaped",
                std::make_unique<ObjShapedOperatorFamily>());
OPERATOR("kde.core.ref", Ref);
OPERATOR("kde.core.stub", Stub);
OPERATOR_FAMILY("kde.core.updated", std::make_unique<UpdatedOperatorFamily>());
OPERATOR_FAMILY("kde.core.uuobj", std::make_unique<UuObjOperatorFamily>());
OPERATOR("kde.core.with_attr", WithAttr);
OPERATOR_FAMILY("kde.core.with_attrs",
                std::make_unique<WithAttrsOperatorFamily>());
OPERATOR("kde.core.with_bag", WithBag);
OPERATOR("kde.core.with_merged_bag", WithMergedBag);
//
OPERATOR("kde.dicts._dict_update", DictUpdate);
OPERATOR("kde.dicts._get_values", GetValues);
OPERATOR("kde.dicts._get_values_by_keys", GetValuesByKeys);
OPERATOR("kde.dicts._like", DictLike);
OPERATOR("kde.dicts._shaped", DictShaped);
OPERATOR("kde.dicts.get_keys", GetKeys);
OPERATOR("kde.dicts.has_dict", HasDict);
OPERATOR("kde.dicts.is_dict", IsDict);
OPERATOR("kde.dicts.size", DictSize);
//
OPERATOR("kde.ids._agg_uuid", AggUuid);
OPERATOR("kde.ids._deep_uuid", DeepUuid);
OPERATOR("kde.ids.decode_itemid", DecodeItemId);
OPERATOR("kde.ids.encode_itemid", EncodeItemId);
OPERATOR_FAMILY("kde.ids.uuid", std::make_unique<UuidOperatorFamily>());
OPERATOR_FAMILY("kde.ids.uuid_for_dict",
                std::make_unique<UuidForDictOperatorFamily>());
OPERATOR_FAMILY("kde.ids.uuid_for_list",
                std::make_unique<UuidForListOperatorFamily>());
OPERATOR("kde.ids.uuids_with_allocation_size", UuidsWithAllocationSize);
//
OPERATOR("kde.json.to_json", ToJson);
//
OPERATOR("kde.lists._explode", Explode);
OPERATOR("kde.lists._implode", Implode);
OPERATOR("kde.lists._like", ListLike);
OPERATOR("kde.lists._shaped", ListShaped);
OPERATOR("kde.lists.has_list", HasList);
OPERATOR("kde.lists.is_list", IsList);
OPERATOR("kde.lists.size", ListSize);
//
OPERATOR("kde.masking._agg_all", AggAll);
OPERATOR("kde.masking._agg_any", AggAny);
OPERATOR("kde.masking._has_not", HasNot);
OPERATOR("kde.masking.apply_mask", ApplyMask);
OPERATOR("kde.masking.coalesce", Coalesce);
OPERATOR("kde.masking.has", Has);
//
OPERATOR("kde.math._agg_inverse_cdf", AggInverseCdf);
OPERATOR("kde.math._agg_max", AggMax);
OPERATOR("kde.math._agg_mean", AggMean);
OPERATOR("kde.math._agg_median", AggMedian);
OPERATOR("kde.math._agg_min", AggMin);
OPERATOR("kde.math._agg_std", AggStd);
OPERATOR("kde.math._agg_sum", AggSum);
OPERATOR("kde.math._agg_var", AggVar);
OPERATOR("kde.math._cdf", Cdf);
OPERATOR("kde.math._cum_max", CumMax);
OPERATOR("kde.math._cum_min", CumMin);
OPERATOR("kde.math._cum_sum", CumSum);
OPERATOR("kde.math._softmax", Softmax);
OPERATOR("kde.math.abs", Abs);
OPERATOR("kde.math.ceil", Ceil);
OPERATOR("kde.math.divide", Divide);
OPERATOR("kde.math.exp", Exp);
OPERATOR("kde.math.floor", Floor);
OPERATOR("kde.math.floordiv", FloorDiv);
OPERATOR("kde.math.log", Log);
OPERATOR("kde.math.log10", Log10);
OPERATOR("kde.math.maximum", Maximum);
OPERATOR("kde.math.minimum", Minimum);
OPERATOR("kde.math.mod", Mod);
OPERATOR("kde.math.multiply", Multiply);
OPERATOR("kde.math.neg", Neg);
OPERATOR("kde.math.pos", Pos);
OPERATOR("kde.math.pow", Pow);
OPERATOR("kde.math.round", Round);
OPERATOR("kde.math.sigmoid", Sigmoid);
OPERATOR("kde.math.sign", Sign);
OPERATOR("kde.math.subtract", Subtract);
//
OPERATOR("kde.schema._agg_common_schema", AggCommonSchema);
OPERATOR("kde.schema._internal_maybe_named_schema", InternalMaybeNamedSchema);
OPERATOR("kde.schema._unsafe_cast_to", UnsafeCastTo);
OPERATOR("kde.schema.cast_to", CastTo);
OPERATOR("kde.schema.cast_to_implicit", CastToImplicit);
OPERATOR("kde.schema.cast_to_narrow", CastToNarrow);
OPERATOR("kde.schema.dict_schema", DictSchema);
OPERATOR("kde.schema.get_item_schema", GetItemSchema);
OPERATOR("kde.schema.get_key_schema", GetKeySchema);
OPERATOR("kde.schema.get_nofollowed_schema", GetNoFollowedSchema);
OPERATOR("kde.schema.get_obj_schema", GetObjSchema);
OPERATOR("kde.schema.get_primitive_schema", GetPrimitiveSchema);
OPERATOR("kde.schema.get_schema", GetSchema);
OPERATOR("kde.schema.get_value_schema", GetValueSchema);
OPERATOR("kde.schema.is_any_schema", IsAnySchema);
OPERATOR("kde.schema.is_dict_schema", IsDictSchema);
OPERATOR("kde.schema.is_entity_schema", IsEntitySchema);
OPERATOR("kde.schema.is_itemid_schema", IsItemIdSchema);
OPERATOR("kde.schema.is_list_schema", IsListSchema);
OPERATOR("kde.schema.is_primitive_schema", IsPrimitiveSchema);
OPERATOR("kde.schema.is_struct_schema", IsStructSchema);
OPERATOR("kde.schema.list_schema", ListSchema);
OPERATOR_FAMILY("kde.schema.named_schema",
                std::make_unique<NamedSchemaOperatorFamily>());
OPERATOR_FAMILY("kde.schema.new_schema",
                std::make_unique<NewSchemaOperatorFamily>());
OPERATOR("kde.schema.nofollow_schema", CreateNoFollowSchema);
OPERATOR_FAMILY("kde.schema.uu_schema",
                std::make_unique<UuSchemaOperatorFamily>());
OPERATOR("kde.schema.with_schema", WithSchema);
//
OPERATOR("kde.shapes._expand_to_shape", ExpandToShape);
OPERATOR_FAMILY("kde.shapes._new_with_size",
                std::make_unique<JaggedShapeCreateWithSizeOperatorFamily>());
OPERATOR("kde.shapes._reshape", Reshape);
OPERATOR("kde.shapes.get_shape", GetShape);
OPERATOR_FAMILY("kde.shapes.new",
                std::make_unique<JaggedShapeCreateOperatorFamily>());
//
OPERATOR("kde.slices._collapse", Collapse);
OPERATOR_FAMILY("kde.slices._concat_or_stack",
                arolla::MakeVariadicInputOperatorFamily(ConcatOrStack));
OPERATOR("kde.slices._dense_rank", DenseRank);
OPERATOR("kde.slices._inverse_mapping", InverseMapping);
OPERATOR("kde.slices._ordinal_rank", OrdinalRank);
OPERATOR("kde.slices._select", Select);
OPERATOR_FAMILY("kde.slices.align", std::make_unique<AlignOperatorFamily>());
OPERATOR_FAMILY("kde.slices.group_by_indices",
                arolla::MakeVariadicInputOperatorFamily(GroupByIndices));
OPERATOR_FAMILY("kde.slices.group_by_indices_sorted",
                arolla::MakeVariadicInputOperatorFamily(GroupByIndicesSorted));
OPERATOR("kde.slices.inverse_select", InverseSelect);
OPERATOR("kde.slices.is_empty", IsEmpty);
OPERATOR("kde.slices.reverse", Reverse);
OPERATOR_FAMILY("kde.slices.subslice",
                std::make_unique<SubsliceOperatorFamily>());
OPERATOR("kde.slices.take", Take);
OPERATOR("kde.slices.translate", Translate);
OPERATOR("kde.slices.unique", Unique);
//
OPERATOR("kde.strings._agg_join", AggJoin);
OPERATOR("kde.strings._decode_base64", DecodeBase64);
OPERATOR_FAMILY("kde.strings._test_only_format_wrapper",
                arolla::MakeVariadicInputOperatorFamily(TestOnlyFormatWrapper));
OPERATOR("kde.strings.contains", Contains);
OPERATOR("kde.strings.count", Count);
OPERATOR("kde.strings.decode", Decode);
OPERATOR("kde.strings.encode", Encode);
OPERATOR("kde.strings.encode_base64", EncodeBase64);
OPERATOR("kde.strings.find", Find);
OPERATOR_FAMILY("kde.strings.format", std::make_unique<FormatOperatorFamily>());
OPERATOR_FAMILY("kde.strings.join",
                arolla::MakeVariadicInputOperatorFamily(Join));
OPERATOR("kde.strings.length", Length);
OPERATOR("kde.strings.lower", Lower);
OPERATOR("kde.strings.lstrip", Lstrip);
OPERATOR_FAMILY("kde.strings.printf",
                arolla::MakeVariadicInputOperatorFamily(Printf));
OPERATOR("kde.strings.regex_extract", RegexExtract);
OPERATOR("kde.strings.regex_match", RegexMatch);
OPERATOR("kde.strings.replace", Replace);
OPERATOR("kde.strings.rfind", Rfind);
OPERATOR("kde.strings.rstrip", Rstrip);
OPERATOR("kde.strings.split", Split);
OPERATOR("kde.strings.strip", Strip);
OPERATOR("kde.strings.substr", Substr);
OPERATOR("kde.strings.upper", Upper);
//
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
// go/keep-sorted end

}  // namespace
}  // namespace koladata::ops
