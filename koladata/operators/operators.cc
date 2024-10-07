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
#include "koladata/data_slice_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/allocation.h"
#include "koladata/operators/assertion.h"
#include "koladata/operators/comparison.h"
#include "koladata/operators/core.h"
#include "koladata/operators/logical.h"
#include "koladata/operators/math.h"
#include "koladata/operators/predicates.h"
#include "koladata/operators/schema.h"
#include "koladata/operators/shapes.h"
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
OPERATOR("kde.comparison.equal", Equal);
OPERATOR("kde.comparison.greater", Greater);
OPERATOR("kde.comparison.greater_equal", GreaterEqual);
OPERATOR("kde.comparison.less", Less);
OPERATOR("kde.comparison.less_equal", LessEqual);
//
OPERATOR("kde.core._clone", Clone);
OPERATOR("kde.core._collapse", Collapse);
OPERATOR_FAMILY("kde.core._concat_or_stack",
                arolla::MakeVariadicInputOperatorFamily(ConcatOrStack));
OPERATOR("kde.core._deep_clone", DeepClone);
OPERATOR("kde.core._dense_rank", DenseRank);
OPERATOR("kde.core._dict_update", DictUpdate);
OPERATOR("kde.core._explode", Explode);
OPERATOR("kde.core._extract", Extract);
OPERATOR("kde.core._get_attr", GetAttr);
OPERATOR("kde.core._get_attr_with_default", GetAttrWithDefault);
OPERATOR("kde.core._get_item", GetItem);
OPERATOR("kde.core._get_list_item_by_range", GetListItemByRange);
OPERATOR("kde.core._get_values", GetValues);
OPERATOR("kde.core._get_values_by_keys", GetValuesByKeys);
OPERATOR("kde.core._inverse_mapping", InverseMapping);
OPERATOR("kde.core._ordinal_rank", OrdinalRank);
OPERATOR("kde.core._select", Select);
OPERATOR("kde.core._shallow_clone", ShallowClone);
OPERATOR("kde.core.add", Add);
OPERATOR_FAMILY("kde.core.align", std::make_unique<AlignOperatorFamily>());
OPERATOR("kde.core.at", At);
OPERATOR_FAMILY("kde.core.attrs", std::make_unique<AttrsOperatorFamily>());
OPERATOR("kde.core.dict_size", DictSize);
OPERATOR_FAMILY("kde.core.enriched",
                std::make_unique<EnrichedOperatorFamily>());
OPERATOR("kde.core.follow", Follow);
OPERATOR("kde.core.get_db", GetDb);
OPERATOR("kde.core.get_keys", GetKeys);
OPERATOR("kde.core.get_nofollowed_schema", GetNoFollowedSchema);
OPERATOR_FAMILY("kde.core.group_by_indices",
                arolla::MakeVariadicInputOperatorFamily(GroupByIndices));
OPERATOR_FAMILY("kde.core.group_by_indices_sorted",
                arolla::MakeVariadicInputOperatorFamily(GroupByIndicesSorted));
OPERATOR("kde.core.is_dict", IsDict);
OPERATOR("kde.core.is_empty", IsEmpty);
OPERATOR("kde.core.is_list", IsList);
OPERATOR("kde.core.is_primitive", IsPrimitive);
OPERATOR("kde.core.itemid_bits", ItemIdBits);
OPERATOR("kde.core.list_size", ListSize);
OPERATOR_FAMILY("kde.core.new_shaped",
                std::make_unique<NewShapedOperatorFamily>());
OPERATOR("kde.core.no_db", NoDb);
OPERATOR("kde.core.nofollow", NoFollow);
OPERATOR("kde.core.nofollow_schema", CreateNoFollowSchema);
OPERATOR("kde.core.ref", Ref);
OPERATOR("kde.core.reverse", Reverse);
OPERATOR("kde.core.reverse_select", ReverseSelect);
OPERATOR_FAMILY("kde.core.subslice",
                std::make_unique<SubsliceOperatorFamily>());
OPERATOR("kde.core.translate", Translate);
OPERATOR("kde.core.unique", Unique);
OPERATOR_FAMILY("kde.core.updated", std::make_unique<UpdatedOperatorFamily>());
OPERATOR_FAMILY("kde.core.uu", std::make_unique<UuOperatorFamily>());
OPERATOR_FAMILY("kde.core.uuid", std::make_unique<UuidOperatorFamily>());
OPERATOR_FAMILY("kde.core.uuobj", std::make_unique<UuObjOperatorFamily>());
OPERATOR_FAMILY("kde.core.with_attrs",
                std::make_unique<WithAttrsOperatorFamily>());
OPERATOR("kde.core.with_db", WithDb);
OPERATOR("kde.core.with_merged_bag", WithMergedBag);
//
OPERATOR("kde.logical._agg_all", AggAll);
OPERATOR("kde.logical._agg_any", AggAny);
OPERATOR("kde.logical._has_not", HasNot);
OPERATOR("kde.logical.apply_mask", ApplyMask);
OPERATOR("kde.logical.coalesce", Coalesce);
OPERATOR("kde.logical.has", Has);
//
OPERATOR("kde.math._agg_max", AggMax);
OPERATOR("kde.math._agg_mean", AggMean);
OPERATOR("kde.math._agg_median", AggMedian);
OPERATOR("kde.math._agg_min", AggMin);
OPERATOR("kde.math._agg_std", AggStd);
OPERATOR("kde.math._agg_sum", AggSum);
OPERATOR("kde.math._agg_var", AggVar);
OPERATOR("kde.math._cum_max", CumMax);
OPERATOR("kde.math._cum_min", CumMin);
OPERATOR("kde.math._cum_sum", CumSum);
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
OPERATOR("kde.math.pow", Pow);
OPERATOR("kde.math.round", Round);
OPERATOR("kde.math.subtract", Subtract);
//
OPERATOR_FAMILY("kde.schema._new_schema",
                std::make_unique<NewSchemaOperatorFamily>());
OPERATOR("kde.schema.cast_to", CastTo);
OPERATOR("kde.schema.cast_to_implicit", CastToImplicit);
OPERATOR("kde.schema.cast_to_narrow", CastToNarrow);
OPERATOR("kde.schema.decode", Decode);
OPERATOR("kde.schema.dict_schema", DictSchema);
OPERATOR("kde.schema.encode", Encode);
OPERATOR("kde.schema.get_item_schema", GetItemSchema);
OPERATOR("kde.schema.get_key_schema", GetKeySchema);
OPERATOR("kde.schema.get_obj_schema", GetObjSchema);
OPERATOR("kde.schema.get_primitive_schema", GetPrimitiveSchema);
OPERATOR("kde.schema.get_schema", GetSchema);
OPERATOR("kde.schema.get_value_schema", GetValueSchema);
OPERATOR("kde.schema.list_schema", ListSchema);
OPERATOR_FAMILY("kde.schema.uu_schema",
                std::make_unique<UuSchemaOperatorFamily>());
OPERATOR("kde.schema.with_schema", WithSchema);
//
OPERATOR("kde.shapes._expand_to_shape", ExpandToShape);
OPERATOR("kde.shapes._reshape", Reshape);
OPERATOR_FAMILY("kde.shapes.create",
                std::make_unique<JaggedShapeCreateOperatorFamily>());
OPERATOR("kde.shapes.get_shape", GetShape);
//
OPERATOR("kde.strings._agg_join", AggJoin);
OPERATOR_FAMILY("kde.strings._test_only_format_wrapper",
                arolla::MakeVariadicInputOperatorFamily(TestOnlyFormatWrapper));
OPERATOR("kde.strings.contains", Contains);
OPERATOR("kde.strings.count", Count);
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
