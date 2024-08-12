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
#include "koladata/casting.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/align.h"
#include "koladata/operators/allocation.h"
#include "koladata/operators/assertion.h"
#include "koladata/operators/clone.h"
#include "koladata/operators/collapse.h"
#include "koladata/operators/comparison.h"
#include "koladata/operators/convert_and_eval.h"
#include "koladata/operators/dict.h"
#include "koladata/operators/equal.h"
#include "koladata/operators/explode.h"
#include "koladata/operators/extract.h"
#include "koladata/operators/get_attr.h"
#include "koladata/operators/group_by.h"
#include "koladata/operators/itemid.h"
#include "koladata/operators/jagged_shape.h"
#include "koladata/operators/list.h"
#include "koladata/operators/logical.h"
#include "koladata/operators/math.h"
#include "koladata/operators/nofollow.h"
#include "koladata/operators/reverse.h"
#include "koladata/operators/schema.h"
#include "koladata/operators/select.h"
#include "koladata/operators/shallow_clone.h"
#include "koladata/operators/slice.h"
#include "koladata/operators/translate.h"
#include "koladata/operators/uu_schema.h"
#include "koladata/operators/uuid.h"
#include "koladata/operators/uuobj.h"
#include "arolla/qexpr/optools.h"

namespace koladata::ops {
namespace {

#define OPERATOR AROLLA_REGISTER_QEXPR_OPERATOR
#define OPERATOR_FAMILY AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY

// go/keep-sorted start ignore_prefixes=OPERATOR,OPERATOR_FAMILY
OPERATOR("kde.allocation.new_dictid_shaped", NewDictIdShaped);
OPERATOR("kde.allocation.new_itemid_shaped", NewItemIdShaped);
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
OPERATOR("kde.core._clone_with_schema", CloneWithSchema);
OPERATOR("kde.core._collapse", Collapse);
OPERATOR("kde.core._explode", Explode);
OPERATOR("kde.core._extract", Extract);
OPERATOR("kde.core._extract_with_schema", ExtractWithSchema);
OPERATOR("kde.core._get_attr", GetAttr);
OPERATOR("kde.core._get_attr_with_default", GetAttrWithDefault);
OPERATOR("kde.core._select", Select);
OPERATOR("kde.core._shallow_clone", ShallowClone);
OPERATOR("kde.core._shallow_clone_with_schema", ShallowCloneWithSchema);
OPERATOR_FAMILY("kde.core._uuid", UuidOperatorFamily);
OPERATOR_FAMILY("kde.core._uuobj", UuObjOperatorFamily);
OPERATOR_FAMILY("kde.core.align", AlignOperatorFamily);
OPERATOR("kde.core.at", At);
OPERATOR("kde.core.dict_size", DictSize);
OPERATOR("kde.core.follow", Follow);
OPERATOR("kde.core.get_nofollowed_schema", GetNoFollowedSchema);
OPERATOR("kde.core.get_primitive_schema", GetPrimitiveSchema);
OPERATOR_FAMILY("kde.core.group_by_indices", GroupByIndicesFamily);
OPERATOR_FAMILY("kde.core.group_by_indices_sorted", GroupByIndicesSortedFamily);
OPERATOR("kde.core.itemid_bits", ItemIdBits);
OPERATOR("kde.core.list_size", ListSize);
OPERATOR("kde.core.nofollow", NoFollow);
OPERATOR("kde.core.nofollow_schema", CreateNoFollowSchema);
OPERATOR("kde.core.reverse", Reverse);
OPERATOR("kde.core.reverse_select", ReverseSelect);
OPERATOR_FAMILY("kde.core.subslice", SubsliceOperatorFamily);
OPERATOR("kde.core.translate", Translate);
OPERATOR("kde.core.unique", Unique);
//
OPERATOR("kde.logical._has_not", HasNot);
OPERATOR("kde.logical.apply_mask", ApplyMask);
OPERATOR("kde.logical.coalesce", Coalesce);
OPERATOR("kde.logical.has", Has);
//
OPERATOR("kde.math.maximum", Maximum);
OPERATOR("kde.math.minimum", Minimum);
OPERATOR("kde.math.multiply", Multiply);
OPERATOR("kde.math.subtract", Subtract);
//
OPERATOR("kde.schema._list_schema", ListSchema);
OPERATOR_FAMILY("kde.schema._new_schema", NewSchemaOperatorFamily);
OPERATOR_FAMILY("kde.schema._uu_schema", UuSchemaOperatorFamily);
OPERATOR("kde.schema.cast_to", CastTo);
OPERATOR("kde.schema.decode", Decode);
OPERATOR("kde.schema.encode", Encode);
OPERATOR("kde.schema.with_schema", WithSchema);
//
OPERATOR("kde.shapes._expand_to_shape", ExpandToShape);
OPERATOR("kde.shapes._reshape", Reshape);
OPERATOR_FAMILY("kde.shapes.create", JaggedShapeCreateOperatorFamily);
OPERATOR("kde.shapes.get_shape", GetShape);
//
OPERATOR_FAMILY("koda_internal.convert_and_eval", ConvertAndEvalFamily);
OPERATOR_FAMILY("koda_internal.convert_and_eval_with_shape",
                ConvertAndEvalWithShapeFamily);
OPERATOR("koda_internal.to_arolla_boolean", ToArollaBoolean);
OPERATOR("koda_internal.to_arolla_dense_array_int64", ToArollaDenseArrayInt64);
OPERATOR("koda_internal.to_arolla_dense_array_text", ToArollaDenseArrayText);
OPERATOR("koda_internal.to_arolla_dense_array_unit", ToArollaDenseArrayUnit);
OPERATOR("koda_internal.to_arolla_float64", ToArollaFloat64);
OPERATOR("koda_internal.to_arolla_int64", ToArollaInt64);
// go/keep-sorted end

}  // namespace
}  // namespace koladata::ops
