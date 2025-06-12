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
#include "koladata/jagged_shape_qtype.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"  // IWYU pragma: keep
#include "arolla/jagged_shape/qtype/qtype.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/testing/repr_token_eq.h"

namespace koladata {
namespace {

using ::arolla::CreateDenseArray;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::TypedValue;
using ::arolla::testing::ReprTokenEq;

TEST(JaggedShapeQTypeTest, JaggedShapeQType) {
  arolla::QTypePtr type = GetJaggedShapeQType();
  EXPECT_NE(type, nullptr);
  EXPECT_EQ(type->name(), "JAGGED_SHAPE");
  EXPECT_EQ(type->type_info(), typeid(arolla::JaggedDenseArrayShape));
  EXPECT_EQ(type->value_qtype(), nullptr);
  EXPECT_TRUE(type == GetJaggedShapeQType());
  EXPECT_FALSE(IsJaggedShapeQType(type));
}

TEST(JaggedShapeQTypeTest, TypedValueRepr) {
  {
    ASSERT_OK_AND_ASSIGN(auto edge, DenseArrayEdge::FromSplitPoints(
                                        CreateDenseArray<int64_t>({0, 2})));
    ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges({edge}));
    // TypedValue repr (-> python repr) works.
    ASSERT_OK_AND_ASSIGN(
        auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
    EXPECT_THAT(tv.GenReprToken(), ReprTokenEq("JaggedShape(2)"));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto shape, JaggedDenseArrayShape::FromEdges({}));
    ASSERT_OK_AND_ASSIGN(
        auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
    EXPECT_THAT(tv.GenReprToken(), ReprTokenEq("JaggedShape()"));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2})));
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2, 5})));
    ASSERT_OK_AND_ASSIGN(auto shape,
                         JaggedDenseArrayShape::FromEdges({edge1, edge2}));
    ASSERT_OK_AND_ASSIGN(
        auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
    EXPECT_THAT(tv.GenReprToken(), ReprTokenEq("JaggedShape(2, [2, 3])"));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2})));
    ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                         CreateDenseArray<int64_t>({0, 2, 5})));
    ASSERT_OK_AND_ASSIGN(auto edge3,
                         DenseArrayEdge::FromSplitPoints(
                             CreateDenseArray<int64_t>({0, 2, 4, 6, 9, 12})));
    ASSERT_OK_AND_ASSIGN(
        auto shape, JaggedDenseArrayShape::FromEdges({edge1, edge2, edge3}));
    ASSERT_OK_AND_ASSIGN(
        auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
    EXPECT_THAT(tv.GenReprToken(),
                ReprTokenEq("JaggedShape(2, [2, 3], [2, 2, 2, 3, 3])"));
  }
}

TEST(JaggedShapeQTypeTest, Fingerprint) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(auto shape1,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  ASSERT_OK_AND_ASSIGN(auto shape2,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  // Same shape -> same fingerprint.
  ASSERT_OK_AND_ASSIGN(
      auto tv1, TypedValue::FromValueWithQType(shape1, GetJaggedShapeQType()));
  ASSERT_OK_AND_ASSIGN(
      auto tv2, TypedValue::FromValueWithQType(shape2, GetJaggedShapeQType()));
  EXPECT_EQ(tv1.GetFingerprint(), tv2.GetFingerprint());
  // Fingerprints of JaggedDenseArrayShape and JaggedShape are different.
  auto tv3 = TypedValue::FromValue(shape1);
  EXPECT_NE(tv1.GetFingerprint(), tv3.GetFingerprint());
}

TEST(JaggedShapeQTypeTest, CopyTo) {
  ASSERT_OK_AND_ASSIGN(auto edge1, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 2})));
  ASSERT_OK_AND_ASSIGN(auto edge2, DenseArrayEdge::FromSplitPoints(
                                       CreateDenseArray<int64_t>({0, 1, 3})));
  ASSERT_OK_AND_ASSIGN(auto shape,
                       JaggedDenseArrayShape::FromEdges({edge1, edge2}));
  ASSERT_OK_AND_ASSIGN(
      auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
  auto tv_copy = TypedValue(tv.AsRef());  // Copies the raw (void) data.
  EXPECT_EQ(tv.GetFingerprint(), tv_copy.GetFingerprint());
}

}  // namespace
}  // namespace koladata
