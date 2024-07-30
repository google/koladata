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
#include "koladata/internal/casting.h"

#include <cstdint>
#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::schema {
namespace {

using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::testing::DataBagEqual;
using ::koladata::internal::testing::IsEquivalentTo;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::HasSubstr;

TEST(CastingTest, ToInt32_DataItem) {
  auto to_int32 = schema::ToInt32();
  EXPECT_THAT(to_int32(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_int32(DataItem(1)), IsOkAndHolds(IsEquivalentTo(DataItem(1))));
  EXPECT_THAT(to_int32(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(1))));
  EXPECT_THAT(to_int32(DataItem(1.5f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1))));
  EXPECT_THAT(to_int32(DataItem(1.5)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1))));
  EXPECT_THAT(to_int32(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1))));
  EXPECT_THAT(to_int32(DataItem(false)),
              IsOkAndHolds(IsEquivalentTo(DataItem(0))));
  EXPECT_THAT(to_int32(DataItem(int64_t{1ll << 38})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast int64{274877906944} to int32"));
  EXPECT_THAT(
      to_int32(DataItem(std::numeric_limits<float>::infinity())),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast inf to int32"));
  EXPECT_THAT(to_int32(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT32"));
}

TEST(CastingTest, ToInt32_DataSlice) {
  auto to_int32 = schema::ToInt32();
  EXPECT_THAT(to_int32(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(
      to_int32(DataSliceImpl::Create({DataItem(1), DataItem(2), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(1), DataItem(2), DataItem()}))));
  EXPECT_THAT(
      to_int32(DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()})),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::Create({DataItem(1), DataItem()}))));
  EXPECT_THAT(to_int32(DataSliceImpl::Create({DataItem(1.5f), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1), DataItem()}))));
  EXPECT_THAT(to_int32(DataSliceImpl::Create({DataItem(1.5), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1), DataItem()}))));
  EXPECT_THAT(to_int32(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1), DataItem()}))));
  EXPECT_THAT(
      to_int32(DataSliceImpl::Create({DataItem(1), DataItem(int64_t{2})})),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::Create({DataItem(1), DataItem(2)}))));
  EXPECT_THAT(to_int32(DataSliceImpl::Create(
                  {DataItem(int64_t{1ll << 38}), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast int64{274877906944} to int32"));
  EXPECT_THAT(
      to_int32(DataSliceImpl::Create(
          {DataItem(std::numeric_limits<float>::infinity()), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast inf to int32"));
  EXPECT_THAT(
      to_int32(DataSliceImpl::Create({DataItem(1), DataItem(arolla::kUnit)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to INT32"));
}

TEST(CastingTest, ToInt64_DataItem) {
  auto to_int64 = schema::ToInt64();
  EXPECT_THAT(to_int64(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_int64(DataItem(1)),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{1}))));
  EXPECT_THAT(to_int64(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{1}))));
  EXPECT_THAT(to_int64(DataItem(1.0f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{1}))));
  EXPECT_THAT(to_int64(DataItem(1.0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{1}))));
  EXPECT_THAT(to_int64(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{1}))));
  EXPECT_THAT(to_int64(DataItem(false)),
              IsOkAndHolds(IsEquivalentTo(DataItem(int64_t{0}))));
  EXPECT_THAT(
      to_int64(DataItem(std::numeric_limits<float>::infinity())),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast inf to int64"));
  EXPECT_THAT(to_int64(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT64"));
}

TEST(CastingTest, ToInt64_DataSlice) {
  auto to_int64 = schema::ToInt64();
  EXPECT_THAT(to_int64(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_int64(DataSliceImpl::Create({DataItem(1), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()}))));
  EXPECT_THAT(
      to_int64(DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()}))));
  EXPECT_THAT(to_int64(DataSliceImpl::Create({DataItem(1.0f), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()}))));
  EXPECT_THAT(to_int64(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()}))));
  EXPECT_THAT(to_int64(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()}))));
  EXPECT_THAT(
      to_int64(DataSliceImpl::Create({DataItem(1), DataItem(int64_t{2})})),
      IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
          {DataItem(int64_t{1}), DataItem(int64_t{2})}))));
  EXPECT_THAT(
      to_int64(DataSliceImpl::Create(
          {DataItem(std::numeric_limits<float>::infinity()), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast inf to int64"));
  EXPECT_THAT(
      to_int64(DataSliceImpl::Create({DataItem(1), DataItem(arolla::kUnit)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to INT64"));
}

TEST(CastingTest, ToFloat32_DataItem) {
  auto to_float32 = schema::ToFloat32();
  EXPECT_THAT(to_float32(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_float32(DataItem(1)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0f))));
  EXPECT_THAT(to_float32(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0f))));
  EXPECT_THAT(to_float32(DataItem(1.0f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0f))));
  EXPECT_THAT(to_float32(DataItem(1.0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0f))));
  EXPECT_THAT(to_float32(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0f))));
  EXPECT_THAT(to_float32(DataItem(false)),
              IsOkAndHolds(IsEquivalentTo(DataItem(0.0f))));
  EXPECT_THAT(to_float32(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT32"));
}

TEST(CastingTest, ToFloat32_DataSlice) {
  auto to_float32 = schema::ToFloat32();
  EXPECT_THAT(to_float32(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_float32(DataSliceImpl::Create({DataItem(1), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0f), DataItem()}))));
  EXPECT_THAT(
      to_float32(DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()})),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::Create({DataItem(1.0f), DataItem()}))));
  EXPECT_THAT(to_float32(DataSliceImpl::Create({DataItem(1.0f), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0f), DataItem()}))));
  EXPECT_THAT(to_float32(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0f), DataItem()}))));
  EXPECT_THAT(to_float32(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0f), DataItem()}))));
  EXPECT_THAT(
      to_float32(DataSliceImpl::Create({DataItem(1), DataItem(int64_t{2})})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(1.0f), DataItem(2.0f)}))));
  EXPECT_THAT(to_float32(DataSliceImpl::Create(
                  {DataItem(1.0f), DataItem(arolla::kUnit)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT32"));
}

TEST(CastingTest, ToFloat64_DataItem) {
  auto to_float64 = schema::ToFloat64();
  EXPECT_THAT(to_float64(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_float64(DataItem(1)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0))));
  EXPECT_THAT(to_float64(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0))));
  EXPECT_THAT(to_float64(DataItem(1.0f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0))));
  EXPECT_THAT(to_float64(DataItem(1.0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0))));
  EXPECT_THAT(to_float64(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(1.0))));
  EXPECT_THAT(to_float64(DataItem(false)),
              IsOkAndHolds(IsEquivalentTo(DataItem(0.0))));
  EXPECT_THAT(to_float64(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT64"));
}

TEST(CastingTest, ToFloat64_DataSlice) {
  auto to_float64 = schema::ToFloat64();
  EXPECT_THAT(to_float64(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_float64(DataSliceImpl::Create({DataItem(1), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0), DataItem()}))));
  EXPECT_THAT(
      to_float64(DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()})),
      IsOkAndHolds(
          IsEquivalentTo(DataSliceImpl::Create({DataItem(1.0), DataItem()}))));
  EXPECT_THAT(to_float64(DataSliceImpl::Create({DataItem(1.0f), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0), DataItem()}))));
  EXPECT_THAT(to_float64(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0), DataItem()}))));
  EXPECT_THAT(to_float64(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(1.0), DataItem()}))));
  EXPECT_THAT(
      to_float64(DataSliceImpl::Create({DataItem(1), DataItem(int64_t{2})})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(1.0), DataItem(2.0)}))));
  EXPECT_THAT(to_float64(DataSliceImpl::Create(
                  {DataItem(1.0), DataItem(arolla::kUnit)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT64"));
}

TEST(CastingTest, ToNone_DataItem) {
  auto to_none = schema::ToNone();
  EXPECT_THAT(to_none(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_none(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "only missing values can be converted to NONE"));
}

TEST(CastingTest, ToNone_DataSlice) {
  auto to_none = schema::ToNone();
  auto none_slice = DataSliceImpl::CreateEmptyAndUnknownType(3);
  EXPECT_THAT(to_none(none_slice), IsOkAndHolds(IsEquivalentTo(none_slice)));
  EXPECT_THAT(
      to_none(DataSliceImpl::Create(arolla::CreateEmptyDenseArray<int>(3))),
      IsOkAndHolds(IsEquivalentTo(none_slice)));
  EXPECT_THAT(to_none(DataSliceImpl::Create(
                  arolla::CreateEmptyDenseArray<internal::ObjectId>(3))),
              IsOkAndHolds(IsEquivalentTo(none_slice)));
  EXPECT_THAT(
      to_none(DataSliceImpl::Create(arolla::CreateEmptyDenseArray<int>(3),
                                    arolla::CreateEmptyDenseArray<float>(3))),
      IsOkAndHolds(IsEquivalentTo(none_slice)));
  EXPECT_THAT(to_none(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "only empty slices can be converted to NONE"));
}

TEST(CastingTest, ToExpr_DataItem) {
  auto to_expr = schema::ToExpr();
  EXPECT_THAT(to_expr(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(
      to_expr(DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")))),
      IsOkAndHolds(IsEquivalentTo(
          DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x"))))));
  EXPECT_THAT(
      to_expr(DataItem(arolla::kUnit)),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to EXPR"));
}

TEST(CastingTest, ToExpr_DataSlice) {
  auto to_expr = schema::ToExpr();
  EXPECT_THAT(to_expr(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_expr(DataSliceImpl::Create(
                  {DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x"))),
                   DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x"))),
                   DataItem()}))));
  EXPECT_THAT(
      to_expr(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to EXPR"));
}

TEST(CastingTest, ToText_DataItem) {
  auto to_text = schema::ToText();
  EXPECT_THAT(to_text(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_text(DataItem(arolla::Text("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("foo")))));
  EXPECT_THAT(to_text(DataItem(1)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("1")))));
  EXPECT_THAT(to_text(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("1")))));
  EXPECT_THAT(to_text(DataItem(1.5f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("1.5")))));
  EXPECT_THAT(to_text(DataItem(1.5)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("1.5")))));
  EXPECT_THAT(to_text(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("true")))));
  EXPECT_THAT(to_text(DataItem(arolla::kUnit)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("present")))));
  EXPECT_THAT(to_text(DataItem(arolla::Bytes("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("b'foo'")))));
  EXPECT_THAT(
      to_text(DataItem(arolla::Bytes("te\0xt"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("b'te'")))));
  EXPECT_THAT(to_text(DataItem(arolla::Bytes("te\xC0\0xt"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("b'te\xC0'")))));
  EXPECT_THAT(to_text(DataItem(internal::AllocateSingleObject())),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast ITEMID to TEXT"));
}

TEST(CastingTest, ToText_DataSlice) {
  auto to_text = schema::ToText();
  EXPECT_THAT(to_text(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_text(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create({DataItem(1), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("1")), DataItem()}))));
  EXPECT_THAT(
      to_text(DataSliceImpl::Create({DataItem(int64_t{1}), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(arolla::Text("1")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create({DataItem(1.5f), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("1.5")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create({DataItem(1.5), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("1.5")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("true")), DataItem()}))));
  EXPECT_THAT(
      to_text(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
          {DataItem(arolla::Text("present")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("te\0xt")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("b'te'")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create({DataItem(arolla::Text("foo")),
                                             DataItem(arolla::Bytes("te\0xt")),
                                             DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")),
                   DataItem(arolla::Text("b'te'")), DataItem()}))));
  EXPECT_THAT(to_text(DataSliceImpl::Create(
                  {DataItem(internal::AllocateSingleObject()), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast ITEMID to TEXT"));
}

TEST(CastingTest, ToBytes_DataItem) {
  auto to_bytes = schema::ToBytes();
  EXPECT_THAT(to_bytes(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_bytes(DataItem(arolla::Bytes("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("foo")))));
  EXPECT_THAT(
      to_bytes(DataItem(arolla::Bytes("te\xC0\0xt"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("te\xC0\0xt")))));
  EXPECT_THAT(to_bytes(DataItem(arolla::Text("foo"))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast TEXT to BYTES"));
}

TEST(CastingTest, ToBytes_DataSlice) {
  auto to_bytes = schema::ToBytes();
  EXPECT_THAT(to_bytes(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_bytes(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")), DataItem()}))));
  EXPECT_THAT(to_bytes(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast TEXT to BYTES"));
}

TEST(CastingTest, Decode_DataItem) {
  auto decode = schema::Decode();
  EXPECT_THAT(decode(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(decode(DataItem(arolla::Text("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("foo")))));
  EXPECT_THAT(decode(DataItem(arolla::Bytes("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("foo")))));
  EXPECT_THAT(decode(DataItem(arolla::Bytes("te\0xt"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("te\0xt")))));
  EXPECT_THAT(
      decode(DataItem(arolla::Bytes("\xEF\xBF\xBD"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Text("\xEF\xBF\xBD")))));
  EXPECT_THAT(decode(DataItem(arolla::Bytes("te\xC0\0xt"))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid UTF-8 sequence at position 2"));
  EXPECT_THAT(
      decode(DataItem(arolla::kUnit)),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to TEXT"));
}

TEST(CastingTest, Decode_DataSlice) {
  auto decode = schema::Decode();
  EXPECT_THAT(decode(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(decode(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")), DataItem()}))));
  EXPECT_THAT(decode(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("te\0xt")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("te\0xt")), DataItem()}))));
  EXPECT_THAT(decode(DataSliceImpl::Create({DataItem(arolla::Text("foo")),
                                            DataItem(arolla::Bytes("te\0xt")),
                                            DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")),
                   DataItem(arolla::Text("te\0xt")), DataItem()}))));
  EXPECT_THAT(decode(DataSliceImpl::Create(
                  {DataItem(arolla::Text("foo")),
                   DataItem(arolla::Bytes("te\xC0\0xt")), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid UTF-8 sequence at position 2"));
  EXPECT_THAT(
      decode(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to TEXT"));
}

TEST(CastingTest, Encode_DataItem) {
  auto encode = schema::Encode();
  EXPECT_THAT(encode(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(encode(DataItem(arolla::Bytes("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("foo")))));
  EXPECT_THAT(encode(DataItem(arolla::Text("foo"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("foo")))));
  EXPECT_THAT(encode(DataItem(arolla::Text("te\0xt"))),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("te\0xt")))));
  EXPECT_THAT(
      encode(DataItem(arolla::Text("\xEF\xBF\xBD"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("\xEF\xBF\xBD")))));
  EXPECT_THAT(
      encode(DataItem(arolla::Bytes("te\xC0\0xt"))),
      IsOkAndHolds(IsEquivalentTo(DataItem(arolla::Bytes("te\xC0\0xt")))));
  EXPECT_THAT(encode(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BYTES"));
}

TEST(CastingTest, Encode_DataSlice) {
  auto encode = schema::Encode();
  EXPECT_THAT(encode(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(encode(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")), DataItem()}))));
  EXPECT_THAT(encode(DataSliceImpl::Create(
                  {DataItem(arolla::Text("te\0xt")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("te\0xt")), DataItem()}))));
  EXPECT_THAT(encode(DataSliceImpl::Create({DataItem(arolla::Bytes("foo")),
                                              DataItem(arolla::Text("te\0xt")),
                                              DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")),
                   DataItem(arolla::Bytes("te\0xt")), DataItem()}))));
  EXPECT_THAT(encode(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")),
                   DataItem(arolla::Text("te\xC0\0xt")), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(DataSliceImpl::Create(
                  {DataItem(arolla::Bytes("foo")),
                   DataItem(arolla::Bytes("te\xC0\0xt")), DataItem()}))));
  EXPECT_THAT(
      encode(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to BYTES"));
}

TEST(CastingTest, ToMask_DataItem) {
  auto to_mask = schema::ToMask();
  EXPECT_THAT(to_mask(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_mask(DataItem(arolla::kUnit)),
              IsOkAndHolds(IsEquivalentTo(DataItem(arolla::kUnit))));
  EXPECT_THAT(to_mask(DataItem(1)), StatusIs(absl::StatusCode::kInvalidArgument,
                                             "cannot cast INT32 to MASK"));
}

TEST(CastingTest, ToMask_DataSlice) {
  auto to_mask = schema::ToMask();
  EXPECT_THAT(to_mask(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(
      to_mask(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()}))));
  EXPECT_THAT(to_mask(DataSliceImpl::Create({DataItem(1), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast INT32 to MASK"));
}

TEST(CastingTest, ToBool_DataItem) {
  auto to_bool = schema::ToBool();
  EXPECT_THAT(to_bool(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_bool(DataItem(true)),
              IsOkAndHolds(IsEquivalentTo(DataItem(true))));
  EXPECT_THAT(to_bool(DataItem(false)),
              IsOkAndHolds(IsEquivalentTo(DataItem(false))));
  EXPECT_THAT(to_bool(DataItem(0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(false))));
  EXPECT_THAT(to_bool(DataItem(1)),
              IsOkAndHolds(IsEquivalentTo(DataItem(true))));
  EXPECT_THAT(to_bool(DataItem(int64_t{0})),
              IsOkAndHolds(IsEquivalentTo(DataItem(false))));
  EXPECT_THAT(to_bool(DataItem(int64_t{1})),
              IsOkAndHolds(IsEquivalentTo(DataItem(true))));
  EXPECT_THAT(to_bool(DataItem(0.0f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(false))));
  EXPECT_THAT(to_bool(DataItem(1.0f)),
              IsOkAndHolds(IsEquivalentTo(DataItem(true))));
  EXPECT_THAT(to_bool(DataItem(0.0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(false))));
  EXPECT_THAT(to_bool(DataItem(1.0)),
              IsOkAndHolds(IsEquivalentTo(DataItem(true))));
  EXPECT_THAT(to_bool(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BOOLEAN"));
}

TEST(CastingTest, ToBool_DataSlice) {
  auto to_bool = schema::ToBool();
  EXPECT_THAT(to_bool(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create({DataItem(true), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(true), DataItem()}))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create({DataItem(1), DataItem(0)})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(true), DataItem(false)}))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create(
                  {DataItem(int64_t{1}), DataItem(int64_t{0})})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(true), DataItem(false)}))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create({DataItem(1.0f), DataItem(0.0f)})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(true), DataItem(false)}))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create({DataItem(1.0), DataItem(0.0)})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(true), DataItem(false)}))));
  EXPECT_THAT(to_bool(DataSliceImpl::Create({DataItem(false), DataItem(1.0)})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(false), DataItem(true)}))));
  EXPECT_THAT(
      to_bool(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to BOOLEAN"));
}

TEST(CastingTest, ToItemId_DataItem) {
  auto to_item_id = schema::ToItemId();
  auto obj_id = internal::AllocateSingleObject();
  EXPECT_THAT(to_item_id(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_item_id(DataItem(obj_id)),
              IsOkAndHolds(IsEquivalentTo(DataItem(obj_id))));
  EXPECT_THAT(to_item_id(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to ITEMID"));
}

TEST(CastingTest, ToItemId_DataSlice) {
  auto to_item_id = schema::ToItemId();
  auto obj_id = internal::AllocateSingleObject();
  EXPECT_THAT(to_item_id(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(to_item_id(DataSliceImpl::Create({DataItem(obj_id), DataItem()})),
              IsOkAndHolds(IsEquivalentTo(
                  DataSliceImpl::Create({DataItem(obj_id), DataItem()}))));
  EXPECT_THAT(
      to_item_id(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to ITEMID"));
}

TEST(CastingTest, ToSchema_DataItem) {
  auto to_schema = schema::ToSchema();
  auto schema_obj = internal::AllocateExplicitSchema();
  auto obj_id = internal::AllocateSingleObject();
  EXPECT_THAT(to_schema(DataItem()), IsOkAndHolds(IsEquivalentTo(DataItem())));
  EXPECT_THAT(to_schema(DataItem(schema::kInt32)),
              IsOkAndHolds(IsEquivalentTo(DataItem(schema::kInt32))));
  EXPECT_THAT(to_schema(DataItem(schema_obj)),
              IsOkAndHolds(IsEquivalentTo(DataItem(schema_obj))));
  EXPECT_THAT(to_schema(DataItem(obj_id)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat("cannot cast %v to SCHEMA", obj_id)));
  EXPECT_THAT(to_schema(DataItem(arolla::kUnit)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to SCHEMA"));
}

TEST(CastingTest, ToSchema_DataSlice) {
  auto to_schema = schema::ToSchema();
  auto schema_obj = internal::AllocateExplicitSchema();
  auto obj_id = internal::AllocateSingleObject();
  EXPECT_THAT(to_schema(DataSliceImpl::CreateEmptyAndUnknownType(3)),
              IsOkAndHolds(
                  IsEquivalentTo(DataSliceImpl::CreateEmptyAndUnknownType(3))));
  EXPECT_THAT(
      to_schema(DataSliceImpl::Create({DataItem(schema::kInt32), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(schema::kInt32), DataItem()}))));
  EXPECT_THAT(
      to_schema(DataSliceImpl::Create({DataItem(schema_obj), DataItem()})),
      IsOkAndHolds(IsEquivalentTo(
          DataSliceImpl::Create({DataItem(schema_obj), DataItem()}))));
  EXPECT_THAT(to_schema(DataSliceImpl::Create({DataItem(obj_id), DataItem()})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat("cannot cast %v to SCHEMA", obj_id)));
  EXPECT_THAT(
      to_schema(DataSliceImpl::Create({DataItem(arolla::kUnit), DataItem()})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to SCHEMA"));
}

TEST(CastingTest, ToObject_Construction) {
  EXPECT_OK(schema::ToObject::Make());
  EXPECT_OK(schema::ToObject::Make(/*validate_schema=*/false));

  // Ok to pass arbitrary schemas.
  DataItem entity_schema(internal::AllocateExplicitSchema());
  EXPECT_OK(schema::ToObject::Make(entity_schema));
  EXPECT_OK(schema::ToObject::Make(DataItem(schema::kInt32)));
  // Not ok to pass an arbitrary object.
  auto obj = DataItem(internal::AllocateSingleObject());
  EXPECT_THAT(schema::ToObject::Make(obj),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat("expected a schema, got %v", obj)));
  // Not ok to pass a NoFollow schema.
  EXPECT_THAT(
      schema::ToObject::Make(*schema::NoFollowSchemaItem(entity_schema)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "schema must not be a NoFollow schema"));
}

TEST(CastingTest, ToObject_DataItem) {
  {
    // without entity schema - without validation.
    ASSERT_OK_AND_ASSIGN(auto to_object,
                         schema::ToObject::Make(/*validate_schema=*/false));
    EXPECT_OK(to_object(DataItem()));
    EXPECT_OK(to_object(DataItem(1.0)));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_OK(to_object(obj));

    // Non-entity schema.
    ASSERT_OK_AND_ASSIGN(to_object,
                         schema::ToObject::Make(DataItem(schema::kFloat32),
                                                /*validate_schema=*/false));
    EXPECT_OK(to_object(DataItem()));
    EXPECT_OK(to_object(DataItem(1.0)));
  }
  {
    // without entity schema - with validation.
    ASSERT_OK_AND_ASSIGN(auto to_object, schema::ToObject::Make());
    EXPECT_OK(to_object(DataItem()));
    EXPECT_OK(to_object(DataItem(1.0)));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(obj),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(/*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(to_object(obj),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         absl::StrFormat("missing schema for %v", obj)));

    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK(db_impl->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_OK(to_object(obj));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Non-entity schema.
    ASSERT_OK_AND_ASSIGN(to_object,
                         schema::ToObject::Make(DataItem(schema::kFloat32)));
    EXPECT_OK(to_object(DataItem()));
    EXPECT_OK(to_object(DataItem(1.0)));
  }
  {
    // with entity schema - without validation.
    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(
        auto to_object,
        schema::ToObject::Make(entity_schema, /*validate_schema=*/false));
    EXPECT_OK(to_object(DataItem()));
    EXPECT_THAT(
        to_object(DataItem(1.0)),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(obj),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    // Setting the schema of a primitive is not ok.
    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(
                       entity_schema, /*validate_schema=*/false, &*db_impl));
    EXPECT_THAT(to_object(DataItem(1.0)),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("primitive is not allowed")));

    // Setting the schema.
    EXPECT_OK(to_object(obj));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting another schema is ok.
    DataItem schema2(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(
        to_object,
        schema::ToObject::Make(schema2, /*validate_schema=*/false, &*db_impl));
    EXPECT_OK(to_object(obj));
    auto expected_db_2 = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db_2->SetAttr(obj, schema::kSchemaAttr, schema2));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db_2));
  }
  {
    // with entity schema - with validation.
    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(auto to_object, schema::ToObject::Make(entity_schema));
    EXPECT_OK(to_object(DataItem()));
    EXPECT_THAT(
        to_object(DataItem(1.0)),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(obj),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    // Setting the schema of a primitive is not ok.
    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(entity_schema,
                                          /*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(to_object(DataItem(1.0)),
                StatusIs(absl::StatusCode::kFailedPrecondition,
                         HasSubstr("primitive is not allowed")));

    // Setting the schema.
    EXPECT_OK(to_object(obj));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting the same schema is OK.
    EXPECT_OK(to_object(obj));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting another schema is not ok.
    DataItem schema2(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(
        to_object,
        schema::ToObject::Make(schema2, /*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(
        to_object(obj),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrFormat(
                     "existing schema %v differs from the provided schema %v",
                     entity_schema, schema2)));
  }
}

TEST(CastingTest, ToObject_DataSlice) {
  {
    // without entity schema - without validation.
    ASSERT_OK_AND_ASSIGN(auto to_object,
                         schema::ToObject::Make(/*validate_schema=*/false));
    EXPECT_OK(to_object(DataSliceImpl::CreateEmptyAndUnknownType(3)));
    EXPECT_OK(to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_OK(to_object(DataSliceImpl::Create({obj, DataItem()})));
    EXPECT_OK(
        to_object(DataSliceImpl::Create({obj, DataItem(1.0), DataItem()})));

    // Non-entity schema.
    ASSERT_OK_AND_ASSIGN(to_object,
                         schema::ToObject::Make(DataItem(schema::kFloat32),
                                                /*validate_schema=*/false));
    EXPECT_OK(to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})));
  }
  {
    // without entity schema - with validation.
    ASSERT_OK_AND_ASSIGN(auto to_object, schema::ToObject::Make());
    EXPECT_OK(to_object(DataSliceImpl::CreateEmptyAndUnknownType(3)));
    EXPECT_OK(to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})));

    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({obj, DataItem()})),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    // Missing schema is not ok.
    auto obj2 = DataItem(internal::AllocateSingleObject());
    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    auto object_slice =
        DataSliceImpl::Create({obj, obj2, DataItem(1.0), DataItem()});
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(/*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(to_object(object_slice),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "missing schema for some objects"));

    // Still one missing schema.
    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK(db_impl->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(to_object(object_slice),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "missing schema for some objects"));

    // No missing schemas is ok.
    DataItem schema2(internal::AllocateExplicitSchema());
    ASSERT_OK(db_impl->SetAttr(obj2, schema::kSchemaAttr, schema2));
    EXPECT_OK(to_object(object_slice));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    ASSERT_OK(expected_db->SetAttr(obj2, schema::kSchemaAttr, schema2));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Non-entity schema.
    ASSERT_OK_AND_ASSIGN(to_object,
                         schema::ToObject::Make(DataItem(schema::kFloat32)));
    EXPECT_OK(to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})));
  }
  {
    // with entity schema - without validation.
    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(
        auto to_object,
        schema::ToObject::Make(entity_schema, /*validate_schema=*/false));
    EXPECT_OK(to_object(DataSliceImpl::CreateEmptyAndUnknownType(3)));
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({DataItem(obj), DataItem()})),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    // Setting the schema of a primitive is not ok.
    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(
                       entity_schema, /*validate_schema=*/false, &*db_impl));
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({obj, DataItem(1.0), DataItem()})),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("primitives is not allowed")));

    // Reset.
    db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(
                       entity_schema, /*validate_schema=*/false, &*db_impl));

    // Setting the schema.
    auto obj_slice = DataSliceImpl::Create({obj, DataItem()});
    EXPECT_OK(to_object(obj_slice));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting another schema is ok.
    DataItem schema2(internal::AllocateExplicitSchema());
    auto obj2 = DataItem(internal::AllocateSingleObject());
    auto obj_slice_2 = DataSliceImpl::Create({obj, obj2, DataItem()});
    ASSERT_OK_AND_ASSIGN(
        to_object,
        schema::ToObject::Make(schema2, /*validate_schema=*/false, &*db_impl));
    EXPECT_OK(to_object(obj_slice_2));
    auto expected_db_2 = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db_2->SetAttr(obj, schema::kSchemaAttr, schema2));
    ASSERT_OK(expected_db_2->SetAttr(obj2, schema::kSchemaAttr, schema2));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db_2));
  }
  {
    // with entity schema - with validation.
    DataItem entity_schema(internal::AllocateExplicitSchema());
    ASSERT_OK_AND_ASSIGN(auto to_object, schema::ToObject::Make(entity_schema));
    EXPECT_OK(to_object(DataSliceImpl::CreateEmptyAndUnknownType(3)));
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({DataItem(1.0), DataItem()})),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));
    auto obj = DataItem(internal::AllocateSingleObject());
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({DataItem(obj), DataItem()})),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));

    // Setting the schema of a primitive is not ok.
    auto db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(entity_schema,
                                          /*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(
        to_object(DataSliceImpl::Create({obj, DataItem(1.0), DataItem()})),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 HasSubstr("primitives is not allowed")));

    // Reset.
    db_impl = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK_AND_ASSIGN(
        to_object, schema::ToObject::Make(entity_schema,
                                          /*validate_schema=*/true, &*db_impl));

    // Setting the schema.
    auto obj_slice = DataSliceImpl::Create({obj, DataItem()});
    EXPECT_OK(to_object(obj_slice));
    auto expected_db = internal::DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(expected_db->SetAttr(obj, schema::kSchemaAttr, entity_schema));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting the same schema is OK.
    EXPECT_OK(to_object(obj_slice));
    EXPECT_THAT(db_impl, DataBagEqual(expected_db));

    // Setting another schema is not.
    DataItem schema2(internal::AllocateExplicitSchema());
    auto obj2 = DataItem(internal::AllocateSingleObject());
    ASSERT_OK(db_impl->SetAttr(obj2, schema::kSchemaAttr, schema2));
    auto obj_slice_2 = DataSliceImpl::Create({obj, obj2, DataItem()});
    ASSERT_OK_AND_ASSIGN(
        to_object,
        schema::ToObject::Make(schema2, /*validate_schema=*/true, &*db_impl));
    EXPECT_THAT(
        to_object(obj_slice_2),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            absl::StrFormat(
                "existing schemas %v differ from the provided schema %v",
                DataSliceImpl::Create({entity_schema, schema2, DataItem()}),
                schema2)));
  }
}

}  // namespace
}  // namespace koladata::schema
