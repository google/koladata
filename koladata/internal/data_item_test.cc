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
#include "koladata/internal/data_item.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/testing/test_operators.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::arolla::GetQType;
using ::arolla::TypedRef;
using ::arolla::TypedValue;
using ::testing::MatchesRegex;

TEST(DataItemTest, EmptyObject) {
  DataItem item = DataItem(AllocateSingleObject());
  EXPECT_EQ(item.dtype(), GetQType<ObjectId>());
  EXPECT_TRUE(item.holds_value<ObjectId>());
}

TEST(DataItemTest, Mask) {
  DataItem item = DataItem(arolla::Unit());
  EXPECT_EQ(item.dtype(), GetQType<arolla::Unit>());
  EXPECT_TRUE(item.holds_value<arolla::Unit>());
}

TEST(DataItemTest, ExprQuote) {
  arolla::expr::ExprOperatorPtr op =
      std::make_shared<arolla::expr::testing::DummyOp>(
          "op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_OK_AND_ASSIGN(auto expr,
                       arolla::expr::CallOp(op, {arolla::expr::Leaf("x")}));
  DataItem item = DataItem(arolla::expr::ExprQuote(expr));
  EXPECT_EQ(item.dtype(), GetQType<arolla::expr::ExprQuote>());
  EXPECT_TRUE(item.holds_value<arolla::expr::ExprQuote>());

  auto x = arolla::expr::ExprQuote(arolla::expr::Leaf("x"));
  auto y = arolla::expr::ExprQuote(arolla::expr::Leaf("y"));

  EXPECT_FALSE(DataItem::Less()(DataItem(x), DataItem(x)));
  EXPECT_FALSE(DataItem::Less()(DataItem(x), x));
  EXPECT_TRUE(DataItem::Less()(DataItem(x), DataItem(y)) ||
              DataItem::Less()(DataItem(y), DataItem(x)));
  EXPECT_TRUE(DataItem::Less()(DataItem(x), y) ||
              DataItem::Less()(DataItem(y), x));
  EXPECT_NE(DataItem::Less()(DataItem(x), DataItem(y)),
            DataItem::Less()(DataItem(y), x));
}

TEST(DataItemTest, DType) {
  DataItem item = DataItem(schema::kInt32);
  EXPECT_EQ(item.dtype(), GetQType<schema::DType>());
  EXPECT_TRUE(item.holds_value<schema::DType>());

  EXPECT_TRUE(
      DataItem::Less()(DataItem(schema::kInt32), schema::DType(schema::kBool)));
}

TEST(DataItemTest, Object) {
  ObjectId id = AllocateSingleObject();
  for (DataItem item : {DataItem(id), DataItem(std::make_optional(id)),
                        DataItem(arolla::MakeOptionalValue(id))}) {
    EXPECT_EQ(item.dtype(), GetQType<ObjectId>());
    ASSERT_TRUE(item.holds_value<ObjectId>());
    EXPECT_EQ(item.value<ObjectId>(), id);
    EXPECT_FALSE(item.holds_value<int>());
  }
}

TEST(DataItemTest, Missing) {
  for (DataItem item : {DataItem(), DataItem(std::optional<ObjectId>()),
                        DataItem(std::optional<int>()),
                        DataItem(arolla::OptionalValue<ObjectId>())}) {
    EXPECT_EQ(item.dtype(), arolla::GetNothingQType());
    EXPECT_FALSE(item.has_value());
  }
}

TEST(DataItemTest, MoveValue) {
  DataItem item(arolla::Text(
      "TEST(DataItemTest, MoveValue) { DataItem item(arolla::Text(\"\"));}"));
  absl::string_view view_before = item.value<arolla::Text>().view();
  arolla::Text text = std::move(item).MoveValue<arolla::Text>();
  absl::string_view view_after = text.view();
  // Check that the value was moved rather than copied.
  EXPECT_EQ(view_before.begin(), view_after.begin());
}

TEST(DataItemTest, PresentPrimitive) {
  for (DataItem item : {DataItem(5.0f), DataItem(std::make_optional(5.0f))}) {
    EXPECT_EQ(item.dtype(), GetQType<float>());
    ASSERT_TRUE(item.holds_value<float>());
    EXPECT_EQ(item.value<float>(), 5.0f);
  }
}

TEST(DataItemTest, PresentPrimitiveTypedValue) {
  for (auto item_or_status : {DataItem::Create(TypedRef::FromValue(57)),
                              DataItem::Create(TypedValue::FromValue(57))}) {
    EXPECT_TRUE(item_or_status.ok());
    auto item = std::move(item_or_status).value();
    EXPECT_EQ(item.dtype(), GetQType<int>());
    ASSERT_TRUE(item.holds_value<int>());
    EXPECT_EQ(item.value<int>(), 57);
  }
}

TEST(DataItemTest, PresentOptionalTypedValue) {
  auto value = arolla::OptionalValue<int>(57);
  for (auto item_or_status : {DataItem::Create(TypedRef::FromValue(value)),
                              DataItem::Create(TypedValue::FromValue(value))}) {
    EXPECT_TRUE(item_or_status.ok());
    auto item = std::move(item_or_status).value();
    EXPECT_EQ(item.dtype(), GetQType<int>());
    ASSERT_TRUE(item.holds_value<int>());
    EXPECT_EQ(item.value<int>(), 57);
  }
}

TEST(DataItemTest, MissingOptionalTypedValue) {
  auto value = arolla::OptionalValue<int>();
  for (auto item_or_status : {DataItem::Create(TypedRef::FromValue(value)),
                              DataItem::Create(TypedValue::FromValue(value))}) {
    EXPECT_TRUE(item_or_status.ok());
    auto item = std::move(item_or_status).value();
    EXPECT_EQ(item.dtype(), arolla::GetNothingQType());
    EXPECT_FALSE(item.has_value());
  }
}

TEST(DataItemTest, InvalidTypedValue) {
  auto value = arolla::DenseArray<int>();
  for (auto item_or_status : {DataItem::Create(TypedRef::FromValue(value)),
                              DataItem::Create(TypedValue::FromValue(value))}) {
    EXPECT_THAT(item_or_status,
                ::absl_testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                         "DataItem cannot be created from "
                                         "value with type DENSE_ARRAY_INT32"));
  }
}

TEST(DataItemTest, DebugString) {
  EXPECT_EQ(DataItem(5).DebugString(), "5");
  EXPECT_EQ(DataItem(static_cast<int64_t>(5)).DebugString(), "5");
  EXPECT_EQ(DataItem(float{3.14}).DebugString(), "3.14");
  EXPECT_EQ(DataItem(static_cast<double>(3.14)).DebugString(), "3.14");
  EXPECT_EQ(DataItem(arolla::Text("abc")).DebugString(), "'abc'");
  EXPECT_EQ(DataItem(arolla::Text("a'b\"c with \" quotes")).DebugString(),
            "'a'b\"c with \" quotes'");
  EXPECT_EQ(DataItem(arolla::Bytes("abc")).DebugString(), "b'abc'");
  EXPECT_EQ(DataItem(arolla::Bytes("\010\011\012\013\014\015")).DebugString(),
            "b'\\x08\\t\\n\\x0b\\x0c\\r'");
  EXPECT_NE(DataItem(AllocateSingleObject()).DebugString(), "");
}

TEST(DataItemTest, DebugString_float) {
  EXPECT_EQ(DataItem(std::numeric_limits<float>::quiet_NaN()).DebugString(),
            "nan");
  EXPECT_EQ(DataItem(float{-1.}).DebugString(), "-1.0");
  EXPECT_EQ(DataItem(float{-0.}).DebugString(), "-0.0");
  EXPECT_THAT(DataItem(float{1}).DebugString(), "1.0");
  EXPECT_THAT(DataItem(float{0.2}).DebugString(), "0.2");
  EXPECT_THAT(DataItem(float{1e30}).DebugString(), "1e30");
  EXPECT_THAT(DataItem(float{1e-30}).DebugString(), "1e-30");
  EXPECT_THAT(DataItem(std::numeric_limits<float>::infinity()).DebugString(),
              "inf");
  EXPECT_THAT(DataItem(-std::numeric_limits<float>::infinity()).DebugString(),
              "-inf");
  EXPECT_THAT(DataItem(std::numeric_limits<float>::quiet_NaN()).DebugString(),
              "nan");
  EXPECT_THAT(DataItem(-std::numeric_limits<float>::quiet_NaN()).DebugString(),
              "nan");
  EXPECT_THAT(
      DataItem(std::numeric_limits<float>::signaling_NaN()).DebugString(),
      "nan");
  EXPECT_THAT(
      DataItem(-std::numeric_limits<float>::signaling_NaN()).DebugString(),
      "nan");
}

TEST(DataItem, DebugString_float64) {
  EXPECT_THAT(DataItem(double{-1.}).DebugString(), "-1.0");
  EXPECT_THAT(DataItem(double{-0.}).DebugString(), "-0.0");
  EXPECT_THAT(DataItem(double{0.}).DebugString(), "0.0");
  EXPECT_THAT(DataItem(double{1}).DebugString(), "1.0");
  EXPECT_THAT(DataItem(double{0.2}).DebugString(), "0.2");
  EXPECT_THAT(DataItem(double{1e30}).DebugString(), "1e30");
  EXPECT_THAT(DataItem(double{1e-30}).DebugString(), "1e-30");
  EXPECT_THAT(DataItem(std::numeric_limits<double>::infinity()).DebugString(),
              "inf");
  EXPECT_THAT(DataItem(-std::numeric_limits<double>::infinity()).DebugString(),
              "-inf");
  EXPECT_THAT(DataItem(std::numeric_limits<double>::quiet_NaN()).DebugString(),
              "nan");
  EXPECT_THAT(DataItem(-std::numeric_limits<double>::quiet_NaN()).DebugString(),
              "nan");
  EXPECT_THAT(DataItem(double{0.2f}).DebugString(), "0.2");
}

TEST(DataItemTest, AbslStringify) {
  EXPECT_EQ(absl::StrCat(DataItem(5)), "5");
  EXPECT_EQ(absl::StrCat(DataItem(static_cast<int64_t>(5))), "5");
  EXPECT_EQ(absl::StrCat(DataItem(float{3.14})), "3.14");
  EXPECT_EQ(absl::StrCat(DataItem(static_cast<double>(3.14))), "3.14");
  EXPECT_EQ(absl::StrCat(DataItem(arolla::Text("abc"))), "'abc'");
  EXPECT_EQ(absl::StrCat(DataItem(arolla::Text("a'b\"c with \" quotes"))),
            "'a'b\"c with \" quotes'");
  EXPECT_EQ(absl::StrCat(DataItem(arolla::Bytes("abc"))), "b'abc'");
  EXPECT_EQ(absl::StrCat(DataItem(arolla::Bytes("\010\011\012\013\014\015"))),
            "b'\\x08\\t\\n\\x0b\\x0c\\r'");
  EXPECT_NE(absl::StrCat(DataItem(AllocateSingleObject())), "");
}

TEST(DataItemTest, Hash) {
  auto hasher = DataItem::Hash();
  EXPECT_EQ(hasher(DataItem()), hasher(DataItem()));
  EXPECT_NE(hasher(DataItem()), hasher(DataItem(0)));
  EXPECT_EQ(hasher(DataItem(0.0f)), hasher(DataItem(0.0f)));
  EXPECT_EQ(hasher(DataItem(0.0f)), hasher(0.0f));
  EXPECT_NE(hasher(DataItem(0.0f)), hasher(DataItem(1.0f)));
  EXPECT_NE(hasher(DataItem(0.0f)), hasher(1.0f));
  EXPECT_NE(hasher(DataItem(0.0f)), hasher(DataItem(0)));
  EXPECT_NE(hasher(DataItem(0.0f)), hasher(0));
  EXPECT_EQ(hasher(DataItem(0)), hasher(DataItem(int64_t{0})));
  EXPECT_EQ(hasher(DataItem(0)), hasher(int64_t{0}));
  EXPECT_EQ(hasher(DataItem(0.f)), hasher(DataItem(0.)));
  EXPECT_EQ(hasher(DataItem(0.f)), hasher(0.));
  EXPECT_NE(hasher(DataItem(1.)), hasher(DataItem(2.f)));
  EXPECT_NE(hasher(DataItem(1.)), hasher(2.f));
  EXPECT_NE(hasher(DataItem()), hasher(DataItem(schema::DType())));
  EXPECT_EQ(hasher(DataItem(schema::DType())),
            hasher(DataItem(schema::DType())));
  EXPECT_EQ(hasher(DataItem(schema::kInt32)), hasher(DataItem(schema::kInt32)));
  EXPECT_EQ(hasher(DataItem(schema::kInt32)), hasher(schema::kInt32));
  EXPECT_NE(hasher(DataItem(schema::kInt32)), hasher(schema::kFloat32));
  EXPECT_NE(hasher(DataItem(schema::kInt32)), hasher(0));
  EXPECT_NE(hasher(DataItem(schema::kInt32)),
            hasher(DataItem(schema::kFloat32)));
  EXPECT_NE(hasher(DataItem(schema::kInt32)),
            hasher(DataItem(schema::DType())));
}

TEST(DataItemTest, IsEquivalentTo) {
  EXPECT_TRUE(DataItem(0).IsEquivalentTo(DataItem(0)));
  EXPECT_FALSE(DataItem(0).IsEquivalentTo(DataItem(1)));
  EXPECT_FALSE(DataItem(0).IsEquivalentTo(DataItem(0.f)));
  EXPECT_FALSE(DataItem(int64_t{5}).IsEquivalentTo(DataItem(5)));
  EXPECT_FALSE(DataItem(6.f).IsEquivalentTo(DataItem(6.)));
  EXPECT_FALSE(DataItem(5.).IsEquivalentTo(DataItem(6.)));
  EXPECT_TRUE(DataItem(5.).IsEquivalentTo(DataItem(5.)));
}

TEST(DataItemTest, Equality) {
  auto eq = DataItem::Eq();
  EXPECT_TRUE(eq(DataItem(0), DataItem(0)));
  EXPECT_FALSE(eq(DataItem(0), DataItem(1)));
  EXPECT_FALSE(eq(DataItem(0), DataItem(0.f)));
  EXPECT_TRUE(eq(DataItem(int64_t{5}), DataItem(5)));
  EXPECT_TRUE(eq(DataItem(6.f), DataItem(6.)));
  EXPECT_FALSE(eq(DataItem(5.), DataItem(6.f)));
}

TEST(DataItemTest, Less) {
  auto less = DataItem::Less();
  EXPECT_FALSE(less(DataItem(0), DataItem(0)));
  EXPECT_TRUE(less(DataItem(0), DataItem(1)));
  EXPECT_TRUE(less(DataItem(0), DataItem(0.f)));
  EXPECT_FALSE(less(DataItem(int64_t{5}), DataItem(5)));
  EXPECT_FALSE(less(DataItem(5), DataItem(int64_t{5})));
  EXPECT_FALSE(less(DataItem(4.f), DataItem(int64_t{5})));
  EXPECT_FALSE(less(DataItem(7.f), DataItem(6.)));
  EXPECT_TRUE(less(DataItem(5.), DataItem(6.f)));
}

TEST(DataItemTest, IsKodaScalarSortable) {
  EXPECT_TRUE(IsKodaScalarSortable<int64_t>());
  EXPECT_TRUE(IsKodaScalarSortable<float>());
  EXPECT_TRUE(IsKodaScalarSortable<double>());
  EXPECT_TRUE(IsKodaScalarSortable<arolla::Text>());
  EXPECT_TRUE(IsKodaScalarSortable<arolla::Bytes>());
  EXPECT_TRUE(IsKodaScalarSortable<absl::string_view>());
  EXPECT_FALSE(IsKodaScalarSortable<arolla::expr::ExprQuote>());
  EXPECT_FALSE(IsKodaScalarSortable<schema::DType>());
  EXPECT_FALSE(IsKodaScalarSortable<ObjectId>());
}

TEST(DataItemTest, IsKodaScalarQTypeSortable) {
  EXPECT_TRUE(IsKodaScalarQTypeSortable(arolla::GetQType<int64_t>()));
  EXPECT_TRUE(IsKodaScalarQTypeSortable(arolla::GetQType<float>()));
  EXPECT_TRUE(IsKodaScalarQTypeSortable(arolla::GetQType<double>()));
  EXPECT_TRUE(IsKodaScalarQTypeSortable(arolla::GetQType<arolla::Text>()));
  EXPECT_TRUE(IsKodaScalarQTypeSortable(arolla::GetQType<arolla::Bytes>()));
  EXPECT_FALSE(
      IsKodaScalarQTypeSortable(arolla::GetQType<arolla::expr::ExprQuote>()));
  EXPECT_FALSE(IsKodaScalarQTypeSortable(arolla::GetQType<schema::DType>()));
  EXPECT_FALSE(IsKodaScalarQTypeSortable(arolla::GetQType<ObjectId>()));
}

TEST(DataItemTest, IsList) {
  EXPECT_TRUE(DataItem(AllocateSingleList()).is_list());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_list());
  EXPECT_FALSE(DataItem(0).is_list());
  EXPECT_FALSE(DataItem(4.f).is_list());
  EXPECT_FALSE(DataItem().is_list());
}

TEST(DataItemTest, IsDict) {
  EXPECT_TRUE(DataItem(AllocateSingleDict()).is_dict());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_dict());
  EXPECT_FALSE(DataItem(0).is_dict());
  EXPECT_FALSE(DataItem(4.f).is_dict());
  EXPECT_FALSE(DataItem().is_dict());
}

TEST(DataItemTest, IsSchema) {
  EXPECT_TRUE(DataItem(AllocateExplicitSchema()).is_schema());
  EXPECT_TRUE(DataItem(schema::kAny).is_schema());
  EXPECT_TRUE(DataItem(schema::kExpr).is_schema());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_schema());
  EXPECT_FALSE(DataItem(0).is_schema());
  EXPECT_FALSE(DataItem(4.f).is_schema());
}

TEST(DataItemTest, IsPrimitiveSchema) {
  EXPECT_TRUE(DataItem(schema::kMask).is_primitive_schema());
  EXPECT_TRUE(DataItem(schema::kInt32).is_primitive_schema());
  EXPECT_TRUE(DataItem(schema::kExpr).is_primitive_schema());
  EXPECT_FALSE(DataItem(AllocateExplicitSchema()).is_primitive_schema());
  EXPECT_FALSE(DataItem(schema::kAny).is_primitive_schema());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_primitive_schema());
  EXPECT_FALSE(DataItem(0).is_primitive_schema());
  EXPECT_FALSE(DataItem(4.f).is_primitive_schema());
}

TEST(DataItemTest, IsEntitySchema) {
  EXPECT_FALSE(DataItem(schema::kMask).is_entity_schema());
  EXPECT_FALSE(DataItem(schema::kObject).is_entity_schema());
  EXPECT_FALSE(DataItem(schema::kAny).is_entity_schema());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_entity_schema());
  EXPECT_FALSE(DataItem(0).is_entity_schema());
  EXPECT_FALSE(DataItem(4.f).is_entity_schema());
  EXPECT_TRUE(DataItem(AllocateExplicitSchema()).is_entity_schema());
  // Implicit entity schema.
  EXPECT_TRUE(
      DataItem(
          internal::CreateUuidWithMainObject<
              internal::ObjectId::kUuidImplicitSchemaFlag>(
              internal::AllocateSingleObject(),
              arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()))
          .is_entity_schema());
}

TEST(DataItemTest, IsImplicitSchema) {
  EXPECT_FALSE(DataItem(schema::kMask).is_implicit_schema());
  EXPECT_FALSE(DataItem(schema::kObject).is_implicit_schema());
  EXPECT_FALSE(DataItem(schema::kAny).is_implicit_schema());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).is_implicit_schema());
  EXPECT_FALSE(DataItem(0).is_implicit_schema());
  EXPECT_FALSE(DataItem(4.f).is_implicit_schema());
  EXPECT_FALSE(DataItem(AllocateExplicitSchema()).is_implicit_schema());
  // Implicit entity schema.
  EXPECT_TRUE(
      DataItem(
          internal::CreateUuidWithMainObject<
              internal::ObjectId::kUuidImplicitSchemaFlag>(
              internal::AllocateSingleObject(),
              arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()))
          .is_implicit_schema());
}

TEST(DataItemTest, ContainsOnlyLists) {
  EXPECT_TRUE(DataItem(AllocateSingleList()).ContainsOnlyLists());
  EXPECT_TRUE(DataItem().ContainsOnlyLists());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).ContainsOnlyLists());
  EXPECT_FALSE(DataItem(0).ContainsOnlyLists());
  EXPECT_FALSE(DataItem(4.f).ContainsOnlyLists());
}

TEST(DataItemTest, ContainsOnlyDicts) {
  EXPECT_TRUE(DataItem(AllocateSingleDict()).ContainsOnlyDicts());
  EXPECT_TRUE(DataItem().ContainsOnlyDicts());
  EXPECT_FALSE(DataItem(AllocateSingleObject()).ContainsOnlyDicts());
  EXPECT_FALSE(DataItem(0).ContainsOnlyDicts());
  EXPECT_FALSE(DataItem(4.f).ContainsOnlyDicts());
}

TEST(DataItemTest, ItemView) {
  // Equality
  EXPECT_EQ(DataItem(0), DataItem::View<int>{0});
  EXPECT_NE(DataItem(0), DataItem::View<int>{1});
  EXPECT_NE(DataItem(0), DataItem::View<float>{0});
  EXPECT_EQ(DataItem(5), DataItem::View<int64_t>{5});
  EXPECT_NE(DataItem(5.f), DataItem::View<double>{6.});
  EXPECT_EQ(DataItem(5.f), DataItem::View<double>{5.});

  EXPECT_EQ(DataItem(arolla::Bytes("abc")),
            DataItem::View<arolla::Bytes>{absl::string_view("abc")});
  EXPECT_NE(DataItem(arolla::Bytes("abc")),
            DataItem::View<arolla::Text>{absl::string_view("abc")});
  EXPECT_NE(DataItem(arolla::Bytes("abc")),
            DataItem::View<arolla::Bytes>{absl::string_view("cde")});

  // Less
  auto less = DataItem::Less();
  EXPECT_FALSE(less(DataItem(0), DataItem::View<int>{0}));
  EXPECT_TRUE(less(DataItem(0), DataItem::View<int>{1}));
  EXPECT_TRUE(less(DataItem(0), DataItem::View<float>{0}));
  EXPECT_FALSE(less(DataItem(int64_t{5}), DataItem::View<int>{5}));
  EXPECT_FALSE(less(DataItem(5), DataItem::View<int64_t>{5}));
  EXPECT_TRUE(less(DataItem(5.f), DataItem::View<double>{6.}));
  EXPECT_TRUE(less(DataItem(5.), DataItem::View<float>{6.f}));

  EXPECT_FALSE(less(DataItem(arolla::Bytes("abc")),
                    DataItem::View<arolla::Bytes>{absl::string_view("abc")}));
  EXPECT_FALSE(less(DataItem(arolla::Bytes("abc")),
                    DataItem::View<arolla::Text>{absl::string_view("abc")}));
  EXPECT_TRUE(less(DataItem(arolla::Bytes("abc")),
                   DataItem::View<arolla::Bytes>{absl::string_view("cde")}));

  // Hash
  auto hasher = DataItem::Hash();
  EXPECT_EQ(hasher(DataItem(0)), hasher(DataItem::View<int>{0}));
  EXPECT_NE(hasher(DataItem(0)), hasher(DataItem::View<int>{1}));
  EXPECT_NE(hasher(DataItem(0)), hasher(DataItem::View<float>{0}));
  EXPECT_EQ(hasher(DataItem(0)), hasher(DataItem::View<int64_t>{0}));
  EXPECT_EQ(hasher(DataItem(0.f)), hasher(DataItem::View<double>{0}));
  EXPECT_NE(hasher(DataItem(1.)), hasher(DataItem::View<float>{2.f}));

  EXPECT_EQ(hasher(DataItem(arolla::Bytes("abc"))),
            hasher(DataItem::View<arolla::Bytes>{absl::string_view("abc")}));
  EXPECT_NE(hasher(DataItem(arolla::Bytes("abc"))),
            hasher(DataItem::View<arolla::Text>{absl::string_view("abc")}));
  EXPECT_NE(hasher(DataItem(arolla::Bytes("abc"))),
            hasher(DataItem::View<arolla::Bytes>{absl::string_view("cde")}));
}

TEST(DataItemTest, ArollaFingerprint) {
  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(DataItem()).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem()).Finish());
  // NOTE: This expectation fails if data_.index() is not included in the
  // Fingerprint computation.
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(DataItem()).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem(0)).Finish());

  EXPECT_EQ(arolla::FingerprintHasher("salt").Combine(DataItem(0.0f)).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem(0.0f)).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(DataItem(-0.0f)).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem(0.0f)).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(DataItem(0.0f)).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem(1.0f)).Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt").Combine(DataItem(0.0f)).Finish(),
            arolla::FingerprintHasher("salt").Combine(DataItem(0)).Finish());
  EXPECT_EQ(arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::DType()))
                .Finish(),
            arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::DType()))
                .Finish());
  EXPECT_EQ(arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::kInt32))
                .Finish(),
            arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::kInt32))
                .Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::kFloat32))
                .Finish(),
            arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::kInt32))
                .Finish());
  EXPECT_NE(arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::DType()))
                .Finish(),
            arolla::FingerprintHasher("salt")
                .Combine(DataItem(schema::kInt32))
                .Finish());
}

TEST(DataItemTest, TestRepr) {
  EXPECT_EQ(DataItemRepr(DataItem(0)), "0");
  EXPECT_EQ(DataItemRepr(DataItem(arolla::Text("a"))), "'a'");
  EXPECT_EQ(DataItemRepr(DataItem(arolla::Text("a")), {.strip_quotes = true}),
            "a");
  EXPECT_THAT(DataItemRepr(DataItem(AllocateSingleObject())),
              MatchesRegex(R"regex(\$[0-9a-f]{32}:0)regex"));
  EXPECT_THAT(DataItemRepr(DataItem(CreateUuidObject(
                  arolla::FingerprintHasher("").Combine(57).Finish()))),
              MatchesRegex(R"regex(k[0-9a-f]{32}:0)regex"));
  EXPECT_EQ(DataItemRepr(DataItem(double{1.23456789}), {.show_dtype = true}),
            "float64{1.2345679}");
  EXPECT_EQ(DataItemRepr(DataItem(int64_t{123}), {.show_dtype = true}),
            "int64{123}");
}

}  // namespace
}  // namespace koladata::internal
