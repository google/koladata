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
#include <cstdint>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/jagged_shape_qtype.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using arolla::TypedValue;
using internal::DataBagImpl;
using internal::DataBagImplPtr;
using internal::DataItem;
using internal::DataSliceImpl;
using internal::testing::IsEquivalentTo;
using ::testing::ElementsAreArray;

TEST(SerializationTest, DataItem) {
  std::vector<DataItem> items{
      DataItem(1),
      DataItem(2.f),
      DataItem(3l),
      DataItem(3.5),
      DataItem(),
      DataItem(internal::AllocateSingleObject()),
      DataItem(arolla::kUnit),
      DataItem(arolla::Text("abc")),
      DataItem(arolla::Bytes("cba")),
      DataItem(schema::kBytes),
      DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")))};
  std::vector<TypedValue> typed_values;
  typed_values.reserve(items.size());
  for (const DataItem& item : items) {
    typed_values.push_back(TypedValue::FromValue(item));
  }
  ASSERT_OK_AND_ASSIGN(auto proto,
                       arolla::serialization::Encode(typed_values, {}));
  ASSERT_OK_AND_ASSIGN(auto decode_result,
                       arolla::serialization::Decode(proto));
  ASSERT_EQ(decode_result.exprs.size(), 0);
  ASSERT_EQ(decode_result.values.size(), items.size());
  for (int i = 0; i < items.size(); ++i) {
    ASSERT_OK_AND_ASSIGN(DataItem res_item,
                         decode_result.values[i].As<DataItem>());
    EXPECT_EQ(res_item, items[i]);
  }
}

TEST(SerializationTest, DTypes_DataItem) {
  // Ensures that all DTypes are supported during DataItem serialization.
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    schema::DType schema = schema::GetDType<T>();
    internal::DataItem schema_item(schema);
    ASSERT_OK_AND_ASSIGN(auto proto,
                         arolla::serialization::Encode(
                             {TypedValue::FromValue(schema_item)}, {}));
    ASSERT_OK_AND_ASSIGN(auto decode_result,
                         arolla::serialization::Decode(proto));
    ASSERT_EQ(decode_result.exprs.size(), 0);
    ASSERT_EQ(decode_result.values.size(), 1);
    ASSERT_OK_AND_ASSIGN(DataItem res_item,
                         decode_result.values[0].As<DataItem>());
    EXPECT_THAT(res_item, IsEquivalentTo(schema_item));
  });
}

TEST(SerializationTest, DTypes_DataSliceImpl) {
  // Ensures that all DTypes are supported during DataSliceImpl serialization.
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    schema::DType schema = schema::GetDType<T>();
    auto slice = DataSliceImpl::Create({DataItem(schema)});
    ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                         {TypedValue::FromValue(slice)}, {}));
    ASSERT_OK_AND_ASSIGN(auto decode_result,
                         arolla::serialization::Decode(proto));
    ASSERT_EQ(decode_result.exprs.size(), 0);
    ASSERT_EQ(decode_result.values.size(), 1);
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res,
                         decode_result.values[0].As<DataSliceImpl>());
    EXPECT_THAT(res, ::testing::ElementsAreArray(slice));
  });
}

TEST(SerializationTest, DTypes_Validation) {
  {
    // DataItem.
    internal::DataItem invalid_schema(schema::DType::UnsafeFromId(-1));
    ASSERT_OK_AND_ASSIGN(auto proto,
                         arolla::serialization::Encode(
                             {TypedValue::FromValue(invalid_schema)}, {}));
    EXPECT_THAT(arolla::serialization::Decode(proto),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
  {
    // DataSliceImpl.
    auto invalid_schemas =
        DataSliceImpl::Create({DataItem(schema::DType::UnsafeFromId(-1))});
    ASSERT_OK_AND_ASSIGN(auto proto,
                         arolla::serialization::Encode(
                             {TypedValue::FromValue(invalid_schemas)}, {}));
    EXPECT_THAT(arolla::serialization::Decode(proto),
                StatusIs(absl::StatusCode::kInvalidArgument));
  }
}

TEST(SerializationTest, JaggedShapeQType) {
  ASSERT_OK_AND_ASSIGN(auto proto,
                       arolla::serialization::Encode(
                           {TypedValue::FromValue(GetJaggedShapeQType())}, {}));
  ASSERT_OK_AND_ASSIGN(auto decode_result,
                       arolla::serialization::Decode(proto));
  ASSERT_OK_AND_ASSIGN(arolla::QTypePtr res,
                       decode_result.values[0].As<arolla::QTypePtr>());
  EXPECT_EQ(res, GetJaggedShapeQType());
}

TEST(SerializationTest, JaggedShapeQValue) {
  auto shape = test::ShapeFromSplitPoints({{0, 2}});
  ASSERT_OK_AND_ASSIGN(
      auto tv, TypedValue::FromValueWithQType(shape, GetJaggedShapeQType()));
  ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode({tv}, {}));
  ASSERT_OK_AND_ASSIGN(auto decode_result,
                       arolla::serialization::Decode(proto));
  EXPECT_EQ(decode_result.values[0].GetType(), GetJaggedShapeQType());
  ASSERT_OK_AND_ASSIGN(
      arolla::JaggedDenseArrayShape res,
      decode_result.values[0].As<arolla::JaggedDenseArrayShape>());
  EXPECT_TRUE(res.IsEquivalentTo(shape));
}

TEST(SerializationTest, DataSliceImpl) {
  auto slice = DataSliceImpl::Create(
      {DataItem(1), DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x"))),
       DataItem(2), DataItem(), DataItem(arolla::Text("abc")), DataItem(2.0),
       DataItem(1.5f), DataItem(arolla::Bytes("cba")),
       DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("y"))),
       DataItem(3)});
  ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                       {TypedValue::FromValue(slice)}, {}));
  ASSERT_OK_AND_ASSIGN(auto decode_result,
                       arolla::serialization::Decode(proto));
  ASSERT_EQ(decode_result.exprs.size(), 0);
  ASSERT_EQ(decode_result.values.size(), 1);
  ASSERT_OK_AND_ASSIGN(DataSliceImpl res,
                       decode_result.values[0].As<DataSliceImpl>());
  EXPECT_THAT(res, ::testing::ElementsAreArray(slice));
}

TEST(SerializationTest, BigDataSliceImplInt64) {
  for (const int64_t mega_size : {1, 10}) {
    std::vector<int64_t> values(mega_size * 1000 * 1000);
    std::iota(values.begin(), values.end(), 0);
    auto slice = DataSliceImpl::Create(arolla::CreateFullDenseArray(values));
    std::vector<TypedValue> typed_values;
    ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                         {TypedValue::FromValue(slice)}, {}));
    // Size is 4MB for 1M values and 48MB for 10M values.
    EXPECT_LT(proto.ByteSizeLong(), mega_size * 5 * 1000 * 1000);

    if (mega_size == 1) {
      EXPECT_EQ(proto.decoding_steps_size(), 3);
    } else if (mega_size == 10) {
      // Big slice is split into parts, so we have more decoding steps.
      EXPECT_EQ(proto.decoding_steps_size(), 13);
      for (const auto& step : proto.decoding_steps()) {
        EXPECT_LT(step.ByteSizeLong(), 20000000);
      }
    } else {
      ASSERT_TRUE(false);
    }

    ASSERT_OK_AND_ASSIGN(auto decode_result,
                         arolla::serialization::Decode(proto));
    ASSERT_EQ(decode_result.exprs.size(), 0);
    ASSERT_EQ(decode_result.values.size(), 1);
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res,
                         decode_result.values[0].As<DataSliceImpl>());
    EXPECT_THAT(res, ::testing::ElementsAreArray(slice));
  }
}

TEST(SerializationTest, BigDataSliceImplBytes) {
  for (const int64_t string_size : {10, 4000000}) {
    std::string s;
    s.resize(string_size);
    for (char& c : s) c = 'A';
    arolla::StringsBuffer::Builder bldr(10);
    for (int i = 0; i < 10; ++i) {
      s[0] = 'A' + i;
      bldr.Set(i, s);
    }
    auto slice = DataSliceImpl::Create(
        arolla::DenseArray<arolla::Bytes>{std::move(bldr).Build()});
    std::vector<TypedValue> typed_values;
    ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                         {TypedValue::FromValue(slice)}, {}));
    // 182 bytes for 10 10B strings and 40MB for 10 4MB strings.
    EXPECT_LT(proto.ByteSizeLong(), 100 + 1.05 * string_size * 10);

    if (string_size == 10) {
      EXPECT_EQ(proto.decoding_steps_size(), 3);
    } else if (string_size == 4000000) {
      // Big slice is split into parts, so we have more decoding steps.
      EXPECT_EQ(proto.decoding_steps_size(), 7);
      for (const auto& step : proto.decoding_steps()) {
        EXPECT_LT(step.ByteSizeLong(), 15000000);
      }
    } else {
      ASSERT_TRUE(false);
    }

    ASSERT_OK_AND_ASSIGN(auto decode_result,
                         arolla::serialization::Decode(proto));
    ASSERT_EQ(decode_result.exprs.size(), 0);
    ASSERT_EQ(decode_result.values.size(), 1);
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res,
                         decode_result.values[0].As<DataSliceImpl>());
    EXPECT_THAT(res, ::testing::ElementsAreArray(slice));
  }
}

TEST(SerializationTest, DataSliceImplObjectLinkToParentIdBytesSize) {
  constexpr int64_t kSize = 1000000;
  constexpr int64_t kPerParent = 100;
  auto slice = DataSliceImpl::AllocateEmptyObjects(kSize / kPerParent);
  std::vector<DataItem> items;
  items.reserve(kSize);
  for (const auto& item : slice) {
    for (int i = 0; i < kPerParent; ++i) {
     items.push_back(DataItem(item));
    }
  }
  slice = DataSliceImpl::Create(items);

  std::vector<TypedValue> typed_values;
  ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                       {TypedValue::FromValue(slice)}, {}));
  // Real number is 3MB. We set a limit a bit higher.
  EXPECT_LT(proto.ByteSizeLong(), 3.5 * 1000 * 1000);
}

// Quite common case that may be useful to optimize in the future.
TEST(SerializationTest, BigDataSliceImplObjectIdFullAlloc) {
  for (const int64_t mega_size : {1, 10}) {
    auto slice = DataSliceImpl::AllocateEmptyObjects(mega_size * 1000 * 1000);
    std::vector<TypedValue> typed_values;
    ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                         {TypedValue::FromValue(slice)}, {}));
    // Size is 3MB for 1M values and 12.2MB for 10M values.
    EXPECT_LT(proto.ByteSizeLong(), 3.1 * mega_size * 1000 * 1000);

    if (mega_size == 1) {
      EXPECT_EQ(proto.decoding_steps_size(), 5);
    } else if (mega_size == 10) {
      // Big slice is split into parts, so we have more decoding steps.
      EXPECT_EQ(proto.decoding_steps_size(), 6);
    } else {
      ASSERT_TRUE(false);
    }

    ASSERT_OK_AND_ASSIGN(auto decode_result,
                         arolla::serialization::Decode(proto));
    ASSERT_EQ(decode_result.exprs.size(), 0);
    ASSERT_EQ(decode_result.values.size(), 1);
    ASSERT_OK_AND_ASSIGN(DataSliceImpl res,
                         decode_result.values[0].As<DataSliceImpl>());
    EXPECT_THAT(res, ::testing::ElementsAreArray(slice));
  }
}

TEST(SerializationTest, SchemaAttributesForBigAllocs) {
  static constexpr int64_t kSize = 57;
  auto schema_slice = DataSliceImpl::ObjectsFromAllocation(
      internal::AllocateExplicitSchemas(kSize), kSize);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataBagImpl & db_impl, db->GetMutableImpl());
  ASSERT_OK(db_impl.SetSchemaAttr(schema_slice, "a", schema_slice));

  ASSERT_OK_AND_ASSIGN(auto before, db_impl.GetSchemaAttrs(schema_slice[0]));

  ASSERT_OK_AND_ASSIGN(auto proto, arolla::serialization::Encode(
                                       {TypedValue::FromValue(db)}, {}));
  ASSERT_OK_AND_ASSIGN(auto decode_result,
                       arolla::serialization::Decode(proto));
  ASSERT_EQ(decode_result.exprs.size(), 0);
  ASSERT_EQ(decode_result.values.size(), 1);
  ASSERT_OK_AND_ASSIGN(DataBagPtr res,
                       decode_result.values[0].As<DataBagPtr>());
  EXPECT_THAT(res->GetImpl().GetSchemaAttr(schema_slice, "a"),
              IsOkAndHolds(ElementsAreArray(schema_slice)));
  EXPECT_THAT(res->GetImpl().GetSchemaAttrs(schema_slice[0]),
              IsOkAndHolds(ElementsAreArray(before)));
}

}  // namespace
}  // namespace koladata
