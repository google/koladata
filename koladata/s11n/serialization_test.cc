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
#include <vector>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata {
namespace {

using arolla::TypedValue;
using internal::DataItem;
using internal::DataSliceImpl;

class SerializationTest : public ::testing::Test {
 protected:
  void SetUp() override { arolla::InitArolla(); }
};

TEST_F(SerializationTest, DataItem) {
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

TEST_F(SerializationTest, DataSliceImpl) {
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

}  // namespace
}  // namespace koladata
