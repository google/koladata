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
// Basic tests for JSON converter internals. The top-level operator tests are
// in python.

#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/json.h"

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

namespace koladata::ops::json_internal {
namespace {

TEST(JsonInternalTest, JsonBoolToDataItem) {
  EXPECT_THAT(JsonBoolToDataItem(false, internal::DataItem(schema::kBool)),
              IsOkAndHolds(internal::DataItem(false)));
  EXPECT_THAT(JsonBoolToDataItem(true, internal::DataItem(schema::kBool)),
              IsOkAndHolds(internal::DataItem(true)));

  EXPECT_THAT(JsonBoolToDataItem(false, internal::DataItem(schema::kMask)),
              IsOkAndHolds(internal::DataItem()));
  EXPECT_THAT(JsonBoolToDataItem(true, internal::DataItem(schema::kMask)),
              IsOkAndHolds(internal::DataItem(arolla::Unit())));

  EXPECT_THAT(JsonBoolToDataItem(false, internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(false)));
  EXPECT_THAT(JsonBoolToDataItem(true, internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(true)));

  EXPECT_THAT(JsonBoolToDataItem(false, internal::DataItem(schema::kInt32)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonBoolToDataItem(true, internal::DataItem(schema::kInt32)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(1))));
}

TEST(JsonInternalTest, JsonNumberToDataItem_Int64) {
  // T / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kInt32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kInt64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kFloat32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kFloat64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));

  // OBJECT / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(
      JsonNumberToDataItem(static_cast<int64_t>(10000000000LL),
                           internal::DataItem(schema::kObject),
                           internal::DataItem(schema::kObject)),
      IsOkAndHolds(internal::DataItem(static_cast<int64_t>(10000000000LL))));
  EXPECT_THAT(
      JsonNumberToDataItem(static_cast<int64_t>(-10000000000LL),
                           internal::DataItem(schema::kObject),
                           internal::DataItem(schema::kObject)),
      IsOkAndHolds(internal::DataItem(static_cast<int64_t>(-10000000000LL))));

  // OBJECT / T
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kInt32)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kInt64)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat32)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));

  // Out of range
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(10000000000LL),
                                   internal::DataItem(schema::kInt32),
                                   internal::DataItem(schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Invalid schema
  EXPECT_THAT(JsonNumberToDataItem(static_cast<int64_t>(0),
                                   internal::DataItem(schema::kItemId),
                                   internal::DataItem(schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(JsonInternalTest, JsonNumberToDataItem_Uint64) {
  // T / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kInt32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kInt64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kFloat32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kFloat64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));

  // OBJECT / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(
      JsonNumberToDataItem(static_cast<uint64_t>(10000000000LL),
                           internal::DataItem(schema::kObject),
                           internal::DataItem(schema::kObject)),
      IsOkAndHolds(internal::DataItem(static_cast<int64_t>(10000000000LL))));
  EXPECT_THAT(
      JsonNumberToDataItem(static_cast<uint64_t>(-10000000000LL),
                           internal::DataItem(schema::kObject),
                           internal::DataItem(schema::kObject)),
      IsOkAndHolds(internal::DataItem(static_cast<int64_t>(-10000000000LL))));

  // OBJECT / T
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kInt32)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kInt64)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat32)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));

  // Out of range
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(10000000000LL),
                                   internal::DataItem(schema::kInt32),
                                   internal::DataItem(schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // Invalid schema
  EXPECT_THAT(JsonNumberToDataItem(static_cast<uint64_t>(0),
                                   internal::DataItem(schema::kItemId),
                                   internal::DataItem(schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(JsonInternalTest, JsonNumberToDataItem_Double) {
  // T / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kFloat32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kFloat64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kInt32),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kInt64),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kString),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(arolla::Text("0"))));

  // OBJECT / OBJECT
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));

  // OBJECT / T
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat32)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0))));
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kObject),
                                   internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0))));

  // Invalid schema
  EXPECT_THAT(JsonNumberToDataItem(static_cast<double>(0), "0.0",
                                   internal::DataItem(schema::kItemId),
                                   internal::DataItem(schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(JsonInternalTest, JsonStringToDataItem) {
  EXPECT_THAT(JsonStringToDataItem("abc", internal::DataItem(schema::kString)),
              IsOkAndHolds(internal::DataItem(arolla::Text("abc"))));
  EXPECT_THAT(JsonStringToDataItem("abc", internal::DataItem(schema::kObject)),
              IsOkAndHolds(internal::DataItem(arolla::Text("abc"))));

  // FLOAT64 parsing
  EXPECT_THAT(JsonStringToDataItem("0.0", internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0.0))));
  EXPECT_THAT(JsonStringToDataItem("0", internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(internal::DataItem(static_cast<double>(0.0))));
  EXPECT_THAT(
      JsonStringToDataItem("-0.0", internal::DataItem(schema::kFloat64)),
      IsOkAndHolds(internal::DataItem(static_cast<double>(-0.0))));
  EXPECT_THAT(JsonStringToDataItem("inf", internal::DataItem(schema::kFloat64)),
              IsOkAndHolds(
                  internal::DataItem(std::numeric_limits<double>::infinity())));
  EXPECT_THAT(
      JsonStringToDataItem("-inf", internal::DataItem(schema::kFloat64)),
      IsOkAndHolds(
          internal::DataItem(-std::numeric_limits<double>::infinity())));
  {
    ASSERT_OK_AND_ASSIGN(
        auto item,
        JsonStringToDataItem("nan", internal::DataItem(schema::kFloat64)));
    EXPECT_TRUE(std::isnan(item.value<double>()));
  }

  EXPECT_THAT(JsonStringToDataItem("abc", internal::DataItem(schema::kFloat64)),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // FLOAT32 parsing
  EXPECT_THAT(JsonStringToDataItem("0.0", internal::DataItem(schema::kFloat32)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0.0))));
  EXPECT_THAT(JsonStringToDataItem("0", internal::DataItem(schema::kFloat32)),
              IsOkAndHolds(internal::DataItem(static_cast<float>(0.0))));
  EXPECT_THAT(
      JsonStringToDataItem("-0.0", internal::DataItem(schema::kFloat32)),
      IsOkAndHolds(internal::DataItem(static_cast<float>(-0.0))));
  EXPECT_THAT(
      JsonStringToDataItem("inf", internal::DataItem(schema::kFloat32)),
      IsOkAndHolds(internal::DataItem(std::numeric_limits<float>::infinity())));
  EXPECT_THAT(
      JsonStringToDataItem("-inf", internal::DataItem(schema::kFloat32)),
      IsOkAndHolds(
          internal::DataItem(-std::numeric_limits<float>::infinity())));
  {
    ASSERT_OK_AND_ASSIGN(
        auto item,
        JsonStringToDataItem("nan", internal::DataItem(schema::kFloat32)));
    EXPECT_TRUE(std::isnan(item.value<float>()));
  }

  EXPECT_THAT(JsonStringToDataItem("abc", internal::DataItem(schema::kFloat32)),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // INT64 parsing
  EXPECT_THAT(JsonStringToDataItem("0", internal::DataItem(schema::kInt64)),
              IsOkAndHolds(internal::DataItem(static_cast<int64_t>(0))));

  // Note: intentionally does not supporting the normal INT64 wrapping, because
  // strtol does not.
  EXPECT_THAT(JsonStringToDataItem("1000000000000000000000",
                                   internal::DataItem(schema::kInt64)),
              StatusIs(absl::StatusCode::kInvalidArgument));

  // INT32 parsing
  EXPECT_THAT(JsonStringToDataItem("0", internal::DataItem(schema::kInt32)),
              IsOkAndHolds(internal::DataItem(static_cast<int32_t>(0))));

  // Note: intentionally does not supporting the normal INT64 wrapping, because
  // strtol does not.
  EXPECT_THAT(
      JsonStringToDataItem("10000000000", internal::DataItem(schema::kInt32)),
      StatusIs(absl::StatusCode::kInvalidArgument));

  // BYTES (base64) parsing
  EXPECT_THAT(
      JsonStringToDataItem("YWJjZA==", internal::DataItem(schema::kBytes)),
      IsOkAndHolds(internal::DataItem(arolla::Bytes("abcd"))));
  EXPECT_THAT(
      JsonStringToDataItem("YWJjZA", internal::DataItem(schema::kBytes)),
      IsOkAndHolds(internal::DataItem(arolla::Bytes("abcd"))));
  EXPECT_THAT(JsonStringToDataItem("$$$", internal::DataItem(schema::kBytes)),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// Just simple tests, actual coverage will come from python layer where it is
// easier to compare Koda DataBag contents.

TEST(JsonInternalTest, JsonArrayToList) {
  {
    auto bag = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        auto list,
        JsonArrayToList({internal::DataItem(1), internal::DataItem(2),
                         internal::DataItem(3)},
                        internal::DataItem(schema::kObject),
                        internal::DataItem(schema::kObject), true, bag));
    EXPECT_TRUE(list.is_list());
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(list, internal::DataItem(schema::kObject), bag)
            ->ExplodeList(0, std::nullopt));
    EXPECT_THAT(
        slice.slice().AsDataItemDenseArray(),
        testing::ElementsAre(internal::DataItem(1), internal::DataItem(2),
                             internal::DataItem(3)));
  }

  {
    auto bag = DataBag::Empty();
    auto item_schema = *DataSlice::Create(internal::DataItem(schema::kInt32),
                                          internal::DataItem(schema::kSchema));
    auto list_schema = *CreateListSchema(bag, item_schema);
    auto list = *JsonArrayToList(
        {internal::DataItem(1), internal::DataItem(2), internal::DataItem(3)},
        list_schema.item(), item_schema.item(), true, bag);
    EXPECT_TRUE(list.is_list());
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(list, internal::DataItem(schema::kObject), bag)
            ->ExplodeList(0, std::nullopt));
    EXPECT_THAT(
        slice.slice().AsDataItemDenseArray(),
        testing::ElementsAre(internal::DataItem(1), internal::DataItem(2),
                             internal::DataItem(3)));
  }
}

TEST(JsonInternalTest, JsonObjectToDict) {
  auto bag = DataBag::Empty();
  auto dict_schema = *CreateDictSchema(
      bag,
      *DataSlice::Create(internal::DataItem(schema::kString),
                         internal::DataItem(schema::kSchema)),
      *DataSlice::Create(internal::DataItem(schema::kInt32),
                         internal::DataItem(schema::kSchema)));
  ASSERT_OK_AND_ASSIGN(
      auto dict, JsonObjectToDict({"a", "b", "c"},
                                  {internal::DataItem(1), internal::DataItem(2),
                                   internal::DataItem(3)},
                                  dict_schema.item(), true, bag));
  EXPECT_TRUE(dict.is_dict());
}

TEST(JsonInternalTest, JsonObjectToEntity) {
  {
    auto bag = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        auto entity,
        JsonObjectToEntity({"a", "b", "c"},
                           {internal::DataItem(1), internal::DataItem(2),
                            internal::DataItem(3)},
                           internal::DataItem(schema::kObject), std::nullopt,
                           std::nullopt, true, bag));
    EXPECT_TRUE(entity.is_entity());
  }

  {
    auto bag = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        auto entity, JsonObjectToEntity(
                         {"a", "b", "c"},
                         {internal::DataItem(1), internal::DataItem(2),
                          internal::DataItem(3)},
                         internal::DataItem(schema::kObject),
                         "json_object_keys", "json_object_values", true, bag));
    EXPECT_TRUE(entity.is_entity());
  }

  {
    auto bag = DataBag::Empty();
    auto entity_schema = *CreateEntitySchema(
        bag, {"a", "b", "c"},
        {*DataSlice::Create(internal::DataItem(schema::kInt32),
                            internal::DataItem(schema::kSchema)),
         *DataSlice::Create(internal::DataItem(schema::kInt32),
                            internal::DataItem(schema::kSchema)),
         *DataSlice::Create(internal::DataItem(schema::kInt32),
                            internal::DataItem(schema::kSchema))});
    ASSERT_OK_AND_ASSIGN(
        auto entity,
        JsonObjectToEntity({"a", "b", "c"},
                           {internal::DataItem(1), internal::DataItem(2),
                            internal::DataItem(3)},
                           entity_schema.item(), std::nullopt, std::nullopt,
                           true, bag));
    EXPECT_TRUE(entity.is_entity());
  }

  {
    auto bag = DataBag::Empty();
    auto entity_schema = *CreateEntitySchema(
        bag, {"a", "b", "c", "json_object_keys", "json_object_values"},
        {
            *DataSlice::Create(internal::DataItem(schema::kInt32),
                               internal::DataItem(schema::kSchema)),
            *DataSlice::Create(internal::DataItem(schema::kInt32),
                               internal::DataItem(schema::kSchema)),
            *DataSlice::Create(internal::DataItem(schema::kInt32),
                               internal::DataItem(schema::kSchema)),
            *CreateListSchema(
                bag, *DataSlice::Create(internal::DataItem(schema::kString),
                                        internal::DataItem(schema::kSchema))),
            *CreateListSchema(
                bag, *DataSlice::Create(internal::DataItem(schema::kObject),
                                        internal::DataItem(schema::kSchema))),
        });
    ASSERT_OK_AND_ASSIGN(
        auto entity,
        JsonObjectToEntity({"a", "b", "c"},
                           {internal::DataItem(1), internal::DataItem(2),
                            internal::DataItem(3)},
                           entity_schema.item(), "json_object_keys",
                           "json_object_values", true, bag));
    EXPECT_TRUE(entity.is_entity());
  }
}

}  // namespace
}  // namespace koladata::ops::json_internal
