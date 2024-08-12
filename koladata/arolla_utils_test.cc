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
#include "koladata/arolla_utils.h"

#include <cstdint>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/matchers.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"

namespace arolla {

AROLLA_DECLARE_DENSE_ARRAY_QTYPE(DENSE_ARRAY_OBJECT_ID,
                                 ::koladata::internal::ObjectId);

AROLLA_DEFINE_DENSE_ARRAY_QTYPE(DENSE_ARRAY_OBJECT_ID,
                                ::koladata::internal::ObjectId);

}  // namespace arolla

namespace koladata {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::arolla::CreateFullDenseArray;
using ::arolla::DenseArray;
using ::arolla::Text;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;

internal::DataItem kAnySchema(schema::kAny);

TEST(DataSliceUtils, ToArollaArray) {
  auto values_1 = CreateDenseArray<int>({1, std::nullopt, 3});
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_1), shape));

  ASSERT_OK_AND_ASSIGN(auto arolla_val, DataSliceToArollaValue(ds));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<int>>(),
              ElementsAreArray(values_1));

  ASSERT_OK_AND_ASSIGN(auto arolla_ref, DataSliceToArollaRef(ds));
  EXPECT_THAT(arolla_ref.UnsafeAs<DenseArray<int>>(),
              ElementsAreArray(values_1));

  auto values_2 = CreateDenseArray<Text>(
      {Text("abc"), std::nullopt, Text("xyz")});
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_2), shape));

  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<Text>>(),
              ElementsAreArray(values_2));

  ASSERT_OK_AND_ASSIGN(arolla_ref, DataSliceToArollaRef(ds));
  EXPECT_THAT(arolla_ref.UnsafeAs<DenseArray<Text>>(),
              ElementsAreArray(values_2));

  // Empty array, but not "empty" slice, i.e. it has a typed array inside.
  auto values_3 = CreateDenseArray<int>(
      {std::nullopt, std::nullopt, std::nullopt});
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_3), shape));

  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<int>>(),
              ElementsAreArray(values_3));

  ASSERT_OK_AND_ASSIGN(arolla_ref, DataSliceToArollaRef(ds));
  EXPECT_THAT(arolla_ref.UnsafeAs<DenseArray<int>>(),
              ElementsAreArray(values_3));
}

// NOTE: Empty and unknown slices work only with TypedValue and not TypedRef.
TEST(DataSliceUtils, ToArollaValueEmptyMultidimSlice) {
  auto ds_impl = internal::DataSliceImpl::Builder(3).Build();  // empty slice.
  ASSERT_TRUE(ds_impl.is_empty_and_unknown());
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);

  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::Create(ds_impl, shape, internal::DataItem(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(auto arolla_val, DataSliceToArollaValue(ds));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<int>>(),
              ElementsAre(std::nullopt, std::nullopt, std::nullopt));

  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::Create(ds_impl, shape, internal::DataItem(schema::kFloat32)));
  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<float>>(),
              ElementsAre(std::nullopt, std::nullopt, std::nullopt));

  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(ds_impl, shape, kAnySchema));
  EXPECT_THAT(DataSliceToArollaValue(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));

  // We can pass a fallback schema.
  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(ds_impl, shape, kAnySchema));
  ASSERT_OK_AND_ASSIGN(
      arolla_val,
      DataSliceToArollaValue(
          ds, /*fallback_schema=*/internal::DataItem(schema::kInt32)));
  EXPECT_THAT(arolla_val.UnsafeAs<DenseArray<int>>(),
              ElementsAre(std::nullopt, std::nullopt, std::nullopt));

  // The fallback schema must be a primitive schema.
  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(ds_impl, shape, kAnySchema));
  EXPECT_THAT(DataSliceToArollaValue(ds, /*fallback_schema=*/kAnySchema),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));

  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::Create(
          ds_impl, shape,
          internal::DataItem(internal::AllocateExplicitSchema())));
  EXPECT_THAT(DataSliceToArollaValue(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));
}

TEST(DataSliceUtils, ToArollaValueMixedSlice) {
  auto values_1 = CreateDenseArray<int>({1, std::nullopt, 3});
  auto values_2 = CreateDenseArray<float>({std::nullopt, 2., std::nullopt});
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::Create(internal::DataSliceImpl::Create(values_1, values_2),
                        shape, kAnySchema));

  EXPECT_THAT(DataSliceToArollaValue(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("only DataSlices with primitive values of the "
                                 "same type can be converted to Arolla value, "
                                 "got: MIXED")));

  EXPECT_THAT(DataSliceToArollaRef(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("only DataSlices with primitive values of the "
                                 "same type can be converted to Arolla value, "
                                 "got: MIXED")));
}

TEST(DataSliceUtils, ToArollaValueScalar) {
  auto values_1 = CreateDenseArray<int>({1});
  auto shape = DataSlice::JaggedShape::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_1), shape));

  ASSERT_OK_AND_ASSIGN(auto arolla_val, DataSliceToArollaValue(ds));
  EXPECT_EQ(arolla_val.UnsafeAs<int>(), 1);

  auto values_2 = CreateDenseArray<Text>({Text("abc")});
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_2), shape));

  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_EQ(arolla_val.UnsafeAs<Text>(), Text("abc"));

  // Optional - empty, but typed
  auto values_3 = CreateDenseArray<int>({std::nullopt});
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_3), shape));

  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_EQ(arolla_val.UnsafeAs<arolla::OptionalValue<int>>(),
            arolla::OptionalValue<int>{});

  // Optional - empty, untyped, but with primitive Schema.
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::Create(internal::DataItem(),
                        internal::DataItem(schema::kInt32)));

  ASSERT_OK_AND_ASSIGN(arolla_val, DataSliceToArollaValue(ds));
  EXPECT_EQ(arolla_val.UnsafeAs<arolla::OptionalValue<int>>(),
            arolla::OptionalValue<int>{});

  // Error - empty, untyped, without primitive Schema.
  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(internal::DataItem(), kAnySchema));
  EXPECT_THAT(DataSliceToArollaValue(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));

  // Optional - empty, untyped, without primitive Schema, but with fallback.
  ASSERT_OK_AND_ASSIGN(ds, DataSlice::Create(internal::DataItem(), kAnySchema));
  ASSERT_OK_AND_ASSIGN(
      arolla_val,
      DataSliceToArollaValue(
          ds, /*fallback_schema=*/internal::DataItem(schema::kInt32)));
  EXPECT_EQ(arolla_val.UnsafeAs<arolla::OptionalValue<int>>(),
            arolla::OptionalValue<int>{});
}

TEST(DataSliceUtils, FromDenseArray) {
  auto values = CreateFullDenseArray<int>({1, 2, 3});
  auto typed_value = arolla::TypedValue::FromValue(values);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSliceFromPrimitivesDenseArray(typed_value.AsRef()));
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  EXPECT_EQ(ds.size(), values.size());
  EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int>());

  ASSERT_OK_AND_ASSIGN(auto darray, DataSliceToDenseArray(ds));
  EXPECT_THAT(darray.UnsafeAs<DenseArray<int>>(), ElementsAreArray(values));
}

TEST(DataSliceUtils, FromDenseArrayError) {
  auto object = arolla::TypedValue::FromValue(internal::ObjectId());
  EXPECT_THAT(DataSliceFromPrimitivesDenseArray(object.AsRef()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected DenseArray, but got: OBJECT_ID")));

  auto values = CreateDenseArray<internal::ObjectId>(
      {internal::ObjectId(), std::nullopt});
  auto typed_value = arolla::TypedValue::FromValue(values);
  EXPECT_THAT(DataSliceFromPrimitivesDenseArray(typed_value.AsRef()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported array element type: OBJECT_ID")));
}

TEST(DataSliceUtils, ToEmptyDenseArray) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::Create(internal::DataSliceImpl::Builder(3).Build(), shape,
                        internal::DataItem(schema::kInt32)));
  ASSERT_OK_AND_ASSIGN(auto darray, DataSliceToDenseArray(ds));
  EXPECT_THAT(darray.UnsafeAs<DenseArray<int>>(),
              ElementsAre(std::nullopt, std::nullopt, std::nullopt));

  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::Create(internal::DataSliceImpl::Builder(3).Build(), shape,
                        internal::DataItem(schema::kFloat32)));
  ASSERT_OK_AND_ASSIGN(darray, DataSliceToDenseArray(ds));
  EXPECT_THAT(darray.UnsafeAs<DenseArray<float>>(),
              ElementsAre(std::nullopt, std::nullopt, std::nullopt));
}

TEST(DataSliceUtils, ToDenseArrayError) {
  auto values_1 = CreateDenseArray<int>({1, std::nullopt, 3});
  auto values_2 = CreateDenseArray<float>({std::nullopt, 2., std::nullopt});
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::Create(internal::DataSliceImpl::Create(values_1, values_2),
                        shape, kAnySchema));

  EXPECT_THAT(DataSliceToDenseArray(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("only DataSlices with primitive values of the "
                                 "same type can be converted to Arolla value, "
                                 "got: MIXED")));

  ASSERT_OK_AND_ASSIGN(
      ds, DataSlice::Create(internal::DataSliceImpl::AllocateEmptyObjects(3),
                            shape, kAnySchema));
  EXPECT_THAT(DataSliceToDenseArray(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("unsupported dtype for conversions to "
                                 "Arolla value: ANY")));

  // Empty with ANY schema.
  ASSERT_OK_AND_ASSIGN(
      ds, DataSlice::Create(internal::DataSliceImpl::Builder(3).Build(),
                            shape, kAnySchema));
  EXPECT_THAT(DataSliceToDenseArray(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));

  // Empty with allocated schema.
  ASSERT_OK_AND_ASSIGN(
      ds,
      DataSlice::Create(
          internal::DataSliceImpl::Builder(3).Build(), shape,
          internal::DataItem(internal::AllocateExplicitSchema())));
  EXPECT_THAT(DataSliceToDenseArray(ds),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("empty slices can be converted to Arolla value"
                                 " only if they have primitive schema")));
}

TEST(DataSliceUtils, FromArray) {
  auto values = arolla::CreateArray<int>({1, 2, 3});
  auto typed_value = arolla::TypedValue::FromValue(values);
  ASSERT_OK_AND_ASSIGN(auto ds,
                       DataSliceFromPrimitivesArray(typed_value.AsRef()));
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  EXPECT_EQ(ds.size(), values.size());
  EXPECT_THAT(ds.GetShape(), IsEquivalentTo(shape));
  EXPECT_EQ(ds.dtype(), arolla::GetQType<int>());

  ASSERT_OK_AND_ASSIGN(auto darray, DataSliceToDenseArray(ds));
  EXPECT_THAT(darray.UnsafeAs<DenseArray<int>>(), ElementsAre(1, 2, 3));
}

TEST(DataSliceUtils, FromArrayError) {
  auto object = arolla::TypedValue::FromValue(internal::ObjectId());
  EXPECT_THAT(DataSliceFromPrimitivesArray(object.AsRef()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("expected Arolla Array, but got: OBJECT_ID")));

  auto values = arolla::CreateArray<uint64_t>({1234, std::nullopt});
  auto typed_value = arolla::TypedValue::FromValue(values);
  EXPECT_THAT(DataSliceFromPrimitivesArray(typed_value.AsRef()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported array element type: UINT64")));
}

}  // namespace
}  // namespace koladata
