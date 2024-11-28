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

#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::internal::ObjectId;
using ::koladata::internal::testing::DataBagEqual;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsSupersetOf;

// Returns the set of DTypes that implicitly casts to `dtype`.
const absl::flat_hash_set<schema::DType>& GetDTypesCastableTo(
    schema::DType dtype) {
  static absl::NoDestructor<
      absl::flat_hash_map<schema::DType, absl::flat_hash_set<schema::DType>>>
      lower_bounds([] {
        absl::flat_hash_map<schema::DType, absl::flat_hash_set<schema::DType>>
            lower_bounds;
        arolla::meta::foreach_type(
            schema::supported_dtype_values(), [&](auto tpe1) {
              using T1 = typename decltype(tpe1)::type;
              auto dtype_1 = schema::GetDType<T1>();
              arolla::meta::foreach_type(
                  schema::supported_dtype_values(), [&](auto tpe2) {
                    using T2 = typename decltype(tpe2)::type;
                    auto dtype_2 = schema::GetDType<T2>();
                    auto maybe_schema =
                        schema::CommonSchema(internal::DataItem(dtype_1),
                                             internal::DataItem(dtype_2));
                    if (maybe_schema.ok() &&
                        *maybe_schema == internal::DataItem(dtype_1)) {
                      lower_bounds[dtype_1].insert(dtype_2);
                    }
                  });
            });
        return lower_bounds;
      }());
  return lower_bounds->at(dtype);
};

struct CastingTestCase {
  DataSlice input;
  DataSlice output;
};

// Asserts that all DTypes that can be implicitly cast to `dtype` are tested.
void AssertLowerBoundDTypesAreTested(
    schema::DType dtype, const std::vector<CastingTestCase>& test_cases) {
  absl::flat_hash_set<schema::DType> tested_slice_dtypes;
  absl::flat_hash_set<schema::DType> tested_item_dtypes;
  for (const auto& test_case : test_cases) {
    auto schema = test_case.input.GetSchemaImpl();
    if (!schema.holds_value<schema::DType>()) {
      continue;
    }
    auto dtype = schema.value<schema::DType>();
    if (test_case.input.is_item()) {
      tested_item_dtypes.insert(dtype);
    } else {
      tested_slice_dtypes.insert(dtype);
    }
  }
  ASSERT_THAT(tested_slice_dtypes, IsSupersetOf(GetDTypesCastableTo(dtype)));
  ASSERT_THAT(tested_item_dtypes, IsSupersetOf(GetDTypesCastableTo(dtype)));
}

using CastingToInt32Test = ::testing::TestWithParam<CastingTestCase>;
using CastingToInt64Test = ::testing::TestWithParam<CastingTestCase>;
using CastingToFloat32Test = ::testing::TestWithParam<CastingTestCase>;
using CastingToFloat64Test = ::testing::TestWithParam<CastingTestCase>;
using CastingToNoneTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToExprTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToStrTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToBytesTest = ::testing::TestWithParam<CastingTestCase>;
using CastingDecodeTest = ::testing::TestWithParam<CastingTestCase>;
using CastingEncodeTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToMaskTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToBoolTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToAnyTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToItemIdTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToEntityTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToSchemaTest = ::testing::TestWithParam<CastingTestCase>;
using CastingToObjectTest = ::testing::TestWithParam<CastingTestCase>;

TEST_P(CastingToInt32Test, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto int32_slice, ToInt32(input));
  EXPECT_THAT(int32_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToInt32TestSuite, CastingToInt32Test, ::testing::ValuesIn([] {
      arolla::InitArolla();
      auto int32_slice =
          test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kInt32)},
          {int32_slice, int32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kInt64),
           int32_slice},
          {test::DataSlice<float>({1.0f, 2.0f, std::nullopt}, schema::kFloat32),
           int32_slice},
          {test::DataSlice<double>({1.0, 2.0, std::nullopt}, schema::kFloat64),
           int32_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool),
           test::DataSlice<int>({1, 0, std::nullopt}, schema::kInt32)},
          {test::DataSlice<arolla::Text>({"1", "2", std::nullopt},
                                         schema::kString),
           int32_slice},
          {test::DataSlice<arolla::Bytes>({"1", "2", std::nullopt},
                                          schema::kBytes),
           int32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kObject),
           int32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kAny),
           int32_slice},
          {test::MixedDataSlice<int, int64_t>({1, std::nullopt, std::nullopt},
                                              {std::nullopt, 2, std::nullopt}),
           int32_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kInt32)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(arolla::Text("1"), schema::kString),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(arolla::Bytes("1"), schema::kBytes),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(1, schema::kObject),
           test::DataItem(1, schema::kInt32)},
          {test::DataItem(1, schema::kAny), test::DataItem(1, schema::kInt32)},
      };
      AssertLowerBoundDTypesAreTested(schema::kInt32, test_cases);
      return test_cases;
    }()));

TEST(Casting, Int32Errors) {
  EXPECT_THAT(
      ToInt32(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                            schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToInt32(test::MixedDataSlice<int64_t, arolla::Unit>(
                  {1, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT32"));
  EXPECT_THAT(
      ToInt32(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToInt32(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT32"));
  EXPECT_THAT(ToInt32(test::DataItem(arolla::Text("1.5"), schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unable to parse INT32: 1.5"));
}

TEST_P(CastingToInt64Test, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto int64_slice, ToInt64(input));
  EXPECT_THAT(int64_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToInt64TestSuite, CastingToInt64Test, ::testing::ValuesIn([] {
      auto int64_slice =
          test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kInt64);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kInt64)},
          {test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32),
           int64_slice},
          {int64_slice, int64_slice},
          {test::DataSlice<float>({1.0f, 2.0f, std::nullopt}, schema::kFloat32),
           int64_slice},
          {test::DataSlice<double>({1.0, 2.0, std::nullopt}, schema::kFloat64),
           int64_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool),
           test::DataSlice<int64_t>({1, 0, std::nullopt}, schema::kInt64)},
          {test::DataSlice<arolla::Text>({"1", "2", std::nullopt},
                                         schema::kString),
           int64_slice},
          {test::DataSlice<arolla::Bytes>({"1", "2", std::nullopt},
                                          schema::kBytes),
           int64_slice},
          {test::DataSlice<int>({1, 2, std::nullopt}, schema::kObject),
           int64_slice},
          {test::DataSlice<int>({1, 2, std::nullopt}, schema::kAny),
           int64_slice},
          {test::MixedDataSlice<int, int64_t>({1, std::nullopt, std::nullopt},
                                              {std::nullopt, 2, std::nullopt}),
           int64_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kInt64)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(arolla::Text("1"), schema::kString),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(arolla::Bytes("1"), schema::kBytes),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(1, schema::kObject),
           test::DataItem(int64_t{1}, schema::kInt64)},
          {test::DataItem(1, schema::kAny),
           test::DataItem(int64_t{1}, schema::kInt64)},
      };
      AssertLowerBoundDTypesAreTested(schema::kInt64, test_cases);
      return test_cases;
    }()));

TEST(Casting, Int64Errors) {
  EXPECT_THAT(
      ToInt64(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                            schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToInt64(test::MixedDataSlice<int64_t, arolla::Unit>(
                  {1, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT64"));
  EXPECT_THAT(
      ToInt64(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToInt64(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to INT64"));
  EXPECT_THAT(ToInt64(test::DataItem(arolla::Text("1.5"), schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unable to parse INT64: 1.5"));
}

TEST_P(CastingToFloat32Test, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto float32_slice, ToFloat32(input));
  EXPECT_THAT(float32_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToFloat32TestSuite, CastingToFloat32Test, ::testing::ValuesIn([] {
      auto float32_slice =
          test::DataSlice<float>({1.0f, 2.0f, std::nullopt}, schema::kFloat32);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kFloat32)},
          {test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32),
           float32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kInt64),
           float32_slice},
          {float32_slice, float32_slice},
          {test::DataSlice<double>({1.0, 2.0, std::nullopt}, schema::kFloat64),
           float32_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool),
           test::DataSlice<float>({1.0f, 0.0f, std::nullopt},
                                  schema::kFloat32)},
          {test::DataSlice<arolla::Text>({"1.0", "2", std::nullopt},
                                         schema::kString),
           float32_slice},
          {test::DataSlice<arolla::Bytes>({"1.0", "2", std::nullopt},
                                          schema::kBytes),
           float32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kObject),
           float32_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kAny),
           float32_slice},
          {test::MixedDataSlice<int, float>({1, std::nullopt, std::nullopt},
                                            {std::nullopt, 2.0f, std::nullopt}),
           float32_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kFloat32)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(arolla::Text("1.0"), schema::kString),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(arolla::Bytes("1.0"), schema::kBytes),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(1, schema::kObject),
           test::DataItem(1.0f, schema::kFloat32)},
          {test::DataItem(1, schema::kAny),
           test::DataItem(1.0f, schema::kFloat32)},
      };
      AssertLowerBoundDTypesAreTested(schema::kFloat32, test_cases);
      return test_cases;
    }()));

TEST(Casting, Float32Errors) {
  EXPECT_THAT(
      ToFloat32(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                              schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToFloat32(test::MixedDataSlice<int64_t, arolla::Unit>(
                  {1, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT32"));
  EXPECT_THAT(
      ToFloat32(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToFloat32(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT32"));
  EXPECT_THAT(ToFloat32(test::DataItem(arolla::Text("foo"), schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unable to parse FLOAT32: foo"));
}

TEST_P(CastingToFloat64Test, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto float64_slice, ToFloat64(input));
  EXPECT_THAT(float64_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToFloat64TestSuite, CastingToFloat64Test, ::testing::ValuesIn([] {
      auto float64_slice =
          test::DataSlice<double>({1.0, 2.0, std::nullopt}, schema::kFloat64);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kFloat64)},
          {test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32),
           float64_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kInt64),
           float64_slice},
          {test::DataSlice<float>({1.0f, 2.0f, std::nullopt}, schema::kFloat32),
           float64_slice},
          {float64_slice, float64_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool),
           test::DataSlice<double>({1.0, 0.0, std::nullopt}, schema::kFloat64)},
          {test::DataSlice<arolla::Text>({"1.0", "2", std::nullopt},
                                         schema::kString),
           float64_slice},
          {test::DataSlice<arolla::Bytes>({"1.0", "2", std::nullopt},
                                          schema::kBytes),
           float64_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kObject),
           float64_slice},
          {test::DataSlice<int64_t>({1, 2, std::nullopt}, schema::kAny),
           float64_slice},
          {test::MixedDataSlice<int, double>({1, std::nullopt, std::nullopt},
                                             {std::nullopt, 2.0, std::nullopt}),
           float64_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kFloat64)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(arolla::Text("1.0"), schema::kString),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(arolla::Bytes("1.0"), schema::kBytes),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(1, schema::kObject),
           test::DataItem(1.0, schema::kFloat64)},
          {test::DataItem(1, schema::kAny),
           test::DataItem(1.0, schema::kFloat64)},
      };
      AssertLowerBoundDTypesAreTested(schema::kFloat64, test_cases);
      return test_cases;
    }()));

TEST(Casting, Float64Errors) {
  EXPECT_THAT(
      ToFloat64(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                              schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToFloat64(test::MixedDataSlice<int64_t, arolla::Unit>(
                  {1, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT64"));
  EXPECT_THAT(
      ToFloat64(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToFloat64(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to FLOAT64"));
  EXPECT_THAT(ToFloat64(test::DataItem(arolla::Text("foo"), schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unable to parse FLOAT64: foo"));
}

TEST_P(CastingToNoneTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto none_slice, ToNone(input));
  EXPECT_THAT(none_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToNoneTestSuite, CastingToNoneTest, ::testing::ValuesIn([] {
      auto none_slice = test::EmptyDataSlice(3, schema::kNone);
      auto none_item = test::DataItem(std::nullopt, schema::kNone);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {none_slice, none_slice},
          {test::EmptyDataSlice(3, schema::kInt32), none_slice},
          {test::EmptyDataSlice(3, schema::kInt64), none_slice},
          {test::EmptyDataSlice(3, schema::kFloat32), none_slice},
          {test::EmptyDataSlice(3, schema::kFloat64), none_slice},
          {test::EmptyDataSlice(3, schema::kItemId), none_slice},
          {test::EmptyDataSlice(3, schema::kSchema), none_slice},
          {test::EmptyDataSlice(3, schema::kObject), none_slice},
          {test::EmptyDataSlice(3, schema::kMask), none_slice},
          {test::EmptyDataSlice(3, schema::kBool), none_slice},
          {test::EmptyDataSlice(3, schema::kBytes), none_slice},
          {test::EmptyDataSlice(3, schema::kString), none_slice},
          {test::EmptyDataSlice(3, schema::kExpr), none_slice},
          {test::EmptyDataSlice(3, schema::kAny), none_slice},
          {test::EmptyDataSlice(3, internal::AllocateExplicitSchema()),
           none_slice},
          // DataItem cases.
          {none_item, none_item},
          {test::DataItem(std::nullopt, schema::kInt32), none_item},
          {test::DataItem(std::nullopt, schema::kInt64), none_item},
          {test::DataItem(std::nullopt, schema::kFloat32), none_item},
          {test::DataItem(std::nullopt, schema::kFloat64), none_item},
          {test::DataItem(std::nullopt, schema::kItemId), none_item},
          {test::DataItem(std::nullopt, schema::kSchema), none_item},
          {test::DataItem(std::nullopt, schema::kObject), none_item},
          {test::DataItem(std::nullopt, schema::kMask), none_item},
          {test::DataItem(std::nullopt, schema::kBool), none_item},
          {test::DataItem(std::nullopt, schema::kBytes), none_item},
          {test::DataItem(std::nullopt, schema::kString), none_item},
          {test::DataItem(std::nullopt, schema::kExpr), none_item},
          {test::DataItem(std::nullopt, schema::kAny), none_item},
          {test::DataItem(std::nullopt, internal::AllocateExplicitSchema()),
           none_item},
      };
      AssertLowerBoundDTypesAreTested(schema::kNone, test_cases);
      return test_cases;
    }()));

TEST(Casting, NoneErrors) {
  EXPECT_THAT(ToNone(test::DataSlice<arolla::Unit>(
                  {arolla::kUnit, std::nullopt}, schema::kMask)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "only empty slices can be converted to NONE"));
  EXPECT_THAT(ToNone(test::DataItem(arolla::kUnit, schema::kMask)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "only missing values can be converted to NONE"));
}

TEST_P(CastingToExprTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto expr_slice, ToExpr(input));
  EXPECT_THAT(expr_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToExprTestSuite, CastingToExprTest, ::testing::ValuesIn([] {
      auto x = arolla::expr::ExprQuote(arolla::expr::Leaf("x"));
      auto expr_slice = test::DataSlice<arolla::expr::ExprQuote>(
          {x, std::nullopt}, schema::kExpr);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kExpr)},
          {expr_slice, expr_slice},
          {test::DataSlice<arolla::expr::ExprQuote>({x, std::nullopt},
                                                    schema::kObject),
           expr_slice},
          {test::DataSlice<arolla::expr::ExprQuote>({x, std::nullopt},
                                                    schema::kAny),
           expr_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kExpr)},
          {test::DataItem(x, schema::kExpr), test::DataItem(x, schema::kExpr)},
          {test::DataItem(x, schema::kObject),
           test::DataItem(x, schema::kExpr)},
          {test::DataItem(x, schema::kAny), test::DataItem(x, schema::kExpr)},
      };
      AssertLowerBoundDTypesAreTested(schema::kExpr, test_cases);
      return test_cases;
    }()));

TEST(Casting, ExprErrors) {
  auto x = arolla::expr::ExprQuote(arolla::expr::Leaf("x"));
  EXPECT_THAT(
      ToExpr(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                           schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(
      ToExpr(test::MixedDataSlice<arolla::expr::ExprQuote, arolla::Unit>(
          {x, std::nullopt, std::nullopt},
          {std::nullopt, arolla::kUnit, std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to EXPR"));
  EXPECT_THAT(
      ToExpr(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(
      ToExpr(test::DataItem(arolla::kUnit, schema::kObject)),
      StatusIs(absl::StatusCode::kInvalidArgument, "cannot cast MASK to EXPR"));
}

TEST_P(CastingToStrTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto text_slice, ToStr(input));
  EXPECT_THAT(text_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToStrTestSuite, CastingToStrTest, ::testing::ValuesIn([] {
      auto text_slice = test::DataSlice<arolla::Text>(
          {"fo\0o", "bar", std::nullopt}, schema::kString);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kString)},
          {text_slice, text_slice},
          {test::DataSlice<int>({1, 2, 3}, schema::kInt32),
           test::DataSlice<arolla::Text>({"1", "2", "3"}, schema::kString)},
          {test::DataSlice<int64_t>({1, 2, 3}, schema::kInt64),
           test::DataSlice<arolla::Text>({"1", "2", "3"}, schema::kString)},
          {test::DataSlice<float>({1.5f, 2.5f, 3.5f}, schema::kFloat32),
           test::DataSlice<arolla::Text>({"1.5", "2.5", "3.5"},
                                         schema::kString)},
          {test::DataSlice<double>({1.5, 2.5, 3.5}, schema::kFloat64),
           test::DataSlice<arolla::Text>({"1.5", "2.5", "3.5"},
                                         schema::kString)},
          {test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kMask),
           test::DataSlice<arolla::Text>({"present", std::nullopt},
                                         schema::kString)},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool),
           test::DataSlice<arolla::Text>({"true", "false", std::nullopt},
                                         schema::kString)},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kBytes),
           test::DataSlice<arolla::Text>({"b'fo'", "b'bar'", std::nullopt},
                                         schema::kString)},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kObject),
           test::DataSlice<arolla::Text>({"b'fo'", "b'bar'", std::nullopt},
                                         schema::kString)},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kAny),
           test::DataSlice<arolla::Text>({"b'fo'", "b'bar'", std::nullopt},
                                         schema::kString)},
          {test::MixedDataSlice<arolla::Text, arolla::Bytes>(
               {"fo\0o", std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}),
           test::DataSlice<arolla::Text>({"fo\0o", "b'bar'", std::nullopt},
                                         schema::kString)},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kString)},
          {test::DataItem("fo\0o", schema::kString),
           test::DataItem("fo\0o", schema::kString)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem("1", schema::kString)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem("1", schema::kString)},
          {test::DataItem(1.5f, schema::kFloat32),
           test::DataItem("1.5", schema::kString)},
          {test::DataItem(1.5, schema::kFloat64),
           test::DataItem("1.5", schema::kString)},
          {test::DataItem(arolla::kUnit, schema::kMask),
           test::DataItem("present", schema::kString)},
          {test::DataItem(true, schema::kBool),
           test::DataItem("true", schema::kString)},
          {test::DataItem("fo\0o", schema::kObject),
           test::DataItem("fo\0o", schema::kString)},
          {test::DataItem("fo\0o", schema::kAny),
           test::DataItem("fo\0o", schema::kString)},
      };
      AssertLowerBoundDTypesAreTested(schema::kString, test_cases);
      return test_cases;
    }()));

TEST(Casting, TextErrors) {
  EXPECT_THAT(ToStr(test::DataSlice<internal::ObjectId>(
                  {std::nullopt, std::nullopt}, schema::kItemId)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: ITEMID"));
  EXPECT_THAT(
      ToStr(test::MixedDataSlice<arolla::Text, internal::ObjectId>(
          {"foo", std::nullopt, std::nullopt},
          {std::nullopt, internal::AllocateSingleObject(), std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast ITEMID to STRING"));
  EXPECT_THAT(ToStr(test::DataItem(std::nullopt, schema::kItemId)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: ITEMID"));
  EXPECT_THAT(
      ToStr(test::DataItem(internal::AllocateSingleObject(), schema::kObject)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast ITEMID to STRING"));
}

TEST_P(CastingToBytesTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto text_slice, ToBytes(input));
  EXPECT_THAT(text_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToBytesTestSuite, CastingToBytesTest, ::testing::ValuesIn([] {
      auto bytes_slice = test::DataSlice<arolla::Bytes>(
          {"fo\0o", "bar", std::nullopt}, schema::kBytes);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kBytes)},
          {bytes_slice, bytes_slice},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kObject),
           bytes_slice},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kAny),
           bytes_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kObject),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kAny),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
      };
      AssertLowerBoundDTypesAreTested(schema::kBytes, test_cases);
      return test_cases;
    }()));

TEST(Casting, BytesErrors) {
  EXPECT_THAT(
      ToBytes(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                            schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToBytes(test::MixedDataSlice<arolla::Bytes, arolla::Unit>(
                  {"foo", std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BYTES"));
  EXPECT_THAT(
      ToBytes(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToBytes(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BYTES"));
}

TEST_P(CastingDecodeTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto text_slice, Decode(input));
  EXPECT_THAT(text_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingDecodeTestSuite, CastingDecodeTest, ::testing::ValuesIn([] {
      auto text_slice = test::DataSlice<arolla::Text>(
          {"fo\0o", "bar", std::nullopt}, schema::kString);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kString)},
          {text_slice, text_slice},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kBytes),
           text_slice},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kObject),
           text_slice},
          {test::DataSlice<arolla::Bytes>({"fo\0o", "bar", std::nullopt},
                                          schema::kAny),
           text_slice},
          {test::MixedDataSlice<arolla::Text, arolla::Bytes>(
               {"fo\0o", std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}),
           text_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kString)},
          {test::DataItem("fo\0o", schema::kString),
           test::DataItem("fo\0o", schema::kString)},
          {test::DataItem("fo\0o", schema::kObject),
           test::DataItem("fo\0o", schema::kString)},
          {test::DataItem("fo\0o", schema::kAny),
           test::DataItem("fo\0o", schema::kString)},
      };
      AssertLowerBoundDTypesAreTested(schema::kString, test_cases);
      return test_cases;
    }()));

TEST(Casting, DecodeErrors) {
  EXPECT_THAT(
      Decode(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                           schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(Decode(test::MixedDataSlice<arolla::Text, arolla::Unit>(
                  {"foo", std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to STRING"));
  EXPECT_THAT(Decode(test::MixedDataSlice<arolla::Text, arolla::Bytes>(
                  {"foo", std::nullopt, std::nullopt},
                  {std::nullopt, "te\xC0\0xt", std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid UTF-8 sequence at position 2"));
  EXPECT_THAT(
      Decode(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(Decode(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to STRING"));
  EXPECT_THAT(
      Decode(test::DataItem(arolla::Bytes("te\xC0\0xt"), schema::kBytes)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "invalid UTF-8 sequence at position 2"));
}

TEST_P(CastingEncodeTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto bytes_slice, Encode(input));
  EXPECT_THAT(bytes_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingEncodeTestSuite, CastingEncodeTest, ::testing::ValuesIn([] {
      auto bytes_slice = test::DataSlice<arolla::Bytes>(
          {"fo\0o", "bar", std::nullopt}, schema::kBytes);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kBytes)},
          {bytes_slice, bytes_slice},
          {test::DataSlice<arolla::Text>({"fo\0o", "bar", std::nullopt},
                                         schema::kString),
           bytes_slice},
          {test::DataSlice<arolla::Text>({"fo\0o", "bar", std::nullopt},
                                         schema::kObject),
           bytes_slice},
          {test::DataSlice<arolla::Text>({"fo\0o", "bar", std::nullopt},
                                         schema::kAny),
           bytes_slice},
          {test::MixedDataSlice<arolla::Bytes, arolla::Text>(
               {"fo\0o", std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}),
           bytes_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kObject),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
          {test::DataItem(arolla::Bytes("fo\0o"), schema::kAny),
           test::DataItem(arolla::Bytes("fo\0o"), schema::kBytes)},
      };
      AssertLowerBoundDTypesAreTested(schema::kBytes, test_cases);
      return test_cases;
    }()));

TEST(Casting, EncodeErrors) {
  EXPECT_THAT(
      Encode(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                           schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(Encode(test::MixedDataSlice<arolla::Text, arolla::Unit>(
                  {"foo", std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BYTES"));
  EXPECT_THAT(
      Encode(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(Encode(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to BYTES"));
}

TEST_P(CastingToMaskTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto mask_slice, ToMask(input));
  EXPECT_THAT(mask_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToMaskTestSuite, CastingToMaskTest, ::testing::ValuesIn([] {
      auto mask_slice = test::DataSlice<arolla::Unit>(
          {arolla::kUnit, std::nullopt}, schema::kMask);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kMask)},
          {mask_slice, mask_slice},
          {test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kObject),
           mask_slice},
          {test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kAny),
           mask_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kMask)},
          {test::DataItem(arolla::kUnit, schema::kMask),
           test::DataItem(arolla::kUnit, schema::kMask)},
          {test::DataItem(arolla::kUnit, schema::kObject),
           test::DataItem(arolla::kUnit, schema::kMask)},
          {test::DataItem(arolla::kUnit, schema::kAny),
           test::DataItem(arolla::kUnit, schema::kMask)},
      };
      AssertLowerBoundDTypesAreTested(schema::kMask, test_cases);
      return test_cases;
    }()));

TEST(Casting, MaskErrors) {
  EXPECT_THAT(ToMask(test::DataSlice<int>({1, std::nullopt}, schema::kInt32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: INT32"));
  EXPECT_THAT(ToMask(test::MixedDataSlice<int64_t, arolla::Unit>(
                  {1, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast INT64 to MASK"));
  EXPECT_THAT(ToMask(test::DataItem(1, schema::kInt32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: INT32"));
  EXPECT_THAT(ToMask(test::DataItem(1, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast INT32 to MASK"));
}

TEST_P(CastingToBoolTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto bool_slice, ToBool(input));
  EXPECT_THAT(bool_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToBoolTestSuite, CastingToBoolTest, ::testing::ValuesIn([] {
      auto bool_slice =
          test::DataSlice<bool>({true, false, std::nullopt}, schema::kBool);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kBool)},
          {bool_slice, bool_slice},
          {test::DataSlice<int>({1, 0, std::nullopt}, schema::kInt32),
           bool_slice},
          {test::DataSlice<int64_t>({int64_t{1}, int64_t{0}, std::nullopt},
                                    schema::kInt64),
           bool_slice},
          {test::DataSlice<float>({1.0f, 0.0f, std::nullopt}, schema::kFloat32),
           bool_slice},
          {test::DataSlice<double>({1.0, 0.0, std::nullopt}, schema::kFloat64),
           bool_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kObject),
           bool_slice},
          {test::DataSlice<bool>({true, false, std::nullopt}, schema::kAny),
           bool_slice},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kBool)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(true, schema::kObject),
           test::DataItem(true, schema::kBool)},
          {test::DataItem(true, schema::kAny),
           test::DataItem(true, schema::kBool)},
      };
      AssertLowerBoundDTypesAreTested(schema::kBool, test_cases);
      return test_cases;
    }()));

TEST(Casting, BoolErrors) {
  EXPECT_THAT(ToBool(test::DataSlice<arolla::Text>({"foo", std::nullopt},
                                                   schema::kString)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: STRING"));
  EXPECT_THAT(ToBool(test::MixedDataSlice<arolla::Text, bool>(
                  {"foo", std::nullopt, std::nullopt},
                  {std::nullopt, true, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast STRING to BOOLEAN"));
  EXPECT_THAT(ToBool(test::DataItem(arolla::Text("foo"), schema::kString)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "unsupported schema: STRING"));
  EXPECT_THAT(ToBool(test::DataItem(arolla::Text("foo"), schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast STRING to BOOLEAN"));
}

TEST_P(CastingToAnyTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto any_slice, ToAny(input));
  EXPECT_THAT(any_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToAnyTestSuite, CastingToAnyTest, ::testing::ValuesIn([] {
      auto empty_objects = internal::DataSliceImpl::AllocateEmptyObjects(3);
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kAny)},
          {test::DataSlice<int>({1, std::nullopt}, schema::kInt32),
           test::DataSlice<int>({1, std::nullopt}, schema::kAny)},
          {test::DataSlice<int64_t>({1, std::nullopt}, schema::kInt64),
           test::DataSlice<int64_t>({1, std::nullopt}, schema::kAny)},
          {test::DataSlice<float>({1.0f, std::nullopt}, schema::kFloat32),
           test::DataSlice<float>({1.0f, std::nullopt}, schema::kAny)},
          {test::DataSlice<double>({1.0, std::nullopt}, schema::kFloat64),
           test::DataSlice<double>({1.0, std::nullopt}, schema::kAny)},
          {test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kMask),
           test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kAny)},
          {test::DataSlice<bool>({true, std::nullopt}, schema::kBool),
           test::DataSlice<bool>({true, std::nullopt}, schema::kAny)},
          {test::DataSlice<arolla::Bytes>({"foo", std::nullopt},
                                          schema::kBytes),
           test::DataSlice<arolla::Bytes>({"foo", std::nullopt}, schema::kAny)},
          {test::DataSlice<arolla::Text>({"foo", std::nullopt},
                                         schema::kString),
           test::DataSlice<arolla::Text>({"foo", std::nullopt}, schema::kAny)},
          {test::DataSlice<arolla::expr::ExprQuote>(
               {arolla::expr::ExprQuote(arolla::expr::Leaf("x")), std::nullopt},
               schema::kExpr),
           test::DataSlice<arolla::expr::ExprQuote>(
               {arolla::expr::ExprQuote(arolla::expr::Leaf("x")), std::nullopt},
               schema::kAny)},
          {test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema),
           test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kAny)},
          {*DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kItemId)),
           *DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kAny))},
          {test::MixedDataSlice<int, arolla::Text>(
               {1, std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}),
           test::MixedDataSlice<int, arolla::Text>(
               {1, std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}, schema::kAny)},
          {test::DataSlice<int>({1, std::nullopt}, schema::kAny),
           test::DataSlice<int>({1, std::nullopt}, schema::kAny)},
          {*DataSlice::Create(
               empty_objects, DataSlice::JaggedShape::FlatFromSize(3),
               internal::DataItem(internal::AllocateExplicitSchema())),
           *DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kAny))},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kAny)},
          {test::DataItem(1, schema::kInt32), test::DataItem(1, schema::kAny)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(int64_t{1}, schema::kAny)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(1.0f, schema::kAny)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(1.0, schema::kAny)},
          {test::DataItem(arolla::kUnit, schema::kMask),
           test::DataItem(arolla::kUnit, schema::kAny)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(true, schema::kAny)},
          {test::DataItem(arolla::Bytes("foo"), schema::kBytes),
           test::DataItem(arolla::Bytes("foo"), schema::kAny)},
          {test::DataItem(arolla::Text("foo"), schema::kString),
           test::DataItem(arolla::Text("foo"), schema::kAny)},
          {test::DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")),
                          schema::kExpr),
           test::DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")),
                          schema::kAny)},
          {test::DataItem(schema::kInt32, schema::kSchema),
           test::DataItem(schema::kInt32, schema::kAny)},
          {test::DataItem(empty_objects[0], schema::kItemId),
           test::DataItem(empty_objects[0], schema::kAny)},
          {test::DataItem(1, schema::kObject), test::DataItem(1, schema::kAny)},
          {test::DataItem(empty_objects[0], internal::AllocateExplicitSchema()),
           test::DataItem(empty_objects[0], schema::kAny)},
          {test::DataItem(1, schema::kAny), test::DataItem(1, schema::kAny)},
      };
      AssertLowerBoundDTypesAreTested(schema::kAny, test_cases);
      return test_cases;
    }()));

TEST_P(CastingToItemIdTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto item_id_slice, ToItemId(input));
  EXPECT_THAT(item_id_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToItemIdTestSuite, CastingToItemIdTest, ::testing::ValuesIn([] {
      auto empty_objects = internal::DataSliceImpl::AllocateEmptyObjects(3);
      auto item_id_slice = *DataSlice::Create(
          empty_objects, DataSlice::JaggedShape::FlatFromSize(3),
          internal::DataItem(schema::kItemId));
      auto explicit_schema = internal::AllocateExplicitSchema();
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kItemId)},
          {*DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kItemId)),
           item_id_slice},
          {*DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kObject)),
           item_id_slice},
          {*DataSlice::Create(empty_objects,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kAny)),
           item_id_slice},
          {*DataSlice::Create(
               empty_objects, DataSlice::JaggedShape::FlatFromSize(3),
               internal::DataItem(internal::AllocateExplicitSchema())),
           item_id_slice},
          {test::DataSlice<internal::ObjectId>(
              {explicit_schema, std::nullopt, explicit_schema},
              schema::kSchema),
           test::DataSlice<internal::ObjectId>(
              {explicit_schema, std::nullopt, explicit_schema},
              schema::kItemId)},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kItemId)},
          {test::DataItem(empty_objects[0], schema::kItemId),
           test::DataItem(empty_objects[0], schema::kItemId)},
          {test::DataItem(empty_objects[0], schema::kObject),
           test::DataItem(empty_objects[0], schema::kItemId)},
          {test::DataItem(empty_objects[0], schema::kAny),
           test::DataItem(empty_objects[0], schema::kItemId)},
          {test::DataItem(empty_objects[0], internal::AllocateExplicitSchema()),
           test::DataItem(empty_objects[0], schema::kItemId)},
          {test::Schema(explicit_schema),
           test::DataItem(explicit_schema, schema::kItemId)},
      };
      AssertLowerBoundDTypesAreTested(schema::kItemId, test_cases);
      return test_cases;
    }()));

TEST(Casting, ItemIdErrors) {
  EXPECT_THAT(
      ToItemId(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                             schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(
      ToItemId(test::MixedDataSlice<internal::ObjectId, arolla::Unit>(
          {internal::AllocateSingleObject(), std::nullopt, std::nullopt},
          {std::nullopt, arolla::kUnit, std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "cannot cast MASK to ITEMID"));
  EXPECT_THAT(
      ToItemId(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToItemId(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to ITEMID"));
  EXPECT_THAT(ToItemId(test::Schema(schema::kInt32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast DTYPE to ITEMID"));
}

TEST_P(CastingToSchemaTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto schema_slice, ToSchema(input));
  EXPECT_THAT(schema_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToSchemaTestSuite, CastingToSchemaTest, ::testing::ValuesIn([] {
      auto schema_obj = internal::AllocateExplicitSchema();
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kSchema)},
          {test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema),
           test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema)},
          {test::DataSlice<internal::ObjectId>({schema_obj, std::nullopt},
                                               schema::kSchema),
           test::DataSlice<internal::ObjectId>({schema_obj, std::nullopt},
                                               schema::kSchema)},
          {test::MixedDataSlice<schema::DType, internal::ObjectId>(
               {schema::kInt32, std::nullopt}, {std::nullopt, schema_obj},
               schema::kSchema),
           test::MixedDataSlice<schema::DType, internal::ObjectId>(
               {schema::kInt32, std::nullopt}, {std::nullopt, schema_obj},
               schema::kSchema)},
          {test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kObject),
           test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema)},
          {test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kAny),
           test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema)},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kSchema)},
          {test::DataItem(schema::kInt32, schema::kSchema),
           test::DataItem(schema::kInt32, schema::kSchema)},
          {test::DataItem(schema_obj, schema::kSchema),
           test::DataItem(schema_obj, schema::kSchema)},
          {test::DataItem(schema::kInt32, schema::kObject),
           test::DataItem(schema::kInt32, schema::kSchema)},
          {test::DataItem(schema::kInt32, schema::kAny),
           test::DataItem(schema::kInt32, schema::kSchema)},
      };
      AssertLowerBoundDTypesAreTested(schema::kSchema, test_cases);
      return test_cases;
    }()));

TEST(Casting, SchemaErrors) {
  auto obj_id = internal::AllocateSingleObject();
  EXPECT_THAT(
      ToSchema(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                             schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToSchema(test::MixedDataSlice<schema::DType, arolla::Unit>(
                  {schema::kInt32, std::nullopt, std::nullopt},
                  {std::nullopt, arolla::kUnit, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to SCHEMA"));
  EXPECT_THAT(ToSchema(test::DataSlice<internal::ObjectId>(
                  {obj_id, std::nullopt}, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat("cannot cast %v to SCHEMA", obj_id)));
  EXPECT_THAT(
      ToSchema(test::DataItem(arolla::kUnit, schema::kMask)),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(ToSchema(test::DataItem(arolla::kUnit, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "cannot cast MASK to SCHEMA"));
  EXPECT_THAT(ToSchema(test::DataItem(obj_id, schema::kObject)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       absl::StrFormat("cannot cast %v to SCHEMA", obj_id)));
}

TEST_P(CastingToEntityTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  const auto& entity_schema = output.GetSchemaImpl();
  ASSERT_TRUE(entity_schema.is_entity_schema());
  ASSERT_OK_AND_ASSIGN(auto item_id_slice, ToEntity(input, entity_schema));
  EXPECT_THAT(item_id_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToEntityTestSuite, CastingToEntityTest, ::testing::ValuesIn([] {
      auto obj = internal::AllocateSingleObject();
      auto entity_schema_1 = internal::AllocateExplicitSchema();
      auto entity_schema_2 = internal::AllocateExplicitSchema();

      // Write a bogus schema to the db.
      auto entity_schema_3 = internal::AllocateExplicitSchema();
      auto db = DataBag::Empty();
      EXPECT_OK(db->GetMutableImpl().value().get().SetAttr(
          internal::DataItem(obj), schema::kSchemaAttr,
          internal::DataItem(entity_schema_3)));

      // NOTE: The input is casted to the output's entity_schema.
      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, entity_schema_1)},
          {test::DataSlice<ObjectId>({obj}, entity_schema_1),
           test::DataSlice<ObjectId>({obj}, entity_schema_1)},
          {test::DataSlice<ObjectId>({obj}, entity_schema_1),
           test::DataSlice<ObjectId>({obj}, entity_schema_2)},
          {test::DataSlice<ObjectId>({obj}, entity_schema_1, db),
           test::DataSlice<ObjectId>({obj}, entity_schema_2, db)},
          {test::DataSlice<ObjectId>({obj}, schema::kObject),
           test::DataSlice<ObjectId>({obj}, entity_schema_1)},
          {test::DataSlice<ObjectId>({obj}, schema::kAny),
           test::DataSlice<ObjectId>({obj}, entity_schema_1)},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, entity_schema_1)},
          {test::DataItem(obj, entity_schema_1),
           test::DataItem(obj, entity_schema_1)},
          {test::DataItem(obj, entity_schema_1),
           test::DataItem(obj, entity_schema_2)},
          {test::DataItem(obj, entity_schema_1, db),
           test::DataItem(obj, entity_schema_2, db)},
          {test::DataItem(obj, schema::kObject),
           test::DataItem(obj, entity_schema_1)},
          {test::DataItem(obj, schema::kAny),
           test::DataItem(obj, entity_schema_1)},
      };
      return test_cases;
    }()));

TEST(Casting, EntityErrors) {
  auto entity_schema = internal::DataItem(internal::AllocateExplicitSchema());
  EXPECT_THAT(ToEntity(test::EmptyDataSlice(3, schema::kNone),
                       internal::DataItem(schema::kInt32)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected an entity schema, got: INT32"));
  EXPECT_THAT(
      ToEntity(test::EmptyDataSlice(3, schema::kNone), internal::DataItem(1)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected an entity schema, got: 1"));
  EXPECT_THAT(
      ToEntity(test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                             schema::kMask),
               entity_schema),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(
      ToEntity(
          test::MixedDataSlice<internal::ObjectId, arolla::Unit>(
              {internal::AllocateSingleObject(), std::nullopt, std::nullopt},
              {std::nullopt, arolla::kUnit, std::nullopt}),
          entity_schema),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "cannot cast MASK to ITEMID; while casting to entity schema: %v",
              entity_schema)));
  EXPECT_THAT(
      ToEntity(test::DataItem(arolla::kUnit, schema::kMask), entity_schema),
      StatusIs(absl::StatusCode::kInvalidArgument, "unsupported schema: MASK"));
  EXPECT_THAT(
      ToEntity(test::DataItem(arolla::kUnit, schema::kObject), entity_schema),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          absl::StrFormat(
              "cannot cast MASK to ITEMID; while casting to entity schema: %v",
              entity_schema)));
}

TEST_P(CastingToObjectTest, Casting) {
  const auto& input = GetParam().input;
  const auto& output = GetParam().output;
  ASSERT_OK_AND_ASSIGN(auto obj_slice, ToObject(input));
  EXPECT_THAT(obj_slice, IsEquivalentTo(output));
}

INSTANTIATE_TEST_SUITE_P(
    CastingToObjectTestSuite, CastingToObjectTest, ::testing::ValuesIn([] {
      auto object_slice = internal::DataSliceImpl::AllocateEmptyObjects(3);
      auto schema_slice = internal::DataSliceImpl::Create(
          3, internal::DataItem(internal::AllocateExplicitSchema()));
      auto db = DataBag::Empty();
      EXPECT_OK(db->GetMutableImpl().value().get().SetAttr(
          object_slice, schema::kSchemaAttr, schema_slice));

      std::vector<CastingTestCase> test_cases = {
          // DataSliceImpl cases.
          {test::EmptyDataSlice(3, schema::kNone),
           test::EmptyDataSlice(3, schema::kObject)},
          {test::DataSlice<int>({1, std::nullopt}, schema::kInt32),
           test::DataSlice<int>({1, std::nullopt}, schema::kObject)},
          {test::DataSlice<int64_t>({1, std::nullopt}, schema::kInt64),
           test::DataSlice<int64_t>({1, std::nullopt}, schema::kObject)},
          {test::DataSlice<float>({1.0f, std::nullopt}, schema::kFloat32),
           test::DataSlice<float>({1.0f, std::nullopt}, schema::kObject)},
          {test::DataSlice<double>({1.0, std::nullopt}, schema::kFloat64),
           test::DataSlice<double>({1.0, std::nullopt}, schema::kObject)},
          {test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kMask),
           test::DataSlice<arolla::Unit>({arolla::kUnit, std::nullopt},
                                         schema::kObject)},
          {test::DataSlice<bool>({true, std::nullopt}, schema::kBool),
           test::DataSlice<bool>({true, std::nullopt}, schema::kObject)},
          {test::DataSlice<arolla::Bytes>({"foo", std::nullopt},
                                          schema::kBytes),
           test::DataSlice<arolla::Bytes>({"foo", std::nullopt},
                                          schema::kObject)},
          {test::DataSlice<arolla::Text>({"foo", std::nullopt},
                                         schema::kString),
           test::DataSlice<arolla::Text>({"foo", std::nullopt},
                                         schema::kObject)},
          {test::DataSlice<arolla::expr::ExprQuote>(
               {arolla::expr::ExprQuote(arolla::expr::Leaf("x")), std::nullopt},
               schema::kExpr),
           test::DataSlice<arolla::expr::ExprQuote>(
               {arolla::expr::ExprQuote(arolla::expr::Leaf("x")), std::nullopt},
               schema::kObject)},
          {test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kSchema),
           test::DataSlice<schema::DType>({schema::kInt32, std::nullopt},
                                          schema::kObject)},
          {*DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kItemId), db),
           *DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kObject), db)},
          {test::MixedDataSlice<int, arolla::Text>(
               {1, std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}),
           test::MixedDataSlice<int, arolla::Text>(
               {1, std::nullopt, std::nullopt},
               {std::nullopt, "bar", std::nullopt}, schema::kObject)},
          {test::DataSlice<int>({1, std::nullopt}, schema::kAny),
           test::DataSlice<int>({1, std::nullopt}, schema::kObject)},
          {*DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kAny), db),
           *DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kObject), db)},
          {*DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              schema_slice[0], db),
           *DataSlice::Create(object_slice,
                              DataSlice::JaggedShape::FlatFromSize(3),
                              internal::DataItem(schema::kObject), db)},
          // DataItem cases.
          {test::DataItem(std::nullopt, schema::kNone),
           test::DataItem(std::nullopt, schema::kObject)},
          {test::DataItem(1, schema::kInt32),
           test::DataItem(1, schema::kObject)},
          {test::DataItem(int64_t{1}, schema::kInt64),
           test::DataItem(int64_t{1}, schema::kObject)},
          {test::DataItem(1.0f, schema::kFloat32),
           test::DataItem(1.0f, schema::kObject)},
          {test::DataItem(1.0, schema::kFloat64),
           test::DataItem(1.0, schema::kObject)},
          {test::DataItem(arolla::kUnit, schema::kMask),
           test::DataItem(arolla::kUnit, schema::kObject)},
          {test::DataItem(true, schema::kBool),
           test::DataItem(true, schema::kObject)},
          {test::DataItem(arolla::Bytes("foo"), schema::kBytes),
           test::DataItem(arolla::Bytes("foo"), schema::kObject)},
          {test::DataItem(arolla::Text("foo"), schema::kString),
           test::DataItem(arolla::Text("foo"), schema::kObject)},
          {test::DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")),
                          schema::kExpr),
           test::DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")),
                          schema::kObject)},
          {test::DataItem(schema::kInt32, schema::kSchema),
           test::DataItem(schema::kInt32, schema::kObject)},
          {test::DataItem(object_slice[0], schema::kItemId, db),
           test::DataItem(object_slice[0], schema::kObject, db)},
          {test::DataItem(1, schema::kObject),
           test::DataItem(1, schema::kObject)},
          {test::DataItem(object_slice[0], schema_slice[0], db),
           test::DataItem(object_slice[0], schema::kObject, db)},
          {test::DataItem(1, schema::kAny), test::DataItem(1, schema::kObject)},
          {test::DataItem(object_slice[0], schema::kAny, db),
           test::DataItem(object_slice[0], schema::kObject, db)},
      };
      AssertLowerBoundDTypesAreTested(schema::kObject, test_cases);
      return test_cases;
    }()));

TEST(Casting, ToObjectEmbedding) {
  auto object_slice = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto schema_slice = internal::DataSliceImpl::Create(
      3, internal::DataItem(internal::AllocateExplicitSchema()));
  {
    // Slice.
    auto db = DataBag::Empty();
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                          schema_slice[0], db));
    EXPECT_THAT(ToObject(slice),
                IsOkAndHolds(IsEquivalentTo(*DataSlice::Create(
                    object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                    internal::DataItem(schema::kObject), db))));

    auto expected_db = DataBag::Empty();
    auto& expected_db_impl = expected_db->GetMutableImpl().value().get();
    ASSERT_OK(expected_db_impl.SetAttr(object_slice, schema::kSchemaAttr,
                                       schema_slice));
    EXPECT_THAT(db->GetImpl(), DataBagEqual(expected_db_impl));
  }
  {
    // Item.
    auto db = DataBag::Empty();
    auto item = test::DataItem(object_slice[0], schema_slice[0], db);
    EXPECT_THAT(ToObject(item), IsOkAndHolds(IsEquivalentTo(test::DataItem(
                                    object_slice[0], schema::kObject, db))));
    auto expected_db = DataBag::Empty();
    auto& expected_db_impl = expected_db->GetMutableImpl().value().get();
    ASSERT_OK(expected_db_impl.SetAttr(object_slice[0], schema::kSchemaAttr,
                                       schema_slice[0]));
    EXPECT_THAT(db->GetImpl(), DataBagEqual(expected_db_impl));
  }
  {
    // No schema validation (overwrite).
    auto db = DataBag::Empty();
    auto& db_impl = db->GetMutableImpl().value().get();
    ASSERT_OK(
        db_impl.SetAttr(object_slice[0], schema::kSchemaAttr, schema_slice[0]));
    auto schema_2 = internal::DataItem(internal::AllocateExplicitSchema());
    auto item = test::DataItem(object_slice[0], schema_2, db);
    EXPECT_THAT(ToObject(item, /*validate_schema=*/false),
                IsOkAndHolds(IsEquivalentTo(
                    test::DataItem(object_slice[0], schema::kObject, db))));
    auto expected_db = DataBag::Empty();
    auto& expected_db_impl = expected_db->GetMutableImpl().value().get();
    ASSERT_OK(expected_db_impl.SetAttr(object_slice[0], schema::kSchemaAttr,
                                       schema_2));
    EXPECT_THAT(db->GetImpl(), DataBagEqual(expected_db_impl));
  }
}

TEST(Casting, ToObjectImmutableBagWithPrimitives) {
  // A primitive slice with an immutable bag should succeed.
  auto db = DataBag::ImmutableEmptyWithFallbacks({});
  auto slice = test::DataSlice<int>({1, 2, std::nullopt}, schema::kInt32, db);
  EXPECT_THAT(ToObject(slice),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int>(
                  {1, 2, std::nullopt}, schema::kObject, db))));
}

TEST(Casting, ToObjectErrors) {
  auto object_slice = internal::DataSliceImpl::AllocateEmptyObjects(3);
  auto schema_slice = internal::DataSliceImpl::Create(
      3, internal::DataItem(internal::AllocateExplicitSchema()));
  {
    // Immutable bag.
    auto db = DataBag::ImmutableEmptyWithFallbacks({});
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                          schema_slice[0], db));
    EXPECT_THAT(
        ToObject(slice),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "cannot embed object schema without a mutable DataBag"));
  }
  {
    // Different schema compared to existing one.
    auto db = DataBag::Empty();
    ASSERT_OK(db->GetMutableImpl().value().get().SetAttr(
        object_slice, schema::kSchemaAttr, schema_slice));
    auto schema_2 = internal::DataItem(internal::AllocateExplicitSchema());

    // Slice.
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                          schema_2, db));
    EXPECT_THAT(
        ToObject(slice),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrFormat(
                     "existing schemas %v differ from the provided schema %v",
                     schema_slice, schema_2)));
    // Item.
    auto item = test::DataItem(object_slice[0], schema_2, db);
    EXPECT_THAT(
        ToObject(item),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrFormat(
                     "existing schema %v differs from the provided schema %v",
                     schema_slice[0], schema_2)));
  }
  {
    // Missing schema for e.g. ANY.
    auto db = DataBag::Empty();

    // Slice.
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                          internal::DataItem(schema::kAny), db));
    EXPECT_THAT(ToObject(slice), StatusIs(absl::StatusCode::kInvalidArgument,
                                          "missing schema for some objects"));
    // Item.
    auto item = test::DataItem(object_slice[0], schema::kAny, db);
    EXPECT_THAT(
        ToObject(item),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrFormat("missing schema for %v", object_slice[0])));
  }
  {
    // No DataBag.
    // Slice.
    ASSERT_OK_AND_ASSIGN(
        auto slice,
        DataSlice::Create(object_slice, DataSlice::JaggedShape::FlatFromSize(3),
                          internal::DataItem(schema::kAny)));
    EXPECT_THAT(
        ToObject(slice),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("cannot embed object schema without a mutable DataBag")));
    // Item.
    auto item = test::DataItem(object_slice[0], schema::kAny);
    EXPECT_THAT(
        ToObject(item),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("cannot embed object schema without a mutable DataBag")));
  }
}

TEST(Casting, CastToImplicit) {
  {
    // Invalid schema error.
    EXPECT_THAT(CastToImplicit(test::DataItem(std::nullopt, schema::kNone),
                               internal::DataItem(1)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a schema, got 1"));
  }
  {
    // Casting errors and successes.
    // No common schema.
    EXPECT_THAT(
        CastToImplicit(test::DataItem(1, schema::kInt32),
                       internal::DataItem(internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
    // Common schema is not the provided schema.
    EXPECT_THAT(CastToImplicit(test::DataItem(1.0f, schema::kFloat32),
                               internal::DataItem(schema::kInt32)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "unsupported implicit cast from FLOAT32 to INT32"));
    // Implicit casting succeeds if the common schema is the provided schema.
    EXPECT_THAT(
        CastToImplicit(test::DataItem(1, schema::kInt32),
                       internal::DataItem(schema::kFloat32)),
        IsOkAndHolds(IsEquivalentTo(test::DataItem(1.0f, schema::kFloat32))));
  }
}

TEST(Casting, CastToImplicit_CoversAllDTypes) {
  // Ensures that all schemas are supported by CastToImplicit.
  // DTypes.
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    schema::DType schema = schema::GetDType<T>();
    internal::DataItem schema_item(schema);
    EXPECT_THAT(
        CastToImplicit(test::EmptyDataSlice(3, schema::kNone), schema_item),
        IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema_item))));
  });
  // Entities.
  auto schema = internal::DataItem(internal::AllocateExplicitSchema());
  EXPECT_THAT(CastToImplicit(test::EmptyDataSlice(3, schema::kNone), schema),
              IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema))));
}

TEST(Casting, CastToExplicit) {
  {
    // Invalid schema error.
    EXPECT_THAT(CastToExplicit(test::DataItem(std::nullopt, schema::kNone),
                               internal::DataItem(1)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a schema, got 1"));
  }
  {
    // Casting errors and successes.
    // Explicit cast is allowed (if possible) even if the common schema is not
    // the provided schema.
    EXPECT_THAT(
        CastToExplicit(test::DataItem(1.0f, schema::kFloat32),
                       internal::DataItem(schema::kInt32)),
        IsOkAndHolds(IsEquivalentTo(test::DataItem(1, schema::kInt32))));
    // But it can also fail if the explicit cast is not allowed.
    EXPECT_THAT(
        CastToExplicit(test::DataItem(1, schema::kInt32),
                       internal::DataItem(internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "unsupported schema: INT32"));
  }
  {
    // Casting to OBJECT with embedding.
    auto obj = internal::DataItem(internal::AllocateSingleObject());
    auto schema_1 = internal::DataItem(internal::AllocateExplicitSchema());
    auto schema_2 = internal::DataItem(internal::AllocateExplicitSchema());

    auto db = DataBag::Empty();
    auto& db_impl = db->GetMutableImpl().value().get();
    ASSERT_OK(db_impl.SetAttr(obj, schema::kSchemaAttr, schema_1));

    auto item = test::DataItem(obj, schema_2, db);
    // With validation -> failure.
    EXPECT_THAT(
        CastToExplicit(item, internal::DataItem(schema::kObject)),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 absl::StrFormat(
                     "existing schema %v differs from the provided schema %v",
                     schema_1, schema_2)));

    // Without validation -> success.
    EXPECT_THAT(
        CastToExplicit(item, internal::DataItem(schema::kObject),
                       /*validate_schema=*/false),
        IsOkAndHolds(IsEquivalentTo(test::DataItem(obj, schema::kObject, db))));
    auto expected_db = DataBag::Empty();
    auto& expected_db_impl = expected_db->GetMutableImpl().value().get();
    ASSERT_OK(expected_db_impl.SetAttr(obj, schema::kSchemaAttr, schema_2));
    EXPECT_THAT(db->GetImpl(), DataBagEqual(expected_db_impl));
  }
}

TEST(Casting, CastToExplicit_CoversAllDTypes) {
  // Ensures that all schemas are supported by CastToExplicit.
  // DTypes.
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    schema::DType schema = schema::GetDType<T>();
    internal::DataItem schema_item(schema);
    EXPECT_THAT(
        CastToExplicit(test::EmptyDataSlice(3, schema::kNone), schema_item),
        IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema_item))));
  });
  // Entities.
  auto schema = internal::DataItem(internal::AllocateExplicitSchema());
  EXPECT_THAT(CastToExplicit(test::EmptyDataSlice(3, schema::kNone), schema),
              IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema))));
}

TEST(Casting, CastToNarrow) {
  {
    // Invalid schema error.
    EXPECT_THAT(CastToNarrow(test::DataItem(std::nullopt, schema::kNone),
                             internal::DataItem(1)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected a schema, got 1"));
  }
  {
    // Casting errors and successes.
    // No common schema.
    EXPECT_THAT(
        CastToNarrow(test::DataItem(1, schema::kInt32),
                     internal::DataItem(internal::AllocateExplicitSchema())),
        StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
    // Common schema is not the provided schema.
    EXPECT_THAT(CastToNarrow(test::DataItem(1.0f, schema::kFloat32),
                             internal::DataItem(schema::kInt32)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "unsupported narrowing cast to INT32 for the given "
                         "FLOAT32 DataSlice"));
    // Narrowed schema is not implicitly castable to provided schema.
    EXPECT_THAT(CastToNarrow(test::DataItem(1.0f, schema::kObject),
                             internal::DataItem(schema::kInt32)),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "unsupported narrowing cast to INT32 for the given "
                         "OBJECT DataSlice"));
    // Casting succeeds if the common schema is the provided schema.
    EXPECT_THAT(
        CastToNarrow(test::DataItem(1, schema::kInt32),
                     internal::DataItem(schema::kFloat32)),
        IsOkAndHolds(IsEquivalentTo(test::DataItem(1.0f, schema::kFloat32))));
    // Casting succeeds if the narrowed schema is implicitly castable to the
    // provided schema.
    EXPECT_THAT(
        CastToNarrow(test::DataItem(1, schema::kObject),
                     internal::DataItem(schema::kFloat32)),
        IsOkAndHolds(IsEquivalentTo(test::DataItem(1.0f, schema::kFloat32))));
  }
}

TEST(Casting, CastToNarrow_CoversAllDTypes) {
  // Ensures that all schemas are supported by CastToNarrow.
  // DTypes.
  arolla::meta::foreach_type(schema::supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    schema::DType schema = schema::GetDType<T>();
    internal::DataItem schema_item(schema);
    EXPECT_THAT(
        CastToNarrow(test::EmptyDataSlice(3, schema::kNone), schema_item),
        IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema_item))));
  });
  // Entities.
  auto schema = internal::DataItem(internal::AllocateExplicitSchema());
  EXPECT_THAT(CastToNarrow(test::EmptyDataSlice(3, schema::kNone), schema),
              IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(3, schema))));
}

TEST(Alignment, AlignSchemas) {
  auto slice_float =
      test::DataSlice<float>({3.0f, 2.0f, 1.0f}, schema::kFloat32);
  auto slice_int = test::DataSlice<int>({1, 2, 3}, schema::kInt32);
  auto item_int = test::DataItem(1, schema::kInt32);
  auto item_obj =
      test::DataItem(internal::AllocateSingleObject(), schema::kObject);
  {
    // Single alignment - slice.
    ASSERT_OK_AND_ASSIGN(auto res, AlignSchemas({slice_int}));
    EXPECT_THAT(res.slices, ElementsAre(IsEquivalentTo(slice_int)));
    EXPECT_THAT(res.common_schema,
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
  }
  {
    // Single alignment - item.
    ASSERT_OK_AND_ASSIGN(auto res, AlignSchemas({item_int}));
    EXPECT_THAT(res.slices, ElementsAre(IsEquivalentTo(item_int)));
    EXPECT_THAT(res.common_schema,
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
  }
  {
    // Multiple alignment - same schema.
    ASSERT_OK_AND_ASSIGN(auto res, AlignSchemas({slice_int, item_int}));
    EXPECT_THAT(res.slices, ElementsAre(IsEquivalentTo(slice_int),
                                        IsEquivalentTo(item_int)));
    EXPECT_THAT(res.common_schema,
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
  }
  {
    // Multiple alignment - different schema.
    ASSERT_OK_AND_ASSIGN(auto res,
                         AlignSchemas({slice_int, item_int, slice_float}));
    EXPECT_THAT(
        res.slices,
        ElementsAre(IsEquivalentTo(test::DataSlice<float>({1.0f, 2.0f, 3.0f},
                                                          schema::kFloat32)),
                    IsEquivalentTo(test::DataItem(1.0f, schema::kFloat32)),
                    IsEquivalentTo(slice_float)));
    EXPECT_THAT(res.common_schema,
                IsEquivalentTo(internal::DataItem(schema::kFloat32)));
  }
  {
    // Alignment with objects.
    ASSERT_OK_AND_ASSIGN(auto res, AlignSchemas({item_obj, item_int}));
    EXPECT_THAT(
        res.slices,
        ElementsAre(IsEquivalentTo(item_obj),
                    IsEquivalentTo(test::DataItem(1, schema::kObject))));
    EXPECT_THAT(res.common_schema,
                IsEquivalentTo(internal::DataItem(schema::kObject)));
  }
}

TEST(Alignment, AlignSchemas_Errors) {
  {
    // Invalid arity.
    EXPECT_THAT(AlignSchemas({}), StatusIs(absl::StatusCode::kInvalidArgument,
                                           "expected at least one slice"));
  }
  {
    // No common schema.
    auto item_int = test::DataItem(1, schema::kInt32);
    auto item_itemid =
        test::DataItem(internal::AllocateSingleObject(), schema::kItemId);
    EXPECT_THAT(
        AlignSchemas({item_int, item_itemid}),
        StatusIs(absl::StatusCode::kInvalidArgument, "no common schema"));
  }
}

}  // namespace
}  // namespace koladata
