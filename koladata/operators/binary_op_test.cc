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
#include "koladata/operators/binary_op.h"
#include <cstdint>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/testing/matchers.h"

namespace koladata::ops {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

template <class T1, class T2>
struct NumericArgs {
  constexpr static bool value =
      std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2> &&
      !std::is_same_v<T1, bool> && !std::is_same_v<T2, bool>;
};

struct TestOpSub {
  template <class T>
  T operator()(T a, T b)
    requires(std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
  {
    return a - b;
  }
};

struct TestOpSubNoConstraint {
  template <class T>
  T operator()(T a, T b) {
    return a - b;
  }
};

struct TestOpNotStandardCasting {
  int operator()(int a, int b) { return a + b; }
  float operator()(float a, float b) { return a + b; }
  float operator()(float a, int b) { return a - b; }
  float operator()(int a, float b) { return a - b; }
};

struct TestOpDiv {
  template <typename T>
  absl::StatusOr<T> operator()(T lhs, T rhs) const
    requires(std::is_arithmetic_v<T> && !std::is_same_v<T, bool>)
  {
    if (rhs == 0) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "division by zero");
    }
    return lhs / rhs;
  }
};

struct TestOpConcat {
  arolla::Text operator()(const arolla::Text& lhs, const arolla::Text& rhs) {
    return arolla::Text(absl::StrCat(lhs.view(), rhs.view()));
  }
  arolla::Bytes operator()(const arolla::Bytes& lhs, const arolla::Bytes& rhs) {
    return absl::StrCat(lhs, rhs);
  }

  // Used in batch mode for both Text and Bytes.
  std::string operator()(absl::string_view lhs, absl::string_view rhs) {
    return absl::StrCat(lhs, rhs);
  }
};

template <class T1, class T2>
struct Concatable {
  constexpr static bool value =
      (std::is_same_v<T1, arolla::Text> && std::is_same_v<T2, arolla::Text>) ||
      (std::is_same_v<T1, arolla::Bytes> && std::is_same_v<T2, arolla::Bytes>);
};

TEST(BinaryOpTest, SimpleScalar) {
  auto empty_i32 = DataSlice::Create(internal::DataItem(),
                                     internal::DataItem(schema::kInt32))
                       .value();
  auto empty_f32 = DataSlice::Create(internal::DataItem(),
                                     internal::DataItem(schema::kFloat32))
                       .value();

  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5),
                            DataSlice::CreateFromScalar(3)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2))));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5),
                            DataSlice::CreateFromScalar(3.0f)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2.0f))));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5),
                            DataSlice::CreateFromScalar(3.0)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2.0))));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5L),
                              DataSlice::CreateFromScalar(3)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2L))));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(empty_i32, DataSlice::CreateFromScalar(3)),
      IsOkAndHolds(testing::IsEquivalentTo(empty_i32)));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(empty_i32, DataSlice::CreateFromScalar(3.0f)),
      IsOkAndHolds(testing::IsEquivalentTo(empty_f32)));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5), empty_f32),
      IsOkAndHolds(testing::IsEquivalentTo(empty_f32)));
  EXPECT_THAT(BinaryOpEval<TestOpSub>(empty_i32, empty_i32),
              IsOkAndHolds(testing::IsEquivalentTo(empty_i32)));
}

TEST(BinaryOpTest, OverloadsAndConstraints) {
  // TestOpSub works well both with and without `NumericArgs` constraint
  // because there is `requires` in functor definition.
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5),
                            DataSlice::CreateFromScalar(3)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2))));
  EXPECT_THAT(
      (BinaryOpEval<TestOpSub, NumericArgs>(
          DataSlice::CreateFromScalar(5), DataSlice::CreateFromScalar(3))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2))));

  // TestOpSubNoConstraint doesn't compile without `NumericArgs` because
  // it tries all types including e.g. Text - Text.
  EXPECT_THAT(
      (BinaryOpEval<TestOpSubNoConstraint, NumericArgs>(
          DataSlice::CreateFromScalar(5), DataSlice::CreateFromScalar(3))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2))));

  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(arolla::Text("abc")),
                            DataSlice::CreateFromScalar(arolla::Text("cdf"))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("unsupported type combination: TEXT, TEXT")));

  EXPECT_THAT((BinaryOpEval<TestOpSubNoConstraint, NumericArgs>(
                  DataSlice::CreateFromScalar(arolla::Text("abc")),
                  DataSlice::CreateFromScalar(arolla::Text("cdf")))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported type combination: TEXT, TEXT")));

  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(
          DataSlice::CreateFromScalar(5), DataSlice::CreateFromScalar(3))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(8))));
  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(DataSlice::CreateFromScalar(5.f),
                                            DataSlice::CreateFromScalar(3.f))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(8.f))));

  // Explicit overloads (int, float) and (float, int) have higher priority than
  // casting to (float, float).
  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(
          DataSlice::CreateFromScalar(5.f), DataSlice::CreateFromScalar(3))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2.f))));
  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(
          DataSlice::CreateFromScalar(5), DataSlice::CreateFromScalar(3.f))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2.f))));

  // There is no (int64, float) overload, so it casts to (float, float)
  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(
          DataSlice::CreateFromScalar(5L), DataSlice::CreateFromScalar(3.f))),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(8.f))));

  // Neither (int64, double) nor (double, double) is defined, so it returns
  // an error.
  EXPECT_THAT(
      (BinaryOpEval<TestOpNotStandardCasting>(DataSlice::CreateFromScalar(5L),
                                            DataSlice::CreateFromScalar(3.))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("unsupported type combination: INT64, FLOAT64")));
}

TEST(BinaryOpTest, AllMissing) {
  internal::DataItem i32(schema::kInt32);
  internal::DataItem f64(schema::kFloat64);
  auto impl_empty10 = internal::DataSliceImpl::CreateEmptyAndUnknownType(10);
  auto impl_i32_2 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<int32_t>({1, 2}));
  auto impl_i32_10 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  auto impl_f64_10 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<double>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  auto shape = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 5, 10}))
                        .value()})
                   .value();

  auto empty_scalar_i32 = DataSlice::Create(internal::DataItem(), i32).value();
  auto empty_scalar_f64 = DataSlice::Create(internal::DataItem(), f64).value();
  auto empty_flat_i32 =
      DataSlice::CreateWithFlatShape(impl_empty10, i32).value();
  auto empty_flat_f64 =
      DataSlice::CreateWithFlatShape(impl_empty10, f64).value();
  auto empty_2d_i32 = DataSlice::Create(impl_empty10, shape, i32).value();
  auto empty_2d_f64 = DataSlice::Create(impl_empty10, shape, f64).value();
  auto full_flat_i32 =
      DataSlice::CreateWithFlatShape(impl_i32_10, i32).value();
  auto full_flat_f64 =
      DataSlice::CreateWithFlatShape(impl_f64_10, f64).value();
  auto full_2d_i32 = DataSlice::Create(impl_i32_10, shape, i32).value();
  auto full_2d_f64 = DataSlice::Create(impl_f64_10, shape, f64).value();

  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5), empty_flat_f64),
      IsOkAndHolds(testing::IsEquivalentTo(empty_flat_f64)));
  EXPECT_THAT(
      BinaryOpEval<TestOpSub>(DataSlice::CreateFromScalar(5), empty_2d_i32),
      IsOkAndHolds(testing::IsEquivalentTo(empty_2d_i32)));
  EXPECT_THAT(BinaryOpEval<TestOpSub>(empty_scalar_i32, empty_2d_i32),
              IsOkAndHolds(testing::IsEquivalentTo(empty_2d_i32)));
  EXPECT_THAT(BinaryOpEval<TestOpSub>(empty_scalar_f64, empty_flat_i32),
              IsOkAndHolds(testing::IsEquivalentTo(empty_flat_f64)));
  EXPECT_THAT(BinaryOpEval<TestOpSub>(
                  empty_2d_f64,
                  DataSlice::CreateWithFlatShape(impl_i32_2, i32).value()),
              IsOkAndHolds(testing::IsEquivalentTo(empty_2d_f64)));

  EXPECT_THAT(BinaryOpEval<TestOpSub>(empty_2d_f64, empty_flat_i32),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("shapes are not compatible")));
}

TEST(BinaryOpTest, ScalarAndSlice) {
  internal::DataItem i32(schema::kInt32);
  internal::DataItem f64(schema::kFloat64);
  auto shape = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 5, 10}))
                        .value()})
                   .value();

  auto slice =
      DataSlice::Create(
          internal::DataSliceImpl::Create(arolla::CreateDenseArray<int32_t>(
              {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})),
          shape, i32)
          .value();
  auto scalar = DataSlice::CreateFromScalar(-10.0);

  {  // slice - scalar
    auto expected_res =
        DataSlice::Create(
            internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
                {11, 12, 13, 14, 15, 16, 17, 18, 19, 20})),
            shape, f64)
            .value();
    EXPECT_THAT(BinaryOpEval<TestOpSub>(slice, scalar),
                IsOkAndHolds(testing::IsEquivalentTo(expected_res)));
  }
  {  // scalar - slice
    auto expected_res =
        DataSlice::Create(
            internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
                {-11, -12, -13, -14, -15, -16, -17, -18, -19, -20})),
            shape, f64)
            .value();
    EXPECT_THAT(BinaryOpEval<TestOpSub>(scalar, slice),
                IsOkAndHolds(testing::IsEquivalentTo(expected_res)));
  }
}

TEST(BinaryOpTest, SameShape) {
  internal::DataItem i32(schema::kInt32);
  internal::DataItem f64(schema::kFloat64);
  auto impl_f64_10 =
      internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
          {-10, -20, -30, -40, -50, -60, -70, -80, -90, -100}));
  auto impl_i32_10 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  auto impl_res =
      internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
          {11, 22, 33, 44, 55, 66, 77, 88, 99, 110}));
  auto shape = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 5, 10}))
                        .value()})
                   .value();

  auto arg1 = DataSlice::Create(impl_i32_10, shape, i32).value();
  auto arg2 = DataSlice::Create(impl_f64_10, shape, f64).value();
  auto expected_res = DataSlice::Create(impl_res, shape, f64).value();

  EXPECT_THAT(BinaryOpEval<TestOpSub>(arg1, arg2),
              IsOkAndHolds(testing::IsEquivalentTo(expected_res)));
}

TEST(BinaryOpTest, DifferentShape) {
  internal::DataItem i32(schema::kInt32);
  internal::DataItem f64(schema::kFloat64);
  auto impl_f64_2 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<double>({-10, -20}));
  auto impl_i32_10 = internal::DataSliceImpl::Create(
      arolla::CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  auto shape = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 5, 10}))
                        .value()})
                   .value();

  auto arg1 = DataSlice::Create(impl_i32_10, shape, i32).value();
  auto arg2 = DataSlice::CreateWithFlatShape(impl_f64_2, f64).value();

  {
    auto expected_res =
        DataSlice::Create(
            internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
                {11, 12, 13, 14, 15, 26, 27, 28, 29, 30})),
            shape, f64)
            .value();
    EXPECT_THAT(BinaryOpEval<TestOpSub>(arg1, arg2),
                IsOkAndHolds(testing::IsEquivalentTo(expected_res)));
  }
  {
    auto expected_res =
        DataSlice::Create(
            internal::DataSliceImpl::Create(arolla::CreateDenseArray<double>(
                {-11, -12, -13, -14, -15, -26, -27, -28, -29, -30})),
            shape, f64)
            .value();
    EXPECT_THAT(BinaryOpEval<TestOpSub>(arg2, arg1),
                IsOkAndHolds(testing::IsEquivalentTo(expected_res)));
  }
}

TEST(BinaryOpTest, StatusPropagation) {
  internal::DataItem i32(schema::kInt32);
  auto shape2d = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2, 4}))
                        .value()})
                   .value();

  auto slice1d = DataSlice::CreateWithFlatShape(
                     internal::DataSliceImpl::Create(
                         arolla::CreateDenseArray<int32_t>({1, 2})),
                     i32)
                     .value();
  auto slice1d_with_zero = DataSlice::CreateWithFlatShape(
                               internal::DataSliceImpl::Create(
                                   arolla::CreateDenseArray<int32_t>({0, 1})),
                               i32)
                               .value();

  auto slice2d =
      DataSlice::Create(internal::DataSliceImpl::Create(
                            arolla::CreateDenseArray<int32_t>({1, 2, 3, 4})),
                        shape2d, i32)
          .value();
  auto slice2d_with_zero =
      DataSlice::Create(internal::DataSliceImpl::Create(
                            arolla::CreateDenseArray<int32_t>({1, 2, 0, 4})),
                        shape2d, i32)
          .value();

  // scalar / scalar
  EXPECT_THAT(
      BinaryOpEval<TestOpDiv>(DataSlice::CreateFromScalar(6),
                              DataSlice::CreateFromScalar(3)),
      IsOkAndHolds(testing::IsEquivalentTo(DataSlice::CreateFromScalar(2))));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(DataSlice::CreateFromScalar(6),
                                      DataSlice::CreateFromScalar(0)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // scalar / slice
  EXPECT_THAT(
      BinaryOpEval<TestOpDiv>(DataSlice::CreateFromScalar(6), slice1d)->slice(),
      ElementsAre(6, 3));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(DataSlice::CreateFromScalar(6),
                                      slice1d_with_zero),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // slice / scalar
  EXPECT_THAT(
      BinaryOpEval<TestOpDiv>(slice1d, DataSlice::CreateFromScalar(1))->slice(),
      ElementsAre(1, 2));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(slice1d, DataSlice::CreateFromScalar(0)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // slice / slice
  EXPECT_THAT(
      BinaryOpEval<TestOpDiv>(slice1d, slice1d)->slice(),
      ElementsAre(1, 1));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(slice1d, slice1d_with_zero),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // slice2d / slice
  EXPECT_THAT(
      BinaryOpEval<TestOpDiv>(slice2d, slice1d)->slice(),
      ElementsAre(1, 2, 1, 2));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(slice2d, slice1d_with_zero),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));

  // slice / slice2d
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(slice1d, slice2d)->slice(),
              ElementsAre(1, 0, 0, 0));
  EXPECT_THAT(BinaryOpEval<TestOpDiv>(slice1d, slice2d_with_zero),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("division by zero")));
}

TEST(BinaryOpTest, BytesAndText) {
  internal::DataItem sbytes(schema::kBytes);
  internal::DataItem stext(schema::kString);

  auto bytes = [](std::initializer_list<absl::string_view> data)
      -> internal::DataSliceImpl {
    arolla::DenseArrayBuilder<arolla::Bytes> bldr(data.size());
    int i = 0;
    for (auto v : data) {
      bldr.Set(i++, v);
    }
    return internal::DataSliceImpl::Create(std::move(bldr).Build());
  };
  auto texts = [](std::initializer_list<absl::string_view> data)
      -> internal::DataSliceImpl {
    arolla::DenseArrayBuilder<arolla::Text> bldr(data.size());
    int i = 0;
    for (auto v : data) {
      bldr.Set(i++, v);
    }
    return internal::DataSliceImpl::Create(std::move(bldr).Build());
  };

  auto shape2d = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, 2, 4}))
                        .value()})
                   .value();

  auto bytes1d =
      DataSlice::CreateWithFlatShape(bytes({"a", "b"}), sbytes).value();
  auto texts1d =
      DataSlice::CreateWithFlatShape(texts({"a", "b"}), stext).value();

  auto bytes2d =
      DataSlice::Create(bytes({"x", "y", "z", "v"}), shape2d, sbytes).value();
  auto texts2d =
      DataSlice::Create(texts({"x", "y", "z", "v"}), shape2d, stext).value();

  // scalar / scalar
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Text("a")),
                  DataSlice::CreateFromScalar(arolla::Text("b")))),
              IsOkAndHolds(testing::IsEquivalentTo(
                  DataSlice::CreateFromScalar(arolla::Text("ab")))));
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Bytes("a")),
                  DataSlice::CreateFromScalar(arolla::Bytes("b")))),
              IsOkAndHolds(testing::IsEquivalentTo(
                  DataSlice::CreateFromScalar(arolla::Bytes("ab")))));
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Bytes("a")),
                  DataSlice::CreateFromScalar(arolla::Text("b")))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported type combination")));
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Bytes("a")),
                  DataSlice::CreateFromScalar(0))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported type combination")));

  // scalar / slice
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Text("A")), texts1d)
                  ->slice()),
              ElementsAre(arolla::Text("Aa"), arolla::Text("Ab")));
  EXPECT_THAT((BinaryOpEval<TestOpConcat, Concatable>(
                  DataSlice::CreateFromScalar(arolla::Bytes("A")), bytes1d)
                  ->slice()),
              ElementsAre(arolla::Bytes("Aa"), arolla::Bytes("Ab")));

  // slice / slice
  EXPECT_THAT(
      (BinaryOpEval<TestOpConcat, Concatable>(texts1d, texts1d)->slice()),
      ElementsAre(arolla::Text("aa"), arolla::Text("bb")));
  EXPECT_THAT(
      (BinaryOpEval<TestOpConcat, Concatable>(bytes1d, bytes1d)->slice()),
      ElementsAre(arolla::Bytes("aa"), arolla::Bytes("bb")));

  // slice2d / slice
  EXPECT_THAT(
      (BinaryOpEval<TestOpConcat, Concatable>(texts2d, texts1d)->slice()),
      ElementsAre(arolla::Text("xa"), arolla::Text("ya"), arolla::Text("zb"),
                  arolla::Text("vb")));
  EXPECT_THAT(
      (BinaryOpEval<TestOpConcat, Concatable>(bytes2d, bytes1d)->slice()),
      ElementsAre(arolla::Bytes("xa"), arolla::Bytes("ya"), arolla::Bytes("zb"),
                  arolla::Bytes("vb")));
}

}  // namespace
}  // namespace koladata::ops
