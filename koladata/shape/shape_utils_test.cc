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
#include "koladata/shape/shape_utils.h"

#include <cstdint>
#include <functional>
#include <initializer_list>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_slice.h"
#include "koladata/testing/matchers.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"

namespace koladata::shape {
namespace {

using ::arolla::CreateFullDenseArray;
using ::koladata::testing::IsEquivalentTo;
using ::koladata::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

DataSlice::JaggedShape::Edge CreateEdge(
    std::initializer_list<int64_t> split_points) {
  return *DataSlice::JaggedShape::Edge::FromSplitPoints(
      CreateFullDenseArray(std::vector<int64_t>(split_points)));
}

absl::StatusOr<DataSlice::JaggedShapePtr> ShapeFromEdges(
    std::initializer_list<DataSlice::JaggedShape::Edge> edges) {
  return DataSlice::JaggedShape::FromEdges(edges);
}

struct ValueProvider {
  using value_type = DataSlice;
  const DataSlice& operator()(const DataSlice& ds) { return ds; }
};

struct RefProvider {
  using value_type = std::reference_wrapper<const DataSlice>;
  std::reference_wrapper<const DataSlice> operator()(const DataSlice& ds) {
    return std::cref(ds);
  }
};

template <typename Provider>
class ShapeUtilsTest : public ::testing::Test {
 public:
  using provider_t = Provider;
  using value_t = typename Provider::value_type;
};

using ProviderTestTypes = ::testing::Types<ValueProvider, RefProvider>;
TYPED_TEST_SUITE(ShapeUtilsTest, ProviderTestTypes);

TYPED_TEST(ShapeUtilsTest, GetCommonShape) {
  typename TestFixture::provider_t provider;
  using value_t = typename TestFixture::value_t;
  {
    // 1 input.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values), shape_1));
    std::vector<value_t> slices{provider(ds)};
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape<DataSlice>(slices));
    EXPECT_THAT(shape_1, IsEquivalentTo(*common_shape));
  }
  {
    // Same shape.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_1));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape<DataSlice>(slices));
    EXPECT_THAT(shape_1, IsEquivalentTo(*common_shape));
  }
  {
    // Normal expansion required.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto edge_1 = CreateEdge({0, 3});
    auto edge_2 = CreateEdge({0, 2, 4, 6});
    ASSERT_OK_AND_ASSIGN(auto shape_2, ShapeFromEdges({edge_1, edge_2}));
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_2));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    ASSERT_OK_AND_ASSIGN(auto common_shape, GetCommonShape<DataSlice>(slices));
    EXPECT_THAT(shape_2, IsEquivalentTo(*common_shape));
  }
  {
    // Non-compatible shapes.
    auto edge_1 = CreateEdge({0, 3});
    auto edge_2_1 = CreateEdge({0, 2, 4, 6});
    auto edge_2_2 = CreateEdge({0, 2, 4, 5});
    ASSERT_OK_AND_ASSIGN(auto shape_1, ShapeFromEdges({edge_1, edge_2_1}));
    ASSERT_OK_AND_ASSIGN(auto shape_2, ShapeFromEdges({edge_1, edge_2_2}));
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_2));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    EXPECT_THAT(GetCommonShape<DataSlice>(slices),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible")));
  }
  {
    // No inputs.
    std::vector<DataSlice> slices;
    EXPECT_THAT(
        GetCommonShape<DataSlice>(slices),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("computing a common shape requires at least 1 input")));
  }
}

TYPED_TEST(ShapeUtilsTest, Align) {
  typename TestFixture::provider_t provider;
  using value_t = typename TestFixture::value_t;
  {
    // 1 input.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values), shape_1));
    std::vector<value_t> slices{provider(ds)};
    ASSERT_OK_AND_ASSIGN(auto aligned_slices, Align<DataSlice>(slices));
    EXPECT_EQ(slices.size(), aligned_slices.size());
    EXPECT_THAT(get_referred_value(slices[0]),
                IsEquivalentTo(aligned_slices[0]));
  }
  {
    // Same shape.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_1));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    ASSERT_OK_AND_ASSIGN(auto aligned_slices, Align<DataSlice>(slices));
    EXPECT_EQ(slices.size(), aligned_slices.size());
    EXPECT_THAT(get_referred_value(slices[0]),
                IsEquivalentTo(aligned_slices[0]));
    EXPECT_THAT(get_referred_value(slices[1]),
                IsEquivalentTo(aligned_slices[1]));
  }
  {
    // Non-compatible shapes.
    auto edge_1 = CreateEdge({0, 3});
    auto edge_2_1 = CreateEdge({0, 2, 4, 6});
    auto edge_2_2 = CreateEdge({0, 2, 4, 5});
    ASSERT_OK_AND_ASSIGN(auto shape_1, ShapeFromEdges({edge_1, edge_2_1}));
    ASSERT_OK_AND_ASSIGN(auto shape_2, ShapeFromEdges({edge_1, edge_2_2}));
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_2));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    EXPECT_THAT(Align<DataSlice>(slices),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible")));
  }
  {
    // Compatible, but different shapes that are being aligned.
    auto shape_1 = DataSlice::JaggedShape::FlatFromSize(3);
    auto edge_1 = CreateEdge({0, 3});
    auto edge_2 = CreateEdge({0, 2, 4, 6});
    ASSERT_OK_AND_ASSIGN(auto shape_2, ShapeFromEdges({edge_1, edge_2}));
    auto values_1 = CreateFullDenseArray<int>({1, 2, 3});
    ASSERT_OK_AND_ASSIGN(
        auto ds_1,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_1), shape_1));
    auto values_2 = CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(values_2), shape_2));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};

    ASSERT_OK_AND_ASSIGN(auto aligned, Align<DataSlice>(slices));
    EXPECT_THAT(aligned[0].GetShape(), IsEquivalentTo(*shape_2));
    EXPECT_THAT(aligned[0].slice().template values<int>(),
                ElementsAre(1, 1, 2, 2, 3, 3));
    EXPECT_THAT(aligned[1].GetShape(), IsEquivalentTo(*shape_2));
    EXPECT_THAT(aligned[1].slice().template values<int>(),
                ElementsAre(1, 2, 3, 4, 5, 6));
  }
}

TYPED_TEST(ShapeUtilsTest, AlignNonScalars) {
  typename TestFixture::provider_t provider;
  using value_t = typename TestFixture::value_t;
  {
    // 1 input - non-scalar.
    ASSERT_OK_AND_ASSIGN(auto ds, DataSlice::CreateWithSchemaFromData(
                                      internal::DataSliceImpl::Create(
                                          CreateFullDenseArray<int>({1, 2, 3})),
                                      DataSlice::JaggedShape::FlatFromSize(3)));
    std::vector<value_t> slices{provider(ds)};
    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars<DataSlice>(slices));
    EXPECT_EQ(slices.size(), aligned_slices.size());
    EXPECT_THAT(get_referred_value(slices[0]),
                IsEquivalentTo(aligned_slices[0]));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds.GetShape()));
  }
  {
    // 1 input - scalar.
    ASSERT_OK_AND_ASSIGN(
        auto ds,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1})),
            DataSlice::JaggedShape::Empty()));
    std::vector<value_t> slices{provider(ds)};
    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars<DataSlice>(slices));
    EXPECT_EQ(slices.size(), aligned_slices.size());
    EXPECT_THAT(get_referred_value(slices[0]),
                IsEquivalentTo(aligned_slices[0]));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds.GetShape()));
  }
  {
    // Mix of scalars and non-scalars.
    ASSERT_OK_AND_ASSIGN(
        auto ds_rank_0,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1})),
            DataSlice::JaggedShape::Empty()));
    ASSERT_OK_AND_ASSIGN(auto ds_rank_1, DataSlice::CreateWithSchemaFromData(
                                      internal::DataSliceImpl::Create(
                                          CreateFullDenseArray<int>({1, 2, 3})),
                                      DataSlice::JaggedShape::FlatFromSize(3)));
    ASSERT_OK_AND_ASSIGN(
        auto ds_rank_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(
                CreateFullDenseArray<int>({1, 2, 3, 4, 5, 6})),
            DataSlice::JaggedShape::FromEdges(
                {
                    DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3)
                        .value(),
                    DataSlice::JaggedShape::Edge::FromUniformGroups(3, 2)
                        .value(),
                })
                .value()));

    std::vector<value_t> slices{provider(ds_rank_0), provider(ds_rank_1),
                                provider(ds_rank_2)};
    ASSERT_OK_AND_ASSIGN((auto [aligned_slices, aligned_shape]),
                         AlignNonScalars<DataSlice>(slices));
    EXPECT_EQ(slices.size(), aligned_slices.size());
    EXPECT_THAT(get_referred_value(slices[0]),
                IsEquivalentTo(aligned_slices[0]));
    EXPECT_THAT(aligned_slices[1].GetShape(),
                IsEquivalentTo(ds_rank_2.GetShape()));
    EXPECT_THAT(aligned_slices[1].slice().template values<int>(),
                ElementsAre(1, 1, 2, 2, 3, 3));
    EXPECT_THAT(aligned_slices[2].GetShape(),
                IsEquivalentTo(ds_rank_2.GetShape()));
    EXPECT_THAT(aligned_slices[2].slice().template values<int>(),
                ElementsAre(1, 2, 3, 4, 5, 6));
    EXPECT_THAT(aligned_shape, IsEquivalentTo(ds_rank_2.GetShape()));
  }
  {
    // Non-compatible shapes.
    ASSERT_OK_AND_ASSIGN(auto ds_1,
                         DataSlice::CreateWithSchemaFromData(
                             internal::DataSliceImpl::Create(
                                 CreateFullDenseArray<int>({1, 2, 3})),
                             DataSlice::JaggedShape::FlatFromSize(3)));
    ASSERT_OK_AND_ASSIGN(
        auto ds_2,
        DataSlice::CreateWithSchemaFromData(
            internal::DataSliceImpl::Create(CreateFullDenseArray<int>({1, 2})),
            DataSlice::JaggedShape::FlatFromSize(2)));
    std::vector<value_t> slices{provider(ds_1), provider(ds_2)};
    EXPECT_THAT(AlignNonScalars<DataSlice>(slices),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("shapes are not compatible")));
  }
}

}  // namespace
}  // namespace koladata::shape
