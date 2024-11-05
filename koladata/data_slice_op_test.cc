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
#include "koladata/data_slice_op.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/dense_array/dense_array.h"

namespace koladata {
namespace {

TEST(IsDefinedOnMixedImpl, Op) {
  struct Op_NonMixed {
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataItem> operator()(
        const internal::DataItem&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
  };
  static_assert(!IsDefinedOnMixedImpl<Op_NonMixed>::value);

  struct Op_NotFullyMixed_1 {
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataItem> operator()(
        const internal::DataItem&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
  };
  static_assert(!IsDefinedOnMixedImpl<Op_NotFullyMixed_1>::value);

  struct Op_NotFullyMixed_2 {
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataItem> operator()(
        const internal::DataItem&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataItem&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
  };
  static_assert(!IsDefinedOnMixedImpl<Op_NotFullyMixed_2>::value);

  struct Op_FullyMixed {
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataItem> operator()(
        const internal::DataItem&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataSliceImpl&, const internal::DataItem&) {
      return absl::InvalidArgumentError("");
    }
    absl::StatusOr<internal::DataSliceImpl> operator()(
        const internal::DataItem&, const internal::DataSliceImpl&) {
      return absl::InvalidArgumentError("");
    }
  };
  static_assert(IsDefinedOnMixedImpl<Op_FullyMixed>::value);
}

struct TestOp1 {
  absl::StatusOr<internal::DataSliceImpl> operator()(
      const internal::DataSliceImpl& ds, int arg_1, float arg_2) {
    if (arg_1 == 42) {
      return absl::InvalidArgumentError("Error is raised");
    }
    internal::SliceBuilder bldr(ds.size());
    ds.values<float>().ForEach([&](int64_t id, bool present, float value) {
      if (present && id < arg_1) {
        bldr.InsertIfNotSet(id, value + arg_2);
      }
    });
    return std::move(bldr).Build();
  }

  absl::StatusOr<internal::DataItem> operator()(const internal::DataItem& ds,
                                                int arg_1, float arg_2) {
    if (arg_1 == 42) {
      return absl::InvalidArgumentError("Error is raised");
    }
    return internal::DataItem(ds.value<float>() + arg_1 * arg_2);
  }
};

struct TestOp2 {
  internal::DataSliceImpl operator()(const internal::DataSliceImpl& ds,
                                     int arg_1, float arg_2) {
    internal::SliceBuilder bldr(ds.size());
    ds.values<float>().ForEach([&](int64_t id, bool present, float value) {
      if (present && id < arg_1) {
        bldr.InsertIfNotSet(id, value + arg_2);
      }
    });
    return std::move(bldr).Build();
  }

  internal::DataItem operator()(const internal::DataItem& ds, int arg_1,
                                float arg_2) {
    return internal::DataItem(ds.value<float>() + arg_1 * arg_2);
  }
};

TEST(DataSliceOp, SingleDataSliceArg_DataItem) {
  auto shape = DataSlice::JaggedShape::Empty();
  auto schema = internal::DataItem(schema::kFloat32);
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::Create(internal::DataItem(static_cast<float>(3.14)), schema));
  // StatusOr<DataItem> output of op.
  ASSERT_OK_AND_ASSIGN(
      auto res, DataSliceOp<TestOp1>()(ds, shape, schema, nullptr, 3, 2.71));
  EXPECT_FLOAT_EQ(res.item().value<float>(), 3.14 + 3 * 2.71);
  // DataItem output of op.
  ASSERT_OK_AND_ASSIGN(
      res, DataSliceOp<TestOp2>()(ds, shape, schema, nullptr, 3, 2.71));
  EXPECT_FLOAT_EQ(res.item().value<float>(), 3.14 + 3 * 2.71);

  // Error propagation.
  EXPECT_THAT(
      DataSliceOp<TestOp1>()(ds, shape, schema, nullptr, 42, 2.71),
      ::absl_testing::StatusIs(absl::StatusCode::kInvalidArgument,
                               ::testing::HasSubstr("Error is raised")));
}

TEST(DataSliceOp, SingleDataSliceArg_DataSlice) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto schema = internal::DataItem(schema::kFloat32);
  auto values = arolla::CreateDenseArray<float>({2.71, std::nullopt, 2.71});
  ASSERT_OK_AND_ASSIGN(
      auto ds,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values), shape));
  // StatusOr<DataSlice> output of op.
  ASSERT_OK_AND_ASSIGN(
      auto res, DataSliceOp<TestOp1>()(ds, shape, schema, nullptr, 2, 1.));
  EXPECT_THAT(res.slice().values<float>(),
              ::testing::ElementsAre(3.71, std::nullopt, std::nullopt));
  // DataItem output of op.
  ASSERT_OK_AND_ASSIGN(
      res, DataSliceOp<TestOp2>()(ds, shape, schema, nullptr, 2, 1.));
  EXPECT_THAT(res.slice().values<float>(),
              ::testing::ElementsAre(3.71, std::nullopt, std::nullopt));

  // Error propagation.
  EXPECT_THAT(
      DataSliceOp<TestOp1>()(ds, shape, schema, nullptr, 42, 2.71),
      ::absl_testing::StatusIs(absl::StatusCode::kInvalidArgument,
                               ::testing::HasSubstr("Error is raised")));
}

struct TestOp2Args {
  internal::DataSliceImpl operator()(const internal::DataSliceImpl& ds_1,
                                     const internal::DataSliceImpl& ds_2) {
    auto add = [&](arolla::DenseArray<int> a, arolla::DenseArray<int> b) {
      arolla::DenseArrayBuilder<int> bldr(ds_1.size());
      a.ForEach([&](int64_t id, bool present, int value) {
        bldr.Set(id, value + b[id].value);
      });
      return std::move(bldr).Build();
    };
    return internal::DataSliceImpl::Create(
        add(ds_1.values<int>(), ds_2.values<int>()));
  }

  internal::DataItem operator()(const internal::DataItem& item_1,
                                const internal::DataItem& item_2) {
    return internal::DataItem(item_1.value<int>() + item_2.value<int>());
  }
};

struct TestOp2Args_Mixed {
  internal::DataSliceImpl operator()(const internal::DataSliceImpl& ds_1,
                                     const internal::DataSliceImpl& ds_2) {
    return TestOp2Args()(ds_1, ds_2);
  }

  internal::DataItem operator()(const internal::DataItem& item_1,
                                const internal::DataItem& item_2) {
    return TestOp2Args()(item_1, item_2);
  }

  // Mixed signature.
  internal::DataSliceImpl operator()(const internal::DataSliceImpl& ds_1,
                                     const internal::DataItem& item_2) {
    auto add = [&](arolla::DenseArray<int> a, int b) {
      arolla::DenseArrayBuilder<int> bldr(ds_1.size());
      a.ForEach([&](int64_t id, bool present, int value) {
        bldr.Set(id, value + b);
      });
      return std::move(bldr).Build();
    };
    return internal::DataSliceImpl::Create(
        add(ds_1.values<int>(), item_2.value<int>()));
  }

  internal::DataSliceImpl operator()(const internal::DataItem& item_1,
                                     const internal::DataSliceImpl& ds_2) {
    return (*this)(ds_2, item_1);
  }
};

TEST(DataSliceOp, TwoDataSliceArgs_DataItem) {
  auto shape = DataSlice::JaggedShape::Empty();
  auto schema = internal::DataItem(schema::kInt32);
  ASSERT_OK_AND_ASSIGN(auto ds_1,
                       DataSlice::Create(internal::DataItem(42), schema));
  ASSERT_OK_AND_ASSIGN(auto ds_2,
                       DataSlice::Create(internal::DataItem(12), schema));
  ASSERT_OK_AND_ASSIGN(auto res,
                       DataSliceOp<TestOp2Args>()(ds_1, ds_2, schema, nullptr));
  EXPECT_EQ(res.item().value<int>(), 54);
}

TEST(DataSliceOp, TwoDataSliceArgs_DataSlice) {
  auto shape = DataSlice::JaggedShape::FlatFromSize(3);
  auto schema = internal::DataItem(schema::kInt32);
  auto values_1 = arolla::CreateDenseArray<int>({2, 12, 7});
  ASSERT_OK_AND_ASSIGN(
      auto ds_1,
      DataSlice::CreateWithSchemaFromData(
          internal::DataSliceImpl::Create(values_1), shape));
  ASSERT_OK_AND_ASSIGN(auto ds_2,
                       DataSlice::Create(internal::DataItem(42), schema));

  // Auto-aligns scalars by default.
  static_assert(!IsDefinedOnMixedImpl<TestOp2Args>::value);
  ASSERT_OK_AND_ASSIGN(auto res,
                       DataSliceOp<TestOp2Args>()(ds_1, ds_2, schema, nullptr));
  EXPECT_THAT(res.slice().values<int>(), ::testing::ElementsAre(44, 54, 49));

  // No scalar alignment.
  static_assert(IsDefinedOnMixedImpl<TestOp2Args_Mixed>::value);
  ASSERT_OK_AND_ASSIGN(res,
                       DataSliceOp<TestOp2Args>()(ds_1, ds_2, schema, nullptr));
  EXPECT_THAT(res.slice().values<int>(), ::testing::ElementsAre(44, 54, 49));
}

}  // namespace
}  // namespace koladata
