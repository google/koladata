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
#include "koladata/internal/op_utils/coalesce_with_filtered.h"

#include <initializer_list>
#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

using ::testing::ElementsAre;

using ::arolla::CreateDenseArray;
using ::arolla::Unit;

// Just a simple test since the implementation is a trivial composition of other
// operators.
TEST(CoalesceWithFilteredTest, DataSlicePrimitiveValues) {
  {
    // Int.
    auto f = CreateDenseArray<int>({1, 2, std::nullopt, 4});
    auto l =
        CreateDenseArray<int>({1, std::nullopt, std::nullopt, std::nullopt});
    auto r = CreateDenseArray<int>({10, 20, 30, std::nullopt});
    auto fds = DataSliceImpl::Create(f);
    auto lds = DataSliceImpl::Create(l);
    auto rds = DataSliceImpl::Create(r);

    ASSERT_OK_AND_ASSIGN(auto res, CoalesceWithFiltered(fds, lds, rds));
    EXPECT_THAT(res, ElementsAre(1, 20, std::nullopt, std::nullopt));
  }
}

}  // namespace
}  // namespace koladata::internal
