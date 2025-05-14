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
#include "koladata/internal/types.h"

#include <cstdint>
#include <set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/missing_value.h"

namespace koladata::internal {
namespace {

using ::testing::UnorderedElementsAreArray;

TEST(TypesTest, ScalarTypeId) {
  EXPECT_EQ(ScalarTypeId<MissingValue>(), 0);
  EXPECT_NE(ScalarTypeId<arolla::Text>(), ScalarTypeId<arolla::Unit>());
  EXPECT_NE(ScalarTypeId<int64_t>(), ScalarTypeId<int32_t>());
  EXPECT_NE(ScalarTypeId<double>(), ScalarTypeId<float>());
  std::vector<int8_t> type_ids = {ScalarTypeId<MissingValue>()};
  arolla::meta::foreach_type<supported_types_list>([&](auto type_meta) {
    using T = typename decltype(type_meta)::type;
    type_ids.push_back(ScalarTypeId<T>());
    EXPECT_EQ(ScalarTypeIdToQType(ScalarTypeId<T>()), arolla::GetQType<T>());
  });
  EXPECT_THAT(std::set<int8_t>(type_ids.begin(), type_ids.end()),
              UnorderedElementsAreArray(type_ids));
  EXPECT_EQ(ScalarTypeIdToQType(ScalarTypeId<MissingValue>()), nullptr);
  EXPECT_EQ(ScalarTypeIdToQType(-1), nullptr);
}

}  // namespace
}  // namespace koladata::internal
