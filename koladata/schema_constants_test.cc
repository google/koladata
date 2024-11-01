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
#include "koladata/schema_constants.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "koladata/internal/dtype.h"

namespace koladata {
namespace {

TEST(SchemaConstants, Schemas) {
  absl::flat_hash_set<schema::DType> schema_vals;
  for (const auto& schema_const : SupportedSchemas()) {
    schema_vals.insert(schema_const.item().value<schema::DType>());
    EXPECT_EQ(schema_const.GetSchemaImpl(), schema::kSchema);
  }

  EXPECT_THAT(schema_vals, ::testing::UnorderedElementsAre(
                               schema::kInt32, schema::kInt64, schema::kFloat32,
                               schema::kFloat64, schema::kBool, schema::kMask,
                               schema::kBytes, schema::kString, schema::kExpr,
                               schema::kAny, schema::kItemId, schema::kObject,
                               schema::kSchema, schema::kNone));
}

}  // namespace
}  // namespace koladata
