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
#include "koladata/schema_constants.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_set.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/apply_filter.h"

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
                               schema::kItemId, schema::kObject,
                               schema::kSchema, schema::kNone));
}

TEST(SchemaConstants, AnyPrimitiveFilter) {
  const DataSlice& any_primitive = AnyPrimitiveFilter();
  EXPECT_EQ(any_primitive.GetSchemaImpl(), schema::kSchema);
  EXPECT_TRUE(any_primitive.is_item());
  EXPECT_TRUE(any_primitive.item().holds_value<internal::ObjectId>());
  EXPECT_TRUE(any_primitive.item().value<internal::ObjectId>().IsUuid());
  EXPECT_EQ(any_primitive.item(),
            internal::schema_filters::AnyPrimitiveFilter());
}

TEST(SchemaConstants, AnySchemaFilter) {
  const DataSlice& any_schema = AnySchemaFilter();
  EXPECT_EQ(any_schema.GetSchemaImpl(), schema::kSchema);
  EXPECT_TRUE(any_schema.is_item());
  EXPECT_TRUE(any_schema.item().holds_value<internal::ObjectId>());
  EXPECT_TRUE(any_schema.item().value<internal::ObjectId>().IsUuid());
  EXPECT_EQ(any_schema.item(), internal::schema_filters::AnySchemaFilter());
}

TEST(SchemaConstants, AnyPrimitiveAndAnySchemaAreDifferent) {
  EXPECT_NE(AnyPrimitiveFilter().item(), AnySchemaFilter().item());
}

}  // namespace
}  // namespace koladata
