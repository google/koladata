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
#include "koladata/functor/parallel/transform_config.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/equals_proto.h"
#include "koladata/functor/parallel/transform_config.pb.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::testing::EqualsProto;

TEST(ParallelTransformConfigTest, Nullptr) {
  ParallelTransformConfigPtr config = nullptr;
  EXPECT_EQ(arolla::Repr(config), "parallel_transform_config{nullptr}");
  EXPECT_EQ(arolla::TypedValue::FromValue(config).GetFingerprint(),
            arolla::TypedValue::FromValue(config).GetFingerprint());
}

TEST(ParallelTransformConfigTest, Repr) {
  ParallelTransformConfigPtr config = std::make_shared<ParallelTransformConfig>(
      /*allow_runtime_transforms=*/false,
      ParallelTransformConfig::ReplacementMap());
  EXPECT_EQ(arolla::Repr(config), "parallel_transform_config");
}

TEST(ParallelTransformConfigTest, Fingerprint) {
  ParallelTransformConfigPtr config1 =
      std::make_shared<ParallelTransformConfig>(
          /*allow_runtime_transforms=*/false,
          ParallelTransformConfig::ReplacementMap());
  ParallelTransformConfigPtr config2 =
      std::make_shared<ParallelTransformConfig>(
          /*allow_runtime_transforms=*/false,
          ParallelTransformConfig::ReplacementMap());
  EXPECT_NE(arolla::TypedValue::FromValue(config1).GetFingerprint(),
            arolla::TypedValue::FromValue(config2).GetFingerprint());
}

TEST(ParallelTransformConfigTest, Getters) {
  ASSERT_OK_AND_ASSIGN(auto op, arolla::expr::LookupOperator("core.get_nth"));
  ParallelTransformConfigProto::ArgumentTransformation transformation;
  transformation.add_arguments(
      ParallelTransformConfigProto::ArgumentTransformation::ORIGINAL_ARGUMENTS);
  transformation.add_keep_literal_argument_indices(1);
  ParallelTransformConfig::ReplacementMap replacements = {
      {arolla::RandomFingerprint(),
       {.op = op, .argument_transformation = transformation}}};
  ParallelTransformConfigPtr config = std::make_shared<ParallelTransformConfig>(
      /*allow_runtime_transforms=*/true, replacements);
  EXPECT_TRUE(config->allow_runtime_transforms());
  ASSERT_EQ(config->operator_replacements().size(), 1);
  EXPECT_EQ(config->operator_replacements().begin()->second.op, op);
  EXPECT_THAT(
      config->operator_replacements().begin()->second.argument_transformation,
      EqualsProto(
          "arguments: ORIGINAL_ARGUMENTS keep_literal_argument_indices: 1"));
  config = std::make_shared<ParallelTransformConfig>(
      /*allow_runtime_transforms=*/false, replacements);
  EXPECT_FALSE(config->allow_runtime_transforms());
}

}  // namespace
}  // namespace koladata::functor::parallel
