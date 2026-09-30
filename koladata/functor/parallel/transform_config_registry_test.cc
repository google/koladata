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
#include "koladata/functor/parallel/transform_config_registry.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/testing/test_operators.h"
#include "koladata/functor/parallel/transform_config.h"
#include "koladata/functor/parallel/transform_config.pb.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::IsOkAndHolds;
using ::arolla::expr::testing::DummyOp;

class TransformConfigRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override { ResetDefaultParallelTransformConfigForTesting(); }
};

TEST_F(TransformConfigRegistryTest, ReusesConfigUntilExtended) {
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config,
                       GetDefaultParallelTransformConfig());
  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr runtime_config,
                       GetDefaultParallelTransformConfig(
                           /*allow_runtime_transforms=*/true));
  EXPECT_NE(config, runtime_config);
  EXPECT_THAT(GetDefaultParallelTransformConfig(), IsOkAndHolds(config));
  EXPECT_THAT(
      GetDefaultParallelTransformConfig(/*allow_runtime_transforms=*/true),
      IsOkAndHolds(runtime_config));

  auto op = std::make_shared<DummyOp>(
      "test.reuse_op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_THAT(arolla::expr::RegisterOperator("test.reuse_op", op), IsOk());
  ASSERT_THAT(
      ExtendDefaultParallelTransformConfig(R"pb(from_op: "test.reuse_op"
                                                to_op: "test.reuse_op")pb"),
      IsOk());

  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr extended_config,
                       GetDefaultParallelTransformConfig());
  EXPECT_NE(extended_config, config);
  EXPECT_TRUE(
      extended_config->operator_replacements().contains(op->fingerprint()));
}

TEST_F(TransformConfigRegistryTest, ResetDropsExtensions) {
  auto op = std::make_shared<DummyOp>(
      "test.reset_op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  ASSERT_THAT(arolla::expr::RegisterOperator("test.reset_op", op), IsOk());
  constexpr absl::string_view kExtension =
      R"pb(from_op: "test.reset_op" to_op: "test.reset_op")pb";
  ASSERT_THAT(ExtendDefaultParallelTransformConfig(kExtension), IsOk());
  ASSERT_THAT(ExtendDefaultParallelTransformConfig(kExtension), IsOk());
  ASSERT_FALSE(GetDefaultParallelTransformConfig().ok());

  ResetDefaultParallelTransformConfigForTesting();

  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config,
                       GetDefaultParallelTransformConfig());
  EXPECT_FALSE(config->operator_replacements().contains(op->fingerprint()));
}

TEST_F(TransformConfigRegistryTest, ExtendAndGet) {
  auto from_op = std::make_shared<DummyOp>(
      "test.from_op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());
  auto to_op = std::make_shared<DummyOp>(
      "test.to_op", arolla::expr::ExprOperatorSignature::MakeVariadicArgs());

  // Register them so LookupOperator can find them.
  EXPECT_THAT(arolla::expr::RegisterOperator("test.from_op", from_op), IsOk());
  EXPECT_THAT(arolla::expr::RegisterOperator("test.to_op", to_op), IsOk());

  std::string text_proto = R"pb(from_op: "test.from_op" to_op: "test.to_op")pb";

  EXPECT_THAT(ExtendDefaultParallelTransformConfig(text_proto), IsOk());

  ASSERT_OK_AND_ASSIGN(ParallelTransformConfigPtr config,
                       GetDefaultParallelTransformConfig());
  ASSERT_NE(config, nullptr);

  auto it = config->operator_replacements().find(from_op->fingerprint());
  ASSERT_NE(it, config->operator_replacements().end());
  EXPECT_EQ(it->second.op->display_name(), "test.to_op");
  ASSERT_FALSE(config->allow_runtime_transforms());

  ASSERT_OK_AND_ASSIGN(
      ParallelTransformConfigPtr config_with_runtime_transforms,
      GetDefaultParallelTransformConfig(/*allow_runtime_transforms=*/true));
  ASSERT_TRUE(config_with_runtime_transforms->allow_runtime_transforms());

  EXPECT_THAT(ExtendDefaultParallelTransformConfig(text_proto), IsOk());

  EXPECT_THAT(GetDefaultParallelTransformConfig(),
              absl_testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr("duplicate operator replacement")));
}

}  // namespace
}  // namespace koladata::functor::parallel
