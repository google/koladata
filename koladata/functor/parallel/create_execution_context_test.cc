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
#include "koladata/functor/parallel/create_execution_context.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/testing/equals_proto.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/execution_config.pb.h"
#include "koladata/proto/from_proto.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsProto;
using ::testing::ElementsAre;
using ::testing::HasSubstr;

TEST(CreateExecutionContextTest, Default) {
  ExecutionConfig config;
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_OK_AND_ASSIGN(auto execution_context,
                       CreateExecutionContext(config_slice));
  EXPECT_EQ(execution_context->operator_replacements().size(), 0);
  EXPECT_FALSE(execution_context->allow_runtime_transforms());
}

TEST(CreateExecutionContextTest, Basic) {
  ExecutionConfig config;
  auto* replacement = config.add_operator_replacements();
  replacement->set_from_op("core.get_nth");
  replacement->set_to_op("core.make_tuple");
  auto* transformation = replacement->mutable_argument_transformation();
  transformation->add_arguments(
      ExecutionConfig::ArgumentTransformation::EXECUTOR);
  transformation->add_arguments(
      ExecutionConfig::ArgumentTransformation::ORIGINAL_ARGUMENTS);
  transformation->add_keep_literal_argument_indices(1);
  config.set_allow_runtime_transforms(true);
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_OK_AND_ASSIGN(auto execution_context,
                       CreateExecutionContext(config_slice));
  ASSERT_OK_AND_ASSIGN(arolla::expr::ExprOperatorPtr op_before,
                       arolla::expr::LookupOperator("core.get_nth"));
  ASSERT_OK_AND_ASSIGN(op_before,
                       arolla::expr::DecayRegisteredOperator(op_before));
  ASSERT_OK_AND_ASSIGN(arolla::expr::ExprOperatorPtr op_after,
                       arolla::expr::LookupOperator("core.make_tuple"));
  EXPECT_EQ(execution_context->operator_replacements().size(), 1);
  EXPECT_EQ(execution_context->operator_replacements().begin()->first,
            op_before->fingerprint());
  EXPECT_EQ(execution_context->operator_replacements()
                .begin()
                ->second.op->fingerprint(),
            op_after->fingerprint());
  EXPECT_THAT(execution_context->operator_replacements()
                  .begin()
                  ->second.argument_transformation,
              EqualsProto("arguments: EXECUTOR arguments: ORIGINAL_ARGUMENTS "
                          "keep_literal_argument_indices: 1"));
  EXPECT_TRUE(execution_context->allow_runtime_transforms());
}

TEST(CreateExecutionContextTest, OriginalArgumentsImplied) {
  ExecutionConfig config;
  auto* replacement = config.add_operator_replacements();
  replacement->set_from_op("core.get_nth");
  replacement->set_to_op("core.make_tuple");
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  ASSERT_OK_AND_ASSIGN(auto execution_context,
                       CreateExecutionContext(config_slice));
  EXPECT_THAT(
      execution_context->operator_replacements()
          .begin()
          ->second.argument_transformation.arguments(),
      ElementsAre(ExecutionConfig::ArgumentTransformation::ORIGINAL_ARGUMENTS));
  EXPECT_THAT(
      execution_context->operator_replacements()
          .begin()
          ->second.argument_transformation.keep_literal_argument_indices(),
      ElementsAre());
}

TEST(CreateExecutionContextTest, UnknownFromOperator) {
  ExecutionConfig config;
  auto* replacement = config.add_operator_replacements();
  replacement->set_from_op("core.non_existing_operator");
  replacement->set_to_op("core.make_tuple");
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  EXPECT_THAT(
      CreateExecutionContext(config_slice),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("operator not found: core.non_existing_operator")));
}

TEST(CreateExecutionContextTest, UnknownToOperator) {
  ExecutionConfig config;
  auto* replacement = config.add_operator_replacements();
  replacement->set_from_op("core.get_nth");
  replacement->set_to_op("core.non_existing_operator");
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  EXPECT_THAT(
      CreateExecutionContext(config_slice),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("operator not found: core.non_existing_operator")));
}

TEST(CreateExecutionContextTest, DuplicateFromOperator) {
  ExecutionConfig config;
  auto* replacement = config.add_operator_replacements();
  replacement->set_from_op("kd.call");
  replacement->set_to_op("kd.call");
  replacement = config.add_operator_replacements();
  replacement->set_from_op("kd.functor.call");
  replacement->set_to_op("kd.call");
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  ASSERT_OK_AND_ASSIGN(
      DataSlice config_slice,
      config_slice_1d.Reshape(DataSlice::JaggedShape::Empty()));
  EXPECT_THAT(
      CreateExecutionContext(config_slice),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("duplicate operator replacement for: kd.functor.call")));
}

TEST(CreateExecutionContextTest, InvalidConfigShape) {
  ExecutionConfig config;
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice config_slice_1d, FromProto(db, {&config}));
  EXPECT_THAT(CreateExecutionContext(config_slice_1d),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("config must be a scalar, got rank 1")));
}

}  // namespace
}  // namespace koladata::functor::parallel
