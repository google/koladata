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
#include "koladata/functor/parallel/execution_context.h"

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "arolla/util/testing/equals_proto.h"
#include "koladata/functor/parallel/execution_config.pb.h"

namespace koladata::functor::parallel {
namespace {

using ::arolla::testing::EqualsProto;

TEST(ExecutionContextTest, Nullptr) {
  ExecutionContextPtr context = nullptr;
  EXPECT_EQ(arolla::Repr(context), "execution_context{nullptr}");
  EXPECT_EQ(arolla::TypedValue::FromValue(context).GetFingerprint(),
            arolla::TypedValue::FromValue(context).GetFingerprint());
}

TEST(ExecutionContextTest, Repr) {
  ExecutionContextPtr context = std::make_shared<ExecutionContext>(
      /*allow_runtime_transforms=*/false, ExecutionContext::ReplacementMap());
  EXPECT_EQ(arolla::Repr(context), "execution_context");
}

TEST(ExecutionContextTest, Fingerprint) {
  ExecutionContextPtr context1 = std::make_shared<ExecutionContext>(
      /*allow_runtime_transforms=*/false, ExecutionContext::ReplacementMap());
  ExecutionContextPtr context2 = std::make_shared<ExecutionContext>(
      /*allow_runtime_transforms=*/false, ExecutionContext::ReplacementMap());
  EXPECT_NE(arolla::TypedValue::FromValue(context1).GetFingerprint(),
            arolla::TypedValue::FromValue(context2).GetFingerprint());
}

TEST(ExecutionContextTest, Getters) {
  ASSERT_OK_AND_ASSIGN(auto op, arolla::expr::LookupOperator("core.get_nth"));
  ExecutionConfig::ArgumentTransformation transformation;
  transformation.add_arguments(
      ExecutionConfig::ArgumentTransformation::ORIGINAL_ARGUMENTS);
  transformation.add_keep_literal_argument_indices(1);
  ExecutionContext::ReplacementMap replacements = {
      {arolla::RandomFingerprint(),
       {.op = op, .argument_transformation = transformation}}};
  ExecutionContextPtr context = std::make_shared<ExecutionContext>(
      /*allow_runtime_transforms=*/true, replacements);
  EXPECT_TRUE(context->allow_runtime_transforms());
  ASSERT_EQ(context->operator_replacements().size(), 1);
  EXPECT_EQ(context->operator_replacements().begin()->second.op, op);
  EXPECT_THAT(
      context->operator_replacements().begin()->second.argument_transformation,
      EqualsProto(
          "arguments: ORIGINAL_ARGUMENTS keep_literal_argument_indices: 1"));
  context = std::make_shared<ExecutionContext>(
      /*allow_runtime_transforms=*/false, replacements);
  EXPECT_FALSE(context->allow_runtime_transforms());
}

}  // namespace
}  // namespace koladata::functor::parallel
