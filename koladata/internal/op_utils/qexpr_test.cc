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
#include "koladata/internal/op_utils/qexpr.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/error_utils.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/qexpr/eval_context.h"

namespace koladata {
namespace {

using ::absl_testing::StatusIs;

TEST(QExpr, MakeBoundOperator) {
  auto bound_op = MakeBoundOperator(
      "op_name", [](arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
        return absl::InvalidArgumentError("test error");
      });
  arolla::EvaluationContext ctx;
  arolla::FrameLayout memory_layout = arolla::FrameLayout::Builder().Build();
  arolla::MemoryAllocation alloc(&memory_layout);

  bound_op->Run(&ctx, alloc.frame());

  EXPECT_THAT(ctx.status(), StatusIs(absl::StatusCode::kInvalidArgument,
                                     "op_name: test error"));
}

}  // namespace
}  // namespace koladata
