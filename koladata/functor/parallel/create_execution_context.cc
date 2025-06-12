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

#include <memory>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/execution_config.pb.h"
#include "koladata/functor/parallel/execution_context.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/proto/to_proto.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor::parallel {

absl::StatusOr<ExecutionContextPtr> CreateExecutionContext(ExecutorPtr executor,
                                                           DataSlice config) {
  if (config.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(absl::StrCat(
        "config must be a scalar, got rank ", config.GetShape().rank()));
  }
  ExecutionConfig config_proto;
  ASSIGN_OR_RETURN(DataSlice config_1d,
                   config.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  RETURN_IF_ERROR(ToProto(config_1d, {&config_proto}));

  absl::flat_hash_map<arolla::Fingerprint, ExecutionContext::Replacement>
      operator_replacements;
  operator_replacements.reserve(config_proto.operator_replacements_size());
  for (const auto& replacement : config_proto.operator_replacements()) {
    arolla::expr::ExprOperatorPtr from_op =
        arolla::expr::ExprOperatorRegistry::GetInstance()->LookupOperatorOrNull(
            replacement.from_op());
    if (from_op == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrCat("operator not found: ", replacement.from_op()));
    }
    ASSIGN_OR_RETURN(from_op,
                     arolla::expr::DecayRegisteredOperator(std::move(from_op)));
    arolla::expr::ExprOperatorPtr to_op =
        arolla::expr::ExprOperatorRegistry::GetInstance()->LookupOperatorOrNull(
            replacement.to_op());
    if (to_op == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrCat("operator not found: ", replacement.to_op()));
    }
    if (operator_replacements.contains(from_op->fingerprint())) {
      return absl::InvalidArgumentError(absl::StrCat(
          "duplicate operator replacement for: ", replacement.from_op()));
    }
    auto transformation = replacement.argument_transformation();
    if (transformation.arguments_size() == 0) {
      transformation.add_arguments(
          ExecutionConfig::ArgumentTransformation::ORIGINAL_ARGUMENTS);
    }
    operator_replacements[from_op->fingerprint()] = {
        .op = std::move(to_op),
        .argument_transformation = std::move(transformation)};
  }
  return std::make_shared<ExecutionContext>(std::move(executor),
                                            std::move(operator_replacements));
}

}  // namespace koladata::functor::parallel
