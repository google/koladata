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
#ifndef KOLADATA_FUNCTOR_PARALLEL_EXECUTION_CONTEXT_H_
#define KOLADATA_FUNCTOR_PARALLEL_EXECUTION_CONTEXT_H_

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "koladata/functor/parallel/execution_config.pb.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {

// This class corresponds to one parallel execution, keeps a link to the
// executor and stores the configuration of the parallel execution.
//
// It is used to implement parallel_call. Advanced users that operate on futures
// and streams directly shouldn't use this class and should use the executor
// directly.
//
// The configuration is map of operator replacements. For each operator
// replacement, the key in the map is the fingerprint of the decayed version of
// the original operator, and the value is the description of its parallel
// version.
class ExecutionContext {
 public:
  struct Replacement {
    arolla::expr::ExprOperatorPtr op;
    ExecutionConfig::ArgumentTransformation argument_transformation;
  };

  using ReplacementMap = absl::flat_hash_map<arolla::Fingerprint, Replacement>;

  ExecutionContext(ExecutorPtr executor, ReplacementMap operator_replacements)
      : executor_(std::move(executor)),
        operator_replacements_(std::move(operator_replacements)) {}

  const ExecutorPtr& executor() const { return executor_; }
  const ReplacementMap& operator_replacements() const {
    return operator_replacements_;
  }
  // Returns the uuid of the execution context. This is a randomly generated
  // fingerprint, unique for each instance, that is used to compute
  // the fingerprint of the QValue.
  const arolla::Fingerprint& uuid() const { return uuid_; }

  // Disallow copy but allow move.
  ExecutionContext(const ExecutionContext&) = delete;
  ExecutionContext& operator=(const ExecutionContext&) = delete;
  ExecutionContext(ExecutionContext&&) = default;
  ExecutionContext& operator=(ExecutionContext&&) = default;

 private:
  ExecutorPtr executor_;
  ReplacementMap operator_replacements_;
  arolla::Fingerprint uuid_ = arolla::RandomFingerprint();
};

using ExecutionContextPtr = std::shared_ptr<ExecutionContext>;

}  // namespace koladata::functor::parallel

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(
    koladata::functor::parallel::ExecutionContextPtr);
AROLLA_DECLARE_REPR(koladata::functor::parallel::ExecutionContextPtr);
AROLLA_DECLARE_SIMPLE_QTYPE(EXECUTION_CONTEXT,
                            koladata::functor::parallel::ExecutionContextPtr);

}  // namespace arolla

#endif  // KOLADATA_FUNCTOR_PARALLEL_EXECUTION_CONTEXT_H_
