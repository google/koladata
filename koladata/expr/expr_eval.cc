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
#include "koladata/expr/expr_eval.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "koladata/expr/expr_operators.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/io/typed_refs_input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {
namespace {

// We use the same size for expr transformations and compilation caches
// since in most cases we have 1:1 correspondence between them.
static constexpr size_t kCacheSize = 4096;

// Information about an expression for fetching inputs for evaluation.
struct ExprInfo {
  std::vector<std::string> leaf_keys;
  absl::flat_hash_map<std::string, int> input_leaf_index;
  absl::flat_hash_map<std::string, int> variable_leaf_index;
};

// An expression and associated information compatible with the Arolla C++
// evaluation API.
struct TransformedExpr {
  arolla::expr::ExprNodePtr expr;
  ExprInfo info;
};

using TransformedExprPtr = std::shared_ptr<const TransformedExpr>;

// LRU cache for koda-specific expr transformations and information.
// This class is thread-safe.
class ExprTransformationCache {
  using Impl = arolla::LruCache<arolla::Fingerprint, TransformedExprPtr>;

 public:
  absl::Nullable<TransformedExprPtr> LookupOrNull(
      const arolla::Fingerprint& fingerprint) ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    if (auto* res = cache_.LookupOrNull(fingerprint)) {
      return *res;
    }
    return nullptr;
  }

  absl::Nonnull<TransformedExprPtr> Put(const arolla::Fingerprint& fingerprint,
                                        TransformedExprPtr value)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return *cache_.Put(fingerprint, std::move(value));
  }

  void Clear() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    cache_.Clear();
  }

  static ExprTransformationCache& Instance() {
    static absl::NoDestructor<ExprTransformationCache> instance;
    return *instance;
  }

 private:
  ExprTransformationCache() : cache_(kCacheSize) {}
  friend class absl::NoDestructor<ExprTransformationCache>;

  Impl cache_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

// Replaces all `I.x` and `V.x` inputs with leaves.
absl::StatusOr<TransformedExpr> ReplaceInputsWithLeaves(
    const arolla::expr::ExprNodePtr& expr) {
  TransformedExpr res;
  auto transform_expr = [&res](arolla::expr::ExprNodePtr node)
      -> absl::StatusOr<arolla::expr::ExprNodePtr> {
    ASSIGN_OR_RETURN(auto decayed_op,
                     arolla::expr::DecayRegisteredOperator(node->op()));
    if (arolla::fast_dynamic_downcast_final<const InputOperator*>(
            decayed_op.get()) == nullptr) {
      return node;
    }
    auto parse_text = [](const arolla::expr::ExprNodePtr& n) {
      return n->qvalue()->UnsafeAs<arolla::Text>().view();
    };
    auto container_name = parse_text(node->node_deps()[0]);
    if (container_name != "I" && container_name != "V") {
      return absl::InvalidArgumentError(
          absl::StrFormat("unknown input container: [%s]", container_name));
    }
    auto input_name = parse_text(node->node_deps()[1]);
    // This name will be visible in error messages, so we try to make it nice.
    auto leaf_name = absl::StrCat(container_name, ".", input_name);
    auto& leaf_index = container_name == "I" ? res.info.input_leaf_index
                                             : res.info.variable_leaf_index;
    bool was_inserted =
        leaf_index.emplace(input_name, res.info.leaf_keys.size()).second;
    if (was_inserted) {
      res.info.leaf_keys.push_back(leaf_name);
    }
    return arolla::expr::Leaf(leaf_name);
  };
  ASSIGN_OR_RETURN(res.expr, arolla::expr::Transform(expr, transform_expr));
  return res;
}

// Transforms the provided Koda expression into an expression that is compatible
// with the Arolla C++ API. In particular, this function replaces all `I.x` and
// `V.x` inputs with leaves. Includes information about the expression for
// fetching inputs for evaluation.
absl::StatusOr<TransformedExprPtr> TransformExprForEval(
    const arolla::expr::ExprNodePtr& expr) {
  if (auto result =
          ExprTransformationCache::Instance().LookupOrNull(expr->fingerprint());
      result != nullptr) {
    return result;
  }
  if (auto placeholders = arolla::expr::GetPlaceholderKeys(expr);
      !placeholders.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("the inputs to kd.eval() must be specified as I.x, but "
                        "the provided expression has placeholders: [%s]",
                        absl::StrJoin(placeholders, ", ")));
  }
  if (auto leaves = arolla::expr::GetLeafKeys(expr); !leaves.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("the inputs to kd.eval() must be specified as I.x, but "
                        "the provided expression has leaves: [%s]",
                        absl::StrJoin(leaves, ", ")));
  }
  ASSIGN_OR_RETURN(auto transformed_expr, ReplaceInputsWithLeaves(expr));
  return ExprTransformationCache::Instance().Put(
      expr->fingerprint(),
      std::make_shared<TransformedExpr>(std::move(transformed_expr)));
}

using CompiledExpr = std::function<absl::StatusOr<arolla::TypedValue>(
    absl::Span<const arolla::TypedRef>)>;

using Compiler = arolla::ExprCompiler<absl::Span<const arolla::TypedRef>,
                                      arolla::TypedValue>;

// LRU cache for Arolla expr compilation.
// This class is thread-safe.
class CompilationCache {
  using Impl = arolla::LruCache<arolla::Fingerprint, CompiledExpr>;

 public:
  CompiledExpr LookupOrNull(const arolla::Fingerprint& fingerprint)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    if (auto* res = cache_.LookupOrNull(fingerprint)) {
      // This copies std::function, which is fine since it is more or less a
      // shared_ptr.
      return *res;
    }
    return {};
  }

  CompiledExpr Put(const arolla::Fingerprint& fingerprint, CompiledExpr value)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return *cache_.Put(fingerprint, std::move(value));
  }

  void Clear() ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    cache_.Clear();
  }

  static CompilationCache& Instance() {
    static absl::NoDestructor<CompilationCache> instance;
    return *instance;
  }

 private:
  CompilationCache() : cache_(kCacheSize) {}
  friend class absl::NoDestructor<CompilationCache>;

  Impl cache_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

absl::StatusOr<CompiledExpr> Compile(
    const arolla::expr::ExprNodePtr& expr,
    absl::Span<const std::string> leaf_keys,
    absl::Span<const arolla::TypedRef> leaf_values) {
  if (leaf_values.size() != leaf_keys.size()) {
    return absl::InternalError(absl::StrFormat(
        "internal kd.eval error: passed %d leaf values, while %d was needed",
        leaf_values.size(), leaf_keys.size()));
  }
  // TODO: Instead of creating a fingerprint, we can use a tuple
  // as key.
  arolla::FingerprintHasher hasher("koladata.expr_eval");
  hasher.Combine(expr->fingerprint());
  for (const auto& value : leaf_values) {
    hasher.Combine(value.GetType());
  }
  arolla::Fingerprint key = std::move(hasher).Finish();
  CompiledExpr fn = CompilationCache::Instance().LookupOrNull(key);
  if (!fn) {
    std::vector<std::pair<std::string, arolla::QTypePtr>> args(
        leaf_values.size());
    for (size_t i = 0; i < leaf_values.size(); ++i) {
      args[i] = {leaf_keys[i], leaf_values[i].GetType()};
    }
    ASSIGN_OR_RETURN(
        fn,
        Compiler()
            // Most of our expressions are small and don't contain any literals.
            // In such cases the always clone thread safety policy is faster.
            .SetAlwaysCloneThreadSafetyPolicy()
            .SetInputLoader(arolla::CreateTypedRefsInputLoader(args))
            .Compile(expr));
    fn = CompilationCache::Instance().Put(key, std::move(fn));
  }
  return fn;
}

}  // namespace

absl::StatusOr<arolla::TypedValue> EvalExprWithCompilationCache(
    const arolla::expr::ExprNodePtr& expr,
    absl::Span<const std::pair<std::string, arolla::TypedRef>> inputs,
    absl::Span<const std::pair<std::string, arolla::TypedRef>> variables) {
  ASSIGN_OR_RETURN(auto transformed_expr, TransformExprForEval(expr));
  const auto& expr_info = transformed_expr->info;

  // Parse inputs.
  std::vector<arolla::TypedRef> input_qvalues(
      expr_info.leaf_keys.size(), arolla::TypedRef::UnsafeFromRawPointer(
                                      arolla::GetNothingQType(), nullptr));
  int input_count = 0;
  for (const auto& [input_name, input_value] : inputs) {
    auto it = expr_info.input_leaf_index.find(input_name);
    if (it == expr_info.input_leaf_index.end()) {
      continue;
    }
    if (input_qvalues[it->second].GetRawPointer() != nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("input [%s] passed twice to kd.eval()", input_name));
    }
    input_qvalues[it->second] = input_value;
    ++input_count;
  }
  for (const auto& [variable_name, variable_value] : variables) {
    auto it = expr_info.variable_leaf_index.find(variable_name);
    if (it == expr_info.variable_leaf_index.end()) {
      continue;
    }
    if (input_qvalues[it->second].GetRawPointer() != nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "variable [%s] passed twice to kd.eval()", variable_name));
    }
    input_qvalues[it->second] = variable_value;
    ++input_count;
  }

  // Validate inputs w.r.t. the expression.
  if (input_count != expr_info.leaf_keys.size()) {
    std::vector<absl::string_view> missing_leaf_keys;
    for (int i = 0; i < expr_info.leaf_keys.size(); ++i) {
      if (input_qvalues[i].GetRawPointer() == nullptr) {
        missing_leaf_keys.push_back(expr_info.leaf_keys[i]);
      }
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("kd.eval() has missing inputs for: [%s]",
                        absl::StrJoin(missing_leaf_keys, ", ")));
  }

  ASSIGN_OR_RETURN(
      auto compiled_expr,
      Compile(transformed_expr->expr, expr_info.leaf_keys, input_qvalues));
  return compiled_expr(input_qvalues);
}

void ClearCompilationCache() {
  ExprTransformationCache::Instance().Clear();
  CompilationCache::Instance().Clear();
}

}  // namespace koladata::expr
