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
#include "py/koladata/expr/py_expr_eval.h"

#include <Python.h>  // IWYU pragma: keep

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "py/arolla/abc/py_cached_eval.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/exceptions/py_exception_utils.h"
#include "py/koladata/types/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {

namespace {

using ::koladata::expr::InputOperator;

static constexpr size_t kExprTransformationCacheSize = 1024;

// Information about an expression for fetching inputs for evaluation.
struct ExprInfo {
  std::vector<std::string> leaf_keys;
  absl::flat_hash_map<std::string, int> leaf_key_index;
};

// An expression and associated information compatible with the Arolla C++
// evaluation API.
struct TransformedExpr {
  arolla::expr::ExprNodePtr expr;
  ExprInfo info;
};

using TransformedExprPtr = std::shared_ptr<const TransformedExpr>;

// LRU cache for koda-specific expr transformations and information.
//
// NOTE: We rely on the GIL for synchronization of the cache.
class ExprTransformationCache {
  using Impl = arolla::LruCache<arolla::Fingerprint, TransformedExprPtr>;

 public:
  static absl::Nullable<TransformedExprPtr> LookupOrNull(
      const arolla::Fingerprint& fingerprint) {
    arolla::python::DCheckPyGIL();
    if (auto* res = Get().LookupOrNull(fingerprint)) {
      return *res;
    }
    return nullptr;
  }

  static absl::Nonnull<TransformedExprPtr> Put(
      const arolla::Fingerprint& fingerprint, TransformedExprPtr value) {
    arolla::python::DCheckPyGIL();
    return *Get().Put(fingerprint, std::move(value));
  }

  static void Clear() {
    arolla::python::DCheckPyGIL();
    Get().Clear();
  }

 private:
  static Impl& Get() {
    static absl::NoDestructor<Impl> cache(kExprTransformationCacheSize);
    return *cache;
  }
};

// Replaces all `I.x` inputs with `L.x` leaves.
absl::StatusOr<arolla::expr::ExprNodePtr> ReplaceInputsWithLeaves(
    const arolla::expr::ExprNodePtr& expr) {
  auto transform_expr = [](arolla::expr::ExprNodePtr node)
      -> absl::StatusOr<arolla::expr::ExprNodePtr> {
    ASSIGN_OR_RETURN(auto decayed_op,
                     arolla::expr::DecayRegisteredOperator(node->op()));
    if (arolla::fast_dynamic_downcast_final<const InputOperator*>(
            decayed_op.get()) != nullptr) {
      auto parse_text = [](const arolla::expr::ExprNodePtr& n) {
        return n->qvalue()->UnsafeAs<arolla::Text>().view();
      };
      if (parse_text(node->node_deps()[0]) != "I") {
        return node;
      }
      return arolla::expr::Leaf(parse_text(node->node_deps()[1]));
    }
    return node;
  };
  return arolla::expr::Transform(expr, transform_expr);
}

// Fetches information about the provided expression.
absl::StatusOr<ExprInfo> GetExprInfo(const arolla::expr::ExprNodePtr& expr) {
  if (auto placeholders = arolla::expr::GetPlaceholderKeys(expr);
      !placeholders.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the expression provided to kd.eval() contains placeholders for: [%s]",
        absl::StrJoin(placeholders, ", ")));
  }

  ExprInfo info;
  info.leaf_keys = arolla::expr::GetLeafKeys(expr);
  info.leaf_key_index.reserve(info.leaf_keys.size());
  for (int i = 0; i < info.leaf_keys.size(); ++i) {
    info.leaf_key_index[info.leaf_keys[i]] = i;
  }
  return info;
}

// Transforms the provided Koda expression into an expression that is compatible
// with the Arolla C++ API. In particular, this function replaces all `I.x`
// inputs with `L.x` leaves. Includes information about the expression for
// fetching inputs for evaluation.
absl::StatusOr<TransformedExprPtr> TransformExprForEval(
    const arolla::expr::ExprNodePtr& expr) {
  if (auto result = ExprTransformationCache::LookupOrNull(expr->fingerprint());
      result != nullptr) {
    return result;
  }
  // Apply transformations such as I.x -> L.x to enable expr evaluation through
  // the Arolla C++ API, and place into cache.
  ASSIGN_OR_RETURN(auto transformed_expr, ReplaceInputsWithLeaves(expr));
  ASSIGN_OR_RETURN(auto expr_info, GetExprInfo(transformed_expr));
  return ExprTransformationCache::Put(
      expr->fingerprint(),
      std::make_shared<TransformedExpr>(std::move(transformed_expr),
                                        std::move(expr_info)));
}

}  // namespace

absl::Nullable<PyObject*> PyEvalExpr(PyObject* /*self*/, PyObject** py_args,
                                     Py_ssize_t nargs, PyObject* py_kwnames) {
  arolla::python::DCheckPyGIL();
  static const absl::NoDestructor<FastcallArgParser> parser(
      /*pos_only_n=*/1, /*parse_kwargs=*/true);
  FastcallArgParser::Args args;
  if (!parser->Parse(py_args, nargs, py_kwnames, args)) {
    return nullptr;
  }

  // Parse expr.
  if (nargs < 1) {
    return PyErr_Format(PyExc_ValueError,
                        "kd.eval() expects exactly one positional input");
  }
  auto expr = arolla::python::UnwrapPyExpr(py_args[0]);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(PyExc_TypeError,
                        "kd.eval() expects an expression, got expr: %s",
                        Py_TYPE(py_args[0])->tp_name);
  }

  // Apply transformations such as I.x -> L.x to enable expr evaluation through
  // the Arolla C++ API.
  ASSIGN_OR_RETURN(auto transformed_expr_and_info, TransformExprForEval(expr),
                   (arolla::python::SetPyErrFromStatus(_), nullptr));
  const auto& expr_info = transformed_expr_and_info->info;

  // Parse inputs.
  std::vector<arolla::TypedRef> input_qvalues(
      expr_info.leaf_key_index.size(), arolla::TypedRef::UnsafeFromRawPointer(
                                           arolla::GetNothingQType(), nullptr));
  int input_count = 0;
  for (int i = 0; i < args.kw_names.size(); ++i) {
    auto it = expr_info.leaf_key_index.find(args.kw_names[i]);
    if (it == expr_info.leaf_key_index.end()) {
      continue;
    }
    const auto* typed_value = arolla::python::UnwrapPyQValue(args.kw_values[i]);
    if (typed_value == nullptr) {
      PyErr_Clear();
      return PyErr_Format(
          PyExc_TypeError,
          "kd.eval() expects all inputs to be QValues, got: %s=%s",
          std::string(it->first).c_str(), Py_TYPE(args.kw_values[i])->tp_name);
    }
    input_qvalues[it->second] = typed_value->AsRef();
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
    return PyErr_Format(PyExc_ValueError,
                        "kd.eval() has missing inputs for: [%s]",
                        absl::StrJoin(missing_leaf_keys, ", ").c_str());
  }

  // Evaluate the expression.
  ASSIGN_OR_RETURN(
      auto result,
      arolla::python::EvalExprWithCompilationCache(
          transformed_expr_and_info->expr, expr_info.leaf_keys, input_qvalues),
      (koladata::python::SetKodaPyErrFromStatus(_), nullptr));
  return arolla::python::WrapAsPyQValue(std::move(result));
}

PyObject* PyClearEvalCache(PyObject* /*self*/, PyObject* /*py_args*/) {
  ExprTransformationCache::Clear();
  Py_RETURN_NONE;
}

}  // namespace koladata::python
