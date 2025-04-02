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
#include "koladata/functor/auto_variables.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/functor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "arolla/expr/annotation_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/visitors/substitution.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using arolla::expr::ExprNodePtr;
using arolla::expr::ExprOperatorPtr;
using arolla::expr::ExprQuote;

absl::StatusOr<bool> IsSliceOperator(const ExprNodePtr& node) {
  if (!node->is_op()) {
    return false;
  }
  ASSIGN_OR_RETURN(ExprOperatorPtr slice_op,
                   arolla::expr::DecayRegisteredOperator(
                       arolla::expr::ExprOperatorRegistry::GetInstance()
                           ->LookupOperatorOrNull("kd.slice")));
  ASSIGN_OR_RETURN(auto op, arolla::expr::DecayRegisteredOperator(node->op()));
  return op == slice_op;
}

// We want to avoid creating duplicate computations by extracting a sub-expr
// into multiple variables. This class finds shared sub-exprs, so we can extract
// them as well.
class SharedNodeTracker {
 public:
  void AddParentVariable(const ExprNodePtr& node,
                         const arolla::Fingerprint& parent_fingerprint) {
    auto [it, inserted] = node_to_parent_variable_.emplace(
        node->fingerprint(), parent_fingerprint);

    // If already present in the map and has different value, then it a shared
    // node that will be used in several extracted variables.
    // Set to nullptr - special value for shared nodes.
    if (!inserted && it->second != parent_fingerprint) {
      it->second = std::nullopt;
    }
  };

  template <typename Fn>
  absl::Status Process(const ExprNodePtr& expr, Fn&& to_be_extracted) {
    // Iterating in reverse post_order guarantees that we will see a node before
    // all its dependencies, so that we can check if it has multiple variable
    // parents.
    arolla::expr::PostOrder post_order(expr);
    for (int64_t i = post_order.nodes_size() - 1; i >= 0; --i) {
      const ExprNodePtr& node = post_order.node(i);
      // `parent_fingerprint` is what will be set as parent variable to children
      // of this node.
      arolla::Fingerprint parent_fingerprint = node->fingerprint();
      if (!to_be_extracted(node)) {
        auto it = node_to_parent_variable_.find(node->fingerprint());
        if (it == node_to_parent_variable_.end()) {
          continue;
        }
        // If this node has parent variable, we propagate it to children.
        // If the parent variable is nullopt (multiple parent), then this node
        // will become a variable of its own, so using this node's
        // fingerprint.
        parent_fingerprint = it->second.value_or(node->fingerprint());
      }
      for (const auto& child : node->node_deps()) {
        AddParentVariable(child, parent_fingerprint);
      }
    }
    return absl::OkStatus();
  }

  // Returns true if the node will be used by multiple extracted variables
  // (and thus need to become a variable as well).
  bool IsSharedNode(const ExprNodePtr& node) const {
    auto node_to_parent_it = node_to_parent_variable_.find(node->fingerprint());
    return node_to_parent_it != node_to_parent_variable_.end() &&
           !node_to_parent_it->second.has_value();
  }

 private:
  // This maps from a node fingerprint to the fingerprint of its ancestor that
  // will become a variable. `nullopt` means that the node has multiple such
  // parents.
  absl::flat_hash_map<arolla::Fingerprint, std::optional<arolla::Fingerprint>>
      node_to_parent_variable_;
};

// Wrapper for arolla::expr::WithNewDependencies, needed because
// PostOrderTraverse passes arguments as span of pointers.
absl::StatusOr<ExprNodePtr> WithNewDeps(
    const ExprNodePtr& node, absl::Span<const ExprNodePtr* const> new_deps) {
  if (!node->is_op() && !new_deps.empty()) {
    return absl::FailedPreconditionError("leaf node shoudn't have deps");
  }
  std::vector<ExprNodePtr> new_deps_vec;
  new_deps_vec.reserve(new_deps.size());
  for (const auto& v : new_deps) {
    new_deps_vec.push_back(*v);
  }
  return arolla::expr::WithNewDependencies(node, std::move(new_deps_vec));
};

// For 'simple' nodes, we do not extract them into variables even if they are
// used multiple times in the expression. This helps readability of the
// resulting functor.
bool IsSimple(const ExprNodePtr& node) {
  return !node->is_op() || expr::IsInput(node);
};

bool IsSimpleSlice(const DataSlice& slice) {
  return slice.GetBag() == nullptr && slice.is_item() &&
         (slice.GetSchema().IsPrimitiveSchema() ||
          slice.GetSchemaImpl() == internal::DataItem(schema::kNone));
};

absl::StatusOr<ExprNodePtr> ExtractAutoVariables(
    const ExprNodePtr& expr, const SharedNodeTracker& shared_node_tracker,
    absl::flat_hash_map<std::string, DataSlice>& vars) {
  ASSIGN_OR_RETURN(expr::InputContainer var_container,
                   expr::InputContainer::Create("V"));

  absl::flat_hash_set<arolla::Fingerprint> aux_variable_fingerprints;

  absl::flat_hash_map<std::string, int> prefix_to_counter;
  auto create_unique_variable = [&](const std::string& prefix) {
    int& counter = prefix_to_counter[prefix];
    while (true) {
      std::string var_name = absl::StrCat(prefix, "_", counter++);
      if (!vars.contains(var_name)) {
        return var_name;
      }
    }
  };

  auto transform_literal =
      [&](const ExprNodePtr& node,
          bool is_shared_node) -> absl::StatusOr<ExprNodePtr> {
    if (!node->qvalue().has_value()) {
      return absl::FailedPreconditionError("literal has no value");
    }
    if (node->qvalue()->GetType() != arolla::GetQType<DataSlice>()) {
      return node;
    }
    DataSlice val = node->qvalue()->UnsafeAs<DataSlice>();
    if (IsSimpleSlice(val)) {
      return node;
    }
    // We need to implode the DataSlice into lists if it is not a DataItem.
    std::string var_name = create_unique_variable("aux");
    ASSIGN_OR_RETURN(auto var, var_container.CreateInput(var_name));
    int ndim = val.GetShape().rank();
    if (ndim > 0) {
      ASSIGN_OR_RETURN(val, Implode(DataBag::Empty(), val, ndim));
      ASSIGN_OR_RETURN(
          var,
          arolla::expr::CallOp(
              "kd.explode",
              {var, koladata::expr::MakeLiteral(arolla::TypedValue::FromValue(
                        DataSlice::CreateFromScalar(ndim)))}));
    }
    vars[var_name] = val;
    // When a node has multiple parents, we cannot replace its aux name
    // with a wrapping name in the logic below, so we skip adding it to
    // aux_variable_fingerprints.
    if (!is_shared_node) {
      aux_variable_fingerprints.insert(var->fingerprint());
    }
    return var;
  };

  auto process_node = [&](const ExprNodePtr& old_node,
                          absl::Span<const ExprNodePtr* const> new_deps)
      -> absl::StatusOr<ExprNodePtr> {
    ASSIGN_OR_RETURN(auto node, WithNewDeps(old_node, new_deps));
    bool is_shared_node = shared_node_tracker.IsSharedNode(old_node);
    if (expr::IsLiteral(node)) {
      return transform_literal(node, is_shared_node);
    }
    ASSIGN_OR_RETURN(bool is_slice_op_node, IsSliceOperator(node));
    if (is_slice_op_node) {
      // Our binding policy for kd.slice produces kd.slice(<literal slice>).
      // When that is named we want the name to be used instead of aux_,
      // so we remove the unnecessary slice() wrapper in that case.
      const auto& val = node->node_deps()[0];
      const auto& schema = node->node_deps()[1];
      if (aux_variable_fingerprints.contains(val->fingerprint()) &&
          expr::IsLiteral(schema) && schema->qvalue().has_value() &&
          schema->qvalue()->GetType() == arolla::GetUnspecifiedQType()) {
        return val;
      }
    }
    if (arolla::expr::IsNameAnnotation(node)) {
      std::string_view expr_name = arolla::expr::ReadNameAnnotation(node);
      std::string name(expr_name);
      if (vars.contains(name)) {
        name = create_unique_variable(name);
      }
      auto child = node->node_deps()[0];
      if (aux_variable_fingerprints.contains(child->fingerprint())) {
        // If a literal DataSlice was named, avoid creating a temporary name
        // and use the real name instead. The auxiliary variable might have
        // been wrapped with kde.explode(), so we do a sub_inputs instead of
        // just replacing with V[name].
        aux_variable_fingerprints.erase(child->fingerprint());
        ASSIGN_OR_RETURN(std::vector<std::string> input_names,
                         var_container.ExtractInputNames(child));
        if (input_names.size() != 1) {
          return absl::FailedPreconditionError(
              "expected input_names.size() == 1");
        }
        auto var_it = vars.find(input_names[0]);
        DataSlice v = var_it->second;
        vars.erase(var_it);
        vars.emplace(name, std::move(v));
        ASSIGN_OR_RETURN(auto from, var_container.CreateInput(input_names[0]));
        ASSIGN_OR_RETURN(auto to, var_container.CreateInput(name));
        return arolla::expr::SubstituteByFingerprint(
            child, {{from->fingerprint(), to}});
      }
      vars.emplace(name, DataSlice::CreateFromScalar(ExprQuote{child}));
      return var_container.CreateInput(name);
    }
    if (!is_shared_node || IsSimple(node)) {
      return node;
    }
    std::string var_name = create_unique_variable("aux");
    vars[var_name] = DataSlice::CreateFromScalar(ExprQuote{node});
    return var_container.CreateInput(var_name);
  };

  return arolla::expr::PostOrderTraverse(expr, process_node);
}

}  // namespace

absl::StatusOr<DataSlice> AutoVariables(const DataSlice& functor) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) return absl::InvalidArgumentError("functor expected");

  ASSIGN_OR_RETURN(DataSlice signature, functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN(auto attr_names, functor.GetAttrNames());

  absl::flat_hash_map<std::string, DataSlice> vars;
  std::vector<std::string> expr_names;
  std::vector<ExprNodePtr> exprs_vec;
  for (const auto& attr_name : attr_names) {
    if (attr_name == kSignatureAttrName) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice var, functor.GetAttr(attr_name));
    DCHECK(var.is_item());
    if (var.is_item() &&
        var.GetSchemaImpl() == internal::DataItem(schema::kExpr)) {
      ASSIGN_OR_RETURN(auto var_expr, var.item().value<ExprQuote>().expr());
      expr_names.push_back(attr_name);
      exprs_vec.push_back(std::move(var_expr));
    }
    vars.emplace(attr_name, std::move(var));
  }

  SharedNodeTracker shared_node_tracker;
  ASSIGN_OR_RETURN(expr::InputContainer var_container,
                   expr::InputContainer::Create("V"));
  for (size_t i = 0; i < expr_names.size(); ++i) {
    ASSIGN_OR_RETURN(auto v, var_container.CreateInput(expr_names[i]));
    shared_node_tracker.AddParentVariable(exprs_vec[i], v->fingerprint());
  }

  // It is important to transform everything at once if there is some shared
  // named subtree, so we create a single tuple Expr.
  ASSIGN_OR_RETURN(
    ExprNodePtr combined,
    arolla::expr::BindOp("core.make_tuple", std::move(exprs_vec), {}));

  RETURN_IF_ERROR(
      shared_node_tracker.Process(combined, arolla::expr::IsNameAnnotation));

  // Extract variables from `combined` to `vars`.
  ASSIGN_OR_RETURN(combined,
                   ExtractAutoVariables(combined, shared_node_tracker, vars));

  if (combined->node_deps().size() != expr_names.size()) {
    return absl::InternalError("wrong deps count after transformation");
  }
  for (int64_t i = 0; i < expr_names.size(); ++i) {
    vars[expr_names[i]] =
        DataSlice::CreateFromScalar(ExprQuote{combined->node_deps()[i]});
  }
  auto returns = vars.extract(kReturnsAttrName);
  if (returns.empty()) {
    return absl::InternalError("no 'returns' after transformation");
  }
  std::vector<std::pair<std::string, DataSlice>> vars_vector(vars.begin(),
                                                             vars.end());
  return CreateFunctor(returns.mapped(), signature, vars_vector);
}

}  // namespace koladata::functor
