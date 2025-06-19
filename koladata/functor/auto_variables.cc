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
#include "koladata/functor/auto_variables.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
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
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/functor/functor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

using arolla::expr::ExprNodePtr;
using arolla::expr::ExprOperatorPtr;
using arolla::expr::ExprQuote;

absl::string_view kAuxVariablePrefix = "_aux";

absl::StatusOr<bool> IsSliceOrItemOperator(const ExprNodePtr& node) {
  if (!node->is_op()) {
    return false;
  }
  ASSIGN_OR_RETURN(ExprOperatorPtr slice_op,
                   arolla::expr::DecayRegisteredOperator(
                       arolla::expr::ExprOperatorRegistry::GetInstance()
                           ->LookupOperatorOrNull("kd.slice")));
  ASSIGN_OR_RETURN(ExprOperatorPtr item_op,
                   arolla::expr::DecayRegisteredOperator(
                       arolla::expr::ExprOperatorRegistry::GetInstance()
                           ->LookupOperatorOrNull("kd.item")));
  ASSIGN_OR_RETURN(auto op, arolla::expr::DecayRegisteredOperator(node->op()));
  return (op == slice_op) || (op == item_op);
}

// We want to avoid creating duplicate computations by extracting a sub-expr
// into multiple variables. This class finds shared sub-exprs, so we can extract
// them as well.
class SharedNodeTracker {
 public:
  explicit SharedNodeTracker(const arolla::expr::PostOrder& post_order)
      : post_order_(post_order),
        node_to_parent_tag_(post_order.nodes_size(), kUnitialized) {}

  void AddTopLevelVariable(size_t node_index, size_t variable_id) {
    AddParentTag(node_index, post_order_.nodes_size() + 1 + variable_id);
  }

  template <typename Fn>
  absl::Status Process(Fn&& to_be_extracted) {
    // Iterating in reverse post_order guarantees that we will see a node before
    // all its dependencies, so that we can check if it has multiple variable
    // parents.
    for (int64_t post_order_index = post_order_.nodes_size() - 1;
         post_order_index >= 0; post_order_index--) {
      const ExprNodePtr& node = post_order_.node(post_order_index);
      size_t parent_tag_for_children = post_order_index;
      if (!to_be_extracted(node)) {
        size_t parent_tag = node_to_parent_tag_[post_order_index];
        if (parent_tag == kUnitialized) {
          continue;
        }
        // If this node has a single parent variable, we propagate it to
        // children. Otherwise, we will create a variable for this node itself.
        if (parent_tag != kMultipleParent) {
          parent_tag_for_children = parent_tag;
        }
      }
      for (size_t child : post_order_.dep_indices(post_order_index)) {
        AddParentTag(child, parent_tag_for_children);
      }
    }
    return absl::OkStatus();
  }

  // Returns true if the node will be used by multiple extracted variables
  // (and thus need to become a variable as well).
  bool IsSharedNode(size_t post_order_index) const {
    return node_to_parent_tag_[post_order_index] == kMultipleParent;
  }

  const arolla::expr::PostOrder& GetPostOrder() const { return post_order_; }

 private:
  static constexpr size_t kUnitialized = ~size_t{};
  static constexpr size_t kMultipleParent = kUnitialized - 1;

  // Adds a parent tag for a node.
  // If the node already has a parent tag, then it means that the node is
  // shared and we should assign kMultipleParent tag to it.
  void AddParentTag(size_t node_index, size_t parent_tag) {
    DCHECK_NE(parent_tag, kUnitialized);
    DCHECK_NE(parent_tag, kMultipleParent);
    size_t& parent = node_to_parent_tag_[node_index];
    if (parent == kUnitialized) {
      parent = parent_tag;
    } else if (parent != parent_tag) {
      // If already assigned and has different value, then it a shared
      // node that will be used in several extracted variables.
      parent = kMultipleParent;
    }
  };

  const arolla::expr::PostOrder& post_order_;
  // This maps from a node id in post_order_ to the parent tag.
  // There are 4 possible values:
  // - kUnitialized: the node has not been assigned a parent yet.
  // - kMultipleParent: the node is shared and will be used in several
  //   extracted variables.
  // - post order id of the parent:
  //      the node has a single parent with the given id.
  // - special tag for top level variables: larger than post_order_.size().
  std::vector<size_t> node_to_parent_tag_;
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

bool IsDataSliceLiteral(const ExprNodePtr& node) {
  return expr::IsLiteral(node) && node->qvalue().has_value() &&
         node->qvalue()->GetType() == arolla::GetQType<DataSlice>();
}

bool IsUnspecifiedLiteral(const ExprNodePtr& node) {
  return expr::IsLiteral(node) && node->qvalue().has_value() &&
         node->qvalue()->GetType() == arolla::GetUnspecifiedQType();
};

template <typename IsExtractNeededFn>
absl::StatusOr<ExprNodePtr> ExtractAutoVariables(
    const ExprNodePtr& expr, const SharedNodeTracker& shared_node_tracker,
    absl::flat_hash_map<std::string, DataSlice>& vars,
    IsExtractNeededFn&& is_extract_needed) {
  ASSIGN_OR_RETURN(expr::InputContainer var_container,
                   expr::InputContainer::Create("V"));

  absl::flat_hash_set<arolla::Fingerprint> aux_variable_fingerprints;

  absl::flat_hash_map<std::string, int> prefix_to_counter;
  auto create_unique_variable = [&](absl::string_view prefix) {
    int& counter = prefix_to_counter[prefix];
    while (true) {
      std::string var_name = absl::StrCat(prefix, "_", counter++);
      if (!vars.contains(var_name)) {
        return var_name;
      }
    }
  };

  auto extract_slice = [&](DataSlice slice,
                           bool is_shared_node) -> absl::StatusOr<ExprNodePtr> {
    // We need to implode the DataSlice into lists if it is not a DataItem.
    std::string var_name = create_unique_variable(kAuxVariablePrefix);
    ASSIGN_OR_RETURN(auto var, var_container.CreateInput(var_name));
    int ndim = slice.GetShape().rank();
    if (ndim > 0) {
      ASSIGN_OR_RETURN(slice, Implode(DataBag::Empty(), slice, ndim));
      ASSIGN_OR_RETURN(
          var,
          arolla::expr::CallOp(
              "kd.explode",
              {var, koladata::expr::MakeLiteral(arolla::TypedValue::FromValue(
                        DataSlice::CreateFromScalar(ndim)))}));
    }
    vars.emplace(var_name, std::move(slice));
    // When a node has multiple parents, we cannot replace its aux name
    // with a wrapping name in the logic below, so we skip adding it to
    // aux_variable_fingerprints.
    if (!is_shared_node) {
      aux_variable_fingerprints.insert(var->fingerprint());
    }
    return var;
  };

  const auto& post_order = shared_node_tracker.GetPostOrder();

  size_t post_order_index = 0;
  auto transform_node = [&](ExprNodePtr node) -> absl::StatusOr<ExprNodePtr> {
    bool is_shared_node = shared_node_tracker.IsSharedNode(post_order_index);
    bool extract_needed = is_extract_needed(post_order.node(post_order_index));
    post_order_index++;
    if (expr::IsLiteral(node)) {
      if (!node->qvalue().has_value()) {
        return absl::FailedPreconditionError("literal has no value");
      }
      if (node->qvalue()->GetType() == arolla::GetQType<DataSlice>()) {
        DataSlice val = node->qvalue()->UnsafeAs<DataSlice>();
        if (!IsSimpleSlice(val)) {
          return extract_slice(val, is_shared_node);
        }
      }
      if (!extract_needed) {
        return node;
      }
    }
    ASSIGN_OR_RETURN(bool is_slice_op_node, IsSliceOrItemOperator(node));
    if (is_slice_op_node) {
      // Our binding policy for kd.slice/kd.item produces
      // kd.slice/kd.item(<literal slice>).
      // When that is named we want the name to be used instead of aux_,
      // so we remove the unnecessary slice()/item() wrapper in that case.
      const auto& val = node->node_deps()[0];
      const auto& schema = node->node_deps()[1];
      if ((IsDataSliceLiteral(val) ||
           aux_variable_fingerprints.contains(val->fingerprint())) &&
          IsUnspecifiedLiteral(schema)) {
        return val;
      }
    }
    if (!extract_needed && (!is_shared_node || IsSimple(node))) {
      return node;
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
      if (IsDataSliceLiteral(child)) {
        DataSlice val = child->qvalue()->UnsafeAs<DataSlice>();
        if (!IsSimpleSlice(val)) {
          // Non-simple slices should have been extracted above via
          // extract_slice, so we should never reach this branch.
          return absl::InternalError("this should never happen");
        }
        vars.emplace(name, std::move(val));
        return var_container.CreateInput(name);
      }
      vars.emplace(name, DataSlice::CreateFromScalar(ExprQuote{child}));
      return var_container.CreateInput(name);
    }
    std::string var_name = create_unique_variable(kAuxVariablePrefix);
    vars[var_name] = DataSlice::CreateFromScalar(ExprQuote{node});
    return var_container.CreateInput(var_name);
  };

  return arolla::expr::TransformOnPostOrder(post_order, transform_node);
}

}  // namespace

absl::StatusOr<DataSlice> AutoVariables(
    const DataSlice& functor,
    absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract) {
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) return absl::InvalidArgumentError("functor expected");

  ASSIGN_OR_RETURN(DataSlice signature, functor.GetAttr(kSignatureAttrName));
  ASSIGN_OR_RETURN(auto attr_names, functor.GetAttrNames());

  absl::flat_hash_map<std::string, DataSlice> vars;
  std::vector<std::string> expr_names;
  std::vector<ExprNodePtr> exprs_vec;

  vars.reserve(attr_names.size());
  expr_names.reserve(attr_names.size());
  exprs_vec.reserve(attr_names.size());

  for (const auto& attr_name : attr_names) {
    if (attr_name == kSignatureAttrName) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSlice var, functor.GetAttr(attr_name));
    DCHECK(var.is_item());
    if (var.is_item() &&
        var.GetSchemaImpl() == internal::DataItem(schema::kExpr)) {
      ASSIGN_OR_RETURN(auto var_expr, var.item().value<ExprQuote>().expr());
      // Already a variable, so we shouldn't extract it again (would lead to
      // trivial assignments like V.aux_1 = V.aux_0).
      extra_nodes_to_extract.erase(var_expr->fingerprint());

      expr_names.push_back(attr_name);
      exprs_vec.push_back(std::move(var_expr));
    }
    vars.emplace(attr_name, std::move(var));
  }

  // It is important to transform everything at once if there is some shared
  // named subtree, so we create a single tuple Expr.
  ASSIGN_OR_RETURN(
      ExprNodePtr combined,
      arolla::expr::BindOp("core.make_tuple", std::move(exprs_vec), {}));

  arolla::expr::PostOrder post_order(combined);
  SharedNodeTracker shared_node_tracker(post_order);
  size_t root_node_index = post_order.nodes_size() - 1;
  // Fake parent index for the top level variables.
  // It is important for the case, where variables point to exactly the same
  // computation graph.
  size_t var_id = 0;
  for (size_t child_index : post_order.dep_indices(root_node_index)) {
    shared_node_tracker.AddTopLevelVariable(child_index, var_id++);
  }

  auto is_extract_needed = [&extra_nodes_to_extract](const ExprNodePtr& node) {
    return arolla::expr::IsNameAnnotation(node) ||
           extra_nodes_to_extract.contains(node->fingerprint());
  };

  RETURN_IF_ERROR(shared_node_tracker.Process(is_extract_needed));

  // Extract variables from `combined` to `vars`.
  ASSIGN_OR_RETURN(combined, ExtractAutoVariables(combined, shared_node_tracker,
                                                  vars, is_extract_needed));

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
  std::vector<absl::string_view> var_names;
  std::vector<DataSlice> var_values;
  var_names.reserve(vars.size());
  var_values.reserve(vars.size());
  for (auto& [name, value] : vars) {
    var_names.emplace_back(name);
    var_values.emplace_back(std::move(value));
  }
  return CreateFunctor(returns.mapped(), signature, std::move(var_names),
                       std::move(var_values));
}

}  // namespace koladata::functor
