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
#include "py/koladata/operators/py_unified_binding_policy.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/expr/expr_operators.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::MakeNamedTuple;
using ::arolla::MakeTuple;
using ::arolla::Text;
using ::arolla::TypedRef;
using ::arolla::TypedValue;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNode;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::Leaf;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::RegisteredOperator;
using ::arolla::python::AuxBindingPolicy;
using ::arolla::python::DCheckPyGIL;
using ::arolla::python::IsPyExprInstance;
using ::arolla::python::IsPyQValueInstance;
using ::arolla::python::PyErr_FormatFromCause;
using ::arolla::python::PyObjectPtr;
using ::arolla::python::PyTuple_AsSpan;
using ::arolla::python::QValueOrExpr;
using ::arolla::python::SetPyErrFromStatus;
using ::arolla::python::Signature;
using ::arolla::python::UnsafeUnwrapPyExpr;
using ::arolla::python::UnsafeUnwrapPyQValue;
using ::koladata::expr::kNonDeterministicTokenLeafKey;
using ::koladata::expr::LiteralOperator;

// Extracts and stores the `<options>` part of the `aux_policy` string for the
// unified binding into the output parameter `result`. If the function is
// successful, it returns `true`. Otherwise, it returns `false` and sets the
// appropriate Python exception.
//
bool ExtractUnifiedPolicyOpts(const ExprOperatorSignature& signature,
                              absl::string_view& result) {
  result = signature.aux_policy;
  if (!absl::ConsumePrefix(&result, kUnifiedPolicy) ||
      !absl::ConsumePrefix(&result, ":")) {
    return PyErr_Format(
        PyExc_RuntimeError,
        "UnifiedBindingPolicy: unexpected binding policy name: %s",
        absl::Utf8SafeCHexEscape(signature.aux_policy).c_str());
  }
  if (result.size() != signature.parameters.size()) {
    return PyErr_Format(
        PyExc_RuntimeError,
        "UnifiedBindingPolicy: mismatch between the number of options and "
        "parameters: len(aux_policy_opts)=%zu, "
        "len(signature.parameters)=%zu",
        result.size(), signature.parameters.size());
  }
  return true;
}

// A sentinel entity indicating to use the default value.
static PyObject kSentinelDefaultValue;
// A sentinel entity indicating to use `py_result_var_args`.
static PyObject kSentinelVarArgs;
// A sentinel entity indicating to use `py_result_var_kwargs`.
static PyObject kSentinelVarKwargs;
// A sentinel entity indicating to use a non-deterministc expression.
static PyObject kSentinelNonDeterministic;

// A lower-level binding-arguments function without boxing python values.
// This function processes arguments for the given "unified" operator
// signature and populates the output parameters (`py_result*`).
//
// The main result is stored in `py_result`, which has a one-to-one mapping
// with the signature parameters. If `py_result[i]` points to
// `kSentinelDefaultValue`, it indicates that the argument's value should be
// taken from the signature's default (the function ensures that a default
// value exists). Similarly, a pointer to `kSentinelVarArgs` or
// `kSentinelVarKwargs` indicates that the value should be taken from
// `py_result_var_args` or `py_result_var_kwargs`, respectively.
//
// If the function is successful, it returns `true`. Otherwise, it returns
// `false` and sets the corresponding Python exception.
//
// Note: The order of keys in `py_result_var_kwargs` is defined by the memory
// addresses of the stored values, which are of type `PyObject**`.
//
bool PrebindUnifiedArguments(
    const ExprOperatorSignature& signature, PyObject** py_args,
    Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
    std::vector<PyObject*>& py_result,
    absl::Span<PyObject*>& py_result_var_args,
    absl::flat_hash_map<absl::string_view, PyObject**>& py_result_var_kwargs) {
  absl::string_view opts;
  if (!ExtractUnifiedPolicyOpts(signature, opts)) {
    return false;
  }

  // Preprocess `*args`, `**kwargs`.
  const size_t py_args_size = PyVectorcall_NARGS(nargsf);
  absl::flat_hash_map<absl::string_view, PyObject**> py_kwargs;
  {
    absl::Span<PyObject*> py_kwnames;
    PyTuple_AsSpan(py_tuple_kwnames, &py_kwnames);
    py_kwargs.reserve(py_kwnames.size());
    for (size_t i = 0; i < py_kwnames.size(); ++i) {
      Py_ssize_t kwname_size = 0;
      const char* kwname_data =
          PyUnicode_AsUTF8AndSize(py_kwnames[i], &kwname_size);
      if (kwname_data == nullptr) {
        return false;
      }
      py_kwargs[absl::string_view(kwname_data, kwname_size)] =
          py_args + py_args_size + i;
    }
  }

  py_result.reserve(opts.size());
  size_t i = 0;

  // Bind the positional parameters using `*args`.
  for (; i < opts.size() && i < py_args_size; ++i) {
    DCHECK_EQ(py_result.size(), i);
    const char opt = opts[i];
    const auto& param = signature.parameters[i];
    if (opt == kUnifiedPolicyOptPositionalOnly) {
      py_result.push_back(py_args[i]);
    } else if (opt == kUnifiedPolicyOptPositionalOrKeyword) {
      if (py_kwargs.contains(param.name)) {
        return PyErr_Format(PyExc_TypeError,
                            "multiple values for argument '%s'",
                            absl::Utf8SafeCHexEscape(param.name).c_str());
      }
      py_result.push_back(py_args[i]);
    } else {
      break;
    }
  }
  bool has_unprocessed_args = false;
  if (i < opts.size() && opts[i] == kUnifiedPolicyOptVarPositional) {
    py_result.push_back(&kSentinelVarArgs);
    py_result_var_args = absl::Span<PyObject*>(py_args + i, py_args_size - i);
    i += 1;
  } else {
    has_unprocessed_args = (i < py_args_size);
  }

  // Bind remaining parameters using `**kwargs` and the default values.
  std::vector<absl::string_view> missing_positional_params;
  std::vector<absl::string_view> missing_keyword_only_params;
  for (; i < opts.size(); ++i) {
    const char opt = opts[i];
    const auto& param = signature.parameters[i];
    if (opt == kUnifiedPolicyOptPositionalOnly) {
      if (param.default_value.has_value()) {
        py_result.push_back(&kSentinelDefaultValue);
      } else {
        missing_positional_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptPositionalOrKeyword) {
      auto it = py_kwargs.find(param.name);
      if (it != py_kwargs.end()) {
        py_result.push_back(*it->second);
        py_kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        py_result.push_back(&kSentinelDefaultValue);
      } else {
        missing_positional_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptVarPositional) {
      py_result.push_back(&kSentinelVarArgs);
    } else if (opt == kUnifiedPolicyOptRequiredKeywordOnly) {
      auto it = py_kwargs.find(param.name);
      if (it != py_kwargs.end()) {
        py_result.push_back(*it->second);
        py_kwargs.erase(it);
      } else {
        missing_keyword_only_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptOptionalKeywordOnly) {
      auto it = py_kwargs.find(param.name);
      if (it != py_kwargs.end()) {
        py_result.push_back(*it->second);
        py_kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        py_result.push_back(&kSentinelDefaultValue);
      } else {
        // Gracefully handle a missing default value for an optional parameter.
        missing_keyword_only_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptVarKeyword) {
      py_result.push_back(&kSentinelVarKwargs);
      py_result_var_kwargs = std::move(py_kwargs);
      py_kwargs.clear();  // Ensure `py_kwargs` state after value move.
    } else if (opt == kUnifiedPolicyOptNonDeterministic) {
      py_result.push_back(&kSentinelNonDeterministic);
    } else {
      return PyErr_Format(
          PyExc_RuntimeError,
          "UnifiedBindingPolicy: unexpected option='%s', param='%s'",
          absl::Utf8SafeCHexEscape(opts.substr(i, 1)).c_str(),
          absl::Utf8SafeCHexEscape(param.name).c_str());
    }
  }

  // Report for missing arguments.
  if (missing_positional_params.size() == 1) {
    return PyErr_Format(
        PyExc_TypeError, "missing 1 required positional argument: '%s'",
        absl::Utf8SafeCHexEscape(missing_positional_params[0]).c_str());
  } else if (missing_positional_params.size() > 1) {
    std::ostringstream message;
    message << "missing " << missing_positional_params.size()
            << " required positional arguments: ";
    for (size_t j = 0; j + 1 < missing_positional_params.size(); ++j) {
      if (j > 0) {
        message << ", ";
      }
      message << "'" << absl::Utf8SafeCHexEscape(missing_positional_params[j])
              << "'";
    }
    message << " and '"
            << absl::Utf8SafeCHexEscape(missing_positional_params.back())
            << "'";
    PyErr_SetString(PyExc_TypeError, std::move(message).str().c_str());
    return false;
  }
  if (missing_keyword_only_params.size() == 1) {
    return PyErr_Format(
        PyExc_TypeError, "missing 1 required keyword-only argument: '%s'",
        absl::Utf8SafeCHexEscape(missing_keyword_only_params[0]).c_str());
  } else if (missing_keyword_only_params.size() > 1) {
    std::ostringstream message;
    message << "missing " << missing_keyword_only_params.size()
            << " required keyword-only arguments: ";
    for (size_t j = 0; j + 1 < missing_keyword_only_params.size(); ++j) {
      if (j > 0) {
        message << ", ";
      }
      message << "'" << absl::Utf8SafeCHexEscape(missing_keyword_only_params[j])
              << "'";
    }
    message << " and '"
            << absl::Utf8SafeCHexEscape(missing_keyword_only_params.back())
            << "'";
    PyErr_SetString(PyExc_TypeError, std::move(message).str().c_str());
    return false;
  }

  if (has_unprocessed_args) {
    size_t count_positionals = 0;
    size_t count_required_positionals = 0;
    for (size_t j = 0; j < opts.size(); ++j) {
      if (opts[j] == kUnifiedPolicyOptPositionalOnly ||
          opts[j] == kUnifiedPolicyOptPositionalOrKeyword) {
        count_positionals += 1;
        count_required_positionals +=
            !signature.parameters[j].default_value.has_value();
      }
    }
    if (count_positionals == count_required_positionals) {
      if (count_positionals == 1) {
        PyErr_Format(PyExc_TypeError,
                     "takes 1 positional argument but %zu were given",
                     py_args_size);
      } else {
        PyErr_Format(PyExc_TypeError,
                     "takes %zu positional arguments but %zu were given",
                     count_positionals, py_args_size);
      }
    } else {
      PyErr_Format(
          PyExc_TypeError,
          "takes from %zu to %zu positional arguments but %zu were given",
          count_required_positionals, count_positionals, py_args_size);
    }
    return false;
  }
  if (!py_kwargs.empty()) {
    auto it = std::min_element(py_kwargs.begin(), py_kwargs.end(),
                               [](const auto& lhs, const auto& rhs) {
                                 return lhs.second < rhs.second;
                               });
    return PyErr_Format(PyExc_TypeError, "an unexpected keyword argument: '%s'",
                        absl::Utf8SafeCHexEscape(it->first).c_str());
  }

  DCHECK_EQ(py_result.size(), opts.size());
  return true;
}

// Returns a koldata literal.
ExprNodePtr MakeKdLiteral(TypedValue value) {
  ExprAttributes attr(value);
  return ExprNode::UnsafeMakeOperatorNode(
      std::make_shared<LiteralOperator>(std::move(value)), {}, std::move(attr));
}

// Returns a non-deterministic token.
absl::StatusOr<ExprNodePtr> GenNonDeterministicToken() {
  static const absl::NoDestructor op(
      std::make_shared<RegisteredOperator>("koda_internal.non_deterministic"));
  static const absl::NoDestructor leaf(Leaf(kNonDeterministicTokenLeafKey));
  static absl::NoDestructor<absl::BitGen> bitgen;
  auto seed = absl::Uniform<int64_t>(absl::IntervalClosed, *bitgen, 0,
                                     std::numeric_limits<int64_t>::max());
  return MakeOpNode(*op, {*leaf, Literal(seed)});
}

// Returns a Python function `koladata.types.py_boxing.as_qvalue_or_expr` if
// successful. Otherwise, returns `nullptr` and sets a Python exception.
PyObjectPtr GetPyCallableAsQValueOrExprFn() {
  static PyObject* module_name =
      PyUnicode_InternFromString("koladata.types.py_boxing");
  auto py_module = PyObjectPtr::Own(PyImport_GetModule(module_name));
  if (py_module != nullptr) {
    return PyObjectPtr::Own(
        PyObject_GetAttrString(py_module.get(), "as_qvalue_or_expr"));
  }
  return PyObjectPtr{};
}

// Returns QValue|Expr if successful. Otherwise, returns `std::nullopt` and
// sets a Python exception.
std::optional<QValueOrExpr> AsQValueOrExpr(
    PyObject* py_callable_as_qvalue_or_expr, PyObject* py_arg) {
  // Forward QValues and Exprs unchanged.
  if (IsPyExprInstance(py_arg)) {
    return UnsafeUnwrapPyExpr(py_arg);
  } else if (IsPyQValueInstance(py_arg)) {
    return UnsafeUnwrapPyQValue(py_arg);
  }
  auto py_result = PyObjectPtr::Own(
      PyObject_CallOneArg(py_callable_as_qvalue_or_expr, py_arg));
  if (py_result == nullptr) {
    return std::nullopt;
  }
  if (IsPyExprInstance(py_result.get())) {
    return UnsafeUnwrapPyExpr(py_result.get());
  } else if (IsPyQValueInstance(py_result.get())) {
    return UnsafeUnwrapPyQValue(py_result.get());
  }
  PyErr_Format(
      PyExc_RuntimeError,
      "expected QValue or Expr, but as_qvalue_or_expr(arg: %s) returned %s",
      Py_TYPE(py_arg)->tp_name, Py_TYPE(py_result.get())->tp_name);
  return std::nullopt;
}

// Boxing for non-variadic arguments.
std::optional<QValueOrExpr> AsQValueOrExprArg(
    PyObject* py_callable_as_qvalue_or_expr, absl::string_view arg_name,
    PyObject* py_arg) {
  if (auto result = AsQValueOrExpr(py_callable_as_qvalue_or_expr, py_arg)) {
    return result;
  }
  // Add context for TypeError and ValueError, and forward any other
  // exceptions.
  auto* py_exc = PyErr_Occurred();
  if (PyErr_GivenExceptionMatches(py_exc, PyExc_TypeError) ||
      PyErr_GivenExceptionMatches(py_exc, PyExc_ValueError)) {
    PyErr_FormatFromCause(py_exc,
                          "unable to represent argument `%s` as QValue or Expr",
                          absl::Utf8SafeCHexEscape(arg_name).c_str());
  }
  return std::nullopt;
}

// Boxing for variadic-positional arguments.
std::optional<QValueOrExpr> AsQValueOrExprVarArgs(
    PyObject* py_callable_as_qvalue_or_expr, absl::string_view arg_name,
    absl::Span<PyObject* const> py_args) {
  if (py_args.empty()) {  // Fast empty case.
    static const absl::NoDestructor<QValueOrExpr> kEmptyTuple(
        MakeTuple(absl::Span<TypedRef>{}));
    return *kEmptyTuple;
  }
  // Apply the boxing rules to the arguments.
  std::vector<QValueOrExpr> args;
  args.reserve(py_args.size());
  bool has_exprs = false;
  for (size_t i = 0; i < py_args.size(); ++i) {
    auto arg = AsQValueOrExpr(py_callable_as_qvalue_or_expr, py_args[i]);
    if (!arg.has_value()) {
      // Add context for TypeError and ValueError, and forward any other
      // exceptions.
      auto* py_exc = PyErr_Occurred();
      if (PyErr_GivenExceptionMatches(py_exc, PyExc_TypeError) ||
          PyErr_GivenExceptionMatches(py_exc, PyExc_ValueError)) {
        PyErr_FormatFromCause(
            py_exc, "unable to represent argument `%s[%zu]` as QValue or Expr",
            absl::Utf8SafeCHexEscape(arg_name).c_str(), i);
      }
      return std::nullopt;
    }
    has_exprs = has_exprs || std::holds_alternative<ExprNodePtr>(*arg);
    args.push_back(*std::move(arg));
  }
  // If all arguments end up to be values, construct a value; otherwise
  // make an expression.
  if (!has_exprs) {
    std::vector<TypedRef> field_values;
    field_values.reserve(args.size());
    for (auto& arg : args) {
      field_values.push_back(std::get_if<TypedValue>(&arg)->AsRef());
    }
    return MakeTuple(field_values);
  }
  std::vector<ExprNodePtr> node_deps;
  node_deps.reserve(args.size());
  for (auto& arg : args) {
    if (auto* expr = std::get_if<ExprNodePtr>(&arg)) {
      node_deps.push_back(std::move(*expr));
    } else {
      node_deps.push_back(
          MakeKdLiteral(std::move(*std::get_if<TypedValue>(&arg))));
    }
  }
  static const absl::NoDestructor make_tuple_op(
      std::make_shared<RegisteredOperator>("core.make_tuple"));
  ASSIGN_OR_RETURN(auto result,
                   MakeOpNode(*make_tuple_op, std::move(node_deps)),
                   (SetPyErrFromStatus(_), std::nullopt));
  return result;
}

// Boxing for variadic-keyword arguments.
std::optional<QValueOrExpr> AsQValueOrExprVarKwArgs(
    PyObject* py_callable_as_qvalue_or_expr,
    const absl::flat_hash_map<absl::string_view, PyObject**>& py_kwargs) {
  const size_t n = py_kwargs.size();
  if (n == 0) {  // Fast empty case.
    static const absl::NoDestructor<QValueOrExpr> kEmptyNamedtuple(
        *MakeNamedTuple({}, absl::Span<const TypedRef>{}));
    return *kEmptyNamedtuple;
  }
  std::vector<std::pair<absl::string_view, PyObject**>> ordered_py_kwargs(
      py_kwargs.begin(), py_kwargs.end());
  std::sort(
      ordered_py_kwargs.begin(), ordered_py_kwargs.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
  // Apply the boxing rules to the arguments.
  std::vector<QValueOrExpr> args;
  args.reserve(n);
  bool has_exprs = false;
  for (const auto& [k, pptr] : ordered_py_kwargs) {
    auto arg = AsQValueOrExpr(py_callable_as_qvalue_or_expr, *pptr);
    if (!arg.has_value()) {
      // Add context for TypeError and ValueError, and forward any other
      // exceptions.
      auto* py_exc = PyErr_Occurred();
      if (PyErr_GivenExceptionMatches(py_exc, PyExc_TypeError) ||
          PyErr_GivenExceptionMatches(py_exc, PyExc_ValueError)) {
        PyErr_FormatFromCause(
            py_exc, "unable to represent argument `%s` as QValue or Expr",
            absl::Utf8SafeCHexEscape(k).c_str());
      }
      return std::nullopt;
    }
    has_exprs = has_exprs || std::holds_alternative<ExprNodePtr>(*arg);
    args.push_back(*std::move(arg));
  }
  // If all arguments end up to be values, construct a value; otherwise
  // make an expression.
  if (!has_exprs) {
    std::vector<std::string> field_names;
    std::vector<TypedRef> field_values;
    field_names.reserve(n);
    field_values.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      field_names.emplace_back(ordered_py_kwargs[i].first);
      field_values.emplace_back(std::get_if<TypedValue>(&args[i])->AsRef());
    }
    ASSIGN_OR_RETURN(auto result, MakeNamedTuple(field_names, field_values),
                     (SetPyErrFromStatus(_), std::nullopt));
    return result;
  }
  std::string field_names;
  std::vector<ExprNodePtr> node_deps;
  node_deps.reserve(1 + n);
  node_deps.emplace_back(nullptr);
  for (size_t i = 0; i < n; ++i) {
    if (i > 0) {
      absl::StrAppend(&field_names, ",");
    }
    absl::StrAppend(&field_names, ordered_py_kwargs[i].first);
    if (auto* expr = std::get_if<ExprNodePtr>(&args[i])) {
      node_deps.push_back(std::move(*expr));
    } else {
      node_deps.push_back(
          MakeKdLiteral(std::move(*std::get_if<TypedValue>(&args[i]))));
    }
  }
  node_deps[0] = Literal(Text(std::move(field_names)));
  static const absl::NoDestructor make_namedtuple_op(
      std::make_shared<RegisteredOperator>("namedtuple.make"));
  ASSIGN_OR_RETURN(auto result,
                   MakeOpNode(*make_namedtuple_op, std::move(node_deps)),
                   (SetPyErrFromStatus(_), std::nullopt));
  return result;
}

// The unified binding policy class.
class UnifiedBindingPolicy : public AuxBindingPolicy {
  PyObject* MakePythonSignature(
      const ExprOperatorSignature& signature) const final {
    DCheckPyGIL();
    absl::string_view opts;
    if (!ExtractUnifiedPolicyOpts(signature, opts)) {
      return nullptr;
    }
    Signature result;
    result.parameters.reserve(opts.size());
    for (size_t i = 0; i < opts.size(); ++i) {
      const char opt = opts[i];
      const auto& param = signature.parameters[i];
      if (opt == kUnifiedPolicyOptPositionalOnly) {
        result.parameters.push_back(
            {param.name, "positional-only", param.default_value});
      } else if (opt == kUnifiedPolicyOptPositionalOrKeyword) {
        result.parameters.push_back(
            {param.name, "positional-or-keyword", param.default_value});
      } else if (opt == kUnifiedPolicyOptVarPositional) {
        result.parameters.push_back({param.name, "variadic-positional"});
      } else if (opt == kUnifiedPolicyOptRequiredKeywordOnly) {
        result.parameters.push_back({param.name, "keyword-only"});
      } else if (opt == kUnifiedPolicyOptOptionalKeywordOnly) {
        result.parameters.push_back(
            {param.name, "keyword-only", param.default_value});
      } else if (opt == kUnifiedPolicyOptVarKeyword) {
        result.parameters.push_back({param.name, "variadic-keyword"});
      } else if (opt == kUnifiedPolicyOptNonDeterministic) {
        // pass
      } else {
        return PyErr_Format(
            PyExc_RuntimeError,
            "UnifiedBindingPolicy: unexpected option='%s', param='%s'",
            absl::Utf8SafeCHexEscape(opts.substr(i, 1)).c_str(),
            absl::Utf8SafeCHexEscape(param.name).c_str());
      }
    }
    return WrapAsPySignature(result);
  }

  // (See the base class.)
  bool BindArguments(
      const ExprOperatorSignature& signature, PyObject** py_args,
      Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
      absl::Nonnull<std::vector<QValueOrExpr>*> result) const final {
    DCheckPyGIL();
    std::vector<PyObject*> py_result;
    absl::Span<PyObject*> py_result_var_args;
    absl::flat_hash_map<absl::string_view, PyObject**> py_result_var_kwargs;
    if (!PrebindUnifiedArguments(signature, py_args, nargsf, py_tuple_kwnames,
                                 py_result, py_result_var_args,
                                 py_result_var_kwargs)) {
      return false;
    }
    const auto py_callable_as_qvalue_or_expr = GetPyCallableAsQValueOrExprFn();
    if (py_callable_as_qvalue_or_expr == nullptr) {
      return false;
    }
    DCHECK_EQ(py_result.size(), signature.parameters.size());
    result->reserve(py_result.size());
    for (size_t i = 0; i < py_result.size(); ++i) {
      const auto& param = signature.parameters[i];
      DCHECK_NE(py_result[i], nullptr);
      if (py_result[i] == &kSentinelDefaultValue) {
        DCHECK(param.default_value.has_value());
        result->push_back(*param.default_value);
      } else if (py_result[i] == &kSentinelVarArgs) {
        auto arg = AsQValueOrExprVarArgs(py_callable_as_qvalue_or_expr.get(),
                                         param.name, py_result_var_args);
        if (!arg.has_value()) {
          return false;
        }
        result->push_back(*std::move(arg));
      } else if (py_result[i] == &kSentinelVarKwargs) {
        auto arg = AsQValueOrExprVarKwArgs(py_callable_as_qvalue_or_expr.get(),
                                           py_result_var_kwargs);
        if (!arg.has_value()) {
          return false;
        }
        result->push_back(*std::move(arg));
      } else if (py_result[i] == &kSentinelNonDeterministic) {
        ASSIGN_OR_RETURN(auto arg, GenNonDeterministicToken(),
                         (SetPyErrFromStatus(_), false));
        result->push_back(std::move(arg));
      } else {
        auto arg = AsQValueOrExprArg(py_callable_as_qvalue_or_expr.get(),
                                     param.name, py_result[i]);
        if (!arg.has_value()) {
          return false;
        }
        result->push_back(*std::move(arg));
      }
    }
    return true;
  }

  // (See the base class.)
  absl::Nullable<ExprNodePtr> DoMakeLiteral(TypedValue&& value) const final {
    DCheckPyGIL();
    return MakeKdLiteral(std::move(value));
  }
};

}  // namespace

bool RegisterUnifiedBindingPolicy() {
  return RegisterAuxBindingPolicy(kUnifiedPolicy,
                                  std::make_shared<UnifiedBindingPolicy>());
}

}  // namespace koladata::python