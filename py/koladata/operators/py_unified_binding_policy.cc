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
#include "py/koladata/operators/py_unified_binding_policy.h"

#include <Python.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/expr/expr_operators.h"
#include "koladata/expr/non_determinism.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_signature.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
namespace {

using ::arolla::MakeEmptyNamedTuple;
using ::arolla::MakeEmptyTuple;
using ::arolla::MakeNamedTuple;
using ::arolla::MakeTuple;
using ::arolla::Text;
using ::arolla::TypedRef;
using ::arolla::TypedValue;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::RegisteredOperator;
using ::arolla::python::AuxBindingPolicy;
using ::arolla::python::DCheckPyGIL;
using ::arolla::python::IsPyExprInstance;
using ::arolla::python::IsPyQValueInstance;
using ::arolla::python::PyErr_AddNote;
using ::arolla::python::PyObjectPtr;
using ::arolla::python::PyTuple_AsSpan;
using ::arolla::python::QValueOrExpr;
using ::arolla::python::SetPyErrFromStatus;
using ::arolla::python::Signature;
using ::arolla::python::UnsafeUnwrapPyExpr;
using ::arolla::python::UnsafeUnwrapPyQValue;

// Extracts `<binding_options>` and '<boxing_options>' parts of the `aux_policy`
// string for the unified binding into the output parameter `result`. If the
// function is successful, it returns `true`. Otherwise, it returns `false` and
// sets the appropriate Python exception.
//
bool ExtractUnifiedPolicyOpts(const ExprOperatorSignature& signature,
                              absl::string_view& result_binding_options,
                              absl::string_view& result_boxing_options) {
  if (signature.aux_policy_name != kUnifiedPolicy) {
    return PyErr_Format(
        PyExc_RuntimeError,
        "UnifiedBindingPolicy: unexpected binding policy name: %s",
        absl::Utf8SafeCHexEscape(signature.aux_policy_name).c_str());
  }
  absl::string_view string = signature.aux_policy_options;
  const auto idx = string.find(':');
  if (idx != absl::string_view::npos) {
    result_binding_options = string.substr(0, idx);
    result_boxing_options = string.substr(idx + 1);
  } else {
    result_binding_options = string;
    result_boxing_options = {};
  }
  return true;
}

// A sentinel entity indicating to use the default value.
static PyObject kSentinelDefaultValue;
// A sentinel entity indicating to use `py_var_args`.
static PyObject kSentinelVarArgs;
// A sentinel entity indicating to use `py_var_kwargs`.
static PyObject kSentinelVarKwargs;
// A sentinel entity indicating to use a non-deterministic expression.
static PyObject kSentinelNonDeterministic;

// Note: The order of keys in `PyVarKwargs` is defined by the memory
// addresses of the stored values, which are of type `PyObject**`.
using PyVarKwarg = absl::flat_hash_map<absl::string_view, PyObject**>;

void ReportMissingPositionalParameters(
    absl::Span<const absl::string_view> missing_positional_params);

void ReportMissingKeywordOnlyParameters(
    absl::Span<const absl::string_view> missing_keyword_only_params);

void ReportUnprocessedPositionalArguments(
    const ExprOperatorSignature& signature, absl::string_view binding_options,
    size_t py_args_size);

void ReportUnprocessedKeywordArguments(const PyVarKwarg& py_var_kwargs);

// A lower-level binding-arguments function without boxing python values.
// This function processes arguments for the given "unified" operator
// signature and populates the output parameters (`result_py_*`).
//
// The main result is stored in `result_py_bound_args`, which has a one-to-one
// mapping with the signature parameters. If `result_py_bound_args[i]` points to
// `kSentinelDefaultValue`, it indicates that the argument's value should be
// taken from the signature's default (the function ensures that a default
// value exists). Similarly, a pointer to `kSentinelVarArgs` or
// `kSentinelVarKwargs` indicates that the value should be taken from
// `result_py_var_args` or `result_py_var_kwargs`, respectively.
//
// If the function is successful, it returns `true`. Otherwise, it returns
// `false` and sets the corresponding Python exception.
bool UnifiedBindArguments(const ExprOperatorSignature& signature,
                          absl::string_view binding_options, PyObject** py_args,
                          Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
                          std::vector<PyObject*>& result_py_bound_args,
                          absl::Span<PyObject*>& result_py_var_args,
                          PyVarKwarg& result_py_var_kwargs) {
  const auto& params = signature.parameters;
  const size_t py_args_size = PyVectorcall_NARGS(nargsf);
  if (binding_options.size() != params.size()) {
    return PyErr_Format(PyExc_RuntimeError,
                        "UnifiedBindingPolicy: mismatch between "
                        "the binding_options and "
                        "parameters: len(binding_options)=%zu, "
                        "len(signature.parameters)=%zu",
                        binding_options.size(), params.size());
  }

  // Load keyword arguments into a `py_var_kwargs` hashtable.
  PyVarKwarg py_var_kwargs;
  {
    absl::Span<PyObject*> py_kwnames;
    PyTuple_AsSpan(py_tuple_kwnames, &py_kwnames);
    py_var_kwargs.reserve(py_kwnames.size());
    for (size_t i = 0; i < py_kwnames.size(); ++i) {
      Py_ssize_t kwname_size = 0;
      const char* kwname_data =
          PyUnicode_AsUTF8AndSize(py_kwnames[i], &kwname_size);
      if (kwname_data == nullptr) {
        return false;
      }
      py_var_kwargs[absl::string_view(kwname_data, kwname_size)] =
          py_args + py_args_size + i;
    }
  }

  result_py_bound_args.reserve(params.size());
  size_t i = 0;

  // Process positional arguments.
  for (; i < params.size() && i < py_args_size; ++i) {
    DCHECK_EQ(result_py_bound_args.size(), i);
    const auto& param = params[i];
    const char opt = binding_options[i];
    if (opt == kUnifiedPolicyOptPositionalOnly) {
      result_py_bound_args.push_back(py_args[i]);
    } else if (opt == kUnifiedPolicyOptPositionalOrKeyword) {
      if (py_var_kwargs.contains(param.name)) {
        return PyErr_Format(PyExc_TypeError,
                            "multiple values for argument '%s'",
                            absl::Utf8SafeCHexEscape(param.name).c_str());
      }
      result_py_bound_args.push_back(py_args[i]);
    } else {
      break;
    }
  }
  bool has_unprocessed_positional_arguments = false;
  if (i < params.size() &&
      binding_options[i] == kUnifiedPolicyOptVarPositional) {
    result_py_bound_args.push_back(&kSentinelVarArgs);
    result_py_var_args = absl::Span<PyObject*>(py_args + i, py_args_size - i);
    i += 1;
  } else {
    has_unprocessed_positional_arguments = (i < py_args_size);
  }

  // Bind remaining parameters using keyword arguments and the default values.
  std::vector<absl::string_view> missing_positional_params;
  std::vector<absl::string_view> missing_keyword_only_params;
  for (; i < params.size(); ++i) {
    const auto& param = params[i];
    const char opt = binding_options[i];
    if (opt == kUnifiedPolicyOptPositionalOnly) {
      if (param.default_value.has_value()) {
        result_py_bound_args.push_back(&kSentinelDefaultValue);
      } else {
        missing_positional_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptPositionalOrKeyword) {
      auto it = py_var_kwargs.find(param.name);
      if (it != py_var_kwargs.end()) {
        result_py_bound_args.push_back(*it->second);
        py_var_kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        result_py_bound_args.push_back(&kSentinelDefaultValue);
      } else {
        missing_positional_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptVarPositional) {
      result_py_bound_args.push_back(&kSentinelVarArgs);
    } else if (opt == kUnifiedPolicyOptRequiredKeywordOnly) {
      auto it = py_var_kwargs.find(param.name);
      if (it != py_var_kwargs.end()) {
        result_py_bound_args.push_back(*it->second);
        py_var_kwargs.erase(it);
      } else {
        missing_keyword_only_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptOptionalKeywordOnly) {
      auto it = py_var_kwargs.find(param.name);
      if (it != py_var_kwargs.end()) {
        result_py_bound_args.push_back(*it->second);
        py_var_kwargs.erase(it);
      } else if (param.default_value.has_value()) {
        result_py_bound_args.push_back(&kSentinelDefaultValue);
      } else {
        // Gracefully handle a missing default value for an optional parameter.
        missing_keyword_only_params.push_back(param.name);
      }
    } else if (opt == kUnifiedPolicyOptVarKeyword) {
      result_py_bound_args.push_back(&kSentinelVarKwargs);
      result_py_var_kwargs = std::move(py_var_kwargs);
      py_var_kwargs.clear();  // Ensure `py_var_kwargs` state after value move.
    } else if (opt == kUnifiedPolicyOptNonDeterministic) {
      result_py_bound_args.push_back(&kSentinelNonDeterministic);
    } else {
      return PyErr_Format(
          PyExc_RuntimeError,
          "UnifiedBindingPolicy: unexpected binding_option='%s', param='%s'",
          absl::Utf8SafeCHexEscape(absl::string_view(&opt, 1)).c_str(),
          absl::Utf8SafeCHexEscape(param.name).c_str());
    }
  }

  if (!missing_positional_params.empty()) {
    ReportMissingPositionalParameters(missing_positional_params);
    return false;
  } else if (!missing_keyword_only_params.empty()) {
    ReportMissingKeywordOnlyParameters(missing_keyword_only_params);
    return false;
  } else if (has_unprocessed_positional_arguments) {
    ReportUnprocessedPositionalArguments(signature, binding_options,
                                         py_args_size);
    return false;
  } else if (!py_var_kwargs.empty()) {
    ReportUnprocessedKeywordArguments(py_var_kwargs);
    return false;
  }
  DCHECK_EQ(result_py_bound_args.size(), params.size());
  return true;
}

void ReportMissingPositionalParameters(
    absl::Span<const absl::string_view> missing_positional_params) {
  DCHECK(!missing_positional_params.empty());
  if (missing_positional_params.size() == 1) {
    PyErr_Format(
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
  }
}

void ReportMissingKeywordOnlyParameters(
    absl::Span<const absl::string_view> missing_keyword_only_params) {
  DCHECK(!missing_keyword_only_params.empty());
  if (missing_keyword_only_params.size() == 1) {
    PyErr_Format(
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
  }
}

void ReportUnprocessedPositionalArguments(
    const ExprOperatorSignature& signature, absl::string_view binding_options,
    size_t py_args_size) {
  size_t count_positionals = 0;
  size_t count_required_positionals = 0;
  for (size_t j = 0; j < binding_options.size(); ++j) {
    if (binding_options[j] == kUnifiedPolicyOptPositionalOnly ||
        binding_options[j] == kUnifiedPolicyOptPositionalOrKeyword) {
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
}

void ReportUnprocessedKeywordArguments(const PyVarKwarg& py_var_kwargs) {
  auto it = std::min_element(
      py_var_kwargs.begin(), py_var_kwargs.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
  PyErr_Format(PyExc_TypeError, "an unexpected keyword argument: '%s'",
               absl::Utf8SafeCHexEscape(it->first).c_str());
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
  PyErr_AddNote(
      absl::StrFormat("Error occurred while processing argument: `%s`",
                      absl::Utf8SafeCHexEscape(arg_name)));
  return std::nullopt;
}

// Boxing for variadic-positional arguments.
std::optional<QValueOrExpr> AsQValueOrExprVarArgs(
    PyObject* py_callable_as_qvalue_or_expr, absl::string_view arg_name,
    absl::Span<PyObject* const> py_args) {
  if (py_args.empty()) {  // Fast empty case.
    return MakeEmptyTuple();
  }
  // Apply the boxing rules to the arguments.
  std::vector<QValueOrExpr> args;
  args.reserve(py_args.size());
  bool has_exprs = false;
  for (size_t i = 0; i < py_args.size(); ++i) {
    auto arg = AsQValueOrExpr(py_callable_as_qvalue_or_expr, py_args[i]);
    if (!arg.has_value()) {
      PyErr_AddNote(
          absl::StrFormat("Error occurred while processing argument: `%s[%zu]`",
                          absl::Utf8SafeCHexEscape(arg_name), i));
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
      // Note: Consider adding a note with the argument name to the error.
      ASSIGN_OR_RETURN(node_deps.emplace_back(),
                       koladata::expr::MakeLiteral(
                           std::move(*std::get_if<TypedValue>(&arg))),
                       (SetPyErrFromStatus(_), std::nullopt));
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
std::optional<QValueOrExpr> AsQValueOrExprVarKwargs(
    PyObject* py_callable_as_qvalue_or_expr, const PyVarKwarg& py_var_kwargs) {
  const size_t n = py_var_kwargs.size();
  if (n == 0) {  // Fast empty case.
    return MakeEmptyNamedTuple();
  }
  std::vector<std::pair<absl::string_view, PyObject**>> ordered_py_var_kwargs(
      py_var_kwargs.begin(), py_var_kwargs.end());
  std::sort(
      ordered_py_var_kwargs.begin(), ordered_py_var_kwargs.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
  // Apply the boxing rules to the arguments.
  std::vector<QValueOrExpr> args;
  args.reserve(n);
  bool has_exprs = false;
  for (const auto& [k, pptr] : ordered_py_var_kwargs) {
    auto arg = AsQValueOrExpr(py_callable_as_qvalue_or_expr, *pptr);
    if (!arg.has_value()) {
      PyErr_AddNote(
          absl::StrFormat("Error occurred while processing argument: `%s`",
                          absl::Utf8SafeCHexEscape(k)));
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
      field_names.emplace_back(ordered_py_var_kwargs[i].first);
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
    absl::StrAppend(&field_names, ordered_py_var_kwargs[i].first);
    if (auto* expr = std::get_if<ExprNodePtr>(&args[i])) {
      node_deps.push_back(std::move(*expr));
    } else {
      // Note: Consider adding a note with the argument name to the error.
      ASSIGN_OR_RETURN(node_deps.emplace_back(),
                       koladata::expr::MakeLiteral(
                           std::move(*std::get_if<TypedValue>(&args[i]))),
                       (SetPyErrFromStatus(_), std::nullopt));
    }
  }
  node_deps[0] = arolla::expr::Literal(Text(std::move(field_names)));
  static const absl::NoDestructor namedtuple_op(
      std::make_shared<RegisteredOperator>("namedtuple.make"));
  ASSIGN_OR_RETURN(auto result,
                   MakeOpNode(*namedtuple_op, std::move(node_deps)),
                   (SetPyErrFromStatus(_), std::nullopt));
  return result;
}

// Returns the `koladata.types.py_boxing` python module if successful.
PyObjectPtr GetPyBoxingModule() {
  static PyObject* module_name =
      PyUnicode_InternFromString("koladata.types.py_boxing");
  auto py_module_dict = PyObjectPtr::NewRef(PyImport_GetModuleDict());
  if (py_module_dict == nullptr) {
    return nullptr;
  }
  auto py_module = PyObjectPtr::NewRef(
      PyDict_GetItemWithError(py_module_dict.get(), module_name));
  if (py_module == nullptr) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_ImportError,
                      "`koladata.types.py_boxing` is not imported yet");
    }
  }
  return py_module;
}

// A lower-level boxing-arguments function that works with pre-bound python
// values.
//
// If the function is successful, it returns `true`. Otherwise, it returns
// `false` and sets the corresponding Python exception.
bool UnifiedBoxBoundArguments(const ExprOperatorSignature& signature,
                              absl::string_view boxing_options,
                              absl::Span<PyObject* const> py_bound_args,
                              absl::Span<PyObject* const> py_var_args,
                              PyVarKwarg py_var_kwargs,
                              std::vector<QValueOrExpr>& result) {
  // Load the boxing functions.
  absl::InlinedVector<PyObjectPtr, 10> py_boxing_fns;
  auto py_boxing_module = GetPyBoxingModule();
  if (py_boxing_module == nullptr) {
    return false;
  }
  static PyObject* py_default_boxing_fn_name =
      PyUnicode_InternFromString("as_qvalue_or_expr");
  if (auto* py_boxing_fn =
          PyObject_GetAttr(py_boxing_module.get(), py_default_boxing_fn_name)) {
    py_boxing_fns.push_back(PyObjectPtr::Own(py_boxing_fn));
  } else {
    return false;
  }
  for (size_t offset = boxing_options.find(';');
       offset != absl::string_view::npos;) {
    auto py_boxing_fn_name = PyObjectPtr::Own(
        PyUnicode_FromStringAndSize(boxing_options.data(), offset));
    if (py_boxing_fn_name == nullptr) {
      return false;
    }
    if (auto* py_boxing_fn =
            PyObject_GetAttr(py_boxing_module.get(), py_boxing_fn_name.get())) {
      py_boxing_fns.push_back(PyObjectPtr::Own(py_boxing_fn));
    } else {
      return false;
    }
    boxing_options.remove_prefix(offset + 1);
    offset = boxing_options.find(';');
  }

  // Perform boxing.
  DCHECK_EQ(py_bound_args.size(), signature.parameters.size());
  result.clear();
  result.reserve(py_bound_args.size());
  for (size_t i = 0; i < py_bound_args.size(); ++i) {
    auto& param = signature.parameters[i];
    const char opt = (i < boxing_options.size() ? boxing_options[i] : '0');
    PyObject* py_boxing_fn;
    if (const size_t d = opt - '0'; d > 9) {
      PyErr_Format(
          PyExc_RuntimeError,
          "UnifiedBindingPolicy: unexpected boxing_option='%s', param='%s'",
          absl::Utf8SafeCHexEscape(absl::string_view(&opt, 1)).c_str(),
          absl::Utf8SafeCHexEscape(param.name).c_str());
      return false;
    } else if (d >= py_boxing_fns.size()) {
      PyErr_Format(PyExc_RuntimeError,
                   "UnifiedBindingPolicy: boxing_fn index is out of range: %zu",
                   d);
      return false;
    } else {
      py_boxing_fn = py_boxing_fns[d].get();
    }
    auto* py_bound_arg = py_bound_args[i];
    DCHECK_NE(py_bound_arg, nullptr);
    if (py_bound_arg == &kSentinelDefaultValue) {
      DCHECK(param.default_value.has_value());
      result.push_back(*param.default_value);
    } else if (py_bound_arg == &kSentinelVarArgs) {
      auto arg = AsQValueOrExprVarArgs(py_boxing_fn, param.name, py_var_args);
      if (!arg.has_value()) {
        return false;
      }
      result.push_back(*std::move(arg));
    } else if (py_bound_arg == &kSentinelVarKwargs) {
      auto arg = AsQValueOrExprVarKwargs(py_boxing_fn, py_var_kwargs);
      if (!arg.has_value()) {
        return false;
      }
      result.push_back(*std::move(arg));
    } else if (py_bound_arg == &kSentinelNonDeterministic) {
      ASSIGN_OR_RETURN(auto arg, expr::GenNonDeterministicToken(),
                       (SetPyErrFromStatus(_), false));
      result.push_back(std::move(arg));
    } else {
      auto arg = AsQValueOrExprArg(py_boxing_fn, param.name, py_bound_arg);
      if (!arg.has_value()) {
        return false;
      }
      result.push_back(*std::move(arg));
    }
  }
  return true;
}

// The unified binding policy class.
class UnifiedBindingPolicy : public AuxBindingPolicy {
  PyObject* MakePythonSignature(
      const ExprOperatorSignature& signature) const final {
    DCheckPyGIL();
    absl::string_view binding_options;
    absl::string_view boxing_options;
    if (!ExtractUnifiedPolicyOpts(signature, binding_options, boxing_options)) {
      return nullptr;
    }
    if (binding_options.size() != signature.parameters.size()) {
      return PyErr_Format(PyExc_RuntimeError,
                          "UnifiedBindingPolicy: mismatch between "
                          "the binding_options and "
                          "parameters: len(binding_options)=%zu, "
                          "len(signature.parameters)=%zu",
                          binding_options.size(), signature.parameters.size());
    }
    Signature result;
    result.parameters.reserve(binding_options.size());
    for (size_t i = 0; i < binding_options.size(); ++i) {
      const char opt = binding_options[i];
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
            "UnifiedBindingPolicy: unexpected binding_option='%s', param='%s'",
            absl::Utf8SafeCHexEscape(absl::string_view(&opt, 1)).c_str(),
            absl::Utf8SafeCHexEscape(param.name).c_str());
      }
    }
    return WrapAsPySignature(result);
  }

  // (See the base class.)
  bool BindArguments(
      const ExprOperatorSignature& signature, PyObject** py_args,
      Py_ssize_t nargsf, PyObject* py_tuple_kwnames,
      std::vector<QValueOrExpr>* absl_nonnull result) const final {
    DCheckPyGIL();
    absl::string_view binding_options;
    absl::string_view boxing_options;
    if (!ExtractUnifiedPolicyOpts(signature, binding_options, boxing_options)) {
      return false;
    }
    std::vector<PyObject*> py_bound_args;
    absl::Span<PyObject*> py_var_args;
    PyVarKwarg py_var_kwargs;
    if (!UnifiedBindArguments(signature, binding_options, py_args, nargsf,
                              py_tuple_kwnames, py_bound_args, py_var_args,
                              py_var_kwargs)) {
      return false;
    }
    return UnifiedBoxBoundArguments(signature, boxing_options, py_bound_args,
                                    py_var_args, std::move(py_var_kwargs),
                                    *result);
  }

  // (See the base class.)
  ExprNodePtr absl_nullable MakeLiteral(TypedValue&& value) const final {
    ASSIGN_OR_RETURN(auto result, koladata::expr::MakeLiteral(std::move(value)),
                     SetPyErrFromStatus(_));
    return result;
  }
};

}  // namespace

bool RegisterUnifiedBindingPolicy() {
  return RegisterAuxBindingPolicy(kUnifiedPolicy,
                                  std::make_shared<UnifiedBindingPolicy>());
}

}  // namespace koladata::python
