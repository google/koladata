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
#include "koladata/functor/signature.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<Signature> Signature::Create(
    absl::Span<const Parameter> parameters) {
  absl::flat_hash_set<std::string> names;
  // First, do all single-parameter checks.
  for (const auto& parameter : parameters) {
    bool was_inserted = names.insert(parameter.name).second;
    if (!was_inserted) {
      return absl::InvalidArgumentError(
          absl::StrFormat("duplicate parameter name: [%s]", parameter.name));
    }
    if (parameter.default_value.has_value()) {
      if (parameter.kind == Signature::Parameter::Kind::kVarPositional ||
          parameter.kind == Signature::Parameter::Kind::kVarKeyword) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "default value is not allowed for a %v parameter [%s]",
            parameter.kind, parameter.name));
      }
      if (auto rank = parameter.default_value->GetShape().rank(); rank != 0) {
        // We could lift this restriction in the future if needed, but that
        // would make the Koda representation of the signature more complex,
        // so I propose to do it only if necessary. Since the variables also
        // have to be DataItems, this seems like a logical extension of that.
        return absl::InvalidArgumentError(
            absl::StrFormat("default value for parameter [%s] must be a data "
                            "item, but has rank %d",
                            parameter.name, rank));
      }
    }
  }

  // Then, do all checks on pairs of consecutive parameters.
  for (size_t i = 0; i + 1 < parameters.size(); ++i) {
    const auto& current_kind = parameters[i].kind;
    const auto& next_kind = parameters[i + 1].kind;
    if (current_kind > next_kind) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "a %v parameter [%s] cannot follow a %v parameter [%s]", next_kind,
          parameters[i + 1].name, current_kind, parameters[i].name));
    }
    if (current_kind == next_kind &&
        (current_kind == Signature::Parameter::Kind::kVarPositional ||
         current_kind == Signature::Parameter::Kind::kVarKeyword)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("at most one %v parameter is allowed", current_kind));
    }
    if ((next_kind == Signature::Parameter::Kind::kPositionalOnly ||
         next_kind == Signature::Parameter::Kind::kPositionalOrKeyword) &&
        !parameters[i + 1].default_value.has_value() &&
        parameters[i].default_value.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("a positional parameter [%s] without a default value "
                          "cannot follow [%s] with a default value",
                          parameters[i + 1].name, parameters[i].name));
    }
  }

  return Signature(parameters);
}

Signature::Signature(absl::Span<const Parameter> parameters)
    : parameters_(parameters.begin(), parameters.end()) {
  for (size_t i = 0; i < parameters_.size(); ++i) {
    const auto& parameter = parameters_[i];
    if (parameter.kind == Signature::Parameter::Kind::kKeywordOnly ||
        parameter.kind == Signature::Parameter::Kind::kPositionalOrKeyword) {
      keyword_parameter_index_[parameter.name] = i;
    }
  }
}

absl::StatusOr<std::vector<arolla::TypedValue>> BindArguments(
    const Signature& signature, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) {
  if (args.size() < kwnames.size()) {
    return absl::InvalidArgumentError("args.size < kwnames.size()");
  }
  const size_t kwargs_offset = args.size() - kwnames.size();

  const auto& parameters = signature.parameters();
  const auto& keyword_parameter_index = signature.keyword_parameter_index();
  std::vector<arolla::TypedValue> bound_arguments(
      parameters.size(), arolla::TypedValue::UnsafeFromTypeDefaultConstructed(
                             arolla::GetNothingQType()));

  std::vector<arolla::TypedRef> unknown_args;
  std::vector<std::string> unknown_kwarg_names;
  std::vector<arolla::TypedRef> unknown_kwarg_values;

  // Process positional arguments.
  for (size_t i = 0; i < kwargs_offset; ++i) {
    if (i >= parameters.size() ||
        (parameters[i].kind != Signature::Parameter::Kind::kPositionalOnly &&
         parameters[i].kind !=
             Signature::Parameter::Kind::kPositionalOrKeyword)) {
      unknown_args.push_back(args[i]);
    } else {
      bound_arguments[i] = arolla::TypedValue(args[i]);
    }
  }

  // Process keyword arguments.
  for (size_t i = kwargs_offset; i < args.size(); ++i) {
    const auto& name = kwnames[i - kwargs_offset];
    const auto& value = args[i];
    auto it = keyword_parameter_index.find(name);
    if (it == keyword_parameter_index.end()) {
      unknown_kwarg_names.push_back(name);
      unknown_kwarg_values.push_back(value);
    } else {
      if (bound_arguments[it->second].GetType() != arolla::GetNothingQType()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("parameter [%s] specified twice", name));
      }
      bound_arguments[it->second] = arolla::TypedValue(value);
    }
  }

  // Handle variadic parameters.
  for (size_t i = 0; i < parameters.size(); ++i) {
    const auto& parameter = parameters[i];
    if (parameter.kind == Signature::Parameter::Kind::kVarPositional) {
      bound_arguments[i] = arolla::MakeTuple(unknown_args);
      unknown_args.clear();
    } else if (parameter.kind == Signature::Parameter::Kind::kVarKeyword) {
      ASSIGN_OR_RETURN(
          bound_arguments[i],
          arolla::MakeNamedTuple(unknown_kwarg_names, unknown_kwarg_values));
      unknown_kwarg_names.clear();
      unknown_kwarg_values.clear();
    }
  }
  if (!unknown_args.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("too many positional arguments passed (%d extra)",
                        unknown_args.size()));
  }
  if (!unknown_kwarg_names.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unknown keyword arguments: [%s]",
                        absl::StrJoin(unknown_kwarg_names, ", ")));
  }

  // Handle default values. This makes sure we do not return the auxiliary
  // values with NothingQType back to the user.
  for (size_t i = 0; i < parameters.size(); ++i) {
    if (bound_arguments[i].GetType() == arolla::GetNothingQType()) {
      const auto& parameter = parameters[i];
      if (parameter.default_value.has_value()) {
        bound_arguments[i] =
            arolla::TypedValue::FromValue(*parameter.default_value);
      } else {
        return absl::InvalidArgumentError(
            absl::StrFormat("no value provided for %v parameter [%s]",
                            parameter.kind, parameter.name));
      }
    }
  }

  return bound_arguments;
}

}  // namespace koladata::functor
