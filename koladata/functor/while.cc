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
#include "koladata/functor/while.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/sequence/mutable_sequence.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/functor/call.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<std::vector<arolla::TypedValue>> WhileWithCompilationCache(
    const DataSlice& condition_fn, const DataSlice& body_fn,
    absl::Span<const std::string> var_names,
    std::vector<arolla::TypedValue> var_values, int num_yields_vars) {
  if (var_values.size() != var_names.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the number of variable values must match the number of variable "
        "names, got %d != %d", var_values.size(), var_names.size()));
  }
  if (num_yields_vars < 0 || num_yields_vars > var_names.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "invalid number of yields vars: %d not between 0 and %d",
        num_yields_vars, var_names.size()));
  }
  absl::flat_hash_map<absl::string_view, int64_t> var_name_to_index;
  var_name_to_index.reserve(var_names.size());
  for (int64_t i = 0; i < var_names.size(); ++i) {
    if (!var_name_to_index.insert({var_names[i], i}).second) {
      return absl::InvalidArgumentError(
          absl::StrFormat("duplicate variable name: `%s`", var_names[i]));
    }
  }
  auto nonyield_var_names = var_names.subspan(num_yields_vars);
  std::vector<arolla::TypedRef> nonyield_var_value_refs;
  nonyield_var_value_refs.reserve(var_values.size() - num_yields_vars);
  for (int64_t i = num_yields_vars; i < var_values.size(); ++i) {
    nonyield_var_value_refs.push_back(var_values[i].AsRef());
  }
  std::vector<std::vector<arolla::TypedValue>> yields_var_values;
  yields_var_values.reserve(num_yields_vars);
  for (int64_t i = 0; i < num_yields_vars; ++i) {
    yields_var_values.emplace_back(
        std::vector<arolla::TypedValue>({var_values[i]}));
  }

  while (true) {
    ASSIGN_OR_RETURN(
        auto condition_value,
        CallFunctorWithCompilationCache(
            condition_fn,
            nonyield_var_value_refs,
            nonyield_var_names));
    if (condition_value.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("condition_fn must return a DataSlice, got type `%s`",
                          condition_value.GetType()->name()));
    }
    const auto& condition_slice = condition_value.UnsafeAs<DataSlice>();
    const auto& condition_schema = condition_slice.GetSchemaImpl();
    if (!condition_slice.is_item() ||
        (condition_schema != schema::kMask &&
         condition_schema != schema::kObject) ||
        (condition_slice.item().has_value() &&
         !condition_slice.item().holds_value<arolla::Unit>())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "condition_fn must return a MASK DataItem, got %s",
          DataSliceRepr(condition_slice)));
    }
    if (!condition_slice.item().has_value()) {
      break;
    }

    ASSIGN_OR_RETURN(
        auto updates,
        CallFunctorWithCompilationCache(
            body_fn,
            nonyield_var_value_refs,
            nonyield_var_names));
    if (!arolla::IsNamedTupleQType(updates.GetType())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "body_fn must return a namedtuple, got type `%s`",
          updates.GetType()->name()));
    }
    auto field_names = arolla::GetFieldNames(updates.GetType());
    for (int64_t i = 0; i < field_names.size(); ++i) {
      const auto& field_name = field_names[i];
      auto it = var_name_to_index.find(field_name);
      if (it == var_name_to_index.end()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "body_fn must return a namedtuple with field names that are a "
            "subset of the state variable names, got an unknown field: `%s`",
            field_name));
      }
      int64_t var_index = it->second;
      auto update_value = arolla::TypedValue(updates.GetField(i));
      if (var_values[var_index].GetType() != update_value.GetType()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected `%s` value to have type `%s`, got `%s`",
            var_names[var_index], var_values[var_index].GetType()->name(),
            update_value.GetType()->name()));
      }
      if (var_index < num_yields_vars) {
        yields_var_values[var_index].emplace_back(std::move(update_value));
      } else {
        var_values[var_index] = std::move(update_value);
        nonyield_var_value_refs[var_index - num_yields_vars] =
            var_values[var_index].AsRef();
      }
    }
  }

  for (int64_t i = 0; i < num_yields_vars; ++i) {
    auto value_qtype = var_values[i].GetType();
    ASSIGN_OR_RETURN(auto seq, arolla::MutableSequence::Make(
                                   value_qtype, yields_var_values[i].size()));
    for (int64_t seq_index = 0; seq_index < yields_var_values[i].size();
         ++seq_index) {
      seq.UnsafeSetRef(seq_index, yields_var_values[i][seq_index].AsRef());
    }
    ASSIGN_OR_RETURN(var_values[i], arolla::TypedValue::FromValueWithQType(
                                        std::move(seq).Finish(),
                                        arolla::GetSequenceQType(value_qtype)));
  }

  return std::move(var_values);
}

}  // namespace koladata::functor
