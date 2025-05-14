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
#ifndef THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_PROCESSOR_H_
#define THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_PROCESSOR_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"

namespace koladata::python::fstring {

// Converts DataSlice to string using format_spec.
// "s" has special meaning and can be applied to any type.
// Placeholder string is returned that contains the base64 encoded DataSlice.
// DataSlice is converted to string (without link to DataBag).
absl::StatusOr<std::string> ToDataSlicePlaceholder(
    const DataSlice& ds, absl::string_view format_spec);

// Converts ExprNodePtr to another expression that formats the input
// using format_spec.
// "s" has special meaning and can be applied to any type.
// Placeholder string is returned that contains the base64 encoded ExprNodePtr
// converted to string.
absl::StatusOr<std::string> ToExprPlaceholder(
    const arolla::expr::ExprNodePtr& ds, absl::string_view format_spec);

// Evaluates fstring into DataSlice that contains placeholders returned by
// ToDataSlicePlaceholder (but not ToExprPlaceholder).
absl::StatusOr<arolla::TypedValue> EvaluateFStringDataSlice(
    absl::string_view fstring);

// Creates formatting Expr from fstring that contains placeholders returned
// by ToDataSlicePlaceholder and/or ToExprPlaceholder.
absl::StatusOr<arolla::expr::ExprNodePtr> CreateFStringExpr(
    absl::string_view fstring);

}  // namespace koladata::python::fstring

#endif  // THIRD_PARTY_PY_KOLADATA_FSTRING_FSTRING_PROCESSOR_H_
