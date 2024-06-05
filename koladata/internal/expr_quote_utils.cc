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
#include "koladata/internal/expr_quote_utils.h"

#include <string>

#include "absl/strings/string_view.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/quote.h"
#include "arolla/serialization/encode.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

constexpr absl::string_view kUnsupportedExprQuote = "<unsupported ExprQuote>";
constexpr absl::string_view kUninitializedExprQuote = "<uninitialized ExprQuote>";

}  // namespace

std::string StableFingerprint(const arolla::expr::ExprQuote& expr_quote) {
  ASSIGN_OR_RETURN(auto expr, expr_quote.expr(),
                   std::string(kUninitializedExprQuote));
  ASSIGN_OR_RETURN(auto container_proto,
                   arolla::serialization::Encode({}, {expr}),
                   std::string(kUnsupportedExprQuote));
  return container_proto.SerializeAsString();
}

std::string ExprQuoteDebugString(const arolla::expr::ExprQuote& expr_quote) {
  if (!expr_quote.has_expr()) {
    return std::string(kUninitializedExprQuote);
  }
  return arolla::expr::ToDebugString(*expr_quote);
}

}  // namespace koladata::internal
