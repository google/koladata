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
#include "py/koladata/fstring/fstring_processor.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python::fstring {

namespace {

// NOTE: brackets should have symbols different from base64 encoding.
constexpr absl::string_view kOpenBracket = "___KOLA!:!:!DATA___(";
constexpr absl::string_view kCloseBracket = ")___KOLA@:@:@DATA___";
constexpr absl::string_view kDataSlicePrefix = "DATA_SLICE:";
constexpr absl::string_view kExprPrefix = "EXPR:";

using ::arolla::Text;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::Literal;
using ::koladata::internal::DataItem;

absl::StatusOr<ExprNodePtr> FormattingExpr(const ExprNodePtr& ds_expr,
                                           absl::string_view format_spec) {
  std::string format = "{x}";
  if (!format_spec.empty() && format_spec != "s") {
    format = absl::StrCat("{x:", format_spec, "}");
  }
  ASSIGN_OR_RETURN(
      auto format_ds,
      DataSlice::Create(internal::DataItem(arolla::Text(std::move(format))),
                        internal::DataItem(schema::kString)));
  ASSIGN_OR_RETURN(
      auto format_kwargs,
      arolla::expr::CallOp("namedtuple.make", {Literal(Text("x")), ds_expr}));
  return CallOp("kd.strings.format",
                {Literal(std::move(format_ds)), std::move(format_kwargs)});
}

absl::StatusOr<std::string> SerializeExprToBase64(const ExprNodePtr& expr,
                                                  absl::string_view prefix) {
  ASSIGN_OR_RETURN(auto dump_proto,
                   arolla::serialization::Encode({}, {expr}));
  return absl::StrCat(kOpenBracket, prefix,
                      absl::Base64Escape(dump_proto.SerializeAsString()),
                      kCloseBracket);
}

absl::StatusOr<ExprNodePtr> DeserializeExprFromBase64(
    absl::string_view dump64) {
  std::string dump;
  if (!absl::Base64Unescape(dump64, &dump)) {
    return absl::InternalError("Incorrect FString: fail to parse dump.");
  }
  arolla::serialization_base::ContainerProto dump_proto;
  if (!dump_proto.ParseFromString(dump)) {
    return absl::InternalError("Incorrect FString: fail to parse dump.");
  }
  ASSIGN_OR_RETURN(auto decode_result,
                   arolla::serialization::Decode(dump_proto));
  if (!decode_result.values.empty() || decode_result.exprs.size() != 1) {
    return absl::InternalError("Incorrect FString: expected one expr.");
  }
  return std::move(decode_result).exprs[0];
}

enum class FStringParserMode {
  kDataSlice,
  kExpr,
};

absl::StatusOr<ExprNodePtr> CreateFStringExprImpl(absl::string_view fstring,
                                                  FStringParserMode mode) {
  auto string_to_ds =
      [](absl::string_view text) -> absl::StatusOr<ExprNodePtr> {
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(DataItem(arolla::Text(text)),
                                                DataItem(schema::kString)));
    return arolla::expr::Literal(std::move(ds));
  };

  std::vector<arolla::expr::ExprNodePtr> to_concat;
  bool has_koladata = false;
  absl::Status error = absl::OkStatus();
  while (true) {
    size_t open_offset = fstring.find(kOpenBracket);
    if (open_offset == std::string::npos) {
      break;
    }
    has_koladata = true;
    if (open_offset != 0) {
      ASSIGN_OR_RETURN(to_concat.emplace_back(),
                       string_to_ds(fstring.substr(0, open_offset)));
    }
    fstring.remove_prefix(open_offset + kOpenBracket.size());
    size_t close_offset = fstring.find(kCloseBracket);
    if (close_offset == std::string::npos) {
      return absl::InternalError("incorrect FString: no closing bracket");
    }
    absl::string_view dump64 = fstring.substr(0, close_offset);
    if (absl::ConsumePrefix(&dump64, kExprPrefix)) {
      if (mode == FStringParserMode::kDataSlice) {
        return absl::InvalidArgumentError(
            "f-string contains expression in eager kd.fstr call");
      }
    } else if (!absl::ConsumePrefix(&dump64, kDataSlicePrefix)) {
      return absl::InternalError("incorrect FString: wrong prefix");
    }
    ASSIGN_OR_RETURN(to_concat.emplace_back(),
                     DeserializeExprFromBase64(dump64));
    fstring.remove_prefix(close_offset + kCloseBracket.size());
  }
  if (!fstring.empty()) {
    ASSIGN_OR_RETURN(to_concat.emplace_back(), string_to_ds(fstring));
  }
  if (!has_koladata) {
    return absl::InvalidArgumentError(
        "FString has nothing to format. Forgot to specify format `{...:s}`?");
  }
  return arolla::expr::BindOp("kd.strings.join", to_concat, {});
}

}  // namespace

absl::StatusOr<std::string> ToDataSlicePlaceholder(
    const DataSlice& ds, absl::string_view format_spec) {
  ASSIGN_OR_RETURN(auto text_ds_expr,
                   FormattingExpr(Literal(ds.WithBag(nullptr)), format_spec));
  return SerializeExprToBase64(text_ds_expr, kDataSlicePrefix);
}

absl::StatusOr<std::string> ToExprPlaceholder(const ExprNodePtr& ds,
                                              absl::string_view format_spec) {
  ASSIGN_OR_RETURN(auto text_ds_expr, FormattingExpr(ds, format_spec));
  return SerializeExprToBase64(text_ds_expr, kExprPrefix);
}

absl::StatusOr<ExprNodePtr> CreateFStringExpr(absl::string_view fstring) {
  return CreateFStringExprImpl(fstring, FStringParserMode::kExpr);
}

absl::StatusOr<arolla::TypedValue> EvaluateFStringDataSlice(
    absl::string_view fstring) {
  ASSIGN_OR_RETURN(
      auto expr, CreateFStringExprImpl(fstring, FStringParserMode::kDataSlice));
  // TODO: benefit from compilation cache be using leaves in expr.
  return arolla::expr::Invoke(expr, {});
}

}  // namespace koladata::python::fstring
