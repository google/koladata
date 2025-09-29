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
#include "koladata/functor/call.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "arolla/util/traceme.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/auto_variables.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/functor_storage.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

struct FunctorPreprocessingCache {
  Signature signature;
  arolla::expr::ExprNodePtr combined_expr;
};

absl::StatusOr<FunctorPreprocessingCache> ProcessFunctor(
    const DataSlice& functor) {
  arolla::profiling::TraceMe traceme(
      "::koladata::functor::ProcessFunctorMetadata");
  ASSIGN_OR_RETURN(auto signature_item, functor.GetAttr(kSignatureAttrName));
  // We use detach_default_values_db=true to prevent ownership loop when storing
  // the preprocessed signature in the data bag cache.
  ASSIGN_OR_RETURN(auto signature,
                   KodaSignatureToCppSignature(
                       signature_item, /*detach_default_values_db=*/true));
  ASSIGN_OR_RETURN(auto combined_expr, InlineAllVariables(functor));

  return FunctorPreprocessingCache{std::move(signature),
                                   std::move(combined_expr)};
}

struct CounterGuardTag {};

absl::Status VerifySufficientStackSize(size_t depth) {
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<arolla::TypedValue> CallFunctorWithCompilationCache(
    const DataSlice& functor, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) {
  thread_local size_t depth = 0;
  ++depth;
  absl::Cleanup depth_cleanup([&] { --depth; });
  RETURN_IF_ERROR(VerifySufficientStackSize(depth));
  ASSIGN_OR_RETURN(bool is_functor, IsFunctor(functor));
  if (!is_functor) {
    return absl::InvalidArgumentError(
        absl::StrCat("the first argument of kd.call must be a functor, got ",
                     DataSliceRepr(functor)));
  }

  arolla::profiling::TraceMe traceme([&]() -> std::string {
    auto get_str = [&](const absl::StatusOr<DataSlice>& attr) {
      if (attr.ok() && attr->item().has_value() &&
          attr->item().holds_value<arolla::Text>()) {
        return attr->item().value<arolla::Text>().view();
      } else {
        return absl::string_view();
      }
    };
    auto module = functor.GetAttrOrMissing(functor::kModuleAttrName);
    auto qualname = functor.GetAttrOrMissing(functor::kQualnameAttrName);
    absl::string_view module_str = get_str(module);
    absl::string_view qualname_str = get_str(qualname);
    if (!module_str.empty() && !qualname_str.empty()) {
      return absl::StrCat("<Functor> ", module_str, ".", qualname_str);
    } else {
      return "<Unknown Functor>";
    }
  });

  internal::ObjectId functor_id = functor.item().value<internal::ObjectId>();
  auto bag = functor.GetBag();
  DCHECK(bag != nullptr);  // validated in IsFunctor

  auto cached_data =
      bag->GetCachedMetadataOrNull<FunctorPreprocessingCache>(functor_id);
  if (cached_data == nullptr) {
    ASSIGN_OR_RETURN(auto data, ProcessFunctor(functor));
    cached_data = bag->SetCachedMetadata(functor_id, std::move(data));
  }
  DCHECK(cached_data != nullptr);

  ASSIGN_OR_RETURN(
      auto bound_arguments,
      BindArguments(cached_data->signature, args, kwnames, bag));
  std::vector<std::pair<std::string, arolla::TypedRef>> inputs;
  const auto& parameters = cached_data->signature.parameters();
  inputs.reserve(parameters.size() + 1);
  for (int64_t i = 0; i < parameters.size(); ++i) {
    inputs.emplace_back(parameters[i].name, bound_arguments[i].AsRef());
  }
  inputs.emplace_back(kSelfBagInput, arolla::TypedRef::FromValue(bag));

  return expr::EvalExprWithCompilationCache(cached_data->combined_expr, inputs,
                                            {});
}

}  // namespace koladata::functor
