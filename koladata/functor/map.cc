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
#include "koladata/functor/map.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/call.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/slices.h"
#include "koladata/shape_utils.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

absl::StatusOr<DataSlice> MapFunctorWithCompilationCache(
    const DataSlice& functors, std::vector<DataSlice> args,
    absl::Span<const std::string> kwnames, bool include_missing,
    const expr::EvalOptions& eval_options) {
  args.push_back(functors);
  ASSIGN_OR_RETURN(auto aligned_args, shape::Align(std::move(args)));
  DataSlice aligned_functors = std::move(aligned_args.back());
  aligned_args.pop_back();
  // Pre-allocate the vectors to avoid reallocations in the loop.
  std::vector<arolla::TypedRef> arg_refs;
  arg_refs.reserve(aligned_args.size());
  std::vector<DataSlice> result_slices;
  ASSIGN_OR_RETURN(DataSlice missing,
                   DataSlice::Create(internal::DataItem(std::nullopt),
                                     internal::DataItem(schema::kNone)));

  auto call_on_items = [&kwnames, &missing, &arg_refs, &include_missing,
                        &eval_options](const DataSlice& functor,
                                       const std::vector<DataSlice>& arg_slices)
      -> absl::StatusOr<DataSlice> {
    if (!functor.item().has_value()) {
      return missing;
    }
    arg_refs.clear();
    for (const auto& value : arg_slices) {
      DCHECK(value.is_item());
      if (!include_missing && !value.item().has_value()) {
        return missing;
      }
      arg_refs.push_back(arolla::TypedRef::FromValue(value));
    }
    // We can improve the performance a lot if "functor" is the same for many
    // items, as a lot of the work inside CallFunctorWithCompilationCache could
    // be reused between items then.
    ASSIGN_OR_RETURN(auto result,
                     CallFunctorWithCompilationCache(functor, arg_refs, kwnames,
                                                     eval_options));
    if (result.GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "the functor is expected to be evaluated to a DataItem"
          ", but the result has type `%s` instead",
          result.GetType()->name()));
    }
    const auto& result_slice = result.UnsafeAs<DataSlice>();
    if (!result_slice.is_item()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "the functor is expected to be evaluated to a DataItem"
          ", but the result has shape: %s",
          arolla::Repr(result_slice.GetShape())));
    }
    return result_slice;
  };

  if (aligned_functors.is_item()) {
    return call_on_items(aligned_functors, aligned_args);
  } else {
    std::vector<DataSlice> arg_slices;
    arg_slices.reserve(aligned_args.size());
    const auto& impl = aligned_functors.slice();
    for (int64_t i = 0; i < impl.size(); ++i) {
      ASSIGN_OR_RETURN(
          auto functor,
          DataSlice::Create(impl[i], aligned_functors.GetSchemaImpl(),
                            aligned_functors.GetBag()));
      arg_slices.clear();
      for (const auto& arg : aligned_args) {
        DCHECK(!arg.is_item());
        ASSIGN_OR_RETURN(auto arg_slice,
                         DataSlice::Create(arg.slice()[i], arg.GetSchemaImpl(),
                                           arg.GetBag()));
        arg_slices.push_back(std::move(arg_slice));
      }
      ASSIGN_OR_RETURN(auto result_slice, call_on_items(functor, arg_slices));
      result_slices.push_back(std::move(result_slice));
    }

    ASSIGN_OR_RETURN(DataSlice is_stack,
                     DataSlice::Create(internal::DataItem(true),
                                       internal::DataItem(schema::kBool)));
    ASSIGN_OR_RETURN(DataSlice ndim,
                     DataSlice::Create(internal::DataItem(0),
                                       internal::DataItem(schema::kInt32)));
    std::vector<const DataSlice*> stack_args;
    stack_args.reserve(2 + result_slices.size());
    stack_args.push_back(&is_stack);
    stack_args.push_back(&ndim);
    for (const auto& result_slice : result_slices) {
      stack_args.push_back(&result_slice);
    }
    ASSIGN_OR_RETURN(auto flat_result, ops::ConcatOrStack(stack_args));
    return flat_result.Reshape(aligned_functors.GetShape());
  }
}

}  // namespace koladata::functor
