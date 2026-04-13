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
#include "koladata/operators/schema_filters.h"

#include <utility>

#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/op_utils/apply_filter.h"
#include "arolla/util/status_macros_backport.h"  // NOLINT

namespace koladata::ops {

absl::StatusOr<DataSlice> ApplyFilter(const DataSlice& x,
                                      const DataSlice& filter) {
  RETURN_IF_ERROR(filter.VerifyIsSchema());
  auto x_bag = x.GetBag();
  if (x_bag == nullptr) {
    x_bag = DataBag::Empty();
  }
  auto filter_bag = filter.GetBag();
  if (filter_bag == nullptr) {
    filter_bag = DataBag::Empty();
  }
  const auto& x_bag_impl = x_bag->GetImpl();
  const auto& filter_bag_impl = filter_bag->GetImpl();
  FlattenFallbackFinder x_fb_finder(*x_bag);
  auto x_fallbacks_span = x_fb_finder.GetFlattenFallbacks();
  FlattenFallbackFinder filter_fb_finder(*filter_bag);
  auto filter_fallbacks_span = filter_fb_finder.GetFlattenFallbacks();
  return x.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& result_db_impl, result_db->GetMutableImpl());
    internal::ApplyFilterOp apply_filter_op(result_db_impl);
    RETURN_IF_ERROR(apply_filter_op(impl, x.GetSchemaImpl(), x_bag_impl,
                                    x_fallbacks_span, filter.item(),
                                    filter_bag_impl, filter_fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(impl, x.GetShape(), x.GetSchemaImpl(),
                             std::move(result_db));
  });
}

}  // namespace koladata::ops
