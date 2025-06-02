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
#include "koladata/functor/parallel/stream_loop_internal.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/functor/parallel/stream_qtype.h"
#include "koladata/internal/dtype.h"

namespace koladata::functor::parallel::stream_loop_internal {

absl::StatusOr<ParsedLoopCondition> ParseLoopCondition(
    arolla::TypedRef condition) {
  if (condition.GetType() == arolla::GetQType<DataSlice>()) {
    return ParseLoopConditionDataSlice(condition.UnsafeAs<DataSlice>());
  }
  if (condition.GetType() == GetStreamQType<DataSlice>()) {
    return ParseLoopConditionStream(
        condition.UnsafeAs<StreamPtr>()->MakeReader());
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("the condition functor must return a DATA_SLICE or a "
                      "STREAM[DATA_SLICE], but got %s",
                      condition.GetType()->name()));
}

absl::StatusOr<ParsedLoopCondition> ParseLoopConditionDataSlice(
    const DataSlice& condition) {
  if (condition.is_item()) {
    if (condition.item().holds_value<arolla::Unit>()) {
      return ParsedLoopCondition{true};
    }
    const auto& schema = condition.GetSchemaImpl();
    if (!condition.item().has_value() &&
        (schema == schema::kMask || schema == schema::kObject)) {
      return ParsedLoopCondition{false};
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "the condition value must be a data-item with schema MASK, got %s",
      DataSliceRepr(condition)));
}

absl::StatusOr<ParsedLoopCondition> ParseLoopConditionStream(
    StreamReaderPtr /*absl_nonnull*/ condition) {
  auto try_read_result = condition->TryRead();
  if (auto* item = try_read_result.item()) {
    if (item->GetType() != arolla::GetQType<DataSlice>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("the condition functor must return a DATA_SLICE or a "
                          "STREAM[DATA_SLICE], but got STREAM[%s]",
                          item->GetType()->name()));
    }
    // TODO: Consider enforcing that the stream contains exactly one item.
    return ParseLoopConditionDataSlice(item->UnsafeAs<DataSlice>());
  }
  if (auto* status = try_read_result.close_status()) {
    return !status->ok()
               ? std::move(*status)
               : absl::InvalidArgumentError(
                     "the condition functor returned an empty stream");
  }
  return ParsedLoopCondition{false, std::move(condition)};
}

Vars::Vars(std::vector<arolla::TypedRef> initial_values,
           std::vector<std::string> kwnames)
    : initial_values_holder_(arolla::MakeTuple(initial_values)),
      values_(std::move(initial_values)),
      kwnames_(std::move(kwnames)) {
  for (size_t i = 0; i < values_.size(); ++i) {
    values_[i] = initial_values_holder_.GetField(i);
  }
  DCHECK_GE(values_.size(), kwnames_.size());
  size_t kwnames_offset = values_.size() - kwnames_.size();
  index_.reserve(kwnames_.size());
  for (size_t i = 0; i < kwnames_.size(); ++i) {
    index_.emplace(kwnames_[i],
                   Index{values_[kwnames_offset + i], initial_values_holder_});
  }
}

absl::Status Vars::Update(arolla::TypedValue update) {
  if (!arolla::IsNamedTupleQType(update.GetType())) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected a namedtupe with a subset of initial variables, got type %s",
        update.GetType()->name()));
  }
  const auto& update_field_names = arolla::GetFieldNames(update.GetType());
  for (size_t i = 0; i < update_field_names.size(); ++i) {
    absl::string_view name = update_field_names[i];
    arolla::TypedRef value = update.GetField(i);
    const auto it = index_.find(name);
    if (it == index_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unexpected variable '%s'", name));
    }
    if (it->second.mutable_ref.GetType() != value.GetType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "variable '%s' has type %s, but the provided value has type %s", name,
          it->second.mutable_ref.GetType()->name(), value.GetType()->name()));
    }
    it->second.mutable_ref = value;
    // Note: value is a reference to a field within the `update` value.
    // The `update` is immutable and ref-counted. Here, we make sure that
    // the lifetime of the `update` exceeds the lifetime of the `value`.
    it->second.holder = update;
  }
  return absl::OkStatus();
}

}  // namespace koladata::functor::parallel::stream_loop_internal
