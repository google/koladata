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
#include "koladata/check_frozen.h"

#include <cstdint>
#include <stack>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/repr.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

bool IsFrozenBag(const DataBagPtr absl_nullable& bag) {
  return bag == nullptr || (!bag->IsMutable() && !bag->HasMutableFallbacks());
}

absl::Status CheckFrozenBag(const DataBagPtr absl_nullable& bag) {
  if (IsFrozenBag(bag)) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("DataBag is not frozen: %s", arolla::Repr(bag)));
}

absl::Status CheckFrozenSlice(const DataSlice& slice) {
  if (IsFrozenBag(slice.GetBag())) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("DataSlice is not frozen: %s", arolla::Repr(slice)));
}

absl::Status CheckFrozenCompoundValue(arolla::TypedRef value) {
  std::stack<arolla::TypedRef> stack({value});
  while (!stack.empty()) {
    arolla::TypedRef field = stack.top();
    stack.pop();
    const int64_t field_count = value.GetFieldCount();
    DCHECK_GT(field_count, 0);
    for (int64_t i = 0; i < field_count; ++i) {
      auto sub_field = field.GetField(i);
      if (sub_field.GetType() == arolla::GetQType<DataBagPtr>()) {
        RETURN_IF_ERROR(CheckFrozenBag(sub_field.UnsafeAs<DataBagPtr>()));
      } else if (sub_field.GetType() == arolla::GetQType<DataSlice>()) {
        RETURN_IF_ERROR(CheckFrozenSlice(sub_field.UnsafeAs<DataSlice>()));
      } else if (sub_field.GetFieldCount() > 0) {
        stack.push(sub_field);
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status CheckFrozen(arolla::TypedRef value) {
  if (value.GetType() == arolla::GetQType<DataBagPtr>()) {
    return CheckFrozenBag(value.UnsafeAs<DataBagPtr>());
  } else if (value.GetType() == arolla::GetQType<DataSlice>()) {
    return CheckFrozenSlice(value.UnsafeAs<DataSlice>());
  } else if (value.GetFieldCount() > 0) {
    return CheckFrozenCompoundValue(value);
  }
  return absl::OkStatus();
}

}  // namespace koladata
