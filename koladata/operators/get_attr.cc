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
#include "koladata/operators/get_attr.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

absl::StatusOr<absl::string_view> GetAttrNameAsStr(const DataSlice& attr_name) {
  if (attr_name.GetShape().rank() != 0 ||
      attr_name.dtype() != schema::kText.qtype()) {
    return absl::InvalidArgumentError(
        absl::StrCat("attr_name in kd.get_attr expects TEXT, got: ",
                     arolla::Repr(attr_name)));
  }
  return attr_name.item().value<arolla::Text>().view();
}

}  // namespace

absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name) {
  ASSIGN_OR_RETURN(auto attr_name_str, GetAttrNameAsStr(attr_name));
  return obj.GetAttr(attr_name_str);
}

absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value) {
  ASSIGN_OR_RETURN(auto attr_name_str, GetAttrNameAsStr(attr_name));
  return obj.GetAttrWithDefault(attr_name_str, default_value);
}

}  // namespace koladata::ops
