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
#include "koladata/operators/reverse.h"

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/op_utils/reverse.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Reverse(const DataSlice& obj) {
  if (obj.impl_empty_and_unknown() || obj.GetShape().rank() == 0) {
    return obj;
  }
  return DataSlice::Create(
      koladata::internal::ReverseOp{}(obj.slice(), obj.GetShape()),
      obj.GetShape(), obj.GetSchemaImpl(), obj.GetDb());
}

}  // namespace koladata::ops
