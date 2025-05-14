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
#include "koladata/expr/constants.h"

#include "absl/base/no_destructor.h"
#include "absl/status/statusor.h"
#include "arolla/memory/optional_value.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::expr {

namespace {
absl::StatusOr<DataSlice> MakeUnspecifiedSelfInput() {
  ASSIGN_OR_RETURN(auto present,
                   DataSlice::Create(internal::DataItem(arolla::kPresent),
                                     internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(auto res,
                   CreateUu(DataBag::Empty(), "__self_not_specified__",
                            {"self_not_specified"}, {present}));
  return res.FreezeBag();
}
}  // namespace

// Returns a data slice that should be passed as the value of I.self
// when the user passed no positional argument to kd.eval.
const DataSlice& UnspecifiedSelfInput() {
  // No error is possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{*MakeUnspecifiedSelfInput()};
  return *val;
}

}  // namespace koladata::expr
