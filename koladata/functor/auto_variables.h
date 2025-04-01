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
#ifndef KOLADATA_FUNCTOR_AUTO_VARIABLES_H_
#define KOLADATA_FUNCTOR_AUTO_VARIABLES_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Creates additional variables for DataSlices and named nodes and returns
// a functor with these variables extracted.
// NOTE: Tests are in
// py/koladata/functor/functor_factories_test.py
//
// TODO: Generalize and add `predicate` argument to control which
// nodes should be extracted.
//    optional<std::function<StatusOr<bool>(const ExprNodePtr&)>>& predicate
//
absl::StatusOr<DataSlice> AutoVariables(const DataSlice& functor);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_AUTO_VARIABLES_H_
