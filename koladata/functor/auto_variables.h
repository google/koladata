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

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Returns a functor with additional variables extracted.
// New variables will be created for:
//   1) All named nodes
//   2) DataSlices (except scalar primitives)
//   3) Nodes with fingerprints listed in `extra_nodes_to_extract`.
// NOTE: Tests are in
// py/koladata/functor/auto_variables_test.py
//
absl::StatusOr<DataSlice> AutoVariables(
    const DataSlice& functor,
    absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract = {});

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_AUTO_VARIABLES_H_
