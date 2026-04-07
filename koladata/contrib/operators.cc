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
#include "koladata/contrib/flatten_cyclic_references.h"
#include "koladata/contrib/sanitize_names.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/op_utils/qexpr.h"

namespace koladata::contrib {
namespace {

KODA_QEXPR_OPERATOR("kd_ext.contrib._flatten_cyclic_references",
                    FlattenCyclicReferences,
                    "kd_ext.contrib.flatten_cyclic_references");

KODA_QEXPR_OPERATOR("kd_ext.contrib.sanitize_names",
                    SanitizeNames);

}  // namespace
}  // namespace koladata::contrib
