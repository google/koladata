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
#ifndef KOLADATA_EXPR_CONSTANTS_H_
#define KOLADATA_EXPR_CONSTANTS_H_

#include "koladata/data_slice.h"

namespace koladata::expr {

// Returns a data slice that should be passed as the value of I.self
// when the user passed no positional argument to kd.eval.
// It is a uu-entity that has a "self_not_specified" attribute and nothing else,
// so that getting a different attribute from it produces an error
// that can point the user to the right direction. We might customize
// the error message further if this turns out to be not enough.
const DataSlice& UnspecifiedSelfInput();

}  // namespace koladata::expr

#endif  // KOLADATA_EXPR_CONSTANTS_H_
