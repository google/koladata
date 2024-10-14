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
#ifndef KOLADATA_INTERNAL_OP_UTILS_ITEMID_UTILS_H_
#define KOLADATA_INTERNAL_OP_UTILS_ITEMID_UTILS_H_

#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"

namespace koladata::internal {

// Encode it 128bit unsigned integer into a base62 string.
arolla::Text EncodeBase62(absl::uint128 val);

// Decode a base62 string into a 128bit unsigned integer.
absl::uint128 DecodeBase62(absl::string_view text);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_ITEMID_UTILS_H_
