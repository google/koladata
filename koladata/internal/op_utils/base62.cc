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
#include "koladata/internal/op_utils/base62.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"

namespace koladata::internal {

constexpr static char kBase62Chars[] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr size_t kBase62DigitIndex = 0;
constexpr size_t kBase62UpperCaseIndex = 10;
constexpr size_t kBase62LowerCaseIndex = 36;


constexpr static absl::uint128 kBase =
    std::char_traits<char>::length(kBase62Chars);

arolla::Text EncodeBase62(absl::uint128 val) {
  std::string res;
  if (val == 0) {
    return arolla::Text("0");
  }
  while (val > 0) {
    size_t idx = absl::Uint128Low64(val % kBase);
    res += kBase62Chars[idx];
    val = val / kBase;
  }
  std::reverse(res.begin(), res.end());
  return arolla::Text(std::move(res));
}

absl::uint128 DecodeBase62(absl::string_view text) {
  absl::uint128 res = 0;
  for (char c : text) {
    int idx = 0;
    if (c >= 'a' && c <= 'z') {
      idx = c - 'a' + kBase62LowerCaseIndex;
    } else if (c >= 'A' && c <= 'Z') {
      idx = c - 'A' + kBase62UpperCaseIndex;
    } else if (c >= '0' && c <= '9') {
      idx = c - '0' + kBase62DigitIndex;
    }
    res = res * kBase + idx;
  }
  return res;
}

}  // namespace koladata::internal
