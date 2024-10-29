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
#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>
#include <cstdint>

#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"

namespace koladata::internal {

constexpr uint64_t kBase62Length = 22;
constexpr static char kBase62Chars[] =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr uint64_t kBase62DigitIndex = 0;
constexpr uint64_t kBase62UpperCaseIndex = 10;
constexpr uint64_t kBase62LowerCaseIndex = 36;

// pow(31**10, -1, 2**64)
constexpr uint64_t kLowInverse = 7459615142622190913;
// pow(31**10, -1, 2**128) / 2**64
constexpr uint64_t kHighInverse = 5689246992239689896;
constexpr absl::uint128 kInverse = absl::MakeUint128(kHighInverse, kLowInverse);
constexpr uint64_t k62Power10 = 839299365868340224;

constexpr static absl::uint128 kBase =
    std::char_traits<char>::length(kBase62Chars);

namespace {

absl::uint128 DivideBy62Power10(absl::uint128& val, uint64_t reminder) {
  // Calculate val / 62^10
  return ((val - reminder) >> 10) * kInverse;
}

}  // namespace

arolla::Text EncodeBase62(absl::uint128 val) {
  std::string res(kBase62Length, '0');
  size_t idx = kBase62Length;

  auto append_char = [&](uint64_t val, uint64_t step_size) {
    for (; step_size > 0; --step_size) {
      uint64_t cidx = val % 62;
      res[--idx] = kBase62Chars[cidx];
      val = val / 62;
    }
  };

  uint64_t reminder = absl::Uint128Low64(val % k62Power10);
  append_char(reminder, 10);

  val = DivideBy62Power10(val, reminder);
  reminder = absl::Uint128Low64(val % k62Power10);
  append_char(reminder, 10);

  val = DivideBy62Power10(val, reminder);
  append_char(absl::Uint128Low64(val), 2);
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
