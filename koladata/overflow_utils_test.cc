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
#include "koladata/overflow_utils.h"

#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/numeric/int128.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/meta.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;

constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();
constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();

// =========================================================================
// SafeMul tests
// =========================================================================

TEST(SafeMulTest, NormalCases) {
  EXPECT_THAT(SafeMul(3, 4), IsOkAndHolds(12));
  EXPECT_THAT(SafeMul(0, kInt64Max), IsOkAndHolds(0));
  EXPECT_THAT(SafeMul(1, kInt64Max), IsOkAndHolds(kInt64Max));
  EXPECT_THAT(SafeMul(-1, 5), IsOkAndHolds(-5));
  EXPECT_THAT(SafeMul(-3, -4), IsOkAndHolds(12));
}

TEST(SafeMulTest, OverflowCases) {
  EXPECT_THAT(SafeMul(kInt64Max, 2),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeMul(2, kInt64Max),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeMul(kInt64Max / 2 + 1, 2),
              StatusIs(absl::StatusCode::kInvalidArgument));
  // d * d where d = 2^32 → 2^64, overflows int64.
  EXPECT_THAT(SafeMul(int64_t{1} << 32, int64_t{1} << 32),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeMul(kInt64Min, -1),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeMul(-1, kInt64Min),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SafeMulHelperTest, StickyOverflow) {
  bool overflow = true;
  EXPECT_EQ(safe_mul(3, 4, &overflow), 12);
  EXPECT_TRUE(overflow);
}

TEST(SafeMulHelperTest, ParametricReturnType) {
  bool overflow = false;
  auto res1 = safe_mul(2, 3ul, &overflow);
  static_assert(std::is_same_v<decltype(res1), int64_t>);
  EXPECT_EQ(res1, 6);
  EXPECT_FALSE(overflow);

  auto res2 = safe_mul<int64_t>(2, 3ul, &overflow);
  static_assert(std::is_same_v<decltype(res2), int64_t>);
  EXPECT_EQ(res2, 6);
  EXPECT_FALSE(overflow);

  auto res3 = safe_mul<int32_t>(2, 3ul, &overflow);
  static_assert(std::is_same_v<decltype(res3), int32_t>);
  EXPECT_EQ(res3, 6);
  EXPECT_FALSE(overflow);

  auto res4 = safe_mul<uint64_t>(2, 3ul, &overflow);
  static_assert(std::is_same_v<decltype(res4), uint64_t>);
  EXPECT_EQ(res4, 6ul);
  EXPECT_FALSE(overflow);
}

// =========================================================================
// SafeAdd tests
// =========================================================================

TEST(SafeAddTest, NormalCases) {
  EXPECT_THAT(SafeAdd(3, 4), IsOkAndHolds(7));
  EXPECT_THAT(SafeAdd(0, 0), IsOkAndHolds(0));
  EXPECT_THAT(SafeAdd(-1, 1), IsOkAndHolds(0));
  EXPECT_THAT(SafeAdd(kInt64Max - 1, 1), IsOkAndHolds(kInt64Max));
  EXPECT_THAT(SafeAdd(kInt64Min + 1, -1), IsOkAndHolds(kInt64Min));
}

TEST(SafeAddTest, OverflowCases) {
  EXPECT_THAT(SafeAdd(kInt64Max, 1),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeAdd(1, kInt64Max),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeAdd(kInt64Min, -1),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeAdd(-1, kInt64Min),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(SafeAdd(kInt64Max / 2 + 1, kInt64Max / 2 + 1),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SafeAddHelperTest, StickyOverflow) {
  bool overflow = true;
  EXPECT_EQ(safe_add(3, 4, &overflow), 7);
  EXPECT_TRUE(overflow);
}

TEST(SafeAddHelperTest, ParametricReturnType) {
  bool overflow = false;
  auto res1 = safe_add(2, 2ul, &overflow);
  static_assert(std::is_same_v<decltype(res1), int64_t>);
  EXPECT_EQ(res1, 4);
  EXPECT_FALSE(overflow);

  auto res2 = safe_add<int64_t>(2, 2ul, &overflow);
  static_assert(std::is_same_v<decltype(res2), int64_t>);
  EXPECT_EQ(res2, 4);
  EXPECT_FALSE(overflow);

  auto res3 = safe_add<int32_t>(2, 2ul, &overflow);
  static_assert(std::is_same_v<decltype(res3), int32_t>);
  EXPECT_EQ(res3, 4);
  EXPECT_FALSE(overflow);

  auto res4 = safe_add<uint64_t>(2, 2ul, &overflow);
  static_assert(std::is_same_v<decltype(res4), uint64_t>);
  EXPECT_EQ(res4, 4ul);
  EXPECT_FALSE(overflow);
}

// =========================================================================
// SafeAbs tests
// =========================================================================

TEST(SafeAbsTest, NormalCases) {
  EXPECT_THAT(SafeAbs(0), IsOkAndHolds(0));
  EXPECT_THAT(SafeAbs(42), IsOkAndHolds(42));
  EXPECT_THAT(SafeAbs(-42), IsOkAndHolds(42));
  EXPECT_THAT(SafeAbs(kInt64Max), IsOkAndHolds(kInt64Max));
  EXPECT_THAT(SafeAbs(kInt64Min + 1), IsOkAndHolds(kInt64Max));
}

TEST(SafeAbsTest, Int64MinFails) {
  EXPECT_THAT(SafeAbs(kInt64Min), StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(SafeAbsHelperTest, StickyOverflow) {
  bool overflow = true;
  EXPECT_EQ(safe_abs(42, &overflow), 42);
  EXPECT_TRUE(overflow);
}

TEST(SafeAbsHelperTest, ParametricReturnType) {
  bool overflow = false;
  auto res1 = safe_abs(-42, &overflow);
  static_assert(std::is_same_v<decltype(res1), int64_t>);
  EXPECT_EQ(res1, 42);
  EXPECT_FALSE(overflow);

  auto res2 = safe_abs<int64_t>(-42, &overflow);
  static_assert(std::is_same_v<decltype(res2), int64_t>);
  EXPECT_EQ(res2, 42);
  EXPECT_FALSE(overflow);

  auto res3 = safe_abs<int32_t>(-42, &overflow);
  static_assert(std::is_same_v<decltype(res3), int32_t>);
  EXPECT_EQ(res3, 42);
  EXPECT_FALSE(overflow);

  auto res4 = safe_abs<uint64_t>(-42, &overflow);
  static_assert(std::is_same_v<decltype(res4), uint64_t>);
  EXPECT_EQ(res4, 42ul);
  EXPECT_FALSE(overflow);
}

// =========================================================================
// Exhaustive narrow-type tests (int8/uint8 → int8/uint8/int16/uint16)
//
// These iterate over all 256×256 input combinations for 8-bit types and
// verify results against a portable wide reference (absl::int128), ensuring
// correctness across all signed/unsigned input and result type combinations.
// Using absl::int128 (rather than __int128) keeps these tests portable, so
// they can also verify a custom implementation on platforms that lack the
// __builtin_*_overflow intrinsics.
// =========================================================================

// Returns true if `wide_value` fits in [Res_min, Res_max].
template <typename Res>
bool FitsInType(absl::int128 wide_value) {
  return wide_value >= absl::int128(std::numeric_limits<Res>::min()) &&
         wide_value <= absl::int128(std::numeric_limits<Res>::max());
}

using NarrowArgTypes = arolla::meta::type_list<int8_t, uint8_t>;
using NarrowResultTypes =
    arolla::meta::type_list<int8_t, uint8_t, int16_t, uint16_t>;

// --- Exhaustive safe_mul ---

template <typename Res, typename Lhs, typename Rhs>
void ExhaustiveMulTest() {
  for (int lhs = std::numeric_limits<Lhs>::min();
       lhs <= std::numeric_limits<Lhs>::max(); ++lhs) {
    for (int rhs = std::numeric_limits<Rhs>::min();
         rhs <= std::numeric_limits<Rhs>::max(); ++rhs) {
      Lhs x = static_cast<Lhs>(lhs);
      Rhs y = static_cast<Rhs>(rhs);
      absl::int128 wide_value = absl::int128(x) * absl::int128(y);
      bool expect_overflow = !FitsInType<Res>(wide_value);

      bool overflow = false;
      Res result = safe_mul<Res>(x, y, &overflow);
      ASSERT_EQ(overflow, expect_overflow)
          << "safe_mul<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
          << "{" << lhs << "}, " << typeid(Rhs).name() << "{" << rhs
          << "}): expected overflow=" << expect_overflow;
      if (!expect_overflow) {
        ASSERT_EQ(result, static_cast<Res>(wide_value))
            << "safe_mul<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
            << "{" << lhs << "}, " << typeid(Rhs).name() << "{" << rhs << "})";
      }
    }
  }
}

TEST(ExhaustiveNarrowTest, Multiplication) {
  arolla::meta::foreach_type<NarrowArgTypes>([&](auto lhs_type) {
    arolla::meta::foreach_type<NarrowArgTypes>([&](auto rhs_type) {
      arolla::meta::foreach_type<NarrowResultTypes>([&](auto res_type) {
        using Lhs = typename decltype(lhs_type)::type;
        using Rhs = typename decltype(rhs_type)::type;
        using Res = typename decltype(res_type)::type;
        ExhaustiveMulTest<Res, Lhs, Rhs>();
      });
    });
  });
}

// --- Exhaustive safe_add ---

template <typename Res, typename Lhs, typename Rhs>
void ExhaustiveAddTest() {
  for (int lhs = std::numeric_limits<Lhs>::min();
       lhs <= std::numeric_limits<Lhs>::max(); ++lhs) {
    for (int rhs = std::numeric_limits<Rhs>::min();
         rhs <= std::numeric_limits<Rhs>::max(); ++rhs) {
      Lhs x = static_cast<Lhs>(lhs);
      Rhs y = static_cast<Rhs>(rhs);
      absl::int128 wide_value = absl::int128(x) + absl::int128(y);
      bool expect_overflow = !FitsInType<Res>(wide_value);

      bool overflow = false;
      Res result = safe_add<Res>(x, y, &overflow);
      ASSERT_EQ(overflow, expect_overflow)
          << "safe_add<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
          << "{" << lhs << "}, " << typeid(Rhs).name() << "{" << rhs
          << "}): expected overflow=" << expect_overflow;
      if (!expect_overflow) {
        ASSERT_EQ(result, static_cast<Res>(wide_value))
            << "safe_add<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
            << "{" << lhs << "}, " << typeid(Rhs).name() << "{" << rhs << "})";
      }
    }
  }
}

TEST(ExhaustiveNarrowTest, Addition) {
  arolla::meta::foreach_type<NarrowArgTypes>([&](auto lhs_type) {
    arolla::meta::foreach_type<NarrowArgTypes>([&](auto rhs_type) {
      arolla::meta::foreach_type<NarrowResultTypes>([&](auto res_type) {
        using Lhs = typename decltype(lhs_type)::type;
        using Rhs = typename decltype(rhs_type)::type;
        using Res = typename decltype(res_type)::type;
        ExhaustiveAddTest<Res, Lhs, Rhs>();
      });
    });
  });
}

// --- Exhaustive safe_abs ---

template <typename Res, typename T>
void ExhaustiveAbsTest() {
  for (int val = std::numeric_limits<T>::min();
       val <= std::numeric_limits<T>::max(); ++val) {
    T x = static_cast<T>(val);
    absl::int128 wide_value = absl::int128(x);
    if (wide_value < 0) wide_value = -wide_value;
    bool expect_overflow = !FitsInType<Res>(wide_value);

    bool overflow = false;
    Res result = safe_abs<Res>(x, &overflow);
    ASSERT_EQ(overflow, expect_overflow)
        << "safe_abs<" << typeid(Res).name() << ">(" << typeid(T).name() << "{"
        << val << "}): expected overflow=" << expect_overflow;
    if (!expect_overflow) {
      ASSERT_EQ(result, static_cast<Res>(wide_value))
          << "safe_abs<" << typeid(Res).name() << ">(" << typeid(T).name()
          << "{" << val << "})";
    }
  }
}

TEST(ExhaustiveNarrowTest, AbsoluteValue) {
  arolla::meta::foreach_type<NarrowArgTypes>([&](auto arg_type) {
    arolla::meta::foreach_type<NarrowResultTypes>([&](auto res_type) {
      using T = typename decltype(arg_type)::type;
      using Res = typename decltype(res_type)::type;
      ExhaustiveAbsTest<Res, T>();
    });
  });
}

// =========================================================================
// Wider-type boundary tests
//
// For each k-bit integer type T, generates a set of boundary values near
// the extremes:
//   signed T:   [min, min+64] union [-64, 64] union [max-64, max]
//   unsigned T: [0, 64] union [max-64, max]
// and verifies all pairwise operations against absl::int128.
//
// Parameterized over multiple type widths (16, 32, 64) and both signed
// and unsigned variants, with various result types including same-width,
// wider, narrower, and opposite-signedness conversions.
// =========================================================================

using WiderArgTypes = arolla::meta::type_list<int16_t, uint16_t, int32_t,
                                              uint32_t, int64_t, uint64_t>;
using WiderResultTypes = arolla::meta::type_list<int16_t, uint16_t, int32_t,
                                                 uint32_t, int64_t, uint64_t>;

template <typename T>
std::vector<T> BoundaryValues() {
  static_assert(sizeof(T) >= 2,
                "Use exhaustive tests for 8-bit types instead.");
  std::vector<T> vals;
  constexpr T kMin = std::numeric_limits<T>::min();
  constexpr T kMax = std::numeric_limits<T>::max();
  // Near T min.
  for (int i = 0; i <= 64; ++i) {
    vals.push_back(static_cast<T>(kMin + i));
  }
  // Near zero: [-64, 64] for signed types.
  if constexpr (std::is_signed_v<T>) {
    for (int i = -64; i <= 64; ++i) {
      vals.push_back(static_cast<T>(i));
    }
  }
  // Near T max.
  for (int i = 64; i >= 0; --i) {
    vals.push_back(static_cast<T>(kMax - i));
  }
  return vals;
}

// --- Boundary safe_mul ---

template <typename Res, typename Lhs, typename Rhs>
void BoundaryMulTest() {
  const auto lhs_vals = BoundaryValues<Lhs>();
  const auto rhs_vals = BoundaryValues<Rhs>();
  for (Lhs x : lhs_vals) {
    for (Rhs y : rhs_vals) {
      if (x >= std::numeric_limits<uint64_t>::max() - 64 &&
          y >= std::numeric_limits<uint64_t>::max() - 64) {
        // On some compilers, the absl::int128(x) * absl::int128(y) below does
        // not wrap around, causing the expect_overflow value to be false when
        // it should be true. We skip these cases, and cover them in the manual
        // sixty-four bit test below.
        continue;
      }

      absl::int128 wide_value = absl::int128(x) * absl::int128(y);
      bool expect_overflow = !FitsInType<Res>(wide_value);

      bool overflow = false;
      Res result = safe_mul<Res>(x, y, &overflow);
      ASSERT_EQ(overflow, expect_overflow)
          << "safe_mul<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
          << "{" << absl::int128(x) << "}, " << typeid(Rhs).name() << "{"
          << absl::int128(y) << "}): expected overflow=" << expect_overflow;
      if (!expect_overflow) {
        ASSERT_EQ(result, static_cast<Res>(wide_value))
            << "safe_mul<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
            << "{" << absl::int128(x) << "}, " << typeid(Rhs).name() << "{"
            << absl::int128(y) << "})";
      }
    }
  }
}

TEST(WiderTypeBoundaryTest, Multiplication) {
  arolla::meta::foreach_type<WiderArgTypes>([&](auto lhs_type) {
    arolla::meta::foreach_type<WiderArgTypes>([&](auto rhs_type) {
      arolla::meta::foreach_type<WiderResultTypes>([&](auto res_type) {
        using Lhs = typename decltype(lhs_type)::type;
        using Rhs = typename decltype(rhs_type)::type;
        using Res = typename decltype(res_type)::type;
        BoundaryMulTest<Res, Lhs, Rhs>();
      });
    });
  });
}

// The above test works "incidentally" for uint64_t on some compilers: the
// result of uint64_t multiplication can overflow int128, e.g.
// (2**64 - 1)**2 > 2**127 - 1
// but then the overflow result is negative and smaller than int64::min, which
// satisfies the expectation of overflow in uint64_t and int64_t results.
// However, we skip these cases above, because some compilers do not produce the
// expected overflow result that wraps around.
// One option to deal with it is to use wider-than-int128 types for the
// wide_value, e.g. the cpp_int of boost/multiprecision, or a signed 256-bit
// integer type from the ac_datatypes library (ac_int<256, true>).
// Another option, which we use here, is to manually test the cases where the
// multiplication inputs are close to the max values.

using SixtyFourBitTypes = arolla::meta::type_list<int64_t, uint64_t>;

template <typename T>
std::vector<T> TopBoundaryValues() {
  std::vector<T> vals;
  constexpr T kMax = std::numeric_limits<T>::max();
  // Near T max.
  for (int i = 64; i >= 0; --i) {
    vals.push_back(static_cast<T>(kMax - i));
  }
  return vals;
}

TEST(WiderTypeBoundaryTest, ManualSixtyFourBitMultiplicationOverflow) {
  arolla::meta::foreach_type<SixtyFourBitTypes>([&](auto lhs_type) {
    arolla::meta::foreach_type<SixtyFourBitTypes>([&](auto rhs_type) {
      arolla::meta::foreach_type<WiderResultTypes>([&](auto res_type) {
        using Lhs = typename decltype(lhs_type)::type;
        using Rhs = typename decltype(rhs_type)::type;
        using Res = typename decltype(res_type)::type;
        for (Lhs x : TopBoundaryValues<Lhs>()) {
          for (Rhs y : TopBoundaryValues<Rhs>()) {
            bool overflow = false;
            safe_mul<Res>(x, y, &overflow);
            ASSERT_TRUE(overflow);
          }
        }
      });
    });
  });
}

// --- Boundary safe_add ---

template <typename Res, typename Lhs, typename Rhs>
void BoundaryAddTest() {
  const auto lhs_vals = BoundaryValues<Lhs>();
  const auto rhs_vals = BoundaryValues<Rhs>();
  for (Lhs x : lhs_vals) {
    for (Rhs y : rhs_vals) {
      absl::int128 wide_value = absl::int128(x) + absl::int128(y);
      bool expect_overflow = !FitsInType<Res>(wide_value);

      bool overflow = false;
      Res result = safe_add<Res>(x, y, &overflow);
      ASSERT_EQ(overflow, expect_overflow)
          << "safe_add<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
          << "{" << absl::int128(x) << "}, " << typeid(Rhs).name() << "{"
          << absl::int128(y) << "}): expected overflow=" << expect_overflow;
      if (!expect_overflow) {
        ASSERT_EQ(result, static_cast<Res>(wide_value))
            << "safe_add<" << typeid(Res).name() << ">(" << typeid(Lhs).name()
            << "{" << absl::int128(x) << "}, " << typeid(Rhs).name() << "{"
            << absl::int128(y) << "})";
      }
    }
  }
}

TEST(WiderTypeBoundaryTest, Addition) {
  arolla::meta::foreach_type<WiderArgTypes>([&](auto lhs_type) {
    arolla::meta::foreach_type<WiderArgTypes>([&](auto rhs_type) {
      arolla::meta::foreach_type<WiderResultTypes>([&](auto res_type) {
        using Lhs = typename decltype(lhs_type)::type;
        using Rhs = typename decltype(rhs_type)::type;
        using Res = typename decltype(res_type)::type;
        BoundaryAddTest<Res, Lhs, Rhs>();
      });
    });
  });
}

// --- Boundary safe_abs ---

template <typename Res, typename T>
void BoundaryAbsTest() {
  const auto vals = BoundaryValues<T>();
  for (T x : vals) {
    absl::int128 wide_value = absl::int128(x);
    if (wide_value < 0) wide_value = -wide_value;
    bool expect_overflow = !FitsInType<Res>(wide_value);

    bool overflow = false;
    Res result = safe_abs<Res>(x, &overflow);
    ASSERT_EQ(overflow, expect_overflow)
        << "safe_abs<" << typeid(Res).name() << ">(" << typeid(T).name() << "{"
        << absl::int128(x) << "}): expected overflow=" << expect_overflow;
    if (!expect_overflow) {
      ASSERT_EQ(result, static_cast<Res>(wide_value))
          << "safe_abs<" << typeid(Res).name() << ">(" << typeid(T).name()
          << "{" << absl::int128(x) << "})";
    }
  }
}

TEST(WiderTypeBoundaryTest, AbsoluteValue) {
  arolla::meta::foreach_type<WiderArgTypes>([&](auto arg_type) {
    arolla::meta::foreach_type<WiderResultTypes>([&](auto res_type) {
      using T = typename decltype(arg_type)::type;
      using Res = typename decltype(res_type)::type;
      BoundaryAbsTest<Res, T>();
    });
  });
}

}  // namespace
}  // namespace koladata
