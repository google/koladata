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
#include "koladata/internal/dtype.h"

#include <cstdint>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "koladata/internal/stable_fingerprint.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace {

// Dummy QType to test VerifyQTypeSupported.
struct New {
  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
    hasher->Combine(1234);
  }
};

}  // namespace

namespace arolla {

AROLLA_DECLARE_SIMPLE_QTYPE(NEW, New);
AROLLA_DEFINE_SIMPLE_QTYPE(NEW, New);

}  // namespace arolla

namespace koladata::schema {
namespace {

using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

using arolla::CreateDenseArray;
using arolla::FingerprintHasher;
using internal::StableFingerprintHasher;

std::vector<arolla::QTypePtr> SupportedQTypes() {
  return {
    arolla::GetQType<int64_t>(),
    arolla::GetQType<int>(),
    arolla::GetQType<float>(),
    arolla::GetQType<double>(),
    arolla::GetQType<bool>(),
    arolla::GetQType<arolla::Unit>(),
    arolla::GetQType<arolla::Bytes>(),
    arolla::GetQType<arolla::Text>(),
    arolla::GetQType<arolla::expr::ExprQuote>(),
  };
}

std::vector<DType> SupportedDTypes() {
  return {GetDType<int64_t>(),
          GetDType<int>(),
          GetDType<float>(),
          GetDType<double>(),
          GetDType<bool>(),
          GetDType<arolla::Unit>(),
          GetDType<arolla::Bytes>(),
          GetDType<arolla::Text>(),
          GetDType<arolla::expr::ExprQuote>(),
          GetDType<AnyDType>(),
          GetDType<ItemIdDType>(),
          GetDType<ObjectDType>(),
          GetDType<SchemaDType>(),
          GetDType<NoneDType>()};
}

// Make sure that all dtypes defined for DataItem and DataSlice (without DType)
// can also be stored in DType itself.
TEST(DType, TypesCoverage) {
  using dtypes_and_dtype = arolla::meta::concat_t<
      supported_dtype_values, arolla::meta::type_list<DType>>;
  arolla::meta::foreach_type(
      internal::supported_primitives_list(), [&](auto tpe) {
        static_assert(arolla::meta::contains_v<dtypes_and_dtype,
                                               typename decltype(tpe)::type>);
      });
}

TEST(DType, DefaultDType) {
  EXPECT_EQ(DType(), kAny);
}

TEST(DType, VerifyQTypeSupported) {
  for (const auto& qtype : SupportedQTypes()) {
    EXPECT_TRUE(DType::VerifyQTypeSupported(qtype));
  }

  EXPECT_FALSE(DType::VerifyQTypeSupported(
      arolla::GetQType<arolla::DenseArray<float>>()));

  EXPECT_FALSE(DType::VerifyQTypeSupported(arolla::GetQType<New>()));

  EXPECT_FALSE(DType::VerifyQTypeSupported(
      arolla::GetQType<arolla::DenseArray<arolla::Text>>()));

  EXPECT_FALSE(DType::VerifyQTypeSupported(
      arolla::GetQType<arolla::OptionalValue<float>>()));
}

TEST(DType, IsPrimitive) {
  EXPECT_TRUE(kInt32.is_primitive());
  EXPECT_TRUE(kInt64.is_primitive());
  EXPECT_TRUE(kFloat32.is_primitive());
  EXPECT_TRUE(kFloat64.is_primitive());
  EXPECT_TRUE(kBool.is_primitive());
  EXPECT_TRUE(kMask.is_primitive());
  EXPECT_TRUE(kBytes.is_primitive());
  EXPECT_TRUE(kText.is_primitive());
  EXPECT_TRUE(kExpr.is_primitive());

  // Non-primitives
  EXPECT_FALSE(kAny.is_primitive());
  EXPECT_FALSE(kObject.is_primitive());
  EXPECT_FALSE(kSchema.is_primitive());
  EXPECT_FALSE(kItemId.is_primitive());
  EXPECT_FALSE(kNone.is_primitive());
}

TEST(DType, Fingerprint) {
  auto dtypes = SupportedDTypes();
  for (int i = 0; i < dtypes.size() - 1; ++i) {
    EXPECT_EQ(FingerprintHasher("salt").Combine(dtypes[i]).Finish(),
              FingerprintHasher("salt").Combine(dtypes[i]).Finish());
    for (int j = i + 1; j < dtypes.size(); ++j) {
      EXPECT_NE(FingerprintHasher("salt").Combine(dtypes[i]).Finish(),
                FingerprintHasher("salt").Combine(dtypes[j]).Finish());
    }
  }
}

TEST(DType, StableFingerprint) {
  auto dtypes = SupportedDTypes();
  for (int i = 0; i < dtypes.size() - 1; ++i) {
    EXPECT_EQ(
        StableFingerprintHasher("salt").Combine(dtypes[i]).Finish(),
        StableFingerprintHasher("salt").Combine(dtypes[i]).Finish());
    for (int j = i + 1; j < dtypes.size(); ++j) {
      EXPECT_NE(
          StableFingerprintHasher("salt").Combine(dtypes[i]).Finish(),
          StableFingerprintHasher("salt").Combine(dtypes[j]).Finish());
    }
  }
}

TEST(DType, FromQType) {
  for (const auto& qtype : SupportedQTypes()) {
    ASSERT_OK_AND_ASSIGN(DType dtype, DType::FromQType(qtype));
    EXPECT_EQ(dtype.qtype(), qtype);
  }
  EXPECT_THAT(DType::FromQType(arolla::GetQType<arolla::DenseArray<int>>()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unsupported QType: DENSE_ARRAY_INT32")));
  EXPECT_EQ(GetDType<AnyDType>().qtype(), arolla::GetNothingQType());
  EXPECT_EQ(GetDType<ItemIdDType>().qtype(), arolla::GetNothingQType());
  EXPECT_EQ(GetDType<ObjectDType>().qtype(), arolla::GetNothingQType());
  EXPECT_EQ(GetDType<SchemaDType>().qtype(), arolla::GetNothingQType());
  EXPECT_EQ(GetDType<NoneDType>().qtype(), arolla::GetNothingQType());
}

TEST(DType, TypeId) {
  for (const auto& dtype : SupportedDTypes()) {
    EXPECT_EQ(DType(dtype.type_id()), dtype);
    EXPECT_GE(static_cast<int>(dtype.type_id()), 0);
    EXPECT_LT(static_cast<int>(dtype.type_id()), kNextDTypeId);
  }
}

TEST(DType, Equality) {
  EXPECT_EQ(schema::kInt32, schema::kInt32);
  EXPECT_NE(schema::kFloat32, schema::kInt32);
}

TEST(DType, AbslHash) {
  std::vector<DType> cases{
    kInt32,
    kInt32,
    kFloat32,
    kText,
    kText,
    kBytes,
    kExpr,
  };
  // NOTE: Here we test each type through `supported_dtype_values` which has
  // the benefit of testing any newly added types. The upper part is something
  // that makes sure we test at least something in case `supported_dtype_values`
  // reduce.
  arolla::meta::foreach_type(supported_dtype_values(), [&](auto tpe) {
    // 2x to also test equivalence classes.
    cases.push_back(GetDType<typename decltype(tpe)::type>());
    cases.push_back(GetDType<typename decltype(tpe)::type>());
  });
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(cases));
}

TEST(DType, DenseArray) {
  auto array = CreateDenseArray<DType>({kInt32, std::nullopt, kAny});

  EXPECT_TRUE(array[0].present);
  EXPECT_FALSE(array[1].present);
  EXPECT_TRUE(array[2].present);

  EXPECT_EQ(array[0].value.qtype(), arolla::GetQType<int>());
  EXPECT_EQ(array[2].value, kAny);
}

TEST(DType, name) {
  // Base types.
  EXPECT_EQ(kInt32.name(), "INT32");
  EXPECT_EQ(kInt64.name(), "INT64");
  EXPECT_EQ(kFloat32.name(), "FLOAT32");
  EXPECT_EQ(kFloat64.name(), "FLOAT64");
  EXPECT_EQ(kBool.name(), "BOOLEAN");
  EXPECT_EQ(kMask.name(), "MASK");
  EXPECT_EQ(kBytes.name(), "BYTES");
  EXPECT_EQ(kText.name(), "STRING");
  EXPECT_EQ(kExpr.name(), "EXPR");

  // Special meaning - Schema types
  EXPECT_EQ(kAny.name(), "ANY");
  EXPECT_EQ(kObject.name(), "OBJECT");
  EXPECT_EQ(kSchema.name(), "SCHEMA");
  EXPECT_EQ(kItemId.name(), "ITEMID");
  EXPECT_EQ(kNone.name(), "NONE");

  arolla::meta::foreach_type(supported_dtype_values(), [&](auto tpe) {
    using T = typename decltype(tpe)::type;
    EXPECT_FALSE(GetDType<T>().name().empty());
  });
}

TEST(DType, AbslStringify) {
  for (DType dtype : SupportedDTypes()) {
    EXPECT_EQ(absl::StrCat(dtype), dtype.name());
  }
}

TEST(DType, TypedValueRepr) {
  for (DType dtype : SupportedDTypes()) {
    EXPECT_EQ(arolla::TypedValue::FromValue(dtype).Repr(), dtype.name());
  }
}

}  // namespace
}  // namespace koladata::schema
