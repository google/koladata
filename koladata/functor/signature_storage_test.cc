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
#include "koladata/functor/signature_storage.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/signature.h"
#include "koladata/functor/signature_utils.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/object_factories.h"
#include "koladata/operators/masking.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata::functor {

namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::FieldsAre;
using ::testing::HasSubstr;
using ::testing::Optional;

TEST(RoundTripTest, Basic) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto my_obj,
                       ObjectCreator::FromAttrs(DataBag::Empty(), {"foo"},
                                                {test::DataItem(57)}));
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOrKeyword,
      .default_value = my_obj,
  };
  Signature::Parameter p3 = {
      .name = "c",
      .kind = Signature::Parameter::Kind::kVarPositional,
  };
  Signature::Parameter p4 = {
      .name = "d",
      .kind = Signature::Parameter::Kind::kKeywordOnly,
      .default_value = test::DataItem(std::nullopt, schema::kInt32),
  };
  Signature::Parameter p5 = {
      .name = "e",
      .kind = Signature::Parameter::Kind::kVarKeyword,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2, p3, p4, p5}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(auto signature2,
                       KodaSignatureToCppSignature(koda_signature));
  EXPECT_THAT(
      signature2.parameters(),
      ElementsAre(
          FieldsAre(p1.name, p1.kind, Eq(std::nullopt)),
          FieldsAre(p2.name, p2.kind,
                    Optional(IsEquivalentTo(p2.default_value.value().WithBag(
                        koda_signature.GetBag())))),
          FieldsAre(p3.name, p3.kind, Eq(std::nullopt)),
          FieldsAre(p4.name, p4.kind,
                    Optional(IsEquivalentTo(p4.default_value.value().WithBag(
                        koda_signature.GetBag())))),
          FieldsAre(p5.name, p5.kind, Eq(std::nullopt))));
  // Verify that we have adopted the triples of the default value.
  EXPECT_THAT(signature2.parameters()[1].default_value->GetAttr("foo"),
              IsOkAndHolds(
                  IsEquivalentTo(test::DataItem(57, koda_signature.GetBag()))));
}

TEST(KodaSignatureToCppSignatureTest, NonScalar) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      auto koda_signature_1d,
      koda_signature.Reshape(DataSlice::JaggedShape::FlatFromSize(1)));
  EXPECT_THAT(
      KodaSignatureToCppSignature(koda_signature_1d),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "signature must be a data item, but has shape: JaggedShape(1)"));
}

TEST(KodaSignatureToCppSignatureTest, DetachDefaultValuesDb) {
  internal::DataItem obj(internal::AllocateSingleObject());
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
      .default_value = *DataSlice::Create(
          obj, internal::DataItem(schema::kObject), DataBag::Empty()),
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(
      Signature with_db,
      KodaSignatureToCppSignature(koda_signature,
                                  /*detach_default_values_db=*/false));
  ASSERT_OK_AND_ASSIGN(
      Signature without_db,
      KodaSignatureToCppSignature(koda_signature,
                                  /*detach_default_values_db=*/true));
  EXPECT_NE(with_db.parameters()[0].default_value->GetBag(), nullptr);
  EXPECT_EQ(without_db.parameters()[0].default_value->GetBag(), nullptr);
}

TEST(KodaSignatureToCppSignatureTest, Missing) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto koda_signature_missing,
                       ops::ApplyMask(koda_signature, missing));
  EXPECT_THAT(
      KodaSignatureToCppSignature(koda_signature_missing),
      StatusIs(absl::StatusCode::kInvalidArgument, "signature is missing"));
}

TEST(KodaSignatureToCppSignatureTest, MissingParameterList) {
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(koda_signature, koda_signature.ForkBag());
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto parameters, koda_signature.GetAttr("parameters"));
  ASSERT_OK_AND_ASSIGN(auto parameters_missing,
                       ops::ApplyMask(parameters, missing));
  ASSERT_OK(koda_signature.SetAttr("parameters", parameters_missing));
  EXPECT_THAT(
      KodaSignatureToCppSignature(koda_signature),
      StatusIs(absl::StatusCode::kInvalidArgument, "parameters are missing"));
}

TEST(KodaSignatureToCppSignatureTest, MissingParameter) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(koda_signature, koda_signature.ForkBag());
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto parameters, koda_signature.GetAttr("parameters"));
  ASSERT_OK(parameters.SetInList(test::DataItem(0), missing));
  EXPECT_THAT(
      KodaSignatureToCppSignature(koda_signature),
      StatusIs(absl::StatusCode::kInvalidArgument, "parameter 0 is missing"));
}

TEST(KodaSignatureToCppSignatureTest, NonTextParameterName) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(koda_signature, koda_signature.ForkBag());
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto parameters, koda_signature.GetAttr("parameters"));
  ASSERT_OK_AND_ASSIGN(auto parameter,
                       parameters.GetFromList(test::DataItem(0)));
  ASSERT_OK(parameter.SetAttr("name", test::DataItem(57)));
  EXPECT_THAT(KodaSignatureToCppSignature(koda_signature),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "parameter 0 does not have a text name"));
}

TEST(KodaSignatureToCppSignatureTest, DuplicateParameterName) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  Signature::Parameter p2 = {
      .name = "b",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1, p2}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(koda_signature, koda_signature.ForkBag());
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto parameters, koda_signature.GetAttr("parameters"));
  ASSERT_OK_AND_ASSIGN(auto parameter,
                       parameters.GetFromList(test::DataItem(0)));
  ASSERT_OK(parameter.SetAttr("name", test::DataItem(arolla::Text("b"))));
  EXPECT_THAT(KodaSignatureToCppSignature(koda_signature),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "duplicate parameter name: [b]"));
}

TEST(KodaSignatureToCppSignatureTest, InvalidKind) {
  Signature::Parameter p1 = {
      .name = "a",
      .kind = Signature::Parameter::Kind::kPositionalOnly,
  };
  ASSERT_OK_AND_ASSIGN(auto signature, Signature::Create({p1}));
  ASSERT_OK_AND_ASSIGN(auto koda_signature,
                       CppSignatureToKodaSignature(signature));
  ASSERT_OK_AND_ASSIGN(koda_signature, koda_signature.ForkBag());
  auto missing = test::DataItem(arolla::kMissing, schema::kMask);
  ASSERT_OK_AND_ASSIGN(auto parameters, koda_signature.GetAttr("parameters"));
  ASSERT_OK_AND_ASSIGN(auto parameter,
                       parameters.GetFromList(test::DataItem(0)));
  ASSERT_OK(parameter.SetAttr("kind", test::DataItem(57)));
  EXPECT_THAT(KodaSignatureToCppSignature(koda_signature),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("unknown parameter kind")));
}

}  // namespace

}  // namespace koladata::functor
