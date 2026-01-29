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
#include "koladata/check_frozen.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/dtype.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

TEST(CheckFrozen, Databag) {
  auto db = DataBag::EmptyMutable();
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(db->Freeze())), IsOk());
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(db)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DataBag is not frozen")));
}

TEST(CheckFrozen, DatabagWithMutableFallbacks) {
  auto db = DataBag::ImmutableEmptyWithDeprecatedMutableFallbacks(
      {DataBag::EmptyMutable()});
  EXPECT_FALSE(db->IsMutable());
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(db)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DataBag is not frozen")));
}

TEST(CheckFrozen, DataSlice) {
  auto ds = test::EntitySchema({"x"}, {test::Schema(schema::kInt32)},
                               DataBag::EmptyMutable());
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(ds.FreezeBag())), IsOk());
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(ds)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("DataSlice is not frozen")));
}

TEST(CheckFrozen, OtherTypes) {
  EXPECT_THAT(CheckFrozen(arolla::TypedRef::FromValue(1)), IsOk());
}

TEST(CheckFrozen, CompoundDataBag) {
  auto db = DataBag::EmptyMutable();
  EXPECT_THAT(CheckFrozen(arolla::MakeTupleFromFields(
                              arolla::TypedRef::FromValue(db->Freeze()))
                              .AsRef()),
              IsOk());
  EXPECT_THAT(
      CheckFrozen(
          arolla::MakeTupleFromFields(arolla::TypedRef::FromValue(db)).AsRef()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataBag is not frozen")));
}

TEST(CheckFrozen, CompoundDataSlice) {
  auto ds = test::EntitySchema({"x"}, {test::Schema(schema::kInt32)},
                               DataBag::EmptyMutable());
  EXPECT_THAT(CheckFrozen(arolla::MakeTupleFromFields(
                              arolla::TypedRef::FromValue(ds.FreezeBag()))
                              .AsRef()),
              IsOk());
  EXPECT_THAT(
      CheckFrozen(
          arolla::MakeTupleFromFields(arolla::TypedRef::FromValue(ds)).AsRef()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataSlice is not frozen")));
}

TEST(CheckFrozen, CompoundNested) {
  auto db = DataBag::EmptyMutable();
  auto ds = test::EntitySchema({"x"}, {test::Schema(schema::kInt32)},
                               DataBag::EmptyMutable());
  auto i32 = arolla::TypedValue::FromValue(1);
  auto f32 = arolla::TypedValue::FromValue(2.5f);
  EXPECT_THAT(
      CheckFrozen(arolla::MakeTupleFromFields(
                      arolla::MakeTupleFromFields(
                          i32, arolla::TypedValue::FromValue(db->Freeze())),
                      *arolla::MakeNamedTuple(
                          {"x", "y"},
                          {f32, arolla::TypedValue::FromValue(ds.FreezeBag())}))
                      .AsRef()),
      IsOk());
  EXPECT_THAT(
      CheckFrozen(arolla::MakeTupleFromFields(
                      arolla::MakeTupleFromFields(
                          i32, arolla::TypedValue::FromValue(db)),
                      *arolla::MakeNamedTuple(
                          {"x", "y"},
                          {f32, arolla::TypedValue::FromValue(ds.FreezeBag())}))
                      .AsRef()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataBag is not frozen")));
  EXPECT_THAT(
      CheckFrozen(arolla::MakeTupleFromFields(
                      arolla::MakeTupleFromFields(
                          i32, arolla::TypedValue::FromValue(db->Freeze())),
                      *arolla::MakeNamedTuple(
                          {"x", "y"}, {f32, arolla::TypedValue::FromValue(ds)}))
                      .AsRef()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("DataSlice is not frozen")));
}

TEST(CheckFrozen, CompoundEmpty) {
  EXPECT_THAT(CheckFrozen(arolla::MakeTupleFromFields().AsRef()), IsOk());
}

}  // namespace
}  // namespace koladata
