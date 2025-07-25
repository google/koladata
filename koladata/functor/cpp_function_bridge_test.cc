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
#include "koladata/functor/cpp_function_bridge.h"

#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/testing/matchers.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/serialization/encode.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"
#include "koladata/functor/call.h"
#include "koladata/functor/parallel/future.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/matchers.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::QValueWith;
using ::koladata::testing::IsEquivalentTo;
using ::testing::HasSubstr;

TEST(CppFunctionBridgeTest, CreateFunctorFromStdFunction) {
  ASSERT_OK_AND_ASSIGN(
      DataSlice functor,
      CreateFunctorFromStdFunction(
          [](absl::Span<const arolla::TypedRef> args)
              -> absl::StatusOr<arolla::TypedValue> {
            ASSIGN_OR_RETURN(DataSlice obj, args[0].As<DataSlice>());
            ASSIGN_OR_RETURN(DataSlice val, args[1].As<DataSlice>());
            ASSIGN_OR_RETURN(DataBagPtr db, args[2].As<DataBagPtr>());
            RETURN_IF_ERROR(obj.WithBag(db).SetAttr("a", val));
            return arolla::TypedValue::FromValue(db);
          },
          "my_functor", "obj, val, db", arolla::GetQType<DataBagPtr>()));
  auto db = DataBag::Empty();
  internal::DataItem obj_item(internal::AllocateSingleObject());
  internal::DataItem a_schema(schema::kInt32);
  ASSERT_OK_AND_ASSIGN(
      internal::DataItem obj_schema,
      db->GetMutableImpl()->get().CreateExplicitSchemaFromFields({"a"},
                                                                 {a_schema}));
  ASSERT_OK_AND_ASSIGN(auto obj, DataSlice::Create(obj_item, obj_schema));
  auto val = DataSlice::CreateFromScalar(42);
  ASSERT_OK_AND_ASSIGN(arolla::TypedValue res_tv,
                       CallFunctorWithCompilationCache(
                           functor,
                           {arolla::TypedValue::FromValue(obj).AsRef(),
                            arolla::TypedValue::FromValue(val).AsRef(),
                            arolla::TypedValue::FromValue(db).AsRef()},
                           {"obj", "val", "db"}));
  ASSERT_OK_AND_ASSIGN(DataBagPtr res_db, res_tv.As<DataBagPtr>());
  EXPECT_EQ(db, res_db);
  ASSERT_OK_AND_ASSIGN(DataSlice res_a, obj.WithBag(db).GetAttr("a"));
  ASSERT_TRUE(res_a.is_item());
  EXPECT_EQ(res_a.item(), internal::DataItem(42));
}

TEST(CppFunctionBridgeTest, CreateFunctorFromFunction) {
  ASSERT_OK_AND_ASSIGN(
      DataSlice functor,
      CreateFunctorFromFunction(
          [](const DataSlice& obj, const DataSlice& val,
             const DataBagPtr& db) -> absl::StatusOr<DataSlice> {
            auto res_obj = obj.WithBag(db);
            RETURN_IF_ERROR(res_obj.SetAttr("a", val));
            return res_obj;
          },
          "my_functor", "obj, val, db"));
  auto db = DataBag::Empty();
  internal::DataItem obj_item(internal::AllocateSingleObject());
  internal::DataItem a_schema(schema::kInt32);
  ASSERT_OK_AND_ASSIGN(
      internal::DataItem obj_schema,
      db->GetMutableImpl()->get().CreateExplicitSchemaFromFields({"a"},
                                                                 {a_schema}));
  ASSERT_OK_AND_ASSIGN(
      auto obj,
      DataSlice::Create(obj_item, obj_schema));
  auto val = DataSlice::CreateFromScalar(42);
  ASSERT_OK_AND_ASSIGN(arolla::TypedValue res_tv,
                       CallFunctorWithCompilationCache(
                           functor,
                           {arolla::TypedValue::FromValue(obj).AsRef(),
                            arolla::TypedValue::FromValue(val).AsRef(),
                            arolla::TypedValue::FromValue(db).AsRef()},
                           {"obj", "val", "db"}));
  ASSERT_OK_AND_ASSIGN(DataSlice res, res_tv.As<DataSlice>());
  ASSERT_OK_AND_ASSIGN(DataSlice res_a, res.GetAttr("a"));
  ASSERT_TRUE(res_a.is_item());
  EXPECT_EQ(res_a.item(), internal::DataItem(42));
}

TEST(CppFunctionBridgeTest, FunctorNotSerializable) {
  ASSERT_OK_AND_ASSIGN(
      DataSlice functor,
      CreateFunctorFromFunction(
          [](const DataSlice& obj, const DataSlice& val,
             const DataBagPtr& db) -> absl::StatusOr<DataSlice> {
            auto res_obj = obj.WithBag(db);
            RETURN_IF_ERROR(res_obj.SetAttr("a", val));
            return res_obj;
          },
          "my_functor", "obj, val, db"));
  EXPECT_THAT(
      arolla::serialization::Encode({arolla::TypedValue::FromValue(functor)},
                                    {}),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("does not support serialization of EXPR_OPERATOR: "
                         "<Operator with name='my_functor'")));
}

TEST(CppFunctionBridgeTest, IntegrationTestWithParallelCall) {
  ASSERT_OK_AND_ASSIGN(DataSlice functor,
                       CreateFunctorFromFunction(
                           [](const DataSlice& x) -> DataSlice { return x; },
                           "my_functor", "x"));
  auto functor_expr = arolla::expr::Literal(functor);
  auto functor_future_expr = arolla::expr::CallOp(
      "koda_internal.parallel.as_future", {std::move(functor_expr)});
  auto executor_expr =
      arolla::expr::CallOp("koda_internal.parallel.get_eager_executor", {});
  auto execution_config_expr = arolla::expr::CallOp(
      "koda_internal.parallel.get_default_execution_config", {});
  auto execution_context_expr = arolla::expr::CallOp(
      "koda_internal.parallel.create_execution_context",
      {std::move(executor_expr), std::move(execution_config_expr)});
  auto unspecified_expr = arolla::expr::Literal(arolla::GetUnspecifiedQValue());
  auto input_value_expr = arolla::expr::Literal(DataSlice::CreateFromScalar(1));
  auto args_expr = arolla::expr::CallOp(
      "kd.tuple", {arolla::expr::CallOp("koda_internal.parallel.as_future",
                                        {std::move(input_value_expr)})});
  auto kwargs_expr = arolla::expr::Literal(arolla::MakeEmptyNamedTuple());
  auto non_deterministic_token_expr =
      arolla::expr::Literal(internal::NonDeterministicToken());
  ASSERT_OK_AND_ASSIGN(
      auto call_expr,
      arolla::expr::CallOp(
          "koda_internal.parallel.parallel_call",
          {std::move(execution_context_expr), std::move(functor_future_expr),
           std::move(args_expr), /*return_type_as=*/unspecified_expr,
           std::move(kwargs_expr), non_deterministic_token_expr}));
  ASSERT_OK_AND_ASSIGN(auto res,
                       expr::EvalExprWithCompilationCache(call_expr, {}, {}));
  ASSERT_OK_AND_ASSIGN(parallel::FuturePtr res_future,
                       res.As<parallel::FuturePtr>());
  EXPECT_THAT(res_future->GetValueForTesting(),
              IsOkAndHolds(QValueWith<DataSlice>(
                  IsEquivalentTo(DataSlice::CreateFromScalar(1)))));
}

}  // namespace
}  // namespace koladata::functor
