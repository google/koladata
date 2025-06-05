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
#include "koladata/internal/op_utils/object_finder.h"

#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

constexpr float NaN = std::numeric_limits<float>::quiet_NaN();

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

class ObjectFinderTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ObjectFinderTest,
                         ::testing::ValuesIn(test_param_values));

using PathItem = std::tuple<std::string, DataItem, DataItem>;

class TestFinder {
 public:
  explicit TestFinder(
      std::vector<PathItem> expected_paths,
      absl::FunctionRef<absl::StatusOr<bool>(const DataItem&, const DataItem&)>
          need_access_path_fn)
      : expected_paths_(expected_paths),
        count_(expected_paths.size()),
        paths_ids_(),
        need_access_path_fn_(need_access_path_fn) {
    for (int64_t i = 0; i < expected_paths.size(); ++i) {
      paths_ids_[std::get<0>(expected_paths_[i])] = i;
    }
  }

  absl::Status ObjectPathCallback(const DataItem& item, const DataItem& schema,
                                  absl::FunctionRef<std::string()> path) {
    ASSIGN_OR_RETURN(bool need_access_path, need_access_path_fn_(item, schema));
    if (need_access_path) {
      return AccessPath(item, schema, path());
    }
    return absl::OkStatus();
  }

  absl::Status AccessPath(const DataItem& item, const DataItem& schema,
                          std::string_view path) {
    auto it = paths_ids_.find(path);
    if (it == paths_ids_.end()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unexpected path: %s", path));
    }
    int64_t index = it->second;
    auto [expected_path, expected_item, expected_schema] =
        expected_paths_[index];
    if (!item.is_nan() || !expected_item.is_nan()) {
      EXPECT_EQ(item, expected_item);
    }
    EXPECT_EQ(schema, expected_schema);
    ++count_[index];
    if (count_[index] > 1) {
      return absl::InvalidArgumentError(
          absl::StrFormat("path %s is accessed more than once", path));
    }
    return absl::OkStatus();
  }

 private:
  std::vector<PathItem> expected_paths_;
  std::vector<int> count_;
  absl::flat_hash_map<std::string, int64_t> paths_ids_;
  absl::FunctionRef<absl::StatusOr<bool>(const DataItem&, const DataItem&)>
      need_access_path_fn_;
};

TEST_P(ObjectFinderTest, EntitySlicePrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema = DataItem(AllocateExplicitSchema());
  auto int_dtype = DataItem(schema::kInt32);
  TriplesT data_triples = {
    {a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}}, {a1, {{"x", DataItem(2)}}},
    {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  TriplesT schema_triples = {
      {schema,
       {{"x", int_dtype}, {"y", int_dtype}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto finder = std::make_shared<TestFinder>(
      std::vector<PathItem>({
          {".S[0].x", DataItem(1), int_dtype},
          {".S[1].x", DataItem(2), int_dtype},
          {".S[2].x", DataItem(3), int_dtype},
          {".S[0].y", DataItem(4), int_dtype},
          {".S[1].y", DataItem(), int_dtype},
          {".S[2].y", DataItem(6), int_dtype},
      }),
      [](const DataItem& item, const DataItem& schema) {
        return schema.is_primitive_schema();
      });
  auto object_finder = ObjectFinder(*db, {});
  ASSERT_OK(object_finder.TraverseSlice(
      obj_ids, schema,
      [&](const DataItem& item, const DataItem& schema,
          absl::FunctionRef<std::string()> path) {
        return finder->ObjectPathCallback(item, schema, path);
      }));
}

TEST_P(ObjectFinderTest, EntitySlicePrimitivesNan) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema = DataItem(AllocateExplicitSchema());
  auto float_dtype = DataItem(schema::kFloat32);
  TriplesT data_triples = {
      {a0, {{"x", DataItem(static_cast<float>(1))}, {"y", DataItem(NaN)}}},
      {a1, {{"x", DataItem(static_cast<float>(2))}}},
      {a2, {{"x", DataItem(NaN)}, {"y", DataItem(static_cast<float>(6))}}}};
  TriplesT schema_triples = {
      {schema,
       {{"x", float_dtype}, {"y", float_dtype}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto finder = std::make_shared<TestFinder>(
      std::vector<PathItem>({
          {".S[0].x", DataItem(static_cast<float>(1)), float_dtype},
          {".S[1].x", DataItem(static_cast<float>(2)), float_dtype},
          {".S[2].x", DataItem(NaN), float_dtype},
          {".S[0].y", DataItem(NaN), float_dtype},
          {".S[1].y", DataItem(), float_dtype},
          {".S[2].y", DataItem(static_cast<float>(6)), float_dtype},
      }),
      [](const DataItem& item, const DataItem& schema) {
        return schema.is_primitive_schema();
      });
  auto object_finder = ObjectFinder(*db, {});
  ASSERT_OK(object_finder.TraverseSlice(
      obj_ids, schema,
      [&](const DataItem& item, const DataItem& schema,
          absl::FunctionRef<std::string()> path) {
        return finder->ObjectPathCallback(item, schema, path);
      }));
}

TEST_P(ObjectFinderTest, ObjectSliceEntities) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b_obj_ids = AllocateEmptyObjects(3);
  auto b0 = b_obj_ids[0];
  auto b1 = b_obj_ids[1];
  auto b2 = b_obj_ids[2];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  auto object_schema = DataItem(schema::kObject);
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema_a}, {"self", a0}, {"b", b0}}},
      {a1, {{schema::kSchemaAttr, schema_a}, {"self", DataItem()}, {"b", b1}}},
      {a2, {{schema::kSchemaAttr, schema_a}, {"self", a2}, {"b", b2}}},
      {b0, {{schema::kSchemaAttr, schema_b}, {"self", b0}}},
      {b1, {{schema::kSchemaAttr, schema_b}, {"self", b1}}},
      {b2, {{schema::kSchemaAttr, schema_b}, {"self", b2}}}};
  TriplesT schema_triples = {
      {schema_a, {{"self", object_schema}, {"b", object_schema}}},
      {schema_b, {{"self", object_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto finder = std::make_shared<TestFinder>(
      std::vector<PathItem>({
          {".S[0]", a0, schema_a},
          {".S[1]", a1, schema_a},
          {".S[2]", a2, schema_a},
          {".S[0].b", b0, schema_b},
          {".S[1].b", b1, schema_b},
          {".S[2].b", b2, schema_b},
      }),
      [](const DataItem& item, const DataItem& schema) {
        return schema.is_struct_schema();
      });
  auto object_finder = ObjectFinder(*db, {});
  ASSERT_OK(object_finder.TraverseSlice(
      obj_ids, object_schema,
      [&](const DataItem& item, const DataItem& schema,
          absl::FunctionRef<std::string()> path) {
        return finder->ObjectPathCallback(item, schema, path);
      }));
}

}  // namespace
}  // namespace koladata::internal
