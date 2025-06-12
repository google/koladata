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
#include "koladata/internal/op_utils/agg_uuid.h"

#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/stable_fingerprint.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

using ::arolla::DenseGroupOps;
using ::arolla::DenseArrayEdge;
using ::arolla::JaggedDenseArrayShape;

class AggUuidAggregator
    : public arolla::Accumulator<
          arolla::AccumulatorType::kAggregator, arolla::OptionalValue<ObjectId>,
          arolla::meta::type_list<>, arolla::meta::type_list<DataItem>> {
 public:
  AggUuidAggregator() : hasher_(StableFingerprintHasher("uuid")), pos_(0) {};

  void Reset() final {
    hasher_ = StableFingerprintHasher("uuid");
    pos_ = 0;
  }

  void Add(DataItem item) final {
    hasher_.Combine(std::move(item).StableFingerprint());
    hasher_.Combine(pos_++);
  }

  arolla::OptionalValue<ObjectId> GetResult() final {
    return CreateUuidObject(std::move(hasher_).Finish());
  }

 private:
  StableFingerprintHasher hasher_;
  int64_t pos_;
};

}  // namespace

absl::StatusOr<DataSliceImpl> AggUuidOp(
    const DataSliceImpl& ds, const JaggedDenseArrayShape& shape) {
  auto array = ds.AsDataItemDenseArray();
  const absl::Span<const DenseArrayEdge> edges = shape.edges();
  auto last_edge = edges.back();
  DenseGroupOps<AggUuidAggregator> agg(arolla::GetHeapBufferFactory());
  ASSIGN_OR_RETURN(auto result, agg.Apply(last_edge, array));
  return DataSliceImpl::Create(std::move(result));
}

}  // namespace koladata::internal
