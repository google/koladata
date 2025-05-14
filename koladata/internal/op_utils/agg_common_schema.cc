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
#include "koladata/internal/op_utils/agg_common_schema.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

using ::arolla::DenseArrayEdge;
using ::arolla::DenseGroupOps;
using ::arolla::JaggedDenseArrayShape;

class AggCommonSchemaAggregator
    : public arolla::Accumulator<arolla::AccumulatorType::kAggregator, DataItem,
                                 arolla::meta::type_list<>,
                                 arolla::meta::type_list<DataItem>> {
 public:
  void Reset() final {
    aggregator_ = schema::CommonSchemaAggregator();
  }

  void Add(DataItem item) final {
    aggregator_.Add(item);
  }

  DataItem GetResult() final {
    // Note that this leaves `aggregator_` in an unspecified but valid state.
    // `Reset()` is guaranteed to be called before the next call to Add or
    // GetResult, making this safe.
    auto common_schema = std::move(aggregator_).Get();
    if (common_schema.ok()) {
      return *std::move(common_schema);
    }
    status_ = std::move(common_schema).status();
    return DataItem();
  }

  absl::Status GetStatus() final { return status_; }

 private:
  schema::CommonSchemaAggregator aggregator_;
  absl::Status status_ = absl::OkStatus();
};

}  // namespace

absl::StatusOr<DataSliceImpl> AggCommonSchemaOp(const DataSliceImpl& ds,
                                                const DenseArrayEdge& edge) {
  auto array = ds.AsDataItemDenseArray();
  DenseGroupOps<AggCommonSchemaAggregator> agg(arolla::GetHeapBufferFactory());
  ASSIGN_OR_RETURN(auto result, agg.Apply(edge, array));
  return DataSliceImpl::Create(std::move(result));
}

}  // namespace koladata::internal
