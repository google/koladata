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
#include "koladata/operators/arolla_bridge.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/const_init.h"
#include "absl/base/no_destructor.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

constexpr size_t kCompilationCacheSize = 4096;

class EvalCompiler {
  using Compiler = ::arolla::ExprCompiler<absl::Span<const ::arolla::TypedRef>,
                                          arolla::TypedValue>;
  using Impl =
      arolla::LruCache<compiler_internal::Key, compiler_internal::CompiledOp,
                       compiler_internal::KeyHash, compiler_internal::KeyEq>;

 public:
  static compiler_internal::CompiledOp Lookup(
      absl::string_view op_name, absl::Span<const arolla::TypedRef> inputs) {
    // Copying std::function is expensive, but given that we use cache, we
    // cannot use std::move.
    //
    // NOTE: If copying a function is more expensive than copying
    // a `std::shared_ptr`, we could use `std::shared_ptr<std::function<>>`.
    // Alternatively, we could consider having a thread-local cache that
    // requires no mutex and no function copying.
    absl::MutexLock lock(&mutex_);
    if (auto* hit =
            cache_->LookupOrNull(compiler_internal::LookupKey{op_name, inputs});
        hit != nullptr) {
      return *hit;
    }
    return {};
  }

  static absl::StatusOr<compiler_internal::CompiledOp> Compile(
      absl::string_view op_name, absl::Span<const arolla::TypedRef> inputs) {
    compiler_internal::CompiledOp fn = Lookup(op_name, inputs);
    if (ABSL_PREDICT_TRUE(fn)) {
      return fn;
    }
    std::vector<arolla::QTypePtr> input_types(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
      input_types[i] = inputs[i].GetType();
    }
    auto expr_op = std::make_shared<arolla::expr::RegisteredOperator>(op_name);
    ASSIGN_OR_RETURN(
        fn, Compiler()
                // Most of the operators are compiled into rather small
                // instruction sequences and don't contain many literals. In
                // such cases the always clone thread safety policy is faster.
                .SetAlwaysCloneThreadSafetyPolicy()
                .CompileOperator(expr_op, input_types));
    absl::MutexLock lock(&mutex_);
    return *cache_->Put(
        compiler_internal::Key{std::string(op_name), std::move(input_types)},
        std::move(fn));
  }

  static void Clear() {
    absl::MutexLock lock(&mutex_);
    cache_->Clear();
  }

 private:
  static absl::NoDestructor<Impl> cache_ ABSL_GUARDED_BY(mutex_);
  static absl::Mutex mutex_;
};

absl::NoDestructor<EvalCompiler::Impl> EvalCompiler::cache_(
    kCompilationCacheSize);

absl::Mutex EvalCompiler::mutex_{absl::kConstInit};

std::string GetQTypeName(arolla::QTypePtr qtype) {
  return schema::DType::VerifyQTypeSupported(qtype)
             ? absl::StrCat(schema::DType::FromQType(qtype)->name())
             : absl::StrCat(qtype->name());
}

absl::Status VerifyCompatibleSchema(
    const DataSlice& slice, absl::Span<const schema::DType> allowed_dtypes) {
  const auto& schema_item = slice.GetSchemaImpl();
  for (const auto& allowed_dtype : allowed_dtypes) {
    if (schema_item == allowed_dtype) {
      return absl::OkStatus();
    }
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported schema: ", schema_item.DebugString()));
}

absl::Status VerifyRank(const DataSlice& slice, size_t rank) {
  if (slice.GetShape().rank() != rank) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected rank %d, but got rank=%d", rank, slice.GetShape().rank()));
  }
  return absl::OkStatus();
}

internal::DataSliceImpl GetSliceImpl(const DataSlice& slice) {
  return slice.GetShape().rank()
             ? slice.slice()
             : internal::DataSliceImpl::Create({slice.item()});
}

absl::StatusOr<DataSlice> SimpleAggEval(absl::string_view op_name,
                                        std::vector<DataSlice> inputs,
                                        internal::DataItem output_schema,
                                        int edge_arg_index, bool is_agg_into) {
  DCHECK_GE(inputs.size(), 1);
  DCHECK_GE(edge_arg_index, 0);
  DCHECK_LE(edge_arg_index, inputs.size());
  schema::CommonSchemaAggregator schema_agg;
  internal::DataItem first_primitive_schema;
  for (const auto& x : inputs) {
    schema_agg.Add(x.GetSchemaImpl());
    // Validate that it has a primitive schema. We additionally use it as a
    // fallback schema for empty-and-unknown inputs.
    ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
    if (!first_primitive_schema.has_value()) {
      first_primitive_schema = std::move(primitive_schema);
    }
  }
  if (!output_schema.has_value()) {
    ASSIGN_OR_RETURN(output_schema, std::move(schema_agg).Get());
  }
  ASSIGN_OR_RETURN((auto [aligned_ds, aligned_shape]),
                   shape::AlignNonScalars(std::move(inputs)));
  if (aligned_shape.rank() == 0) {
    return absl::InvalidArgumentError("expected rank(x) > 0");
  }
  auto result_shape = is_agg_into
                          ? aligned_shape.RemoveDims(aligned_shape.rank() - 1)
                          : aligned_shape;
  // All empty-and-unknown inputs. We then skip evaluation and just broadcast
  // the first input to the common shape and common schema.
  if (!first_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto ds,
                     DataSlice::Create(internal::DataItem(), output_schema));
    return BroadcastToShape(ds, std::move(result_shape));
  }
  // From here on, we know that at least one input has known schema and we
  // should eval.
  std::vector<arolla::TypedValue> typed_value_holder;
  std::vector<arolla::TypedRef> typed_refs(
      aligned_ds.size() + 1, arolla::TypedRef::UnsafeFromRawPointer(
                                 arolla::GetNothingQType(), nullptr));
  typed_value_holder.reserve(aligned_ds.size());
  for (int i = 0; i < aligned_ds.size(); ++i) {
    int typed_ref_i = i < edge_arg_index ? i : i + 1;
    ASSIGN_OR_RETURN(
        typed_refs[typed_ref_i],
        DataSliceToOwnedArollaRef(aligned_ds[i], typed_value_holder,
                                  first_primitive_schema));
  }
  auto edge_tv = arolla::TypedValue::FromValue(aligned_shape.edges().back());
  typed_refs[edge_arg_index] = edge_tv.AsRef();
  ASSIGN_OR_RETURN(auto result, EvalExpr(op_name, typed_refs));
  return DataSliceFromArollaValue(result.AsRef(), std::move(result_shape),
                                  std::move(output_schema));
}

}  // namespace

namespace compiler_internal {

CompiledOp Lookup(absl::string_view op_name,
                  absl::Span<const arolla::TypedRef> inputs) {
  return EvalCompiler::Lookup(op_name, inputs);
}

void ClearCache() { return EvalCompiler::Clear(); }

}  // namespace compiler_internal

absl::StatusOr<arolla::TypedValue> EvalExpr(
    absl::string_view op_name, absl::Span<const arolla::TypedRef> inputs) {
  ASSIGN_OR_RETURN(auto fn, EvalCompiler::Compile(op_name, inputs));
  return fn(inputs);
}

absl::StatusOr<internal::DataItem> GetPrimitiveArollaSchema(
    const DataSlice& x) {
  const auto& schema = x.GetSchemaImpl();
  if (schema.is_primitive_schema()) {
    return schema;
  }
  if (schema.is_entity_schema()) {
    return absl::InvalidArgumentError(
        "DataSlice with Entity schema is not supported");
  }
  if (x.impl_empty_and_unknown()) {
    return internal::DataItem();
  }
  if (schema::DType::VerifyQTypeSupported(x.dtype())) {
    return internal::DataItem(*schema::DType::FromQType(x.dtype()));
  }
  if (x.impl_has_mixed_dtype()) {
    return absl::InvalidArgumentError(
        "DataSlice with mixed types is not supported");
  }
  return absl::InvalidArgumentError(
      absl::StrCat("DataSlice has no primitive schema: ", arolla::Repr(x)));
}

absl::StatusOr<DataSlice> SimplePointwiseEval(
    absl::string_view op_name, std::vector<DataSlice> inputs,
    internal::DataItem output_schema) {
  DCHECK_GE(inputs.size(), 1);
  schema::CommonSchemaAggregator schema_agg;
  internal::DataItem first_primitive_schema;
  for (const auto& x : inputs) {
    schema_agg.Add(x.GetSchemaImpl());
    // Validate that it has a primitive schema. We additionally use it as a
    // fallback schema for empty-and-unknown inputs.
    ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
    if (!first_primitive_schema.has_value()) {
      first_primitive_schema = std::move(primitive_schema);
    }
  }
  // All empty-and-unknown inputs. We then skip evaluation and just broadcast
  // the first input to the common shape and common schema.
  if (!first_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto common_shape, shape::GetCommonShape(inputs));
    if (!output_schema.has_value()) {
      ASSIGN_OR_RETURN(output_schema, std::move(schema_agg).Get());
    }
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(internal::DataItem(),
                                                std::move(output_schema)));
    return BroadcastToShape(ds, std::move(common_shape));
  }
  // From here on, we know that at least one input has known schema and we
  // should eval.
  ASSIGN_OR_RETURN((auto [aligned_ds, aligned_shape]),
                   shape::AlignNonScalars(std::move(inputs)));
  std::vector<arolla::TypedValue> typed_value_holder;
  std::vector<arolla::TypedRef> typed_refs;
  typed_value_holder.reserve(aligned_ds.size());
  typed_refs.reserve(aligned_ds.size());
  for (const auto& x : aligned_ds) {
    ASSIGN_OR_RETURN(auto ref,
                     DataSliceToOwnedArollaRef(x, typed_value_holder,
                                               first_primitive_schema));
    typed_refs.push_back(std::move(ref));
  }
  ASSIGN_OR_RETURN(auto result, EvalExpr(op_name, typed_refs));
  if (output_schema.has_value()) {
    return DataSliceFromArollaValue(result.AsRef(), std::move(aligned_shape),
                                    std::move(output_schema));
  }
  // Get the common schema from both inputs and outputs.
  ASSIGN_OR_RETURN(auto output_qtype, arolla::GetScalarQType(result.GetType()));
  ASSIGN_OR_RETURN(auto result_dtype, schema::DType::FromQType(output_qtype));
  schema_agg.Add(result_dtype);
  ASSIGN_OR_RETURN(output_schema, std::move(schema_agg).Get());
  return DataSliceFromArollaValue(result.AsRef(), std::move(aligned_shape),
                                  std::move(output_schema));
}

absl::StatusOr<DataSlice> SimpleAggIntoEval(absl::string_view op_name,
                                            std::vector<DataSlice> inputs,
                                            internal::DataItem output_schema,
                                            int edge_arg_index) {
  return SimpleAggEval(op_name, std::move(inputs), std::move(output_schema),
                       edge_arg_index,
                       /*is_agg_into=*/true);
}

absl::StatusOr<DataSlice> SimpleAggOverEval(absl::string_view op_name,
                                            std::vector<DataSlice> inputs,
                                            internal::DataItem output_schema,
                                            int edge_arg_index) {
  return SimpleAggEval(op_name, std::move(inputs), std::move(output_schema),
                       edge_arg_index,
                       /*is_agg_into=*/false);
}

absl::StatusOr<bool> ToArollaBoolean(const DataSlice& x) {
  RETURN_IF_ERROR(VerifyRank(x, 0));
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kBool, schema::kAny, schema::kObject}));
  if (!x.item().has_value()) {
    return absl::InvalidArgumentError("expected a present value");
  }
  if (x.dtype() == arolla::GetQType<bool>()) {
    return x.item().value<bool>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

absl::StatusOr<int64_t> ToArollaInt64(const DataSlice& x) {
  RETURN_IF_ERROR(VerifyRank(x, 0));
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kInt32, schema::kInt64, schema::kAny, schema::kObject}));
  if (!x.item().has_value()) {
    return absl::InvalidArgumentError("expected a present value");
  }
  if (x.dtype() == arolla::GetQType<int32_t>()) {
    return static_cast<int64_t>(x.item().value<int32_t>());
  }
  if (x.dtype() == arolla::GetQType<int64_t>()) {
    return x.item().value<int64_t>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

absl::StatusOr<double> ToArollaFloat64(const DataSlice& x) {
  RETURN_IF_ERROR(VerifyRank(x, 0));
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kFloat32, schema::kFloat64, schema::kAny, schema::kObject}));
  if (!x.item().has_value()) {
    return absl::InvalidArgumentError("expected a present value");
  }
  if (x.dtype() == arolla::GetQType<float>()) {
    return static_cast<double>(x.item().value<float>());
  }
  if (x.dtype() == arolla::GetQType<double>()) {
    return x.item().value<double>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

absl::StatusOr<arolla::DenseArray<int64_t>> ToArollaDenseArrayInt64(
    const DataSlice& x) {
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kInt32, schema::kInt64, schema::kAny, schema::kObject}));
  auto slice_impl = GetSliceImpl(x);
  if (slice_impl.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<int64_t>(x.GetShape().size());
  }
  if (slice_impl.is_mixed_dtype()) {
    return absl::InvalidArgumentError("mixed slices are not supported");
  }
  if (x.dtype() == arolla::GetQType<int32_t>()) {
    return arolla::CreateDenseOp([](int32_t v) -> int64_t { return v; })(
        slice_impl.values<int32_t>());
  }
  if (x.dtype() == arolla::GetQType<int64_t>()) {
    return slice_impl.values<int64_t>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

absl::StatusOr<arolla::DenseArray<arolla::Unit>> ToArollaDenseArrayUnit(
    const DataSlice& x) {
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kMask, schema::kAny, schema::kObject}));
  auto slice_impl = GetSliceImpl(x);
  if (slice_impl.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<arolla::Unit>(x.GetShape().size());
  }
  if (slice_impl.is_mixed_dtype()) {
    return absl::InvalidArgumentError("mixed slices are not supported");
  }
  if (x.dtype() == arolla::GetQType<arolla::Unit>()) {
    return slice_impl.values<arolla::Unit>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

absl::StatusOr<arolla::DenseArray<arolla::Text>> ToArollaDenseArrayText(
    const DataSlice& x) {
  RETURN_IF_ERROR(VerifyCompatibleSchema(
      x, {schema::kText, schema::kAny, schema::kObject}));
  auto slice_impl = GetSliceImpl(x);
  if (slice_impl.is_empty_and_unknown()) {
    return arolla::CreateEmptyDenseArray<arolla::Text>(x.GetShape().size());
  }
  if (slice_impl.is_mixed_dtype()) {
    return absl::InvalidArgumentError("mixed slices are not supported");
  }
  if (x.dtype() == arolla::GetQType<arolla::Text>()) {
    return slice_impl.values<arolla::Text>();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported dtype: ", GetQTypeName(x.dtype())));
}

}  // namespace koladata::ops
