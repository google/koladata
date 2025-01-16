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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/const_init.h"
#include "absl/base/no_destructor.h"
#include "absl/base/optimization.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/schema_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/lru_cache.h"
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
                // TODO: b/374841918 - Provide stack trace information in a
                // structured way instead of disabling it.
                .VerboseRuntimeErrors(false)
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

absl::InlinedVector<bool, 16> GetPrimaryOperandMask(
    size_t input_size,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  absl::InlinedVector<bool, 16> is_primary_operand(
      input_size, !primary_operand_indices.has_value());
  if (primary_operand_indices.has_value()) {
    for (int index : *primary_operand_indices) {
      DCHECK(index >= 0 && index < input_size);
      is_primary_operand[index] = true;
    }
  }
  return is_primary_operand;
}

struct PrimaryOperandSchemaInfo {
  internal::DataItem first_primitive_schema;
  internal::DataItem common_schema;
};

absl::StatusOr<PrimaryOperandSchemaInfo> GetPrimaryOperandSchemaInfo(
    absl::Span<const DataSlice> inputs,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  auto is_primary_operand =
      GetPrimaryOperandMask(inputs.size(), primary_operand_indices);
  schema::CommonSchemaAggregator schema_agg;
  internal::DataItem first_primitive_schema;
  for (int i = 0; i < inputs.size(); ++i) {
    const DataSlice& x = inputs[i];
    if (!is_primary_operand[i]) {
      internal::DataItem narrowed_schema = GetNarrowedSchema(x);
      if (!narrowed_schema.is_primitive_schema()) {
        return absl::InternalError(
            absl::StrCat("DataSlice for the non-primary operand ", i + 1,
                         " should have a primitive schema"));
      }
      continue;
    }
    schema_agg.Add(x.GetSchemaImpl());
    // Validate that it has a primitive schema. We additionally use it as a
    // fallback schema for empty-and-unknown inputs.
    ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
    if (!first_primitive_schema.has_value()) {
      first_primitive_schema = std::move(primitive_schema);
    }
  }
  ASSIGN_OR_RETURN(auto common_schema, std::move(schema_agg).Get());
  return {{.first_primitive_schema = std::move(first_primitive_schema),
           .common_schema = std::move(common_schema)}};
}

absl::StatusOr<internal::DataItem> GetResultSchema(
    const arolla::TypedValue& result) {
  ASSIGN_OR_RETURN(auto output_qtype, arolla::GetScalarQType(result.GetType()));
  ASSIGN_OR_RETURN(auto result_dtype, schema::DType::FromQType(output_qtype));
  return internal::DataItem(result_dtype);
}

absl::StatusOr<DataSlice> SimpleAggEval(
    absl::string_view op_name, std::vector<DataSlice> inputs,
    internal::DataItem output_schema, int edge_arg_index, bool is_agg_into,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  DCHECK_GE(inputs.size(), 1);
  DCHECK_GE(edge_arg_index, 0);
  DCHECK_LE(edge_arg_index, inputs.size());
  ASSIGN_OR_RETURN(
      auto primary_operand_schema_info,
      GetPrimaryOperandSchemaInfo(inputs, primary_operand_indices),
      internal::OperatorEvalError(std::move(_), op_name, "invalid inputs"));
  ASSIGN_OR_RETURN(
      (auto [aligned_ds, aligned_shape]),
      shape::AlignNonScalars(std::move(inputs)),
      internal::OperatorEvalError(std::move(_), op_name,
                                  "cannot align all inputs to a common shape"));
  if (aligned_shape.rank() == 0) {
    return internal::OperatorEvalError(op_name, "expected rank(x) > 0");
  }
  auto result_shape = is_agg_into
                          ? aligned_shape.RemoveDims(aligned_shape.rank() - 1)
                          : aligned_shape;
  // All primary inputs are empty-and-unknown. We then skip evaluation and just
  // broadcast the first input to the common shape and common schema. It is the
  // caller's responsibility to make sure that the non-primary inputs have
  // acceptable schemas.
  if (!primary_operand_schema_info.first_primitive_schema.has_value()) {
    if (!output_schema.has_value()) {
      output_schema = std::move(primary_operand_schema_info.common_schema);
    }
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
    ASSIGN_OR_RETURN(typed_refs[typed_ref_i],
                     DataSliceToOwnedArollaRef(
                         aligned_ds[i], typed_value_holder,
                         primary_operand_schema_info.first_primitive_schema));
  }
  auto edge_tv = arolla::TypedValue::FromValue(aligned_shape.edges().back());
  typed_refs[edge_arg_index] = edge_tv.AsRef();
  ASSIGN_OR_RETURN(
      auto result, EvalExpr(op_name, typed_refs),
      internal::OperatorEvalError(
          std::move(_), op_name,
          "successfully converted input DataSlice(s) to DenseArray(s) but "
          "failed to evaluate the Arolla operator"));
  if (!output_schema.has_value()) {
    // Get the common schema from the primary inputs and output.
    ASSIGN_OR_RETURN(auto result_schema, GetResultSchema(result));
    ASSIGN_OR_RETURN(
        output_schema,
        schema::CommonSchema(primary_operand_schema_info.common_schema,
                             result_schema));
  }
  return DataSliceFromArollaValue(result.AsRef(), std::move(result_shape),
                                  std::move(output_schema));
}

}  // namespace

namespace compiler_internal {

CompiledOp Lookup(absl::string_view op_name,
                  absl::Span<const arolla::TypedRef> inputs) {
  return EvalCompiler::Lookup(op_name, inputs);
}

}  // namespace compiler_internal

absl::StatusOr<arolla::TypedValue> EvalExpr(
    absl::string_view op_name, absl::Span<const arolla::TypedRef> inputs) {
  ASSIGN_OR_RETURN(auto fn, EvalCompiler::Compile(op_name, inputs));
  return fn(inputs);
}

void ClearCompilationCache() { return EvalCompiler::Clear(); }

absl::StatusOr<internal::DataItem> GetPrimitiveArollaSchema(
    const DataSlice& x) {
  auto create_error = [&](absl::string_view message) {
    return absl::InvalidArgumentError(
        absl::StrCat(message, ": ", DataSliceRepr(x)));
  };

  const auto& schema = x.GetSchemaImpl();
  if (schema.is_primitive_schema()) {
    return schema;
  }
  if (schema.is_struct_schema()) {
    return create_error("DataSlice with Struct schema is not supported");
  }
  if (x.impl_empty_and_unknown()) {
    return internal::DataItem();
  }
  if (schema::DType::VerifyQTypeSupported(x.dtype())) {
    return internal::DataItem(*schema::DType::FromQType(x.dtype()));
  }
  if (x.impl_has_mixed_dtype()) {
    return create_error("DataSlice with mixed types is not supported");
  }
  return create_error("DataSlice has no primitive schema");
}

absl::StatusOr<DataSlice> SimplePointwiseEval(
    absl::string_view op_name, std::vector<DataSlice> inputs,
    internal::DataItem output_schema,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  DCHECK_GE(inputs.size(), 1);
  ASSIGN_OR_RETURN(auto primary_operand_schema_info,
                   GetPrimaryOperandSchemaInfo(inputs, primary_operand_indices),
                   internal::AsKodaError(std::move(_)));
  // All primary inputs are empty-and-unknown. We then skip evaluation and just
  // broadcast the first input to the common shape and common schema. It is the
  // caller's responsibility to make sure that the non-primary inputs have
  // acceptable schemas.
  if (!primary_operand_schema_info.first_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(
        auto common_shape, shape::GetCommonShape(inputs),
        internal::KodaErrorFromCause("cannot align inputs to a common shape",
                                     std::move(_)));
    if (!output_schema.has_value()) {
      output_schema = std::move(primary_operand_schema_info.common_schema);
    }
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(internal::DataItem(),
                                                std::move(output_schema)));
    return BroadcastToShape(ds, std::move(common_shape));
  }
  // From here on, we know that at least one primary input has known schema and
  // we should eval.
  ASSIGN_OR_RETURN((auto [aligned_ds, aligned_shape]),
                   shape::AlignNonScalars(std::move(inputs)),
                   internal::KodaErrorFromCause(
                       "cannot align inputs to a common shape", std::move(_)));
  std::vector<arolla::TypedValue> typed_value_holder;
  std::vector<arolla::TypedRef> typed_refs;
  typed_value_holder.reserve(aligned_ds.size());
  typed_refs.reserve(aligned_ds.size());
  for (int i = 0; i < aligned_ds.size(); ++i) {
    const auto& x = aligned_ds[i];
    // For non-primary operands, the fallback schema won't be used, because
    // they are guaranteed to have a primitive schema.
    ASSIGN_OR_RETURN(auto ref,
                     DataSliceToOwnedArollaRef(
                         x, typed_value_holder,
                         primary_operand_schema_info.first_primitive_schema));
    typed_refs.push_back(std::move(ref));
  }
  ASSIGN_OR_RETURN(auto result, EvalExpr(op_name, typed_refs),
                   internal::AsKodaError(std::move(_)));
  if (!output_schema.has_value()) {
    // Get the common schema from the primary inputs and output.
    ASSIGN_OR_RETURN(auto result_schema, GetResultSchema(result));
    ASSIGN_OR_RETURN(
        output_schema,
        schema::CommonSchema(primary_operand_schema_info.common_schema,
                             result_schema));
  }
  return DataSliceFromArollaValue(result.AsRef(), std::move(aligned_shape),
                                  std::move(output_schema));
}

absl::StatusOr<DataSlice> SimpleAggIntoEval(
    absl::string_view op_name, std::vector<DataSlice> inputs,
    internal::DataItem output_schema, int edge_arg_index,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  return SimpleAggEval(op_name, std::move(inputs), std::move(output_schema),
                       edge_arg_index,
                       /*is_agg_into=*/true, primary_operand_indices);
}

absl::StatusOr<DataSlice> SimpleAggOverEval(
    absl::string_view op_name, std::vector<DataSlice> inputs,
    internal::DataItem output_schema, int edge_arg_index,
    const std::optional<absl::Span<const int>>& primary_operand_indices) {
  return SimpleAggEval(op_name, std::move(inputs), std::move(output_schema),
                       edge_arg_index,
                       /*is_agg_into=*/false, primary_operand_indices);
}

}  // namespace koladata::ops
