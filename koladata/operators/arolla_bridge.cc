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
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/eval/verbose_runtime_error.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/quote.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/io/typed_refs_input_loader.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/status.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "koladata/shape_utils.h"
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
    absl::MutexLock lock(mutex_);
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
    absl::MutexLock lock(mutex_);
    return *cache_->Put(
        compiler_internal::Key{std::string(op_name), std::move(input_types)},
        std::move(fn));
  }

  static void Clear() {
    absl::MutexLock lock(mutex_);
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
      GetPrimaryOperandSchemaInfo(inputs, primary_operand_indices));
  ASSIGN_OR_RETURN(
      (auto [aligned_ds, aligned_shape]),
      shape::AlignNonScalars(std::move(inputs)),
      internal::KodaErrorFromCause("cannot align all inputs to a common shape",
                                   std::move(_)));
  if (aligned_shape.rank() == 0) {
    return absl::InvalidArgumentError("expected rank(x) > 0");
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
  ASSIGN_OR_RETURN(auto result, EvalExpr(op_name, typed_refs));
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

// Removes Arolla's clarification layers from the error, as it is usually too
// verbose and the error is already amended by the outer operators.
absl::Status SimplifyExprEvaluationError(absl::Status status) {
  const auto* cause = arolla::GetCause(status);
  while (cause != nullptr &&
         (arolla::GetPayload<arolla::NotePayload>(status) != nullptr ||
          arolla::GetPayload<arolla::expr::VerboseRuntimeError>(status) !=
              nullptr)) {
    status = *cause;
    cause = arolla::GetCause(status);
  }
  return status;
}

class ExprEvalCompiler {
  using InputTypes = std::vector<std::pair<std::string, arolla::QTypePtr>>;
  using Inputs = absl::Span<const arolla::TypedRef>;
  using ModelOr =
      absl::StatusOr<std::function<absl::StatusOr<DataSlice>(const Inputs&)>>;
  using Compiler = ::arolla::ExprCompiler<Inputs, DataSlice>;
  using Cache = arolla::LruCache<arolla::Fingerprint, ModelOr>;

 public:
  static ModelOr Compile(const arolla::expr::ExprNodePtr& expr,
                         InputTypes types) {
    arolla::FingerprintHasher hasher("ExprEvalCompiler");
    hasher.Combine(expr->fingerprint());
    hasher.Combine(types.size());
    for (const auto& [name, t] : types) {
      hasher.Combine(name, t);
    }
    arolla::Fingerprint key = std::move(hasher).Finish();
    {
      absl::MutexLock lock(mutex_);
      if (auto* hit = cache_->LookupOrNull(key); hit != nullptr) {
        return *hit;
      }
    }
    ModelOr model = Compiler()
                        .SetInputLoader(arolla::CreateTypedRefsInputLoader(
                            std::move(types)))
                        .SetPoolThreadSafetyPolicy()
                        .Compile(expr);
    absl::MutexLock lock(mutex_);
    return *cache_->Put(key, std::move(model));
  }

  static void Clear() {
    absl::MutexLock lock(mutex_);
    cache_->Clear();
  }

 private:
  static absl::NoDestructor<Cache> cache_ ABSL_GUARDED_BY(mutex_);
  static absl::Mutex mutex_;
};

absl::NoDestructor<ExprEvalCompiler::Cache> ExprEvalCompiler::cache_(
    kCompilationCacheSize);
absl::Mutex ExprEvalCompiler::mutex_{absl::kConstInit};

class ArollaExprEvalOperator : public arolla::QExprOperator {
 public:
  explicit ArollaExprEvalOperator(
      absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataSlice>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator(
        "kd.core._arolla_expr_eval",
        [expr_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         args_slot = input_slots[1],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& expr_slice = frame.Get(expr_slot);
          if (!expr_slice.is_item() ||
              !expr_slice.item().holds_value<arolla::expr::ExprQuote>()) {
            return absl::InvalidArgumentError(
                "first argument is expected to be a DataItem with ExprQuote");
          }
          ASSIGN_OR_RETURN(
              arolla::expr::ExprNodePtr expr,
              expr_slice.item().value<arolla::expr::ExprQuote>().expr());
          ASSIGN_OR_RETURN(expr,
                           arolla::expr::CallOp("koda_internal.to_data_slice",
                                                {std::move(expr)}));

          // Prepare arguments
          absl::Span<const std::string> arg_names =
              arolla::GetFieldNames(args_slot.GetType());
          std::vector<arolla::TypedValue> arg_values;
          std::vector<std::pair<std::string, arolla::QTypePtr>> types_in_order;
          arg_values.reserve(args_slot.SubSlotCount());
          types_in_order.reserve(args_slot.SubSlotCount());
          for (int i = 0; i < args_slot.SubSlotCount(); ++i) {
            ASSIGN_OR_RETURN(
                arolla::TypedValue tv,
                DataSliceToArollaValue(
                    frame.Get(args_slot.SubSlot(i).UnsafeToSlot<DataSlice>())));
            types_in_order.emplace_back(arg_names[i], tv.GetType());
            arg_values.push_back(std::move(tv));
          }
          std::vector<arolla::TypedRef> arg_refs;
          arg_refs.reserve(args_slot.SubSlotCount());
          for (const auto& tv : arg_values) {
            arg_refs.push_back(tv.AsRef());
          }

          ASSIGN_OR_RETURN(
              const auto& model,
              ExprEvalCompiler::Compile(expr, std::move(types_in_order)));
          ASSIGN_OR_RETURN(*frame.GetMutable(output_slot), model(arg_refs));
          return absl::OkStatus();
        });
  }
};

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
  ASSIGN_OR_RETURN(auto result, fn(inputs),
                   SimplifyExprEvaluationError(std::move(_)));
  return result;
}

void ClearCompilationCache() {
  EvalCompiler::Clear();
  ExprEvalCompiler::Clear();
}

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
  ASSIGN_OR_RETURN(
      auto primary_operand_schema_info,
      GetPrimaryOperandSchemaInfo(inputs, primary_operand_indices));
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
  ASSIGN_OR_RETURN(auto result, EvalExpr(op_name, typed_refs));
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

absl::StatusOr<arolla::OperatorPtr> ArollaExprEvalOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataItem with ExprQuote");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[1]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ArollaExprEvalOperator>(input_types), input_types,
      output_type);
}

}  // namespace koladata::ops
