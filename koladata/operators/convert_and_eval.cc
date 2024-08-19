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
#include "koladata/operators/convert_and_eval.h"

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
#include "arolla/expr/expr_operator.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

constexpr size_t kCompilationCacheSize = 4096;

using CompiledOp = std::function<absl::StatusOr<::arolla::TypedValue>(
    absl::Span<const arolla::TypedRef>)>;

using Compiler = ::arolla::ExprCompiler<absl::Span<const ::arolla::TypedRef>,
                                        arolla::TypedValue>;

class EvalCompiler {
 public:
  // TODO: Only allow operators to be passed by name, and place
  // ExprOperatorPtr construction inside of the cached scope.
  static absl::StatusOr<CompiledOp> Compile(
      const arolla::expr::ExprOperatorPtr& expr_op,
      absl::Span<const arolla::TypedRef> inputs) {
    // TODO: Instead of creating a fingerprint, we can use a tuple
    // as key.
    arolla::FingerprintHasher hasher("koladata.convert_and_eval");
    hasher.Combine(expr_op->fingerprint());
    for (const auto& input : inputs) {
      hasher.Combine(input.GetType());
    }
    arolla::Fingerprint key = std::move(hasher).Finish();
    CompiledOp fn;
    {
      // Copying std::function is expensive, but given that we use cache, we
      // cannot use std::move.
      //
      // NOTE: If copying a function is more expensive than copying
      // a `std::shared_ptr`, we could use `std::shared_ptr<std::function<>>`.
      // Alternatively, we could consider having a thread-local cache that
      // requires no mutex and no function copying.
      absl::MutexLock lock(&mutex_);
      if (auto* hit = cache_->LookupOrNull(key); hit != nullptr) {
        fn = *hit;
      }
    }
    if (ABSL_PREDICT_FALSE(!fn)) {
      std::vector<arolla::QTypePtr> input_types(inputs.size());
      for (size_t i = 0; i < inputs.size(); ++i) {
        input_types[i] = inputs[i].GetType();
      }
      ASSIGN_OR_RETURN(
          fn, Compiler()
                  // Most of the operators are compiled into rather small
                  // instruction sequences and don't contain many literals. In
                  // such cases the always clone thread safety policy is faster.
                  .SetAlwaysCloneThreadSafetyPolicy()
                  .CompileOperator(expr_op, input_types));
      absl::MutexLock lock(&mutex_);
      fn = *cache_->Put(key, std::move(fn));
    }
    return fn;
  }

 private:
  static absl::NoDestructor<arolla::LruCache<arolla::Fingerprint, CompiledOp>>
      cache_ ABSL_GUARDED_BY(mutex_);
  static absl::Mutex mutex_;
};

absl::NoDestructor<arolla::LruCache<arolla::Fingerprint, CompiledOp>>
    EvalCompiler::cache_(kCompilationCacheSize);

absl::Mutex EvalCompiler::mutex_{absl::kConstInit};

// Returns the slot indices that holds DataSlices.
std::vector<size_t> GetDataSliceSlotIndices(
    absl::Span<const arolla::TypedSlot> slots) {
  std::vector<size_t> result;
  result.reserve(slots.size());
  for (size_t i = 0; i < slots.size(); ++i) {
    if (slots[i].GetType() == arolla::GetQType<DataSlice>()) {
      result.emplace_back(i);
    }
  }
  return result;
}

template <typename ExecuteFn>
class ConvertAndEvalBaseOp : public arolla::QExprOperator {
 public:
  // Constructs ConvertAndEvalBaseOp with the provided execute_fn. extra_inputs
  // is used to reserve additional capacity to the TypedRef vector passed to the
  // executor.
  ConvertAndEvalBaseOp(std::string name,
                       absl::Span<const arolla::QTypePtr> types,
                       ExecuteFn execute_fn, size_t extra_inputs = 0)
      : arolla::QExprOperator(std::move(name),
                              arolla::QExprOperatorSignature::Get(
                                  types, arolla::GetQType<DataSlice>())),
        execute_fn_(std::move(execute_fn)),
        extra_inputs_(extra_inputs) {
    DCHECK(!types.empty());
    DCHECK(types[0] == arolla::GetQType<arolla::expr::ExprOperatorPtr>());
  }

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> expr_and_input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [expr_slot = expr_and_input_slots[0]
                         .UnsafeToSlot<arolla::expr::ExprOperatorPtr>(),
         ds_input_indices =
             GetDataSliceSlotIndices(expr_and_input_slots.subspan(1)),
         input_slots = std::vector(expr_and_input_slots.begin() + 1,
                                   expr_and_input_slots.end()),
         output_slot = output_slot.UnsafeToSlot<DataSlice>(),
         execute_fn = execute_fn_, extra_inputs = extra_inputs_](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& expr_op = frame.Get(expr_slot);
          std::vector<arolla::TypedRef> input_refs;
          input_refs.reserve(input_slots.size() + extra_inputs);
          for (const auto& slot : input_slots) {
            input_refs.push_back(arolla::TypedRef::FromSlot(slot, frame));
          }
          std::vector<DataSlice> unaligned_ds;
          unaligned_ds.reserve(ds_input_indices.size());
          for (auto idx : ds_input_indices) {
            unaligned_ds.push_back(input_refs[idx].UnsafeAs<DataSlice>());
          }
          ASSIGN_OR_RETURN((auto [aligned_ds, aligned_shape]),
                           shape::AlignNonScalars(std::move(unaligned_ds)),
                           ctx->set_status(std::move(_)));
          std::vector<arolla::TypedValue> typed_value_holder;
          typed_value_holder.reserve(ds_input_indices.size());
          for (size_t i = 0; i < ds_input_indices.size(); ++i) {
            ASSIGN_OR_RETURN(
                input_refs[ds_input_indices[i]],
                DataSliceToOwnedArollaRef(aligned_ds[i], typed_value_holder),
                ctx->set_status(std::move(_)));
          }
          // Pass ownership of `input_refs`, including the extra reserved
          // capacity, to the executor.
          ASSIGN_OR_RETURN(auto result,
                           execute_fn(expr_op, std::move(input_refs),
                                      std::move(aligned_shape)),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(result));
        });
  }

 private:
  ExecuteFn execute_fn_;
  size_t extra_inputs_;
};

absl::Status VerifyConvertAndEvalInputs(
    absl::Span<const arolla::QTypePtr> input_types) {
  if (input_types.empty()) {
    return absl::InvalidArgumentError("requires at least 1 arguments");
  }
  if (input_types[0] != arolla::GetQType<arolla::expr::ExprOperatorPtr>()) {
    return absl::InvalidArgumentError(
        absl::StrCat("the first argument must be an ExprOperator, but got ",
                     input_types[0]->name()));
  }
  for (int i = 1; i < input_types.size(); ++i) {
    if (input_types[i] == arolla::GetQType<DataSlice>()) {
      return absl::OkStatus();
    }
  }
  return absl::InvalidArgumentError("at least one input must be a DataSlice");
}

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

absl::StatusOr<DataSlice> SimpleAggEval(
    const arolla::expr::ExprOperatorPtr& expr_op, const DataSlice& x,
    internal::DataItem output_schema, bool is_agg_into) {
  const auto& shape = x.GetShape();
  if (shape.rank() == 0) {
    return absl::InvalidArgumentError("expected rank(x) > 0");
  }
  auto result_shape = is_agg_into ? shape.RemoveDims(shape.rank() - 1) : shape;
  if (!output_schema.has_value()) {
    output_schema = x.GetSchemaImpl();
  }
  ASSIGN_OR_RETURN(auto primitive_schema, GetPrimitiveArollaSchema(x));
  if (!primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(internal::DataItem(),
                                                std::move(output_schema)));
    return BroadcastToShape(ds, std::move(result_shape));
  }
  std::vector<arolla::TypedValue> typed_value_holder;
  typed_value_holder.reserve(1);
  ASSIGN_OR_RETURN(auto x_ref,
                   DataSliceToOwnedArollaRef(x, typed_value_holder));
  auto edge_tv = arolla::TypedValue::FromValue(shape.edges().back());
  ASSIGN_OR_RETURN(auto result,
                   EvalExpr(expr_op, {std::move(x_ref), edge_tv.AsRef()}));
  return DataSliceFromArollaValue(result.AsRef(), std::move(result_shape),
                                  std::move(output_schema));
}

}  // namespace

absl::StatusOr<arolla::TypedValue> EvalExpr(
    const arolla::expr::ExprOperatorPtr& expr_op,
    absl::Span<const arolla::TypedRef> inputs) {
  ASSIGN_OR_RETURN(auto fn, EvalCompiler::Compile(expr_op, inputs));
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

absl::StatusOr<arolla::OperatorPtr>
ConvertAndEvalWithShapeFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  RETURN_IF_ERROR(VerifyConvertAndEvalInputs(input_types));
  auto execute = [](const arolla::expr::ExprOperatorPtr& expr_op,
                    std::vector<arolla::TypedRef>&& inputs,
                    DataSlice::JaggedShape shape) -> absl::StatusOr<DataSlice> {
    DCHECK_GT(inputs.capacity(), inputs.size());
    auto aligned_shape_tv = arolla::TypedValue::FromValue(std::move(shape));
    inputs.push_back(aligned_shape_tv.AsRef());
    ASSIGN_OR_RETURN(auto result, EvalExpr(expr_op, inputs));
    if (!arolla::IsTupleQType(result.GetType()) ||
        result.GetFieldCount() != 2) {
      return absl::FailedPreconditionError(absl::StrCat(
          "expected a 2-tuple output, got ", result.GetType()->name()));
    }
    ASSIGN_OR_RETURN(auto shape_ref,
                     result.GetField(1).As<DataSlice::JaggedShape>());
    return DataSliceFromArollaValue(result.GetField(0), shape_ref.get());
  };
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<ConvertAndEvalBaseOp<decltype(execute)>>(
          "koda_internal.convert_and_eval_with_shape", input_types,
          std::move(execute), /*extra_inputs=*/1),
      input_types, output_type);
}

absl::StatusOr<DataSlice> SimplePointwiseEval(
    const arolla::expr::ExprOperatorPtr& expr_op, std::vector<DataSlice> inputs,
    internal::DataItem output_schema) {
  DCHECK_GE(inputs.size(), 1);
  schema::CommonSchemaAggregator schema_agg;
  internal::DataItem first_primitive_schema;
  for (const auto& x : inputs) {
    if (x.impl_has_mixed_dtype()) {
      return absl::InvalidArgumentError("mixed slices are not supported");
    }
    schema_agg.Add(x.GetSchemaImpl());
    if (!first_primitive_schema.has_value()) {
      ASSIGN_OR_RETURN(first_primitive_schema, GetPrimitiveArollaSchema(x));
    }
  }
  if (!output_schema.has_value()) {
    ASSIGN_OR_RETURN(output_schema, std::move(schema_agg).Get());
  }
  // All empty-and-unknown inputs. We then skip evaluation and just broadcast
  // the first input to the common shape and common schema.
  if (!first_primitive_schema.has_value()) {
    ASSIGN_OR_RETURN(auto common_shape, shape::GetCommonShape(inputs));
    ASSIGN_OR_RETURN(auto ds,
                     DataSlice::Create(internal::DataItem(), output_schema));
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
  ASSIGN_OR_RETURN(auto result, EvalExpr(expr_op, typed_refs));
  return DataSliceFromArollaValue(result.AsRef(), std::move(aligned_shape),
                                  std::move(output_schema));
}

absl::StatusOr<DataSlice> SimpleAggIntoEval(
    const arolla::expr::ExprOperatorPtr& expr_op, const DataSlice& x,
    internal::DataItem output_schema) {
  return SimpleAggEval(expr_op, x, std::move(output_schema),
                       /*is_agg_into=*/true);
}

absl::StatusOr<DataSlice> SimpleAggOverEval(
    const arolla::expr::ExprOperatorPtr& expr_op, const DataSlice& x,
    internal::DataItem output_schema) {
  return SimpleAggEval(expr_op, x, std::move(output_schema),
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
