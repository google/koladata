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
#include "koladata/operators/core.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/op_utils/deep_clone.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/op_utils/new_ids_like.h"
#include "koladata/internal/op_utils/utils.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/operators/utils.h"
#include "koladata/repr_utils.h"
#include "koladata/schema_utils.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

constexpr auto OpError = ::koladata::internal::ToOperatorEvalError;

absl::StatusOr<DataBagPtr> Attrs(const DataSlice& obj, bool update_schema,
                                 absl::Span<const absl::string_view> attr_names,
                                 absl::Span<const DataSlice> attr_values) {
  DCHECK_EQ(attr_names.size(), attr_values.size());
  if (obj.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set attributes on a DataSlice without a DataBag");
  }

  DataBagPtr result_db = DataBag::Empty();
  RETURN_IF_ERROR(AdoptStub(result_db, obj));

  if (!update_schema && obj.GetBag() != nullptr &&
      obj.GetSchemaImpl() != schema::kAny &&
      obj.GetSchemaImpl() != schema::kSchema) {
    // When update_schema is false, we copy the attributes from the source,
    // if they exist, so that SetAttrs can raise on conflict.
    bool object_mode = obj.GetSchemaImpl() == schema::kObject;
    ASSIGN_OR_RETURN(auto src_schema,
                     object_mode ? obj.GetObjSchema() : obj.GetSchema());
    auto dst_schema = src_schema.WithBag(result_db);
    for (const auto& attr_name : attr_names) {
      ASSIGN_OR_RETURN(auto attr_schema,
                       src_schema.GetAttrOrMissing(attr_name));
      if (!attr_schema.IsEmpty()) {
        // We remove the bag so that we don't try to adopt the schema,
        // as it is only necessary for conflict detection.
        RETURN_IF_ERROR(
            dst_schema.SetAttr(attr_name, attr_schema.WithBag(nullptr)));
      }
    }
  }

  // TODO: Remove after `SetAttrs` performs its own adoption.
  AdoptionQueue adoption_queue;
  for (const auto& value : attr_values) {
    adoption_queue.Add(value);
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*result_db));
  RETURN_IF_ERROR(
      obj.WithBag(result_db).SetAttrs(attr_names, attr_values, update_schema))
      .With([&](const absl::Status& status) {
        // We add obj.GetBag() to the error message context so that we can
        // properly print the old schema.
        return AssembleErrorMessage(
            status, {
                        .db = DataBag::ImmutableEmptyWithFallbacks(
                            {obj.GetBag(), result_db}),
                        .ds = obj,
                    });
      });
  ;
  result_db->UnsafeMakeImmutable();
  return result_db;
}

class AttrsOperator : public arolla::QExprOperator {
 public:
  explicit AttrsOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataBagPtr>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [slice_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         update_schema_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataBagPtr>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& slice = frame.Get(slice_slot);
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          const auto& attr_names = GetAttrNames(named_tuple_slot);
          const auto& values =
              GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result,
                           Attrs(slice, update_schema, attr_names, values),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(result));
        });
  }
};

absl::StatusOr<DataSlice> WithAttrs(
    const DataSlice& obj, bool update_schema,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> attr_values) {
  ASSIGN_OR_RETURN(DataBagPtr attrs_db,
                   Attrs(obj, update_schema, attr_names, attr_values));
  return obj.WithBag(
      DataBag::CommonDataBag({std::move(attrs_db), obj.GetBag()}));
}

}  // namespace

class WithAttrsOperator : public arolla::QExprOperator {
 public:
  explicit WithAttrsOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return arolla::MakeBoundOperator(
        [slice_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         update_schema_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[2],
         output_slot = output_slot.UnsafeToSlot<DataSlice>()](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const auto& slice = frame.Get(slice_slot);
          ASSIGN_OR_RETURN(
              bool update_schema,
              GetBoolArgument(frame.Get(update_schema_slot), "update_schema"),
              ctx->set_status(std::move(_)));
          const auto& attr_names = GetAttrNames(named_tuple_slot);
          const auto& values =
              GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result,
                           WithAttrs(slice, update_schema, attr_names, values),
                           ctx->set_status(std::move(_)));
          frame.Set(output_slot, std::move(result));
        });
  }
};

absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y))
      .With(OpError("kd.core.add"));
  RETURN_IF_ERROR(ExpectCanBeAdded("x", x)).With(OpError("kd.core.add"));
  RETURN_IF_ERROR(ExpectCanBeAdded("y", y)).With(OpError("kd.core.add"));
  return SimplePointwiseEval("kde.core._add_impl", {x, y});
}

DataSlice NoBag(const DataSlice& ds) { return ds.WithBag(nullptr); }

absl::StatusOr<DataSlice> Ref(const DataSlice& ds) {
  RETURN_IF_ERROR(ToItemId(ds).status());  // Reuse casting logic to validate.
  return ds.WithBag(nullptr);
}

absl::StatusOr<DataBagPtr> GetBag(const DataSlice& ds) {
  return ds.GetBag();
}

DataSlice WithBag(const DataSlice& ds, const DataBagPtr& db) {
  return ds.WithBag(db);
}

absl::StatusOr<DataSlice> WithMergedBag(const DataSlice& ds) {
  constexpr absl::string_view kOperatorName = "kd.with_merged_bag";
  if (ds.GetBag() == nullptr) {
    return internal::OperatorEvalError(
        kOperatorName, "expect the DataSlice to have a DataBag attached");
  }
  ASSIGN_OR_RETURN(
      auto merged_db, ds.GetBag()->MergeFallbacks(),
      internal::OperatorEvalError(std::move(_), kOperatorName,
                                  "failed to merge fallback DataBags"));
  merged_db->UnsafeMakeImmutable();
  return ds.WithBag(std::move(merged_db));
}

namespace {

class EnrichedOrUpdatedOperator final : public arolla::QExprOperator {
 public:
  EnrichedOrUpdatedOperator(absl::Span<const arolla::QTypePtr> input_types,
                            bool is_enriched_operator)
      : arolla::QExprOperator(arolla::QExprOperatorSignature::Get(
            input_types, arolla::GetQType<DataSlice>())),
        is_enriched_operator_(is_enriched_operator) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    return arolla::MakeBoundOperator(
        [input_slots = std::vector<arolla::TypedSlot>(input_slots.begin(),
                                                      input_slots.end()),
         output_slot = output_slot.UnsafeToSlot<DataSlice>(),
         is_enriched_operator = is_enriched_operator_](
            arolla::EvaluationContext* ctx, arolla::FramePtr frame) {
          const DataSlice& ds =
              frame.Get(input_slots[0].UnsafeToSlot<DataSlice>());
          std::vector<DataBagPtr> db_list;
          db_list.reserve(input_slots.size());
          if (is_enriched_operator) {
            db_list.push_back(ds.GetBag());
            for (size_t i = 1; i < input_slots.size(); ++i) {
              db_list.push_back(
                  frame.Get(input_slots[i].UnsafeToSlot<DataBagPtr>()));
            }
          } else {
            for (size_t i = input_slots.size() - 1; i >= 1; --i) {
              db_list.push_back(
                  frame.Get(input_slots[i].UnsafeToSlot<DataBagPtr>()));
            }
            db_list.push_back(ds.GetBag());
          }
          frame.Set(output_slot,
                    ds.WithBag(DataBag::ImmutableEmptyWithFallbacks(db_list)));
        });
  }

  bool is_enriched_operator_;
};

}  // namespace

absl::StatusOr<arolla::OperatorPtr>
EnrichedOrUpdatedOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.empty()) {
    return absl::InvalidArgumentError("requires at least 1 argument");
  }

  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "argument must be DataSlice, but got ", input_types[0]->name()));
  }

  for (const auto& db_input_type : input_types.subspan(1)) {
    if (db_input_type != arolla::GetQType<DataBagPtr>()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "arguments must be DataBag, but got ", db_input_type->name()));
    }
  }

  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<EnrichedOrUpdatedOperator>(input_types,
                                                  is_enriched_operator()),
      input_types, output_type);
}


absl::StatusOr<DataSlice> Extract(const DataSlice& ds,
                                  const DataSlice& schema) {
  return koladata::extract_utils_internal::ExtractWithSchema(ds, schema);
}

absl::StatusOr<DataSlice> IsEmpty(const DataSlice& obj) {
  return AsMask(obj.IsEmpty());
}

absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name) {
  ASSIGN_OR_RETURN(auto attr_name_str,
                   GetStringArgument(attr_name, "attr_name"));
  return obj.GetAttr(attr_name_str);
}

absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value) {
  ASSIGN_OR_RETURN(auto attr_name_str,
                   GetStringArgument(attr_name, "attr_name"));
  return obj.GetAttrWithDefault(attr_name_str, default_value);
}

absl::StatusOr<DataSlice> Stub(const DataSlice& x, const DataSlice& attrs) {
  // TODO: Implement.
  if (!attrs.IsEmpty()) {
    return absl::UnimplementedError("stub attrs not yet implemented");
  }

  auto db = DataBag::Empty();
  RETURN_IF_ERROR(AdoptStub(db, x));
  db->UnsafeMakeImmutable();
  return x.WithBag(std::move(db));
}

absl::StatusOr<arolla::OperatorPtr> AttrsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[2]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AttrsOperator>(input_types), input_types, output_type);
}

absl::StatusOr<DataBagPtr> Attr(const DataSlice& x,
                                const DataSlice& attr_name,
                                const DataSlice& value,
                                const DataSlice& update_schema) {
  ASSIGN_OR_RETURN(absl::string_view attr_name_view,
                   GetStringArgument(attr_name, "attr_name"));
  ASSIGN_OR_RETURN(bool update_schema_arg,
                   GetBoolArgument(update_schema, "update_schema"));
  return Attrs(x, update_schema_arg, {attr_name_view}, {value});
}

absl::StatusOr<arolla::OperatorPtr> WithAttrsOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 3) {
    return absl::InvalidArgumentError("requires exactly 2 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[2]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<WithAttrsOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataSlice> WithAttr(const DataSlice& x,
                                   const DataSlice& attr_name,
                                   const DataSlice& value,
                                   const DataSlice& update_schema) {
  ASSIGN_OR_RETURN(absl::string_view attr_name_view,
                   GetStringArgument(attr_name, "attr_name"));
  ASSIGN_OR_RETURN(bool update_schema_arg,
                   GetBoolArgument(update_schema, "update_schema"));
  return WithAttrs(x, update_schema_arg, {attr_name_view}, {value});
}

absl::StatusOr<DataSlice> Follow(const DataSlice& ds) {
  ASSIGN_OR_RETURN(auto nofollowed_schema_item,
                   schema::GetNoFollowedSchemaItem(ds.GetSchemaImpl()));
  return ds.WithSchema(nofollowed_schema_item);
}

template <>
DataBagPtr Freeze<DataBagPtr>(const DataBagPtr& x) {
  return x->Freeze();
}

template <>
DataSlice Freeze<DataSlice>(const DataSlice& x) {
  return x.FreezeBag();
}

absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     internal::NonDeterministicToken) {
  return ds.VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
      return DataSlice::Create(internal::NewIdsLike(impl), ds.GetShape(),
                               ds.GetSchemaImpl());
    }
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      auto slice_impl = internal::DataSliceImpl::Create(/*size=*/1, impl);
      return DataSlice::Create(internal::NewIdsLike(slice_impl)[0],
                               ds.GetShape(), ds.GetSchemaImpl());
    }
    DCHECK(false);
  });
}

absl::StatusOr<DataSlice> Clone(
    const DataSlice& ds, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken) {
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  ASSIGN_OR_RETURN(DataSlice shallow_clone, ShallowClone(ds, itemid, schema));
  DataSlice shallow_clone_with_fallback = shallow_clone.WithBag(
      DataBag::ImmutableEmptyWithFallbacks({shallow_clone.GetBag(), db}));
  return Extract(std::move(shallow_clone_with_fallback), schema);
}

absl::StatusOr<DataSlice> ShallowClone(
    const DataSlice& obj, const DataSlice& itemid, const DataSlice& schema,
    internal::NonDeterministicToken) {
  const auto& db = obj.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  if (obj.GetShape().rank() != itemid.GetShape().rank()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "obj and itemid must have the same rank. Got rank(obj): ",
        obj.GetShape().rank(), ", rank(itemid): ", itemid.GetShape().rank()));
  }
  const auto& schema_db = schema.GetBag();
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<internal::DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return obj.VisitImpl([&]<class T>(
                           const T& impl) -> absl::StatusOr<DataSlice> {
    const T& itemid_impl = itemid.impl<T>();
    auto result_db = DataBag::Empty();
    ASSIGN_OR_RETURN(auto result_db_impl, result_db->GetMutableImpl());
    internal::ShallowCloneOp clone_op(&result_db_impl.get());
    absl::Nullable<const internal::DataBagImpl*> schema_db_impl = nullptr;
    internal::DataBagImpl::FallbackSpan schema_fallbacks;
    if (schema_db != nullptr && schema_db != db) {
      schema_db_impl = &(schema_db->GetImpl());
      FlattenFallbackFinder schema_fb_finder(*schema_db);
      schema_fallbacks = schema_fb_finder.GetFlattenFallbacks();
    }
    ASSIGN_OR_RETURN((auto [result_slice_impl, result_schema_impl]),
                      clone_op(impl, itemid_impl, schema_impl, db->GetImpl(),
                              std::move(fallbacks_span), schema_db_impl,
                              std::move(schema_fallbacks)));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), obj.GetShape(),
                              std::move(result_schema_impl),
                              std::move(result_db));
  });
}

absl::StatusOr<DataSlice> DeepClone(
    const DataSlice& ds, const DataSlice& schema,
    internal::NonDeterministicToken) {
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  const auto& schema_db = schema.GetBag();
  if (schema_db != nullptr && schema_db != db) {
    ASSIGN_OR_RETURN(auto extracted_ds, Extract(ds, schema));
    return DeepClone(extracted_ds, schema.WithBag(extracted_ds.GetBag()));
  }
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<internal::DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::Empty();
    ASSIGN_OR_RETURN(auto result_db_impl, result_db->GetMutableImpl());
    internal::DeepCloneOp deep_clone_op(&result_db_impl.get());
    ASSIGN_OR_RETURN(
        (auto [result_slice_impl, result_schema_impl]),
        deep_clone_op(impl, schema_impl, db->GetImpl(), fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             std::move(result_schema_impl),
                             std::move(result_db));
  });
}

}  // namespace koladata::ops
