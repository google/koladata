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
#include "koladata/operators/core.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/text.h"
#include "koladata/adoption_utils.h"
#include "koladata/attr_error_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/error_repr_utils.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/coalesce_with_filtered.h"
#include "koladata/internal/op_utils/deep_clone.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/new_ids_like.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/qexpr.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/object_factories.h"
#include "koladata/operators/predicates.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

using ::koladata::internal::DataItem;

namespace {

// Updates the schema of the `result_db` to later be able to detect conflicts
// when setting attributes `attr_names` on `obj`. For each attribute name we
// prepare to set the corresponding attribute on the entire `obj` slice.
absl::Status UpdateSchemaForConflictDetection(
    const DataSlice& obj, absl::Span<const absl::string_view> attr_names,
    DataBagPtr& result_db) {
  bool object_mode = obj.GetSchemaImpl() == schema::kObject;
  ASSIGN_OR_RETURN(auto src_schema,
                   object_mode ? obj.GetObjSchema() : obj.GetSchema());
  auto dst_schema = src_schema.WithBag(result_db);
  for (const auto& attr_name : attr_names) {
    ASSIGN_OR_RETURN(auto attr_schema, src_schema.GetAttrOrMissing(attr_name));
    if (!attr_schema.IsEmpty()) {
      // We remove the bag so that we don't try to adopt the schema,
      // as it is only necessary for conflict detection.
      RETURN_IF_ERROR(
          dst_schema.SetAttr(attr_name, attr_schema.WithBag(nullptr)));
    }
  }
  return absl::OkStatus();
}

// Updates the schema of the `result_db` to later be able to detect conflicts
// when setting attributes `attr_names` on `obj`. For each attribute name we
// prepare to set the corresponding attribute on the corresponding item of
// `obj`, in a pointwise manner.
absl::Status UpdateSchemaForConflictDetection(const DataSlice& obj,
                                              const DataSlice& attr_names,
                                              DataBagPtr& result_db) {
  bool object_mode = obj.GetSchemaImpl() == schema::kObject;
  ASSIGN_OR_RETURN(DataSlice src_schema,
                   object_mode ? obj.GetObjSchema() : obj.GetSchema());
  DataSlice dst_schema = src_schema.WithBag(result_db);

  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(absl::Span<const DataSlice>{obj, attr_names}));
  const auto& aligned_obj = aligned_slices[0].impl<internal::DataSliceImpl>();
  const auto& aligned_attr_name =
      aligned_slices[1].impl<internal::DataSliceImpl>();

  absl::Status status = absl::OkStatus();
  if (object_mode) {
    DCHECK(!src_schema.is_item());
    DCHECK(!dst_schema.is_item());
    // In this case, src_schema and dst_schema are not DataItems, so we need to
    // iterate over all sub-items.
    DCHECK(aligned_obj.dtype() == arolla::GetQType<internal::ObjectId>());
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t offset, internal::ObjectId id, std::string_view attr_name) {
          if (!status.ok()) {
            return;
          }
          status = [&]() -> absl::Status {
            auto schema_item =
                src_schema.impl<internal::DataSliceImpl>()[offset];
            ASSIGN_OR_RETURN(
                DataSlice item_schema,
                DataSlice::Create(schema_item, DataItem(schema::kSchema),
                                  obj.GetBag()));
            ASSIGN_OR_RETURN(DataSlice attr_schema,
                             item_schema.GetAttrOrMissing(attr_name));
            if (!attr_schema.IsEmpty()) {
              // We remove the bag so that we don't try to adopt the schema,
              // as it is only necessary for conflict detection.
              ASSIGN_OR_RETURN(
                  DataSlice dst_item_schema,
                  DataSlice::Create(schema_item, DataItem(schema::kSchema),
                                    result_db));
              RETURN_IF_ERROR(dst_item_schema.SetAttr(
                  attr_name, item_schema.WithBag(nullptr)));
            }
            return absl::OkStatus();
          }();
        },
        aligned_obj.values<internal::ObjectId>(),
        aligned_attr_name.values<arolla::Text>()));
  } else {
    aligned_attr_name.values<arolla::Text>().ForEachPresent(
        [&](int64_t i, std::string_view attr_name) {
          if (!status.ok()) {
            return;
          }
          status = [&]() -> absl::Status {
            ASSIGN_OR_RETURN(auto attr_schema,
                             src_schema.GetAttrOrMissing(attr_name));
            if (!attr_schema.IsEmpty()) {
              // We remove the bag so that we don't try to adopt the schema,
              // as it is only necessary for conflict detection.
              RETURN_IF_ERROR(
                  dst_schema.SetAttr(attr_name, attr_schema.WithBag(nullptr)));
            }
            return absl::OkStatus();
          }();
        });
  }
  RETURN_IF_ERROR(std::move(status));
  return absl::OkStatus();
}

absl::StatusOr<DataBagPtr> Attrs(const DataSlice& obj, bool overwrite_schema,
                                 absl::Span<const absl::string_view> attr_names,
                                 absl::Span<const DataSlice> attr_values) {
  DCHECK_EQ(attr_names.size(), attr_values.size());
  RETURN_IF_ERROR(CheckEligibleForSetAttr(obj)).SetPrepend()
      << "failed to create attribute update; ";
  DataBagPtr result_db = DataBag::EmptyMutable();
  RETURN_IF_ERROR(AdoptStub(result_db, obj));

  if (!overwrite_schema && obj.GetBag() != nullptr &&
      obj.GetSchemaImpl() != schema::kNone &&
      obj.GetSchemaImpl() != schema::kSchema) {
    // When overwrite_schema is false, we copy the attributes from the source,
    // if they exist, so that SetAttrs can raise on conflict.
    RETURN_IF_ERROR(
        UpdateSchemaForConflictDetection(obj, attr_names, result_db));
  }

  // TODO: Remove after `SetAttrs` performs its own adoption.
  AdoptionQueue adoption_queue;
  for (const auto& value : attr_values) {
    adoption_queue.Add(value);
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*result_db));
  RETURN_IF_ERROR(obj.WithBag(result_db).SetAttrs(attr_names, attr_values,
                                                  overwrite_schema))
      .With([&](absl::Status status) {
        return KodaErrorCausedByIncompatibleSchemaError(
            std::move(status), obj.GetBag(), result_db, obj);
      });
  result_db->UnsafeMakeImmutable();
  return result_db;
}

absl::StatusOr<DataBagPtr> AttrsForAttrNameSlice(const DataSlice& obj,
                                                 bool overwrite_schema,
                                                 const DataSlice& attr_names,
                                                 const DataSlice& attr_values) {
  RETURN_IF_ERROR(CheckEligibleForSetAttr(obj)).SetPrepend()
      << "failed to create attribute update; ";
  DataBagPtr result_db = DataBag::EmptyMutable();
  RETURN_IF_ERROR(AdoptStub(result_db, obj));

  if (!overwrite_schema && obj.GetBag() != nullptr &&
      obj.GetSchemaImpl() != schema::kNone &&
      obj.GetSchemaImpl() != schema::kSchema) {
    // When overwrite_schema is false, we copy the attributes from the source,
    // if they exist, so that SetAttrs can raise on conflict.
    RETURN_IF_ERROR(
        UpdateSchemaForConflictDetection(obj, attr_names, result_db));
  }

  // TODO: Remove after `SetAttrs` performs its own adoption.
  AdoptionQueue adoption_queue;
  adoption_queue.Add(attr_values);

  RETURN_IF_ERROR(adoption_queue.AdoptInto(*result_db));
  RETURN_IF_ERROR(
      obj.WithBag(result_db).SetAttr(attr_names, attr_values, overwrite_schema))
      .With([&](absl::Status status) {
        return KodaErrorCausedByIncompatibleSchemaError(
            std::move(status), obj.GetBag(), result_db, obj);
      });
  result_db->UnsafeMakeImmutable();
  return result_db;
}

class AttrsImplOperator : public arolla::QExprOperator {
 public:
  explicit AttrsImplOperator(absl::Span<const arolla::QTypePtr> input_types)
      : QExprOperator(input_types, arolla::GetQType<DataBagPtr>()) {}

  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    return MakeBoundOperator<~KodaOperatorWrapperFlags::kWrapError>(
        "kd.core._attrs_impl",
        [slice_slot = input_slots[0].UnsafeToSlot<DataSlice>(),
         update_schema_slot = input_slots[1].UnsafeToSlot<DataSlice>(),
         extend_schema_slot = input_slots[2].UnsafeToSlot<DataSlice>(),
         named_tuple_slot = input_slots[3],
         output_slot = output_slot.UnsafeToSlot<DataBagPtr>()](
            arolla::EvaluationContext* ctx,
            arolla::FramePtr frame) -> absl::Status {
          const auto& slice = frame.Get(slice_slot);
          ASSIGN_OR_RETURN(bool overwrite_schema,
                           GetBoolArgument(frame.Get(update_schema_slot),
                                           "overwrite_schema"));

          ASSIGN_OR_RETURN(
              bool extend_schema,
              GetBoolArgument(frame.Get(extend_schema_slot), "extend_schema"));

          RETURN_IF_ERROR(CheckEligibleForSetAttr(slice));
          const auto& attr_names = GetFieldNames(named_tuple_slot);
          if (!extend_schema) {
            const auto& schema = slice.GetSchema();
            if (!schema.IsEntitySchema()) {
              return absl::InvalidArgumentError(
                  absl::StrCat("x must have an Entity schema, actual schema: ",
                               SchemaToStr(schema)));
            }
            for (absl::string_view attr_name : attr_names) {
              ASSIGN_OR_RETURN(auto has_attr, schema.HasAttr(attr_name));
              if (has_attr.IsEmpty()) {
                return absl::InvalidArgumentError(absl::StrCat(
                    "attribute '", attr_name,
                    "' not found in schema: ", SchemaToStr(schema)));
              }
            }
          }
          const auto& values = GetValueDataSlices(named_tuple_slot, frame);
          ASSIGN_OR_RETURN(auto result,
                           Attrs(slice, overwrite_schema, attr_names, values));
          frame.Set(output_slot, std::move(result));
          return absl::OkStatus();
        });
  }
};

class WithTimestampOperator final : public arolla::QExprOperator {
 public:
  using QExprOperator::QExprOperator;
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const final {
    ASSIGN_OR_RETURN(arolla::FrameLayout::Slot<DataSlice> timestamp_slot,
                     output_slot.SubSlot(1).ToSlot<DataSlice>());
    return arolla::MakeBoundOperator(
        [input_slot = input_slots[0], output_slot = output_slot,
         timestamp_slot = timestamp_slot](arolla::EvaluationContext*,
                                          arolla::FramePtr frame) {
          input_slot.CopyTo(frame, output_slot.SubSlot(0), frame);
          absl::Time now = absl::Now();
          absl::Duration since_epoch = now - absl::UnixEpoch();
          double unix_timestamp_double = absl::ToDoubleSeconds(since_epoch);
          DataSlice timestamp_ds =
              DataSlice::CreatePrimitive(unix_timestamp_double);
          frame.Set(timestamp_slot, std::move(timestamp_ds));
          return absl::OkStatus();
        });
  }
};

}  // namespace

DataSlice NoBag(const DataSlice& ds) { return ds.WithBag(nullptr); }

absl::StatusOr<DataSlice> Ref(const DataSlice& ds) {
  RETURN_IF_ERROR(ToItemId(ds).status());  // Reuse casting logic to validate.
  return ds.WithBag(nullptr);
}

absl::StatusOr<DataBagPtr> GetBag(const DataSlice& ds) { return ds.GetBag(); }

DataSlice WithBag(const DataSlice& ds, const DataBagPtr& db) {
  return ds.WithBag(db);
}

absl::StatusOr<DataSlice> WithMergedBag(const DataSlice& ds) {
  if (ds.GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "expect the DataSlice to have a DataBag attached");
  }
  ASSIGN_OR_RETURN(auto merged_db, ds.GetBag()->MergeFallbacks(),
                   internal::KodaErrorFromCause(
                       "failed to merge fallback DataBags", std::move(_)));
  merged_db->UnsafeMakeImmutable();
  return ds.WithBag(std::move(merged_db));
}

namespace {

class EnrichedOrUpdatedOperator final : public arolla::QExprOperator {
 public:
  EnrichedOrUpdatedOperator(absl::Span<const arolla::QTypePtr> input_types,
                            bool is_enriched_operator)
      : arolla::QExprOperator(input_types, arolla::GetQType<DataSlice>()),
        is_enriched_operator_(is_enriched_operator) {}

 private:
  absl::StatusOr<std::unique_ptr<arolla::BoundOperator>> DoBind(
      absl::Span<const arolla::TypedSlot> input_slots,
      arolla::TypedSlot output_slot) const override {
    absl::string_view op_name =
        is_enriched_operator_ ? "kd.core.enriched" : "kd.core.updated";
    return MakeBoundOperator(
        std::string(op_name),
        [input_slots = std::vector<arolla::TypedSlot>(input_slots.begin(),
                                                      input_slots.end()),
         output_slot = output_slot.UnsafeToSlot<DataSlice>(),
         is_enriched_operator = is_enriched_operator_,
         op_name](arolla::EvaluationContext* ctx,
                  arolla::FramePtr frame) -> absl::Status {
          const DataSlice& ds =
              frame.Get(input_slots[0].UnsafeToSlot<DataSlice>());
          auto orig_bag = ds.GetBag();
          if (orig_bag != nullptr && orig_bag->IsMutable()) {
            return absl::InvalidArgumentError(
                absl::StrCat(op_name,
                             " requires the original DataBag to be immutable; "
                             "either freeze it, or use mutable API"));
          }
          if (orig_bag != nullptr && orig_bag->HasMutableFallbacks()) {
            orig_bag = orig_bag->Freeze();
          }
          std::vector<DataBagPtr> db_list;
          db_list.reserve(input_slots.size());
          if (is_enriched_operator) {
            db_list.push_back(std::move(orig_bag));
            for (size_t i = 1; i < input_slots.size(); ++i) {
              const auto& bag =
                  frame.Get(input_slots[i].UnsafeToSlot<DataBagPtr>());
              db_list.push_back(bag != nullptr && (bag->IsMutable() ||
                                                   bag->HasMutableFallbacks())
                                    ? bag->Freeze()
                                    : bag);
            }
          } else {
            for (size_t i = input_slots.size() - 1; i >= 1; --i) {
              const auto& bag =
                  frame.Get(input_slots[i].UnsafeToSlot<DataBagPtr>());
              db_list.push_back(bag != nullptr && (bag->IsMutable() ||
                                                   bag->HasMutableFallbacks())
                                    ? bag->Freeze()
                                    : bag);
            }
            db_list.push_back(std::move(orig_bag));
          }
          ASSIGN_OR_RETURN(DataBagPtr res_db,
                           DataBag::ImmutableEmptyWithFallbacks(db_list));
          frame.Set(output_slot, ds.WithBag(std::move(res_db)));
          return absl::OkStatus();
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

namespace {

absl::Status ValidateGetAttrArguments(const DataSlice& obj,
                                      const DataSlice& attr_name) {
  RETURN_IF_ERROR(ExpectString("attr_name", attr_name));
  RETURN_IF_ERROR(ValidateAttrLookupAllowed(obj)).SetPrepend()
      << "failed to get attribute; ";
  return absl::OkStatus();
}

// Processes a single item (obj, attr_name) pair for GetAttr (by slice) methods.
absl::Status ProcessSingleItem(const DataItem& obj, std::string_view attr_name,
                               size_t offset,
                               const internal::DataBagImpl& db_impl,
                               internal::DataBagImpl::FallbackSpan fallbacks,
                               const DataItem& schema,
                               const internal::DataSliceImpl& schema_attr,
                               internal::SliceBuilder& res_builder,
                               internal::SliceBuilder& schema_builder,
                               bool allow_missing = false) {
  ASSIGN_OR_RETURN(DataItem res, db_impl.GetAttr(obj, attr_name, fallbacks));
  res_builder.InsertIfNotSetAndUpdateAllocIds(offset, res);

  DataItem schema_res;
  if (schema.is_object_schema() && attr_name == schema::kSchemaAttr) {
    schema_res = internal::DataItem(schema::kSchema);
  } else {
    const DataItem& schema_item =
        schema.is_struct_schema() ? schema : schema_attr[offset];
    if (allow_missing) {
      ASSIGN_OR_RETURN(schema_res, db_impl.GetSchemaAttrAllowMissing(
                                       schema_item, attr_name, fallbacks));
    } else {
      ASSIGN_OR_RETURN(
          schema_res, db_impl.GetSchemaAttr(schema_item, attr_name, fallbacks));
    }
  }
  schema_builder.InsertIfNotSetAndUpdateAllocIds(offset, schema_res);
  return absl::OkStatus();
}

// Processes a single item (obj, attr_name) pair for GetAttr (by slice)
// methods when obj is a schema item.
absl::Status ProcessSingleSchemaItem(
    const DataItem& obj, std::string_view attr_name, size_t offset,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks,
    internal::SliceBuilder& res_builder, internal::SliceBuilder& schema_builder,
    bool allow_missing = false) {
  DataItem res;
  if (allow_missing) {
    ASSIGN_OR_RETURN(
        res, db_impl.GetSchemaAttrAllowMissing(obj, attr_name, fallbacks));
  } else {
    ASSIGN_OR_RETURN(res, db_impl.GetSchemaAttr(obj, attr_name, fallbacks));
  }
  res_builder.InsertIfNotSet(offset, res);

  DataItem schema_res;
  if (attr_name == schema::kSchemaNameAttr) {
    schema_res = internal::DataItem(schema::kString);
  } else if (attr_name == schema::kSchemaMetadataAttr) {
    schema_res = internal::DataItem(schema::kObject);
  } else {
    schema_res = internal::DataItem(schema::kSchema);
  }
  schema_builder.InsertIfNotSet(offset, schema_res);

  return absl::OkStatus();
}

// Returns DataSliceImpl of schema attribute if schema is OBJECT and empty
// DataSliceImpl otherwise.
absl::StatusOr<internal::DataSliceImpl> GetObjSchemaAttr(
    const internal::DataSliceImpl& obj, const DataItem& schema_item,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  if (schema_item.is_struct_schema() || schema_item.is_schema_schema()) {
    return internal::DataSliceImpl();
  }
  DCHECK(schema_item.is_object_schema());
  return db_impl.GetObjSchemaAttr(obj, fallbacks);
}

// Returns the fetched attributes and corresponding schemas.
absl::StatusOr<std::pair<internal::DataSliceImpl, internal::DataSliceImpl>>
GetAttrImpl(const internal::DataSliceImpl& aligned_obj,
            const internal::DataSliceImpl& aligned_attr_name,
            const internal::DataItem& obj_schema, const DataBag& db,
            bool allow_missing) {
  DCHECK_EQ(aligned_obj.size(), aligned_attr_name.size());
  // In case `aligned_obj` is empty, we still want to ensure that the schema
  // has the attribute so we don't exit early.
  if (aligned_attr_name.is_empty_and_unknown()) {
    return std::make_pair(
        internal::DataSliceImpl::CreateEmptyAndUnknownType(aligned_obj.size()),
        internal::DataSliceImpl::CreateEmptyAndUnknownType(aligned_obj.size()));
  }

  FlattenFallbackFinder fb_finder(db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  const internal::DataBagImpl& db_impl = db.GetImpl();

  // Empty if schema is not schema::kObject.
  ASSIGN_OR_RETURN(
      internal::DataSliceImpl schema_attr,
      GetObjSchemaAttr(aligned_obj, obj_schema, db_impl, fallbacks));

  internal::SliceBuilder res_builder(aligned_obj.size());
  internal::SliceBuilder schema_builder(aligned_obj.size());
  absl::Status status = absl::OkStatus();
  RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
      [&](int64_t offset, arolla::OptionalValue<DataItem> item,
          std::string_view attr_name) {
        if (!status.ok()) {
          return;
        }
        DataItem actual_item =
            item.present ? std::move(item).value : DataItem();
        if (obj_schema.is_schema_schema()) {
          status = ProcessSingleSchemaItem(actual_item, attr_name, offset,
                                           db_impl, fallbacks, res_builder,
                                           schema_builder, allow_missing);
        } else {
          status = ProcessSingleItem(
              actual_item, attr_name, offset, db_impl, fallbacks, obj_schema,
              schema_attr, res_builder, schema_builder, allow_missing);
        }
      },
      aligned_obj.AsDataItemDenseArray(),
      aligned_attr_name.values<arolla::Text>()));
  RETURN_IF_ERROR(std::move(status));
  auto schema_res = std::move(schema_builder).Build();
  ASSIGN_OR_RETURN(auto schema_filter, internal::HasOp()(schema_res));
  ASSIGN_OR_RETURN(
      auto res,
      internal::PresenceAndOp()(std::move(res_builder).Build(), schema_filter));
  return std::make_pair(std::move(res), std::move(schema_res));
}

}  // namespace

absl::StatusOr<DataSlice> GetAttr(const DataSlice& obj,
                                  const DataSlice& attr_name) {
  if (attr_name.is_item()) {
    ASSIGN_OR_RETURN(absl::string_view attr_name_str,
                     GetStringArgument(attr_name, "attr_name"));
    return obj.GetAttr(attr_name_str);
  }
  RETURN_IF_ERROR(ValidateGetAttrArguments(obj, attr_name));
  DCHECK_NE(obj.GetBag(), nullptr);  // Checked by ValidateGetAttrArguments.
  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(absl::Span<const DataSlice>{obj, attr_name}));
  ASSIGN_OR_RETURN(
      (auto [res_slice, schema_res_slice]),
      GetAttrImpl(aligned_slices[0].impl<internal::DataSliceImpl>(),
                  aligned_slices[1].impl<internal::DataSliceImpl>(),
                  obj.GetSchemaImpl(), *obj.GetBag(),
                  /*allow_missing=*/false));
  ASSIGN_OR_RETURN(DataItem schema_res, schema::CommonSchema(schema_res_slice));
  if (!schema_res.has_value()) {
    schema_res = DataItem(schema::kNone);
  }
  return DataSlice::Create(std::move(res_slice), aligned_slices[0].GetShape(),
                           DataItem(std::move(schema_res)), obj.GetBag());
}

absl::StatusOr<DataSlice> GetAttrWithDefault(const DataSlice& obj,
                                             const DataSlice& attr_name,
                                             const DataSlice& default_value) {
  if (attr_name.is_item()) {
    ASSIGN_OR_RETURN(absl::string_view attr_name_str,
                     GetStringArgument(attr_name, "attr_name"));
    return obj.GetAttrWithDefault(attr_name_str, default_value);
  }
  RETURN_IF_ERROR(ValidateGetAttrArguments(obj, attr_name));
  DCHECK_NE(obj.GetBag(), nullptr);  // Checked by ValidateGetAttrArguments.
  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(absl::Span<const DataSlice>{obj, attr_name}));
  ASSIGN_OR_RETURN(
      (auto [res_slice, schema_res_slice]),
      GetAttrImpl(aligned_slices[0].impl<internal::DataSliceImpl>(),
                  aligned_slices[1].impl<internal::DataSliceImpl>(),
                  obj.GetSchemaImpl(), *obj.GetBag(),
                  /*allow_missing=*/true));
  ASSIGN_OR_RETURN(
      auto expanded_default,
      BroadcastToShape(default_value, aligned_slices[0].GetShape()));
  ASSIGN_OR_RETURN(internal::DataSliceImpl res,
                   internal::CoalesceWithFiltered(
                       aligned_slices[0].impl<internal::DataSliceImpl>(),
                       std::move(res_slice),
                       expanded_default.impl<internal::DataSliceImpl>()));

  ASSIGN_OR_RETURN(DataItem schema_res, schema::CommonSchema(schema_res_slice));
  ASSIGN_OR_RETURN(schema_res, schema::CommonSchema(
                                   schema_res, default_value.GetSchemaImpl()));
  if (!schema_res.has_value()) {
    schema_res = DataItem(schema::kNone);
  }

  ASSIGN_OR_RETURN(auto result_db,
                   WithAdoptedValues(obj.GetBag(), default_value));
  return DataSlice::Create(std::move(res), aligned_slices[0].GetShape(),
                           DataItem(std::move(schema_res)),
                           std::move(result_db));
}

absl::StatusOr<DataSlice> HasAttr(const DataSlice& obj,
                                  const DataSlice& attr_name) {
  if (attr_name.is_item()) {
    ASSIGN_OR_RETURN(absl::string_view attr_name_str,
                     GetStringArgument(attr_name, "attr_name"));
    return obj.HasAttr(attr_name_str);
  }
  RETURN_IF_ERROR(ValidateGetAttrArguments(obj, attr_name));
  DCHECK_NE(obj.GetBag(), nullptr);  // Checked by ValidateGetAttrArguments.
  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(absl::Span<const DataSlice>{obj, attr_name}));
  ASSIGN_OR_RETURN(
      (auto [res, _]),
      GetAttrImpl(aligned_slices[0].impl<internal::DataSliceImpl>(),
                  aligned_slices[1].impl<internal::DataSliceImpl>(),
                  obj.GetSchemaImpl(), *obj.GetBag(),
                  /*allow_missing=*/true));
  ASSIGN_OR_RETURN(res, internal::HasOp()(res));
  return DataSlice::Create(std::move(res), aligned_slices[0].GetShape(),
                           DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> GetAttrNames(const DataSlice& ds,
                                       const DataSlice& intersection) {
  ASSIGN_OR_RETURN(bool intersection_arg,
                   GetBoolArgument(intersection, "intersection"));
  ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names_set,
                   ds.GetAttrNames(/*union_object_attrs=*/!intersection_arg));
  internal::SliceBuilder builder(attr_names_set.size());
  auto typed_builder = builder.typed<arolla::Text>();
  int64_t id = 0;
  for (absl::string_view attr_name : attr_names_set) {
    typed_builder.InsertIfNotSet(id, attr_name);
    ++id;
  }
  return DataSlice::CreateWithFlatShape(std::move(builder).Build(),
                                        DataItem(schema::kString));
}

absl::StatusOr<DataSlice> Stub(const DataSlice& x, const DataSlice& attrs) {
  // TODO: Implement.
  if (!attrs.IsEmpty()) {
    return absl::UnimplementedError("stub attrs not yet implemented");
  }

  auto db = DataBag::EmptyMutable();
  RETURN_IF_ERROR(AdoptStub(db, x));
  db->UnsafeMakeImmutable();
  return x.WithBag(std::move(db));
}

absl::StatusOr<arolla::OperatorPtr> AttrsImplOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 4) {
    return absl::InvalidArgumentError("requires exactly 4 arguments");
  }
  if (input_types[0] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires first argument to be DataSlice");
  }
  if (input_types[1] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires second argument to be DataSlice");
  }
  if (input_types[2] != arolla::GetQType<DataSlice>()) {
    return absl::InvalidArgumentError(
        "requires third argument to be DataSlice");
  }
  RETURN_IF_ERROR(VerifyNamedTuple(input_types[3]));
  return arolla::EnsureOutputQTypeMatches(
      std::make_shared<AttrsImplOperator>(input_types), input_types,
      output_type);
}

absl::StatusOr<DataBagPtr> Attr(const DataSlice& x, const DataSlice& attr_name,
                                const DataSlice& value,
                                const DataSlice& overwrite_schema) {
  ASSIGN_OR_RETURN(bool overwrite_schema_arg,
                   GetBoolArgument(overwrite_schema, "overwrite_schema"));
  if (attr_name.is_item()) {
    ASSIGN_OR_RETURN(absl::string_view attr_name_view,
                     GetStringArgument(attr_name, "attr_name"));
    return Attrs(x, overwrite_schema_arg, {attr_name_view}, {value});
  }
  RETURN_IF_ERROR(ExpectString("attr_name", attr_name));
  return AttrsForAttrNameSlice(x, overwrite_schema_arg, attr_name, value);
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

absl::StatusOr<DataSlice> GetMetadata(const DataSlice& ds) {
  auto schema = ds.GetSchemaImpl();
  if (schema == schema::kSchema) {
    return ds.GetAttr(schema::kSchemaMetadataAttr);
  }
  return absl::InvalidArgumentError(
      absl::StrCat("failed to get metadata; cannot get for a DataSlice with ",
                   SchemaToStr(ds.GetSchema()), " schema"));
}

absl::StatusOr<DataSlice> CreateMetadata(const DataSlice& ds) {
  auto result_db = DataBag::EmptyMutable();
  auto result = CreateMetadata(result_db, ds, {}, {});
  result_db->UnsafeMakeImmutable();
  return result;
}

absl::StatusOr<DataSlice> NewIdsLike(const DataSlice& ds,
                                     internal::NonDeterministicToken) {
  return ds.VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
      return DataSlice::Create(internal::NewIdsLike(impl), ds.GetShape(),
                               ds.GetSchemaImpl());
    }
    if constexpr (std::is_same_v<T, DataItem>) {
      auto slice_impl = internal::DataSliceImpl::Create(/*size=*/1, impl);
      return DataSlice::Create(internal::NewIdsLike(slice_impl)[0],
                               ds.GetShape(), ds.GetSchemaImpl());
    }
    DCHECK(false);
  });
}

absl::StatusOr<DataSlice> Clone(const DataSlice& ds, const DataSlice& itemid,
                                const DataSlice& schema,
                                internal::NonDeterministicToken) {
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  ASSIGN_OR_RETURN(DataSlice shallow_clone, ShallowClone(ds, itemid, schema));
  DataSlice shallow_clone_with_fallback = shallow_clone.WithBag(
      DataBag::ImmutableEmptyWithDeprecatedMutableFallbacks(
          {shallow_clone.GetBag(), db}));
  return Extract(std::move(shallow_clone_with_fallback), schema);
}

absl::StatusOr<DataSlice> ShallowClone(const DataSlice& obj,
                                       const DataSlice& itemid,
                                       const DataSlice& schema,
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
  const auto& schema_impl = schema.impl<DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return obj.VisitImpl(
      [&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        const T& itemid_impl = itemid.impl<T>();
        auto result_db = DataBag::EmptyMutable();
        ASSIGN_OR_RETURN(auto& result_db_impl, result_db->GetMutableImpl());
        internal::ShallowCloneOp clone_op(&result_db_impl);
        const internal::DataBagImpl* absl_nullable schema_db_impl = nullptr;
        FlattenFallbackFinder schema_fb_finder;
        internal::DataBagImpl::FallbackSpan schema_fallbacks;
        if (schema_db != nullptr && schema_db != db) {
          schema_db_impl = &(schema_db->GetImpl());
          schema_fb_finder = FlattenFallbackFinder(*schema_db);
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

absl::StatusOr<DataSlice> DeepClone(const DataSlice& ds,
                                    const DataSlice& schema,
                                    internal::NonDeterministicToken) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  const auto& schema_impl = schema.impl<DataItem>();
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    ASSIGN_OR_RETURN(auto is_primitive, IsPrimitive(ds));
    if (ds.IsEmpty() || is_primitive.present_count()) {
      return ds.WithSchema(schema);
    }
    return absl::InvalidArgumentError("cannot clone without a DataBag");
  }
  const auto& schema_db = schema.GetBag();
  if (schema_db != nullptr && schema_db != db) {
    ASSIGN_OR_RETURN(auto extracted_ds, Extract(ds, schema));
    return DeepClone(extracted_ds, schema.WithBag(extracted_ds.GetBag()));
  }
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& result_db_impl, result_db->GetMutableImpl());
    internal::DeepCloneOp deep_clone_op(&result_db_impl);
    ASSIGN_OR_RETURN(
        auto result_slice_impl,
        deep_clone_op(impl, schema_impl, db->GetImpl(), fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             schema_impl, std::move(result_db));
  });
}

absl::StatusOr<arolla::OperatorPtr> WithTimestampOperatorFamily::DoGetOperator(
    absl::Span<const arolla::QTypePtr> input_types,
    arolla::QTypePtr output_type) const {
  if (input_types.size() != 2) {
    return absl::InvalidArgumentError("requires 2 argument");
  }
  RETURN_IF_ERROR(ops::VerifyIsNonDeterministicToken(input_types[1]));
  return std::make_shared<WithTimestampOperator>(input_types, output_type);
}

}  // namespace koladata::ops
