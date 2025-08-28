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
#ifndef KOLADATA_OPERATORS_UNARY_OP_H_
#define KOLADATA_OPERATORS_UNARY_OP_H_

#include <optional>
#include <type_traits>
#include <utility>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace unary_op_impl {

struct DummyArgsChecker {
  template <class T>
  static constexpr bool kIsInvocable = true;

  absl::Status CheckArgs(const DataSlice&) const {
    return absl::OkStatus();
  }
};

template <class Fn, class T>
absl::StatusOr<DataSlice> EvalScalar(const internal::DataItem& item,
                                     internal::DataItem output_schema) {
  if (!item.has_value()) {
    return DataSlice::UnsafeCreate(internal::DataItem(),
                                   std::move(output_schema));
  }
  if constexpr (arolla::IsStatusOrT<decltype(Fn{}(item.value<T>()))>::value) {
    auto res_or = Fn{}(item.value<T>());
    if (res_or.ok()) {
      return DataSlice::UnsafeCreate(internal::DataItem(*res_or),
                                     std::move(output_schema));
    } else {
      return res_or.status();
    }
  } else {
    return DataSlice::UnsafeCreate(internal::DataItem(Fn{}(item.value<T>())),
                                   std::move(output_schema));
  }
}

template <class Fn, class T, class OutT>
absl::StatusOr<DataSlice> EvalSlice(const DataSlice& in,
                                    internal::DataItem output_schema) {
  const internal::DataSliceImpl& slice = in.slice();
  if (slice.is_empty_and_unknown()) {
    return DataSlice::UnsafeCreate(
        internal::DataSliceImpl::CreateEmptyAndUnknownType(slice.size()),
        in.GetShape(), std::move(output_schema));
  }
  constexpr int flags =
      arolla::DenseOpFlags::kNoBitmapOffset |
      (arolla::IsRunOnMissingOp<Fn>::value ? arolla::DenseOpFlags::kRunOnMissing
                                           : 0);
  auto fn = [](arolla::view_type_t<T> v) { return Fn{}(v); };
  auto r =
      arolla::CreateDenseOp<flags, decltype(fn), OutT>(fn)(slice.values<T>());
  if constexpr (arolla::IsStatusOrT<decltype(r)>::value) {
    RETURN_IF_ERROR(r.status());
    return DataSlice::UnsafeCreate(
        internal::DataSliceImpl::Create(std::move(*r)), in.GetShape(),
        std::move(output_schema));
  } else {
    return DataSlice::UnsafeCreate(
        internal::DataSliceImpl::Create(std::move(r)), in.GetShape(),
        std::move(output_schema));
  }
}

}  // namespace unary_op_impl

template <class Fn, class ArgsChecker = unary_op_impl::DummyArgsChecker>
absl::StatusOr<DataSlice> UnaryOpEval(
    const DataSlice& in,
    const ArgsChecker& args_checker = unary_op_impl::DummyArgsChecker(),
    std::optional<schema::DType> output_dtype = std::nullopt) {
  if (!in.GetSchemaImpl().holds_value<schema::DType>()) {
    RETURN_IF_ERROR(args_checker.CheckArgs(in));
    return absl::InvalidArgumentError(absl::StrCat(
        "argument must have primitive type, got ", in.GetSchemaImpl()));
  }
  schema::DType schema_type = in.GetSchemaImpl().value<schema::DType>();
  arolla::QTypePtr type =
      schema_type == schema::kObject ? in.dtype() : schema_type.qtype();

  if (type == arolla::GetNothingQType()) {
    RETURN_IF_ERROR(args_checker.CheckArgs(in));
    if (in.impl_has_mixed_dtype()) {
      return absl::InvalidArgumentError(
          "DataSlice with mixed types is not supported");
    }
    if (!output_dtype.has_value()) {
      // Here schema_type is either NONE or OBJECT (otherwise `type` wouldn't be
      // NothingQType).
      output_dtype = schema_type;
    }
    if (in.is_item()) {
      return DataSlice::UnsafeCreate(internal::DataItem(),
                                     internal::DataItem(*output_dtype));
    } else {
      return DataSlice::UnsafeCreate(
          internal::DataSliceImpl::CreateEmptyAndUnknownType(in.size()),
          in.GetShape(), internal::DataItem(*output_dtype));
    }
  }

  bool type_supported = false;
  absl::StatusOr<DataSlice> res;
  arolla::meta::foreach_type(
      schema::supported_primitive_dtypes(), [&](auto tpe) {
        using T = typename std::decay_t<decltype(tpe)>::type;
        if (type != arolla::GetQType<T>()) {
          return;
        }
        if (absl::Status st = CheckType<T>(in); !st.ok()) {
          res = st;
          return;
        }
        if constexpr (ArgsChecker::template kIsInvocable<T> &&
                      std::is_invocable_v<Fn, T>) {
          type_supported = true;
          using FnOutT = decltype(Fn{}(std::declval<T>()));
          using OutT =
              arolla::strip_optional_t<arolla::strip_statusor_t<FnOutT>>;
          if (output_dtype.has_value()) {
            if (output_dtype != schema::kObject &&
                arolla::GetQType<OutT>() != output_dtype->qtype()) {
              res = absl::FailedPreconditionError(
                  absl::StrFormat("Fn output type is not consistent with "
                                  "output schema: %s vs %s",
                                  arolla::GetQType<OutT>()->name(),
                                  output_dtype->qtype()->name()));
              return;
            }
          } else {
            output_dtype = schema_type == schema::kObject
                               ? schema::kObject
                               : schema::GetDType<OutT>();
          }
          internal::DataItem output_schema(*output_dtype);
          if (in.is_item()) {
            res = unary_op_impl::EvalScalar<Fn, T>(in.item(),
                                                   std::move(output_schema));
          } else {
            res = unary_op_impl::EvalSlice<Fn, T, OutT>(
                in, std::move(output_schema));
          }
        }
      });
  if (!type_supported) {
    RETURN_IF_ERROR(args_checker.CheckArgs(in));
    res = absl::InvalidArgumentError(
        absl::StrCat("unsupported input type: ", type->name()));
  }
  return res;
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_UNARY_OP_H_
