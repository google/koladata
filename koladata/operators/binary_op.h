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
#ifndef KOLADATA_OPERATORS_BINARY_OP_H_
#define KOLADATA_OPERATORS_BINARY_OP_H_

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/aggregation_ops_interface.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/view_types.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace binary_op_impl {

template <class T>
struct NextTypeToCast {
  using type = void;
};

template <>
struct NextTypeToCast<int32_t> {
  using type = int64_t;
};

template <>
struct NextTypeToCast<int64_t> {
  using type = float;
};

template <>
struct NextTypeToCast<float> {
  using type = double;
};

template <class From, class To>
struct IsCastable
    : std::conditional_t<
          std::is_same_v<From, To>, std::true_type,
          std::conditional_t<
              std::is_same_v<From, void>, std::false_type,
              IsCastable<typename NextTypeToCast<From>::type, To>>> {};

template <class T1, class T2>
using common_type_t =
    std::conditional_t<IsCastable<T1, T2>::value, T2,
                       std::conditional_t<IsCastable<T2, T1>::value, T1, void>>;

// `CastingAdapter<Fn>{}(T1, T2)` calls either `Fn{}(T1, T2)` (if such overload
// exists) or `Fn{}(common_type_t<T1, T2>, common_type_t<T1, T2>)` otherwise.
// `common_type_t` reimplements casting rules defined in
// `schema_internal::GetDTypeLattice`. GetDTypeLattice can't be used here
// directly because we need type deduction in compile time.
// TODO: try to reuse GetDTypeLattice.
template <class Fn>
struct CastingAdapter {
  template <class T1, class T2>
  static constexpr bool is_invocable_v =
      std::is_invocable_v<Fn, T1, T2> ||
      std::is_invocable_v<Fn, common_type_t<T1, T2>, common_type_t<T1, T2>>;

  template <class T1, class T2>
  auto operator()(const T1& v1, const T2& v2)
    requires(is_invocable_v<T1, T2>)
  {
    if constexpr (std::is_invocable_v<Fn, T1, T2>) {
      return Fn{}(v1, v2);
    } else {
      using CommonT = common_type_t<T1, T2>;
      return Fn{}(CommonT(v1), CommonT(v2));
    }
  }
};

template <class Fn, class T1, class T2>
using FnOutT = arolla::strip_statusor_t<decltype(CastingAdapter<Fn>{}(
    std::declval<T1>(), std::declval<T2>()))>;

template <class Fn, class T1, class T2>
static constexpr bool FnReturnsStatus =
    arolla::IsStatusOrT<decltype(CastingAdapter<Fn>{}(
        std::declval<T1>(), std::declval<T2>()))>::value;

template <class T>
absl::Status CheckType(const internal::DataItem& item) {
  if (item.has_value() && !item.holds_value<T>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("value doesn't match schema; expected %s, got %s",
                        arolla::GetQType<T>()->name(), item.dtype()->name()));
  }
  return absl::OkStatus();
}

template <class T>
absl::Status CheckType(const internal::DataSliceImpl& slice) {
  if (!slice.is_empty_and_unknown() && slice.dtype() != arolla::GetQType<T>()) {
    if (slice.is_mixed_dtype()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value doesn't match schema; expected %s, got mixed slice",
          arolla::GetQType<T>()->name()));
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("value doesn't match schema; expected %s, got %s",
                        arolla::GetQType<T>()->name(), slice.dtype()->name()));
  }
  return absl::OkStatus();
}

template <class T>
absl::Status CheckType(const DataSlice& slice) {
  return slice.VisitImpl([](const auto& imp) { return CheckType<T>(imp); });
}

template <class Fn, class T1, class T2>
std::conditional_t<FnReturnsStatus<Fn, T1, T2>,
                   absl::StatusOr<internal::DataItem>, internal::DataItem>
EvalScalar(const internal::DataItem& in1, const internal::DataItem& in2) {
  // Already checked by the caller
  DCHECK_OK(CheckType<T1>(in1));
  DCHECK_OK(CheckType<T2>(in2));

  if (!in1.has_value() || !in2.has_value()) {
    return internal::DataItem();
  }
  if constexpr (FnReturnsStatus<Fn, T1, T2>) {
    ASSIGN_OR_RETURN(auto res,
                     CastingAdapter<Fn>{}(in1.value<T1>(), in2.value<T2>()));
    return internal::DataItem(std::move(res));
  } else {
    return internal::DataItem(
        CastingAdapter<Fn>{}(in1.value<T1>(), in2.value<T2>()));
  }
}

template <class Fn, class T1, class T2>
absl::StatusOr<internal::DataSliceImpl> EvalSliceScalar(
    const internal::DataSliceImpl& slice, const internal::DataItem& scalar,
    bool swap_args) {
  // Already checked by the caller
  DCHECK(!slice.is_empty_and_unknown());
  DCHECK(scalar.has_value());
  DCHECK_OK(CheckType<T1>(slice));
  DCHECK_OK(CheckType<T2>(scalar));

  const arolla::DenseArray<T1>& arr = slice.values<T1>();
  arolla::view_type_t<T2> val = scalar.value<T2>();

  constexpr int flags =
      arolla::DenseOpFlags::kNoBitmapOffset |
      (arolla::IsRunOnMissingOp<Fn>::value ? arolla::DenseOpFlags::kRunOnMissing
                                           : 0);
  using OutT = FnOutT<Fn, T1, T2>;
  using ArrayOutT = arolla::DenseArray<OutT>;
  constexpr bool HasStatus = FnReturnsStatus<Fn, T1, T2>;
  std::conditional_t<HasStatus, absl::StatusOr<ArrayOutT>, ArrayOutT> res;
  if (swap_args) {
    auto fn = [&](arolla::view_type_t<T1> varr) {
      return CastingAdapter<Fn>{}(val, varr);
    };
    res = arolla::CreateDenseOp<flags, decltype(fn), OutT>(fn)(arr);
  } else {
    auto fn = [&](arolla::view_type_t<T1> varr) {
      return CastingAdapter<Fn>{}(varr, val);
    };
    res = arolla::CreateDenseOp<flags, decltype(fn), OutT>(fn)(arr);
  }
  if constexpr (HasStatus) {
    RETURN_IF_ERROR(res.status());
    return internal::DataSliceImpl::Create(std::move(*res));
  } else {
    return internal::DataSliceImpl::Create(std::move(res));
  }
}

template <class Fn, class OutT, class ParentT, class ChildT, bool SwapArgs>
class FnWithExpandAccumulator final
    : public arolla::Accumulator<arolla::AccumulatorType::kPartial, OutT,
                         arolla::meta::type_list<ParentT>,
                         arolla::meta::type_list<ChildT>> {
 public:
  void Reset(arolla::view_type_t<ParentT> pval) final {
    pval_ = pval;
  }

  void Add(arolla::view_type_t<ChildT> cval) final {
    if constexpr (SwapArgs) {
      ConsumeResult(CastingAdapter<Fn>{}(cval, pval_));
    } else {
      ConsumeResult(CastingAdapter<Fn>{}(pval_, cval));
    }
  }

  arolla::view_type_t<OutT> GetResult() final { return result_; }

  absl::Status GetStatus() final { return status_; }

 private:
  arolla::view_type_t<ParentT> pval_;
  OutT result_;
  absl::Status status_ = absl::OkStatus();

  void ConsumeResult(auto r) {
    if constexpr (arolla::IsStatusOrT<decltype(r)>::value) {
      if (r.ok()) {
        result_ = std::move(*r);
      } else if (status_.ok()) {
        status_ = r.status();
      }
    } else {
      result_ = std::move(r);
    }
  }
};

template <class Fn, class T1, class T2>
absl::StatusOr<internal::DataSliceImpl> EvalSlice(const DataSlice& in1,
                                                  const DataSlice& in2) {
  // Already checked by the caller
  DCHECK(!in1.is_item() || !in2.is_item());
  DCHECK_OK(CheckType<T1>(in1));
  DCHECK_OK(CheckType<T2>(in2));

  const auto& shape1 = in1.GetShape();
  const auto& shape2 = in2.GetShape();

  auto is_empty = [](const DataSlice& ds) {
    return ds.is_item() ? !ds.item().has_value()
                        : ds.slice().is_empty_and_unknown();
  };

  if (is_empty(in1) || is_empty(in2)) {
    size_t size = shape1.rank() > shape2.rank() ? shape1.size() : shape2.size();
    return internal::DataSliceImpl::CreateEmptyAndUnknownType(size);
  }

  internal::DataSliceImpl output_slice;
  if (in2.is_item()) {
    return EvalSliceScalar<Fn, T1, T2>(in1.slice(), in2.item(),
                                       /*swap_args=*/false);
  } else if (in1.is_item()) {
    return EvalSliceScalar<Fn, T2, T1>(in2.slice(), in1.item(),
                                       /*swap_args=*/true);
  }

  const arolla::DenseArray<T1>& arr1 = in1.slice().values<T1>();
  const arolla::DenseArray<T2>& arr2 = in2.slice().values<T2>();

  using OutT = FnOutT<Fn, T1, T2>;
  arolla::DenseArray<OutT> res_arr;

  if (shape1.rank() == shape2.rank()) {
    constexpr int flags = arolla::DenseOpFlags::kNoBitmapOffset |
                          (arolla::IsRunOnMissingOp<Fn>::value
                               ? arolla::DenseOpFlags::kRunOnMissing
                               : 0);
    auto fn = [](arolla::view_type_t<T1> v1, arolla::view_type_t<T2> v2) {
      return CastingAdapter<Fn>{}(v1, v2);
    };
    ASSIGN_OR_RETURN(
        res_arr,
        (arolla::CreateDenseOp<flags, decltype(fn), OutT>(fn)(arr1, arr2)));
  } else if (shape1.rank() < shape2.rank()) {
    arolla::DenseGroupOps<
        FnWithExpandAccumulator<Fn, OutT, T1, T2, /*SwapArgs=*/false>>
        agg(arolla::GetHeapBufferFactory());
    ASSIGN_OR_RETURN(res_arr,
                     agg.Apply(shape1.GetBroadcastEdge(shape2), arr1, arr2));
  } else {
    arolla::DenseGroupOps<
        FnWithExpandAccumulator<Fn, OutT, T2, T1, /*SwapArgs=*/true>>
        agg(arolla::GetHeapBufferFactory());
    ASSIGN_OR_RETURN(res_arr,
                     agg.Apply(shape2.GetBroadcastEdge(shape1), arr2, arr1));
  }

  return internal::DataSliceImpl::Create(std::move(res_arr));
}

template <class T1, class T2>
struct AllowAllArgs : std::true_type {};

inline absl::StatusOr<schema::DType> DefaultEmptyAndUnknownHandler(
    schema::DType t1, schema::DType t2) {
  if (t1 == schema::kObject || t2 == schema::kNone) {
    return t1;
  } else {
    return t2;
  }
}

inline absl::Status NoCheckArgs(const DataSlice&, const DataSlice&) {
  return absl::OkStatus();
}

}  // namespace binary_op_impl

template <
    class Fn,
    template <class, class> class IsInvocable = binary_op_impl::AllowAllArgs,
    class CheckArgsFn = decltype(binary_op_impl::NoCheckArgs),
    class EmptyAndUnknownHandler =
        decltype(binary_op_impl::DefaultEmptyAndUnknownHandler)>
absl::StatusOr<DataSlice> BinaryOpEval(
    const DataSlice& in1, const DataSlice& in2,
    CheckArgsFn check_args = binary_op_impl::NoCheckArgs,
    EmptyAndUnknownHandler empty_and_unknown_handler =
        binary_op_impl::DefaultEmptyAndUnknownHandler) {
  if (in1.GetShape().rank() < in2.GetShape().rank()
          ? !in1.GetShape().IsBroadcastableTo(in2.GetShape())
          : !in2.GetShape().IsBroadcastableTo(in1.GetShape())) {
    RETURN_IF_ERROR(check_args(in1, in2));
    return absl::InvalidArgumentError(absl::StrFormat(
        "shapes are not compatible: %v vs %v", arolla::Repr(in1.GetShape()),
        arolla::Repr(in2.GetShape())));
  }
  const DataSlice::JaggedShape& output_shape =
      in1.GetShape().rank() > in2.GetShape().rank() ? in1.GetShape()
                                                    : in2.GetShape();

  if (!in1.GetSchemaImpl().holds_value<schema::DType>() ||
      !in1.GetSchemaImpl().holds_value<schema::DType>()) {
    RETURN_IF_ERROR(check_args(in1, in2));
    return absl::InvalidArgumentError(
        absl::StrFormat("arguments must have primitive types, got %v, %v",
                        in1.GetSchemaImpl(), in2.GetSchemaImpl()));
  }
  schema::DType schema_type1 = in1.GetSchemaImpl().value<schema::DType>();
  schema::DType schema_type2 = in2.GetSchemaImpl().value<schema::DType>();

  arolla::QTypePtr type1 =
      schema_type1 == schema::kObject ? in1.dtype() : schema_type1.qtype();
  arolla::QTypePtr type2 =
      schema_type2 == schema::kObject ? in2.dtype() : schema_type2.qtype();

  if (type1 == arolla::GetNothingQType() ||
      type2 == arolla::GetNothingQType()) {
    RETURN_IF_ERROR(check_args(in1, in2));
    if (in1.impl_has_mixed_dtype() || in2.impl_has_mixed_dtype()) {
      return absl::InvalidArgumentError(
          "DataSlice with mixed types is not supported");
    }
    ASSIGN_OR_RETURN(schema::DType output_dtype,
                     empty_and_unknown_handler(schema_type1, schema_type2));
    if (output_shape.rank() == 0) {
      return DataSlice::UnsafeCreate(internal::DataItem(),
                                     internal::DataItem(output_dtype));
    } else {
      return DataSlice::UnsafeCreate(
          internal::DataSliceImpl::CreateEmptyAndUnknownType(
              output_shape.size()),
          output_shape, internal::DataItem(output_dtype));
    }
  }

  bool output_type_is_object =
      schema_type1 == schema::kObject || schema_type2 == schema::kObject;

  bool res_assigned = false;
  absl::StatusOr<DataSlice> res;
  arolla::meta::foreach_type(
      schema::supported_primitive_dtypes(), [&](auto tpe1) {
        using T1 = typename std::decay_t<decltype(tpe1)>::type;
        if (type1 != arolla::GetQType<T1>()) {
          return;
        }
        if (absl::Status st = binary_op_impl::CheckType<T1>(in1); !st.ok()) {
          res = st;
          return;
        }
        arolla::meta::foreach_type(
            schema::supported_primitive_dtypes(), [&](auto tpe2) {
              using T2 = typename std::decay_t<decltype(tpe2)>::type;
              if (type2 != arolla::GetQType<T2>()) {
                return;
              }
              if (absl::Status st = binary_op_impl::CheckType<T2>(in2);
                  !st.ok()) {
                res = st;
                return;
              }
              if constexpr (IsInvocable<T1, T2>::value &&
                            binary_op_impl::CastingAdapter<
                                Fn>::template is_invocable_v<T1, T2>) {
                // NOTE: here T1, T2 correspond to schema types
                internal::DataItem output_schema(
                    output_type_is_object
                        ? schema::kObject
                        : schema::GetDType<
                              binary_op_impl::FnOutT<Fn, T1, T2>>());
                if (in1.is_item() && in2.is_item()) {
                  if constexpr (binary_op_impl::FnReturnsStatus<Fn, T1, T2>) {
                    absl::StatusOr<internal::DataItem> output_value =
                        binary_op_impl::EvalScalar<Fn, T1, T2>(in1.item(),
                                                               in2.item());
                    if (output_value.ok()) {
                      res = DataSlice::UnsafeCreate(std::move(*output_value),
                                                    std::move(output_schema));
                    } else {
                      res = output_value.status();
                    }
                  } else {
                    res = DataSlice::UnsafeCreate(
                        binary_op_impl::EvalScalar<Fn, T1, T2>(in1.item(),
                                                               in2.item()),
                        std::move(output_schema));
                  }
                } else {
                  absl::StatusOr<internal::DataSliceImpl> output_slice =
                      binary_op_impl::EvalSlice<Fn, T1, T2>(in1, in2);
                  if (output_slice.ok()) {
                    res = DataSlice::UnsafeCreate(std::move(*output_slice),
                                                  output_shape,
                                                  std::move(output_schema));
                  } else {
                    res = output_slice.status();
                  }
                }
                res_assigned = true;
              }
            });
      });
  if (!res_assigned) {
    RETURN_IF_ERROR(check_args(in1, in2));
    res = absl::InvalidArgumentError(absl::StrFormat(
        "unsupported type combination: %s, %s", type1->name(), type2->name()));
  }
  return res;
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_BINARY_OP_H_
