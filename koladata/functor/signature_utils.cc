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
#include "koladata/functor/signature_utils.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/text.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/signature.h"
#include "koladata/signature_storage.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

absl::StatusOr<DataSlice> MakeParameterKindConstant(absl::string_view name) {
  ASSIGN_OR_RETURN(auto name_attr,
                   DataSlice::Create(internal::DataItem(arolla::Text(name)),
                                     internal::DataItem(schema::kString)));
  ASSIGN_OR_RETURN(auto res,
                   CreateUu(DataBag::EmptyMutable(), "__parameter_kind__",
                            {"kind"}, {std::move(name_attr)}));
  return res.FreezeBag();
}

absl::StatusOr<DataSlice> MakeNoDefaultValueMarker() {
  ASSIGN_OR_RETURN(auto present,
                   DataSlice::Create(internal::DataItem(arolla::kPresent),
                                     internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(
      auto res,
      CreateUu(DataBag::EmptyMutable(), "__parameter_no_default_value__",
               {kNoDefaultValueParameterField}, {std::move(present)}));
  return res.FreezeBag();
}

}  // namespace

absl::StatusOr<std::vector<arolla::TypedValue>> BindArguments(
    const Signature& signature, absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames, DataBagPtr default_values_db) {
  if (args.size() < kwnames.size()) {
    return absl::InvalidArgumentError("args.size < kwnames.size()");
  }
  const size_t kwargs_offset = args.size() - kwnames.size();

  const auto& parameters = signature.parameters();
  const auto& keyword_parameter_index = signature.keyword_parameter_index();
  std::vector<arolla::TypedValue> bound_arguments(
      parameters.size(), arolla::TypedValue::UnsafeFromTypeDefaultConstructed(
                             arolla::GetNothingQType()));

  std::vector<arolla::TypedRef> unknown_args;
  std::vector<std::string> unknown_kwarg_names;
  std::vector<arolla::TypedRef> unknown_kwarg_values;

  // Process positional arguments.
  for (size_t i = 0; i < kwargs_offset; ++i) {
    if (i >= parameters.size() ||
        (parameters[i].kind != Signature::Parameter::Kind::kPositionalOnly &&
         parameters[i].kind !=
             Signature::Parameter::Kind::kPositionalOrKeyword)) {
      unknown_args.push_back(args[i]);
    } else {
      bound_arguments[i] = arolla::TypedValue(args[i]);
    }
  }

  // Process keyword arguments.
  for (size_t i = kwargs_offset; i < args.size(); ++i) {
    const auto& name = kwnames[i - kwargs_offset];
    const auto& value = args[i];
    auto it = keyword_parameter_index.find(name);
    if (it == keyword_parameter_index.end()) {
      unknown_kwarg_names.push_back(name);
      unknown_kwarg_values.push_back(value);
    } else {
      if (bound_arguments[it->second].GetType() != arolla::GetNothingQType()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("parameter [%s] specified twice", name));
      }
      bound_arguments[it->second] = arolla::TypedValue(value);
    }
  }

  // Handle variadic parameters.
  for (size_t i = 0; i < parameters.size(); ++i) {
    const auto& parameter = parameters[i];
    if (parameter.kind == Signature::Parameter::Kind::kVarPositional) {
      bound_arguments[i] = arolla::MakeTuple(unknown_args);
      unknown_args.clear();
    } else if (parameter.kind == Signature::Parameter::Kind::kVarKeyword) {
      ASSIGN_OR_RETURN(
          bound_arguments[i],
          arolla::MakeNamedTuple(unknown_kwarg_names, unknown_kwarg_values));
      unknown_kwarg_names.clear();
      unknown_kwarg_values.clear();
    }
  }
  if (!unknown_args.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("too many positional arguments passed (%d extra)",
                        unknown_args.size()));
  }
  if (!unknown_kwarg_names.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unknown keyword arguments: [%s]",
                        absl::StrJoin(unknown_kwarg_names, ", ")));
  }

  // Handle default values. This makes sure we do not return the auxiliary
  // values with NothingQType back to the user.
  for (size_t i = 0; i < parameters.size(); ++i) {
    if (bound_arguments[i].GetType() == arolla::GetNothingQType()) {
      const auto& parameter = parameters[i];
      if (parameter.default_value.has_value()) {
        if (default_values_db != nullptr) {
          bound_arguments[i] = arolla::TypedValue::FromValue(
              parameter.default_value->WithBag(default_values_db));
        } else {
          bound_arguments[i] =
              arolla::TypedValue::FromValue(*parameter.default_value);
        }
      } else {
        return absl::InvalidArgumentError(
            absl::StrFormat("no value provided for %v parameter [%s]",
                            parameter.kind, parameter.name));
      }
    }
  }

  return bound_arguments;
}

const DataSlice& PositionalOnlyParameterKind() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{
      *MakeParameterKindConstant(kPositionalOnlyParameterName)};
  return *val;
}

const DataSlice& PositionalOrKeywordParameterKind() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{
      *MakeParameterKindConstant(kPositionalOrKeywordParameterName)};
  return *val;
}

const DataSlice& VarPositionalParameterKind() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{
      *MakeParameterKindConstant(kVarPositionalParameterName)};
  return *val;
}

const DataSlice& KeywordOnlyParameterKind() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{
      *MakeParameterKindConstant(kKeywordOnlyParameterName)};
  return *val;
}

const DataSlice& VarKeywordParameterKind() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{
      *MakeParameterKindConstant(kVarKeywordParameterName)};
  return *val;
}

const DataSlice& NoDefaultValueMarker() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<DataSlice> val{*MakeNoDefaultValueMarker()};
  return *val;
}

absl::StatusOr<DataSlice> ParameterKindToKoda(Signature::Parameter::Kind kind) {
  using enum Signature::Parameter::Kind;
  switch (kind) {
    case kPositionalOnly:
      return PositionalOnlyParameterKind();
    case kPositionalOrKeyword:
      return PositionalOrKeywordParameterKind();
    case kVarPositional:
      return VarPositionalParameterKind();
    case kKeywordOnly:
      return KeywordOnlyParameterKind();
    case kVarKeyword:
      return VarKeywordParameterKind();
  }
  return absl::InternalError("unknown parameter kind");
}

absl::StatusOr<DataSlice> CppSignatureToKodaSignature(
    const Signature& signature) {
  std::vector<internal::DataItem> koda_parameters;
  auto db = DataBag::EmptyMutable();
  AdoptionQueue adoption_queue;
  for (const auto& param : signature.parameters()) {
    ASSIGN_OR_RETURN(auto kind, ParameterKindToKoda(param.kind));
    ASSIGN_OR_RETURN(
        auto name,
        DataSlice::Create(internal::DataItem(arolla::Text(param.name)),
                          internal::DataItem(schema::kString)));
    DataSlice default_value;
    if (param.default_value.has_value()) {
      default_value = *param.default_value;
    } else {
      default_value = NoDefaultValueMarker();
    }
    adoption_queue.Add(kind);
    adoption_queue.Add(default_value);
    ASSIGN_OR_RETURN(
        auto koda_param,
        ObjectCreator::FromAttrs(
            db, {"kind", "name", "default_value"},
            {std::move(kind), std::move(name), std::move(default_value)}));
    koda_parameters.push_back(koda_param.item());
  }
  ASSIGN_OR_RETURN(
      auto koda_parameters_slice,
      DataSlice::Create(
          internal::DataSliceImpl::Create(koda_parameters),
          DataSlice::JaggedShape::FlatFromSize(koda_parameters.size()),
          internal::DataItem(schema::kObject), db));
  ASSIGN_OR_RETURN(auto koda_parameter_list,
                   CreateListsFromLastDimension(db, koda_parameters_slice));
  ASSIGN_OR_RETURN(auto koda_signature,
                   ObjectCreator::FromAttrs(db, {"parameters"},
                                            {std::move(koda_parameter_list)}));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));
  return koda_signature.UnsafeMakeWholeOnImmutableDb();
}

namespace {

const Signature& CppArgsKwargsSignature() {
  static absl::NoDestructor<Signature> val(
      Signature::Create(
          {Signature::Parameter{
               .name = "args",
               .kind = Signature::Parameter::Kind::kVarPositional},
           Signature::Parameter{
               .name = "kwargs",
               .kind = Signature::Parameter::Kind::kVarKeyword}})
          .value());
  return *val;
}
}  // namespace

const DataSlice& KodaArgsKwargsSignature() {
  static absl::NoDestructor<DataSlice> val{
      CppSignatureToKodaSignature(CppArgsKwargsSignature()).value()};
  return *val;
}

}  // namespace koladata::functor
