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
#include "koladata/functor/signature_storage.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/functor/signature.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/list.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

absl::StatusOr<DataSlice> MakeParameterKindConstant(absl::string_view name) {
  ASSIGN_OR_RETURN(auto name_attr,
                   DataSlice::Create(internal::DataItem(arolla::Text(name)),
                                     internal::DataItem(schema::kText)));
  ASSIGN_OR_RETURN(auto res, CreateUu(DataBag::Empty(), "__parameter_kind__",
                                      {"kind"}, {name_attr}));
  return res.Freeze();
}

absl::StatusOr<DataSlice> MakeNoDefaultValueMarker() {
  ASSIGN_OR_RETURN(auto present,
                   DataSlice::Create(internal::DataItem(arolla::kPresent),
                                     internal::DataItem(schema::kMask)));
  ASSIGN_OR_RETURN(auto res,
                   CreateUu(DataBag::Empty(), "__parameter_no_default_value__",
                            {"no_default_value"}, {present}));
  return res.Freeze();
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

absl::StatusOr<Signature::Parameter::Kind> KodaToParameterKind(
    const DataSlice& kind) {
  using enum Signature::Parameter::Kind;
  if (kind.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("kind must be a data item, but has shape: %s",
                        arolla::Repr(kind.GetShape())));
  }
  ASSIGN_OR_RETURN(auto positional_only, PositionalOnlyParameterKind());
  if (kind.item() == positional_only.item()) {
    return kPositionalOnly;
  }
  ASSIGN_OR_RETURN(auto positional_or_keyword,
                   PositionalOrKeywordParameterKind());
  if (kind.item() == positional_or_keyword.item()) {
    return kPositionalOrKeyword;
  }
  ASSIGN_OR_RETURN(auto var_positional, VarPositionalParameterKind());
  if (kind.item() == var_positional.item()) {
    return kVarPositional;
  }
  ASSIGN_OR_RETURN(auto keyword_only, KeywordOnlyParameterKind());
  if (kind.item() == keyword_only.item()) {
    return kKeywordOnly;
  }
  ASSIGN_OR_RETURN(auto var_keyword, VarKeywordParameterKind());
  if (kind.item() == var_keyword.item()) {
    return kVarKeyword;
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("unknown parameter kind: [%s]", arolla::Repr(kind)));
}

}  // namespace

absl::StatusOr<DataSlice> CppSignatureToKodaSignature(
    const Signature& signature) {
  std::vector<internal::DataItem> koda_parameters;
  auto db = DataBag::Empty();
  AdoptionQueue adoption_queue;
  for (const auto& param : signature.parameters()) {
    ASSIGN_OR_RETURN(auto kind, ParameterKindToKoda(param.kind));
    ASSIGN_OR_RETURN(
        auto name,
        DataSlice::Create(internal::DataItem(arolla::Text(param.name)),
                          internal::DataItem(schema::kText)));
    DataSlice default_value;
    if (param.default_value.has_value()) {
      default_value = param.default_value.value();
    } else {
      ASSIGN_OR_RETURN(default_value, NoDefaultValueMarker());
    }
    adoption_queue.Add(kind);
    adoption_queue.Add(default_value);
    ASSIGN_OR_RETURN(auto koda_param, ObjectCreator::FromAttrs(
                                          db, {"kind", "name", "default_value"},
                                          {kind, name, default_value}));
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
  ASSIGN_OR_RETURN(
      auto koda_signature,
      ObjectCreator::FromAttrs(db, {"parameters"}, {koda_parameter_list}));
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));
  return koda_signature.Freeze();
}

absl::StatusOr<Signature> KodaSignatureToCppSignature(
    const DataSlice& signature) {
  if (signature.GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("signature must be a data item, but has shape: %s",
                        arolla::Repr(signature.GetShape())));
  }
  if (!signature.present_count()) {
    return absl::InvalidArgumentError("signature is missing");
  }
  ASSIGN_OR_RETURN(auto parameter_list, signature.GetAttr("parameters"));
  if (!parameter_list.present_count()) {
    return absl::InvalidArgumentError("parameters are missing");
  }
  ASSIGN_OR_RETURN(auto list_size, ops::ListSize(parameter_list));
  if (list_size.GetShape().rank() != 0 ||
      !list_size.item().holds_value<int64_t>()) {
    return absl::InternalError("ListSize did not return an int64_t scalar");
  }
  auto list_size_val = list_size.item().value<int64_t>();
  ASSIGN_OR_RETURN(auto no_default_value, NoDefaultValueMarker());
  std::vector<Signature::Parameter> res;
  for (int64_t i = 0; i < list_size_val; ++i) {
    ASSIGN_OR_RETURN(auto param_as_1d, parameter_list.ExplodeList(i, i + 1));
    ASSIGN_OR_RETURN(auto param,
                     param_as_1d.Reshape(DataSlice::JaggedShape::Empty()));
    if (!param.present_count()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("parameter %d is missing", i));
    }
    ASSIGN_OR_RETURN(auto name, param.GetAttr("name"));
    if (!name.item().holds_value<arolla::Text>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("parameter %d does not have a text name", i));
    }
    auto name_str = name.item().value<arolla::Text>().view();
    ASSIGN_OR_RETURN(auto kind, param.GetAttr("kind"));
    ASSIGN_OR_RETURN(auto kind_enum, KodaToParameterKind(kind));
    ASSIGN_OR_RETURN(auto default_value, param.GetAttr("default_value"));
    std::optional<DataSlice> default_value_opt = std::nullopt;
    if (default_value.item() != no_default_value.item()) {
      default_value_opt = default_value;
    }
    res.push_back(Signature::Parameter{.name = std::string(name_str),
                                       .kind = kind_enum,
                                       .default_value = default_value_opt});
  }
  return Signature::Create(res);
}

const absl::StatusOr<DataSlice>& PositionalOnlyParameterKind() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeParameterKindConstant("positional_only")};
  return *val;
}

const absl::StatusOr<DataSlice>& PositionalOrKeywordParameterKind() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeParameterKindConstant("positional_or_keyword")};
  return *val;
}

const absl::StatusOr<DataSlice>& VarPositionalParameterKind() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeParameterKindConstant("var_positional")};
  return *val;
}

const absl::StatusOr<DataSlice>& KeywordOnlyParameterKind() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeParameterKindConstant("keyword_only")};
  return *val;
}

const absl::StatusOr<DataSlice>& VarKeywordParameterKind() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeParameterKindConstant("var_keyword")};
  return *val;
}

const absl::StatusOr<DataSlice>& NoDefaultValueMarker() {
  static absl::NoDestructor<absl::StatusOr<DataSlice>> val{
      MakeNoDefaultValueMarker()};
  return *val;
}

}  // namespace koladata::functor
