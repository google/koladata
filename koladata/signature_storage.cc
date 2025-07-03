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
#include "koladata/signature_storage.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/signature.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::functor {

namespace {

internal::DataItem MakeParameterKindUuid(absl::string_view name) {
  auto data_item = internal::DataItem(arolla::Text(name));
  return internal::CreateUuidFromFields(
      "__parameter_kind__", {"kind"}, {std::cref(data_item)});
}

const internal::DataItem& PositionalOnlyParameterKindUuid() {
  static absl::NoDestructor<internal::DataItem> val{
      MakeParameterKindUuid(kPositionalOnlyParameterName)};
  return *val;
}

const internal::DataItem& PositionalOrKeywordParameterKindUuid() {
  static absl::NoDestructor<internal::DataItem> val{
      MakeParameterKindUuid(kPositionalOrKeywordParameterName)};
  return *val;
}

const internal::DataItem& VarPositionalParameterKindUuid() {
  static absl::NoDestructor<internal::DataItem> val{
      MakeParameterKindUuid(kVarPositionalParameterName)};
  return *val;
}

const internal::DataItem& KeywordOnlyParameterKindUuid() {
  static absl::NoDestructor<internal::DataItem> val{
      MakeParameterKindUuid(kKeywordOnlyParameterName)};
  return *val;
}

const internal::DataItem& VarKeywordParameterKindUuid() {
  static absl::NoDestructor<internal::DataItem> val{
      MakeParameterKindUuid(kVarKeywordParameterName)};
  return *val;
}

internal::DataItem MakeNoDefaultValueMarkerUuid() {
  auto data_item = internal::DataItem(arolla::kPresent);
  return internal::CreateUuidFromFields("__parameter_no_default_value__",
                                        {kNoDefaultValueParameterField},
                                        {std::cref(data_item)});
}

const internal::DataItem& NoDefaultValueMarkerUuid() {
  // No errors are possible here, so we ignore status.
  static absl::NoDestructor<internal::DataItem> val{
      MakeNoDefaultValueMarkerUuid()};
  return *val;
}

absl::StatusOr<Signature::Parameter::Kind> KodaToParameterKind(
    const DataSlice& kind) {
  using enum Signature::Parameter::Kind;
  if (!kind.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("kind must be a data item, but has shape: %s",
                        arolla::Repr(kind.GetShape())));
  }
  if (kind.item() == PositionalOnlyParameterKindUuid()) {
    return kPositionalOnly;
  }
  if (kind.item() == PositionalOrKeywordParameterKindUuid()) {
    return kPositionalOrKeyword;
  }
  if (kind.item() == VarPositionalParameterKindUuid()) {
    return kVarPositional;
  }
  if (kind.item() == KeywordOnlyParameterKindUuid()) {
    return kKeywordOnly;
  }
  if (kind.item() == VarKeywordParameterKindUuid()) {
    return kVarKeyword;
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("unknown parameter kind: [%s]", DataSliceRepr(kind)));
}

}  // namespace

absl::StatusOr<Signature> KodaSignatureToCppSignature(
    const DataSlice& signature, bool detach_default_values_db) {
  if (!signature.is_item()) {
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
  ASSIGN_OR_RETURN(auto list_size, ListSize(parameter_list));
  if (!list_size.is_item() || !list_size.item().holds_value<int64_t>()) {
    return absl::InternalError("ListSize did not return an int64_t scalar");
  }
  auto list_size_val = list_size.item().value<int64_t>();
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
    if (default_value.item() != NoDefaultValueMarkerUuid()) {
      default_value_opt = default_value;
    }
    if (detach_default_values_db && default_value_opt &&
        default_value_opt->GetBag() != nullptr) {
      default_value_opt = default_value_opt->WithBag(nullptr);
    }
    res.push_back(Signature::Parameter{.name = std::string(name_str),
                                       .kind = kind_enum,
                                       .default_value = default_value_opt});
  }
  return Signature::Create(res);
}

}  // namespace koladata::functor
