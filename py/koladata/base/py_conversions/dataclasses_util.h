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
#ifndef PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
#define PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_

#include <Python.h>

#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/fingerprint.h"
#include "py/arolla/py_utils/py_utils.h"

namespace koladata::python {

// Util class for working with Python dataclasses (getting attributes, making
// instances, etc.) It also owns the dataclass object instances it creates.
class DataClassesUtil {
 public:
  struct AttrResult {
    std::vector<std::string> attr_names;
    std::vector<PyObject*> values;
  };

  DataClassesUtil() = default;

  absl::StatusOr<arolla::python::PyObjectPtr> MakeDataClassInstance(
      absl::Span<const absl::string_view> attr_names);

  // Returns attribute names and values on success and if `py_obj` represents a
  // Python object for which parsing attributes is supported (e.g. dataclasses).
  // Returned values are borrowed references owned by DataClassesUtil. If the
  // result is `std::nullopt`, the `py_obj` cannot provide attributes.
  absl::StatusOr<std::optional<AttrResult>> GetAttrNamesAndValues(
      PyObject* py_obj);

  // Get the values of object's attributes for corresponding attr_names.
  // Returned values are borrowed references owned by DataClassesUtil.
  absl::StatusOr<std::vector<PyObject*>> GetAttrValues(
      PyObject* py_obj, absl::Span<const absl::string_view> sorted_attr_names);

  // Returns the type of the given field in the class.
  // If the class is a dataclass, the type of the field is returned.
  // If the class field is a SimpleNamespace, the SimpleNamespace class is
  // returned.
  // - `list[SomeType]` and `attr_name==__items__` -> SomeType is returned.
  // - `dict[OneType, SecondType]`:
  //   - 'attr_name == '__keys__' -> OneType is returned.
  //   - 'attr_name == '__values__' -> SecondType is returned.
  // In all other cases, an error is returned.
  // `for_primitive` indicates whether the caller's intention is to use it for a
  // primitive type. This is added to support the SimpleNamespace case: its
  // fields are not typed, so with `for_primitive=True` the type of the field is
  // considered to be `None`, so that any type can be assigned to it; otherwise
  // it will be `SimpleNamespace`.
  absl::StatusOr<arolla::python::PyObjectPtr> GetClassFieldType(
      arolla::python::PyObjectPtr absl_nonnull py_class,
      absl::string_view attr_name, bool for_primitive = false);

  // Returns true if the given attribute exists in the dataclass and it is
  // optional, i.e. `SomeType | None` or `Optional[SomeType]`.
  // If the attribute is not present, returns False
  // If the attribute is present but cannot be assigned None, returns False.
  absl::StatusOr<bool> HasOptionalField(arolla::python::PyObjectPtr
                                        absl_nonnull py_class,
                                        absl::string_view attr_name);

  // Creates a class instance with the given attributes and values.
  // The `attr_names` and `attr_values` should have the same size.
  absl::StatusOr<arolla::python::PyObjectPtr> CreateClassInstanceKwargs(
      arolla::python::PyObjectPtr absl_nonnull py_class,
      absl::Span<const std::string> attr_names,
      absl::Span<const arolla::python::PyObjectPtr absl_nonnull> attr_values);

  // Creates a class instance with the given args.
  // Returned value is owned by DataClassesUtil.
  absl::StatusOr<arolla::python::PyObjectPtr> CreateClassInstanceArgs(
      arolla::python::PyObjectPtr absl_nonnull py_class,
      absl::Span<const arolla::python::PyObjectPtr absl_nonnull> args);

  // Returns a new reference to the SimpleNamespace class.
  absl::StatusOr<arolla::python::PyObjectPtr> GetSimpleNamespaceClass();

 private:
  absl::StatusOr<arolla::python::PyObjectPtr> MakeDataClass(
      absl::Span<const absl::string_view> attr_names);

  absl::Status InitFns();

  absl::flat_hash_map<arolla::Fingerprint, arolla::python::PyObjectPtr>
      dataclasses_cache_;

  // Cache (dict) for type hints.
  // The key is a class and the value is a dict of attribute names to types.
  // This is used to avoid looking up the same type hints multiple times for the
  // same class.
  arolla::python::PyObjectPtr type_hints_cache_;

  arolla::python::PyObjectPtr fn_make_dataclass_;
  arolla::python::PyObjectPtr fn_fields_;
  arolla::python::PyObjectPtr fn_get_class_field_type_;
  arolla::python::PyObjectPtr fn_has_optional_field_;

  arolla::python::PyObjectPtr simple_namespace_class_;
  bool fns_initialized_ = false;

  // DataClasses returns borrowed references to the client, so it needs to own
  // them as it got new references from dataclass object.
  std::deque<arolla::python::PyObjectPtr> owned_values_;
};

}  // namespace koladata::python

#endif  // PY_KOLADATA_PY_CONVERSIONS_DATACLASSES_UTIL_H_
