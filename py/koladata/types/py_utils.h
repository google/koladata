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
#ifndef THIRD_PARTY_PY_KOLADATA_TYPES_PY_UTILS_H_
#define THIRD_PARTY_PY_KOLADATA_TYPES_PY_UTILS_H_

#include <Python.h>

#include <cstddef>
#include <initializer_list>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"

namespace koladata::python {

// Verifies `rhs` and returns it converted to a DataSlice. If
// `prohibit_boxing_to_multi_dim_slice` is true (shape of a left-hand side will
// be non-0 ranked), `rhs` may only be a DataSlice (or its subclass) instance or
// a Python value convertible to a DataItem (not a Python list or a dict).
// In case `rhs` is a complex object, such as Python list or
// dict, create Koda data is stored in `db`. In case the assignment cannot
// happen or converting `rhs` to DataSlice is not successful, appropriate error
// is returned.
absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    PyObject* rhs, bool prohibit_boxing_to_multi_dim_slice,
    const DataBagPtr& db, AdoptionQueue& adoption_queue);

// The same as above, but relies on JaggedShape and DataBag from `lhs_ds`, which
// has a meaning of assigning `rhs` to an `lhs_ds` object / entity.
absl::StatusOr<DataSlice> AssignmentRhsFromPyValue(
    const DataSlice& lhs_ds, PyObject* rhs, AdoptionQueue& adoption_queue);

// Converts PyObject* arguments to DataSlices with proper error reporting in the
// context of creating objects and entities. `db` is used to create lists /
// dicts from args.
absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, const std::vector<PyObject*>& args,
    AdoptionQueue& adoption_queue);

// Unwraps DataSlices from PyObject* arguments or throws an error if something
// other than a DataSlice is encountered.
absl::StatusOr<std::vector<DataSlice>> UnwrapDataSlices(
    const std::vector<PyObject*>& args);

// Same as above, but if `prohibit_boxing_to_multi_dim_slice` is true (meaning
// the LHS to which `args` will be assigned is a Slice and not an Item), `args`
// must not contain Python dicts or lists / tuples, etc.
absl::StatusOr<std::vector<DataSlice>> ConvertArgsToDataSlices(
    const DataBagPtr& db, bool prohibit_boxing_to_multi_dim_slice,
    const std::vector<PyObject*>& args, AdoptionQueue& adoption_queue);

// Abstraction that facilitates parsing of arguments passed to a Python function
// or method implemented in C Python and registered with FASTCALL | KEYWORD
// flags.
//
// Initialized with:
// * pos_only_n - number of positional only arguments;
// * parse_kwargs - whether keyword only arguments passed as **kwargs are parsed
//     into the result or unexpected keyword argument Error is reported.
// * kw_only_arg_names [optional] - initializer_list of keyword-only arguments.
// * vararg literal strings - names of positional-keyword arguments where their
//     order determines which argument name is used when arguments are passed by
//     position.
//
// Example usage:
//   static const absl::NoDestructor<FastcallArgParser> parser(
//       /*pos_only_n=*/2, /*parse_kwargs=*/false, "a", "b");
//   auto res = parser.Parse(py_args, nargs, py_kwnames);
//   if (res == nullptr) {
//     return nullptr;
//   }
//
//   // Access value for argument "a":
//   res->pos_kw_values[0];
class FastcallArgParser {
 public:
  struct Args {
    // TODO: Consider if all arguments should be stored in this
    // structure, including positional-only, as well. Although they are rare.
    std::vector<PyObject*> pos_kw_values;
    absl::flat_hash_map<absl::string_view, PyObject*> kw_only_args;
    std::vector<absl::string_view> kw_names;
    std::vector<PyObject*> kw_values;
  };

  // TODO: Consider deprecating `ArgNames` variadic signature and
  // support mutliple initializer_list signature.
  template <typename... ArgName>
  FastcallArgParser(size_t pos_only_n, bool parse_kwargs,
                    ArgName... pos_kw_arg_names)
      : pos_only_n_(pos_only_n),
        parse_kwargs_(parse_kwargs),
        pos_kw_to_pos_(ArgNames(pos_kw_arg_names...).to_pos) {}

  template <typename... ArgName>
  FastcallArgParser(size_t pos_only_n, bool parse_kwargs,
                    std::initializer_list<absl::string_view> kw_only_arg_names,
                    ArgName... pos_kw_arg_names)
      : pos_only_n_(pos_only_n),
        parse_kwargs_(parse_kwargs),
        pos_kw_to_pos_(ArgNames(pos_kw_arg_names...).to_pos),
        kw_only_arg_names_(kw_only_arg_names) {}

  // Parses the positional-keyword, keyword-only and variadic keyword arguments
  // for FASTCALL methods into FastcallArgParser::Args, which contains:
  // * pos_kw_values - values of positional-keyword arguments;
  // * kw_only_args - a map from argument names to their values for keyword-only
  //     arguments;
  // * kw_names - names of keyword arguments, present only if
  //     `FastcallArgParser` was initialized with parse_kwargs=true.
  // * kw_values - values of keyword arguments, present only if
  //     `FastcallArgParser` was initialized with parse_kwargs=true.
  //
  // NOTE: All values are collected as borrowed pointers to `PyObject`s or
  // nullptr, if they are missing (can happen only for positional-keyword). For
  // keyword-only arguments, the value is just missing for the argument name for
  // missing argument values.
  //
  // The method parses positional-keyword arguments by their names and position,
  // depending on how the Python caller specified them. If FastcallArgParser was
  // initialized with an initializer_list of keyword-only argument names, those
  // arguments will be parsed into a `kw_only_args` map. If FastcallArgParser
  // was initialized with `parse_kwargs=true`, the rest of the keyword arguments
  // are collected into `*kw_names` and `*kw_values`. Otherwise,
  // unexpected-keyword error is raised. Positional-only arguments are ignored,
  // while for missing arguments, the caller should decide after calling this
  // method if they are optional or mandatory.
  //
  // `py_args`, `nargs` and `py_kwnames` should just be passed down from the
  // method / functions arguments that are registered as FASTCALL | KEYWORDS.
  //
  // If `nargs` < number of positional-only arguments or `nargs` is larger than
  // the total number of expected positional arguments, an appropriate Error is
  // set.
  //
  // Returns true if Python arguments were collected into `args` and false in
  // case of an error, in which case appropriate Python error is set.
  bool Parse(PyObject* const* py_args, Py_ssize_t nargs, PyObject* py_kwnames,
             Args& args) const;

 private:
  static constexpr size_t kKwargsVectorCapacity = 8;

  // A helper struct that allows us to build a mapping from arg_names to their
  // position in which they were listed in the constructor. It is safer compared
  // to users writing the mapping themselves, which can cause errors.
  //
  // Example:
  //   ArgNames arg_names("a", "b", "c");
  //   arg_names.to_pos;  # Returns a mapping ("a" -> 0, "b" -> 1, "c" -> 2)
  struct ArgNames {
    const absl::flat_hash_map<absl::string_view, size_t> to_pos;

    template <typename... ArgName>
    explicit ArgNames(ArgName... arg_names) :
      ArgNames(std::make_index_sequence<sizeof...(arg_names)>{},
               std::forward<ArgName>(arg_names)...) {}

    template <std::size_t... Is, typename... ArgName>
    ArgNames(std::index_sequence<Is...>, ArgName... arg_names) :
      to_pos{{arg_names, Is}...} {}
  };

  size_t pos_only_n_;
  bool parse_kwargs_ = false;
  // NOTE: Safe to use `absl::string_view` as those are literals and set in the
  // same scope in which this function is called.
  const absl::flat_hash_map<absl::string_view, size_t> pos_kw_to_pos_;
  const absl::flat_hash_set<absl::string_view> kw_only_arg_names_;
};

/****************** Utility functions for fetching arguments ******************/

// NOTE: The following utility functions fetch PyObject* argument values either
// by `arg_pos` for positional-keyword arguments or by `arg_name` for
// keyword-only arguments.

// Populates `arg` output argument if `args` contain a valid argument at
// position `arg_pos`. Returns true on success, false on error, in which case it
// also sets Python Exception.
//
// Returned string_view is valid as long as `args` are not deallocated.
bool ParseUnicodeArg(const FastcallArgParser::Args& args, size_t arg_pos,
                     absl::string_view arg_name_for_error,
                     absl::string_view& arg);

// Populates `arg` DataSlice output argument if `args` contain a valid argument
// named `arg_name`. Returns true on success, false on error, in which case it
// also sets Python Exception.
bool ParseDataSliceArg(const FastcallArgParser::Args& args,
                       absl::string_view arg_name,
                       std::optional<DataSlice>& arg);

// Verifies that argument at position `arg_pos` is present in parsed `args` and
// stores this value into `arg`. If not present at the correct position or not a
// boolean, sets Python exception and returns false. On success, returns
// true.
bool ParseBoolArg(const FastcallArgParser::Args& args, size_t arg_pos,
                  absl::string_view arg_name_for_error, bool& arg);

// Populates `arg` boolean output argument if `args` contain a valid argument
// named `arg_name`. Returns true on success, false on error, in which case it
// also sets Python Exception.
bool ParseBoolArg(const FastcallArgParser::Args& args,
                  absl::string_view arg_name, bool& arg);

}  // namespace koladata::python

#endif  // THIRD_PARTY_PY_KOLADATA_TYPES_PY_UTILS_H_
