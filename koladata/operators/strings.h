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
#ifndef KOLADATA_OPERATORS_STRINGS_H_
#define KOLADATA_OPERATORS_STRINGS_H_

#include <vector>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/qtype.h"
#include "absl/types/span.h"

namespace koladata::ops {

// kde.strings.format operator.
// Formats according to Python str.format.
// Has a lot of limitations.
// Must have two arguments:
// 1. First parameter is the format specification,
//    which must have BYTES or TEXT dtype.
// 2. Named tuple of DataSlice with format arguments.
class FormatOperatorFamily : public arolla::OperatorFamily {
  absl::StatusOr<arolla::OperatorPtr> DoGetOperator(
      absl::Span<const arolla::QTypePtr> input_types,
      arolla::QTypePtr output_type) const final;
};

// kde.strings._test_only_format_wrapper.
// It is used for koda_operator_coverage_test.
// It has compatible with arolla signature:
// format, arg_names, arg 1, arg 2, ...
absl::StatusOr<DataSlice> TestOnlyFormatWrapper(std::vector<DataSlice> slices);

// go/keep-sorted start ignore_prefixes=absl::StatusOr<DataSlice>
// kde.strings.agg_join.
absl::StatusOr<DataSlice> AggJoin(const DataSlice& x, const DataSlice& sep);
//
// kde.strings.contains.
absl::StatusOr<DataSlice> Contains(const DataSlice& x, const DataSlice& substr);
//
// kde.strings.count.
absl::StatusOr<DataSlice> Count(const DataSlice& x, const DataSlice& substr);
//
// kde.strings.find.
absl::StatusOr<DataSlice> Find(const DataSlice& x, const DataSlice& substr,
                               const DataSlice& start, const DataSlice& end,
                               const DataSlice& failure_value);
//
// kde.strings.join.
absl::StatusOr<DataSlice> Join(std::vector<DataSlice> slices);
//
// kde.strings.length.
absl::StatusOr<DataSlice> Length(const DataSlice& x);
//
// kde.strings.lower.
absl::StatusOr<DataSlice> Lower(const DataSlice& x);
//
// kde.strings.lstrip.
absl::StatusOr<DataSlice> Lstrip(const DataSlice& s, const DataSlice& chars);
//
// kde.strings.printf.
absl::StatusOr<DataSlice> Printf(std::vector<DataSlice> slices);
//
// kde.strings.replace.
absl::StatusOr<DataSlice> Replace(const DataSlice& s,
                                  const DataSlice& old_substr,
                                  const DataSlice& new_substr,
                                  const DataSlice& max_subs);
//
// kde.strings.rfind.
absl::StatusOr<DataSlice> Rfind(const DataSlice& x, const DataSlice& substr,
                                const DataSlice& start, const DataSlice& end,
                                const DataSlice& failure_value);
//
// kde.strings.rstrip.
absl::StatusOr<DataSlice> Rstrip(const DataSlice& s, const DataSlice& chars);
//
// kde.strings._split.
absl::StatusOr<DataSlice> Split(const DataSlice& x, const DataSlice& sep);
//
// kde.strings.strip.
absl::StatusOr<DataSlice> Strip(const DataSlice& s, const DataSlice& chars);
//
// kde.strings.substr.
absl::StatusOr<DataSlice> Substr(const DataSlice& x, const DataSlice& start,
                                 const DataSlice& end);
//
// kde.strings.upper.
absl::StatusOr<DataSlice> Upper(const DataSlice& x);
// go/keep-sorted end

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_STRINGS_H_
