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
#ifndef KOLADATA_FUNCTOR_AUTO_VARIABLES_H_
#define KOLADATA_FUNCTOR_AUTO_VARIABLES_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "arolla/util/fingerprint.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Returns a functor with additional variables extracted. More specifically,
// for each node (subexpression) in each expression in the functor, we may
// replace it with a variable V.new_name, and add a new_name variable equal to
// the replaced node. The resulting functor is expected evaluate to the same
// value as the original functor, modulo the literal DataSlice DataBag merging
// described below.
//
// The following nodes are replaced with variables:
//   1) All named nodes
//   2) literal DataSlices (except scalar primitives)
//   3) Nodes with fingerprints listed in `extra_nodes_to_extract`.
//   4) Additional nodes that need to be replaced with variables to avoid
//      duplicating the computations: those nodes that appear as sub-nodes in 2+
//      nodes that need to become variables according to previous rules, except
//      nodes which are just a literal, leaf or input.
//
// In most cases, when we create a new variable, we give it an auto-generated
// name like "_aux_57", but there are exceptions to that:
// 1) If a node is wrapped with a name annotation, we use the name from the
//    annotation instead, and strip the name annotation from the expression
//    for the new variable. This is done to help readability and manipulation
//    of the resulting functor.
// 2) If the name from the annotation is already taken, we append a counter
//    to the name until we find a name that is not taken.
//
// Note that for literal DataSlices, instead of creating a new variable
// with the value containing the literal, we create a new variable containing
// the literal DataSlice itself, without wrapping it in an ExprQuote.
// This has the following consequences:
// 1) If the DataSlice is not a DataItem, we first implode it to a DataItem,
//    and we put kd.explode(V.new_name, ndim=...) instead of just V.new_name
//    in the containing expression.
// 2) The DataSlice is adopted into the DataBag of the resulting functor, which
//    in turn means:
//    a) The corresponding V.new_name variable will evaluate to a DataSlice
//       with the functor DataBag attached, and not the original DataBag that
//       was there in the literal.
//    b) There may be conflicts if two such literals have different values
//       for an attribute of the same object. An exception is raised in this
//       case, similar to the behavior of kd.obj().
//    c) If two literals have non-conflicting but different attributes for the
//       same object, in the resulting functor both will have all merged
//       attributes. In particular, if you have both "obj" and "obj.stub()"
//       literals, then "obj.stub()" will effectively become "obj" as well.
// 3) Assuming the sub-functors, if any, were also processed by AutoVariables,
//    and assuming we have only literal DataSlices, but no literal DataBags,
//    no literal tuples of DataSlices or other similar containers, then the
//    resulting functor will not have pointers to any other DataBag than its
//    own, and we won't have chains like DataBag->ExprQuote->DataBag->...
//    This simplifies user manipulations of the resulting functor.
//
// If a literal DataSlice is also wrapped with a name annotation, we will
// create a variable with the name from the annotation instead of an
// auto-generated name.
//
// If a subexpression is a result of kd.lazy.slice(...) or kd.lazy.item(...)
// binding (and therefore has a literal DataSlice inside additional nodes such
// as kd.lazy.slice or the source location annotations), we will remove the
// wrapping no-op nodes, so that we can then find the wrapping name annotation.
// This allows kd.slice(...).with_name('my_name') to be the canonical recipe
// for creating a variable with a specific name equal to a specific DataItem
// when tracing.
//
// If a literal DataSlice is also listed in `extra_nodes_to_extract`, we will
// make sure we also create another variable for the created `kd.explode`
// expression, if any, to guarantee that all nodes listed in
// `extra_nodes_to_extract` become variables.
//
// NOTE: Tests are in
// py/koladata/functor/auto_variables_test.py
//
absl::StatusOr<DataSlice> AutoVariables(
    const DataSlice& functor,
    absl::flat_hash_set<arolla::Fingerprint> extra_nodes_to_extract = {});

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_AUTO_VARIABLES_H_
