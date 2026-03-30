# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contrib operators."""

from arolla import arolla
from koladata import kd

P = arolla.P
to_arolla_int64 = arolla.abc.lookup_operator('koda_internal.to_arolla_int64')


@kd.optools.as_backend_operator('kd_ext.contrib._flatten_cyclic_references')
def _flatten_cyclic_references(x, max_recursion_depth, non_deterministic):  # pylint: disable=unused-argument
  """Creates a DataSlice with tree-like copy of the input DataSlice."""
  raise NotImplementedError('implemented in the backend')


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator(
    'kd_ext.contrib.flatten_cyclic_references',
    qtype_constraints=[
        kd.optools.constraints.expect_data_slice(P.x),
        kd.optools.constraints.expect_data_slice(P.max_recursion_depth),
    ],
)
def flatten_cyclic_references(x, *, max_recursion_depth):  # pylint: disable=unused-argument
  """Creates a DataSlice with tree-like copy of the input DataSlice.

  The entities themselves and all their attributes including both top-level and
  non-top-level attributes are cloned (with new ItemIds) while creating the
  tree-like copy. The max_recursion_depth argument controls the maximum number
  of times the same entity can occur on the path from the root to a leaf.
  Note: resulting DataBag might have an exponential size, compared to the input
  DataBag.

  Args:
    x: DataSlice to flatten.
    max_recursion_depth: Maximum recursion depth.

  Returns:
    A DataSlice with tree-like attributes structure.
  """
  max_recursion_depth = to_arolla_int64(max_recursion_depth)
  return _flatten_cyclic_references(
      P.x, max_recursion_depth, kd.eager.optools.unified_non_deterministic_arg()
  )


@kd.optools.add_to_registry(via_cc_operator_package=True)
@kd.optools.as_lambda_operator('kd_ext.contrib.value_counts')
def value_counts(x):
  """Returns Dicts mapping entries in `x` to their count over the last dim.

  Similar to Pandas' `value_counts`.

  The output is a `x.get_ndim() - 1`-dimensional DataSlice containing one
  Dict per aggregated row in `x`. Each Dict maps the values to the number of
  occurrences (as an INT64) in the final dimension.

  Example:
    x = kd.slice([[4, 3, 4], [None, 2], [2, 1, 4, 1], [None]])
    kd_ext.contrib.value_counts(x)
      # -> [Dict{4: 2, 3: 1}, Dict{2: 1}, Dict{2: 1, 1: 2, 4: 1}, Dict{}]

  Args:
    x: the non-scalar DataSlice to compute occurrences for.
  """
  grouped = kd.group_by(x)
  return kd.dict(kd.collapse(grouped), kd.agg_count(grouped))
