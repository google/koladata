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

from koladata import kd


@kd.optools.add_to_registry()
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
  grouped = kd.lazy.group_by(x)
  return kd.lazy.dict(kd.lazy.collapse(grouped), kd.lazy.agg_count(grouped))
